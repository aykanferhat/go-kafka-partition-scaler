package integration

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	partitionscaler "github.com/Trendyol/go-kafka-partition-scaler"

	"github.com/IBM/sarama"

	"github.com/Trendyol/go-kafka-partition-scaler/pkg/log"

	"github.com/testcontainers/testcontainers-go"
	containerKafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"gotest.tools/v3/assert"
)

const (
	topicConfigName     = "topic"
	clusterName         = "cluster"
	everyTenSeconds     = "@every 10s"
	everyFifteenSeconds = "@every 15s"
	everyTwentySeconds  = "@every 20s"
	everyThirtySeconds  = "@every 30s"
	topic               = "message.topic.0"
	retryTopic          = "message.topic.RETRY.0"
	errorTopic          = "message.topic.ERROR.0"
	groupID             = "message.topic.consumer.0"
	errorGroupID        = "error-group"
	partition           = int32(0)
	totalPartition      = int32(1)
)

func InitializeTestCluster(
	ctx context.Context,
	t *testing.T,
	clusterConfigsMap partitionscaler.ClusterConfigMap,
	producerTopicMap partitionscaler.ProducerTopicConfigMap,
	consumerConfigs partitionscaler.ConsumerGroupConfigMap,
	consumersList []*partitionscaler.ConsumerGroupConsumers,
	consumerInterceptor partitionscaler.ConsumerInterceptor,
	consumerErrorInterceptor partitionscaler.ConsumerErrorInterceptor,
	producerInterceptor partitionscaler.ProducerInterceptor,
	lastStepFunc func(ctx context.Context, message *partitionscaler.ConsumerMessage, err error),
	partition int32,
) (kafkaContainer *containerKafka.KafkaContainer, producers partitionscaler.Producer, consumers map[string]partitionscaler.ConsumerGroup, errorConsumers map[string]partitionscaler.ErrorConsumerGroup) {
	for {
		timeoutContext, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		startedChan := make(chan bool)
		kafkaContainer, producers, consumers, errorConsumers = initializeTestCluster(
			ctx,
			t,
			clusterConfigsMap,
			producerTopicMap,
			consumerConfigs,
			consumersList,
			consumerInterceptor,
			consumerErrorInterceptor,
			producerInterceptor,
			lastStepFunc,
			partition,
		)
		go func() {
			for _, consumerGroup := range consumers {
				consumerGroup.WaitConsumerStart()
			}
			startedChan <- true
		}()

		select {
		case <-startedChan:
			cancel()
			for _, consumerGroup := range consumers {
				consumerGroup.Unsubscribe()
				consumerGroup.WaitConsumerStop()
			}
			return kafkaContainer, producers, consumers, errorConsumers
		case <-timeoutContext.Done():
			cancel()
			_ = kafkaContainer.Terminate(ctx)
			continue
		}
	}
}

func InitializeErrorConsumerTestCluster(
	ctx context.Context,
	t *testing.T,
	clusterConfigsMap partitionscaler.ClusterConfigMap,
	producerTopicMap partitionscaler.ProducerTopicConfigMap,
	consumerConfigs partitionscaler.ConsumerGroupErrorConfigMap,
	consumerLists []*partitionscaler.ConsumerGroupErrorConsumers,
	consumerErrorInterceptor partitionscaler.ConsumerErrorInterceptor,
	producerInterceptor partitionscaler.ProducerInterceptor,
	lastStepFunc func(ctx context.Context, message *partitionscaler.ConsumerMessage, err error),
) (kafkaContainer *containerKafka.KafkaContainer, producers partitionscaler.Producer, errorConsumers map[string]partitionscaler.ErrorConsumerGroup) {
	for {
		timeoutContext, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		startedChan := make(chan bool)
		kafkaContainer, producers, errorConsumers = initializeErrorConsumerTestCluster(
			ctx,
			t,
			clusterConfigsMap,
			producerTopicMap,
			consumerConfigs,
			consumerLists,
			consumerErrorInterceptor,
			producerInterceptor,
			lastStepFunc,
		)
		go func() {
			for _, errorConsumer := range errorConsumers {
				errorConsumer.Subscribe()
				errorConsumer.WaitConsumerStart()
			}
			startedChan <- true
		}()

		select {
		case <-startedChan:
			cancel()
			for _, errorConsumer := range errorConsumers {
				errorConsumer.Unsubscribe()
				errorConsumer.WaitConsumerStop()
			}
			return kafkaContainer, producers, errorConsumers
		case <-timeoutContext.Done():
			cancel()
			_ = kafkaContainer.Terminate(ctx)
			continue
		}
	}
}

//nolint:funlen
func initializeTestCluster(
	ctx context.Context,
	t *testing.T,
	clusterConfigsMap partitionscaler.ClusterConfigMap,
	producerTopicMap partitionscaler.ProducerTopicConfigMap,
	consumerConfigs partitionscaler.ConsumerGroupConfigMap,
	consumersList []*partitionscaler.ConsumerGroupConsumers,
	consumerInterceptor partitionscaler.ConsumerInterceptor,
	consumerErrorInterceptor partitionscaler.ConsumerErrorInterceptor,
	producerInterceptor partitionscaler.ProducerInterceptor,
	lastStepFunc func(ctx context.Context, message *partitionscaler.ConsumerMessage, err error),
	partition int32,
) (*containerKafka.KafkaContainer, partitionscaler.Producer, map[string]partitionscaler.ConsumerGroup, map[string]partitionscaler.ErrorConsumerGroup) {
	kafkaContainer, err := containerKafka.RunContainer(ctx,
		containerKafka.WithClusterID("test-cluster"),
		testcontainers.WithImage("confluentinc/confluent-local:7.5.0"),
	)
	if err != nil {
		assert.NilError(t, err)
	}
	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		assert.NilError(t, err)
	}
	for _, clusterConfig := range clusterConfigsMap {
		clusterConfig.Brokers = strings.Join(brokers, ",")
	}
	for configName := range consumerConfigs {
		consumerGroupConfig, err := consumerConfigs.GetConfigWithDefault(configName)
		if err != nil {
			assert.NilError(t, err)
		}
		clusterConfig, err := clusterConfigsMap.GetConfigWithDefault(consumerGroupConfig.Cluster)
		if err != nil {
			assert.NilError(t, err)
		}
		if err := createTopic(clusterConfig, consumerGroupConfig.Name, partition); err != nil {
			assert.NilError(t, err)
		}
		if err := createTopic(clusterConfig, consumerGroupConfig.Retry, 1); err != nil {
			assert.NilError(t, err)
		}
		if err := createTopic(clusterConfig, consumerGroupConfig.Error, 1); err != nil {
			assert.NilError(t, err)
		}
	}
	log.Logger = log.NewConsoleLog(log.INFO)
	producers, err := partitionscaler.NewProducerBuilderWithConfig(clusterConfigsMap, producerTopicMap).
		Interceptor(producerInterceptor).
		Initialize()
	if err != nil {
		assert.NilError(t, err)
	}
	consumers, errorConsumers, err := partitionscaler.NewConsumerBuilder(clusterConfigsMap, consumerConfigs, consumersList).
		LastStepFunc(lastStepFunc).
		Interceptor(consumerInterceptor).
		ErrorInterceptor(consumerErrorInterceptor).
		Initialize()
	if err != nil {
		assert.NilError(t, err)
	}
	return kafkaContainer, producers, consumers, errorConsumers
}

//nolint:funlen
func initializeErrorConsumerTestCluster(
	ctx context.Context,
	t *testing.T,
	clusterConfigsMap partitionscaler.ClusterConfigMap,
	producerTopicMap partitionscaler.ProducerTopicConfigMap,
	consumerConfigs partitionscaler.ConsumerGroupErrorConfigMap,
	consumerLists []*partitionscaler.ConsumerGroupErrorConsumers,
	consumerErrorInterceptor partitionscaler.ConsumerErrorInterceptor,
	producerInterceptor partitionscaler.ProducerInterceptor,
	lastStepFunc func(ctx context.Context, message *partitionscaler.ConsumerMessage, err error),
) (*containerKafka.KafkaContainer, partitionscaler.Producer, map[string]partitionscaler.ErrorConsumerGroup) {
	kafkaContainer, err := containerKafka.RunContainer(ctx,
		containerKafka.WithClusterID("test-cluster"),
		testcontainers.WithImage("confluentinc/confluent-local:7.5.0"),
	)
	if err != nil {
		assert.NilError(t, err)
	}
	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		assert.NilError(t, err)
	}
	for _, clusterConfig := range clusterConfigsMap {
		clusterConfig.Brokers = strings.Join(brokers, ",")
	}
	for configName := range consumerConfigs {
		consumerGroupConfig, err := consumerConfigs.GetConfigWithDefault(configName)
		if err != nil {
			assert.NilError(t, err)
		}
		clusterConfig, err := clusterConfigsMap.GetConfigWithDefault(consumerGroupConfig.Cluster)
		if err != nil {
			assert.NilError(t, err)
		}
		for _, topic := range consumerGroupConfig.Topics {
			if err := createTopic(clusterConfig, topic, 1); err != nil {
				assert.NilError(t, err)
			}
		}
	}
	time.Sleep(2 * time.Second) // After creating a topic, wait for synchronization.
	log.Logger = log.NewConsoleLog(log.INFO)
	producers, err := partitionscaler.NewProducerBuilderWithConfig(clusterConfigsMap, producerTopicMap).
		Interceptor(producerInterceptor).
		Initialize()
	if err != nil {
		assert.NilError(t, err)
	}
	errorConsumers, err := partitionscaler.NewErrorConsumerBuilder(clusterConfigsMap, consumerConfigs).
		Consumers(consumerLists).
		LastStepFunc(lastStepFunc).
		ErrorInterceptor(consumerErrorInterceptor).
		Initialize()
	if err != nil {
		assert.NilError(t, err)
	}
	return kafkaContainer, producers, errorConsumers
}

func createTopic(clusterConfig *partitionscaler.ClusterConfig, topicName string, partition int32) error {
	clusterAdmin, err := sarama.NewClusterAdmin(clusterConfig.GetBrokers(), nil)
	if err != nil {
		return err
	}
	if err := clusterAdmin.CreateTopic(topicName, &sarama.TopicDetail{NumPartitions: partition, ReplicationFactor: 1}, false); err != nil {
		var topicError *sarama.TopicError
		if errors.As(err, &topicError) && topicError.Err != sarama.ErrTopicAlreadyExists {
			return err
		}
	}
	if err := clusterAdmin.Close(); err != nil {
		return err
	}
	return nil
}
