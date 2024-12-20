package integration

import (
	"context"
	"errors"
	"strings"
	"testing"

	partitionscaler "github.com/aykanferhat/go-kafka-partition-scaler"

	"github.com/IBM/sarama"

	"github.com/aykanferhat/go-kafka-partition-scaler/pkg/log"

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
	kafkaContainer, err := containerKafka.Run(ctx,
		"confluentinc/confluent-local:7.7.0",
		containerKafka.WithClusterID("test-cluster"),
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
	logger := log.NewConsoleLog(log.INFO)
	producers, err := partitionscaler.NewProducerBuilderWithConfig(clusterConfigsMap, producerTopicMap).
		Interceptor(producerInterceptor).
		Log(logger).
		Initialize()
	if err != nil {
		assert.NilError(t, err)
	}
	consumers, errorConsumers, err := partitionscaler.NewConsumerBuilder(clusterConfigsMap, consumerConfigs, consumersList).
		LastStepFunc(lastStepFunc).
		Interceptor(consumerInterceptor).
		ErrorInterceptor(consumerErrorInterceptor).
		Log(logger).
		Initialize(ctx)
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
	kafkaContainer, err := containerKafka.Run(ctx,
		"confluentinc/confluent-local:7.7.0",
		containerKafka.WithClusterID("test-cluster"),
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
	logger := log.NewConsoleLog(log.INFO)
	producers, err := partitionscaler.NewProducerBuilderWithConfig(clusterConfigsMap, producerTopicMap).
		Interceptor(producerInterceptor).
		Log(logger).
		Initialize()
	if err != nil {
		assert.NilError(t, err)
	}
	errorConsumers, err := partitionscaler.NewErrorConsumerBuilder(clusterConfigsMap, consumerConfigs).
		Consumers(consumerLists).
		LastStepFunc(lastStepFunc).
		ErrorInterceptor(consumerErrorInterceptor).
		Log(logger).
		Initialize(ctx)
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
