package integration

import (
	"context"
	"testing"
	"time"

	"github.com/IBM/sarama"

	partitionscaler "github.com/Trendyol/go-kafka-partition-scaler"
	"github.com/Trendyol/go-kafka-partition-scaler/test/testdata"
	"gotest.tools/v3/assert"
)

func Test_ErrorConsumer_ShouldCloseConsumerWhenThereIsNoNewMessage(t *testing.T) {
	// Given
	ctx := context.Background()

	closeConsumerWhenThereIsNoMessage := 5 * time.Second

	clusterConfigsMap := map[string]*partitionscaler.ClusterConfig{
		clusterName: {
			Brokers: "", // dynamic
			Version: sarama.V3_6_0_0.String(),
			ErrorConfig: &partitionscaler.ErrorConfig{
				GroupID:                           errorGroupID,
				Cron:                              everyTwentySeconds,
				MaxErrorCount:                     3,
				MaxProcessingTime:                 1 * time.Second,
				CloseConsumerWhenThereIsNoMessage: closeConsumerWhenThereIsNoMessage,
				CloseConsumerWhenMessageIsNew:     1 * time.Minute,
			},
			ClientID: "client-id",
		},
	}

	consumerConfigs := map[string]*partitionscaler.ConsumerGroupConfig{
		topicConfigName: {
			GroupID:               groupID,
			Name:                  topic,
			Retry:                 retryTopic,
			Error:                 errorTopic,
			RetryCount:            3,
			VirtualPartitionCount: 1,
			MaxProcessingTime:     1 * time.Second,
			Cluster:               clusterName,
		},
	}

	consumedMessageChan := make(chan *partitionscaler.ConsumerMessage, 1)
	consumersList := []*partitionscaler.ConsumerGroupConsumers{
		{
			ConfigName: topicConfigName,
			Consumer:   NewTestMessageConsumer(consumedMessageChan),
		},
	}

	consumerInterceptor := NewTestConsumerHeaderInterceptor()
	consumerErrorInterceptor := NewTestConsumerErrorInterceptor()
	producerInterceptor := NewTestProducerInterceptor()

	// When
	kafkaContainer, _, _, errorConsumerGroups := InitializeTestCluster(
		ctx,
		t,
		clusterConfigsMap,
		map[string]*partitionscaler.ProducerTopic{},
		consumerConfigs,
		consumersList,
		consumerInterceptor,
		consumerErrorInterceptor,
		producerInterceptor,
		func(ctx context.Context, message *partitionscaler.ConsumerMessage, err error) {},
		totalPartition,
	)

	defer func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			assert.NilError(t, err)
		}
	}()

	errorConsumerGroup := errorConsumerGroups[errorGroupID]

	errorConsumerGroup.WaitConsumerStart()

	// Then
	assert.Equal(t, true, errorConsumerGroup.IsRunning())

	errorConsumerGroup.WaitConsumerStop()
	assert.Equal(t, false, errorConsumerGroup.IsRunning())
}

func Test_ErrorConsumer_ShouldCloseConsumerWhenMessageIsNew(t *testing.T) {
	// Given
	ctx := context.Background()

	closeConsumerWhenMessageIsNew := 5 * time.Minute

	clusterConfigsMap := map[string]*partitionscaler.ClusterConfig{
		clusterName: {
			Brokers: "", // dynamic
			Version: sarama.V3_6_0_0.String(),
			ErrorConfig: &partitionscaler.ErrorConfig{
				GroupID:                           errorGroupID,
				Cron:                              everyFifteenSeconds,
				MaxErrorCount:                     1,
				MaxProcessingTime:                 1 * time.Second,
				CloseConsumerWhenThereIsNoMessage: 2 * time.Minute,
				CloseConsumerWhenMessageIsNew:     closeConsumerWhenMessageIsNew,
			},
			ClientID: "client-id",
		},
	}

	consumerConfigs := map[string]*partitionscaler.ConsumerGroupConfig{
		topicConfigName: {
			GroupID:               groupID,
			Name:                  topic,
			Retry:                 retryTopic,
			Error:                 errorTopic,
			RetryCount:            1,
			VirtualPartitionCount: 1,
			MaxProcessingTime:     1 * time.Second,
			Cluster:               clusterName,
		},
	}

	consumedMessageChan := make(chan *partitionscaler.ConsumerMessage, 1000)
	consumersList := []*partitionscaler.ConsumerGroupConsumers{
		{
			ConfigName: topicConfigName,
			Consumer:   NewTestMessageConsumer(consumedMessageChan),
		},
	}

	producerTopicsMap := map[string]*partitionscaler.ProducerTopic{
		topicConfigName: {
			Name:    topic,
			Cluster: clusterName,
		},
	}

	consumerInterceptor := NewTestConsumerHeaderInterceptor()
	consumerErrorInterceptor := NewTestConsumerErrorInterceptor()
	producerInterceptor := NewTestProducerInterceptor()

	// When
	kafkaContainer, producers, consumerGroups, errorConsumerGroups := InitializeTestCluster(
		ctx,
		t,
		clusterConfigsMap,
		producerTopicsMap,
		consumerConfigs,
		consumersList,
		consumerInterceptor,
		consumerErrorInterceptor,
		producerInterceptor,
		func(ctx context.Context, message *partitionscaler.ConsumerMessage, err error) {},
		totalPartition,
	)

	// Clean up the container after
	defer func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			assert.NilError(t, err)
		}
	}()

	consumerGroup := consumerGroups[groupID]
	errorConsumerGroup := errorConsumerGroups[errorGroupID]

	_ = consumerGroup.Subscribe()

	consumerGroup.WaitConsumerStart()
	errorConsumerGroup.WaitConsumerStart()

	produceMessages := []partitionscaler.Message{
		&testdata.TestWrongTypeProducerMessage{Id: "100", Name: "Test Message 1"},
		&testdata.TestWrongTypeProducerMessage{Id: "101", Name: "Test Message 2"},
	}

	if err := producers.ProduceSyncBulk(ctx, produceMessages, 100); err != nil {
		assert.NilError(t, err)
	} // wrong message type

	// Then

	errorConsumerGroup.WaitConsumerStop()
	consumerGroup.Unsubscribe()
	consumerGroup.WaitConsumerStop()

	// Then
	close(consumedMessageChan)
	assert.Equal(t, false, errorConsumerGroup.IsRunning())
}

func Test_ErrorConsumer_ShouldCloseConsumerWhenUnsubscribe(t *testing.T) {
	// Given
	ctx := context.Background()

	clusterConfigsMap := map[string]*partitionscaler.ClusterConfig{
		clusterName: {
			Brokers: "", // dynamic
			Version: sarama.V3_6_0_0.String(),
			ErrorConfig: &partitionscaler.ErrorConfig{
				GroupID:                           errorGroupID,
				Cron:                              everyFifteenSeconds,
				MaxErrorCount:                     1,
				MaxProcessingTime:                 1 * time.Second,
				CloseConsumerWhenThereIsNoMessage: 1 * time.Minute,
				CloseConsumerWhenMessageIsNew:     1 * time.Millisecond,
			},
			ClientID: "client-id",
		},
	}

	consumerConfigs := map[string]*partitionscaler.ConsumerGroupConfig{
		topicConfigName: {
			GroupID:               groupID,
			Name:                  topic,
			Retry:                 retryTopic,
			Error:                 errorTopic,
			RetryCount:            1,
			VirtualPartitionCount: 1,
			MaxProcessingTime:     1 * time.Second,
			Cluster:               clusterName,
		},
	}

	consumedMessageChan := make(chan *partitionscaler.ConsumerMessage, 1000)
	consumersList := []*partitionscaler.ConsumerGroupConsumers{
		{
			ConfigName:    topicConfigName,
			Consumer:      NewTestMessageConsumer(consumedMessageChan),
			ErrorConsumer: NewTestErrorConsumer(),
		},
	}

	producerTopicsMap := map[string]*partitionscaler.ProducerTopic{
		topicConfigName: {
			Name:    topic,
			Cluster: clusterName,
		},
	}

	consumerInterceptor := NewTestConsumerHeaderInterceptor()
	consumerErrorInterceptor := NewTestConsumerErrorInterceptor()
	producerInterceptor := NewTestProducerInterceptor()

	// When
	kafkaContainer, _, consumerGroups, errorConsumerGroups := InitializeTestCluster(
		ctx,
		t,
		clusterConfigsMap,
		producerTopicsMap,
		consumerConfigs,
		consumersList,
		consumerInterceptor,
		consumerErrorInterceptor,
		producerInterceptor,
		func(ctx context.Context, message *partitionscaler.ConsumerMessage, err error) {},
		totalPartition,
	)

	// Clean up the container after
	defer func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			assert.NilError(t, err)
		}
	}()

	consumerGroup := consumerGroups[groupID]
	errorConsumerGroup := errorConsumerGroups[errorGroupID]

	_ = consumerGroup.Subscribe()

	consumerGroup.WaitConsumerStart()
	errorConsumerGroup.WaitConsumerStart()

	// Then
	assert.Equal(t, true, errorConsumerGroup.IsRunning())

	consumerGroup.Unsubscribe()
	errorConsumerGroup.Unsubscribe()

	consumerGroup.WaitConsumerStop()
	errorConsumerGroup.WaitConsumerStop()
	assert.Equal(t, false, errorConsumerGroup.IsRunning())
}
