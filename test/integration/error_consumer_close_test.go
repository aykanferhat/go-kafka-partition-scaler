package integration

import (
	"context"
	"testing"
	"time"

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
			Version: "2.2.0",
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
			Name:                  "message.topic.0",
			Retry:                 "message.topic.RETRY.0",
			Error:                 "message.topic.ERROR.0",
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
	kafkaContainer, _, producers, errorConsumerGroups := InitializeTestCluster(
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
	consumerGroup := producers[groupID]
	errorConsumerGroup := errorConsumerGroups[errorGroupID]

	defer func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			assert.NilError(t, err)
		}
	}()

	consumerGroup.WaitConsumerStart()
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
			Version: "2.2.0",
			ErrorConfig: &partitionscaler.ErrorConfig{
				GroupID:                           errorGroupID,
				Cron:                              everyTwentySeconds,
				MaxErrorCount:                     1,
				MaxProcessingTime:                 1 * time.Second,
				CloseConsumerWhenThereIsNoMessage: 1 * time.Minute,
				CloseConsumerWhenMessageIsNew:     closeConsumerWhenMessageIsNew,
			},
			ClientID: "client-id",
		},
	}

	consumerConfigs := map[string]*partitionscaler.ConsumerGroupConfig{
		topicConfigName: {
			GroupID:               groupID,
			Name:                  topic,
			Retry:                 "message.topic.RETRY.0",
			Error:                 "message.topic.ERROR.0",
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
	consumerGroup := consumerGroups[groupID]
	errorConsumerGroup := errorConsumerGroups[errorGroupID]

	// Clean up the container after
	defer func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			assert.NilError(t, err)
		}
	}()

	consumerGroup.WaitConsumerStart()
	errorConsumerGroup.WaitConsumerStart()

	time.Sleep(2 * time.Second)

	// Then
	assert.Equal(t, true, errorConsumerGroup.IsRunning())

	if err := producers.ProduceSyncBulk(ctx, []partitionscaler.Message{
		&testdata.TestWrongTypeProducerMessage{Id: "100", Name: "Test Message 1"},
		&testdata.TestWrongTypeProducerMessage{Id: "101", Name: "Test Message 2"},
	}, 100); err != nil {
		assert.NilError(t, err)
	} // wrong message type

	time.Sleep(2 * time.Second)

	errorConsumerGroup.WaitConsumerStop()

	assert.Equal(t, false, errorConsumerGroup.IsRunning())
}

func Test_ErrorConsumer_ShouldCloseConsumerWhenUnsubscribe(t *testing.T) {
	// Given
	ctx := context.Background()

	errorConsumerCron := "@every 15s"

	clusterConfigsMap := map[string]*partitionscaler.ClusterConfig{
		clusterName: {
			Brokers: "", // dynamic
			Version: "2.2.0",
			ErrorConfig: &partitionscaler.ErrorConfig{
				GroupID:                           errorGroupID,
				Cron:                              errorConsumerCron,
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
			Retry:                 "message.topic.RETRY.0",
			Error:                 "message.topic.ERROR.0",
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
	consumerGroup := consumerGroups[groupID]
	errorConsumerGroup := errorConsumerGroups[errorGroupID]

	// Clean up the container after
	defer func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			assert.NilError(t, err)
		}
	}()

	consumerGroup.WaitConsumerStart()
	errorConsumerGroup.WaitConsumerStart()

	// Then
	assert.Equal(t, true, errorConsumerGroup.IsRunning())
	errorConsumerGroup.Unsubscribe()
	errorConsumerGroup.WaitConsumerStop()
	assert.Equal(t, false, errorConsumerGroup.IsRunning())
}
