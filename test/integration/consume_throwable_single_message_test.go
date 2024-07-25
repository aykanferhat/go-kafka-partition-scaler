package integration

import (
	"context"
	"testing"
	"time"

	partitionscaler "github.com/Trendyol/go-kafka-partition-scaler"
	"github.com/Trendyol/go-kafka-partition-scaler/pkg/json"
	"github.com/Trendyol/go-kafka-partition-scaler/test/testdata"
	"gotest.tools/v3/assert"
)

func Test_SingleConsumer_ShouldConsumeThrowableMessage(t *testing.T) {
	// Given
	ctx := context.Background()

	maxRetryCount := 1
	virtualPartitionCount := 1
	closeConsumerWhenMessageIsNew := time.Millisecond

	clusterConfigsMap := map[string]*partitionscaler.ClusterConfig{
		clusterName: {
			Brokers: "", // dynamic
			Version: "2.2.0",
			ErrorConfig: &partitionscaler.ErrorConfig{
				GroupID:                           errorGroupID,
				Cron:                              everyThirtySeconds,
				MaxErrorCount:                     1,
				MaxProcessingTime:                 1 * time.Second,
				CloseConsumerWhenThereIsNoMessage: 5 * time.Minute,
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
			RetryCount:            maxRetryCount,
			VirtualPartitionCount: virtualPartitionCount,
			MaxProcessingTime:     1 * time.Second,
			Cluster:               clusterName,
		},
	}

	producerTopicsMap := map[string]*partitionscaler.ProducerTopic{
		topicConfigName: {
			Name:    topic,
			Cluster: clusterName,
		},
	}

	consumedMessageChan := make(chan *partitionscaler.ConsumerMessage, 1)
	consumedErrorMessageChan := make(chan *partitionscaler.ConsumerMessage, 1)
	consumersList := []*partitionscaler.ConsumerGroupConsumers{
		{
			ConfigName:    topicConfigName,
			Consumer:      NewTestMessageConsumer(consumedMessageChan),
			ErrorConsumer: NewTestErrorConsumerWithChannel(consumedErrorMessageChan),
		},
	}

	consumerInterceptor := NewTestConsumerHeaderInterceptor()
	consumerErrorInterceptor := NewTestConsumerErrorInterceptor()
	producerInterceptor := NewTestProducerInterceptor()

	// When
	kafkaContainer, producers, consumers, errorConsumers := InitializeTestCluster(
		ctx,
		t,
		clusterConfigsMap,
		producerTopicsMap,
		consumerConfigs, consumersList,
		consumerInterceptor,
		consumerErrorInterceptor,
		producerInterceptor,
		func(ctx context.Context, message *partitionscaler.ConsumerMessage, err error) {},
		totalPartition,
	)
	consumerGroup := consumers[groupID]

	defer func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			assert.NilError(t, err)
		}
	}()

	consumerGroup.WaitConsumerStart()

	for _, consumerGroup := range errorConsumers {
		consumerGroup.WaitConsumerStart()
	}

	if err := producers.ProduceSync(ctx, &testdata.TestWrongTypeProducerMessage{Id: "100", Name: "Test Message"}); err != nil {
		assert.NilError(t, err)
	} // wrong message type

	// Then
	go func(consumedMessageCh chan *partitionscaler.ConsumerMessage, consumedErrorMessageCh chan *partitionscaler.ConsumerMessage) {
		consumedErrorMessage := <-consumedErrorMessageCh
		var errMessage testdata.TestWrongTypeProducerMessage
		if err := json.Unmarshal(consumedErrorMessage.Value, &errMessage); err != nil {
			assert.NilError(t, err)
		}

		assert.Equal(t, errMessage.Id, "100")
		assert.Equal(t, errMessage.Name, "Test Message")
		assert.Equal(t, consumedErrorMessage.Topic, errorTopic)
		assert.Equal(t, consumedErrorMessage.Partition, int32(0))
		assert.Equal(t, consumedErrorMessage.VirtualPartition, 0)
		assert.Equal(t, consumedErrorMessage.Offset, int64(0))

		close(consumedMessageCh)
		close(consumedErrorMessageCh)
	}(consumedMessageChan, consumedErrorMessageChan)

	consumedMessages := make([]*partitionscaler.ConsumerMessage, 0)
	consumedRetiedMessages := make([]*partitionscaler.ConsumerMessage, 0)
	for consumedMessage := range consumedMessageChan {
		if consumedMessage.Topic == retryTopic {
			consumedRetiedMessages = append(consumedRetiedMessages, consumedMessage)
			continue
		}
		consumedMessages = append(consumedMessages, consumedMessage)
	}

	assert.Equal(t, 1, len(consumedMessages))
	assert.Equal(t, maxRetryCount, len(consumedRetiedMessages))
}

func Test_SingleConsumer_ShouldConsumeThrowableMessageWhenErrorTopicNotFound(t *testing.T) {
	// Given
	ctx := context.Background()

	maxRetryCount := 1
	virtualPartitionCount := 1
	closeConsumerWhenMessageIsNew := time.Millisecond

	clusterConfigsMap := map[string]*partitionscaler.ClusterConfig{
		clusterName: {
			Brokers: "", // dynamic
			Version: "2.2.0",
			ErrorConfig: &partitionscaler.ErrorConfig{
				GroupID:                           errorGroupID,
				Cron:                              everyThirtySeconds,
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
			Retry:                 retryTopic,
			RetryCount:            maxRetryCount,
			VirtualPartitionCount: virtualPartitionCount,
			MaxProcessingTime:     1 * time.Second,
			Cluster:               clusterName,
		},
	}

	producerTopicsMap := map[string]*partitionscaler.ProducerTopic{
		topicConfigName: {
			Name:    topic,
			Cluster: clusterName,
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

	consumedLastStepErrMessageChan := make(chan *partitionscaler.ConsumerMessage, 1)

	// When
	kafkaContainer, producers, consumers, _ := InitializeTestCluster(
		ctx,
		t,
		clusterConfigsMap,
		producerTopicsMap,
		consumerConfigs,
		consumersList,
		consumerInterceptor,
		consumerErrorInterceptor,
		producerInterceptor,
		func(ctx context.Context, message *partitionscaler.ConsumerMessage, err error) {
			consumedLastStepErrMessageChan <- message
		},
		totalPartition,
	)
	consumerGroup := consumers[groupID]

	defer func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			assert.NilError(t, err)
		}
	}()

	consumerGroup.WaitConsumerStart()

	if err := producers.ProduceSync(ctx, &testdata.TestWrongTypeProducerMessage{Id: "100", Name: "Test Message"}); err != nil {
		assert.NilError(t, err)
	} // wrong message type

	// Then
	go func(consumedMessageCh chan *partitionscaler.ConsumerMessage, consumedLastStepErrorMessageCh chan *partitionscaler.ConsumerMessage) {
		consumedErrorMessage := <-consumedLastStepErrorMessageCh
		var errMessage testdata.TestWrongTypeProducerMessage
		if err := json.Unmarshal(consumedErrorMessage.Value, &errMessage); err != nil {
			assert.NilError(t, err)
		}

		assert.Equal(t, errMessage.Id, "100")
		assert.Equal(t, errMessage.Name, "Test Message")
		assert.Equal(t, consumedErrorMessage.Topic, retryTopic)
		assert.Equal(t, consumedErrorMessage.Partition, int32(0))
		assert.Equal(t, consumedErrorMessage.VirtualPartition, 0)
		assert.Equal(t, consumedErrorMessage.Offset, int64(0))

		close(consumedMessageCh)
		close(consumedLastStepErrorMessageCh)
	}(consumedMessageChan, consumedLastStepErrMessageChan)

	consumedMessages := make([]*partitionscaler.ConsumerMessage, 0)
	consumedRetiedMessages := make([]*partitionscaler.ConsumerMessage, 0)
	for consumedMessage := range consumedMessageChan {
		if consumedMessage.Topic == retryTopic {
			consumedRetiedMessages = append(consumedRetiedMessages, consumedMessage)
			continue
		}
		consumedMessages = append(consumedMessages, consumedMessage)
	}

	assert.Equal(t, 1, len(consumedMessages))
	assert.Equal(t, maxRetryCount, len(consumedRetiedMessages))
}

func Test_SingleConsumer_ShouldConsumeThrowableMessageWhenRetryTopicNotFound(t *testing.T) {
	// Given
	ctx := context.Background()

	virtualPartitionCount := 1
	closeConsumerWhenMessageIsNew := time.Millisecond

	clusterConfigsMap := map[string]*partitionscaler.ClusterConfig{
		clusterName: {
			Brokers: "", // dynamic
			Version: "2.2.0",
			ErrorConfig: &partitionscaler.ErrorConfig{
				GroupID:                           errorGroupID,
				Cron:                              everyFifteenSeconds,
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
			Error:                 errorTopic,
			VirtualPartitionCount: virtualPartitionCount,
			MaxProcessingTime:     1 * time.Second,
			Cluster:               clusterName,
		},
	}

	producerTopicsMap := map[string]*partitionscaler.ProducerTopic{
		topicConfigName: {
			Name:    topic,
			Cluster: clusterName,
		},
	}

	consumedMessageChan := make(chan *partitionscaler.ConsumerMessage, 1)
	consumedErrorMessageChan := make(chan *partitionscaler.ConsumerMessage, 1)
	consumersList := []*partitionscaler.ConsumerGroupConsumers{
		{
			ConfigName:    topicConfigName,
			Consumer:      NewTestMessageConsumer(consumedMessageChan),
			ErrorConsumer: NewTestErrorConsumerWithChannel(consumedErrorMessageChan),
		},
	}

	consumerInterceptor := NewTestConsumerHeaderInterceptor()
	consumerErrorInterceptor := NewTestConsumerErrorInterceptor()
	producerInterceptor := NewTestProducerInterceptor()

	// When
	kafkaContainer, producers, consumers, errorConsumers := InitializeTestCluster(
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
	consumerGroup := consumers[groupID]
	errorConsumerGroup := errorConsumers[errorGroupID]

	defer func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			assert.NilError(t, err)
		}
	}()

	consumerGroup.WaitConsumerStart()
	errorConsumerGroup.WaitConsumerStart()

	if err := producers.ProduceSync(ctx, &testdata.TestWrongTypeProducerMessage{Id: "100", Name: "Test Message"}); err != nil {
		assert.NilError(t, err)
	} // wrong message type

	// Then
	go func(consumedMessageCh chan *partitionscaler.ConsumerMessage, consumedErrorMessageCh chan *partitionscaler.ConsumerMessage) {
		consumedErrorMessage := <-consumedErrorMessageCh
		var errMessage testdata.TestWrongTypeProducerMessage
		if err := json.Unmarshal(consumedErrorMessage.Value, &errMessage); err != nil {
			assert.NilError(t, err)
		}

		assert.Equal(t, errMessage.Id, "100")
		assert.Equal(t, errMessage.Name, "Test Message")
		assert.Equal(t, consumedErrorMessage.Topic, errorTopic)
		assert.Equal(t, consumedErrorMessage.Partition, int32(0))
		assert.Equal(t, consumedErrorMessage.VirtualPartition, 0)
		assert.Equal(t, consumedErrorMessage.Offset, int64(0))

		close(consumedMessageCh)
		close(consumedErrorMessageCh)
	}(consumedMessageChan, consumedErrorMessageChan)

	consumedMessages := make([]*partitionscaler.ConsumerMessage, 0)
	for consumedMessage := range consumedMessageChan {
		consumedMessages = append(consumedMessages, consumedMessage)
	}

	assert.Equal(t, 1, len(consumedMessages))
}

func Test_SingleConsumer_ShouldConsumeThrowableMessageWhenRetryAndErrorTopicNotFound(t *testing.T) {
	// Given
	ctx := context.Background()

	virtualPartitionCount := 1

	errorConsumerCron := "@every 15s"
	closeConsumerWhenMessageIsNew := time.Millisecond

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
				CloseConsumerWhenMessageIsNew:     closeConsumerWhenMessageIsNew,
			},
			ClientID: "client-id",
		},
	}

	consumerConfigs := map[string]*partitionscaler.ConsumerGroupConfig{
		topicConfigName: {
			GroupID:               groupID,
			Name:                  topic,
			VirtualPartitionCount: virtualPartitionCount,
			MaxProcessingTime:     1 * time.Second,
			Cluster:               clusterName,
		},
	}

	producerTopicsMap := map[string]*partitionscaler.ProducerTopic{
		topicConfigName: {
			Name:    topic,
			Cluster: clusterName,
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

	consumedLastStepErrorMessageChan := make(chan *partitionscaler.ConsumerMessage, 1)

	// When
	kafkaContainer, producers, consumers, _ := InitializeTestCluster(
		ctx,
		t,
		clusterConfigsMap,
		producerTopicsMap,
		consumerConfigs,
		consumersList,
		consumerInterceptor,
		consumerErrorInterceptor,
		producerInterceptor,
		func(ctx context.Context, message *partitionscaler.ConsumerMessage, err error) {
			consumedLastStepErrorMessageChan <- message
		},
		totalPartition,
	)
	consumerGroup := consumers[groupID]

	defer func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			assert.NilError(t, err)
		}
	}()

	consumerGroup.WaitConsumerStart()

	if err := producers.ProduceSync(ctx, &testdata.TestWrongTypeProducerMessage{Id: "100", Name: "Test Message"}); err != nil {
		assert.NilError(t, err)
	} // wrong message type

	// Then
	go func(consumedMessageCh chan *partitionscaler.ConsumerMessage, consumedErrorMessageCh chan *partitionscaler.ConsumerMessage) {
		consumedErrorMessage := <-consumedErrorMessageCh
		var errMessage testdata.TestWrongTypeProducerMessage
		if err := json.Unmarshal(consumedErrorMessage.Value, &errMessage); err != nil {
			assert.NilError(t, err)
		}

		assert.Equal(t, errMessage.Id, "100")
		assert.Equal(t, errMessage.Name, "Test Message")
		assert.Equal(t, consumedErrorMessage.Topic, topic)
		assert.Equal(t, consumedErrorMessage.Partition, int32(0))
		assert.Equal(t, consumedErrorMessage.VirtualPartition, 0)
		assert.Equal(t, consumedErrorMessage.Offset, int64(0))

		close(consumedMessageCh)
		close(consumedErrorMessageCh)
	}(consumedMessageChan, consumedLastStepErrorMessageChan)

	consumedMessages := make([]*partitionscaler.ConsumerMessage, 0)
	for consumedMessage := range consumedMessageChan {
		consumedMessages = append(consumedMessages, consumedMessage)
	}

	assert.Equal(t, 1, len(consumedMessages))
}
