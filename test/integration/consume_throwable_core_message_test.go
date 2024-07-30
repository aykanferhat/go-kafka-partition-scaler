package integration

import (
	"context"
	"testing"
	"time"

	"github.com/IBM/sarama"

	partitionscaler "github.com/aykanferhat/go-kafka-partition-scaler"
	"github.com/aykanferhat/go-kafka-partition-scaler/pkg/json"
	"github.com/aykanferhat/go-kafka-partition-scaler/test/testdata"
	"gotest.tools/v3/assert"
)

func Test_CoreConsumer_ShouldConsumeThrowableMessage(t *testing.T) {
	// Given
	ctx, cancel := context.WithCancel(context.Background())

	maxRetryCount := 1
	closeConsumerWhenMessageIsNew := time.Millisecond

	clusterConfigsMap := map[string]*partitionscaler.ClusterConfig{
		clusterName: {
			Brokers: "", // dynamic
			Version: sarama.V3_6_0_0.String(),
			ErrorConfig: &partitionscaler.ErrorConfig{
				GroupID:                           errorGroupID,
				Cron:                              everyTenSeconds,
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
			GroupID:           groupID,
			Name:              topic,
			Retry:             retryTopic,
			Error:             errorTopic,
			RetryCount:        maxRetryCount,
			MaxProcessingTime: 1 * time.Second,
			Cluster:           clusterName,
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
			Consumer:      newTestMessageConsumer(consumedMessageChan),
			ErrorConsumer: newTestErrorConsumerWithChannel(consumedErrorMessageChan),
		},
	}

	consumerInterceptor := newTestConsumerHeaderInterceptor()
	consumerErrorInterceptor := newTestConsumerErrorInterceptor()
	producerInterceptor := newTestProducerInterceptor()

	// When
	kafkaContainer, producers, consumers, errorConsumers := initializeTestCluster(
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
		},
		totalPartition,
	)
	consumerGroup := consumers[groupID]
	errorConsumerGroup := errorConsumers[errorGroupID]

	defer func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			assert.NilError(t, err)
		}
		cancel()
	}()

	consumerGroup.WaitConsumerStart()
	errorConsumerGroup.WaitConsumerStart()

	if err := producers.ProduceSync(ctx, &testdata.TestWrongTypeProducerMessage{Id: "100", Name: "Test Message"}); err != nil {
		assert.NilError(t, err)
	} // wrong message type

	// Then
	go func() {
		consumedErrorMessage := <-consumedErrorMessageChan
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

		close(consumedMessageChan)
		close(consumedErrorMessageChan)
	}()

	consumedMessages := make([]*partitionscaler.ConsumerMessage, 0)
	consumedRetiedMessages := make([]*partitionscaler.ConsumerMessage, 0)
	for consumedMessage := range consumedMessageChan {
		if consumedMessage.Topic == retryTopic {
			consumedRetiedMessages = append(consumedRetiedMessages, consumedMessage)
			continue
		}
		consumedMessages = append(consumedMessages, consumedMessage)
	}

	consumerGroup.Unsubscribe()
	errorConsumerGroup.Unsubscribe()

	consumerGroup.WaitConsumerStop()
	errorConsumerGroup.WaitConsumerStop()

	assert.Equal(t, false, errorConsumerGroup.IsRunning())
	assert.Equal(t, 1, len(consumedMessages))
	assert.Equal(t, maxRetryCount, len(consumedRetiedMessages))
}

func Test_CoreConsumer_ShouldConsumeThrowableMessageWhenErrorTopicNotFound(t *testing.T) {
	// Given
	ctx, cancel := context.WithCancel(context.Background())

	maxRetryCount := 1

	clusterConfigsMap := map[string]*partitionscaler.ClusterConfig{
		clusterName: {
			Brokers:  "", // dynamic
			Version:  "2.2.0",
			ClientID: "client-id",
		},
	}

	consumerConfigs := map[string]*partitionscaler.ConsumerGroupConfig{
		topicConfigName: {
			GroupID:           groupID,
			Name:              topic,
			Retry:             retryTopic,
			RetryCount:        maxRetryCount,
			MaxProcessingTime: 1 * time.Second,
			Cluster:           clusterName,
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
			Consumer:   newTestMessageConsumer(consumedMessageChan),
		},
	}

	consumerInterceptor := newTestConsumerHeaderInterceptor()
	consumerErrorInterceptor := newTestConsumerErrorInterceptor()
	producerInterceptor := newTestProducerInterceptor()

	consumedLastStepErrMessageChan := make(chan *partitionscaler.ConsumerMessage, 1)

	// When
	kafkaContainer, producers, consumers, _ := initializeTestCluster(
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

	defer func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			assert.NilError(t, err)
		}
		cancel()
	}()

	consumerGroup := consumers[groupID]
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

		consumerGroup.Unsubscribe()
		consumerGroup.WaitConsumerStop()

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

func Test_CoreConsumer_ShouldConsumeThrowableMessageWhenRetryTopicNotFound(t *testing.T) {
	// Given
	ctx, cancel := context.WithCancel(context.Background())

	closeConsumerWhenMessageIsNew := time.Millisecond

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
				CloseConsumerWhenMessageIsNew:     closeConsumerWhenMessageIsNew,
			},
			ClientID: "client-id",
		},
	}

	consumerConfigs := map[string]*partitionscaler.ConsumerGroupConfig{
		topicConfigName: {
			GroupID:           groupID,
			Name:              topic,
			Error:             errorTopic,
			MaxProcessingTime: 1 * time.Second,
			Cluster:           clusterName,
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
			Consumer:      newTestMessageConsumer(consumedMessageChan),
			ErrorConsumer: newTestErrorConsumerWithChannel(consumedErrorMessageChan),
		},
	}

	consumerInterceptor := newTestConsumerHeaderInterceptor()
	consumerErrorInterceptor := newTestConsumerErrorInterceptor()
	producerInterceptor := newTestProducerInterceptor()

	// When
	kafkaContainer, producers, consumers, errorConsumers := initializeTestCluster(
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

	defer func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			assert.NilError(t, err)
		}
		cancel()
	}()

	consumerGroup := consumers[groupID]
	errorConsumerGroup := errorConsumers[errorGroupID]

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

		consumerGroup.Unsubscribe()
		errorConsumerGroup.Unsubscribe()

		consumerGroup.WaitConsumerStop()
		errorConsumerGroup.WaitConsumerStop()

		close(consumedMessageCh)
		close(consumedErrorMessageCh)
	}(consumedMessageChan, consumedErrorMessageChan)

	consumedMessages := make([]*partitionscaler.ConsumerMessage, 0)
	for consumedMessage := range consumedMessageChan {
		consumedMessages = append(consumedMessages, consumedMessage)
	}

	assert.Equal(t, false, errorConsumerGroup.IsRunning())
	assert.Equal(t, 1, len(consumedMessages))
}

func Test_CoreConsumer_ShouldConsumeThrowableMessageWhenRetryAndErrorTopicNotFound(t *testing.T) {
	// Given
	ctx, cancel := context.WithCancel(context.Background())

	clusterConfigsMap := map[string]*partitionscaler.ClusterConfig{
		clusterName: {
			Brokers:  "", // dynamic
			Version:  "2.2.0",
			ClientID: "client-id",
		},
	}

	consumerConfigs := map[string]*partitionscaler.ConsumerGroupConfig{
		topicConfigName: {
			GroupID:           groupID,
			Name:              topic,
			MaxProcessingTime: 1 * time.Second,
			Cluster:           clusterName,
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
			Consumer:   newTestMessageConsumer(consumedMessageChan),
		},
	}

	consumerInterceptor := newTestConsumerHeaderInterceptor()
	consumerErrorInterceptor := newTestConsumerErrorInterceptor()
	producerInterceptor := newTestProducerInterceptor()

	consumedLastStepErrorMessageChan := make(chan *partitionscaler.ConsumerMessage, 1)

	// When
	kafkaContainer, producers, consumers, errorConsumers := initializeTestCluster(
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

	defer func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			assert.NilError(t, err)
		}
		cancel()
	}()

	consumerGroup := consumers[groupID]
	consumerGroup.WaitConsumerStart()

	if err := producers.ProduceSync(ctx, &testdata.TestWrongTypeProducerMessage{Id: "100", Name: "Test Message"}); err != nil {
		assert.NilError(t, err)
	} // wrong message type

	// Then
	go func() {
		consumedErrorMessage := <-consumedLastStepErrorMessageChan
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

		consumerGroup.Unsubscribe()
		consumerGroup.WaitConsumerStop()

		close(consumedMessageChan)
		close(consumedLastStepErrorMessageChan)
	}()

	consumedMessages := make([]*partitionscaler.ConsumerMessage, 0)
	for consumedMessage := range consumedMessageChan {
		consumedMessages = append(consumedMessages, consumedMessage)
	}

	assert.Equal(t, 0, len(errorConsumers))
	assert.Equal(t, 1, len(consumedMessages))
}
