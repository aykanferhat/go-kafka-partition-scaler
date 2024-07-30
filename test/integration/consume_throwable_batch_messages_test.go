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

func Test_BatchConsumer_ShouldConsumeThrowableMessages(t *testing.T) {
	// Given
	ctx, cancel := context.WithCancel(context.Background())

	maxRetryCount := 3
	batchSize := 3
	closeConsumerWhenMessageIsNew := time.Millisecond

	clusterConfigsMap := map[string]*partitionscaler.ClusterConfig{
		clusterName: {
			Brokers: "", // dynamic
			Version: sarama.V3_6_0_0.String(),
			ErrorConfig: &partitionscaler.ErrorConfig{
				GroupID:                           errorGroupID,
				Cron:                              everyFifteenSeconds,
				MaxErrorCount:                     3,
				MaxProcessingTime:                 10 * time.Second,
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
			Error:                 errorTopic,
			BatchSize:             batchSize,
			RetryCount:            maxRetryCount,
			VirtualPartitionCount: 2,
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

	consumedMessagesChan := make(chan []*partitionscaler.ConsumerMessage)
	consumedErrorMessageChan := make(chan *partitionscaler.ConsumerMessage, 1)
	consumersList := []*partitionscaler.ConsumerGroupConsumers{
		{
			ConfigName:    topicConfigName,
			BatchConsumer: newTestBatchMessageConsumer(consumedMessagesChan),
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

	produceMessages := []partitionscaler.Message{
		&testdata.TestWrongTypeProducerMessage{Id: "111111", Reason: "nameChanged", Version: 0}, // wrong format
		&testdata.TestProducerMessage{Id: 222223, Reason: "nameChanged", Version: 0},
		&testdata.TestProducerMessage{Id: 333335, Reason: "nameChanged", Version: 0},
	}

	if err := producers.ProduceSyncBulk(ctx, produceMessages, 100); err != nil {
		assert.NilError(t, err)
	}

	// Then
	go func(consumedMessageCh chan []*partitionscaler.ConsumerMessage, consumedErrorMessageCh chan *partitionscaler.ConsumerMessage) {
		consumedErrorMessage := <-consumedErrorMessageCh
		var errMessage testdata.TestWrongTypeProducerMessage
		if err := json.Unmarshal(consumedErrorMessage.Value, &errMessage); err != nil {
			assert.NilError(t, err)
		}

		assert.Equal(t, errMessage.Id, "111111")
		assert.Equal(t, errMessage.Reason, "nameChanged")
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
	}(consumedMessagesChan, consumedErrorMessageChan)

	consumedMessages := make([]*partitionscaler.ConsumerMessage, 0)
	consumedRetriedMessages := make([]*partitionscaler.ConsumerMessage, 0)
	for cms := range consumedMessagesChan {
		for _, consumedMessage := range cms {
			if consumedMessage.Topic == retryTopic {
				consumedRetriedMessages = append(consumedRetriedMessages, consumedMessage)
				continue
			}
			consumedMessages = append(consumedMessages, consumedMessage)
		}
	}

	assert.Equal(t, false, errorConsumerGroup.IsRunning())
	assert.Equal(t, 3, len(consumedMessages))
	assert.Equal(t, 3, len(consumedRetriedMessages))
	assert.Equal(t, maxRetryCount, len(consumedMessages))
}

func Test_BatchConsumer_ShouldConsumeThrowableMessagesWhenErrorTopicNotFound(t *testing.T) {
	// Given
	ctx, cancel := context.WithCancel(context.Background())

	maxRetryCount := 3
	batchSize := 3

	clusterConfigsMap := map[string]*partitionscaler.ClusterConfig{
		clusterName: {
			Brokers:  "", // dynamic
			Version:  sarama.V3_6_0_0.String(),
			ClientID: "client-id",
		},
	}

	consumerConfigs := map[string]*partitionscaler.ConsumerGroupConfig{
		topicConfigName: {
			GroupID:               groupID,
			Name:                  topic,
			Retry:                 retryTopic,
			BatchSize:             batchSize,
			RetryCount:            maxRetryCount,
			VirtualPartitionCount: 1,
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

	consumedMessagesChan := make(chan []*partitionscaler.ConsumerMessage)
	consumersList := []*partitionscaler.ConsumerGroupConsumers{
		{
			ConfigName:    topicConfigName,
			BatchConsumer: newTestBatchMessageConsumer(consumedMessagesChan),
		},
	}

	consumerInterceptor := newTestConsumerHeaderInterceptor()
	consumerErrorInterceptor := newTestConsumerErrorInterceptor()
	producerInterceptor := newTestProducerInterceptor()

	consumedErrorMessageChan := make(chan *partitionscaler.ConsumerMessage, 1)

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
			consumedErrorMessageChan <- message
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

	produceMessages := []partitionscaler.Message{
		&testdata.TestWrongTypeProducerMessage{Id: "111111", Reason: "nameChanged", Version: 0}, // wrong format
		&testdata.TestProducerMessage{Id: 222223, Reason: "nameChanged", Version: 0},
		&testdata.TestProducerMessage{Id: 333335, Reason: "nameChanged", Version: 0},
	}

	if err := producers.ProduceSyncBulk(ctx, produceMessages, 100); err != nil {
		assert.NilError(t, err)
	}

	// Then
	go func() {
		consumedErrorMessage := <-consumedErrorMessageChan
		var errMessage testdata.TestWrongTypeProducerMessage
		if err := json.Unmarshal(consumedErrorMessage.Value, &errMessage); err != nil {
			assert.NilError(t, err)
		}

		assert.Equal(t, "111111", errMessage.Id)
		assert.Equal(t, "nameChanged", errMessage.Reason)
		assert.Equal(t, retryTopic, consumedErrorMessage.Topic)
		assert.Equal(t, int32(0), consumedErrorMessage.Partition)
		assert.Equal(t, 0, consumedErrorMessage.VirtualPartition)
		assert.Equal(t, int64(2), consumedErrorMessage.Offset)

		consumerGroup.Unsubscribe()
		consumerGroup.WaitConsumerStop()

		close(consumedMessagesChan)
		close(consumedErrorMessageChan)
	}()

	consumedMessages := make([]*partitionscaler.ConsumerMessage, 0)
	consumedRetriedMessages := make([]*partitionscaler.ConsumerMessage, 0)
	for cms := range consumedMessagesChan {
		for _, consumedMessage := range cms {
			if consumedMessage.Topic == retryTopic {
				consumedRetriedMessages = append(consumedRetriedMessages, consumedMessage)
				continue
			}
			consumedMessages = append(consumedMessages, consumedMessage)
		}
	}

	assert.Equal(t, maxRetryCount, len(consumedRetriedMessages))
	assert.Equal(t, len(produceMessages), len(consumedMessages))
}

func Test_BatchConsumer_ShouldConsumeThrowableMessagesWhenRetryTopicNotFound(t *testing.T) {
	// Given
	ctx, cancel := context.WithCancel(context.Background())

	batchSize := 3
	closeConsumerWhenMessageIsNew := time.Millisecond

	clusterConfigsMap := map[string]*partitionscaler.ClusterConfig{
		clusterName: {
			Brokers: "", // dynamic
			Version: sarama.V3_6_0_0.String(),
			ErrorConfig: &partitionscaler.ErrorConfig{
				GroupID:                           errorGroupID,
				Cron:                              everyFifteenSeconds,
				MaxErrorCount:                     3,
				MaxProcessingTime:                 10 * time.Second,
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
			Error:                 errorTopic,
			BatchSize:             batchSize,
			VirtualPartitionCount: 1,
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

	consumedMessagesChan := make(chan []*partitionscaler.ConsumerMessage)
	consumedErrorMessageChan := make(chan *partitionscaler.ConsumerMessage, 1)
	consumersList := []*partitionscaler.ConsumerGroupConsumers{
		{
			ConfigName:    topicConfigName,
			BatchConsumer: newTestBatchMessageConsumer(consumedMessagesChan),
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

	produceMessages := []partitionscaler.Message{
		&testdata.TestWrongTypeProducerMessage{Id: "111111", Reason: "nameChanged", Version: 0}, // wrong format
		&testdata.TestProducerMessage{Id: 222223, Reason: "nameChanged", Version: 0},
		&testdata.TestProducerMessage{Id: 333335, Reason: "nameChanged", Version: 0},
	}

	if err := producers.ProduceSyncBulk(ctx, produceMessages, 100); err != nil {
		assert.NilError(t, err)
	}

	// Then
	go func() {
		consumedErrorMessage := <-consumedErrorMessageChan
		var errMessage testdata.TestWrongTypeProducerMessage
		if err := json.Unmarshal(consumedErrorMessage.Value, &errMessage); err != nil {
			assert.NilError(t, err)
		}

		assert.Equal(t, errMessage.Id, "111111")
		assert.Equal(t, errMessage.Reason, "nameChanged")
		assert.Equal(t, consumedErrorMessage.Topic, errorTopic)
		assert.Equal(t, consumedErrorMessage.Partition, int32(0))
		assert.Equal(t, consumedErrorMessage.VirtualPartition, 0)
		assert.Equal(t, consumedErrorMessage.Offset, int64(0))

		consumerGroup.Unsubscribe()
		errorConsumerGroup.Unsubscribe()

		consumerGroup.WaitConsumerStop()
		errorConsumerGroup.WaitConsumerStop()

		close(consumedMessagesChan)
		close(consumedErrorMessageChan)
	}()

	consumedMessages := make([]*partitionscaler.ConsumerMessage, 0)
	for cms := range consumedMessagesChan {
		consumedMessages = append(consumedMessages, cms...)
	}

	assert.Equal(t, false, errorConsumerGroup.IsRunning())
	assert.Equal(t, 3, len(consumedMessages))
}

func Test_BatchConsumer_ShouldConsumeThrowableMessagesWhenRetryAndErrorTopicNotFound(t *testing.T) {
	// Given
	ctx, cancel := context.WithCancel(context.Background())

	batchSize := 3
	consumeBatchListenerLatency := time.Second

	clusterConfigsMap := map[string]*partitionscaler.ClusterConfig{
		clusterName: {
			Brokers:  "", // dynamic
			Version:  "2.2.0",
			ClientID: "client-id",
		},
	}

	consumerConfigs := map[string]*partitionscaler.ConsumerGroupConfig{
		topicConfigName: {
			GroupID:                     groupID,
			Name:                        topic,
			BatchSize:                   batchSize,
			VirtualPartitionCount:       1,
			MaxProcessingTime:           1 * time.Second,
			Cluster:                     clusterName,
			ConsumeBatchListenerLatency: consumeBatchListenerLatency,
		},
	}

	producerTopicsMap := map[string]*partitionscaler.ProducerTopic{
		topicConfigName: {
			Name:    topic,
			Cluster: clusterName,
		},
	}

	consumedMessagesChan := make(chan []*partitionscaler.ConsumerMessage)
	consumersList := []*partitionscaler.ConsumerGroupConsumers{
		{
			ConfigName:    topicConfigName,
			BatchConsumer: newTestBatchMessageConsumer(consumedMessagesChan),
		},
	}

	consumerInterceptor := newTestConsumerHeaderInterceptor()
	consumerErrorInterceptor := newTestConsumerErrorInterceptor()
	producerInterceptor := newTestProducerInterceptor()

	consumedLastStepErrorMessageChan := make(chan *partitionscaler.ConsumerMessage, 1)

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

	produceMessages := []partitionscaler.Message{
		&testdata.TestWrongTypeProducerMessage{Id: "111111", Reason: "nameChanged", Version: 0}, // wrong format
		&testdata.TestProducerMessage{Id: 222223, Reason: "nameChanged", Version: 0},
		&testdata.TestProducerMessage{Id: 333335, Reason: "nameChanged", Version: 0},
	}

	if err := producers.ProduceSyncBulk(ctx, produceMessages, 100); err != nil {
		assert.NilError(t, err)
	}

	// Then
	go func(consumedMessageCh chan []*partitionscaler.ConsumerMessage, consumedLastStepErrorMessageCh chan *partitionscaler.ConsumerMessage) {
		consumedErrorMessage := <-consumedLastStepErrorMessageCh
		var errMessage testdata.TestWrongTypeProducerMessage
		if err := json.Unmarshal(consumedErrorMessage.Value, &errMessage); err != nil {
			assert.NilError(t, err)
		}

		assert.Equal(t, errMessage.Id, "111111")
		assert.Equal(t, errMessage.Reason, "nameChanged")
		assert.Equal(t, consumedErrorMessage.Topic, topic)
		assert.Equal(t, consumedErrorMessage.Partition, int32(0))
		assert.Equal(t, consumedErrorMessage.VirtualPartition, 0)
		assert.Equal(t, consumedErrorMessage.Offset, int64(0))

		consumerGroup.Unsubscribe()
		consumerGroup.WaitConsumerStop()

		close(consumedMessageCh)
		close(consumedLastStepErrorMessageCh)
	}(consumedMessagesChan, consumedLastStepErrorMessageChan)

	consumedMessages := make([]*partitionscaler.ConsumerMessage, 0)
	for cms := range consumedMessagesChan {
		consumedMessages = append(consumedMessages, cms...)
	}

	assert.Equal(t, 3, len(consumedMessages))
}
