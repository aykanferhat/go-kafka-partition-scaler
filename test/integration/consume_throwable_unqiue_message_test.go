package integration

import (
	"context"
	"testing"
	"time"

	"github.com/IBM/sarama"

	partitionscaler "github.com/Trendyol/go-kafka-partition-scaler"
	"github.com/Trendyol/go-kafka-partition-scaler/pkg/json"
	"github.com/Trendyol/go-kafka-partition-scaler/test/testdata"
	"gotest.tools/v3/assert"
)

func Test_UniqueConsumer_ShouldConsumeThrowableMessage(t *testing.T) {
	// Given
	ctx, cancel := context.WithCancel(context.Background())

	maxRetryCount := 3
	virtualPartitionCount := 3
	batchSize := 10
	closeConsumerWhenMessageIsNew := time.Millisecond

	clusterConfigsMap := map[string]*partitionscaler.ClusterConfig{
		clusterName: {
			Brokers: "", // dynamic
			Version: sarama.V3_6_0_0.String(),
			ErrorConfig: &partitionscaler.ErrorConfig{
				GroupID:                           errorGroupID,
				Cron:                              everyFifteenSeconds,
				MaxErrorCount:                     3,
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
			Error:                 errorTopic,
			RetryCount:            maxRetryCount,
			BatchSize:             batchSize,
			UniqueListener:        true,
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

	consumedMessageChan := make(chan *partitionscaler.ConsumerMessage, 10)
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
	errorConsumer := errorConsumers[errorGroupID]

	consumerGroup.WaitConsumerStart()
	errorConsumer.WaitConsumerStart()

	produceMessages := []partitionscaler.Message{
		&testdata.TestProducerMessage{Id: 111111, Reason: "nameChanged", Version: 0},
		&testdata.TestProducerMessage{Id: 111111, Reason: "descriptionChanged", Version: 1},
		&testdata.TestProducerMessage{Id: 111111, Reason: "statusChanged", Version: 2},
		&testdata.TestProducerMessage{Id: 111111, Reason: "mediaChanged", Version: 3},
		&testdata.TestWrongTypeProducerMessage{Id: "111111", Reason: "categoryChanged", Version: 4},
	}

	if err := producers.ProduceSyncBulk(ctx, produceMessages, 100); err != nil {
		assert.NilError(t, err)
	}

	// Then
	go func(consumedMessageCh chan *partitionscaler.ConsumerMessage, consumedErrorMessageCh chan *partitionscaler.ConsumerMessage) {
		consumedErrorMessage := <-consumedErrorMessageCh
		var errMessage testdata.TestWrongTypeProducerMessage
		if err := json.Unmarshal(consumedErrorMessage.Value, &errMessage); err != nil {
			assert.NilError(t, err)
		}

		assert.Equal(t, "111111", errMessage.Id)
		assert.Equal(t, "categoryChanged", errMessage.Reason)
		assert.Equal(t, 4, errMessage.Version)
		assert.Equal(t, errorTopic, consumedErrorMessage.Topic)
		assert.Equal(t, int32(0), consumedErrorMessage.Partition)
		assert.Equal(t, 0, consumedErrorMessage.VirtualPartition)
		assert.Equal(t, int64(0), consumedErrorMessage.Offset)

		consumerGroup.Unsubscribe()
		errorConsumer.Unsubscribe()

		consumerGroup.WaitConsumerStop()
		errorConsumer.WaitConsumerStop()

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

func Test_UniqueConsumer_ShouldConsumeThrowableMessageWhenErrorTopicNotFound(t *testing.T) {
	// Given
	ctx, cancel := context.WithCancel(context.Background())

	maxRetryCount := 3
	virtualPartitionCount := 3
	batchSize := 10
	consumeBatchListenerLatency := 1 * time.Second

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
			Retry:                       retryTopic,
			RetryCount:                  maxRetryCount,
			BatchSize:                   batchSize,
			UniqueListener:              true,
			VirtualPartitionCount:       virtualPartitionCount,
			ConsumeBatchListenerLatency: consumeBatchListenerLatency,
			MaxProcessingTime:           1 * time.Second,
			Cluster:                     clusterName,
		},
	}

	producerTopicsMap := map[string]*partitionscaler.ProducerTopic{
		topicConfigName: {
			Name:    topic,
			Cluster: clusterName,
		},
	}

	consumedMessageChan := make(chan *partitionscaler.ConsumerMessage, 10)
	consumersList := []*partitionscaler.ConsumerGroupConsumers{
		{
			ConfigName: topicConfigName,
			Consumer:   newTestMessageConsumer(consumedMessageChan),
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
		&testdata.TestProducerMessage{Id: 111111, Reason: "nameChanged", Version: 0},
		&testdata.TestProducerMessage{Id: 111111, Reason: "descriptionChanged", Version: 1},
		&testdata.TestProducerMessage{Id: 111111, Reason: "statusChanged", Version: 2},
		&testdata.TestProducerMessage{Id: 111111, Reason: "mediaChanged", Version: 3},
		&testdata.TestWrongTypeProducerMessage{Id: "111111", Reason: "categoryChanged", Version: 4},
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
		assert.Equal(t, "categoryChanged", errMessage.Reason)
		assert.Equal(t, 4, errMessage.Version)
		assert.Equal(t, retryTopic, consumedErrorMessage.Topic)
		assert.Equal(t, int32(0), consumedErrorMessage.Partition)
		assert.Equal(t, 0, consumedErrorMessage.VirtualPartition)
		assert.Equal(t, int64(2), consumedErrorMessage.Offset)

		consumerGroup.Unsubscribe()
		consumerGroup.WaitConsumerStop()

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

	assert.Equal(t, 1, len(consumedMessages))
	assert.Equal(t, maxRetryCount, len(consumedRetiedMessages))
}

func Test_UniqueConsumer_ShouldConsumeThrowableMessageWhenRetryTopicNotFound(t *testing.T) {
	// Given
	ctx, cancel := context.WithCancel(context.Background())

	virtualPartitionCount := 3
	batchSize := 10
	consumeBatchListenerLatency := 1 * time.Second
	closeConsumerWhenMessageIsNew := time.Millisecond

	clusterConfigsMap := map[string]*partitionscaler.ClusterConfig{
		clusterName: {
			Brokers: "", // dynamic
			Version: sarama.V3_6_0_0.String(),
			ErrorConfig: &partitionscaler.ErrorConfig{
				GroupID:                           errorGroupID,
				Cron:                              everyThirtySeconds,
				MaxErrorCount:                     3,
				MaxProcessingTime:                 1 * time.Second,
				CloseConsumerWhenThereIsNoMessage: 2 * time.Minute,
				CloseConsumerWhenMessageIsNew:     closeConsumerWhenMessageIsNew,
			},
			ClientID: "client-id",
		},
	}

	consumerConfigs := map[string]*partitionscaler.ConsumerGroupConfig{
		topicConfigName: {
			GroupID:                     groupID,
			Name:                        topic,
			Error:                       errorTopic,
			BatchSize:                   batchSize,
			UniqueListener:              true,
			VirtualPartitionCount:       virtualPartitionCount,
			ConsumeBatchListenerLatency: consumeBatchListenerLatency,
			MaxProcessingTime:           1 * time.Second,
			Cluster:                     clusterName,
		},
	}

	producerTopicsMap := map[string]*partitionscaler.ProducerTopic{
		topicConfigName: {
			Name:    topic,
			Cluster: clusterName,
		},
	}

	consumedMessageChan := make(chan *partitionscaler.ConsumerMessage, 10)
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

	produceMessages := []partitionscaler.Message{
		&testdata.TestProducerMessage{Id: 111111, Reason: "nameChanged", Version: 0},
		&testdata.TestProducerMessage{Id: 111111, Reason: "descriptionChanged", Version: 1},
		&testdata.TestProducerMessage{Id: 111111, Reason: "statusChanged", Version: 2},
		&testdata.TestProducerMessage{Id: 111111, Reason: "mediaChanged", Version: 3},
		&testdata.TestWrongTypeProducerMessage{Id: "111111", Reason: "categoryChanged", Version: 4},
	}

	if err := producers.ProduceSyncBulk(ctx, produceMessages, 100); err != nil {
		assert.NilError(t, err)
	}

	// Then
	go func(consumedMessageCh chan *partitionscaler.ConsumerMessage, consumedErrorMessageCh chan *partitionscaler.ConsumerMessage) {
		consumedErrorMessage := <-consumedErrorMessageCh
		var errMessage testdata.TestWrongTypeProducerMessage
		if err := json.Unmarshal(consumedErrorMessage.Value, &errMessage); err != nil {
			assert.NilError(t, err)
		}

		assert.Equal(t, "111111", errMessage.Id)
		assert.Equal(t, "categoryChanged", errMessage.Reason)
		assert.Equal(t, 4, errMessage.Version)
		assert.Equal(t, errorTopic, consumedErrorMessage.Topic)
		assert.Equal(t, int32(0), consumedErrorMessage.Partition)
		assert.Equal(t, 0, consumedErrorMessage.VirtualPartition)
		assert.Equal(t, int64(0), consumedErrorMessage.Offset)

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

func Test_UniqueConsumer_ShouldConsumeThrowableMessageWhenRetryAndErrorTopicNotFound(t *testing.T) {
	// Given
	ctx, cancel := context.WithCancel(context.Background())

	virtualPartitionCount := 3
	batchSize := 10
	consumeBatchListenerLatency := 1 * time.Second

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
			UniqueListener:              true,
			VirtualPartitionCount:       virtualPartitionCount,
			ConsumeBatchListenerLatency: consumeBatchListenerLatency,
			MaxProcessingTime:           1 * time.Second,
			Cluster:                     clusterName,
		},
	}

	producerTopicsMap := map[string]*partitionscaler.ProducerTopic{
		topicConfigName: {
			Name:    topic,
			Cluster: clusterName,
		},
	}

	consumedMessageChan := make(chan *partitionscaler.ConsumerMessage, 10)
	consumersList := []*partitionscaler.ConsumerGroupConsumers{
		{
			ConfigName: topicConfigName,
			Consumer:   newTestMessageConsumer(consumedMessageChan),
		},
	}

	consumerInterceptor := newTestConsumerHeaderInterceptor()
	consumerErrorInterceptor := newTestConsumerErrorInterceptor()
	producerInterceptor := newTestProducerInterceptor()

	consumedErrorMessageChan := make(chan *partitionscaler.ConsumerMessage, 1)

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
		&testdata.TestProducerMessage{Id: 111111, Reason: "nameChanged", Version: 0},
		&testdata.TestProducerMessage{Id: 111111, Reason: "descriptionChanged", Version: 1},
		&testdata.TestProducerMessage{Id: 111111, Reason: "statusChanged", Version: 2},
		&testdata.TestProducerMessage{Id: 111111, Reason: "mediaChanged", Version: 3},
		&testdata.TestWrongTypeProducerMessage{Id: "111111", Reason: "categoryChanged", Version: 4},
	}

	if err := producers.ProduceSyncBulk(ctx, produceMessages, 100); err != nil {
		assert.NilError(t, err)
	}

	// Then
	go func(consumedMessageCh chan *partitionscaler.ConsumerMessage, consumedErrorMessageCh chan *partitionscaler.ConsumerMessage) {
		consumedErrorMessage := <-consumedErrorMessageCh
		var errMessage testdata.TestWrongTypeProducerMessage
		if err := json.Unmarshal(consumedErrorMessage.Value, &errMessage); err != nil {
			assert.NilError(t, err)
		}

		assert.Equal(t, "111111", errMessage.Id)
		assert.Equal(t, "categoryChanged", errMessage.Reason)
		assert.Equal(t, 4, errMessage.Version)
		assert.Equal(t, topic, consumedErrorMessage.Topic)
		assert.Equal(t, int32(0), consumedErrorMessage.Partition)
		assert.Equal(t, 0, consumedErrorMessage.VirtualPartition)
		assert.Equal(t, int64(4), consumedErrorMessage.Offset)

		consumerGroup.Unsubscribe()
		consumerGroup.WaitConsumerStop()

		close(consumedMessageCh)
		close(consumedErrorMessageCh)
	}(consumedMessageChan, consumedErrorMessageChan)

	consumedMessages := make([]*partitionscaler.ConsumerMessage, 0)
	for consumedMessage := range consumedMessageChan {
		consumedMessages = append(consumedMessages, consumedMessage)
	}

	assert.Equal(t, 0, len(errorConsumers))
	assert.Equal(t, 1, len(consumedMessages))
}
