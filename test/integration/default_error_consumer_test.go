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

func Test_DefaultErrorConsumer_ShouldConsumeThrowableMessage(t *testing.T) {
	// Given
	ctx, cancel := context.WithCancel(context.Background())

	maxRetryCount := 1
	virtualPartitionCount := 1
	maxErrorCount := 3
	closeConsumerWhenMessageIsNew := time.Millisecond

	clusterConfigsMap := map[string]*partitionscaler.ClusterConfig{
		clusterName: {
			Brokers: "", // dynamic
			Version: sarama.V3_6_0_0.String(),
			ErrorConfig: &partitionscaler.ErrorConfig{
				GroupID:                           errorGroupID,
				Cron:                              everyFifteenSeconds,
				MaxErrorCount:                     maxErrorCount,
				MaxProcessingTime:                 1 * time.Minute,
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
	consumersList := []*partitionscaler.ConsumerGroupConsumers{
		{
			ConfigName: topicConfigName,
			Consumer:   newTestMessageConsumer(consumedMessageChan),
		},
	}

	consumerInterceptor := newTestConsumerHeaderInterceptor()
	consumerErrorInterceptor := newTestConsumerErrorInterceptor()
	producerInterceptor := newTestProducerInterceptor()

	consumedErrorMessageChan := make(chan *partitionscaler.ConsumerMessage, 10)

	// When
	kafkaContainer, producers, consumers, errorConsumers := initializeTestCluster(
		ctx,
		t,
		clusterConfigsMap,
		producerTopicsMap,
		consumerConfigs, consumersList,
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
		assert.Equal(t, consumedErrorMessage.Offset, int64(3))

		consumerGroup.Unsubscribe()
		errorConsumerGroup.Unsubscribe()

		consumerGroup.WaitConsumerStop()
		errorConsumerGroup.WaitConsumerStop()

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

	assert.Equal(t, false, errorConsumerGroup.IsRunning())
	assert.Equal(t, 1, len(consumedMessages))
	assert.Equal(t, maxRetryCount+maxErrorCount, len(consumedRetiedMessages))
}
