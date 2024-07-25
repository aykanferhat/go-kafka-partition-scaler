package integration

import (
	"context"
	"testing"
	"time"

	partitionscaler "github.com/Trendyol/go-kafka-partition-scaler"
	"github.com/Trendyol/go-kafka-partition-scaler/test/testdata"
	"gotest.tools/v3/assert"
)

func Test_UniqueConsumer_ShouldConsumeMessage(t *testing.T) {
	// Given
	ctx := context.Background()

	var partition int32
	virtualPartitionCount := 3
	batchSize := 10
	consumeBatchListenerLatency := 1 * time.Second

	clusterConfigsMap := map[string]*partitionscaler.ClusterConfig{
		clusterName: {
			Brokers: "", // dynamic
			Version: "2.2.0",
			ErrorConfig: &partitionscaler.ErrorConfig{
				GroupID:                           errorGroupID,
				Cron:                              "0 */5 * * *",
				MaxErrorCount:                     3,
				MaxProcessingTime:                 1 * time.Second,
				CloseConsumerWhenThereIsNoMessage: 1 * time.Minute,
				CloseConsumerWhenMessageIsNew:     1 * time.Minute,
			},
			ClientID: "client-id",
		},
	}

	consumerConfigs := map[string]*partitionscaler.ConsumerGroupConfig{
		topicConfigName: {
			GroupID:                     groupID,
			Name:                        topic,
			Retry:                       "message.topic.RETRY.0",
			Error:                       "message.topic.ERROR.0",
			RetryCount:                  3,
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
		{Consumer: NewTestMessageConsumer(consumedMessageChan), ConfigName: topicConfigName},
	}

	consumerInterceptor := NewTestConsumerHeaderInterceptor()
	consumerErrorInterceptor := NewTestConsumerErrorInterceptor()
	producerInterceptor := NewTestProducerInterceptor()

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

	produceMessages := []partitionscaler.Message{
		&testdata.TestProducerMessage{Id: 111111, Reason: "nameChanged", Version: 0},
		&testdata.TestProducerMessage{Id: 222223, Reason: "nameChanged", Version: 0},
		&testdata.TestProducerMessage{Id: 333335, Reason: "nameChanged", Version: 0},

		&testdata.TestProducerMessage{Id: 111111, Reason: "descriptionChanged", Version: 1},
		&testdata.TestProducerMessage{Id: 222223, Reason: "descriptionChanged", Version: 1},
		&testdata.TestProducerMessage{Id: 333335, Reason: "descriptionChanged", Version: 1},

		&testdata.TestProducerMessage{Id: 111111, Reason: "statusChanged", Version: 2},
		&testdata.TestProducerMessage{Id: 222223, Reason: "statusChanged", Version: 2},
		&testdata.TestProducerMessage{Id: 333335, Reason: "statusChanged", Version: 2},

		&testdata.TestProducerMessage{Id: 111111, Reason: "mediaChanged", Version: 3},
		&testdata.TestProducerMessage{Id: 222223, Reason: "mediaChanged", Version: 3},
		&testdata.TestProducerMessage{Id: 333335, Reason: "mediaChanged", Version: 3},

		&testdata.TestProducerMessage{Id: 111111, Reason: "categoryChanged", Version: 4},
		&testdata.TestProducerMessage{Id: 222223, Reason: "categoryChanged", Version: 4},
		&testdata.TestProducerMessage{Id: 333335, Reason: "categoryChanged", Version: 4},
	}

	if err := producers.ProduceSyncBulk(ctx, produceMessages, 100); err != nil {
		assert.NilError(t, err)
	}

	// Then
	consumedMessages := make([]*partitionscaler.ConsumerMessage, 0)

	// close channel when consumed all message
	go func() {
		for {
			if consumerGroup.GetLastCommittedOffset(topic, partition) != int64(len(produceMessages)-1) {
				time.Sleep(1 * time.Second)
				continue
			}
			close(consumedMessageChan)
			break
		}
	}()

	virtualPartitionConsumerMessageMap := map[int][]*partitionscaler.ConsumerMessage{}
	for consumedMessage := range consumedMessageChan {
		consumedMessages = append(consumedMessages, consumedMessage)
		if _, exists := virtualPartitionConsumerMessageMap[consumedMessage.VirtualPartition]; !exists {
			virtualPartitionConsumerMessageMap[consumedMessage.VirtualPartition] = []*partitionscaler.ConsumerMessage{}
		}
		virtualPartitionConsumerMessageMap[consumedMessage.VirtualPartition] = append(virtualPartitionConsumerMessageMap[consumedMessage.VirtualPartition], consumedMessage)
	}

	for _, consumerGroup := range consumers {
		consumerGroup.Unsubscribe()
	}
	consumerGroup.WaitConsumerStop()

	assert.Equal(t, len(consumedMessages), 3)
	assert.Equal(t, len(virtualPartitionConsumerMessageMap[0]), 1)
	assert.Equal(t, len(virtualPartitionConsumerMessageMap[1]), 1)
	assert.Equal(t, len(virtualPartitionConsumerMessageMap[2]), 1)
}