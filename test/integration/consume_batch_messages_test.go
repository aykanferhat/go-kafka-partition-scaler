package integration

import (
	"context"
	"testing"
	"time"

	partitionscaler "github.com/Trendyol/go-kafka-partition-scaler"

	"github.com/Trendyol/go-kafka-partition-scaler/test/testdata"
	"gotest.tools/v3/assert"
)

func Test_BatchConsumer_ShouldConsumeMessages(t *testing.T) {
	// Given
	ctx := context.Background()

	batchSize := 3

	clusterConfigsMap := map[string]*partitionscaler.ClusterConfig{
		clusterName: {
			Brokers: "",
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
			GroupID:               groupID,
			Name:                  topic,
			Retry:                 retryTopic,
			Error:                 errorTopic,
			BatchSize:             batchSize,
			RetryCount:            3,
			VirtualPartitionCount: 3,
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
			BatchConsumer: NewTestBatchMessageConsumer(consumedMessagesChan),
			ConfigName:    topicConfigName,
		},
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

	defer func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			assert.NilError(t, err)
		}
	}()

	consumerGroup := consumers[groupID]
	_ = consumerGroup.Subscribe()
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
	}

	if err := producers.ProduceSyncBulk(ctx, produceMessages, 100); err != nil {
		assert.NilError(t, err)
	}

	// Then
	consumedMessagesList := make([][]*partitionscaler.ConsumerMessage, 0)
	go func() {
		for {
			lastCommittedOffset := consumerGroup.GetLastCommittedOffset(topic, partition)
			if lastCommittedOffset != int64(len(produceMessages)-1) {
				continue
			}

			consumerGroup.Unsubscribe()
			consumerGroup.WaitConsumerStop()
			close(consumedMessagesChan)
			break
		}
	}()

	for consumedMessages := range consumedMessagesChan {
		consumedMessagesList = append(consumedMessagesList, consumedMessages)
	}

	assert.Equal(t, len(consumedMessagesList), len(produceMessages)/batchSize)
	assert.Equal(t, len(consumedMessagesList[0]), batchSize)
	assert.Equal(t, len(consumedMessagesList[1]), batchSize)
	assert.Equal(t, len(consumedMessagesList[2]), batchSize)
}
