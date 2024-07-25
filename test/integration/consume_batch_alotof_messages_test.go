package integration

import (
	"context"
	"crypto/rand"
	partitionscaler "github.com/Trendyol/go-kafka-partition-scaler"
	"math/big"
	"testing"
	"time"

	"github.com/Trendyol/go-kafka-partition-scaler/pkg/uuid"
	"github.com/Trendyol/go-kafka-partition-scaler/test/testdata"
	"gotest.tools/v3/assert"
)

func Test_BatchConsumer_ShouldConsumeALotOfMessages(t *testing.T) {
	// Given
	ctx := context.Background()

	partition := int32(0)
	virtualPartitionCount := 10
	batchSize := 20

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
			GroupID:               groupID,
			Name:                  topic,
			Retry:                 retryTopic,
			Error:                 errorTopic,
			RetryCount:            3,
			BatchSize:             batchSize,
			VirtualPartitionCount: virtualPartitionCount,
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
		{BatchConsumer: NewTestBatchMessageConsumer(consumedMessagesChan), ConfigName: topicConfigName},
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

	var produceMessages []partitionscaler.Message
	for i := 0; i < 5000; i++ {
		nBig, err := rand.Int(rand.Reader, big.NewInt(1000))
		if err != nil {
			assert.NilError(t, err)
		}
		id := nBig.Int64()
		reason := uuid.GenerateUUID()
		produceMessages = append(produceMessages, &testdata.TestProducerMessage{Id: id, Reason: reason})
	}

	if err := producers.ProduceSyncBulk(ctx, produceMessages, 1000); err != nil {
		assert.NilError(t, err)
	}

	// Then
	go func() {
		for {
			if consumerGroup.GetLastCommittedOffset(topic, partition) != int64(len(produceMessages)-1) {
				continue
			}
			close(consumedMessagesChan)
			break
		}
	}()

	consumedMessagesList := make([]*partitionscaler.ConsumerMessage, 0)
	for consumedMessages := range consumedMessagesChan {
		consumedMessagesList = append(consumedMessagesList, consumedMessages...)
	}
	for _, consumerGroup := range consumers {
		consumerGroup.Unsubscribe()
	}
	consumerGroup.WaitConsumerStop()
	assert.Equal(t, len(produceMessages), len(consumedMessagesList))
}
