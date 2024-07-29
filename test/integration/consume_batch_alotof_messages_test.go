package integration

import (
	"context"
	"crypto/rand"
	"math/big"
	"testing"

	"github.com/IBM/sarama"
	partitionscaler "github.com/Trendyol/go-kafka-partition-scaler"

	"github.com/Trendyol/go-kafka-partition-scaler/pkg/uuid"
	"github.com/Trendyol/go-kafka-partition-scaler/test/testdata"
	"gotest.tools/v3/assert"
)

func Test_BatchConsumer_ShouldConsumeALotOfMessages(t *testing.T) {
	// Given
	ctx, cancel := context.WithCancel(context.Background())

	virtualPartitionCount := 10
	batchSize := 20

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
		{
			BatchConsumer: newTestBatchMessageConsumer(consumedMessagesChan),
			ConfigName:    topicConfigName,
		},
	}

	consumerInterceptor := newTestConsumerHeaderInterceptor()
	consumerErrorInterceptor := newTestConsumerErrorInterceptor()
	producerInterceptor := newTestProducerInterceptor()

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

			consumerGroup.Unsubscribe()

			consumerGroup.WaitConsumerStop()
			close(consumedMessagesChan)
			break
		}
	}()

	consumedMessagesList := make([]*partitionscaler.ConsumerMessage, 0)
	for consumedMessages := range consumedMessagesChan {
		consumedMessagesList = append(consumedMessagesList, consumedMessages...)
	}
	assert.Equal(t, len(produceMessages), len(consumedMessagesList))
}
