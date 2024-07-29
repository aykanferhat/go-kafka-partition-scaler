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

func Test_SingleConsumer_ShouldConsumeALotOfMessages(t *testing.T) {
	// Given
	ctx, cancel := context.WithCancel(context.Background())

	virtualPartitionCount := 10

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

	consumedMessageChan := make(chan *partitionscaler.ConsumerMessage, 10)

	consumersList := []*partitionscaler.ConsumerGroupConsumers{
		{Consumer: newTestMessageConsumer(consumedMessageChan), ConfigName: topicConfigName},
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
		produceMessages = append(produceMessages, &testdata.TestProducerMessage{
			Id: id, Reason: reason,
		})
	}

	if err := producers.ProduceSyncBulk(ctx, produceMessages, 1000); err != nil {
		assert.NilError(t, err)
	}

	// Then
	consumedMessages := make([]*partitionscaler.ConsumerMessage, 0, len(produceMessages))

	go func() {
		for {
			if len(consumedMessages) != len(produceMessages) {
				continue
			}

			consumerGroup.Unsubscribe()
			consumerGroup.WaitConsumerStop()
			close(consumedMessageChan)
			break
		}
	}()

	for consumedMessage := range consumedMessageChan {
		consumedMessages = append(consumedMessages, consumedMessage)
	}

	assert.Equal(t, len(consumedMessages), len(produceMessages))
}
