package integration

import (
	"context"
	"testing"

	"github.com/IBM/sarama"
	partitionscaler "github.com/aykanferhat/go-kafka-partition-scaler"

	"github.com/aykanferhat/go-kafka-partition-scaler/test/testdata"
	"gotest.tools/v3/assert"
)

func Test_BatchConsumer_ShouldConsumeMessages(t *testing.T) {
	// Given
	ctx, cancel := context.WithCancel(context.Background())

	batchSize := 3

	clusterConfigsMap := map[string]*partitionscaler.ClusterConfig{
		clusterName: {
			Brokers:  "",
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
