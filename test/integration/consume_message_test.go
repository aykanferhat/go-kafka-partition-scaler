package integration

import (
	"context"
	"testing"
	"time"

	"github.com/IBM/sarama"

	partitionscaler "github.com/aykanferhat/go-kafka-partition-scaler"

	"github.com/aykanferhat/go-kafka-partition-scaler/common"
	"github.com/aykanferhat/go-kafka-partition-scaler/test/testdata"
	"gotest.tools/v3/assert"
)

func Test_Consumer_ShouldConsumeMessage(t *testing.T) {
	// Given
	ctx, cancel := context.WithCancel(context.Background())

	clusterConfigsMap := map[string]*partitionscaler.ClusterConfig{
		clusterName: {
			Brokers:  "", // dynamic
			Version:  sarama.V3_6_0_0.String(),
			ClientID: "client-id",
		},
	}

	consumerConfigs := map[string]*partitionscaler.ConsumerGroupConfig{
		topicConfigName: {
			GroupID:           groupID,
			Name:              topic,
			Retry:             retryTopic,
			Error:             errorTopic,
			RetryCount:        3,
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

	consumedMessageChan := make(chan *partitionscaler.ConsumerMessage, 10)

	consumersList := []*partitionscaler.ConsumerGroupConsumers{
		{Consumer: newTestMessageConsumer(consumedMessageChan), ConfigName: topicConfigName},
	}

	producerInterceptor := newTestProducerInterceptor()
	consumerErrorInterceptor := newTestConsumerErrorInterceptor()
	consumerInterceptor := newTestConsumerHeaderInterceptor()

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
		&testdata.TestProducerMessage{Id: 111111, Reason: "nameChanged"},
		&testdata.TestProducerMessage{Id: 222223, Reason: "nameChanged"},
		&testdata.TestProducerMessage{Id: 333335, Reason: "nameChanged"},

		&testdata.TestProducerMessage{Id: 111111, Reason: "descriptionChanged"},
		&testdata.TestProducerMessage{Id: 222223, Reason: "descriptionChanged"},
		&testdata.TestProducerMessage{Id: 333335, Reason: "descriptionChanged"},

		&testdata.TestProducerMessage{Id: 111111, Reason: "statusChanged"},
		&testdata.TestProducerMessage{Id: 222223, Reason: "statusChanged"},
		&testdata.TestProducerMessage{Id: 333335, Reason: "statusChanged"},
	}

	ctx = context.WithValue(ctx, common.ContextKey("key"), "value")
	if err := producers.ProduceSyncBulk(ctx, produceMessages, 100); err != nil {
		assert.NilError(t, err)
	}

	// Then
	consumedMessages := make([]*partitionscaler.ConsumerMessage, 0)

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
