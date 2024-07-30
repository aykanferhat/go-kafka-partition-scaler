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

func Test_Producer_ShouldProduceCustomMessage(t *testing.T) {
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

	consumedMessageChan := make(chan *partitionscaler.ConsumerMessage)

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
		map[string]*partitionscaler.ProducerTopic{},
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

	err := producers.ProduceCustomSync(ctx, &partitionscaler.CustomMessage{
		Key: "key",
		Body: testdata.TestProducerMessage{
			Id:   100,
			Name: "Test Message",
		},
		Topic: &partitionscaler.ProducerTopic{
			Name:    topic,
			Cluster: clusterName,
		},
	})
	if err != nil {
		assert.NilError(t, err)
	}

	// Then
	consumedMessage := <-consumedMessageChan

	consumerGroup.Unsubscribe()
	consumerGroup.WaitConsumerStop()
	close(consumedMessageChan)

	var message testdata.TestConsumedMessage
	if err := json.Unmarshal(consumedMessage.Value, &message); err != nil {
		assert.NilError(t, err)
	}
	assert.Equal(t, message.Id, int32(100))
	assert.Equal(t, message.Name, "Test Message")
	assert.Equal(t, consumedMessage.Topic, topic)
	assert.Equal(t, consumedMessage.Partition, int32(0))
	assert.Equal(t, consumedMessage.VirtualPartition, 0)
	assert.Equal(t, consumedMessage.Offset, int64(0))
}
