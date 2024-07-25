package integration

import (
	"context"
	"testing"
	"time"

	partitionscaler "github.com/Trendyol/go-kafka-partition-scaler"
	"github.com/Trendyol/go-kafka-partition-scaler/pkg/json"
	"github.com/Trendyol/go-kafka-partition-scaler/test/testdata"
	"gotest.tools/v3/assert"
)

func Test_Producer_ShouldProduceCustomMessage(t *testing.T) {
	// Given
	ctx := context.Background()

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
			GroupID:           groupID,
			Name:              topic,
			Retry:             "message.topic.RETRY.0",
			Error:             "message.topic.ERROR.0",
			RetryCount:        3,
			MaxProcessingTime: 1 * time.Second,
			Cluster:           clusterName,
		},
	}

	consumedMessageChan := make(chan *partitionscaler.ConsumerMessage)

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
		map[string]*partitionscaler.ProducerTopic{},
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

	for _, consumerGroup := range consumers {
		consumerGroup.Unsubscribe()
	}

	consumerGroup.WaitConsumerStop()

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
