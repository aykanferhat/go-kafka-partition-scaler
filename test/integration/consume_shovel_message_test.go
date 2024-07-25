package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	partitionscaler "github.com/Trendyol/go-kafka-partition-scaler"

	"github.com/Trendyol/go-kafka-partition-scaler/pkg/json"
	"github.com/Trendyol/go-kafka-partition-scaler/test/testdata"
	"gotest.tools/v3/assert"
)

func Test_ConsumerShovel_ShouldCloseConsumerWhenThereIsNoNewMessage(t *testing.T) {
	// Given
	ctx := context.Background()

	closeConsumerWhenThereIsNoMessage := 10 * time.Second
	closeConsumerWhenMessageIsNew := 5 * time.Second

	clusterConfigsMap := map[string]*partitionscaler.ClusterConfig{
		clusterName: {
			Brokers:  "", // dynamic
			Version:  "2.2.0",
			ClientID: "client-id",
		},
	}

	producerTopicsMap := map[string]*partitionscaler.ProducerTopic{
		topicConfigName: {
			Name:    errorTopic,
			Cluster: clusterName,
		},
	}

	consumerConfigs := map[string]*partitionscaler.ConsumerGroupErrorConfig{
		topicConfigName: {
			GroupID: errorGroupID,
			Topics: []string{
				errorTopic,
			},
			MaxProcessingTime:                 1 * time.Second,
			Cluster:                           clusterName,
			Cron:                              everyTwentySeconds,
			MaxErrorCount:                     3,
			CloseConsumerWhenThereIsNoMessage: closeConsumerWhenThereIsNoMessage,
			CloseConsumerWhenMessageIsNew:     closeConsumerWhenMessageIsNew,
			OffsetInitial:                     "oldest",
		},
	}

	consumedMessageChan := make(chan *partitionscaler.ConsumerMessage, 1)
	consumersList := []*partitionscaler.ConsumerGroupErrorConsumers{
		{
			ConfigName:    topicConfigName,
			ErrorConsumer: NewTestMessageConsumer(consumedMessageChan),
		},
	}

	consumerErrorInterceptor := NewTestConsumerErrorInterceptor()
	producerInterceptor := NewTestProducerInterceptor()

	// When
	kafkaContainer, producers, errorConsumerGroups := InitializeErrorConsumerTestCluster(
		ctx,
		t,
		clusterConfigsMap,
		producerTopicsMap,
		consumerConfigs,
		consumersList,
		consumerErrorInterceptor,
		producerInterceptor,
		func(ctx context.Context, message *partitionscaler.ConsumerMessage, err error) {
			// ignore
		},
	)
	errorConsumerGroup := errorConsumerGroups[errorGroupID]

	defer func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			assert.NilError(t, err)
		}
	}()

	errorConsumerGroup.WaitConsumerStart()

	// When

	if err := producers.ProduceSync(ctx, &testdata.TestProducerMessage{Id: 100, Name: "Test Message"}); err != nil {
		assert.NilError(t, err)
	}

	errorConsumerGroup.WaitConsumerStop()

	errorConsumerGroup.WaitConsumerStart()

	wg := sync.WaitGroup{}
	wg.Add(1)

	// Then
	go func(w *sync.WaitGroup) {
		defer w.Done()
		consumedErrorMessage := <-consumedMessageChan
		var message testdata.TestProducerMessage
		if err := json.Unmarshal(consumedErrorMessage.Value, &message); err != nil {
			assert.NilError(t, err)
		}

		assert.Equal(t, message.Id, int64(100))
		assert.Equal(t, message.Name, "Test Message")
		assert.Equal(t, consumedErrorMessage.Topic, errorTopic)
		assert.Equal(t, consumedErrorMessage.Partition, int32(0))
		assert.Equal(t, consumedErrorMessage.VirtualPartition, 0)
		assert.Equal(t, consumedErrorMessage.Offset, int64(0))
		close(consumedMessageChan)
	}(&wg)
	wg.Wait()
}
