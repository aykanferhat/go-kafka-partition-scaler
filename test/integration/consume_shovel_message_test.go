package integration

import (
	"context"
	"testing"
	"time"

	partitionscaler "github.com/Trendyol/go-kafka-partition-scaler"

	"gotest.tools/v3/assert"
)

func Test_ConsumerShovel_ShouldCloseConsumerWhenThereIsNoNewMessage(t *testing.T) {
	// Given
	ctx := context.Background()

	closeConsumerWhenThereIsNoMessage := 3 * time.Second
	closeConsumerWhenMessageIsNew := 30 * time.Second

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
			Cron:                              everyTenSeconds,
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
	kafkaContainer, _, errorConsumerGroups := InitializeErrorConsumerTestCluster(
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

	defer func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			assert.NilError(t, err)
		}
	}()

	errorConsumerGroup := errorConsumerGroups[errorGroupID]

	errorConsumerGroup.WaitConsumerStart()
	assert.Equal(t, true, errorConsumerGroup.IsRunning())

	errorConsumerGroup.WaitConsumerStop()
	assert.Equal(t, false, errorConsumerGroup.IsRunning())
}
