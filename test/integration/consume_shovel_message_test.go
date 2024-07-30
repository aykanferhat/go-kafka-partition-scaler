package integration

import (
	"context"
	"testing"
	"time"

	"github.com/IBM/sarama"

	partitionscaler "github.com/aykanferhat/go-kafka-partition-scaler"

	"gotest.tools/v3/assert"
)

func Test_ConsumerShovel_ShouldCloseConsumerWhenThereIsNoNewMessage(t *testing.T) {
	// Given
	ctx, cancel := context.WithCancel(context.Background())

	closeConsumerWhenThereIsNoMessage := 3 * time.Second
	closeConsumerWhenMessageIsNew := 5 * time.Minute

	clusterConfigsMap := map[string]*partitionscaler.ClusterConfig{
		clusterName: {
			Brokers:  "", // dynamic
			Version:  sarama.V3_6_0_0.String(),
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
			ErrorConsumer: newTestMessageConsumer(consumedMessageChan),
		},
	}

	consumerErrorInterceptor := newTestConsumerErrorInterceptor()
	producerInterceptor := newTestProducerInterceptor()

	// When
	kafkaContainer, _, errorConsumerGroups := initializeErrorConsumerTestCluster(
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
		cancel()
	}()

	errorConsumerGroup := errorConsumerGroups[errorGroupID]

	errorConsumerGroup.WaitConsumerStart()
	assert.Equal(t, true, errorConsumerGroup.IsRunning())

	errorConsumerGroup.WaitConsumerStop()
	assert.Equal(t, false, errorConsumerGroup.IsRunning())
}
