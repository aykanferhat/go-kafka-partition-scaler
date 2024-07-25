package internal

import (
	"context"
	"testing"
	"time"

	"github.com/Trendyol/go-kafka-partition-scaler/pkg/kafka"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func Test_ConsumerGroup_shouldNewConsumerGroupWithCoreHandler(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()
	// Given
	clusterConfig := &ClusterConfig{
		Brokers: "", // dynamic
		Version: "2.2.0",
		ErrorConfig: &ErrorConfig{
			GroupID:                       errorGroupID,
			Cron:                          "0 */5 * * *",
			MaxErrorCount:                 2,
			CloseConsumerWhenMessageIsNew: 10 * time.Minute,
		},
		ClientID: "client-id",
	}

	groupID := "message.topic.0.consumer"
	consumerGroupConfig := &ConsumerGroupConfig{
		GroupID:                     groupID,
		Name:                        "message.topic.0",
		Retry:                       "message.topic.RETRY.0",
		Error:                       "message.topic.ERROR.0",
		RetryCount:                  3,
		Cluster:                     "cluster",
		ConsumeBatchListenerLatency: 1 * time.Second,
		MaxProcessingTime:           1 * time.Second,
	}

	producer := kafka.NewMockProducer(controller)
	consumers := &ConsumerGroupConsumers{
		ConfigName: "configName",
		Consumer:   NewMockConsumer(controller),
	}

	consumerErrorInterceptor := NewMockConsumerErrorInterceptor(controller)

	initializeContext := ConsumerGroupInitializeContext{
		ClusterConfig:             clusterConfig,
		ConsumerGroupConfig:       consumerGroupConfig,
		Producer:                  producer,
		Consumers:                 consumers,
		LastStep:                  func(ctx context.Context, message *ConsumerMessage, err error) {},
		ConsumerInterceptors:      []ConsumerInterceptor{},
		ConsumerErrorInterceptors: []ConsumerErrorInterceptor{consumerErrorInterceptor},
		Tracers:                   []Tracer{},
	}

	// When
	consumerGroup := NewConsumerGroup(initializeContext)

	// Then
	assert.Equal(t, "message.topic.0.consumer", consumerGroup.GetGroupID())
}

func Test_ConsumerGroup_shouldNewConsumerGroupWithCustomHandler(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()
	// Given
	clusterConfig := &ClusterConfig{
		Brokers: "", // dynamic
		Version: "2.2.0",
		ErrorConfig: &ErrorConfig{
			GroupID:                       errorGroupID,
			Cron:                          "0 */5 * * *",
			MaxErrorCount:                 2,
			CloseConsumerWhenMessageIsNew: 10 * time.Minute,
		},
		ClientID: "client-id",
	}

	groupID := "message.topic.0.consumer"
	consumerGroupConfig := &ConsumerGroupConfig{
		GroupID:                     "message.topic.0.consumer",
		Name:                        "message.topic.0",
		Retry:                       "message.topic.RETRY.0",
		Error:                       "message.topic.ERROR.0",
		RetryCount:                  3,
		Cluster:                     "cluster",
		VirtualPartitionCount:       1,
		ConsumeBatchListenerLatency: 1 * time.Second,
		MaxProcessingTime:           1 * time.Second,
	}

	producer := kafka.NewMockProducer(controller)
	consumers := &ConsumerGroupConsumers{
		ConfigName: "configName",
		Consumer:   NewMockConsumer(controller),
	}

	consumerErrorInterceptor := NewMockConsumerErrorInterceptor(controller)

	initializeContext := ConsumerGroupInitializeContext{
		ClusterConfig:             clusterConfig,
		ConsumerGroupConfig:       consumerGroupConfig,
		Producer:                  producer,
		Consumers:                 consumers,
		LastStep:                  func(ctx context.Context, message *ConsumerMessage, err error) {},
		ConsumerInterceptors:      []ConsumerInterceptor{},
		ConsumerErrorInterceptors: []ConsumerErrorInterceptor{consumerErrorInterceptor},
		Tracers:                   []Tracer{},
	}

	// When
	consumerGroup := NewConsumerGroup(initializeContext)

	// Then
	assert.Equal(t, groupID, consumerGroup.GetGroupID())
}
