package internal

import (
	"context"
	"testing"
	"time"

	"github.com/Trendyol/go-kafka-partition-scaler/pkg/kafka"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func Test_Listener_shouldCreateUniqueListener(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	// Given
	consumers := &ConsumerGroupConsumers{}

	partition := int32(0)
	virtualPartition := 0

	consumerGroupConfig := &ConsumerGroupConfig{
		GroupID:                     "message.topic.0.consumer",
		Name:                        "message.topic.0",
		Retry:                       "message.topic.RETRY.0",
		Error:                       "message.topic.ERROR.0",
		RetryCount:                  3,
		BatchSize:                   2,
		UniqueListener:              true,
		ConsumeBatchListenerLatency: 10 * time.Second,
	}

	processedMessageListener := NewMockProcessedMessageListener(controller)
	producer := kafka.NewMockProducer(controller)

	initializeContext := ConsumerGroupInitializeContext{
		ConsumerGroupConfig:       consumerGroupConfig,
		Producer:                  producer,
		Consumers:                 consumers,
		LastStep:                  func(ctx context.Context, message *ConsumerMessage, err error) {},
		ConsumerInterceptors:      []ConsumerInterceptor{},
		ConsumerErrorInterceptors: []ConsumerErrorInterceptor{},
		Tracers:                   []Tracer{},
	}

	// When
	listener := NewMessageVirtualListener(
		topic,
		partition,
		virtualPartition,
		initializeContext,
		processedMessageListener,
	)

	// Then
	_, ok := listener.(*uniqueMessageListener)
	assert.True(t, ok)
}

func Test_Listener_shouldCreateBatchListener(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	// Given
	consumers := &ConsumerGroupConsumers{}
	partition := int32(0)
	virtualPartition := 0

	consumerGroupConfig := &ConsumerGroupConfig{
		GroupID:                     "message.topic.0.consumer",
		Name:                        "message.topic.0",
		Retry:                       "message.topic.RETRY.0",
		Error:                       "message.topic.ERROR.0",
		RetryCount:                  3,
		BatchSize:                   2,
		ConsumeBatchListenerLatency: 10 * time.Second,
	}

	processedMessageListener := NewMockProcessedMessageListener(controller)
	producer := kafka.NewMockProducer(controller)

	initializeContext := ConsumerGroupInitializeContext{
		ConsumerGroupConfig:       consumerGroupConfig,
		Producer:                  producer,
		Consumers:                 consumers,
		LastStep:                  func(ctx context.Context, message *ConsumerMessage, err error) {},
		ConsumerInterceptors:      []ConsumerInterceptor{},
		ConsumerErrorInterceptors: []ConsumerErrorInterceptor{},
		Tracers:                   []Tracer{},
	}

	// When
	listener := NewMessageVirtualListener(
		topic,
		partition,
		virtualPartition,
		initializeContext,
		processedMessageListener,
	)

	// Then
	_, ok := listener.(*batchMessageListener)
	assert.True(t, ok)
}

func Test_Listener_shouldCreateSingleListener(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	// Given
	consumers := &ConsumerGroupConsumers{}

	partition := int32(0)
	virtualPartition := 0

	consumerGroupConfig := &ConsumerGroupConfig{
		GroupID:    "message.topic.0.consumer",
		Name:       "message.topic.0",
		Retry:      "message.topic.RETRY.0",
		Error:      "message.topic.ERROR.0",
		RetryCount: 3,
	}

	processedMessageListener := NewMockProcessedMessageListener(controller)
	producer := kafka.NewMockProducer(controller)

	initializeContext := ConsumerGroupInitializeContext{
		ConsumerGroupConfig:       consumerGroupConfig,
		Producer:                  producer,
		Consumers:                 consumers,
		LastStep:                  func(ctx context.Context, message *ConsumerMessage, err error) {},
		ConsumerInterceptors:      []ConsumerInterceptor{},
		ConsumerErrorInterceptors: []ConsumerErrorInterceptor{},
		Tracers:                   []Tracer{},
	}

	// When
	listener := NewMessageVirtualListener(
		topic,
		partition,
		virtualPartition,
		initializeContext,
		processedMessageListener,
	)

	// Then
	_, ok := listener.(*singleMessageListener)
	assert.True(t, ok)
}
