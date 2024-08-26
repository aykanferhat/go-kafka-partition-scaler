package internal

import (
	"context"
	"testing"
	"time"

	"github.com/aykanferhat/go-kafka-partition-scaler/pkg/kafka"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func Test_UniqueMessageListener_ShouldListenMessage(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	// Given
	partition := int32(0)
	virtualPartition := 2

	batchSize := 6

	consumerGroupConfig := &ConsumerGroupConfig{
		GroupID:                     "message.topic.0.consumer",
		Name:                        topic,
		Retry:                       "message.topic.RETRY.0",
		Error:                       "message.topic.ERROR.0",
		RetryCount:                  3,
		BatchSize:                   batchSize,
		UniqueListener:              true,
		VirtualPartitionCount:       3,
		VirtualPartitionChanCount:   75,
		Cluster:                     "cluster",
		ConsumeBatchListenerLatency: 1 * time.Second,
		MaxProcessingTime:           20 * time.Second,
	}

	message1 := generateTestConsumerMessageForListener("key1", "value1", 0, virtualPartition)
	message2 := generateTestConsumerMessageForListener("key1", "value1", 1, virtualPartition)
	message3 := generateTestConsumerMessageForListener("key2", "value2", 2, virtualPartition)
	message4 := generateTestConsumerMessageForListener("key2", "value2", 3, virtualPartition)
	message5 := generateTestConsumerMessageForListener("key3", "value3", 4, virtualPartition)
	message6 := generateTestConsumerMessageForListener("key3", "value3", 5, virtualPartition)

	consumer := NewMockConsumer(controller)
	consumer.EXPECT().Consume(gomock.Any(), message2).Return(nil).Times(1)
	consumer.EXPECT().Consume(gomock.Any(), message4).Return(nil).Times(1)
	consumer.EXPECT().Consume(gomock.Any(), message6).Return(nil).Times(1)

	consumers := &ConsumerGroupConsumers{
		ConfigName: "configName",
		Consumer:   consumer,
	}

	processedMessageListener := NewMockProcessedMessageListener(controller)
	processedMessageListener.EXPECT().IsStopped().Return(false).Times(6 + 3)
	processedMessageListener.EXPECT().Publish(gomock.Any()).Times(6)

	producer := kafka.NewMockProducer(controller)

	consumerInterceptor := NewMockConsumerInterceptor(controller)
	consumerInterceptor.EXPECT().OnConsume(gomock.Any(), gomock.Any()).Return(context.Background()).Times(6)

	initializeContext := ConsumerGroupInitializeContext{
		ConsumerGroupConfig:       consumerGroupConfig,
		Producer:                  producer,
		Consumers:                 consumers,
		LastStep:                  func(ctx context.Context, message *ConsumerMessage, err error) {},
		ConsumerInterceptors:      []ConsumerInterceptor{consumerInterceptor},
		ConsumerErrorInterceptors: []ConsumerErrorInterceptor{},
		Tracers:                   []Tracer{},
	}

	listener := newUniqueMessageListener(
		topic,
		partition,
		virtualPartition,
		initializeContext,
		processedMessageListener,
	)
	time.Sleep(1 * time.Second)

	// When
	listener.Publish(message1)
	listener.Publish(message2)
	listener.Publish(message3)
	listener.Publish(message4)
	listener.Publish(message5)
	listener.Publish(message6)

	time.Sleep(2 * time.Second)

	// Then
	listener.Close()
	assert.Equal(t, 0, len(listener.processMessages))
	assert.Equal(t, true, listener.stopped)
}
