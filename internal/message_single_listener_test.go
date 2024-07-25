package internal

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Trendyol/go-kafka-partition-scaler/common"
	"github.com/Trendyol/go-kafka-partition-scaler/pkg/kafka"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func Test_SingleMessageListener_ShouldListenMessage(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	// Given
	partition := int32(0)
	virtualPartition := 2
	maxProcessingTime := 2 * time.Second

	consumerGroupConfig := &ConsumerGroupConfig{
		GroupID:               groupID,
		Name:                  topic,
		Retry:                 retryTopic,
		Error:                 errorTopic,
		RetryCount:            3,
		VirtualPartitionCount: 3,
		Cluster:               cluster,
		MaxProcessingTime:     maxProcessingTime,
	}

	message := generateTestConsumerMessageForListener("key1", "value1", 0, virtualPartition)

	consumerErr := errors.New("error")
	consumer := NewMockConsumer(controller)
	consumer.EXPECT().Consume(gomock.Any(), gomock.Any()).Return(consumerErr).Times(1)

	consumers := &ConsumerGroupConsumers{
		ConfigName: "configName",
		Consumer:   consumer,
	}

	processedMessageListener := NewMockProcessedMessageListener(controller)
	processedMessageListener.EXPECT().IsChannelClosed().Return(false).Times(1)
	processedMessageListener.EXPECT().Publish(message).Times(1)

	producer := kafka.NewMockProducer(controller)
	producer.EXPECT().ProduceSync(gomock.Any(), gomock.Any()).Return(errors.New("SendMessage err")).Times(1)

	consumerInterceptor := NewMockConsumerInterceptor(controller)
	consumerInterceptor.EXPECT().OnConsume(gomock.Any(), message).Return(context.Background()).Times(1)

	consumerErrorInterceptor := NewMockConsumerErrorInterceptor(controller)
	consumerErrorInterceptor.EXPECT().OnError(gomock.Any(), message, consumerErr).Times(1)

	initializeContext := ConsumerGroupInitializeContext{
		ConsumerGroupConfig:       consumerGroupConfig,
		Producer:                  producer,
		Consumers:                 consumers,
		LastStep:                  func(ctx context.Context, message *ConsumerMessage, err error) {},
		ConsumerInterceptors:      []ConsumerInterceptor{consumerInterceptor},
		ConsumerErrorInterceptors: []ConsumerErrorInterceptor{consumerErrorInterceptor},
		Tracers:                   []Tracer{},
	}

	listener := newSingleMessageListener(
		topic,
		partition,
		virtualPartition,
		initializeContext,
		processedMessageListener,
	)
	time.Sleep(1 * time.Second)

	// When
	listener.Publish(message)

	// Then
	time.Sleep(1 * time.Second)
	listener.Close()
	assert.True(t, listener.messageListenerClosed)
}

func Test_SingleMessageListener_ShouldListenRetriedMessage(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	// Given
	partition := int32(0)
	virtualPartition := 2
	retriedCount := 1
	maxRetryCount := 3

	maxProcessingTime := 2 * time.Second

	consumerGroupConfig := &ConsumerGroupConfig{
		GroupID:               groupID,
		Name:                  topic,
		Retry:                 retryTopic,
		Error:                 errorTopic,
		RetryCount:            maxRetryCount,
		VirtualPartitionCount: 3,
		Cluster:               cluster,
		MaxProcessingTime:     maxProcessingTime,
	}

	message := generateTestConsumerMessageWithHeaderForListener(retryTopic, "key", "value", common.RetryTopicCountKey.String(), fmt.Sprint(retriedCount), 0, virtualPartition)

	consumerErr := errors.New("error")
	consumer := NewMockConsumer(controller)
	consumer.EXPECT().Consume(gomock.Any(), gomock.Any()).Return(consumerErr).Times(1)

	consumers := &ConsumerGroupConsumers{
		ConfigName: "configName",
		Consumer:   consumer,
	}

	processedMessageListener := NewMockProcessedMessageListener(controller)
	processedMessageListener.EXPECT().IsChannelClosed().Return(false).Times(1)
	processedMessageListener.EXPECT().Publish(message).Times(1)

	producer := kafka.NewMockProducer(controller)
	producer.EXPECT().ProduceSync(gomock.Any(), gomock.Any()).Return(errors.New("SendMessage err")).Times(1)

	consumerInterceptor := NewMockConsumerInterceptor(controller)
	consumerInterceptor.EXPECT().OnConsume(gomock.Any(), message).Return(context.Background()).Times(1)

	consumerErrorInterceptor := NewMockConsumerErrorInterceptor(controller)
	consumerErrorInterceptor.EXPECT().OnError(gomock.Any(), message, consumerErr).Times(1)

	initializeContext := ConsumerGroupInitializeContext{
		ConsumerGroupConfig:       consumerGroupConfig,
		Producer:                  producer,
		Consumers:                 consumers,
		LastStep:                  func(ctx context.Context, message *ConsumerMessage, err error) {},
		ConsumerInterceptors:      []ConsumerInterceptor{consumerInterceptor},
		ConsumerErrorInterceptors: []ConsumerErrorInterceptor{consumerErrorInterceptor},
		Tracers:                   []Tracer{},
	}

	listener := newSingleMessageListener(
		topic,
		partition,
		virtualPartition,
		initializeContext,
		processedMessageListener,
	)
	time.Sleep(1 * time.Second)

	// When

	listener.Publish(message)

	// Then
	time.Sleep(2 * time.Second)

	listener.Close()
	assert.True(t, listener.messageListenerClosed)
}

func Test_SingleMessageListener_ShouldListenReachedMaxRetriedCount(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	// Given
	partition := int32(0)
	virtualPartition := 2
	retriedCount := 3
	maxRetryCount := 3
	maxProcessingTime := 2 * time.Second

	consumerGroupConfig := &ConsumerGroupConfig{
		GroupID:               groupID,
		Name:                  topic,
		Retry:                 retryTopic,
		Error:                 errorTopic,
		RetryCount:            maxRetryCount,
		VirtualPartitionCount: 3,
		Cluster:               cluster,
		MaxProcessingTime:     maxProcessingTime,
	}

	message := generateTestConsumerMessageWithHeaderForListener(retryTopic, "key", "value", common.RetryTopicCountKey.String(), fmt.Sprint(retriedCount), 0, virtualPartition)

	consumerErr := errors.New("error")
	consumer := NewMockConsumer(controller)
	consumer.EXPECT().Consume(gomock.Any(), gomock.Any()).Return(consumerErr).Times(1)

	consumers := &ConsumerGroupConsumers{
		ConfigName: "configName",
		Consumer:   consumer,
	}

	processedMessageListener := NewMockProcessedMessageListener(controller)
	processedMessageListener.EXPECT().IsChannelClosed().Return(false).Times(1)
	processedMessageListener.EXPECT().Publish(message).Times(1)

	producer := kafka.NewMockProducer(controller)
	producer.EXPECT().ProduceSync(gomock.Any(), gomock.Any()).Return(nil).Times(1)

	consumerInterceptor := NewMockConsumerInterceptor(controller)
	consumerInterceptor.EXPECT().OnConsume(gomock.Any(), message).Return(context.Background()).Times(1)

	consumerErrorInterceptor := NewMockConsumerErrorInterceptor(controller)
	consumerErrorInterceptor.EXPECT().OnError(gomock.Any(), message, consumerErr).Times(1)

	initializeContext := ConsumerGroupInitializeContext{
		ConsumerGroupConfig:       consumerGroupConfig,
		Producer:                  producer,
		Consumers:                 consumers,
		LastStep:                  func(ctx context.Context, message *ConsumerMessage, err error) {},
		ConsumerInterceptors:      []ConsumerInterceptor{consumerInterceptor},
		ConsumerErrorInterceptors: []ConsumerErrorInterceptor{consumerErrorInterceptor},
		Tracers:                   []Tracer{},
	}

	listener := newSingleMessageListener(
		topic,
		partition,
		virtualPartition,
		initializeContext,
		processedMessageListener,
	)
	time.Sleep(1 * time.Second)

	// When
	listener.Publish(message)

	// Then
	time.Sleep(1 * time.Second)
	listener.Close()
	assert.True(t, listener.messageListenerClosed)
}

func Test_SingleMessageListener_ShouldListenMessageWhenRetryTopicNotFoundButExistsErrorTopic(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	// Given
	partition := int32(0)
	virtualPartition := 2
	maxProcessingTime := 2 * time.Second

	consumerGroupConfig := &ConsumerGroupConfig{
		GroupID:               groupID,
		Name:                  topic,
		Error:                 errorTopic,
		VirtualPartitionCount: 3,
		Cluster:               cluster,
		MaxProcessingTime:     maxProcessingTime,
	}

	message := generateTestConsumerMessageForListener("key", "value", 0, virtualPartition)

	consumerErr := errors.New("error")
	consumer := NewMockConsumer(controller)
	consumer.EXPECT().Consume(gomock.Any(), gomock.Any()).Return(consumerErr).Times(1)

	consumers := &ConsumerGroupConsumers{
		ConfigName: "configName",
		Consumer:   consumer,
	}

	processedMessageListener := NewMockProcessedMessageListener(controller)
	processedMessageListener.EXPECT().IsChannelClosed().Return(false).Times(1)
	processedMessageListener.EXPECT().Publish(gomock.Any()).Times(1)

	producer := kafka.NewMockProducer(controller)
	producer.EXPECT().ProduceSync(gomock.Any(), gomock.Any()).Return(nil).Times(1)

	consumerInterceptor := NewMockConsumerInterceptor(controller)
	consumerInterceptor.EXPECT().OnConsume(gomock.Any(), message).Return(context.Background()).Times(1)

	consumerErrorInterceptor := NewMockConsumerErrorInterceptor(controller)
	consumerErrorInterceptor.EXPECT().OnError(gomock.Any(), message, consumerErr).Times(1)

	initializeContext := ConsumerGroupInitializeContext{
		ConsumerGroupConfig:       consumerGroupConfig,
		Producer:                  producer,
		Consumers:                 consumers,
		LastStep:                  func(ctx context.Context, message *ConsumerMessage, err error) {},
		ConsumerInterceptors:      []ConsumerInterceptor{consumerInterceptor},
		ConsumerErrorInterceptors: []ConsumerErrorInterceptor{consumerErrorInterceptor},
		Tracers:                   []Tracer{},
	}

	listener := newSingleMessageListener(
		topic,
		partition,
		virtualPartition,
		initializeContext,
		processedMessageListener,
	)
	time.Sleep(1 * time.Second)

	// When
	listener.Publish(message)

	// Then
	time.Sleep(1 * time.Second)
	listener.Close()
	assert.True(t, listener.messageListenerClosed)
}

func Test_SingleMessageListener_ShouldListenReachedMaxRetriedCountWhenErrorTopicNotFound(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	// Given
	partition := int32(0)
	retryTopic := "message.topic.RETRY.0"
	virtualPartition := 2

	retriedCount := 3
	maxRetryCount := 3

	maxProcessingTime := 2 * time.Second

	consumerGroupConfig := &ConsumerGroupConfig{
		GroupID:               "message.topic.0.consumer",
		Name:                  topic,
		Retry:                 retryTopic,
		RetryCount:            maxRetryCount,
		VirtualPartitionCount: 3,
		Cluster:               "cluster",
		MaxProcessingTime:     maxProcessingTime,
	}

	message := generateTestConsumerMessageWithHeaderForListener(retryTopic, "key", "value", common.RetryTopicCountKey.String(), fmt.Sprint(retriedCount), 0, virtualPartition)

	consumerErr := errors.New("error")
	consumer := NewMockConsumer(controller)
	consumer.EXPECT().Consume(gomock.Any(), gomock.Any()).Return(consumerErr).Times(1)

	consumers := &ConsumerGroupConsumers{
		ConfigName: "configName",
		Consumer:   consumer,
	}

	processedMessageListener := NewMockProcessedMessageListener(controller)
	processedMessageListener.EXPECT().IsChannelClosed().Return(false).Times(1)
	processedMessageListener.EXPECT().Publish(message).Times(1)

	producer := kafka.NewMockProducer(controller)
	producer.EXPECT().ProduceSync(gomock.Any(), gomock.Any()).Times(0)

	consumerInterceptor := NewMockConsumerInterceptor(controller)
	consumerInterceptor.EXPECT().OnConsume(gomock.Any(), message).Return(context.Background()).Times(1)

	consumerErrorInterceptor := NewMockConsumerErrorInterceptor(controller)
	consumerErrorInterceptor.EXPECT().OnError(gomock.Any(), message, consumerErr).Times(1)

	initializeContext := ConsumerGroupInitializeContext{
		ConsumerGroupConfig:       consumerGroupConfig,
		Producer:                  producer,
		Consumers:                 consumers,
		LastStep:                  func(ctx context.Context, message *ConsumerMessage, err error) {},
		ConsumerInterceptors:      []ConsumerInterceptor{consumerInterceptor},
		ConsumerErrorInterceptors: []ConsumerErrorInterceptor{consumerErrorInterceptor},
		Tracers:                   []Tracer{},
	}

	listener := newSingleMessageListener(
		topic,
		partition,
		virtualPartition,
		initializeContext,
		processedMessageListener,
	)
	time.Sleep(1 * time.Second)

	// When
	listener.Publish(message)

	// Then
	time.Sleep(1 * time.Second)
	listener.Close()
	assert.True(t, listener.messageListenerClosed)
}

func Test_SingleMessageListener_ShouldListenMessageWhenErrorAndRetryTopicNotFound(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	// Given
	topic := "message.topic.0"
	partition := int32(0)
	virtualPartition := 2

	maxProcessingTime := 2 * time.Second

	consumerGroupConfig := &ConsumerGroupConfig{
		GroupID:               "message.topic.0.consumer",
		Name:                  topic,
		VirtualPartitionCount: 3,
		Cluster:               "cluster",
		MaxProcessingTime:     maxProcessingTime,
	}

	message := generateTestConsumerMessageForListener("key", "value", 0, virtualPartition)

	consumerErr := errors.New("error")
	consumer := NewMockConsumer(controller)
	consumer.EXPECT().Consume(gomock.Any(), gomock.Any()).Return(consumerErr).Times(1)

	consumers := &ConsumerGroupConsumers{
		ConfigName: "configName",
		Consumer:   consumer,
	}

	processedMessageListener := NewMockProcessedMessageListener(controller)
	processedMessageListener.EXPECT().IsChannelClosed().Return(false).Times(1)
	processedMessageListener.EXPECT().Publish(message).Times(1)

	producer := kafka.NewMockProducer(controller)
	producer.EXPECT().ProduceSync(gomock.Any(), gomock.Any()).Times(0)

	consumerInterceptor := NewMockConsumerInterceptor(controller)
	consumerInterceptor.EXPECT().OnConsume(gomock.Any(), message).Return(context.Background()).Times(1)

	consumerErrorInterceptor := NewMockConsumerErrorInterceptor(controller)
	consumerErrorInterceptor.EXPECT().OnError(gomock.Any(), message, consumerErr).Times(1)

	initializeContext := ConsumerGroupInitializeContext{
		ConsumerGroupConfig:       consumerGroupConfig,
		Producer:                  producer,
		Consumers:                 consumers,
		LastStep:                  func(ctx context.Context, message *ConsumerMessage, err error) {},
		ConsumerInterceptors:      []ConsumerInterceptor{consumerInterceptor},
		ConsumerErrorInterceptors: []ConsumerErrorInterceptor{consumerErrorInterceptor},
		Tracers:                   []Tracer{},
	}

	listener := newSingleMessageListener(
		topic,
		partition,
		virtualPartition,
		initializeContext,
		processedMessageListener,
	)
	time.Sleep(1 * time.Second)

	// When
	listener.Publish(message)

	// Then
	time.Sleep(1 * time.Second)
	listener.Close()
	assert.True(t, listener.messageListenerClosed)
}
