package internal

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aykanferhat/go-kafka-partition-scaler/common"

	"github.com/aykanferhat/go-kafka-partition-scaler/pkg/kafka"
	"github.com/aykanferhat/go-kafka-partition-scaler/pkg/kafka/message"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func Test_BatchMessageListener_ShouldListenBatchMessage(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	// Given
	partition := int32(0)
	virtualPartition := 2
	maxProcessingTime := 2 * time.Second

	consumerGroupConfig := &ConsumerGroupConfig{
		GroupID:                     groupID,
		Name:                        topic,
		Retry:                       retryTopic,
		Error:                       errorTopic,
		RetryCount:                  3,
		BatchSize:                   2,
		VirtualPartitionCount:       3,
		Cluster:                     "cluster",
		ConsumeBatchListenerLatency: 1 * time.Second,
		MaxProcessingTime:           maxProcessingTime,
	}

	firstMessage := generateTestConsumerMessageForListener("key1", "value1", 0, virtualPartition)
	secondMessage := generateTestConsumerMessageForListener("key2", "value2", 1, virtualPartition)
	firstMessagesList := []*ConsumerMessage{
		firstMessage,
		secondMessage,
	}

	thirdMessage := generateTestConsumerMessageForListener("key3", "value3", 2, virtualPartition)
	secondMessagesList := []*ConsumerMessage{
		thirdMessage,
	}

	batchConsumer := NewMockBatchConsumer(controller)
	batchConsumer.EXPECT().Consume(gomock.Any(), gomock.Any()).Return(make(map[*ConsumerMessage]error)).Times(2)

	consumers := &ConsumerGroupConsumers{
		ConfigName:    "configName",
		BatchConsumer: batchConsumer,
	}

	processedMessageListener := NewMockProcessedMessageListener(controller)
	processedMessageListener.EXPECT().IsChannelClosed().Return(false).Times(3)
	for _, msg := range firstMessagesList {
		processedMessageListener.EXPECT().Publish(msg).Times(1)
	}
	for _, msg := range secondMessagesList {
		processedMessageListener.EXPECT().Publish(msg).Times(1)
	}

	producer := kafka.NewMockProducer(controller)

	consumerInterceptor := NewMockConsumerInterceptor(controller)
	consumerInterceptor.EXPECT().OnConsume(gomock.Any(), firstMessage).Return(context.Background()).Times(1)
	consumerInterceptor.EXPECT().OnConsume(gomock.Any(), secondMessage).Return(context.Background()).Times(1)
	consumerInterceptor.EXPECT().OnConsume(gomock.Any(), thirdMessage).Return(context.Background()).Times(1)

	initializeContext := ConsumerGroupInitializeContext{
		ConsumerGroupConfig:       consumerGroupConfig,
		Producer:                  producer,
		Consumers:                 consumers,
		LastStep:                  func(ctx context.Context, message *ConsumerMessage, err error) {},
		ConsumerInterceptors:      []ConsumerInterceptor{consumerInterceptor},
		ConsumerErrorInterceptors: []ConsumerErrorInterceptor{},
		Tracers:                   []Tracer{},
	}

	listener := newBatchMessageListener(
		topic,
		partition,
		virtualPartition,
		initializeContext,
		processedMessageListener,
	)

	// When
	time.Sleep(1 * time.Second)

	for _, msg := range append(firstMessagesList, secondMessagesList...) {
		listener.Publish(msg)
	}

	// Then
	time.Sleep(2 * time.Second)
	listener.Close()
	assert.Equal(t, 0, len(listener.processMessages))
	assert.Equal(t, true, listener.messageListenerClosed)
}

func Test_BatchMessageListener_ShouldListenBatchErrorMessageAndSendRetryTopic(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	// Given
	partition := int32(0)
	virtualPartition := 2
	maxProcessingTime := 2 * time.Second

	consumerGroupConfig := &ConsumerGroupConfig{
		GroupID:                     groupID,
		Name:                        topic,
		Retry:                       retryTopic,
		Error:                       errorTopic,
		RetryCount:                  3,
		BatchSize:                   2,
		VirtualPartitionCount:       3,
		Cluster:                     "cluster",
		ConsumeBatchListenerLatency: 1 * time.Second,
		MaxProcessingTime:           maxProcessingTime,
	}

	firstMessage := generateTestConsumerMessageForListener("key1", "value1", 0, virtualPartition)
	secondMessage := generateTestConsumerMessageForListener("key2", "value2", 1, virtualPartition)

	messages := []*ConsumerMessage{
		firstMessage,
		secondMessage,
	}

	err := errors.New("error")
	batchConsumer := NewMockBatchConsumer(controller)
	batchConsumer.EXPECT().Consume(gomock.Any(), gomock.Any()).Return(map[*ConsumerMessage]error{
		firstMessage:  err,
		secondMessage: err,
	}).Times(1)

	consumers := &ConsumerGroupConsumers{
		ConfigName:    "configName",
		BatchConsumer: batchConsumer,
	}

	processedMessageListener := NewMockProcessedMessageListener(controller)
	processedMessageListener.EXPECT().IsChannelClosed().Return(false).Times(2)
	for _, msg := range messages {
		processedMessageListener.EXPECT().Publish(msg).Times(1)
	}

	producer := kafka.NewMockProducer(controller)
	producer.EXPECT().ProduceSync(gomock.Any(), gomock.Any()).Return(nil).Times(2)

	consumerErrorInterceptor := NewMockConsumerErrorInterceptor(controller)
	consumerErrorInterceptor.EXPECT().OnError(gomock.Any(), firstMessage, err).Times(1)
	consumerErrorInterceptor.EXPECT().OnError(gomock.Any(), secondMessage, err).Times(1)

	initializeContext := ConsumerGroupInitializeContext{
		ConsumerGroupConfig:       consumerGroupConfig,
		Producer:                  producer,
		Consumers:                 consumers,
		LastStep:                  func(ctx context.Context, message *ConsumerMessage, err error) {},
		ConsumerInterceptors:      []ConsumerInterceptor{},
		ConsumerErrorInterceptors: []ConsumerErrorInterceptor{consumerErrorInterceptor},
		Tracers:                   []Tracer{},
	}

	listener := newBatchMessageListener(
		topic,
		partition,
		virtualPartition,
		initializeContext,
		processedMessageListener,
	)

	// When
	for _, msg := range messages {
		listener.Publish(msg)
	}
	time.Sleep(2 * time.Second)

	// Then
	for {
		if len(listener.processMessages) == 0 {
			break
		}
	}
	listener.Close()
	assert.Equal(t, true, listener.messageListenerClosed)
}

func Test_BatchMessageListener_ShouldListenBatchErrorMessageAndDoNotSendRetryTopic(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	// Given
	partition := int32(0)
	virtualPartition := 2
	maxProcessingTime := 2 * time.Second

	consumerGroupConfig := &ConsumerGroupConfig{
		GroupID:                     groupID,
		Name:                        topic,
		Retry:                       retryTopic,
		Error:                       errorTopic,
		RetryCount:                  3,
		BatchSize:                   2,
		VirtualPartitionCount:       3,
		Cluster:                     "cluster",
		ConsumeBatchListenerLatency: 1 * time.Second,
		MaxProcessingTime:           maxProcessingTime,
	}

	firstMessage := generateTestConsumerMessageForListener("key1", "value1", 0, virtualPartition)
	secondMessage := generateTestConsumerMessageForListener("key2", "value2", 1, virtualPartition)

	messages := []*ConsumerMessage{
		firstMessage,
		secondMessage,
	}

	err := errors.New("error")
	batchConsumer := NewMockBatchConsumer(controller)
	batchConsumer.EXPECT().Consume(gomock.Any(), gomock.Any()).Return(map[*ConsumerMessage]error{
		firstMessage:  err,
		secondMessage: err,
	}).Times(1)

	consumers := &ConsumerGroupConsumers{
		ConfigName:    "configName",
		BatchConsumer: batchConsumer,
	}

	consumerErrorInterceptor := NewMockConsumerErrorInterceptor(controller)
	consumerErrorInterceptor.EXPECT().OnError(gomock.Any(), firstMessage, err).Times(1)
	consumerErrorInterceptor.EXPECT().OnError(gomock.Any(), secondMessage, err).Times(1)

	producer := kafka.NewMockProducer(controller)
	producer.EXPECT().ProduceSync(gomock.Any(), gomock.Any()).Return(errors.New("SendMessages error")).Times(2)

	processedMessageListener := NewMockProcessedMessageListener(controller)
	processedMessageListener.EXPECT().IsChannelClosed().Return(false).Times(2)
	for _, msg := range messages {
		processedMessageListener.EXPECT().Publish(msg).Times(1)
	}

	initializeContext := ConsumerGroupInitializeContext{
		ConsumerGroupConfig:       consumerGroupConfig,
		Producer:                  producer,
		Consumers:                 consumers,
		LastStep:                  func(ctx context.Context, message *ConsumerMessage, err error) {},
		ConsumerInterceptors:      []ConsumerInterceptor{},
		ConsumerErrorInterceptors: []ConsumerErrorInterceptor{consumerErrorInterceptor},
		Tracers:                   []Tracer{},
	}

	listener := newBatchMessageListener(
		topic,
		partition,
		virtualPartition,
		initializeContext,
		processedMessageListener,
	)

	// When
	for _, msg := range messages {
		listener.Publish(msg)
	}
	time.Sleep(2 * time.Second)

	// Then
	for {
		if len(listener.processMessages) == 0 {
			break
		}
	}
	listener.Close()
	assert.Equal(t, true, listener.messageListenerClosed)
}

func Test_BatchMessageListener_ShouldListenBatchContextDeadlineMessageAndSendRetryTopic(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	// Given
	partition := int32(0)
	virtualPartition := 2
	maxProcessingTime := 1 * time.Second

	consumerGroupConfig := &ConsumerGroupConfig{
		GroupID:                     groupID,
		Name:                        topic,
		Retry:                       retryTopic,
		Error:                       errorTopic,
		RetryCount:                  3,
		BatchSize:                   2,
		VirtualPartitionCount:       3,
		Cluster:                     "cluster",
		ConsumeBatchListenerLatency: 1 * time.Second,
		MaxProcessingTime:           maxProcessingTime,
	}

	firstMessage := generateTestConsumerMessageForListener("key1", "value1", 0, virtualPartition)
	secondMessage := generateTestConsumerMessageForListener("key2", "value2", 1, virtualPartition)

	messages := []*ConsumerMessage{
		firstMessage,
		secondMessage,
	}

	batchConsumer := NewMockBatchConsumer(controller)
	batchConsumer.EXPECT().Consume(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, messages []*ConsumerMessage) map[ConsumerMessage]error {
			time.Sleep(2 * time.Second)
			return nil
		})

	consumers := &ConsumerGroupConsumers{
		ConfigName:    "configName",
		BatchConsumer: batchConsumer,
	}

	consumerErrorInterceptor := NewMockConsumerErrorInterceptor(controller)
	consumerErrorInterceptor.EXPECT().OnError(gomock.Any(), firstMessage, gomock.Any()).Times(1)
	consumerErrorInterceptor.EXPECT().OnError(gomock.Any(), secondMessage, gomock.Any()).Times(1)

	producer := kafka.NewMockProducer(controller)
	producer.EXPECT().ProduceSync(gomock.Any(), gomock.Any()).Return(nil).Times(2)

	processedMessageListener := NewMockProcessedMessageListener(controller)
	processedMessageListener.EXPECT().IsChannelClosed().Return(false).Times(2)
	processedMessageListener.EXPECT().Publish(firstMessage).Times(1)
	processedMessageListener.EXPECT().Publish(secondMessage).Times(1)

	initializeContext := ConsumerGroupInitializeContext{
		ConsumerGroupConfig:       consumerGroupConfig,
		Producer:                  producer,
		Consumers:                 consumers,
		LastStep:                  func(ctx context.Context, message *ConsumerMessage, err error) {},
		ConsumerInterceptors:      []ConsumerInterceptor{},
		ConsumerErrorInterceptors: []ConsumerErrorInterceptor{consumerErrorInterceptor},
		Tracers:                   []Tracer{},
	}

	listener := newBatchMessageListener(
		topic,
		partition,
		virtualPartition,
		initializeContext,
		processedMessageListener,
	)

	// When
	for _, msg := range messages {
		listener.Publish(msg)
	}
	time.Sleep(2 * time.Second)

	// Then
	for {
		if len(listener.processMessages) == 0 {
			break
		}
	}
	listener.Close()
	assert.Equal(t, true, listener.messageListenerClosed)
}

func Test_BatchMessageListener_ShouldNotListenMessageWhenChannelIsClosed(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	// Given
	partition := int32(0)
	virtualPartition := 2
	maxProcessingTime := 10 * time.Second

	consumerGroupConfig := &ConsumerGroupConfig{
		GroupID:                     groupID,
		Name:                        topic,
		Retry:                       retryTopic,
		Error:                       errorTopic,
		RetryCount:                  3,
		BatchSize:                   2,
		VirtualPartitionCount:       3,
		VirtualPartitionChanCount:   100,
		Cluster:                     "cluster",
		ConsumeBatchListenerLatency: 1 * time.Second,
		MaxProcessingTime:           maxProcessingTime,
	}

	batchConsumer := NewMockBatchConsumer(controller)
	batchConsumer.EXPECT().Consume(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, messages []*ConsumerMessage) map[ConsumerMessage]error {
			time.Sleep(2 * time.Second)
			return nil
		}).AnyTimes()

	consumers := &ConsumerGroupConsumers{
		ConfigName:    "configName",
		BatchConsumer: batchConsumer,
	}

	producer := kafka.NewMockProducer(controller)
	producer.EXPECT().ProduceSync(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	processedMessageListener := NewMockProcessedMessageListener(controller)
	processedMessageListener.EXPECT().IsChannelClosed().Return(false).AnyTimes()
	processedMessageListener.EXPECT().Publish(gomock.Any()).AnyTimes()

	initializeContext := ConsumerGroupInitializeContext{
		ConsumerGroupConfig:       consumerGroupConfig,
		Producer:                  producer,
		Consumers:                 consumers,
		LastStep:                  func(ctx context.Context, message *ConsumerMessage, err error) {},
		ConsumerInterceptors:      []ConsumerInterceptor{},
		ConsumerErrorInterceptors: []ConsumerErrorInterceptor{},
		Tracers:                   []Tracer{},
	}

	listener := newBatchMessageListener(
		topic,
		partition,
		virtualPartition,
		initializeContext,
		processedMessageListener,
	)

	// When
	go func() {
		for i := 0; i < 100; i++ {
			msg := generateTestConsumerMessageForListener("key1", "value1", int64(i), virtualPartition)
			listener.Publish(msg)
		}
	}()
	time.Sleep(100 * time.Millisecond)

	// Then
	listener.Close()
	assert.Equal(t, true, listener.messageListenerClosed)
}

func Test_BatchMessageListener_ShouldListenBatchMessageWhenThereIsNoMessage(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	// Given
	partition := int32(0)
	virtualPartition := 2
	maxProcessingTime := 1 * time.Second

	consumerGroupConfig := &ConsumerGroupConfig{
		GroupID:                     groupID,
		Name:                        topic,
		Retry:                       retryTopic,
		Error:                       errorTopic,
		RetryCount:                  3,
		BatchSize:                   2,
		VirtualPartitionCount:       3,
		VirtualPartitionChanCount:   100,
		Cluster:                     cluster,
		ConsumeBatchListenerLatency: 1 * time.Second,
		MaxProcessingTime:           maxProcessingTime,
	}

	batchConsumer := NewMockBatchConsumer(controller)

	consumers := &ConsumerGroupConsumers{
		ConfigName:    "configName",
		BatchConsumer: batchConsumer,
	}

	producer := kafka.NewMockProducer(controller)
	processedMessageListener := NewMockProcessedMessageListener(controller)

	initializeContext := ConsumerGroupInitializeContext{
		ConsumerGroupConfig:       consumerGroupConfig,
		Producer:                  producer,
		Consumers:                 consumers,
		LastStep:                  func(ctx context.Context, message *ConsumerMessage, err error) {},
		ConsumerInterceptors:      []ConsumerInterceptor{},
		ConsumerErrorInterceptors: []ConsumerErrorInterceptor{},
		Tracers:                   []Tracer{},
	}

	listener := newBatchMessageListener(
		topic,
		partition,
		virtualPartition,
		initializeContext,
		processedMessageListener,
	)

	// When
	listener.processBufferedMessageTicker.Reset(1 * time.Second)

	time.Sleep(5 * time.Second)

	// Then
	listener.Close()
	assert.Equal(t, true, listener.messageListenerClosed)
}

func generateTestConsumerMessageForListener(key string, value string, offset int64, virtualPartition int) *ConsumerMessage {
	return &ConsumerMessage{
		ConsumerMessage: &message.ConsumerMessage{
			Headers:   make([]message.Header, 0),
			Timestamp: time.Time{},
			Key:       common.ToByte(key),
			Value:     common.ToByte(value),
			Topic:     topic,
			Partition: 0,
			Offset:    offset,
		},
		VirtualPartition: virtualPartition,
	}
}

func generateTestConsumerMessageWithHeaderForListener(topic string, key string, value string, headerKey string, headerValue string, offset int64, virtualPartition int) *ConsumerMessage {
	return &ConsumerMessage{
		ConsumerMessage: &message.ConsumerMessage{
			Headers: []message.Header{
				{
					Key:   common.ToByte(headerKey),
					Value: common.ToByte(headerValue),
				},
			},
			Timestamp: time.Time{},
			Key:       common.ToByte(key),
			Value:     common.ToByte(value),
			Topic:     topic,
			Partition: 0,
			Offset:    offset,
		},
		VirtualPartition: virtualPartition,
	}
}
