package internal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Trendyol/go-kafka-partition-scaler/pkg/kafka"
)

func processMessage(ctx context.Context, consumer Consumer, message *ConsumerMessage, maxProcessingTime time.Duration) error {
	contextWithTimeout, cancel := context.WithTimeout(ctx, maxProcessingTime)
	defer cancel()
	resultChan := make(chan error, 1)
	go func(r chan<- error) {
		r <- consumer.Consume(contextWithTimeout, message)
	}(resultChan)
	select {
	case or := <-resultChan:
		return or
	case <-contextWithTimeout.Done():
		return contextWithTimeout.Err()
	}
}

func processMessages(ctx context.Context, consumer BatchConsumer, messages []*ConsumerMessage, maxProcessingTime time.Duration) map[*ConsumerMessage]error {
	contextWithTimeout, cancel := context.WithTimeout(ctx, maxProcessingTime)
	defer cancel()
	resultChan := make(chan map[*ConsumerMessage]error, 1)
	go func(r chan<- map[*ConsumerMessage]error) {
		r <- consumer.Consume(contextWithTimeout, messages)
	}(resultChan)
	select {
	case or := <-resultChan:
		return or
	case <-contextWithTimeout.Done():
		err := contextWithTimeout.Err()
		consumeResults := make(map[*ConsumerMessage]error)
		for _, msg := range messages {
			consumeResults[msg] = err
		}
		return consumeResults
	}
}

func processConsumedMessageError(ctx context.Context, message *ConsumerMessage, err error, initializedContext ConsumerGroupInitializeContext) {
	for _, interceptor := range initializedContext.ConsumerErrorInterceptors {
		interceptor.OnError(ctx, message, err)
	}
	if isMainTopic(message, initializedContext.ConsumerGroupConfig) {
		processConsumedMainTopicMessageError(ctx, message, err, initializedContext)
		return
	}

	if isRetryTopic(message, initializedContext.ConsumerGroupConfig) {
		processConsumedRetryTopicMessageError(ctx, message, err, initializedContext)
		return
	}
}

func processConsumedMainTopicMessageError(ctx context.Context, message *ConsumerMessage, err error, initializedContext ConsumerGroupInitializeContext) {
	if initializedContext.ConsumerGroupConfig.IsNotDefinedRetryAndErrorTopic() {
		initializedContext.LastStep(ctx, message, err)
		return
	}
	if initializedContext.ConsumerGroupConfig.IsNotDefinedRetryTopic() && initializedContext.ConsumerGroupConfig.IsDefinedErrorTopic() {
		messageSendError := sendMessageToTopic(initializedContext.Producer, message, initializedContext.ConsumerGroupConfig.Error, headersForError(message, err.Error()))
		if messageSendError != nil {
			joinedErr := errors.Join(err, messageSendError)
			initializedContext.LastStep(ctx, message, joinedErr)
		}
		return
	}
	messageSendError := sendMessageToTopic(initializedContext.Producer, message, initializedContext.ConsumerGroupConfig.Retry, headersForRetry(message, err.Error()))
	if messageSendError != nil {
		joinedErr := errors.Join(err, messageSendError)
		initializedContext.LastStep(ctx, message, joinedErr)
	}
}

func processConsumedRetryTopicMessageError(ctx context.Context, message *ConsumerMessage, err error, initializedContext ConsumerGroupInitializeContext) {
	retriedCount := getRetriedCount(message)
	if retriedCount >= initializedContext.ConsumerGroupConfig.RetryCount && initializedContext.ConsumerGroupConfig.IsNotDefinedErrorTopic() {
		initializedContext.LastStep(ctx, message, err)
		return
	}
	if retriedCount >= initializedContext.ConsumerGroupConfig.RetryCount && initializedContext.ConsumerGroupConfig.IsDefinedErrorTopic() {
		reachedMaxRetryCountErr := fmt.Errorf("reached max rety count, retriedCount: %d", retriedCount)
		joinedErr := errors.Join(err, reachedMaxRetryCountErr)
		messageSendError := sendMessageToTopic(initializedContext.Producer, message, initializedContext.ConsumerGroupConfig.Error, headersFromRetryToError(message, joinedErr.Error()))
		if messageSendError != nil {
			joinedErr = errors.Join(joinedErr, messageSendError)
			initializedContext.LastStep(ctx, message, joinedErr)
		}
		return
	}
	messageSendError := sendMessageToTopic(initializedContext.Producer, message, initializedContext.ConsumerGroupConfig.Retry, headersFromRetryToRetry(message, err.Error(), retriedCount))
	if messageSendError != nil {
		joinedErr := errors.Join(err, messageSendError)
		initializedContext.LastStep(ctx, message, joinedErr)
	}
}

func sendMessageToTopic(producer kafka.Producer, msg *ConsumerMessage, topic string, headers []kafka.Header) error {
	return producer.ProduceSync(context.Background(), &ProducerMessage{
		Headers:  headers,
		Topic:    topic,
		Key:      string(msg.Key),
		Body:     msg.Value,
		ByteBody: true,
	})
}

func getKey(topic string, partition int32) string {
	return fmt.Sprintf("%s_%d", topic, partition)
}
