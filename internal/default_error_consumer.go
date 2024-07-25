package internal

import (
	"context"

	"github.com/Trendyol/go-kafka-partition-scaler/pkg/kafka"
)

type defaultErrorConsumer struct {
	producer kafka.Producer
}

func NewDefaultErrorConsumer(
	producer kafka.Producer,
) Consumer {
	return &defaultErrorConsumer{
		producer: producer,
	}
}

func (consumer *defaultErrorConsumer) Consume(_ context.Context, message *ConsumerMessage) error {
	targetTopic := getTargetTopic(message)
	if targetTopic == "" {
		return nil
	}
	return sendMessageToTopic(consumer.producer, message, targetTopic, headersFromErrorToRetry(message))
}
