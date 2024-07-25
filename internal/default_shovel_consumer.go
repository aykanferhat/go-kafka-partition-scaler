package internal

import (
	"context"

	"github.com/Trendyol/go-kafka-partition-scaler/pkg/kafka"
)

type defaultShovelConsumer struct {
	producer    kafka.Producer
	targetTopic string
}

func NewDefaultShovelConsumer(
	producer kafka.Producer,
	targetTopic string,
) Consumer {
	return &defaultShovelConsumer{
		producer:    producer,
		targetTopic: targetTopic,
	}
}

func (consumer *defaultShovelConsumer) Consume(_ context.Context, message *ConsumerMessage) error {
	var targetTopic string
	if len(consumer.targetTopic) != 0 {
		targetTopic = consumer.targetTopic
	} else {
		targetTopic = getTargetTopic(message)
	}
	if targetTopic == "" {
		return nil
	}
	return sendMessageToTopic(consumer.producer, message, targetTopic, headersForShovel(message))
}
