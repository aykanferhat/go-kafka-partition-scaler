package internal

import (
	"context"
	"testing"
	"time"

	"github.com/aykanferhat/go-kafka-partition-scaler/pkg/kafka/message"

	"github.com/aykanferhat/go-kafka-partition-scaler/common"
	"github.com/aykanferhat/go-kafka-partition-scaler/pkg/kafka"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

const (
	groupID    = "message.topic.0.consumer"
	topic      = "message.topic.0"
	retryTopic = "message.topic.RETRY.0"
	errorTopic = "message.topic.ERROR.0"
	cluster    = "cluster"
)

func Test_DefaultErrorConsumer_ShouldConsumeMessageThatHasNotTargetTopic(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	// Given

	ctx := context.Background()
	message := generateTestConsumerMessageForDefaultErrorConsumer(topic, "1", "error")

	producer := kafka.NewMockProducer(controller)

	retryConsumer := NewDefaultErrorConsumer(producer)

	// When
	consumeResult := retryConsumer.Consume(ctx, message)

	// Then
	assert.Nil(t, consumeResult)
}

func Test_DefaultErrorConsumer_ShouldConsumeMessageThatHasTargetTopic(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	// Given
	targetTopic := "target-topic"
	ctx := context.Background()
	msg := &ConsumerMessage{
		Headers: []message.Header{
			{
				Key:   common.ToByte(common.ErrorTopicCountKey.String()),
				Value: common.ToByte("1"),
			},
			{
				Key:   common.ToByte(common.ErrorMessageKey.String()),
				Value: common.ToByte("error message"),
			},
			{
				Key:   common.ToByte(common.TargetTopicKey.String()),
				Value: common.ToByte(targetTopic),
			},
		},
		Timestamp:        time.Time{},
		Key:              common.ToByte("key"),
		Value:            common.ToByte("value"),
		Topic:            topic,
		Partition:        0,
		Offset:           0,
		VirtualPartition: 0,
	}

	producer := kafka.NewMockProducer(controller)
	producer.EXPECT().ProduceSync(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, message *ProducerMessage) error {
		assert.Equal(t, targetTopic, message.Topic)
		return nil
	})

	retryConsumer := NewDefaultErrorConsumer(producer)

	// When
	consumeResult := retryConsumer.Consume(ctx, msg)

	// Then
	assert.Nil(t, consumeResult)
}

func generateTestConsumerMessageForDefaultErrorConsumer(topic string, errorCount string, errorMessage string) *ConsumerMessage {
	return &ConsumerMessage{
		Headers: []message.Header{
			{
				Key:   common.ToByte(common.ErrorTopicCountKey.String()),
				Value: common.ToByte(errorCount),
			},
			{
				Key:   common.ToByte(common.ErrorMessageKey.String()),
				Value: common.ToByte(errorMessage),
			},
		},
		Timestamp:        time.Time{},
		Key:              common.ToByte("key"),
		Value:            common.ToByte("value"),
		Topic:            topic,
		Partition:        0,
		Offset:           0,
		VirtualPartition: 0,
	}
}
