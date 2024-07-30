package internal

import (
	"testing"
	"time"

	"github.com/aykanferhat/go-kafka-partition-scaler/pkg/kafka/message"
	"github.com/golang/mock/gomock"
)

func Test_ProcessedMessageListener_shouldMarkProcessedMessages(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	// Given
	partition := int32(0)
	virtualPartition := 0

	processedMessages := []*ConsumerMessage{
		generateTestConsumerMessageForProcessedMessageListener(100),
		generateTestConsumerMessageForProcessedMessageListener(104),
		generateTestConsumerMessageForProcessedMessageListener(103),
		generateTestConsumerMessageForProcessedMessageListener(102),
		generateTestConsumerMessageForProcessedMessageListener(101),
	}

	listener := NewProcessedMessageListener(
		topic,
		partition,
		virtualPartition,
		func(topic string, partition int32, offset int64) {},
	)
	listener.SetFirstConsumedMessage(topic, partition, 100)

	listener.ResetTicker(100 * time.Millisecond)

	// When
	for _, msg := range processedMessages {
		listener.Publish(msg)
	}

	// Then
	time.Sleep(1 * time.Second)
	listener.Close()
}

func generateTestConsumerMessageForProcessedMessageListener(offset int64) *ConsumerMessage {
	return &ConsumerMessage{
		ConsumerMessage: &message.ConsumerMessage{
			Headers:   make([]message.Header, 0),
			Timestamp: time.Time{},
			Topic:     topic,
			Partition: 0,
			Offset:    offset,
		},
		VirtualPartition: 0,
	}
}
