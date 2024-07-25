package internal

import (
	"time"
)

type processedMessage struct {
	GroupID          string
	Topic            string
	Partition        int32
	VirtualPartition int
	Offset           int64
	ProcessingTime   time.Duration
	Latency          time.Duration
}

func newProcessedMessage(message *ConsumerMessage) *processedMessage {
	return &processedMessage{
		GroupID:          message.GroupID,
		Topic:            message.Topic,
		Partition:        message.Partition,
		VirtualPartition: message.VirtualPartition,
		Offset:           message.Offset,
	}
}
