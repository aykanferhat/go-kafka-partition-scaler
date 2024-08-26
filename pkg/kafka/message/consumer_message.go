package message

import (
	"time"
)

type ConsumerMessage struct {
	Timestamp        time.Time
	Topic            string
	Tracer           string
	GroupID          string
	Headers          []Header
	Key              []byte
	Value            []byte
	Offset           int64
	VirtualPartition int
	Partition        int32
}

type Header struct {
	Key   []byte
	Value []byte
}

func (c *ConsumerMessage) SetAdditionalFields(virtualPartition int, groupID string, tracer string) {
	c.VirtualPartition = virtualPartition
	c.GroupID = groupID
	c.Tracer = tracer
}
