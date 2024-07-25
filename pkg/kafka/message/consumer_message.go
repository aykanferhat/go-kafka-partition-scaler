package message

import (
	"time"
)

type ConsumerMessage struct {
	Timestamp time.Time
	Topic     string
	Headers   []Header
	Key       []byte
	Value     []byte
	Offset    int64
	Partition int32
}

type Header struct {
	Key   []byte
	Value []byte
}
