package handler

import (
	"github.com/aykanferhat/go-kafka-partition-scaler/pkg/kafka/message"
)

type (
	MessageHandler        = func(topic string, partition int32, messageChan <-chan *message.ConsumerMessage, commitFunc CommitMessageFunc)
	ConsumerStatusHandler = func(topic string, partition int32, status bool)
	CommitMessageFunc     func(topic string, partition int32, offset int64)
)
