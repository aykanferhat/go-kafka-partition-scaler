package kafka

import "github.com/Trendyol/go-kafka-partition-scaler/pkg/kafka/handler"

type (
	MessageHandler        = handler.MessageHandler
	ConsumerStatusHandler = handler.ConsumerStatusHandler
	CommitMessageFunc     = handler.CommitMessageFunc
)
