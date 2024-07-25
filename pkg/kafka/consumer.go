package kafka

import (
	"time"

	"github.com/Trendyol/go-kafka-partition-scaler/pkg/kafka/config"
	"github.com/Trendyol/go-kafka-partition-scaler/pkg/kafka/sarama"
)

//go:generate mockgen  -source=consumer.go -destination=consumer_mock.go -package=kafka
type ConsumerGroup interface {
	Subscribe() error
	Unsubscribe() error
}

func NewConsumerGroup(
	clusterConfig *ClusterConfig,
	consumerGroupConfig *ConsumerGroupConfig,
	messageHandler MessageHandler,
	consumerStatusHandler ConsumerStatusHandler,
) (ConsumerGroup, error) {
	return sarama.NewConsumerGroup(clusterConfig, consumerGroupConfig, messageHandler, consumerStatusHandler)
}

func NewAuthConfig(username, password string, certificates []string) *config.Auth {
	return &config.Auth{
		Username:     username,
		Password:     password,
		Certificates: certificates,
	}
}

func NewProducerConfig(requiredAcks config.RequiredAcks, compression config.Compression, timeout time.Duration, maxMessageBytes string) *config.ProducerConfig {
	return &config.ProducerConfig{
		RequiredAcks:    requiredAcks,
		Compression:     compression,
		Timeout:         timeout,
		MaxMessageBytes: maxMessageBytes,
	}
}
