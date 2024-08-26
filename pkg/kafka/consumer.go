package kafka

import (
	"context"
	"github.com/aykanferhat/go-kafka-partition-scaler/pkg/log"
	"time"

	"github.com/aykanferhat/go-kafka-partition-scaler/pkg/kafka/config"
	"github.com/aykanferhat/go-kafka-partition-scaler/pkg/kafka/sarama"
)

type ConsumerGroup interface {
	Subscribe(ctx context.Context) error
	Unsubscribe() error
}

func NewConsumerGroup(
	clusterConfig *ClusterConfig,
	consumerGroupConfig *ConsumerGroupConfig,
	messageHandler MessageHandler,
	consumerStatusHandler ConsumerStatusHandler,
	logger log.Logger,
) (ConsumerGroup, error) {
	return sarama.NewConsumerGroup(clusterConfig, consumerGroupConfig, messageHandler, consumerStatusHandler, logger)
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
