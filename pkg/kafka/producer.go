package kafka

import (
	"context"

	"github.com/Trendyol/go-kafka-partition-scaler/pkg/kafka/config"
	"github.com/Trendyol/go-kafka-partition-scaler/pkg/kafka/message"
	"github.com/Trendyol/go-kafka-partition-scaler/pkg/kafka/sarama"
)

//go:generate mockgen  -source=producer.go -destination=producer_mock.go -package=kafka
type Producer interface {
	ProduceAsync(ctx context.Context, message *message.ProducerMessage) error
	ProduceSync(ctx context.Context, message *message.ProducerMessage) error
	ProduceSyncBulk(ctx context.Context, messages []*message.ProducerMessage, size int) error
}

func NewProducer(clusterConfig *config.ClusterConfig) (Producer, error) {
	// we can implement another library, segment io ....
	return sarama.NewProducer(clusterConfig)
}

var (
	WaitForLocal    = config.WaitForLocal
	CompressionNone = config.CompressionNone
)