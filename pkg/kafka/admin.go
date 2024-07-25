package kafka

import (
	"github.com/Trendyol/go-kafka-partition-scaler/pkg/kafka/config"
	"github.com/Trendyol/go-kafka-partition-scaler/pkg/kafka/sarama"
)

type Admin interface {
	CreateTopic(topicName string, partition int32) error
}

func NewAdmin(clusterConfig *config.ClusterConfig) Admin {
	// we can implement another library, segment io ....
	return sarama.NewAdmin(clusterConfig)
}
