package sarama

import (
	"errors"

	"github.com/IBM/sarama"
	"github.com/Trendyol/go-kafka-partition-scaler/pkg/kafka/config"
)

type Admin struct {
	clusterConfig *config.ClusterConfig
}

func NewAdmin(clusterConfig *config.ClusterConfig) *Admin {
	return &Admin{
		clusterConfig: clusterConfig,
	}
}

func (a *Admin) CreateTopic(topicName string, partition int32) error {
	clusterAdmin, err := sarama.NewClusterAdmin(a.clusterConfig.Brokers, nil)
	if err != nil {
		return err
	}
	if err := clusterAdmin.CreateTopic(topicName, &sarama.TopicDetail{NumPartitions: partition, ReplicationFactor: 1}, false); err != nil {
		var topicError *sarama.TopicError
		if errors.As(err, &topicError) && topicError.Err != sarama.ErrTopicAlreadyExists {
			return err
		}
	}
	if err := clusterAdmin.Close(); err != nil {
		return err
	}
	return nil
}
