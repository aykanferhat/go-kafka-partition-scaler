package internal

import "github.com/Trendyol/go-kafka-partition-scaler/pkg/kafka"

type Admin = kafka.Admin

func NewAdmin(config *ClusterConfig) Admin {
	clusterConfig := mapToClusterConfig(config)
	return kafka.NewAdmin(clusterConfig)
}
