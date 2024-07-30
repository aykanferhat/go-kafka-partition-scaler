package internal

import "github.com/aykanferhat/go-kafka-partition-scaler/pkg/kafka"

type Admin = kafka.Admin

func NewAdmin(config *ClusterConfig) Admin {
	clusterConfig := mapToClusterConfig(config)
	return kafka.NewAdmin(clusterConfig)
}
