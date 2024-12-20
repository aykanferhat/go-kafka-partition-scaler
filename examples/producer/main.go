package main

import (
	"context"

	partitionscaler "github.com/aykanferhat/go-kafka-partition-scaler"
	"github.com/aykanferhat/go-kafka-partition-scaler/examples/producer/interceptors"
	"github.com/aykanferhat/go-kafka-partition-scaler/examples/producer/log"
	"github.com/aykanferhat/go-kafka-partition-scaler/examples/producer/model"
)

const (
	clusterConfigPath  = "resources/kafka-cluster-config.yaml"
	producerConfigPath = "resources/producer-topic-config.yaml"
)

func main() {
	clusterConfigMap, err := partitionscaler.ReadKafkaClusterConfig(clusterConfigPath)
	if err != nil {
		panic(err)
	}

	producerTopicConfigMap, err := partitionscaler.ReadKafkaProducerTopicConfig(producerConfigPath)
	if err != nil {
		panic(err)
	}

	producerInterceptor := interceptors.NewProducerInterceptor() // optional

	logger := log.NewLogger() // optional, you can set logger, if you don't want console default logger when level is debug.

	producer, err := partitionscaler.NewProducerBuilderWithConfig(clusterConfigMap, producerTopicConfigMap).
		Log(logger).
		Interceptor(producerInterceptor).
		Initialize()
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	event := &model.Event{
		Id:        1,
		EventType: "updated",
	}

	if err := producer.ProduceSync(ctx, event); err != nil {
		panic(err)
	}
}
