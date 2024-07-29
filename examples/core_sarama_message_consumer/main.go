package main

import (
	"context"

	partitionscaler "github.com/Trendyol/go-kafka-partition-scaler"
	"github.com/Trendyol/go-kafka-partition-scaler/examples/core_sarama_message_consumer/consumers"
	"github.com/Trendyol/go-kafka-partition-scaler/examples/core_sarama_message_consumer/consumers/interceptors"
	"github.com/Trendyol/go-kafka-partition-scaler/examples/core_sarama_message_consumer/log"
)

const (
	clusterConfigPath  = "resources/kafka-cluster-config.yaml"
	consumerConfigPath = "resources/consumer-group-config.yaml"
)

func main() {
	ctx := context.Background()

	clusterConfigMap, err := partitionscaler.ReadKafkaClusterConfig(clusterConfigPath)
	if err != nil {
		panic(err)
	}
	consumerConfig, err := partitionscaler.ReadKafkaConsumerGroupConfig(consumerConfigPath)
	if err != nil {
		panic(err)
	}

	consumerSpecificInterceptor := interceptors.NewConsumerSpecificInterceptor()           // optional
	consumerSpecificErrorInterceptor := interceptors.NewConsumerSpecificErrorInterceptor() // optional

	consumersList := []*partitionscaler.ConsumerGroupConsumers{
		{
			ConfigName:               "coreSingleMessageConsumer",
			Consumer:                 consumers.NewCoreSingleMessageConsumer(),
			ConsumerInterceptors:     []partitionscaler.ConsumerInterceptor{consumerSpecificInterceptor},
			ConsumerErrorInterceptor: consumerSpecificErrorInterceptor,
		},
	}

	consumerGenericInterceptor := interceptors.NewConsumerGenericInterceptor()           // optional
	consumerGenericErrorInterceptor := interceptors.NewConsumerGenericErrorInterceptor() // optional

	logger := log.NewLogger() // optional, you can set logger, if you don't want console default logger when level is debug.

	consumerGroups, errorConsumers, err := partitionscaler.NewConsumerBuilder(clusterConfigMap, consumerConfig, consumersList).
		Interceptor(consumerGenericInterceptor).
		ErrorInterceptor(consumerGenericErrorInterceptor).
		Log(logger).
		Initialize(ctx)
	if err != nil {
		panic(err)
	}
	println(consumerGroups, errorConsumers)
}
