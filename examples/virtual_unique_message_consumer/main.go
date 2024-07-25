package main

import (
	partitionscaler "github.com/Trendyol/go-kafka-partition-scaler"
	"github.com/Trendyol/go-kafka-partition-scaler/examples/virtual_unique_message_consumer/consumers"
	"github.com/Trendyol/go-kafka-partition-scaler/examples/virtual_unique_message_consumer/consumers/interceptors"
	"github.com/Trendyol/go-kafka-partition-scaler/examples/virtual_unique_message_consumer/log"
)

const (
	clusterConfigPath  = "path.../kafka-cluster-config.yaml"
	consumerConfigPath = "path.../consumer-group-config.yaml"
)

func main() {
	clusterConfigMap, err := partitionscaler.ReadKafkaClusterConfigWithProfile(clusterConfigPath, "stage")
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
			ConfigName:               "virtualUniqueMessageConsumer",
			Consumer:                 consumers.NewVirtualUniqueMessageConsumer(),
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
		Initialize()
	if err != nil {
		panic(err)
	}
	println(consumerGroups, errorConsumers)
}
