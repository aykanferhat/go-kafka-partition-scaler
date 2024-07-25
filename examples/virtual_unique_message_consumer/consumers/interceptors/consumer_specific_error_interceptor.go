package interceptors

import (
	"context"
	partitionscaler "github.com/Trendyol/go-kafka-partition-scaler"
)

// optional, we will set specific consumer error interceptor

type consumerSpecificErrorInterceptor struct{}

func NewConsumerSpecificErrorInterceptor() partitionscaler.ConsumerErrorInterceptor {
	return &consumerSpecificErrorInterceptor{}
}

func (d *consumerSpecificErrorInterceptor) OnError(context.Context, *partitionscaler.ConsumerMessage, error) {
	// do nothing
}
