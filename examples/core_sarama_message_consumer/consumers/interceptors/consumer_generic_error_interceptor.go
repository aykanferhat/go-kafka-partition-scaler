package interceptors

import (
	"context"

	partitionscaler "github.com/Trendyol/go-kafka-partition-scaler"
)

// optional, we will set for all consumer

type consumerGenericErrorInterceptor struct{}

func NewConsumerGenericErrorInterceptor() partitionscaler.ConsumerErrorInterceptor {
	return &consumerGenericErrorInterceptor{}
}

func (d *consumerGenericErrorInterceptor) OnError(context.Context, *partitionscaler.ConsumerMessage, error) {
	// do nothing
}
