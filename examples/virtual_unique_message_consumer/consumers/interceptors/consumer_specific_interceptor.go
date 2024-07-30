package interceptors

import (
	"context"

	partitionscaler "github.com/aykanferhat/go-kafka-partition-scaler"
)

// optional, we will set specific consumer interceptor

type consumerSpecificInterceptor struct{}

func NewConsumerSpecificInterceptor() partitionscaler.ConsumerInterceptor {
	return &consumerSpecificInterceptor{}
}

func (c consumerSpecificInterceptor) OnConsume(ctx context.Context, _ *partitionscaler.ConsumerMessage) context.Context {
	return ctx
}
