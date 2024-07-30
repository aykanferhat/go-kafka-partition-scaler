package interceptors

import (
	"context"

	partitionscaler "github.com/aykanferhat/go-kafka-partition-scaler"
)

// optional, we will set for all consumer

type consumerGenericInterceptor struct{}

func NewConsumerGenericInterceptor() partitionscaler.ConsumerInterceptor {
	return &consumerGenericInterceptor{}
}

func (c consumerGenericInterceptor) OnConsume(ctx context.Context, _ *partitionscaler.ConsumerMessage) context.Context {
	return ctx
}
