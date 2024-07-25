package integration

import (
	"context"
	partitionscaler "github.com/Trendyol/go-kafka-partition-scaler"
)

type testConsumerErrorInterceptor struct{}

func NewTestConsumerErrorInterceptor() partitionscaler.ConsumerErrorInterceptor {
	return &testConsumerErrorInterceptor{}
}

func (d *testConsumerErrorInterceptor) OnError(context.Context, *partitionscaler.ConsumerMessage, error) {
	// do nothing
}
