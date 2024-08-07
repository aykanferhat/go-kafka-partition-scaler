package integration

import (
	"context"

	partitionscaler "github.com/aykanferhat/go-kafka-partition-scaler"
)

type testConsumerErrorInterceptor struct{}

func newTestConsumerErrorInterceptor() partitionscaler.ConsumerErrorInterceptor {
	return &testConsumerErrorInterceptor{}
}

func (d *testConsumerErrorInterceptor) OnError(context.Context, *partitionscaler.ConsumerMessage, error) {
	// do nothing
}
