package integration

import (
	"context"

	partitionscaler "github.com/Trendyol/go-kafka-partition-scaler"

	"github.com/Trendyol/go-kafka-partition-scaler/common"
)

type testConsumerHeaderInterceptor struct{}

func newTestConsumerHeaderInterceptor() partitionscaler.ConsumerInterceptor {
	return &testConsumerHeaderInterceptor{}
}

func (d *testConsumerHeaderInterceptor) OnConsume(ctx context.Context, message *partitionscaler.ConsumerMessage) context.Context {
	for _, header := range message.Headers {
		ctx = common.AddToContext(ctx, partitionscaler.ContextKey(header.Key), string(header.Value))
	}
	return ctx
}
