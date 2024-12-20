package integration

import (
	"context"

	partitionscaler "github.com/aykanferhat/go-kafka-partition-scaler"

	"github.com/aykanferhat/go-kafka-partition-scaler/common"
)

type testProducerInterceptor struct{}

func newTestProducerInterceptor() partitionscaler.ProducerInterceptor {
	return &testProducerInterceptor{}
}

func (d *testProducerInterceptor) OnProduce(ctx context.Context, msg *partitionscaler.ProducerMessage) {
	key := common.ContextKey("key")
	value := ctx.Value(key)
	if value == nil {
		return
	}
	msg.Headers = append(msg.Headers, partitionscaler.Header{
		Key:   common.ToByte(key.String()),
		Value: common.ToByte(value.(string)),
	})
}
