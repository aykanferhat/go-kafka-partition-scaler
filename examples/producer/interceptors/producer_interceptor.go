package interceptors

import (
	"context"

	partitionscaler "github.com/aykanferhat/go-kafka-partition-scaler"
)

type producerInterceptor struct{}

func NewProducerInterceptor() partitionscaler.ProducerInterceptor {
	return &producerInterceptor{}
}

func (p producerInterceptor) OnProduce(context.Context, *partitionscaler.ProducerMessage) {
	// do nothing
}
