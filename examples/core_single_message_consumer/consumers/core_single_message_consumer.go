package consumers

import (
	"context"
	partitionscaler "github.com/Trendyol/go-kafka-partition-scaler"
)

type coreSingleMessageConsumer struct{}

func NewCoreSingleMessageConsumer() partitionscaler.Consumer {
	return &coreSingleMessageConsumer{}
}

func (consumer *coreSingleMessageConsumer) Consume(_ context.Context, _ *partitionscaler.ConsumerMessage) error {
	return nil
}
