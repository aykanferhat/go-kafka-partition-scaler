package consumers

import (
	"context"

	partitionscaler "github.com/Trendyol/go-kafka-partition-scaler"
)

type virtualSingleMessageConsumer struct{}

func NewVirtualSingleMessageConsumer() partitionscaler.Consumer {
	return &virtualSingleMessageConsumer{}
}

func (consumer *virtualSingleMessageConsumer) Consume(context.Context, *partitionscaler.ConsumerMessage) error {
	return nil
}
