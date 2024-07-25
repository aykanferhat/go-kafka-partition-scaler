package consumers

import (
	"context"
	partitionscaler "github.com/Trendyol/go-kafka-partition-scaler"
)

type virtualBatchMessageConsumer struct{}

func NewVirtualBatchMessageConsumer() partitionscaler.BatchConsumer {
	return &virtualBatchMessageConsumer{}
}

func (consumer *virtualBatchMessageConsumer) Consume(_ context.Context, _ []*partitionscaler.ConsumerMessage) map[*partitionscaler.ConsumerMessage]error {
	return nil
}
