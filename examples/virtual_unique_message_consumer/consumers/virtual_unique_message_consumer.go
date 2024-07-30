package consumers

import (
	"context"

	partitionscaler "github.com/aykanferhat/go-kafka-partition-scaler"
)

type virtualUniqueMessageConsumer struct{}

func NewVirtualUniqueMessageConsumer() partitionscaler.Consumer {
	return &virtualUniqueMessageConsumer{}
}

func (consumer *virtualUniqueMessageConsumer) Consume(context.Context, *partitionscaler.ConsumerMessage) error {
	return nil
}
