package integration

import (
	"context"

	partitionscaler "github.com/Trendyol/go-kafka-partition-scaler"

	"github.com/Trendyol/go-kafka-partition-scaler/pkg/json"
	"github.com/Trendyol/go-kafka-partition-scaler/test/testdata"
)

type testBatchMessageConsumer struct {
	consumedMessagesChan chan []*partitionscaler.ConsumerMessage
}

func newTestBatchMessageConsumer(
	consumedMessagesChan chan []*partitionscaler.ConsumerMessage,
) partitionscaler.BatchConsumer {
	return &testBatchMessageConsumer{
		consumedMessagesChan: consumedMessagesChan,
	}
}

func (consumer *testBatchMessageConsumer) Consume(_ context.Context, messages []*partitionscaler.ConsumerMessage) map[*partitionscaler.ConsumerMessage]error {
	consumer.consumedMessagesChan <- messages
	errors := make(map[*partitionscaler.ConsumerMessage]error)
	for _, message := range messages {
		var consumedMessage *testdata.TestConsumedMessage
		if err := json.Unmarshal(message.Value, &consumedMessage); err != nil {
			errors[message] = err
			continue
		}
	}
	return errors
}
