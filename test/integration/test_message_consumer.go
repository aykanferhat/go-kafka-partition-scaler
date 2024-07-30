package integration

import (
	"context"

	partitionscaler "github.com/aykanferhat/go-kafka-partition-scaler"

	"github.com/aykanferhat/go-kafka-partition-scaler/pkg/json"
	"github.com/aykanferhat/go-kafka-partition-scaler/test/testdata"
)

type testMessageConsumer struct {
	consumedMessageChan chan *partitionscaler.ConsumerMessage
}

func newTestMessageConsumer(consumedMessageChan chan *partitionscaler.ConsumerMessage) partitionscaler.Consumer {
	return &testMessageConsumer{
		consumedMessageChan: consumedMessageChan,
	}
}

func (consumer *testMessageConsumer) Consume(_ context.Context, message *partitionscaler.ConsumerMessage) error {
	consumer.consumedMessageChan <- message
	var consumedMessage *testdata.TestConsumedMessage
	if err := json.Unmarshal(message.Value, &consumedMessage); err != nil {
		return err
	}
	return nil
}
