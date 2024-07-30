package integration

import (
	"context"

	partitionscaler "github.com/aykanferhat/go-kafka-partition-scaler"
)

type testErrorConsumerWithChannel struct {
	testErrorMessageChan chan *partitionscaler.ConsumerMessage
}

func newTestErrorConsumerWithChannel(
	testErrorMessageChan chan *partitionscaler.ConsumerMessage,
) partitionscaler.Consumer {
	return &testErrorConsumerWithChannel{
		testErrorMessageChan: testErrorMessageChan,
	}
}

func (consumer *testErrorConsumerWithChannel) Consume(_ context.Context, message *partitionscaler.ConsumerMessage) error {
	consumer.testErrorMessageChan <- message
	return nil
}

type testErrorConsumer struct{}

func newTestErrorConsumer() partitionscaler.Consumer {
	return &testErrorConsumer{}
}

func (consumer *testErrorConsumer) Consume(_ context.Context, _ *partitionscaler.ConsumerMessage) error {
	return nil
}
