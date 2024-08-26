package internal

type processedMessage struct {
	Topic     string
	Partition int32
	Offset    int64
}

func newProcessedMessage(message *ConsumerMessage) *processedMessage {
	return &processedMessage{
		Topic:     message.Topic,
		Partition: message.Partition,
		Offset:    message.Offset,
	}
}
