package internal

type MessageVirtualListener interface {
	Publish(*ConsumerMessage)
	Close()
}

func NewMessageVirtualListener(
	topic string,
	partition int32,
	virtualPartition int,
	initializedContext ConsumerGroupInitializeContext,
	processedMessageListener ProcessedMessageListener,
) MessageVirtualListener {
	if initializedContext.ConsumerGroupConfig.BatchSize > 1 {
		if initializedContext.ConsumerGroupConfig.UniqueListener {
			return newUniqueMessageListener(topic, partition, virtualPartition, initializedContext, processedMessageListener)
		}
		return newBatchMessageListener(topic, partition, virtualPartition, initializedContext, processedMessageListener)
	}
	return newSingleMessageListener(topic, partition, virtualPartition, initializedContext, processedMessageListener)
}
