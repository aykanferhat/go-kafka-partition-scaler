package message

func SliceMessages(producerMessages []*ProducerMessage, batchSize int) [][]*ProducerMessage {
	if batchSize <= 0 {
		return nil
	}
	batches := make([][]*ProducerMessage, 0, (len(producerMessages)+batchSize-1)/batchSize)
	for batchSize < len(producerMessages) {
		producerMessages, batches = producerMessages[batchSize:], append(batches, producerMessages[0:batchSize:batchSize])
	}
	return append(batches, producerMessages)
}
