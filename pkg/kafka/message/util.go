package message

func SliceMessages(producerMessages []*ProducerMessage, arraySize int) [][]*ProducerMessage {
	tempList := make([][]*ProducerMessage, 0)
	if len(producerMessages) == arraySize {
		return append(tempList, producerMessages)
	}
	mod := len(producerMessages) % arraySize
	for i := 0; i <= len(producerMessages)-arraySize; i += arraySize {
		arrayPart := producerMessages[i : i+arraySize]
		tempList = append(tempList, arrayPart)
	}
	last := len(producerMessages) - mod
	arrayPart := producerMessages[last:]
	if len(arrayPart) != 0 {
		tempList = append(tempList, arrayPart)
	}
	return tempList
}
