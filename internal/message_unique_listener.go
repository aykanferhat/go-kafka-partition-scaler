package internal

import (
	"context"
	"sync"
	"time"

	"github.com/aykanferhat/go-kafka-partition-scaler/pkg/uuid"
)

type uniqueMessageListener struct {
	messageListener MessageVirtualListener
	*baseMessageVirtualListener
	processBufferedMessageTicker *time.Ticker
	processMutex                 *sync.Mutex
	processMessages              []*ConsumerMessage
}

func (listener *uniqueMessageListener) Publish(message *ConsumerMessage) {
	listener.messageChan <- message
}

func (listener *uniqueMessageListener) scheduleProcessBufferedMessages() {
	go func() {
		for range listener.processBufferedMessageTicker.C {
			listener.processMutex.Lock()
			if len(listener.processMessages) == 0 {
				listener.processMutex.Unlock()
				continue
			}
			listener.processBufferedMessages()
			listener.processMutex.Unlock()
		}
	}()
}

func (listener *uniqueMessageListener) listen() {
	go func() {
		for message := range listener.messageChan {
			if listener.isClosed() {
				continue
			}
			listener.processMutex.Lock()
			listener.processMessages = append(listener.processMessages, message)
			if listener.initializedContext.ConsumerGroupConfig.BatchSize > len(listener.processMessages) {
				listener.processMutex.Unlock()
				continue
			}
			listener.processBufferedMessageTicker.Reset(listener.initializedContext.ConsumerGroupConfig.ConsumeBatchListenerLatency)
			listener.processBufferedMessages()
			listener.processMutex.Unlock()
		}
	}()
}

func (listener *uniqueMessageListener) processBufferedMessages() {
	if len(listener.processMessages) == 0 {
		return
	}
	uniqueMessages := make(map[string][]*ConsumerMessage)
	for _, message := range listener.processMessages {
		var key string
		if len(message.Key) == 0 {
			key = uuid.GenerateUUID()
		} else {
			key = string(message.Key)
		}
		uniqueMessages[key] = append(uniqueMessages[key], message)
	}
	for _, messages := range uniqueMessages {
		for i := 0; i < len(messages)-1; i++ {
			message := messages[i]
			ctx := context.Background()
			ctx = listener.Intercept(ctx, message)
			_, endFunc := listener.Trace(ctx, message)
			listener.processedMessageListener.Publish(message)
			endFunc()
		}
		listener.messageListener.Publish(messages[len(messages)-1])
	}
	listener.processMessages = make([]*ConsumerMessage, 0)
}

func (listener *uniqueMessageListener) isClosed() bool {
	return listener.messageListenerClosed || listener.processedMessageListener.IsChannelClosed()
}

func (listener *uniqueMessageListener) Close() {
	listener.closeOnce.Do(func() {
		listener.messageListener.Close()
		listener.messageListenerClosed = true
		close(listener.messageChan)
		close(listener.processableMessageChan)
		listener.processBufferedMessageTicker.Stop()
	})
}

func newUniqueMessageListener(
	topic string,
	partition int32,
	virtualPartition int,
	initializedContext ConsumerGroupInitializeContext,
	processedMessageListener ProcessedMessageListener,
) *uniqueMessageListener {
	messageListener := &uniqueMessageListener{
		baseMessageVirtualListener: newBaseMessageVirtualListener(
			topic,
			partition,
			virtualPartition,
			initializedContext,
			processedMessageListener,
		),
		messageListener:              newSingleMessageListener(topic, partition, virtualPartition, initializedContext, processedMessageListener),
		processMessages:              make([]*ConsumerMessage, 0),
		processBufferedMessageTicker: time.NewTicker(initializedContext.ConsumerGroupConfig.ConsumeBatchListenerLatency),
		processMutex:                 &sync.Mutex{},
	}
	messageListener.listen()
	messageListener.scheduleProcessBufferedMessages()
	return messageListener
}
