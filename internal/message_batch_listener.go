package internal

import (
	"context"
	"sync"
	"time"
)

type batchMessageListener struct {
	*baseMessageVirtualListener
	waitGroup                    *sync.WaitGroup
	processBufferedMessageTicker *time.Ticker
	processMutex                 *sync.Mutex
	processMessages              []*ConsumerMessage
}

func newBatchMessageListener(
	topic string,
	partition int32,
	virtualPartition int,
	initializedContext ConsumerGroupInitializeContext,
	processedMessageListener ProcessedMessageListener,
) *batchMessageListener {
	messageListener := &batchMessageListener{
		baseMessageVirtualListener: newBaseMessageVirtualListener(
			topic,
			partition,
			virtualPartition,
			initializedContext,
			processedMessageListener,
		),
		waitGroup:                    &sync.WaitGroup{},
		processMessages:              []*ConsumerMessage{},
		processBufferedMessageTicker: time.NewTicker(initializedContext.ConsumerGroupConfig.ConsumeBatchListenerLatency),
		processMutex:                 &sync.Mutex{},
	}
	go func() {
		messageListener.listen()
	}()
	go func() {
		messageListener.tickProcessBufferedMessagesFunc()
	}()
	return messageListener
}

func (listener *batchMessageListener) Publish(message *ConsumerMessage) {
	listener.messageChan <- message
}

func (listener *batchMessageListener) Close() {
	listener.closeOnce.Do(func() {
		listener.waitGroup.Wait()
		listener.messageListenerClosed = true
		close(listener.messageChan)
		close(listener.processableMessageChan)
		listener.processMessages = []*ConsumerMessage{}
		listener.processBufferedMessageTicker.Stop()
	})
}

func (listener *batchMessageListener) tickProcessBufferedMessagesFunc() {
	for range listener.processBufferedMessageTicker.C {
		listener.processMutex.Lock()
		if len(listener.processMessages) == 0 {
			listener.processMutex.Unlock()
			continue
		}
		listener.processBufferedMessages()
		listener.processMutex.Unlock()
	}
}

func (listener *batchMessageListener) listen() {
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
}

func (listener *batchMessageListener) processBufferedMessages() {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc() // when processableMessageChan closed, cancel context

	listener.waitGroup.Add(1)
	done := make(chan bool)
	go func() {
		defer listener.waitGroup.Done()
		tracerFunctions := make([]EndFunc, 0, len(listener.initializedContext.Tracers))
		for _, message := range listener.processMessages {
			var endFunc EndFunc
			ctx, endFunc = listener.Trace(ctx, message)
			tracerFunctions = append(tracerFunctions, endFunc)
			ctx = listener.Intercept(ctx, message)
		}

		errorResults := processMessages(ctx, listener.initializedContext.Consumers.BatchConsumer, listener.processMessages, listener.initializedContext.ConsumerGroupConfig.MaxProcessingTime)
		if len(errorResults) > 0 {
			for message, err := range errorResults {
				processConsumedMessageError(ctx, message, err, listener.initializedContext)
			}
		}

		for _, tracerFunc := range tracerFunctions {
			tracerFunc()
		}
		for _, message := range listener.processMessages {
			listener.processedMessageListener.Publish(message)
		}
		done <- true
	}()
	select {
	case <-done:
		break
	case <-listener.processableMessageChan:
		break
	}
	listener.processMessages = []*ConsumerMessage{}
}

func (listener *batchMessageListener) isClosed() bool {
	return listener.messageListenerClosed || listener.processedMessageListener.IsChannelClosed()
}
