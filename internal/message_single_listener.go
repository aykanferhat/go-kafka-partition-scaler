package internal

import (
	"context"
	"sync"
	"time"
)

type singleMessageListener struct {
	*baseMessageVirtualListener

	waitGroup *sync.WaitGroup
}

func newSingleMessageListener(
	topic string,
	partition int32,
	virtualPartition int,
	initializedContext ConsumerGroupInitializeContext,
	processedMessageListener ProcessedMessageListener,
) *singleMessageListener {
	listener := &singleMessageListener{
		baseMessageVirtualListener: newBaseMessageVirtualListener(
			topic,
			partition,
			virtualPartition,
			initializedContext,
			processedMessageListener,
		),
		waitGroup: &sync.WaitGroup{},
	}
	go func(ml *singleMessageListener) {
		ml.listen()
	}(listener)
	return listener
}

func (listener *singleMessageListener) Publish(message *ConsumerMessage) {
	listener.messageChan <- message
}

func (listener *singleMessageListener) listen() {
	for message := range listener.messageChan {
		if listener.isStopped() {
			continue
		}
		listener.processMessage(message)
	}
}

func (listener *singleMessageListener) processMessage(message *ConsumerMessage) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // when processableMessageChan closed, cancel context

	listener.waitGroup.Add(1)
	done := make(chan bool)
	go func() {
		defer listener.waitGroup.Done()
		ctx = listener.Intercept(ctx, message)
		ctx, endFunc := listener.Trace(ctx, message)
		if err := processMessage(ctx, listener.initializedContext.Consumers.Consumer, message, listener.initializedContext.ConsumerGroupConfig.MaxProcessingTime); err != nil {
			processConsumedMessageError(ctx, message, err, listener.initializedContext)
		}
		listener.processedMessageListener.Publish(message)
		endFunc()
		done <- true
	}()
	select {
	case <-done:
		break
	case <-listener.processableMessageChan:
		break
	}
}

func (listener *singleMessageListener) Close() {
	listener.closeOnce.Do(func() {
		listener.stopped = true
		time.Sleep(150 * time.Millisecond)

		listener.waitGroup.Wait()

		close(listener.messageChan)
		close(listener.processableMessageChan)
	})
}

func (listener *singleMessageListener) isStopped() bool {
	return listener.stopped || listener.processedMessageListener.IsStopped()
}
