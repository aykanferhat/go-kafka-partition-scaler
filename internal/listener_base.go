package internal

import (
	"context"
	"sync"

	"github.com/aykanferhat/go-kafka-partition-scaler/common"
)

type baseMessageVirtualListener struct {
	processedMessageListener ProcessedMessageListener
	messageChan              chan *ConsumerMessage
	processableMessageChan   chan bool
	closeOnce                *sync.Once
	topic                    string
	initializedContext       ConsumerGroupInitializeContext
	virtualPartition         int
	partition                int32
	stopped                  bool
}

func newBaseMessageVirtualListener(
	topic string,
	partition int32,
	virtualPartition int,
	initializedContext ConsumerGroupInitializeContext,
	processedMessageListener ProcessedMessageListener,
) *baseMessageVirtualListener {
	return &baseMessageVirtualListener{
		topic:                    topic,
		partition:                partition,
		virtualPartition:         virtualPartition,
		initializedContext:       initializedContext,
		processedMessageListener: processedMessageListener,
		messageChan:              make(chan *ConsumerMessage, initializedContext.ConsumerGroupConfig.VirtualPartitionChanCount),
		processableMessageChan:   make(chan bool),
		closeOnce:                &sync.Once{},
	}
}

func (listener *baseMessageVirtualListener) Intercept(ctx context.Context, message *ConsumerMessage) context.Context {
	for _, interceptor := range listener.initializedContext.ConsumerInterceptors {
		ctx = interceptor.OnConsume(ctx, message)
	}
	return ctx
}

func (listener *baseMessageVirtualListener) Trace(ctx context.Context, message *ConsumerMessage) (context.Context, func()) {
	tracerFunctions := make([]EndFunc, 0, len(listener.initializedContext.Tracers))
	ctx = context.WithValue(ctx, common.GroupID, message.GroupID)
	ctx = context.WithValue(ctx, common.Topic, message.Topic)
	ctx = context.WithValue(ctx, common.Partition, message.Partition)
	ctx = context.WithValue(ctx, common.MessageConsumedTimestamp, message.Timestamp)
	for _, tr := range listener.initializedContext.Tracers {
		var deferFunc EndFunc
		ctx, deferFunc = tr.Start(ctx, message.Tracer)
		tracerFunctions = append(tracerFunctions, deferFunc)
	}
	return ctx, func() {
		for _, endFunc := range tracerFunctions {
			endFunc()
		}
	}
}

func (listener *baseMessageVirtualListener) EndTracer(endTracerFunc []EndFunc) {
	for _, tracerFunc := range endTracerFunc {
		tracerFunc()
	}
}
