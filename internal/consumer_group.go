package internal

import (
	"context"
	"time"

	"github.com/Trendyol/go-kafka-partition-scaler/pkg/csmap"

	"github.com/Trendyol/go-kafka-partition-scaler/common"
	"github.com/Trendyol/go-kafka-partition-scaler/pkg/kafka/handler"
	"github.com/Trendyol/go-kafka-partition-scaler/pkg/kafka/message"
	"github.com/Trendyol/go-kafka-partition-scaler/pkg/log"

	"github.com/Trendyol/go-kafka-partition-scaler/pkg/kafka"
)

type ConsumerGroup interface {
	GetGroupID() string
	Subscribe() error
	Unsubscribe()
	GetLastCommittedOffset(topic string, partition int32) int64
	WaitConsumerStart()
	WaitConsumerStop()
}

type consumerGroup struct {
	cg                        kafka.ConsumerGroup
	processedMessageListeners *csmap.ConcurrentSwissMap[string, ProcessedMessageListener]
	initializedContext        ConsumerGroupInitializeContext

	consumerGroupStatusListener *ConsumerGroupStatusListener
}

type ConsumerGroupInitializeContext struct {
	ClusterConfig             *ClusterConfig
	ConsumerGroupConfig       *ConsumerGroupConfig
	Producer                  kafka.Producer
	Consumers                 *ConsumerGroupConsumers
	LastStep                  func(context.Context, *ConsumerMessage, error)
	ConsumerInterceptors      []ConsumerInterceptor
	ConsumerErrorInterceptors []ConsumerErrorInterceptor
	Tracers                   []Tracer
}

func NewConsumerGroup(
	initializedContext ConsumerGroupInitializeContext,
) ConsumerGroup {
	consumerGroup := &consumerGroup{
		initializedContext:          initializedContext,
		processedMessageListeners:   csmap.Create[string, ProcessedMessageListener](0),
		consumerGroupStatusListener: newConsumerGroupStatusListener(),
	}
	return consumerGroup
}

func (c *consumerGroup) Handle() handler.MessageHandler {
	tracerName := c.initializedContext.ConsumerGroupConfig.Tracer
	consumer := c.initializedContext.Consumers.Consumer
	return func(topic string, partition int32, messageChan <-chan *message.ConsumerMessage, commitFunc handler.CommitMessageFunc) {
		c.consumerGroupStatusListener.Listen(&ConsumerGroupStatus{Topic: topic, Partition: partition, Status: StartedListening, Time: time.Now()})
		for msg := range messageChan {
			c.consumerGroupStatusListener.Listen(&ConsumerGroupStatus{Topic: topic, Partition: partition, Status: ListenedMessage, Time: time.Now(), Offset: msg.Offset})
			consumerMessage := &ConsumerMessage{ConsumerMessage: msg, VirtualPartition: 0, GroupID: c.initializedContext.ConsumerGroupConfig.GroupID, Tracer: tracerName}
			ctx := context.Background()
			ctx = c.intercept(ctx, consumerMessage)
			endFunc := c.startTracer(ctx, consumerMessage)
			if err := processMessage(ctx, consumer, consumerMessage, c.initializedContext.ConsumerGroupConfig.MaxProcessingTime); err != nil {
				processConsumedMessageError(ctx, consumerMessage, err, c.initializedContext)
			}
			endFunc()
			commitFunc(msg.Topic, msg.Partition, msg.Offset)
		}
	}
}

func (c *consumerGroup) HandleVirtual() handler.MessageHandler {
	tracerName := c.initializedContext.ConsumerGroupConfig.Tracer
	return func(topic string, partition int32, messageChan <-chan *kafka.ConsumerMessage, commitFunc kafka.CommitMessageFunc) {
		processedMessageListener := NewProcessedMessageListener(topic, partition, c.initializedContext.ConsumerGroupConfig.VirtualPartitionCount, commitFunc)
		messageVirtualListeners := csmap.Create[int, MessageVirtualListener](uint64(c.initializedContext.ConsumerGroupConfig.VirtualPartitionCount))
		for virtualPartition := 0; virtualPartition < c.initializedContext.ConsumerGroupConfig.VirtualPartitionCount; virtualPartition++ {
			messageVirtualListener := NewMessageVirtualListener(topic, partition, virtualPartition, c.initializedContext, processedMessageListener)
			messageVirtualListeners.Store(virtualPartition, messageVirtualListener)
		}
		c.processedMessageListeners.Store(getKey(topic, partition), processedMessageListener)
		c.consumerGroupStatusListener.Listen(&ConsumerGroupStatus{Topic: topic, Partition: partition, Status: StartedListening, Time: time.Now()})
		var firstConsumedMessage bool
		for msg := range messageChan {
			if !firstConsumedMessage {
				processedMessageListener.SetFirstConsumedMessage(topic, partition, msg.Offset)
				firstConsumedMessage = true
			}
			c.consumerGroupStatusListener.Listen(&ConsumerGroupStatus{Topic: topic, Partition: partition, Status: ListenedMessage, Time: time.Now(), Offset: msg.Offset})
			virtualPartition := calculateVirtualPartition(string(msg.Key), c.initializedContext.ConsumerGroupConfig.VirtualPartitionCount)
			consumerMessage := &ConsumerMessage{ConsumerMessage: msg, VirtualPartition: virtualPartition, GroupID: c.initializedContext.ConsumerGroupConfig.GroupID, Tracer: tracerName}
			messageVirtualListener, _ := messageVirtualListeners.Load(virtualPartition)
			messageVirtualListener.Publish(consumerMessage)
		}
		messageVirtualListeners.Range(func(_ int, messageVirtualListener MessageVirtualListener) (stop bool) {
			messageVirtualListener.Close()
			return
		})
		processedMessageListener.Close()
	}
}

func (c *consumerGroup) GetGroupID() string {
	return c.initializedContext.ConsumerGroupConfig.GroupID
}

func (c *consumerGroup) Subscribe() error {
	var messageHandler handler.MessageHandler
	if c.initializedContext.ConsumerGroupConfig.VirtualPartitionCount != 0 {
		messageHandler = c.HandleVirtual()
	} else {
		messageHandler = c.Handle()
	}
	cg, err := kafka.NewConsumerGroup(
		mapToClusterConfig(c.initializedContext.ClusterConfig),
		mapToConsumerGroupConfig(c.initializedContext.ConsumerGroupConfig, false),
		messageHandler,
		c.consumerGroupStatusListener.HandleConsumerGroupStatus(),
	)
	if err != nil {
		return err
	}
	if err := cg.Subscribe(); err != nil {
		log.Errorf("consumerGroup Subscribe err: %s", err.Error())
		return err
	}
	c.consumerGroupStatusListener.listenConsumerStart()
	c.cg = cg
	return nil
}

func (c *consumerGroup) Unsubscribe() {
	if c.cg == nil {
		return
	}
	if err := c.cg.Unsubscribe(); err != nil {
		log.Errorf("consumerGroup Unsubscribe err: %s", err.Error())
	}
	c.consumerGroupStatusListener.listenConsumerStop()
}

func (c *consumerGroup) GetLastCommittedOffset(topic string, partition int32) int64 {
	if processedMessageListener, exists := c.processedMessageListeners.Load(getKey(topic, partition)); exists {
		return processedMessageListener.LastCommittedOffset()
	}
	return 0
}

func (c *consumerGroup) WaitConsumerStart() {
	c.consumerGroupStatusListener.WaitConsumerStart()
}

func (c *consumerGroup) WaitConsumerStop() {
	c.consumerGroupStatusListener.WaitConsumerStop()
}

func (c *consumerGroup) intercept(ctx context.Context, message *ConsumerMessage) context.Context {
	for _, interceptor := range c.initializedContext.ConsumerInterceptors {
		ctx = interceptor.OnConsume(ctx, message)
	}
	return ctx
}

func (c *consumerGroup) startTracer(ctx context.Context, message *ConsumerMessage) func() {
	tracerFunctions := make([]EndFunc, 0, len(c.initializedContext.Tracers))
	ctx = context.WithValue(ctx, common.GroupID, message.GroupID)
	ctx = context.WithValue(ctx, common.Topic, message.Topic)
	ctx = context.WithValue(ctx, common.Partition, message.Partition)
	ctx = context.WithValue(ctx, common.MessageConsumedTimestamp, message.Timestamp)

	for _, tr := range c.initializedContext.Tracers {
		var deferFunc EndFunc
		ctx, deferFunc = tr.Start(ctx, message.Tracer)
		tracerFunctions = append(tracerFunctions, deferFunc)
	}
	return func() {
		endTracer(tracerFunctions)
	}
}

func endTracer(endTracerFunc []EndFunc) {
	for _, tracerFunc := range endTracerFunc {
		tracerFunc()
	}
}
