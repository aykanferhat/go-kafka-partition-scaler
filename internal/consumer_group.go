package internal

import (
	"context"
	"sync"

	"github.com/aykanferhat/go-kafka-partition-scaler/pkg/csmap"

	"github.com/aykanferhat/go-kafka-partition-scaler/common"
	"github.com/aykanferhat/go-kafka-partition-scaler/pkg/kafka"
	"github.com/aykanferhat/go-kafka-partition-scaler/pkg/kafka/handler"
	"github.com/aykanferhat/go-kafka-partition-scaler/pkg/kafka/message"
)

type ConsumerGroup interface {
	GetGroupID() string
	Subscribe(context.Context) error
	Unsubscribe()
	GetLastCommittedOffset(topic string, partition int32) int64
	WaitConsumerStart()
	WaitConsumerStop()
}

type consumerGroup struct {
	cg                          kafka.ConsumerGroup
	processedMessageListeners   *csmap.ConcurrentSwissMap[string, ProcessedMessageListener]
	consumerGroupStatusListener *ConsumerGroupStatusListener
	mutex                       *sync.Mutex
	initializedContext          ConsumerGroupInitializeContext
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
		mutex:                       &sync.Mutex{},
	}
	return consumerGroup
}

func (c *consumerGroup) Handle() handler.MessageHandler {
	tracerName := c.initializedContext.ConsumerGroupConfig.Tracer
	consumer := c.initializedContext.Consumers.Consumer
	return func(topic string, partition int32, messageChan <-chan *message.ConsumerMessage, commitFunc handler.CommitMessageFunc) {
		key := getKey(topic, partition)
		c.consumerGroupStatusListener.Change(key, topic, partition, -1, StartedListening)
		for msg := range messageChan {
			c.consumerGroupStatusListener.Change(key, topic, partition, msg.Offset, ListenedMessage)
			msg.SetAdditionalFields(0, c.initializedContext.ConsumerGroupConfig.GroupID, tracerName)
			ctx := context.Background()
			ctx = c.intercept(ctx, msg)
			endFunc := c.startTracer(ctx, msg)
			if err := processMessage(ctx, consumer, msg, c.initializedContext.ConsumerGroupConfig.MaxProcessingTime); err != nil {
				processConsumedMessageError(ctx, msg, err, c.initializedContext)
			}
			endFunc()
			commitFunc(msg.Topic, msg.Partition, msg.Offset)
		}
	}
}

func (c *consumerGroup) HandleVirtual() handler.MessageHandler {
	tracerName := c.initializedContext.ConsumerGroupConfig.Tracer
	return func(topic string, partition int32, messageChan <-chan *message.ConsumerMessage, commitFunc kafka.CommitMessageFunc) {
		processedMessageListener := NewProcessedMessageListener(topic, partition, c.initializedContext.ConsumerGroupConfig.VirtualPartitionCount, commitFunc)
		messageVirtualListeners := make(map[int]MessageVirtualListener)
		for virtualPartition := 0; virtualPartition < c.initializedContext.ConsumerGroupConfig.VirtualPartitionCount; virtualPartition++ {
			messageVirtualListener := NewMessageVirtualListener(topic, partition, virtualPartition, c.initializedContext, processedMessageListener)
			messageVirtualListeners[virtualPartition] = messageVirtualListener
		}
		key := getKey(topic, partition)
		c.processedMessageListeners.Store(key, processedMessageListener)
		c.consumerGroupStatusListener.Change(key, topic, partition, -1, StartedListening)
		var firstConsumedMessage bool
		for msg := range messageChan {
			if !firstConsumedMessage {
				processedMessageListener.SetFirstConsumedMessage(topic, partition, msg.Offset)
				firstConsumedMessage = true
			}
			c.consumerGroupStatusListener.Change(key, topic, partition, msg.Offset, ListenedMessage)
			virtualPartition := calculateVirtualPartition(string(msg.Key), c.initializedContext.ConsumerGroupConfig.VirtualPartitionCount)
			msg.SetAdditionalFields(virtualPartition, c.initializedContext.ConsumerGroupConfig.GroupID, tracerName)
			messageVirtualListener := messageVirtualListeners[virtualPartition]
			messageVirtualListener.Publish(msg)
		}
		for _, messageVirtualListener := range messageVirtualListeners {
			messageVirtualListener.Close()
		}
		processedMessageListener.Close()
	}
}

func (c *consumerGroup) GetGroupID() string {
	return c.initializedContext.ConsumerGroupConfig.GroupID
}

func (c *consumerGroup) Subscribe(ctx context.Context) error {
	c.mutex.Lock()
	defer func() {
		c.mutex.Unlock()
	}()
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
		logger,
	)
	if err != nil {
		return err
	}
	consumerDied, err := cg.Subscribe(ctx)
	if err != nil {
		logger.Errorf("consumerGroup Subscribe err: %s", err.Error())
		return err
	}
	go func() {
		for range consumerDied {
			c.Unsubscribe()
			break
		}
		close(consumerDied)
	}()
	c.consumerGroupStatusListener.listenConsumerStart()
	c.cg = cg
	logger.Infof("consumerGroup Subscribed, groupId: %s", c.GetGroupID())
	return nil
}

func (c *consumerGroup) Unsubscribe() {
	c.mutex.Lock()
	defer func() {
		c.mutex.Unlock()
	}()
	if c.cg == nil {
		return
	}
	if err := c.cg.Unsubscribe(); err != nil {
		logger.Errorf("consumerGroup Unsubscribe err: %s", err.Error())
	}
	c.consumerGroupStatusListener.listenConsumerStop()
	logger.Infof("consumerGroup Unsubscribed, groupId: %s", c.GetGroupID())
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
