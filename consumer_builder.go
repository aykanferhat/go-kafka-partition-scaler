package partitionscaler

import (
	"context"

	"github.com/aykanferhat/go-kafka-partition-scaler/internal"
)

type (
	Consumer                     = internal.Consumer
	ConsumerGroupConfig          = internal.ConsumerGroupConfig
	ConsumerGroupConfigMap       = internal.ConsumerGroupConfigMap
	ConsumerGroupErrorConfig     = internal.ConsumerGroupErrorConfig
	ConsumerGroupErrorConfigMap  = internal.ConsumerGroupErrorConfigMap
	ConsumerGroup                = internal.ConsumerGroup
	BatchConsumer                = internal.BatchConsumer
	ConsumerMessage              = internal.ConsumerMessage
	ConsumerGroupConsumers       = internal.ConsumerGroupConsumers
	ConsumerGroupErrorConsumers  = internal.ConsumerGroupErrorConsumers
	ErrorConsumerGroup           = internal.ErrorConsumerGroup
	ConsumerErrorInterceptor     = internal.ConsumerErrorInterceptor
	ConsumerInterceptor          = internal.ConsumerInterceptor
	TopicPartitionStatus         = internal.ConsumerGroupStatus
	TopicPartitionStatusListener = internal.ConsumerGroupStatusListener
)

type ConsumerBuilder struct {
	consumerErrorInterceptor ConsumerErrorInterceptor
	clusterConfigMap         ClusterConfigMap
	consumerConfigMap        ConsumerGroupConfigMap
	lastStepFunc             func(context.Context, *ConsumerMessage, error)
	consumersList            []*ConsumerGroupConsumers
	consumerInterceptors     []ConsumerInterceptor
	tracers                  []Tracer
}

func NewConsumerBuilder(
	clusterConfigMap ClusterConfigMap,
	consumerConfigMap ConsumerGroupConfigMap,
	consumersList []*ConsumerGroupConsumers,
) *ConsumerBuilder {
	return &ConsumerBuilder{
		clusterConfigMap:     clusterConfigMap,
		consumerConfigMap:    consumerConfigMap,
		consumersList:        consumersList,
		consumerInterceptors: []ConsumerInterceptor{},
		tracers:              []Tracer{},
		lastStepFunc: func(context.Context, *ConsumerMessage, error) {
			// default empty
		},
	}
}

func (c *ConsumerBuilder) Interceptors(consumerInterceptors []ConsumerInterceptor) *ConsumerBuilder {
	c.consumerInterceptors = append(c.consumerInterceptors, consumerInterceptors...)
	return c
}

func (c *ConsumerBuilder) Interceptor(consumerInterceptor ConsumerInterceptor) *ConsumerBuilder {
	c.consumerInterceptors = append(c.consumerInterceptors, consumerInterceptor)
	return c
}

func (c *ConsumerBuilder) Tracers(tracer []Tracer) *ConsumerBuilder {
	c.tracers = append(c.tracers, tracer...)
	return c
}

func (c *ConsumerBuilder) Tracer(tracer Tracer) *ConsumerBuilder {
	c.tracers = append(c.tracers, tracer)
	return c
}

func (c *ConsumerBuilder) ErrorInterceptor(consumerErrorInterceptor ConsumerErrorInterceptor) *ConsumerBuilder {
	c.consumerErrorInterceptor = consumerErrorInterceptor
	return c
}

func (c *ConsumerBuilder) LastStepFunc(lastStepFunc func(context.Context, *ConsumerMessage, error)) *ConsumerBuilder {
	c.lastStepFunc = lastStepFunc
	return c
}

func (c *ConsumerBuilder) Log(l Log) *ConsumerBuilder {
	internal.SetLog(l)
	return c
}

func (c *ConsumerBuilder) Initialize(ctx context.Context) (map[string]ConsumerGroup, map[string]ErrorConsumerGroup, error) {
	producers, err := NewProducerBuilder(c.clusterConfigMap).Initialize()
	if err != nil {
		return nil, nil, err
	}
	clusterConsumerConfigConsumersMap := make(map[string]map[*ConsumerGroupConfig]*ConsumerGroupConsumers)
	consumerGroupMap := make(map[string]ConsumerGroup)
	for _, consumers := range c.consumersList {
		consumerGroupConfig, err := c.consumerConfigMap.GetConfigWithDefault(consumers.ConfigName)
		if err != nil {
			return nil, nil, err
		}
		clusterName := consumerGroupConfig.Cluster
		clusterConfig, err := c.clusterConfigMap.GetConfigWithDefault(clusterName)
		if err != nil {
			return nil, nil, err
		}
		producer, err := producers.GetProducer(clusterName)
		if err != nil {
			return nil, nil, err
		}
		c.consumerInterceptors = append(c.consumerInterceptors, consumers.ConsumerInterceptors...)
		consumerGroupInitializedContext := internal.ConsumerGroupInitializeContext{
			ClusterConfig:        clusterConfig,
			ConsumerGroupConfig:  consumerGroupConfig,
			Producer:             producer,
			Consumers:            consumers,
			LastStep:             c.lastStepFunc,
			ConsumerInterceptors: c.consumerInterceptors,
		}
		consumerGroup := internal.NewConsumerGroup(consumerGroupInitializedContext)
		if err := consumerGroup.Subscribe(ctx); err != nil {
			return nil, nil, err
		}
		if _, exist := clusterConsumerConfigConsumersMap[clusterName]; !exist {
			clusterConsumerConfigConsumersMap[clusterName] = make(map[*ConsumerGroupConfig]*ConsumerGroupConsumers)
		}
		clusterConsumerConfigConsumersMap[clusterName][consumerGroupConfig] = consumers
		consumerGroupMap[consumerGroup.GetGroupID()] = consumerGroup
	}
	errorConsumerGroupMap := make(map[string]ErrorConsumerGroup)
	for clusterName := range c.clusterConfigMap {
		clusterConfig, err := c.clusterConfigMap.GetConfigWithDefault(clusterName)
		if err != nil {
			return nil, nil, err
		}
		if clusterConfig.ErrorConfig == nil {
			continue
		}
		consumerConfigConsumersMap, exists := clusterConsumerConfigConsumersMap[clusterName]
		if !exists {
			continue
		}
		producer, err := producers.GetProducer(clusterName)
		if err != nil {
			return nil, nil, err
		}
		errorTopicConsumerMap := make(map[string]Consumer)
		var topics []string
		for c, consumers := range consumerConfigConsumersMap {
			if c.IsDisabledErrorConsumer() {
				continue
			}
			if consumers.ErrorConsumer != nil {
				errorTopicConsumerMap[c.Error] = consumers.ErrorConsumer
			} else {
				errorTopicConsumerMap[c.Error] = internal.NewDefaultErrorConsumer(producer)
			}
			topics = append(topics, c.Error)
		}
		consumerGroupErrorConfig := &ConsumerGroupErrorConfig{
			GroupID:                           clusterConfig.ErrorConfig.GroupID,
			Topics:                            topics,
			Cron:                              clusterConfig.ErrorConfig.Cron,
			MaxErrorCount:                     clusterConfig.ErrorConfig.MaxErrorCount,
			CloseConsumerWhenThereIsNoMessage: clusterConfig.ErrorConfig.CloseConsumerWhenThereIsNoMessage,
			CloseConsumerWhenMessageIsNew:     clusterConfig.ErrorConfig.CloseConsumerWhenMessageIsNew,
			Cluster:                           clusterName,
			MaxProcessingTime:                 clusterConfig.ErrorConfig.MaxProcessingTime,
			Tracer:                            clusterConfig.ErrorConfig.Tracer,
			FetchMaxBytes:                     clusterConfig.ErrorConfig.FetchMaxBytes,
			OffsetInitial:                     clusterConfig.ErrorConfig.OffsetInitial,
			SessionTimeout:                    clusterConfig.ErrorConfig.SessionTimeout,
			RebalanceTimeout:                  clusterConfig.ErrorConfig.RebalanceTimeout,
			HeartbeatInterval:                 clusterConfig.ErrorConfig.HeartbeatInterval,
		}
		errorConsumer, err := internal.NewErrorConsumerGroup(clusterConfig, consumerGroupErrorConfig, errorTopicConsumerMap, c.lastStepFunc, c.tracers)
		if err != nil {
			return nil, nil, err
		}
		if err := errorConsumer.ScheduleToSubscribe(ctx); err != nil {
			return nil, nil, err
		}
		errorConsumerGroupMap[clusterConfig.ErrorConfig.GroupID] = errorConsumer
	}
	return consumerGroupMap, errorConsumerGroupMap, nil
}
