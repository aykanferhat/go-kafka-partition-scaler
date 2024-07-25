package partitionscaler

import (
	"context"

	"github.com/Trendyol/go-kafka-partition-scaler/internal"
)

type ConsumerErrorBuilder struct {
	clusterConfigMap         ClusterConfigMap
	consumerConfigMap        ConsumerGroupErrorConfigMap
	consumersList            []*ConsumerGroupErrorConsumers
	tracers                  []Tracer
	consumerErrorInterceptor ConsumerErrorInterceptor
	lastStepFunc             func(context.Context, *ConsumerMessage, error)
}

func NewErrorConsumerBuilder(
	clusterConfigMap ClusterConfigMap,
	consumerConfigMap ConsumerGroupErrorConfigMap,
) *ConsumerErrorBuilder {
	return &ConsumerErrorBuilder{
		clusterConfigMap:  clusterConfigMap,
		consumerConfigMap: consumerConfigMap,
		tracers:           []Tracer{},
		lastStepFunc: func(context.Context, *ConsumerMessage, error) {
			// default empty
		},
	}
}

func (c *ConsumerErrorBuilder) Consumers(consumersList []*ConsumerGroupErrorConsumers) *ConsumerErrorBuilder {
	c.consumersList = consumersList
	return c
}

func (c *ConsumerErrorBuilder) Tracers(tracer []Tracer) *ConsumerErrorBuilder {
	c.tracers = append(c.tracers, tracer...)
	return c
}

func (c *ConsumerErrorBuilder) Tracer(tracer Tracer) *ConsumerErrorBuilder {
	c.tracers = append(c.tracers, tracer)
	return c
}

func (c *ConsumerErrorBuilder) ErrorInterceptor(consumerErrorInterceptor ConsumerErrorInterceptor) *ConsumerErrorBuilder {
	c.consumerErrorInterceptor = consumerErrorInterceptor
	return c
}

func (c *ConsumerErrorBuilder) LastStepFunc(lastStepFunc func(context.Context, *ConsumerMessage, error)) *ConsumerErrorBuilder {
	c.lastStepFunc = lastStepFunc
	return c
}

func (c *ConsumerErrorBuilder) Log(l Log) *ConsumerErrorBuilder {
	internal.SetLog(l)
	return c
}

func (c *ConsumerErrorBuilder) Initialize() (map[string]ErrorConsumerGroup, error) {
	producers, err := NewProducerBuilder(c.clusterConfigMap).Initialize()
	if err != nil {
		return nil, err
	}
	errorConsumerGroupMap := make(map[string]ErrorConsumerGroup)
	consumersMap := make(map[string]*ConsumerGroupErrorConsumers)
	for _, consumers := range c.consumersList {
		consumersMap[consumers.ConfigName] = consumers
	}
	for config := range c.consumerConfigMap {
		consumerGroupErrorConfig, err := c.consumerConfigMap.GetConfigWithDefault(config)
		if err != nil {
			return nil, err
		}
		clusterName := consumerGroupErrorConfig.Cluster
		clusterConfig, err := c.clusterConfigMap.GetConfigWithDefault(clusterName)
		if err != nil {
			return nil, err
		}
		producer, err := producers.GetProducer(clusterName)
		if err != nil {
			return nil, err
		}
		consumerGroupConsumers := consumersMap[config]
		errorTopicConsumerMap := make(map[string]Consumer)
		for _, topic := range consumerGroupErrorConfig.Topics {
			if consumerGroupConsumers != nil {
				errorTopicConsumerMap[topic] = consumerGroupConsumers.ErrorConsumer
			} else {
				errorTopicConsumerMap[topic] = internal.NewDefaultShovelConsumer(producer, consumerGroupErrorConfig.TargetTopic)
			}
		}
		consumerGroup, err := internal.NewErrorConsumerGroup(clusterConfig, consumerGroupErrorConfig, errorTopicConsumerMap, c.lastStepFunc, c.tracers)
		if err != nil {
			return nil, err
		}
		if err := consumerGroup.ScheduleToSubscribe(); err != nil {
			return nil, err
		}
		errorConsumerGroupMap[consumerGroupErrorConfig.GroupID] = consumerGroup
	}
	return errorConsumerGroupMap, nil
}
