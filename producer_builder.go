package partitionscaler

import (
	"github.com/aykanferhat/go-kafka-partition-scaler/internal"
)

type ProducerBuilder struct {
	clusterConfigMap ClusterConfigMap
	topicConfigMap   ProducerTopicConfigMap
	interceptors     []ProducerInterceptor
}

func NewProducerBuilder(clusterConfigMap ClusterConfigMap) *ProducerBuilder {
	return NewProducerBuilderWithConfig(clusterConfigMap, make(ProducerTopicConfigMap))
}

func NewProducerBuilderWithConfig(clusterConfigMap ClusterConfigMap, topicConfigMap ProducerTopicConfigMap) *ProducerBuilder {
	return &ProducerBuilder{
		clusterConfigMap: clusterConfigMap,
		interceptors:     make([]ProducerInterceptor, 0),
		topicConfigMap:   topicConfigMap,
	}
}

func (p *ProducerBuilder) Log(l Log) *ProducerBuilder {
	internal.SetLog(l)
	return p
}

func (p *ProducerBuilder) Interceptors(producerInterceptors []ProducerInterceptor) *ProducerBuilder {
	p.interceptors = append(p.interceptors, producerInterceptors...)
	return p
}

func (p *ProducerBuilder) Interceptor(producerInterceptor ProducerInterceptor) *ProducerBuilder {
	p.interceptors = append(p.interceptors, producerInterceptor)
	return p
}

func (p *ProducerBuilder) Initialize() (Producer, error) {
	return internal.NewProducer(p.clusterConfigMap, p.topicConfigMap, p.interceptors)
}
