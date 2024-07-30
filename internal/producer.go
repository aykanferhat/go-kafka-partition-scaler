package internal

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aykanferhat/go-kafka-partition-scaler/pkg/kafka"
)

type Producer interface {
	GetProducer(clusterName string) (kafka.Producer, error)
	GetProducerTopic(configName string) (*ProducerTopic, error)
	ProduceAsync(ctx context.Context, message Message) error
	ProduceAsyncBulk(ctx context.Context, messages []Message) error
	ProduceSync(ctx context.Context, message Message) error
	ProduceSyncBulk(ctx context.Context, messages []Message, size int) error
	ProduceCustomSync(ctx context.Context, message *CustomMessage) error
	ProduceCustomSyncBulk(ctx context.Context, messages []*CustomMessage, size int) error
}

type producer struct {
	producerMap            map[string]kafka.Producer
	producerTopicConfigMap ProducerTopicConfigMap
	producerInterceptors   []ProducerInterceptor
}

var kafkaNewProducer = kafka.NewProducer

func NewProducer(
	clusterConfigMap ClusterConfigMap,
	producerTopicConfigMap ProducerTopicConfigMap,
	producerInterceptors []ProducerInterceptor,
) (Producer, error) {
	producerMap := make(map[string]kafka.Producer)
	for cluster := range clusterConfigMap {
		clusterConfig, err := clusterConfigMap.GetConfigWithDefault(cluster)
		if err != nil {
			return nil, err
		}
		producer, err := kafkaNewProducer(mapToClusterConfig(clusterConfig))
		if err != nil {
			return nil, err
		}
		producerMap[cluster] = producer
	}
	return &producer{
		producerMap:            producerMap,
		producerInterceptors:   producerInterceptors,
		producerTopicConfigMap: producerTopicConfigMap,
	}, nil
}

func (c *producer) GetProducer(clusterName string) (kafka.Producer, error) {
	producer, exist := c.producerMap[strings.ToLower(clusterName)]
	if !exist {
		return nil, NewErrWithArgs("kafka async producer not found. cluster name: %s", clusterName)
	}
	return producer, nil
}

func (c *producer) GetProducerTopic(configName string) (*ProducerTopic, error) {
	return c.producerTopicConfigMap.GetConfig(configName)
}

func (c *producer) ProduceAsync(ctx context.Context, message Message) error {
	cluster, produceMessage, err := c.getProduceMessageFromMessage(message)
	if err != nil {
		return err
	}
	producer, err := c.GetProducer(cluster)
	if err != nil {
		return errors.Join(err, fmt.Errorf("produce async err: %s, topic: %s, cluster: %s", err.Error(), produceMessage.Topic, cluster))
	}
	for _, interceptor := range c.producerInterceptors {
		interceptor.OnProduce(ctx, produceMessage)
	}
	if err := producer.ProduceAsync(ctx, produceMessage); err != nil {
		return errors.Join(err, fmt.Errorf("produce async err: %s, topic: %s, cluster: %s", err.Error(), produceMessage.Topic, cluster))
	}
	return nil
}

func (c *producer) ProduceAsyncBulk(ctx context.Context, messages []Message) error {
	for _, msg := range messages {
		if err := c.ProduceAsync(ctx, msg); err != nil {
			return err
		}
	}
	return nil
}

func (c *producer) ProduceSync(ctx context.Context, message Message) error {
	cluster, produceMessage, err := c.getProduceMessageFromMessage(message)
	if err != nil {
		return err
	}
	producer, err := c.GetProducer(cluster)
	if err != nil {
		return errors.Join(err, fmt.Errorf("produce async err: %s, topic: %s, cluster: %s", err.Error(), produceMessage.Topic, cluster))
	}
	for _, interceptor := range c.producerInterceptors {
		interceptor.OnProduce(ctx, produceMessage)
	}
	if err = producer.ProduceSync(ctx, produceMessage); err != nil {
		return errors.Join(err, fmt.Errorf("produce sync err: %s, topic: %s, cluster: %s", err.Error(), produceMessage.Topic, cluster))
	}
	return nil
}

func (c *producer) ProduceSyncBulk(ctx context.Context, messages []Message, size int) error {
	mappedProducerMessages := make(map[string][]*ProducerMessage)
	for _, msg := range messages {
		cluster, produceMessage, err := c.getProduceMessageFromMessage(msg)
		if err != nil {
			return err
		}
		for _, interceptor := range c.producerInterceptors {
			interceptor.OnProduce(ctx, produceMessage)
		}
		mappedProducerMessages[cluster] = append(mappedProducerMessages[cluster], produceMessage)
	}
	for cluster, producerMessages := range mappedProducerMessages {
		producer, err := c.GetProducer(cluster)
		if err != nil {
			return err
		}
		if err := producer.ProduceSyncBulk(ctx, producerMessages, size); err != nil {
			return err
		}
	}
	return nil
}

func (c *producer) ProduceCustomSync(ctx context.Context, message *CustomMessage) error {
	cluster, produceMessage := c.getProduceMessageFromCustomMessage(message)
	producer, err := c.GetProducer(cluster)
	if err != nil {
		return err
	}
	for _, interceptor := range c.producerInterceptors {
		interceptor.OnProduce(ctx, produceMessage)
	}
	if err = producer.ProduceSync(ctx, produceMessage); err != nil {
		return errors.Join(err, fmt.Errorf("produce custom sync err: %s, topic: %s, cluster: %s", err.Error(), produceMessage.Topic, cluster))
	}
	return nil
}

func (c *producer) ProduceCustomSyncBulk(ctx context.Context, messages []*CustomMessage, size int) error {
	mappedProducerMessages := make(map[string][]*ProducerMessage)
	for _, msg := range messages {
		cluster, produceMessage := c.getProduceMessageFromCustomMessage(msg)
		for _, interceptor := range c.producerInterceptors {
			interceptor.OnProduce(ctx, produceMessage)
		}
		mappedProducerMessages[cluster] = append(mappedProducerMessages[cluster], produceMessage)
	}

	for cluster, producerMessages := range mappedProducerMessages {
		producer, err := c.GetProducer(cluster)
		if err != nil {
			return err
		}
		if err := producer.ProduceSyncBulk(ctx, producerMessages, size); err != nil {
			return err
		}
	}
	return nil
}

func (c *producer) getProduceMessageFromCustomMessage(msg *CustomMessage) (string, *ProducerMessage) {
	return msg.Topic.Cluster, &ProducerMessage{
		Body:  msg.Body,
		Topic: msg.Topic.Name,
		Key:   msg.Key,
	}
}

func (c *producer) getProduceMessageFromMessage(msg Message) (string, *ProducerMessage, error) {
	producerTopic, err := c.GetProducerTopic(msg.GetConfigName())
	if err != nil {
		return "", nil, err
	}
	return producerTopic.Cluster, &ProducerMessage{
		Topic: producerTopic.Name,
		Key:   msg.GetKey(),
		Body:  msg,
	}, nil
}
