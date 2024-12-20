package sarama

import (
	"context"
	"errors"
	"strings"

	"github.com/IBM/sarama"
	"github.com/aykanferhat/go-kafka-partition-scaler/pkg/kafka/config"
	"github.com/aykanferhat/go-kafka-partition-scaler/pkg/kafka/handler"
	"github.com/aykanferhat/go-kafka-partition-scaler/pkg/log"
)

type ConsumerGroupHandler interface {
	Setup(sarama.ConsumerGroupSession) error
	Cleanup(sarama.ConsumerGroupSession) error
	ConsumeClaim(sarama.ConsumerGroupSession, sarama.ConsumerGroupClaim) error
}

var ErrClosedConsumerGroup = sarama.ErrClosedConsumerGroup

type ConsumerGroup struct {
	saramaConfig          *sarama.Config
	clusterConfig         *config.ClusterConfig
	consumerGroupConfig   *config.ConsumerGroupConfig
	messageHandler        handler.MessageHandler
	consumerStatusHandler handler.ConsumerStatusHandler
	client                sarama.Client
	consumerGroup         sarama.ConsumerGroup
	logger                log.Logger
}

func NewConsumerGroup(
	clusterConfig *config.ClusterConfig,
	consumerGroupConfig *config.ConsumerGroupConfig,
	messageHandler handler.MessageHandler,
	consumerStatusHandler handler.ConsumerStatusHandler,
	logger log.Logger,
) (*ConsumerGroup, error) {
	saramaConfig, err := NewSaramaConfig(clusterConfig, consumerGroupConfig, logger)
	if err != nil {
		return nil, err
	}
	return &ConsumerGroup{
		saramaConfig:          saramaConfig,
		clusterConfig:         clusterConfig,
		consumerGroupConfig:   consumerGroupConfig,
		messageHandler:        messageHandler,
		consumerStatusHandler: consumerStatusHandler,
		logger:                logger,
	}, nil
}

const subscribeErr = "Error from consumerGroup group: %s, err: %s"

func (c *ConsumerGroup) Subscribe(ctx context.Context) (chan bool, error) {
	client, err := sarama.NewClient(c.clusterConfig.Brokers, c.saramaConfig)
	if err != nil {
		return nil, err
	}
	consumerGroup, err := sarama.NewConsumerGroupFromClient(c.consumerGroupConfig.GroupID, client)
	if err != nil {
		return nil, err
	}
	consumerDiedChan := make(chan bool)
	consumerGroupHandler := NewConsumerGroupHandler(c.messageHandler, c.consumerStatusHandler)
	go func() {
		for {
			if err := consumerGroup.Consume(ctx, c.consumerGroupConfig.Topics, consumerGroupHandler); err != nil {
				if errors.Is(err, ErrClosedConsumerGroup) {
					consumerDiedChan <- true
					break
				}
				if ctx.Err() != nil {
					c.logger.Errorf(subscribeErr, c.consumerGroupConfig.GroupID, ctx.Err().Error())
					consumerDiedChan <- true
					break
				}
				c.logger.Errorf(subscribeErr, c.consumerGroupConfig.GroupID, err.Error())
				continue
			}
			if ctx.Err() != nil {
				c.logger.Errorf(subscribeErr, c.consumerGroupConfig.GroupID, ctx.Err().Error())
				consumerDiedChan <- true
				break
			}
			if c.consumerGroupConfig.IsErrorConsumer { // we don't want to start again when rebalance or else.
				consumerDiedChan <- true
				break
			}
		}
	}()
	go func() {
		for err := range consumerGroup.Errors() {
			c.logger.Errorf(subscribeErr, c.consumerGroupConfig.GroupID, err.Error())
		}
	}()
	c.client = client
	c.consumerGroup = consumerGroup
	return consumerDiedChan, nil
}

var clientClosedErr = "kafka: tried to use a client that was closed"

func (c *ConsumerGroup) Unsubscribe() error {
	if c.client != nil {
		if err := c.client.Close(); err != nil {
			if !strings.EqualFold(err.Error(), clientClosedErr) {
				return err
			}
		}
	}
	if c.consumerGroup != nil {
		if err := c.consumerGroup.Close(); err != nil {
			if !strings.EqualFold(err.Error(), clientClosedErr) {
				return err
			}
		}
	}
	return nil
}
