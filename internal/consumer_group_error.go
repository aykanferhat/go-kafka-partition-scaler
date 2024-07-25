package internal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Trendyol/go-kafka-partition-scaler/pkg/cron"
	"github.com/Trendyol/go-kafka-partition-scaler/pkg/kafka"
	"github.com/Trendyol/go-kafka-partition-scaler/pkg/log"
)

type ErrorConsumerGroup interface {
	ScheduleToSubscribe() error
	GetGroupID() string
	Subscribe()
	Unsubscribe()
	IsRunning() bool
	WaitConsumerStart()
	WaitConsumerStop()
}

type errorConsumerGroup struct {
	cg                          kafka.ConsumerGroup
	clusterConfig               *ClusterConfig
	errorConsumerConfig         *ConsumerGroupErrorConfig
	errorTopicConsumerMap       map[string]Consumer
	lastStep                    func(context.Context, *ConsumerMessage, error)
	scheduleToSubscribeCron     *cron.Cron
	subscribed                  bool
	running                     bool
	consumerGroupStatusTicker   *time.Ticker
	consumerGroupStatusListener *ConsumerGroupStatusListener
	tracers                     []Tracer
}

func NewErrorConsumerGroup(
	clusterConfig *ClusterConfig,
	errorConsumerConfig *ConsumerGroupErrorConfig,
	errorTopicConsumerMap map[string]Consumer,
	lastStep func(context.Context, *ConsumerMessage, error),
	tracers []Tracer,
) (ErrorConsumerGroup, error) {
	errorConsumerGroup := &errorConsumerGroup{
		clusterConfig:               clusterConfig,
		errorConsumerConfig:         errorConsumerConfig,
		errorTopicConsumerMap:       errorTopicConsumerMap,
		scheduleToSubscribeCron:     cron.NewCron(),
		lastStep:                    lastStep,
		tracers:                     tracers,
		consumerGroupStatusTicker:   time.NewTicker(2 * time.Second),
		consumerGroupStatusListener: newConsumerGroupStatusListener(),
	}
	errorConsumerGroup.listenUnSubscribableStatus()
	return errorConsumerGroup, nil
}

func (c *errorConsumerGroup) Handle() kafka.MessageHandler {
	return func(topic string, partition int32, messageChan <-chan *kafka.ConsumerMessage, commitFunc kafka.CommitMessageFunc) {
		consumer := c.errorTopicConsumerMap[topic]
		c.consumerGroupStatusListener.Listen(&ConsumerGroupStatus{Topic: topic, Partition: partition, Status: StartedListening, Time: time.Now()})
		for msg := range messageChan {
			if time.Since(msg.Timestamp).Nanoseconds() < c.errorConsumerConfig.CloseConsumerWhenMessageIsNew.Nanoseconds() {
				c.consumerGroupStatusListener.Listen(&ConsumerGroupStatus{Topic: topic, Partition: partition, Status: ErrorConsumerOccurredViolation, Time: time.Now(), Offset: msg.Offset})
				continue
			}
			c.consumerGroupStatusListener.Listen(&ConsumerGroupStatus{Topic: topic, Partition: partition, Status: ListenedMessage, Time: time.Now(), Offset: msg.Offset})
			consumerMessage := &ConsumerMessage{ConsumerMessage: msg, GroupID: c.errorConsumerConfig.GroupID, Tracer: c.errorConsumerConfig.Tracer}
			ctx := context.Background()
			errorCount := getErrorCount(consumerMessage)
			if errorCount > c.errorConsumerConfig.MaxErrorCount {
				err := errors.New(getErrorMessage(consumerMessage))
				reachedMaxRetryErrorCountErr := fmt.Errorf("reached max error count, errRetriedCount: %d", errorCount)
				joinedErr := errors.Join(err, reachedMaxRetryErrorCountErr)
				c.lastStep(ctx, consumerMessage, joinedErr)
				commitFunc(consumerMessage.Topic, consumerMessage.Partition, consumerMessage.Offset)
				continue
			}
			if err := processMessage(ctx, consumer, consumerMessage, c.errorConsumerConfig.MaxProcessingTime); err != nil {
				c.lastStep(ctx, consumerMessage, err)
			}
			commitFunc(consumerMessage.Topic, consumerMessage.Partition, consumerMessage.Offset)
		}
	}
}

func (c *errorConsumerGroup) ScheduleToSubscribe() error {
	if err := c.scheduleToSubscribeCron.AddFunc(c.errorConsumerConfig.Cron, c.Subscribe); err != nil {
		return err
	}
	c.scheduleToSubscribeCron.Start()
	return nil
}

func (c *errorConsumerGroup) GetGroupID() string {
	return c.errorConsumerConfig.GroupID
}

func (c *errorConsumerGroup) Subscribe() {
	if !c.existsErrorTopic() {
		return
	}
	if c.subscribed {
		log.Infof("errorConsumerGroup is already subscribed, groupId: %s", c.errorConsumerConfig.GroupID)
		return
	}
	log.Infof("errorConsumerGroup Subscribe, groupId: %s", c.errorConsumerConfig.GroupID)
	c.consumerGroupStatusListener.listenConsumerStart()
	cg, err := kafka.NewConsumerGroup(
		mapToClusterConfig(c.clusterConfig),
		mapToErrorConsumerGroupConfig(c.errorConsumerConfig),
		c.Handle(),
		c.consumerGroupStatusListener.HandleConsumerGroupStatus(),
	)
	if err != nil {
		log.Errorf("errorConsumerGroup Subscribe err: %s", err.Error())
		return
	}
	if err := cg.Subscribe(); err != nil {
		log.Errorf("errorConsumerGroup Subscribe err: %s", err.Error())
		return
	}
	c.cg = cg
	c.subscribed = true
}

func (c *errorConsumerGroup) Unsubscribe() {
	if !c.existsErrorTopic() || c.cg == nil {
		return
	}
	log.Infof("errorConsumerGroup Unsubscribe, groupId: %s", c.errorConsumerConfig.GroupID)
	if err := c.cg.Unsubscribe(); err != nil {
		log.Errorf("errorConsumerGroup Unsubscribe err: %s", err.Error())
		return
	}
	c.consumerGroupStatusListener.listenConsumerStop()
	c.cg = nil
	c.subscribed = false
}

func (c *errorConsumerGroup) IsRunning() bool {
	return c.running
}

func (c *errorConsumerGroup) WaitConsumerStart() {
	c.consumerGroupStatusListener.WaitConsumerStart()
	c.running = true
}

func (c *errorConsumerGroup) WaitConsumerStop() {
	c.consumerGroupStatusListener.WaitConsumerStop()
	c.running = false
}

func (c *errorConsumerGroup) listenUnSubscribableStatus() {
	go func() {
		closeConsumerWhenThereIsNoMessage := c.errorConsumerConfig.CloseConsumerWhenThereIsNoMessage.Nanoseconds()
		for range c.consumerGroupStatusTicker.C {
			if !c.subscribed {
				continue
			}
			var unsubscribable bool
			for _, status := range c.consumerGroupStatusListener.consumerGroupStatusMap.ToMap() {
				if status.Status == ErrorConsumerOccurredViolation ||
					(status.Status == AssignedTopicPartition && time.Since(status.Time).Nanoseconds() > closeConsumerWhenThereIsNoMessage) ||
					(status.Status == StartedListening && time.Since(status.Time).Nanoseconds() > closeConsumerWhenThereIsNoMessage) ||
					(status.Status == ListenedMessage && time.Since(status.Time).Nanoseconds() > closeConsumerWhenThereIsNoMessage) {
					unsubscribable = true
				} else {
					unsubscribable = false
					break
				}
			}
			if unsubscribable {
				c.Unsubscribe()
			}
		}
	}()
}

func (c *errorConsumerGroup) existsErrorTopic() bool {
	return len(c.errorTopicConsumerMap) != 0
}

func mapToErrorConsumerGroupConfig(errorConsumerConfig *ConsumerGroupErrorConfig) *kafka.ConsumerGroupConfig {
	return &kafka.ConsumerGroupConfig{
		GroupID:           errorConsumerConfig.GroupID,
		Topics:            errorConsumerConfig.Topics,
		MaxProcessingTime: errorConsumerConfig.MaxProcessingTime,
		IsErrorConsumer:   true,
		OffsetInitial:     errorConsumerConfig.OffsetInitial,
		FetchMaxBytes:     errorConsumerConfig.FetchMaxBytes,
		SessionTimeout:    errorConsumerConfig.SessionTimeout,
		RebalanceTimeout:  errorConsumerConfig.RebalanceTimeout,
		HeartbeatInterval: errorConsumerConfig.HeartbeatInterval,
	}
}
