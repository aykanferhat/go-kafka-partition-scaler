package sarama

import (
	"github.com/IBM/sarama"
	"github.com/Trendyol/go-kafka-partition-scaler/pkg/kafka/handler"
	"github.com/Trendyol/go-kafka-partition-scaler/pkg/kafka/message"
)

type consumerGroupCoreHandler struct {
	session               sarama.ConsumerGroupSession
	messageHandler        handler.MessageHandler
	consumerStatusHandler handler.ConsumerStatusHandler
	status                bool
}

func NewConsumerGroupHandler(messageHandler handler.MessageHandler, consumerStatusHandler handler.ConsumerStatusHandler) ConsumerGroupHandler {
	return &consumerGroupCoreHandler{
		status:                false,
		messageHandler:        messageHandler,
		consumerStatusHandler: consumerStatusHandler,
	}
}

func (handler *consumerGroupCoreHandler) Setup(session sarama.ConsumerGroupSession) error {
	handler.session = session
	for topic, partitions := range session.Claims() {
		for _, partition := range partitions {
			handler.consumerStatusHandler(topic, partition, true)
		}
	}
	return nil
}

func (handler *consumerGroupCoreHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	messagesChan := make(chan *message.ConsumerMessage)
	go func() {
		handler.messageHandler(claim.Topic(), claim.Partition(), messagesChan, handler.CommitMessage)
	}()
	defer func() {
		close(messagesChan)
	}()
	for {
		select {
		case msg := <-claim.Messages():
			if msg == nil {
				continue
			}
			headers := make([]message.Header, 0, len(msg.Headers))
			for _, hdr := range msg.Headers {
				headers = append(headers, message.Header{Key: hdr.Key, Value: hdr.Value})
			}
			messagesChan <- &message.ConsumerMessage{
				Headers:   headers,
				Timestamp: msg.Timestamp,
				Key:       msg.Key,
				Value:     msg.Value,
				Topic:     msg.Topic,
				Partition: msg.Partition,
				Offset:    msg.Offset,
			}
		case <-session.Context().Done():
			return nil
		}
	}
}

func (handler *consumerGroupCoreHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	for topic, partitions := range session.Claims() {
		for _, partition := range partitions {
			handler.consumerStatusHandler(topic, partition, false)
		}
	}
	return nil
}

func (handler *consumerGroupCoreHandler) CommitMessage(topic string, partition int32, offset int64) {
	if handler.session == nil {
		return
	}
	handler.session.MarkOffset(topic, partition, offset+1, "")
}
