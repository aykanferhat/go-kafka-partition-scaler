package sarama

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/aykanferhat/go-kafka-partition-scaler/pkg/json"
	"github.com/aykanferhat/go-kafka-partition-scaler/pkg/kafka/config"
	"github.com/aykanferhat/go-kafka-partition-scaler/pkg/kafka/message"
)

type Producer struct {
	syncProducer  sarama.SyncProducer
	asyncProducer sarama.AsyncProducer
}

func NewProducer(clusterConfig *config.ClusterConfig) (*Producer, error) {
	saramaConfig, err := NewSaramaConfig(clusterConfig, nil)
	if err != nil {
		return nil, err
	}
	client, err := sarama.NewClient(clusterConfig.Brokers, saramaConfig)
	if err != nil {
		return nil, err
	}
	syncProducer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}
	asyncProducer, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}
	return &Producer{
		syncProducer:  syncProducer,
		asyncProducer: asyncProducer,
	}, nil
}

func (p *Producer) ProduceAsync(_ context.Context, message *message.ProducerMessage) error {
	saramaMessage, err := p.mapToProducerMessage(message)
	if err != nil {
		return err
	}
	p.asyncProducer.Input() <- saramaMessage
	return nil
}

func (p *Producer) ProduceSync(_ context.Context, message *message.ProducerMessage) error {
	produceMessage, err := p.mapToProducerMessage(message)
	if err != nil {
		return err
	}
	if _, _, err = p.syncProducer.SendMessage(produceMessage); err != nil {
		return err
	}
	return nil
}

func (p *Producer) ProduceSyncBulk(_ context.Context, messages []*message.ProducerMessage, size int) error {
	slicedProducerMessages := message.SliceMessages(messages, size)
	for _, producerMessages := range slicedProducerMessages {
		saramaMessages := make([]*sarama.ProducerMessage, 0, len(producerMessages))
		for _, producerMessage := range producerMessages {
			saramaMessage, err := p.mapToProducerMessage(producerMessage)
			if err != nil {
				return err
			}
			saramaMessages = append(saramaMessages, saramaMessage)
		}
		if err := p.syncProducer.SendMessages(saramaMessages); err != nil {
			return err
		}
	}
	return nil
}

func (p *Producer) mapToProducerMessage(message *message.ProducerMessage) (*sarama.ProducerMessage, error) {
	var body []byte
	var err error
	if message.ByteBody {
		body = message.Body.([]byte)
	} else {
		body, err = json.Marshal(message.Body)
	}
	if err != nil {
		return nil, err
	}
	headers := make([]sarama.RecordHeader, 0, len(message.Headers))
	for _, header := range message.Headers {
		headers = append(headers, sarama.RecordHeader{
			Key:   header.Key,
			Value: header.Value,
		})
	}

	var producerMessage *sarama.ProducerMessage
	if message.Key == "" {
		producerMessage = &sarama.ProducerMessage{
			Value:   sarama.StringEncoder(body),
			Topic:   message.Topic,
			Headers: headers,
		}
	} else {
		producerMessage = &sarama.ProducerMessage{
			Value:   sarama.StringEncoder(body),
			Key:     sarama.StringEncoder(message.Key),
			Topic:   message.Topic,
			Headers: headers,
		}
	}
	return producerMessage, nil
}
