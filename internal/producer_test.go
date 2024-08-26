package internal

import (
	"context"
	"fmt"
	"github.com/aykanferhat/go-kafka-partition-scaler/pkg/log"
	"testing"

	"github.com/aykanferhat/go-kafka-partition-scaler/pkg/kafka"
	"github.com/aykanferhat/go-kafka-partition-scaler/test/testdata"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func Test_Producer_ShouldReturnProducerWhenGetProducer(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	// Given
	cluster1Producer := kafka.NewMockProducer(controller)
	cluster2Producer := kafka.NewMockProducer(controller)

	producerTopicConfigMap := make(ProducerTopicConfigMap)
	producerTopicConfigMap["topic1"] = &ProducerTopic{
		Name:    "message.topic1.0",
		Cluster: "cluster1",
	}
	producerTopicConfigMap["topic2"] = &ProducerTopic{
		Name:    "message.topic2.0",
		Cluster: "cluster2",
	}

	clusterConfigMap := make(ClusterConfigMap)
	clusterConfigMap["cluster1"] = &ClusterConfig{
		Brokers: "broker1, broker2, broker3",
		Version: "1",
		ErrorConfig: &ErrorConfig{
			GroupID:       errorGroupID,
			Cron:          "0 */5 * * *",
			MaxErrorCount: 2,
		},
		ClientID: "client1",
	}
	clusterConfigMap["cluster2"] = &ClusterConfig{
		Brokers: "broker1, broker2, broker3",
		Version: "1",
		ErrorConfig: &ErrorConfig{
			GroupID:       errorGroupID,
			Cron:          "0 */5 * * *",
			MaxErrorCount: 2,
		},
		ClientID: "client2",
	}

	old := kafkaNewProducer
	defer func() { kafkaNewProducer = old }()

	kafkaNewProducer = func(clusterConfig *kafka.ClusterConfig, logger log.Logger) (kafka.Producer, error) {
		if clusterConfig.ClientID == "client1" {
			return cluster1Producer, nil
		}
		return cluster2Producer, nil
	}
	producer, err := NewProducer(
		clusterConfigMap,
		producerTopicConfigMap,
		[]ProducerInterceptor{},
		logger,
	)
	assert.Nil(t, err)

	// When
	c1Producer, err := producer.GetProducer("cluster1")

	// Then
	assert.Nil(t, err)
	assert.NotNil(t, c1Producer)
	assert.Equal(t, cluster1Producer, c1Producer)
}

func Test_Producer_throwErrorWhenProducerNotFound(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	// Given
	producerTopicConfigMap := make(ProducerTopicConfigMap)

	clusterConfigMap := make(ClusterConfigMap)

	old := kafkaNewProducer
	defer func() { kafkaNewProducer = old }()

	producer, err := NewProducer(
		clusterConfigMap,
		producerTopicConfigMap,
		[]ProducerInterceptor{},
		logger,
	)

	assert.Nil(t, err)

	// When
	clusterName := "x"
	c1Producer, err := producer.GetProducer(clusterName)

	// Then
	assert.Nil(t, c1Producer)
	assert.Equal(t, fmt.Sprintf("kafka async producer not found. cluster name: %s", clusterName), err.Error())
}

func Test_Producer_ShouldReturnProducerTopic(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	// Given
	cluster1Producer := kafka.NewMockProducer(controller)

	producerTopicConfigMap := make(ProducerTopicConfigMap)
	producerTopicConfigMap["topic1"] = &ProducerTopic{
		Name:    "message.topic1.0",
		Cluster: "cluster1",
	}

	clusterConfigMap := make(ClusterConfigMap)
	clusterConfigMap["cluster1"] = &ClusterConfig{
		Brokers: "broker1, broker2, broker3",
		Version: "1",
		ErrorConfig: &ErrorConfig{
			GroupID:       errorGroupID,
			Cron:          "0 */5 * * *",
			MaxErrorCount: 2,
		},
		ClientID: "client-id",
	}

	old := kafkaNewProducer
	defer func() { kafkaNewProducer = old }()

	kafkaNewProducer = func(clusterConfig *kafka.ClusterConfig, logger log.Logger) (kafka.Producer, error) {
		return cluster1Producer, nil
	}

	producer, err := NewProducer(
		clusterConfigMap,
		producerTopicConfigMap,
		[]ProducerInterceptor{},
		logger,
	)
	assert.Nil(t, err)

	// When
	producerTopic, err := producer.GetProducerTopic("topic1")

	// Then
	assert.Nil(t, err)
	assert.Equal(t, "message.topic1.0", producerTopic.Name)
	assert.Equal(t, "cluster1", producerTopic.Cluster)
}

func Test_Producer_throwErrorWhenProducerTopicNotFound(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	// Given
	producerTopicConfigMap := make(ProducerTopicConfigMap)

	clusterConfigMap := make(ClusterConfigMap)

	producer, err := NewProducer(
		clusterConfigMap,
		producerTopicConfigMap,
		[]ProducerInterceptor{},
		logger,
	)
	assert.Nil(t, err)

	// When
	topic := "x"
	producerTopic, err := producer.GetProducerTopic(topic)

	// Then
	assert.Nil(t, producerTopic)
	assert.Equal(t, fmt.Sprintf("producer topic config not found by name: %s", topic), err.Error())
}

func Test_Producer_ShouldProduceAsyncMessage(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	// Given
	ctx := context.Background()

	producerInterceptor := NewMockProducerInterceptor(controller)
	producerInterceptor.EXPECT().OnProduce(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, message *ProducerMessage) {
		assert.Equal(t, "message.topic1.0", message.Topic)
		assert.Equal(t, "1", message.Key)
	})

	testMessage := &testdata.TestProducerMessage{
		Id: 1,
	}

	cluster1Producer := kafka.NewMockProducer(controller)
	cluster1Producer.EXPECT().ProduceAsync(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, message *ProducerMessage) error {
		assert.Equal(t, "message.topic1.0", message.Topic)
		return nil
	}).Times(1)

	clusterConfigMap := make(ClusterConfigMap)
	clusterConfigMap["cluster1"] = &ClusterConfig{
		Brokers: "broker1, broker2, broker3",
		Version: "1",
		ErrorConfig: &ErrorConfig{
			GroupID:       errorGroupID,
			Cron:          "0 */5 * * *",
			MaxErrorCount: 2,
		},
		ClientID: "client-id",
	}

	old := kafkaNewProducer
	defer func() { kafkaNewProducer = old }()

	kafkaNewProducer = func(clusterConfig *kafka.ClusterConfig, logger log.Logger) (kafka.Producer, error) {
		return cluster1Producer, nil
	}

	producerTopicConfigMap := make(ProducerTopicConfigMap)
	producerTopicConfigMap["topic"] = &ProducerTopic{
		Name:    "message.topic1.0",
		Cluster: "cluster1",
	}

	producer, err := NewProducer(
		clusterConfigMap,
		producerTopicConfigMap,
		[]ProducerInterceptor{producerInterceptor},
		logger,
	)
	assert.Nil(t, err)

	// When
	err = producer.ProduceAsync(ctx, testMessage)

	// Then
	assert.Nil(t, err)
}

func Test_Producer_ShouldProduceAsyncBulkMessage(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	// Given
	ctx := context.Background()

	producerInterceptor := NewMockProducerInterceptor(controller)
	producerInterceptor.EXPECT().OnProduce(gomock.Any(), gomock.Any()).Times(3)

	cluster1Producer := kafka.NewMockProducer(controller)
	cluster1Producer.EXPECT().ProduceAsync(gomock.Any(), gomock.Any()).Times(3)

	clusterConfigMap := make(ClusterConfigMap)
	clusterConfigMap["cluster1"] = &ClusterConfig{
		Brokers: "broker1, broker2, broker3",
		Version: "1",
		ErrorConfig: &ErrorConfig{
			GroupID:       errorGroupID,
			Cron:          "0 */5 * * *",
			MaxErrorCount: 2,
		},
		ClientID: "client-id",
	}

	old := kafkaNewProducer
	defer func() { kafkaNewProducer = old }()

	kafkaNewProducer = func(clusterConfig *kafka.ClusterConfig, logger log.Logger) (kafka.Producer, error) {
		return cluster1Producer, nil
	}

	producerTopicConfigMap := make(ProducerTopicConfigMap)
	producerTopicConfigMap["topic"] = &ProducerTopic{
		Name:    "message.topic1.0",
		Cluster: "cluster1",
	}

	messages := []Message{
		&testdata.TestProducerMessage{
			Id: 1,
		},
		&testdata.TestProducerMessage{
			Id: 2,
		},
		&testdata.TestProducerMessage{
			Id: 3,
		},
	}

	producer, err := NewProducer(
		clusterConfigMap,
		producerTopicConfigMap,
		[]ProducerInterceptor{
			producerInterceptor,
		},
		logger,
	)
	assert.Nil(t, err)

	// When
	err = producer.ProduceAsyncBulk(ctx, messages)

	// Then
	assert.Nil(t, err)
}

func Test_Producer_ShouldProduceSyncMessage(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	// Given
	ctx := context.Background()

	producerInterceptor := NewMockProducerInterceptor(controller)
	producerInterceptor.EXPECT().OnProduce(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, message *ProducerMessage) {
		assert.Equal(t, "message.topic1.0", message.Topic)
		assert.Equal(t, "1", message.Key)
	})

	cluster1Producer := kafka.NewMockProducer(controller)
	cluster1Producer.EXPECT().ProduceSync(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, message *ProducerMessage) error {
		assert.Equal(t, "message.topic1.0", message.Topic)
		assert.Equal(t, "1", message.Key)
		return nil
	}).Times(1)

	producerTopicConfigMap := make(ProducerTopicConfigMap)
	producerTopicConfigMap["topic"] = &ProducerTopic{
		Name:    "message.topic1.0",
		Cluster: "cluster1",
	}

	clusterConfigMap := make(ClusterConfigMap)
	clusterConfigMap["cluster1"] = &ClusterConfig{
		Brokers: "broker1, broker2, broker3",
		Version: "1",
		ErrorConfig: &ErrorConfig{
			GroupID:       errorGroupID,
			Cron:          "0 */5 * * *",
			MaxErrorCount: 2,
		},
		ClientID: "client-id",
	}

	old := kafkaNewProducer
	defer func() { kafkaNewProducer = old }()

	kafkaNewProducer = func(clusterConfig *kafka.ClusterConfig, logger log.Logger) (kafka.Producer, error) {
		return cluster1Producer, nil
	}

	testMessage := &testdata.TestProducerMessage{
		Id: 1,
	}

	producer, err := NewProducer(
		clusterConfigMap,
		producerTopicConfigMap,
		[]ProducerInterceptor{
			producerInterceptor,
		},
		logger,
	)
	assert.Nil(t, err)

	// When
	err = producer.ProduceSync(ctx, testMessage)

	// Then
	assert.Nil(t, err)
}

func Test_Producer_ShouldProduceSyncBulkMessage(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	// Given
	ctx := context.Background()

	producerInterceptor := NewMockProducerInterceptor(controller)
	producerInterceptor.EXPECT().OnProduce(gomock.Any(), gomock.Any()).Times(3)

	cluster1Producer := kafka.NewMockProducer(controller)
	cluster1Producer.EXPECT().ProduceSyncBulk(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)

	producerTopicConfigMap := make(ProducerTopicConfigMap)
	producerTopicConfigMap["topic"] = &ProducerTopic{
		Name:    "message.topic1.0",
		Cluster: "cluster1",
	}

	clusterConfigMap := make(ClusterConfigMap)
	clusterConfigMap["cluster1"] = &ClusterConfig{
		Brokers: "broker1, broker2, broker3",
		Version: "1",
		ErrorConfig: &ErrorConfig{
			GroupID:       errorGroupID,
			Cron:          "0 */5 * * *",
			MaxErrorCount: 2,
		},
		ClientID: "client-id",
	}

	old := kafkaNewProducer
	defer func() { kafkaNewProducer = old }()

	kafkaNewProducer = func(clusterConfig *kafka.ClusterConfig, logger log.Logger) (kafka.Producer, error) {
		return cluster1Producer, nil
	}

	messages := []Message{
		&testdata.TestProducerMessage{
			Id: 1,
		},
		&testdata.TestProducerMessage{
			Id: 2,
		},
		&testdata.TestProducerMessage{
			Id: 3,
		},
	}

	producer, err := NewProducer(
		clusterConfigMap,
		producerTopicConfigMap,
		[]ProducerInterceptor{
			producerInterceptor,
		},
		logger,
	)
	assert.Nil(t, err)

	// When
	err = producer.ProduceSyncBulk(ctx, messages, 3)

	// Then
	assert.Nil(t, err)
}

func Test_Producer_ShouldProduceSyncCustomMessage(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	// Given
	ctx := context.Background()

	producerInterceptor := NewMockProducerInterceptor(controller)
	producerInterceptor.EXPECT().OnProduce(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, message *ProducerMessage) {
		assert.Equal(t, "message.topic1.0", message.Topic)
		assert.Equal(t, "key", message.Key)
	})

	cluster1Producer := kafka.NewMockProducer(controller)
	cluster1Producer.EXPECT().ProduceSync(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, message *ProducerMessage) error {
		assert.Equal(t, "message.topic1.0", message.Topic)
		assert.Equal(t, "key", message.Key)
		return nil
	})

	producerTopicConfigMap := make(ProducerTopicConfigMap)

	produceMessage := &CustomMessage{
		Key: "key",
		Body: &testdata.TestProducerMessage{
			Id: 1,
		},
		Topic: &ProducerTopic{
			Name:    "message.topic1.0",
			Cluster: "cluster1",
		},
	}

	clusterConfigMap := make(ClusterConfigMap)
	clusterConfigMap["cluster1"] = &ClusterConfig{
		Brokers: "broker1, broker2, broker3",
		Version: "1",
		ErrorConfig: &ErrorConfig{
			GroupID:       errorGroupID,
			Cron:          "0 */5 * * *",
			MaxErrorCount: 2,
		},
		ClientID: "client-id",
	}

	old := kafkaNewProducer
	defer func() { kafkaNewProducer = old }()

	kafkaNewProducer = func(clusterConfig *kafka.ClusterConfig, logger log.Logger) (kafka.Producer, error) {
		return cluster1Producer, nil
	}

	producer, err := NewProducer(
		clusterConfigMap,
		producerTopicConfigMap,
		[]ProducerInterceptor{
			producerInterceptor,
		},
		logger,
	)
	assert.Nil(t, err)

	// When
	err = producer.ProduceCustomSync(ctx, produceMessage)

	// Then
	assert.Nil(t, err)
}

func Test_Producer_ShouldProduceSyncCustomBulkMessage(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	// Given
	ctx := context.Background()

	producerInterceptor := NewMockProducerInterceptor(controller)
	producerInterceptor.EXPECT().OnProduce(gomock.Any(), gomock.Any()).Times(3)

	cluster1Producer := kafka.NewMockProducer(controller)
	cluster1Producer.EXPECT().ProduceSyncBulk(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)

	producerTopicConfigMap := make(ProducerTopicConfigMap)

	clusterConfigMap := make(ClusterConfigMap)
	clusterConfigMap["cluster1"] = &ClusterConfig{
		Brokers: "broker1, broker2, broker3",
		Version: "1",
		ErrorConfig: &ErrorConfig{
			GroupID:       errorGroupID,
			Cron:          "0 */5 * * *",
			MaxErrorCount: 2,
		},
		ClientID: "client-id",
	}

	old := kafkaNewProducer
	defer func() { kafkaNewProducer = old }()

	kafkaNewProducer = func(clusterConfig *kafka.ClusterConfig, logger log.Logger) (kafka.Producer, error) {
		return cluster1Producer, nil
	}

	produceMessages := []*CustomMessage{
		{
			Key: "key1",
			Body: &testdata.TestProducerMessage{
				Id: 1,
			},
			Topic: &ProducerTopic{
				Name:    "message.topic1.0",
				Cluster: "cluster1",
			},
		},
		{
			Key: "key2",
			Body: &testdata.TestProducerMessage{
				Id: 2,
			},
			Topic: &ProducerTopic{
				Name:    "message.topic1.0",
				Cluster: "cluster1",
			},
		},
		{
			Key: "key3",
			Body: &testdata.TestProducerMessage{
				Id: 3,
			},
			Topic: &ProducerTopic{
				Name:    "message.topic1.0",
				Cluster: "cluster1",
			},
		},
	}

	producer, err := NewProducer(
		clusterConfigMap,
		producerTopicConfigMap,
		[]ProducerInterceptor{
			producerInterceptor,
		},
		logger,
	)
	assert.Nil(t, err)

	// When
	err = producer.ProduceCustomSyncBulk(ctx, produceMessages, 1)

	// Then
	assert.Nil(t, err)
}
