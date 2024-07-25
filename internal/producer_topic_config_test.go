package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_ProducerTopicConfigMap_ThrowErrWhenConfigNameNotFound(t *testing.T) {
	// Given
	configs := make(ProducerTopicConfigMap)
	configs["producertopic"] = &ProducerTopic{
		Name:    "",
		Cluster: "cluster",
	}

	// When
	producerTopic, err := configs.GetConfig("producerTopic")

	// Then
	assert.Nil(t, producerTopic)
	assert.Equal(t, "producer topic 'name' is required, config name: producerTopic", err.Error())
}

func Test_ProducerTopicConfigMap_ThrowErrWhenConfigNotFound(t *testing.T) {
	// Given
	configs := make(ProducerTopicConfigMap)
	configs["producertopic"] = &ProducerTopic{
		Name:    "message.topic.0",
		Cluster: "cluster",
	}

	// When
	producerTopic, err := configs.GetConfig("notFoundTopicName")

	// Then
	assert.Nil(t, producerTopic)
	assert.Equal(t, "producer topic config not found by name: notFoundTopicName", err.Error())
}

func Test_ProducerTopicConfigMap_ThrowErrWhenClusterNotFound(t *testing.T) {
	// Given
	configs := make(ProducerTopicConfigMap)
	configs["producertopic"] = &ProducerTopic{
		Name:    "message.topic.0",
		Cluster: "",
	}

	// When
	producerTopic, err := configs.GetConfig("producerTopic")

	// Then
	assert.Nil(t, producerTopic)
	assert.Equal(t, "producer topic 'cluster' is required, config name: producerTopic", err.Error())
}

func Test_ProducerTopicConfigMap_ShouldReturnProducerTopicConfig(t *testing.T) {
	// Given
	configs := make(ProducerTopicConfigMap)
	configs["producertopic"] = &ProducerTopic{
		Name:    "message.topic.0",
		Cluster: "cluster",
	}

	// When
	producerTopic, err := configs.GetConfig("producerTopic")

	// Then
	assert.Nil(t, err)
	assert.Equal(t, "message.topic.0", producerTopic.Name)
	assert.Equal(t, "cluster", producerTopic.Cluster)
}
