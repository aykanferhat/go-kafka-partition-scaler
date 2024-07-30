package internal

import (
	"testing"
	"time"

	"github.com/aykanferhat/go-kafka-partition-scaler/common"

	"github.com/stretchr/testify/assert"
)

func Test_ConsumerGroupConfig_ShouldReturnTopics(t *testing.T) {
	// Given
	consumerGroupConfig := &ConsumerGroupConfig{
		GroupID: "message.topic.0.consumer",
		Name:    "message.topic.0",
		Retry:   "message.topic.RETRY.0",
		Error:   "message.topic.ERROR.0",
	}

	// When
	groups := consumerGroupConfig.GetTopics()

	// Then

	assert.Len(t, groups, 2)
}

func Test_ConsumerGroupConfig_ThrowErrWhenConfigNotFound(t *testing.T) {
	// Given

	consumerGroupConfigMap := make(ConsumerGroupConfigMap)
	consumerGroupConfigMap["group"] = &ConsumerGroupConfig{
		GroupID: "message.topic.0.consumer",
		Name:    "message.topic.0",
		Retry:   "message.topic.RETRY.0",
		Error:   "message.topic.ERROR.0",
	}

	// When
	groups, err := consumerGroupConfigMap.GetConfigWithDefault("notFoundConfigName")

	// Then
	assert.Nil(t, groups)
	assert.Equal(t, "config not found: notFoundConfigName", err.Error())
}

func Test_ConsumerGroupConfig_ThrowErrWhenGroupIdNotFound(t *testing.T) {
	// Given

	consumerGroupConfigMap := make(ConsumerGroupConfigMap)
	consumerGroupConfigMap["group"] = &ConsumerGroupConfig{
		GroupID: "",
		Name:    "message.topic.0",
		Retry:   "message.topic.RETRY.0",
		Error:   "message.topic.ERROR.0",
	}

	// When
	groups, err := consumerGroupConfigMap.GetConfigWithDefault("group")

	// Then
	assert.Nil(t, groups)
	assert.Equal(t, "consumer topic config 'groupId' is required, config name: group", err.Error())
}

func Test_ConsumerGroupConfig_ThrowErrWhenNameNotFound(t *testing.T) {
	// Given
	consumerGroupConfigMap := make(ConsumerGroupConfigMap)
	consumerGroupConfigMap["group"] = &ConsumerGroupConfig{
		GroupID: "message.topic.0.consumer",
		Name:    "",
		Retry:   "message.topic.RETRY.0",
		Error:   "message.topic.ERROR.0",
	}

	// When
	groups, err := consumerGroupConfigMap.GetConfigWithDefault("group")

	// Then
	assert.Nil(t, groups)
	assert.Equal(t, "consumer topic config 'name' is required, config name: group", err.Error())
}

func Test_ConsumerGroupConfig_ThrowErrWhenRetryNotFound(t *testing.T) {
	// Given
	consumerGroupConfigMap := make(ConsumerGroupConfigMap)
	consumerGroupConfigMap["group"] = &ConsumerGroupConfig{
		GroupID:    "message.topic.0.consumer",
		Name:       "message.topic.0",
		Retry:      "",
		RetryCount: 3,
		Error:      "message.topic.ERROR.0",
	}

	// When
	groups, err := consumerGroupConfigMap.GetConfigWithDefault("group")

	// Then
	assert.Nil(t, groups)
	assert.Equal(t, "consumer topic config 'retryCount' and  'retry' is required, config name: group", err.Error())
}

func Test_ConsumerGroupConfig_ShouldPassRetryWhenRetryNotFound(t *testing.T) {
	// Given
	consumerGroupConfigMap := make(ConsumerGroupConfigMap)
	consumerGroupConfigMap["group"] = &ConsumerGroupConfig{
		GroupID: "message.topic.0.consumer",
		Name:    "message.topic.0",
		Error:   "message.topic.ERROR.0",
	}

	// When
	group, err := consumerGroupConfigMap.GetConfigWithDefault("group")

	// Then
	assert.Nil(t, err)

	assert.Equal(t, "", group.Retry)
	assert.Equal(t, 0, group.RetryCount)
}

func Test_ConsumerGroupConfig_ShouldReturnBatchConfig(t *testing.T) {
	// Given
	consumerGroupConfigMap := make(ConsumerGroupConfigMap)
	consumerGroupConfigMap["group"] = &ConsumerGroupConfig{
		GroupID:               "message.topic.0.consumer",
		Name:                  "message.topic.0",
		Retry:                 "message.topic.RETRY.0",
		Error:                 "message.topic.ERROR.0",
		BatchSize:             10,
		VirtualPartitionCount: 10,
	}

	// When
	group, err := consumerGroupConfigMap.GetConfigWithDefault("group")

	// Then
	assert.Nil(t, err)

	assert.Equal(t, common.MB, group.FetchMaxBytes)
	assert.Equal(t, 1*time.Second, group.MaxProcessingTime)
	assert.Equal(t, 750, group.VirtualPartitionChanCount)
	assert.Equal(t, 3*time.Second, group.ConsumeBatchListenerLatency)
	assert.Equal(t, OffsetNewest, group.OffsetInitial)
	assert.Equal(t, 10*time.Second, group.SessionTimeout)
	assert.Equal(t, 60*time.Second, group.RebalanceTimeout)
	assert.Equal(t, 3*time.Second, group.HeartbeatInterval)
}

func Test_ConsumerGroupConfig_ShouldReturnConfig(t *testing.T) {
	// Given
	consumerGroupConfigMap := make(ConsumerGroupConfigMap)
	consumerGroupConfigMap["group"] = &ConsumerGroupConfig{
		GroupID:               "message.topic.0.consumer",
		Name:                  "message.topic.0",
		Retry:                 "message.topic.RETRY.0",
		Error:                 "message.topic.ERROR.0",
		VirtualPartitionCount: 10,
	}

	// When
	group, err := consumerGroupConfigMap.GetConfigWithDefault("group")

	// Then
	assert.Nil(t, err)

	assert.Equal(t, common.MB, group.FetchMaxBytes)
	assert.Equal(t, 1*time.Second, group.MaxProcessingTime)
	assert.Equal(t, 75, group.VirtualPartitionChanCount)
	assert.Equal(t, OffsetNewest, group.OffsetInitial)
	assert.Equal(t, 10*time.Second, group.SessionTimeout)
	assert.Equal(t, 60*time.Second, group.RebalanceTimeout)
	assert.Equal(t, 3*time.Second, group.HeartbeatInterval)
}
