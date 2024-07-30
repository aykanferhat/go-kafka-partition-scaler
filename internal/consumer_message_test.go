package internal

import (
	"testing"
	"time"

	"github.com/aykanferhat/go-kafka-partition-scaler/common"
	"github.com/aykanferhat/go-kafka-partition-scaler/pkg/kafka"
	"github.com/stretchr/testify/assert"
)

func Test_ConsumerMessage_ShouldGetRetryCount(t *testing.T) {
	// Given
	msg := &ConsumerMessage{
		ConsumerMessage: &kafka.ConsumerMessage{
			Headers: []kafka.Header{
				{
					Key:   common.ToByte(common.RetryTopicCountKey.String()),
					Value: common.ToByte("1"),
				},
			},
			Timestamp: time.Time{},
			Key:       common.ToByte("key"),
			Value:     common.ToByte("value"),
			Topic:     topic,
			Partition: 0,
			Offset:    0,
		},
		VirtualPartition: 0,
	}

	// When
	retryCount := getRetriedCount(msg)

	// Then
	assert.Equal(t, 2, retryCount)
}

func Test_ConsumerMessage_ShouldGetInitialRetryCount(t *testing.T) {
	// Given
	msg := &ConsumerMessage{
		ConsumerMessage: &kafka.ConsumerMessage{
			Headers:   []kafka.Header{},
			Timestamp: time.Time{},
			Key:       common.ToByte("key"),
			Value:     common.ToByte("value"),
			Topic:     topic,
			Partition: 0,
			Offset:    0,
		},
		VirtualPartition: 0,
	}

	// When
	retryCount := getRetriedCount(msg)

	// Then
	assert.Equal(t, 1, retryCount)
}

func Test_ConsumerMessage_ShouldGetErrorCount(t *testing.T) {
	// Given
	msg := &ConsumerMessage{
		ConsumerMessage: &kafka.ConsumerMessage{
			Headers: []kafka.Header{
				{
					Key:   common.ToByte(common.ErrorTopicCountKey.String()),
					Value: common.ToByte("1"),
				},
			},
			Timestamp: time.Time{},
			Key:       common.ToByte("key"),
			Value:     common.ToByte("value"),
			Topic:     topic,
			Partition: 0,
			Offset:    0,
		},
		VirtualPartition: 0,
	}

	// When
	retryCount := getErrorCount(msg)

	// Then
	assert.Equal(t, 1, retryCount)
}

func Test_ConsumerMessage_ShouldGetInitialErrorCount(t *testing.T) {
	// Given
	msg := &ConsumerMessage{
		ConsumerMessage: &kafka.ConsumerMessage{
			Headers:   []kafka.Header{},
			Timestamp: time.Time{},
			Key:       common.ToByte("key"),
			Value:     common.ToByte("value"),
			Topic:     topic,
			Partition: 0,
			Offset:    0,
		},
		VirtualPartition: 0,
	}

	// When
	retryCount := getErrorCount(msg)

	// Then
	assert.Equal(t, 0, retryCount)
}

func Test_ConsumerMessage_ShouldGetErrorMessage(t *testing.T) {
	// Given
	msg := &ConsumerMessage{
		ConsumerMessage: &kafka.ConsumerMessage{
			Headers: []kafka.Header{
				{
					Key:   common.ToByte(common.ErrorMessageKey.String()),
					Value: common.ToByte("error"),
				},
			},
			Timestamp: time.Time{},
			Key:       common.ToByte("key"),
			Value:     common.ToByte("value"),
			Topic:     topic,
			Partition: 0,
			Offset:    0,
		},
		VirtualPartition: 0,
	}

	// When
	errorMessage := getErrorMessage(msg)

	// Then
	assert.Equal(t, "error", errorMessage)
}

func Test_ConsumerMessage_ShouldGetEmptyErrorMessage(t *testing.T) {
	// Given
	msg := &ConsumerMessage{
		ConsumerMessage: &kafka.ConsumerMessage{
			Headers:   []kafka.Header{},
			Timestamp: time.Time{},
			Key:       common.ToByte("key"),
			Value:     common.ToByte("value"),
			Topic:     topic,
			Partition: 0,
			Offset:    0,
		},
		VirtualPartition: 0,
	}

	// When
	errorMessage := getErrorMessage(msg)

	// Then
	assert.Equal(t, "", errorMessage)
}

func Test_ConsumerMessage_ShouldGetTargetTopic(t *testing.T) {
	// Given
	msg := &ConsumerMessage{
		ConsumerMessage: &kafka.ConsumerMessage{
			Headers: []kafka.Header{
				{
					Key:   common.ToByte(common.TargetTopicKey.String()),
					Value: common.ToByte("target-topic"),
				},
			},
			Timestamp: time.Time{},
			Key:       common.ToByte("key"),
			Value:     common.ToByte("value"),
			Topic:     topic,
			Partition: 0,
			Offset:    0,
		},
		VirtualPartition: 0,
	}

	// When
	targetTopic := getTargetTopic(msg)

	// Then
	assert.Equal(t, "target-topic", targetTopic)
}

func Test_ConsumerMessage_ShouldGetEmptyTargetTopic(t *testing.T) {
	// Given
	msg := &ConsumerMessage{
		ConsumerMessage: &kafka.ConsumerMessage{
			Headers:   []kafka.Header{},
			Timestamp: time.Time{},
			Key:       common.ToByte("key"),
			Value:     common.ToByte("value"),
			Topic:     topic,
			Partition: 0,
			Offset:    0,
		},
		VirtualPartition: 0,
	}

	// When
	targetTopic := getTargetTopic(msg)

	// Then
	assert.Equal(t, "", targetTopic)
}

func Test_ConsumerMessage_ShouldGetHeadersForRetryToError(t *testing.T) {
	// Given
	msg := &ConsumerMessage{
		ConsumerMessage: &kafka.ConsumerMessage{
			Headers: []kafka.Header{
				{
					Key:   common.ToByte("key"),
					Value: common.ToByte("value"),
				},
				{
					Key:   common.ToByte(common.ErrorMessageKey.String()),
					Value: common.ToByte("old error"),
				},
				{
					Key:   common.ToByte(common.RetryTopicCountKey.String()),
					Value: common.ToByte("3"),
				},
				{
					Key:   common.ToByte(common.ErrorTopicCountKey.String()),
					Value: common.ToByte("1"),
				},
			},
			Timestamp: time.Time{},
			Key:       common.ToByte("key"),
			Value:     common.ToByte("value"),
			Topic:     retryTopic,
			Partition: 0,
			Offset:    0,
		},
		VirtualPartition: 0,
	}

	// When
	headerForRetry := headersFromRetryToError(msg, "new error")

	// Then
	assert.Len(t, headerForRetry, 4)
	var existsRetryTopicKey bool
	var existErrorMessageKey bool
	var existsErrorTopicCountKey bool
	var existsKey bool
	for _, header := range headerForRetry {
		if common.ContextKey(header.Key) == common.RetryTopicCountKey {
			existsRetryTopicKey = true
		}
		if common.ContextKey(header.Key) == common.ErrorMessageKey {
			assert.Equal(t, "new error", string(header.Value))
			existErrorMessageKey = true
		}
		if common.ContextKey(header.Key) == common.ErrorTopicCountKey {
			assert.Equal(t, "2", string(header.Value))
			existsErrorTopicCountKey = true
		}
		if common.ContextKey(header.Key) == common.ContextKey("key") {
			assert.Equal(t, "value", string(header.Value))
			existsKey = true
		}
	}
	assert.False(t, existsRetryTopicKey)
	assert.True(t, existErrorMessageKey)
	assert.True(t, existsErrorTopicCountKey)
	assert.True(t, existsKey)
}
