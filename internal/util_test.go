package internal

import (
	"github.com/Trendyol/go-kafka-partition-scaler/common"
	"testing"
	"time"

	"github.com/Trendyol/go-kafka-partition-scaler/pkg/kafka/message"

	"github.com/stretchr/testify/assert"
)

func Test_Util_ShouldCalculateVirtualPartitionSameResultWhenKeySame(t *testing.T) {
	// Given
	id1 := "12312331"
	id2 := "12312331"
	totalVirtualPartition := 10

	message1 := &ConsumerMessage{
		ConsumerMessage: &message.ConsumerMessage{
			Headers:   nil,
			Timestamp: time.Now(),
			Key:       common.ToByte(id1),
			Value:     common.ToByte("value"),
			Topic:     topic,
			Partition: 0,
			Offset:    100,
		},
	}

	message2 := &ConsumerMessage{
		ConsumerMessage: &message.ConsumerMessage{
			Headers:   nil,
			Timestamp: time.Now(),
			Key:       common.ToByte(id2),
			Value:     common.ToByte("value"),
			Topic:     topic,
			Partition: 0,
			Offset:    101,
		},
	}

	// When
	virtualPartition1 := calculateVirtualPartition(string(message1.Key), totalVirtualPartition)
	virtualPartition2 := calculateVirtualPartition(string(message2.Key), totalVirtualPartition)

	// Then
	assert.Equal(t, virtualPartition1, virtualPartition2)
}

func Test_Util_ShouldCalculateVirtualPartitionRandomWhenKeyIsEmpty(t *testing.T) {
	// Given
	id := ""
	totalVirtualPartition := 10

	msg := &ConsumerMessage{
		ConsumerMessage: &message.ConsumerMessage{
			Headers:   nil,
			Timestamp: time.Now(),
			Key:       common.ToByte(id),
			Value:     common.ToByte("value"),
			Topic:     topic,
			Partition: 0,
			Offset:    100,
		},
	}

	// When
	virtualPartition := calculateVirtualPartition(string(msg.Key), totalVirtualPartition)

	// Then
	assert.True(t, true, 0 <= virtualPartition && virtualPartition < 10)
}
