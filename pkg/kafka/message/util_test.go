package message

import (
	"reflect"
	"testing"
)

func TestSliceMessages(t *testing.T) {
	tests := []struct {
		name            string
		inputMessages   []*ProducerMessage
		batchSize       int
		expectedBatches [][]*ProducerMessage
	}{
		{
			name: "Valid batch split",
			inputMessages: []*ProducerMessage{
				{Body: "Message1", Topic: "Topic1", Key: "Key1"},
				{Body: "Message2", Topic: "Topic1", Key: "Key2"},
				{Body: "Message3", Topic: "Topic2", Key: "Key3"},
				{Body: "Message4", Topic: "Topic2", Key: "Key4"},
				{Body: "Message5", Topic: "Topic3", Key: "Key5"},
			},
			batchSize: 2,
			expectedBatches: [][]*ProducerMessage{
				{
					{Body: "Message1", Topic: "Topic1", Key: "Key1"},
					{Body: "Message2", Topic: "Topic1", Key: "Key2"},
				},
				{
					{Body: "Message3", Topic: "Topic2", Key: "Key3"},
					{Body: "Message4", Topic: "Topic2", Key: "Key4"},
				},
				{
					{Body: "Message5", Topic: "Topic3", Key: "Key5"},
				},
			},
		},
		{
			name:          "Empty input",
			inputMessages: []*ProducerMessage{},
			batchSize:     3,
			expectedBatches: [][]*ProducerMessage{
				{},
			},
		},
		{
			name: "Batch size greater than input",
			inputMessages: []*ProducerMessage{
				{Body: "Message1", Topic: "Topic1", Key: "Key1"},
				{Body: "Message2", Topic: "Topic1", Key: "Key2"},
			},
			batchSize: 5,
			expectedBatches: [][]*ProducerMessage{
				{
					{Body: "Message1", Topic: "Topic1", Key: "Key1"},
					{Body: "Message2", Topic: "Topic1", Key: "Key2"},
				},
			},
		},
		{
			name: "Negative batch size",
			inputMessages: []*ProducerMessage{
				{Body: "Message1", Topic: "Topic1", Key: "Key1"},
				{Body: "Message2", Topic: "Topic1", Key: "Key2"},
			},
			batchSize:       -1,
			expectedBatches: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SliceMessages(tt.inputMessages, tt.batchSize)
			if !reflect.DeepEqual(result, tt.expectedBatches) {
				t.Errorf("expected %v, got %v", tt.expectedBatches, result)
			}
		})
	}
}
