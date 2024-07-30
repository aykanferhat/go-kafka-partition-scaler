package internal

import (
	"fmt"
	"strconv"

	"github.com/aykanferhat/go-kafka-partition-scaler/common"
	"github.com/aykanferhat/go-kafka-partition-scaler/pkg/kafka"
	"github.com/aykanferhat/go-kafka-partition-scaler/pkg/kafka/message"
)

type ConsumerMessage struct {
	*kafka.ConsumerMessage
	Tracer           string
	GroupID          string
	VirtualPartition int
}

func getRetriedCount(message *ConsumerMessage) int {
	return getHeaderIntValue(message, common.RetryTopicCountKey) + 1
}

func getErrorCount(message *ConsumerMessage) int {
	return getHeaderIntValue(message, common.ErrorTopicCountKey)
}

func getErrorMessage(message *ConsumerMessage) string {
	return getHeaderStrValue(message, common.ErrorMessageKey)
}

func getTargetTopic(message *ConsumerMessage) string {
	return getHeaderStrValue(message, common.TargetTopicKey)
}

func getHeaderIntValue(message *ConsumerMessage, key common.ContextKey) int {
	value := getHeaderValue(message, key)
	if value == nil {
		return 0
	}
	count, _ := strconv.Atoi(string(value))
	return count
}

func getHeaderStrValue(message *ConsumerMessage, key common.ContextKey) string {
	value := getHeaderValue(message, key)
	if value == nil {
		return ""
	}
	return string(value)
}

func getHeaderValue(message *ConsumerMessage, key common.ContextKey) []byte {
	for _, header := range message.Headers {
		if common.ContextKey(header.Key) == key {
			return header.Value
		}
	}
	return nil
}

func headersForRetry(message *ConsumerMessage, errorMessage string) []kafka.Header {
	messageHeaderMap := make(map[common.ContextKey][]byte)
	for _, header := range message.Headers {
		messageHeaderMap[common.ContextKey(header.Key)] = header.Value
	}
	messageHeaderMap[common.ErrorMessageKey] = common.ToByte(errorMessage)
	messageHeaderMap[common.RetryTopicCountKey] = common.ToByte("0")

	return mapToHeaderArray(messageHeaderMap)
}

func headersForError(message *ConsumerMessage, errorMessage string) []kafka.Header {
	messageHeaderMap := make(map[common.ContextKey][]byte)
	for _, header := range message.Headers {
		messageHeaderMap[common.ContextKey(header.Key)] = header.Value
	}
	messageHeaderMap[common.ErrorMessageKey] = common.ToByte(errorMessage)
	messageHeaderMap[common.ErrorTopicCountKey] = common.ToByte("0")

	return mapToHeaderArray(messageHeaderMap)
}

func headersFromRetryToRetry(message *ConsumerMessage, errorMessage string, retriedCount int) []kafka.Header {
	messageHeaderMap := make(map[common.ContextKey][]byte)
	for _, header := range message.Headers {
		messageHeaderMap[common.ContextKey(header.Key)] = header.Value
	}
	messageHeaderMap[common.ErrorMessageKey] = common.ToByte(errorMessage)
	messageHeaderMap[common.RetryTopicCountKey] = common.ToByte(fmt.Sprint(retriedCount))

	return mapToHeaderArray(messageHeaderMap)
}

func headersFromRetryToError(message *ConsumerMessage, errorMessage string) []kafka.Header {
	messageHeaderMap := make(map[common.ContextKey][]byte)
	for _, header := range message.Headers {
		messageHeaderMap[common.ContextKey(header.Key)] = header.Value
	}
	errorCount := getErrorCount(message)
	messageHeaderMap[common.ErrorTopicCountKey] = common.ToByte(fmt.Sprint(errorCount + 1))
	messageHeaderMap[common.ErrorMessageKey] = common.ToByte(errorMessage)
	messageHeaderMap[common.TargetTopicKey] = common.ToByte(message.Topic)
	delete(messageHeaderMap, common.RetryTopicCountKey)

	return mapToHeaderArray(messageHeaderMap)
}

func headersFromErrorToRetry(message *ConsumerMessage) []kafka.Header {
	messageHeaderMap := make(map[common.ContextKey][]byte)
	for _, header := range message.Headers {
		messageHeaderMap[common.ContextKey(header.Key)] = header.Value
	}
	delete(messageHeaderMap, common.RetryTopicCountKey)

	return mapToHeaderArray(messageHeaderMap)
}

func headersForShovel(message *ConsumerMessage) []kafka.Header {
	messageHeaderMap := make(map[common.ContextKey][]byte)
	for _, header := range message.Headers {
		messageHeaderMap[common.ContextKey(header.Key)] = header.Value
	}
	errorCount := getErrorCount(message)
	messageHeaderMap[common.ErrorTopicCountKey] = common.ToByte(fmt.Sprint(errorCount + 1))
	return mapToHeaderArray(messageHeaderMap)
}

func mapToHeaderArray(messageHeaderMap map[common.ContextKey][]byte) []kafka.Header {
	headers := make([]message.Header, 0)
	for key, bytes := range messageHeaderMap {
		headers = append(headers, message.Header{
			Key:   common.ToByte(key.String()),
			Value: bytes,
		})
	}
	return headers
}

func isMainTopic(message *ConsumerMessage, consumerTopicConfig *ConsumerGroupConfig) bool {
	return message.Topic == consumerTopicConfig.Name
}

func isRetryTopic(message *ConsumerMessage, consumerTopicConfig *ConsumerGroupConfig) bool {
	return message.Topic == consumerTopicConfig.Retry
}
