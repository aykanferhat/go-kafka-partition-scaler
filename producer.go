package partitionscaler

import (
	"github.com/aykanferhat/go-kafka-partition-scaler/internal"
)

type (
	Producer               = internal.Producer
	ProducerTopic          = internal.ProducerTopic
	ProducerTopicConfigMap = internal.ProducerTopicConfigMap
	Message                = internal.Message
	ProducerInterceptor    = internal.ProducerInterceptor
	CustomMessage          = internal.CustomMessage
	ProducerMessage        = internal.ProducerMessage
	Header                 = internal.Header
)
