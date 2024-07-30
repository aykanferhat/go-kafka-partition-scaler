package internal

import (
	"strings"
	"time"

	"github.com/aykanferhat/go-kafka-partition-scaler/common"

	"github.com/aykanferhat/go-kafka-partition-scaler/pkg/kafka/config"
)

type ConsumerGroupConfig struct {
	GroupID                     string        `json:"groupId"`
	OffsetInitial               OffsetInitial `json:"offsetInitial"`
	Retry                       string        `json:"retry"`
	Error                       string        `json:"error"`
	Tracer                      string        `json:"tracer"`
	Cluster                     string        `json:"cluster"`
	Name                        string        `json:"name"`
	FetchMaxBytes               string        `json:"fetchMaxBytes"`
	RetryCount                  int           `json:"retryCount"`
	VirtualPartitionChanCount   int           `json:"virtualPartitionChanCount"`
	ConsumeBatchListenerLatency time.Duration `json:"consumeBatchListenerLatency"`
	MaxProcessingTime           time.Duration `json:"maxProcessingTime"`
	RebalanceTimeout            time.Duration `json:"rebalanceTimeout"`
	HeartbeatInterval           time.Duration `json:"heartbeatInterval"`
	BatchSize                   int           `json:"batchSize"`
	SessionTimeout              time.Duration `json:"sessionTimeout"`
	VirtualPartitionCount       int           `json:"virtualPartitionCount"`
	DisableErrorConsumer        bool          `json:"disableErrorConsumer"`
	UniqueListener              bool          `json:"uniqueListener"`
}

var (
	OffsetNewest = config.OffsetNewest
	OffsetOldest = config.OffsetOldest
)

func (c *ConsumerGroupConfig) GetTopics() []string {
	var topics []string
	topics = append(topics, c.Name)
	if len(c.Retry) != 0 {
		topics = append(topics, c.Retry)
	}
	return topics
}

func (c *ConsumerGroupConfig) IsNotDefinedRetryAndErrorTopic() bool {
	return c.IsNotDefinedRetryTopic() && c.IsNotDefinedErrorTopic()
}

func (c *ConsumerGroupConfig) IsNotDefinedRetryTopic() bool {
	return len(c.Retry) == 0
}

func (c *ConsumerGroupConfig) IsNotDefinedErrorTopic() bool {
	return len(c.Error) == 0
}

func (c *ConsumerGroupConfig) IsDefinedErrorTopic() bool {
	return len(c.Error) > 0
}

func (c *ConsumerGroupConfig) IsDisabledErrorConsumer() bool {
	return c.DisableErrorConsumer || len(c.Error) == 0
}

type ConsumerGroupConfigMap map[string]*ConsumerGroupConfig

func (c ConsumerGroupConfigMap) GetConfigWithDefault(name string) (*ConsumerGroupConfig, error) {
	if cc, exists := c[strings.ToLower(name)]; exists {
		if len(cc.GroupID) == 0 {
			return nil, NewErrWithArgs("consumer topic config 'groupId' is required, config name: %s", name)
		}
		if len(cc.Name) == 0 {
			return nil, NewErrWithArgs("consumer topic config 'name' is required, config name: %s", name)
		}
		if cc.RetryCount > 0 && len(cc.Retry) == 0 {
			return nil, NewErrWithArgs("consumer topic config 'retryCount' and  'retry' is required, config name: %s", name)
		}
		if len(cc.Retry) > 0 && cc.RetryCount == 0 {
			cc.RetryCount = 1
		}
		if len(cc.FetchMaxBytes) == 0 {
			cc.FetchMaxBytes = common.MB
		}
		if cc.MaxProcessingTime == 0 {
			cc.MaxProcessingTime = 1 * time.Second
		}
		if cc.BatchSize > 1 {
			if cc.VirtualPartitionCount > 0 && cc.VirtualPartitionChanCount == 0 {
				cc.VirtualPartitionChanCount = cc.BatchSize * 75
			}
			if cc.ConsumeBatchListenerLatency == 0 {
				cc.ConsumeBatchListenerLatency = 3 * time.Second
			}
		} else if cc.VirtualPartitionCount > 0 && cc.VirtualPartitionChanCount == 0 {
			cc.VirtualPartitionChanCount = 75
		}
		if len(cc.Tracer) == 0 {
			cc.Tracer = cc.GroupID
		}
		if len(cc.OffsetInitial) == 0 {
			cc.OffsetInitial = OffsetNewest
		}
		if cc.SessionTimeout == 0 {
			cc.SessionTimeout = 10 * time.Second
		}
		if cc.RebalanceTimeout == 0 {
			cc.RebalanceTimeout = 60 * time.Second
		}
		if cc.HeartbeatInterval == 0 {
			cc.HeartbeatInterval = 3 * time.Second
		}
		return cc, nil
	}
	return nil, NewErrWithArgs("config not found: %s", name)
}
