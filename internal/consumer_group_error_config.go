package internal

import (
	"strings"
	"time"

	"github.com/Trendyol/go-kafka-partition-scaler/common"
)

type ConsumerGroupErrorConfig struct {
	FetchMaxBytes                     string        `json:"fetchMaxBytes"`
	GroupID                           string        `json:"groupId"`
	TargetTopic                       string        `json:"targetTopic"`
	Cron                              string        `json:"cron"`
	Cluster                           string        `json:"cluster"`
	Tracer                            string        `json:"tracer"`
	OffsetInitial                     OffsetInitial `json:"offsetInitial"`
	Topics                            []string      `json:"topics"`
	MaxErrorCount                     int           `json:"maxErrorCount"`
	CloseConsumerWhenMessageIsNew     time.Duration `json:"closeConsumerWhenMessageIsNew"`
	MaxProcessingTime                 time.Duration `json:"maxProcessingTime"`
	SessionTimeout                    time.Duration `json:"sessionTimeout"`
	RebalanceTimeout                  time.Duration `json:"rebalanceTimeout"`
	HeartbeatInterval                 time.Duration `json:"heartbeatInterval"`
	CloseConsumerWhenThereIsNoMessage time.Duration `json:"closeConsumerWhenThereIsNoMessage"`
}

type ConsumerGroupErrorConfigMap map[string]*ConsumerGroupErrorConfig

func (c ConsumerGroupErrorConfigMap) GetConfigWithDefault(name string) (*ConsumerGroupErrorConfig, error) {
	if config, exists := c[strings.ToLower(name)]; exists {
		if len(config.GroupID) == 0 {
			return nil, NewErrWithArgs("consumer error config 'groupId' is required, config name: %s", name)
		}
		if len(config.Topics) == 0 {
			return nil, NewErrWithArgs("consumer error config 'name' is required, config name: %s", name)
		}
		if len(config.Cron) == 0 {
			return nil, NewErrWithArgs("consumer error config 'cron' is required, config name: %s", name)
		}
		if config.MaxErrorCount == 0 {
			return nil, NewErrWithArgs("consumer error config 'maxErrorCount' is required, config name: %s", name)
		}
		if config.CloseConsumerWhenThereIsNoMessage == 0 {
			config.CloseConsumerWhenThereIsNoMessage = 1 * time.Minute
		}
		if config.CloseConsumerWhenMessageIsNew == 0 {
			config.CloseConsumerWhenMessageIsNew = 1 * time.Minute
		}
		if len(config.FetchMaxBytes) == 0 {
			config.FetchMaxBytes = common.MB
		}
		if config.MaxProcessingTime == 0 {
			config.MaxProcessingTime = 1 * time.Second
		}
		if len(config.Tracer) == 0 {
			config.Tracer = config.GroupID
		}
		if len(config.OffsetInitial) == 0 {
			config.OffsetInitial = OffsetOldest
		}
		if config.SessionTimeout == 0 {
			config.SessionTimeout = 10 * time.Second
		}
		if config.RebalanceTimeout == 0 {
			config.RebalanceTimeout = 60 * time.Second
		}
		if config.HeartbeatInterval == 0 {
			config.HeartbeatInterval = 3 * time.Second
		}
		return config, nil
	}
	return nil, NewErrWithArgs("error consumer config not found: %s", name)
}
