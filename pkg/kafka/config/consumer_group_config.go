package config

import (
	"time"
)

type ConsumerGroupConfig struct {
	GroupID           string
	OffsetInitial     OffsetInitial
	FetchMaxBytes     string
	Topics            []string
	MaxProcessingTime time.Duration
	SessionTimeout    time.Duration
	RebalanceTimeout  time.Duration
	HeartbeatInterval time.Duration
	IsErrorConsumer   bool
}
