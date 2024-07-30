package internal

import (
	"strings"
	"time"

	"github.com/aykanferhat/go-kafka-partition-scaler/common"

	"github.com/aykanferhat/go-kafka-partition-scaler/pkg/kafka/config"

	"github.com/aykanferhat/go-kafka-partition-scaler/pkg/kafka"
)

type ClusterConfig struct {
	ClusterName    string          `json:"-"`
	ErrorConfig    *ErrorConfig    `json:"errorConfig"`
	ProducerConfig *ProducerConfig `json:"producerConfig"`
	Auth           *Auth           `json:"auth"`
	ClientID       string          `json:"clientId"`
	Brokers        string          `json:"brokers"`
	Version        string          `json:"version"`
}

func (config *ClusterConfig) GetBrokers() []string {
	return strings.Split(strings.ReplaceAll(config.Brokers, " ", ""), ",")
}

type Auth struct {
	Username     string   `json:"username"`
	Password     string   `json:"password"`
	Certificates []string `json:"certificates"`
}

type ErrorConfig struct {
	GroupID                           string        `json:"groupId"`
	Cron                              string        `json:"cron"`
	Tracer                            string        `json:"tracer"`
	FetchMaxBytes                     string        `json:"fetchMaxBytes"`
	OffsetInitial                     OffsetInitial `json:"offsetInitial"`
	MaxErrorCount                     int           `json:"maxErrorCount"`
	MaxProcessingTime                 time.Duration `json:"maxProcessingTime"`
	CloseConsumerWhenThereIsNoMessage time.Duration `json:"closeConsumerWhenThereIsNoMessage"`
	CloseConsumerWhenMessageIsNew     time.Duration `json:"closeConsumerWhenMessageIsNew"`
	SessionTimeout                    time.Duration `json:"sessionTimeout"`
	RebalanceTimeout                  time.Duration `json:"rebalanceTimeout"`
	HeartbeatInterval                 time.Duration `json:"heartbeatInterval"`
}

type (
	RequiredAcks  = config.RequiredAcks
	OffsetInitial = config.OffsetInitial
	Compression   = config.Compression
)

type ProducerConfig struct {
	RequiredAcks    RequiredAcks  `json:"requiredAcks"`
	MaxMessageBytes string        `json:"maxMessageBytes"`
	Compression     Compression   `json:"compression"`
	Timeout         time.Duration `json:"timeout"`
}

type ClusterConfigMap map[string]*ClusterConfig

func (c ClusterConfigMap) GetConfigWithDefault(name string) (*ClusterConfig, error) {
	cc, exists := c[strings.ToLower(name)]
	if !exists {
		return nil, NewErrWithArgs("cluster config not found: %s", name)
	}
	cc.ClusterName = name
	if err := validateClusterConfig(cc); err != nil {
		return nil, err
	}
	return cc, nil
}

func validateClusterConfig(clusterConfig *ClusterConfig) error {
	if len(clusterConfig.Brokers) == 0 {
		return NewErrWithArgs("cluster config 'brokers' is required, cluster: %s", clusterConfig.ClusterName)
	}
	if len(clusterConfig.Version) == 0 {
		return NewErrWithArgs("cluster configs 'version' is required, cluster: %s", clusterConfig.ClusterName)
	}
	if err := setErrorConfigDefaults(clusterConfig); err != nil {
		return err
	}
	setProducerConfigDefaults(clusterConfig)
	return nil
}

func setErrorConfigDefaults(config *ClusterConfig) error {
	if config.ErrorConfig == nil {
		return nil
	}
	errorConfig := config.ErrorConfig
	if len(errorConfig.GroupID) == 0 {
		return NewErrWithArgs("error consumer 'groupId' is required, cluster: %s", config.ClusterName)
	}
	if len(errorConfig.Cron) == 0 {
		return NewErrWithArgs("error consumer 'cron' is required, cluster: %s", config.ClusterName)
	}
	if errorConfig.MaxErrorCount == 0 {
		errorConfig.MaxErrorCount = 1
	}
	if errorConfig.MaxProcessingTime == 0 {
		errorConfig.MaxProcessingTime = 1 * time.Second
	}
	if errorConfig.CloseConsumerWhenThereIsNoMessage == 0 {
		errorConfig.CloseConsumerWhenThereIsNoMessage = 5 * time.Minute
	}
	if errorConfig.CloseConsumerWhenMessageIsNew == 0 {
		errorConfig.CloseConsumerWhenMessageIsNew = 5 * time.Minute
	}
	if len(errorConfig.FetchMaxBytes) == 0 {
		errorConfig.FetchMaxBytes = common.MB
	}
	if len(errorConfig.OffsetInitial) == 0 {
		errorConfig.OffsetInitial = OffsetOldest
	}
	if errorConfig.SessionTimeout == 0 {
		errorConfig.SessionTimeout = 10 * time.Second
	}
	if errorConfig.RebalanceTimeout == 0 {
		errorConfig.RebalanceTimeout = 60 * time.Second
	}
	if errorConfig.HeartbeatInterval == 0 {
		errorConfig.HeartbeatInterval = 3 * time.Second
	}
	return nil
}

func setProducerConfigDefaults(config *ClusterConfig) {
	if config.ProducerConfig == nil {
		config.ProducerConfig = &ProducerConfig{}
	}
	producerConfig := config.ProducerConfig
	if len(producerConfig.RequiredAcks) == 0 {
		producerConfig.RequiredAcks = kafka.WaitForLocal
	}
	if producerConfig.Timeout == 0 {
		producerConfig.Timeout = 10 * time.Second
	}
	if len(producerConfig.MaxMessageBytes) == 0 {
		producerConfig.MaxMessageBytes = common.MB
	}
	if len(producerConfig.Compression) == 0 {
		producerConfig.Compression = kafka.CompressionNone
	}
}

func (c ClusterConfigMap) SetAuth(cluster, username, password string, certificatePaths []string) error {
	clusterConfig, err := c.GetConfigWithDefault(cluster)
	if err != nil {
		return err
	}
	if clusterConfig.Auth != nil {
		clusterConfig.Auth.Username = username
		clusterConfig.Auth.Username = password
		clusterConfig.Auth.Certificates = certificatePaths
		return nil
	}
	clusterConfig.Auth = &Auth{
		Username:     username,
		Password:     password,
		Certificates: certificatePaths,
	}
	return nil
}

func mapToClusterConfig(clusterConfig *ClusterConfig) *kafka.ClusterConfig {
	c := &kafka.ClusterConfig{
		Brokers:  clusterConfig.GetBrokers(),
		Version:  clusterConfig.Version,
		ClientID: clusterConfig.ClientID,
	}
	if clusterConfig.ProducerConfig != nil {
		c.ProducerConfig = kafka.NewProducerConfig(
			clusterConfig.ProducerConfig.RequiredAcks,
			clusterConfig.ProducerConfig.Compression,
			clusterConfig.ProducerConfig.Timeout,
			clusterConfig.ProducerConfig.MaxMessageBytes,
		)
	}
	if clusterConfig.Auth != nil {
		c.Auth = kafka.NewAuthConfig(clusterConfig.Auth.Username, clusterConfig.Auth.Password, clusterConfig.Auth.Certificates)
	}
	return c
}

func mapToConsumerGroupConfig(consumerGroupConfig *ConsumerGroupConfig, isErrorConsumer bool) *kafka.ConsumerGroupConfig {
	return &kafka.ConsumerGroupConfig{
		GroupID:           consumerGroupConfig.GroupID,
		Topics:            consumerGroupConfig.GetTopics(),
		MaxProcessingTime: consumerGroupConfig.MaxProcessingTime,
		FetchMaxBytes:     consumerGroupConfig.FetchMaxBytes,
		OffsetInitial:     consumerGroupConfig.OffsetInitial,
		SessionTimeout:    consumerGroupConfig.SessionTimeout,
		RebalanceTimeout:  consumerGroupConfig.RebalanceTimeout,
		HeartbeatInterval: consumerGroupConfig.HeartbeatInterval,
		IsErrorConsumer:   isErrorConsumer,
	}
}
