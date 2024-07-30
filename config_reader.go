package partitionscaler

import (
	"github.com/aykanferhat/go-kafka-partition-scaler/common"
	"github.com/aykanferhat/go-kafka-partition-scaler/internal"
	"github.com/aykanferhat/go-kafka-partition-scaler/pkg/viper"
)

type (
	ClusterConfig    = internal.ClusterConfig
	ErrorConfig      = internal.ErrorConfig
	ProducerConfig   = internal.ProducerConfig
	Auth             = internal.Auth
	ClusterConfigMap = internal.ClusterConfigMap
	Admin            = internal.Admin
	ContextKey       = common.ContextKey
)

func ReadKafkaClusterConfigWithProfile(kafkaConfigPath string, profile string) (ClusterConfigMap, error) {
	var conf map[string]*ClusterConfig
	if err := viper.ReadFileWithProfile(profile, &conf, kafkaConfigPath); err != nil {
		return nil, err
	}
	return conf, nil
}

func ReadKafkaClusterConfig(kafkaConfigPath string) (ClusterConfigMap, error) {
	var conf map[string]*ClusterConfig
	if err := viper.ReadFile(&conf, kafkaConfigPath); err != nil {
		return nil, err
	}
	return conf, nil
}

func ReadKafkaConsumerGroupErrorConfig(kafkaConsumerErrorConfigPath string) (ConsumerGroupErrorConfigMap, error) {
	var conf map[string]*ConsumerGroupErrorConfig
	if err := viper.ReadFile(&conf, kafkaConsumerErrorConfigPath); err != nil {
		return nil, err
	}
	return conf, nil
}

func ReadKafkaConsumerGroupConfig(kafkaConsumerConfigPath string) (ConsumerGroupConfigMap, error) {
	var conf map[string]*ConsumerGroupConfig
	if err := viper.ReadFile(&conf, kafkaConsumerConfigPath); err != nil {
		return nil, err
	}
	return conf, nil
}

func ReadKafkaProducerTopicConfig(kafkaProducerConfigPath string) (ProducerTopicConfigMap, error) {
	var conf map[string]*ProducerTopic
	if err := viper.ReadFile(&conf, kafkaProducerConfigPath); err != nil {
		return nil, err
	}
	return conf, nil
}
