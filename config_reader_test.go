package partitionscaler

import (
	"github.com/Trendyol/go-kafka-partition-scaler/common"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestShouldReadClusterConfig(t *testing.T) {
	// Given
	configPath := "test/testdata/test_cluster_config.yaml"
	profile := "stage"

	// When
	clusterConfig, err := ReadKafkaClusterConfigWithProfile(configPath, profile)

	// Then
	assert.Nil(t, err)

	defaultNoErrorClusterConfig, err := clusterConfig.GetConfigWithDefault("defaultNoError")
	assert.Nil(t, err)

	assert.Equal(t, "test-client-id", defaultNoErrorClusterConfig.ClientID)
	assert.Equal(t, "default-broker1, default-broker2", defaultNoErrorClusterConfig.Brokers)
	assert.Equal(t, "2.2.0", defaultNoErrorClusterConfig.Version)
	assert.Nil(t, defaultNoErrorClusterConfig.ErrorConfig)
	assert.Nil(t, defaultNoErrorClusterConfig.Auth)
	assert.NotNil(t, defaultNoErrorClusterConfig.ProducerConfig)
	assert.Equal(t, "WaitForAll", string(defaultNoErrorClusterConfig.ProducerConfig.RequiredAcks))
	assert.Equal(t, 5*time.Second, defaultNoErrorClusterConfig.ProducerConfig.Timeout)
	assert.NotNil(t, 1000000, defaultNoErrorClusterConfig.ProducerConfig.MaxMessageBytes)

	defaultClusterConfig, err := clusterConfig.GetConfigWithDefault("default")
	assert.Nil(t, err)

	assert.Equal(t, "test-client-id", defaultClusterConfig.ClientID)
	assert.Equal(t, "default-broker1, default-broker2", defaultClusterConfig.Brokers)
	assert.Equal(t, "2.2.0", defaultClusterConfig.Version)
	assert.NotNil(t, defaultClusterConfig.ErrorConfig)
	assert.Equal(t, "test-error-topic", defaultClusterConfig.ErrorConfig.GroupID)
	assert.Equal(t, "0 */5 * * *", defaultClusterConfig.ErrorConfig.Cron)
	assert.Equal(t, 3, defaultClusterConfig.ErrorConfig.MaxErrorCount)
	assert.Equal(t, 1*time.Minute, defaultClusterConfig.ErrorConfig.CloseConsumerWhenThereIsNoMessage)
	assert.Equal(t, 5*time.Minute, defaultClusterConfig.ErrorConfig.CloseConsumerWhenMessageIsNew)
	assert.Equal(t, 1*time.Minute, defaultClusterConfig.ErrorConfig.MaxProcessingTime)
	assert.Nil(t, defaultClusterConfig.Auth)
	assert.NotNil(t, defaultClusterConfig.ProducerConfig)
	assert.Equal(t, "WaitForLocal", string(defaultClusterConfig.ProducerConfig.RequiredAcks))
	assert.Equal(t, 30*time.Second, defaultClusterConfig.ProducerConfig.Timeout)
	assert.NotNil(t, 2000000, defaultClusterConfig.ProducerConfig.MaxMessageBytes)

	stretchClusterConfig, err := clusterConfig.GetConfigWithDefault("stretch")
	assert.Nil(t, err)

	assert.Equal(t, "test-client-id", stretchClusterConfig.ClientID)
	assert.Equal(t, "stretch-broker1, stretch-broker2", stretchClusterConfig.Brokers)
	assert.Equal(t, "2.2.0", stretchClusterConfig.Version)
	assert.NotNil(t, stretchClusterConfig.ErrorConfig)
	assert.Equal(t, "test-error-topic", stretchClusterConfig.ErrorConfig.GroupID)
	assert.Equal(t, "0 */5 * * *", stretchClusterConfig.ErrorConfig.Cron)
	assert.Equal(t, 3, stretchClusterConfig.ErrorConfig.MaxErrorCount)
	assert.Equal(t, 1*time.Minute, stretchClusterConfig.ErrorConfig.CloseConsumerWhenThereIsNoMessage)
	assert.Equal(t, 5*time.Minute, stretchClusterConfig.ErrorConfig.CloseConsumerWhenMessageIsNew)
	assert.Equal(t, 1*time.Minute, stretchClusterConfig.ErrorConfig.MaxProcessingTime)
	assert.NotNil(t, stretchClusterConfig.Auth)
	assert.Equal(t, "username", stretchClusterConfig.Auth.Username)
	assert.Equal(t, "password", stretchClusterConfig.Auth.Password)
	assert.Len(t, stretchClusterConfig.Auth.Certificates, 2)
	assert.NotNil(t, stretchClusterConfig.ProducerConfig)
	assert.Equal(t, "WaitForLocal", string(stretchClusterConfig.ProducerConfig.RequiredAcks))
	assert.Equal(t, 10*time.Second, stretchClusterConfig.ProducerConfig.Timeout)
	assert.NotNil(t, 1000000, stretchClusterConfig.ProducerConfig.MaxMessageBytes)
}

func TestShouldReadConsumerGroupConfig(t *testing.T) {
	// Given
	configPath := "test/testdata/test_consumer-group-config.yaml"

	// When
	consumerGroupConfig, err := ReadKafkaConsumerGroupConfig(configPath)

	// Then
	assert.Nil(t, err)

	coreConsumerGroupConfig, err := consumerGroupConfig.GetConfigWithDefault("coreExample")
	assert.Nil(t, err)

	assert.Equal(t, "product.kafka-partition-scaler-example.topic.0.core", coreConsumerGroupConfig.GroupID)
	assert.Equal(t, "product.kafka-partition-scaler-example.topic.0", coreConsumerGroupConfig.Name)
	assert.Equal(t, "product.kafka-partition-scaler-example.0.core.retry.0", coreConsumerGroupConfig.Retry)
	assert.Equal(t, "product.kafka-partition-scaler-example.error.0", coreConsumerGroupConfig.Error)
	assert.Equal(t, 3, coreConsumerGroupConfig.RetryCount)
	assert.Equal(t, "stretch", coreConsumerGroupConfig.Cluster)
	assert.Equal(t, 0, coreConsumerGroupConfig.VirtualPartitionCount)
	assert.Equal(t, 0, coreConsumerGroupConfig.VirtualPartitionChanCount)
	assert.Equal(t, 0, coreConsumerGroupConfig.BatchSize)
	assert.Equal(t, false, coreConsumerGroupConfig.UniqueListener)
	assert.Equal(t, 0*time.Second, coreConsumerGroupConfig.ConsumeBatchListenerLatency)
	assert.Equal(t, 1*time.Second, coreConsumerGroupConfig.MaxProcessingTime)
	assert.Equal(t, common.MB, coreConsumerGroupConfig.FetchMaxBytes)
	assert.Equal(t, false, coreConsumerGroupConfig.DisableErrorConsumer)
	assert.Equal(t, "newest", string(coreConsumerGroupConfig.OffsetInitial))
	assert.Equal(t, 10*time.Second, coreConsumerGroupConfig.SessionTimeout)
	assert.Equal(t, 60*time.Second, coreConsumerGroupConfig.RebalanceTimeout)
	assert.Equal(t, 3*time.Second, coreConsumerGroupConfig.HeartbeatInterval)

	singleConsumerGroupConfig, err := consumerGroupConfig.GetConfigWithDefault("singleExample")
	assert.Nil(t, err)

	assert.Equal(t, "product.kafka-partition-scaler-example.topic.0.single", singleConsumerGroupConfig.GroupID)
	assert.Equal(t, "product.kafka-partition-scaler-example.topic.0", singleConsumerGroupConfig.Name)
	assert.Equal(t, "product.kafka-partition-scaler-example.0.single.retry.0", singleConsumerGroupConfig.Retry)
	assert.Equal(t, "product.kafka-partition-scaler-example.error.0", singleConsumerGroupConfig.Error)
	assert.Equal(t, 3, singleConsumerGroupConfig.RetryCount)
	assert.Equal(t, "stretch", singleConsumerGroupConfig.Cluster)
	assert.Equal(t, 10, singleConsumerGroupConfig.VirtualPartitionCount)
	assert.Equal(t, 75, singleConsumerGroupConfig.VirtualPartitionChanCount)
	assert.Equal(t, 0, singleConsumerGroupConfig.BatchSize)
	assert.Equal(t, false, singleConsumerGroupConfig.UniqueListener)
	assert.Equal(t, 0*time.Second, singleConsumerGroupConfig.ConsumeBatchListenerLatency)
	assert.Equal(t, 5*time.Second, singleConsumerGroupConfig.MaxProcessingTime)
	assert.Equal(t, common.MB, singleConsumerGroupConfig.FetchMaxBytes)
	assert.Equal(t, false, singleConsumerGroupConfig.DisableErrorConsumer)
	assert.Equal(t, "oldest", string(singleConsumerGroupConfig.OffsetInitial))
	assert.Equal(t, 100*time.Second, singleConsumerGroupConfig.SessionTimeout)
	assert.Equal(t, 80*time.Second, singleConsumerGroupConfig.RebalanceTimeout)
	assert.Equal(t, 10*time.Second, singleConsumerGroupConfig.HeartbeatInterval)

	batchConsumerGroupConfig, err := consumerGroupConfig.GetConfigWithDefault("batchExample")
	assert.Nil(t, err)

	assert.Equal(t, "product.kafka-partition-scaler-example.topic.0.batch", batchConsumerGroupConfig.GroupID)
	assert.Equal(t, "product.kafka-partition-scaler-example.topic.0", batchConsumerGroupConfig.Name)
	assert.Equal(t, "product.kafka-partition-scaler-example.0.batch.retry.0", batchConsumerGroupConfig.Retry)
	assert.Equal(t, "product.kafka-partition-scaler-example.error.0", batchConsumerGroupConfig.Error)
	assert.Equal(t, 5, batchConsumerGroupConfig.RetryCount)
	assert.Equal(t, "stretch", batchConsumerGroupConfig.Cluster)
	assert.Equal(t, 15, batchConsumerGroupConfig.VirtualPartitionCount)
	assert.Equal(t, 2250, batchConsumerGroupConfig.VirtualPartitionChanCount)
	assert.Equal(t, 30, batchConsumerGroupConfig.BatchSize)
	assert.Equal(t, false, batchConsumerGroupConfig.UniqueListener)
	assert.Equal(t, 2*time.Second, batchConsumerGroupConfig.ConsumeBatchListenerLatency)
	assert.Equal(t, 10*time.Second, batchConsumerGroupConfig.MaxProcessingTime)
	assert.Equal(t, common.MB, batchConsumerGroupConfig.FetchMaxBytes)
	assert.Equal(t, false, batchConsumerGroupConfig.DisableErrorConsumer)
	assert.Equal(t, "newest", string(batchConsumerGroupConfig.OffsetInitial))
	assert.Equal(t, 10*time.Second, batchConsumerGroupConfig.SessionTimeout)
	assert.Equal(t, 60*time.Second, batchConsumerGroupConfig.RebalanceTimeout)
	assert.Equal(t, 3*time.Second, batchConsumerGroupConfig.HeartbeatInterval)

	uniqueConsumerGroupConfig, err := consumerGroupConfig.GetConfigWithDefault("uniqueExample")
	assert.Nil(t, err)

	assert.Equal(t, "product.kafka-partition-scaler-example.topic.0.unique", uniqueConsumerGroupConfig.GroupID)
	assert.Equal(t, "product.kafka-partition-scaler-example.topic.0", uniqueConsumerGroupConfig.Name)
	assert.Equal(t, "product.kafka-partition-scaler-example.0.unique.retry.0", uniqueConsumerGroupConfig.Retry)
	assert.Equal(t, "product.kafka-partition-scaler-example.error.0", uniqueConsumerGroupConfig.Error)
	assert.Equal(t, 2, uniqueConsumerGroupConfig.RetryCount)
	assert.Equal(t, "stretch", uniqueConsumerGroupConfig.Cluster)
	assert.Equal(t, 10, uniqueConsumerGroupConfig.VirtualPartitionCount)
	assert.Equal(t, 7500, uniqueConsumerGroupConfig.VirtualPartitionChanCount)
	assert.Equal(t, 100, uniqueConsumerGroupConfig.BatchSize)
	assert.Equal(t, true, uniqueConsumerGroupConfig.UniqueListener)
	assert.Equal(t, 1*time.Second, uniqueConsumerGroupConfig.ConsumeBatchListenerLatency)
	assert.Equal(t, 2*time.Second, uniqueConsumerGroupConfig.MaxProcessingTime)
	assert.Equal(t, common.MB, uniqueConsumerGroupConfig.FetchMaxBytes)
	assert.Equal(t, false, uniqueConsumerGroupConfig.DisableErrorConsumer)
	assert.Equal(t, "newest", string(uniqueConsumerGroupConfig.OffsetInitial))
	assert.Equal(t, 10*time.Second, uniqueConsumerGroupConfig.SessionTimeout)
	assert.Equal(t, 60*time.Second, uniqueConsumerGroupConfig.RebalanceTimeout)
	assert.Equal(t, 3*time.Second, uniqueConsumerGroupConfig.HeartbeatInterval)
}

func TestShouldReadProducerTopicConfig(t *testing.T) {
	// Given
	configPath := "test/testdata/test-producer-kafka-topic.yml"

	// When
	producerTopicConfigMap, err := ReadKafkaProducerTopicConfig(configPath)

	// Then
	assert.Nil(t, err)

	producerTopic1, err := producerTopicConfigMap.GetConfig("topic1")
	assert.Nil(t, err)

	assert.Equal(t, "test.message.topic1.0", producerTopic1.Name)
	assert.Equal(t, "default", producerTopic1.Cluster)

	producerTopic2, err := producerTopicConfigMap.GetConfig("topic2")
	assert.Nil(t, err)

	assert.Equal(t, "test.message.topic2.0", producerTopic2.Name)
	assert.Equal(t, "stretch", producerTopic2.Cluster)
}
