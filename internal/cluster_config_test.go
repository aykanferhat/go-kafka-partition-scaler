package internal

import (
	"testing"
	"time"

	"github.com/aykanferhat/go-kafka-partition-scaler/common"

	"github.com/aykanferhat/go-kafka-partition-scaler/pkg/kafka"
	"github.com/stretchr/testify/assert"
)

const (
	errorGroupID = "error-group"
)

func Test_ClusterConfig_ShouldReturnBrokers(t *testing.T) {
	// Given
	clusterConfig := &ClusterConfig{
		Brokers: "broker1, broker2, broker3",
		Version: "2.2.0",
		ErrorConfig: &ErrorConfig{
			GroupID:       errorGroupID,
			Cron:          "0 */5 * * *",
			MaxErrorCount: 2,
		},
		ClientID: "client-id",
	}

	// When
	brokers := clusterConfig.GetBrokers()

	// Then
	assert.Len(t, brokers, 3)
	assert.Equal(t, "broker1", brokers[0])
	assert.Equal(t, "broker2", brokers[1])
	assert.Equal(t, "broker3", brokers[2])
}

func Test_ClusterConfigMap_ThrowErrWhenConfigNotFound(t *testing.T) {
	// Given
	clusterConfigMap := make(ClusterConfigMap)
	clusterConfigMap["cluster"] = &ClusterConfig{
		Brokers: "broker1, broker2, broker3",
		Version: "2.2.0",
		ErrorConfig: &ErrorConfig{
			GroupID:       errorGroupID,
			Cron:          "0 */5 * * *",
			MaxErrorCount: 2,
		},
		ClientID: "client-id",
	}

	// When
	clusterConfig, err := clusterConfigMap.GetConfigWithDefault("notFoundCluster")

	// Then
	assert.Nil(t, clusterConfig)
	assert.Equal(t, "cluster config not found: notFoundCluster", err.Error())
}

func Test_ClusterConfigMap_ThrowErrWhenBrokersIsEmpty(t *testing.T) {
	// Given
	clusterConfigMap := make(ClusterConfigMap)
	clusterConfigMap["cluster"] = &ClusterConfig{
		Brokers: "",
		Version: "2.2.0",
		ErrorConfig: &ErrorConfig{
			GroupID:       errorGroupID,
			Cron:          "0 */5 * * *",
			MaxErrorCount: 2,
		},
		ClientID: "client-id",
	}

	// When
	clusterConfig, err := clusterConfigMap.GetConfigWithDefault("cluster")

	// Then
	assert.Nil(t, clusterConfig)
	assert.Equal(t, "cluster config 'brokers' is required, cluster: cluster", err.Error())
}

func Test_ClusterConfigMap_ThrowErrWhenVersionIsEmpty(t *testing.T) {
	// Given
	clusterConfigMap := make(ClusterConfigMap)
	clusterConfigMap["cluster"] = &ClusterConfig{
		Brokers: "broker1, broker2, broker3",
		Version: "",
		ErrorConfig: &ErrorConfig{
			GroupID:       errorGroupID,
			Cron:          "0 */5 * * *",
			MaxErrorCount: 2,
		},
		ClientID: "client-id",
	}

	// When
	clusterConfig, err := clusterConfigMap.GetConfigWithDefault("cluster")

	// Then
	assert.Nil(t, clusterConfig)
	assert.Equal(t, "cluster configs 'version' is required, cluster: cluster", err.Error())
}

func Test_ClusterConfigMap_ThrowErrWhenErrorConfigGroupIdIsEmpty(t *testing.T) {
	// Given
	clusterConfigMap := make(ClusterConfigMap)
	clusterConfigMap["cluster"] = &ClusterConfig{
		Brokers: "broker1, broker2, broker3",
		Version: "2.2.0",
		ErrorConfig: &ErrorConfig{
			GroupID:       "",
			Cron:          "0 */5 * * *",
			MaxErrorCount: 2,
		},
		ClientID: "client-id",
	}

	// When
	clusterConfig, err := clusterConfigMap.GetConfigWithDefault("cluster")

	// Then
	assert.Nil(t, clusterConfig)
	assert.Equal(t, "error consumer 'groupId' is required, cluster: cluster", err.Error())
}

func Test_ClusterConfigMap_ThrowErrWhenErrorConfigCronIsEmpty(t *testing.T) {
	// Given
	clusterConfigMap := make(ClusterConfigMap)
	clusterConfigMap["cluster"] = &ClusterConfig{
		Brokers: "broker1, broker2, broker3",
		Version: "2.2.0",
		ErrorConfig: &ErrorConfig{
			GroupID:       errorGroupID,
			Cron:          "",
			MaxErrorCount: 2,
		},
		ClientID: "client-id",
	}

	// When
	clusterConfig, err := clusterConfigMap.GetConfigWithDefault("cluster")

	// Then
	assert.Nil(t, clusterConfig)
	assert.Equal(t, "error consumer 'cron' is required, cluster: cluster", err.Error())
}

func Test_ClusterConfigMap_ShouldReturnClusterConfig(t *testing.T) {
	// Given
	clusterConfigMap := make(ClusterConfigMap)
	clusterConfigMap["cluster"] = &ClusterConfig{
		Brokers: "broker1, broker2, broker3",
		Version: "2.2.0",
		ErrorConfig: &ErrorConfig{
			GroupID:       errorGroupID,
			Cron:          "0 */5 * * *",
			MaxErrorCount: 2,
		},
		ProducerConfig: &ProducerConfig{},
		ClientID:       "client-id",
	}

	// When
	clusterConfig, err := clusterConfigMap.GetConfigWithDefault("cluster")

	// Then
	assert.Nil(t, err)
	assert.Equal(t, 5*time.Minute, clusterConfig.ErrorConfig.CloseConsumerWhenThereIsNoMessage)
	assert.Equal(t, 5*time.Minute, clusterConfig.ErrorConfig.CloseConsumerWhenMessageIsNew)
	assert.Equal(t, 1*time.Second, clusterConfig.ErrorConfig.MaxProcessingTime)
	assert.Equal(t, 2, clusterConfig.ErrorConfig.MaxErrorCount)

	assert.Equal(t, 10*time.Second, clusterConfig.ProducerConfig.Timeout)
	assert.Equal(t, kafka.WaitForLocal, clusterConfig.ProducerConfig.RequiredAcks)
	assert.Equal(t, common.MB, clusterConfig.ProducerConfig.MaxMessageBytes)
}
