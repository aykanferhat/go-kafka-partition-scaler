package sarama

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/aykanferhat/go-kafka-partition-scaler/common"
	"github.com/aykanferhat/go-kafka-partition-scaler/pkg/kafka/config"
	"github.com/aykanferhat/go-kafka-partition-scaler/pkg/log"
)

func NewSaramaConfig(clusterConfig *config.ClusterConfig, consumerGroupConfig *config.ConsumerGroupConfig) (*sarama.Config, error) {
	if len(clusterConfig.ClientID) == 0 {
		return nil, errors.New("clientId is empty in kafka config")
	}
	if strings.EqualFold(log.Logger.Lvl(), log.DEBUG) {
		sarama.Logger = log.Logger
	}
	saramaConfig := sarama.NewConfig()

	if err := setMetadataConfig(saramaConfig, clusterConfig.ClientID, clusterConfig.Version); err != nil {
		return nil, err
	}
	if clusterConfig.Auth != nil {
		if err := addAuthToConfig(saramaConfig, clusterConfig); err != nil {
			return nil, err
		}
	}
	if err := setProducerConfig(saramaConfig, clusterConfig); err != nil {
		return nil, err
	}
	// consumer
	if consumerGroupConfig != nil {
		offsetInitialIndex, err := getSaramaOffsetInitialIndex(consumerGroupConfig.OffsetInitial)
		if err != nil {
			return nil, err
		}
		saramaConfig.Consumer.Return.Errors = true
		saramaConfig.Consumer.Offsets.Initial = offsetInitialIndex
		saramaConfig.Consumer.Group.Session.Timeout = consumerGroupConfig.SessionTimeout
		saramaConfig.Consumer.Group.Heartbeat.Interval = consumerGroupConfig.HeartbeatInterval
		saramaConfig.Consumer.MaxProcessingTime = consumerGroupConfig.MaxProcessingTime
		saramaConfig.Consumer.Fetch.Default = int32(common.ResolveUnionIntOrStringValue(consumerGroupConfig.FetchMaxBytes))
		saramaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()}
		saramaConfig.Consumer.Group.Rebalance.Timeout = consumerGroupConfig.RebalanceTimeout
		saramaConfig.Consumer.Offsets.AutoCommit.Enable = true
		saramaConfig.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second
	} else {
		saramaConfig.Consumer.Return.Errors = true
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
		saramaConfig.Consumer.MaxProcessingTime = 1 * time.Second
		saramaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()}
		saramaConfig.Consumer.Offsets.AutoCommit.Enable = true
		saramaConfig.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second
	}
	return saramaConfig, nil
}

func setProducerConfig(saramaConfig *sarama.Config, clusterConfig *config.ClusterConfig) error {
	saramaRequiredAcks, err := getSaramaRequiredAcks(clusterConfig.ProducerConfig.RequiredAcks)
	if err != nil {
		return err
	}
	compression, err := getSaramaCodec(clusterConfig.ProducerConfig.Compression)
	if err != nil {
		return err
	}
	saramaConfig.Producer.Retry.Max = 2
	saramaConfig.Producer.Retry.Backoff = 1500 * time.Millisecond
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Return.Errors = true
	saramaConfig.Producer.RequiredAcks = saramaRequiredAcks
	saramaConfig.Producer.Compression = compression
	saramaConfig.Producer.Timeout = clusterConfig.ProducerConfig.Timeout
	saramaConfig.Producer.MaxMessageBytes = common.ResolveUnionIntOrStringValue(clusterConfig.ProducerConfig.MaxMessageBytes)
	return nil
}

func setMetadataConfig(saramaConfig *sarama.Config, clientID string, version string) error {
	v, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		return err
	}
	saramaConfig.ChannelBufferSize = 256
	saramaConfig.ApiVersionsRequest = true
	saramaConfig.Version = v
	saramaConfig.ClientID = clientID

	saramaConfig.Metadata.Retry.Max = 1
	saramaConfig.Metadata.Retry.Backoff = 10 * time.Second
	saramaConfig.Metadata.Full = false

	saramaConfig.Net.ReadTimeout = 3 * time.Minute
	saramaConfig.Net.DialTimeout = 3 * time.Minute
	saramaConfig.Net.WriteTimeout = 3 * time.Minute
	return nil
}

func addAuthToConfig(config *sarama.Config, clusterConfig *config.ClusterConfig) error {
	config.Net.SASL.Enable = true
	config.Net.SASL.User = clusterConfig.Auth.Username
	config.Net.SASL.Password = clusterConfig.Auth.Password
	config.Net.SASL.Handshake = true
	config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &xDGSCRAMClient{HashGeneratorFcn: sHA512} }
	config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	config.Net.TLS.Enable = true
	tlsConfiguration, err := createTLSConfiguration(clusterConfig)
	if err != nil {
		return err
	}
	config.Net.TLS.Config = tlsConfiguration
	return nil
}

func createTLSConfiguration(clusterConfig *config.ClusterConfig) (*tls.Config, error) {
	caCertPool := x509.NewCertPool()
	for _, certificate := range clusterConfig.Auth.Certificates {
		caCert, err := os.ReadFile(certificate)
		if err != nil {
			return nil, err
		}
		caCertPool.AppendCertsFromPEM(caCert)
	}
	return &tls.Config{RootCAs: caCertPool}, nil
}

func getSaramaRequiredAcks(acks config.RequiredAcks) (sarama.RequiredAcks, error) {
	switch acks {
	case config.NoResponse:
		return sarama.NoResponse, nil
	case config.WaitForLocal:
		return sarama.WaitForLocal, nil
	case config.WaitForAll:
		return sarama.WaitForAll, nil
	default:
		return 0, errors.New("RequiredAcks value not match, it should be NoResponse, WaitForLocal or WaitForAll")
	}
}

func getSaramaOffsetInitialIndex(i config.OffsetInitial) (int64, error) {
	switch i {
	case config.OffsetNewest:
		return sarama.OffsetNewest, nil
	case config.OffsetOldest:
		return sarama.OffsetOldest, nil
	default:
		return 0, errors.New("OffsetInitial value not match, it should be OffsetNewest or OffsetOldest")
	}
}

func getSaramaCodec(compression config.Compression) (sarama.CompressionCodec, error) {
	switch compression {
	case config.CompressionNone:
		return sarama.CompressionNone, nil
	case config.CompressionGZIP:
		return sarama.CompressionGZIP, nil
	case config.CompressionSnappy:
		return sarama.CompressionSnappy, nil
	case config.CompressionLZ4:
		return sarama.CompressionLZ4, nil
	case config.CompressionZSTD:
		return sarama.CompressionZSTD, nil
	default:
		return 0, errors.New("compression type not found, it should be none, gzip, snappy, lz4 or zstd")
	}
}
