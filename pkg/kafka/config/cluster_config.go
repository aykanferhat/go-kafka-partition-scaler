package config

import (
	"time"
)

type ClusterConfig struct {
	ProducerConfig *ProducerConfig
	Auth           *Auth
	ClientID       string
	Version        string
	Brokers        []string
}

type Auth struct {
	Username     string
	Password     string
	Certificates []string
}

type ProducerConfig struct {
	RequiredAcks    RequiredAcks
	Timeout         time.Duration
	MaxMessageBytes string
	Compression     Compression
}