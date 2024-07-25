package internal

import (
	"strings"
)

type Message interface {
	GetConfigName() string
	GetKey() string
}

type CustomMessage struct {
	Body  any
	Topic *ProducerTopic
	Key   string
}

type ProducerTopic struct {
	Name    string `json:"name"`
	Cluster string `json:"cluster"`
}

type ProducerTopicConfigMap map[string]*ProducerTopic

func (c ProducerTopicConfigMap) GetConfig(name string) (*ProducerTopic, error) {
	if config, exists := c[strings.ToLower(name)]; exists {
		if len(config.Name) == 0 {
			return nil, NewErrWithArgs("producer topic 'name' is required, config name: %s", name)
		}
		if len(config.Cluster) == 0 {
			return nil, NewErrWithArgs("producer topic 'cluster' is required, config name: %s", name)
		}
		return config, nil
	}
	return nil, NewErrWithArgs("producer topic config not found by name: %s", name)
}
