package log

import partitionscaler "github.com/Trendyol/go-kafka-partition-scaler"

// You can set your logger to kafka

type Logger struct{}

func NewLogger() partitionscaler.Log {
	return &Logger{}
}

func (l Logger) Infof(string, ...interface{}) {
	// implement me
}

func (l Logger) Debugf(string, ...interface{}) {
	// implement me
}

func (l Logger) Errorf(string, ...interface{}) {
	// implement me
}

func (l Logger) Print(...interface{}) {
	// implement me
}

func (l Logger) Printf(string, ...interface{}) {
	// implement me
}

func (l Logger) Println(...interface{}) {
	// implement me
}

func (l Logger) Lvl() string {
	return ""
}