package internal

import "github.com/aykanferhat/go-kafka-partition-scaler/pkg/log"

type Logger = log.Logger

var logger Logger = log.NewConsoleLog(log.ERROR)

func GetLogger() Logger {
	return logger
}

func SetLogger(l Logger) {
	logger = l
}
