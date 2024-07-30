package internal

import "github.com/aykanferhat/go-kafka-partition-scaler/pkg/log"

type Log = log.Log

func SetLog(l Log) {
	log.Logger = l
}
