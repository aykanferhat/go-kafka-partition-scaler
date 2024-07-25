package internal

import "github.com/Trendyol/go-kafka-partition-scaler/pkg/log"

type Log = log.Log

func SetLog(l Log) {
	log.Logger = l
}
