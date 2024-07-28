package internal

import (
	"time"

	"github.com/Trendyol/go-kafka-partition-scaler/pkg/csmap"
	"github.com/Trendyol/go-kafka-partition-scaler/pkg/kafka"
)

type Status string

const (
	StartedListening               Status = "STARTED_LISTENING"
	ListenedMessage                Status = "LISTENED_MESSAGE"
	ErrorConsumerOccurredViolation Status = "ERROR_CONSUMER_OCCURRED_VIOLATION"
	AssignedTopicPartition         Status = "ASSIGNED_TOPIC_PARTITION"
	UnassignedTopicPartition       Status = "UNASSIGNED_TOPIC_PARTITION"
)

type ConsumerGroupStatus struct {
	Time      time.Time
	Topic     string
	Status    Status
	Offset    int64
	Partition int32
}

func (t ConsumerGroupStatus) GetKey() string {
	return getKey(t.Topic, t.Partition)
}

func (t ConsumerGroupStatus) IsStarted() bool {
	return t.Status == StartedListening || t.Status == ListenedMessage
}

type ConsumerGroupStatusListener struct {
	consumerGroupStatusMap *csmap.ConcurrentSwissMap[string, *ConsumerGroupStatus]
	startedChan            chan bool
	stoppedChan            chan bool
}

func (listener *ConsumerGroupStatusListener) Listen(status *ConsumerGroupStatus) {
	listener.consumerGroupStatusMap.Store(status.GetKey(), status)
}

func (listener *ConsumerGroupStatusListener) WaitConsumerStart() {
	<-listener.startedChan
}

func (listener *ConsumerGroupStatusListener) WaitConsumerStop() {
	<-listener.stoppedChan
}

func (listener *ConsumerGroupStatusListener) HandleConsumerGroupStatus() kafka.ConsumerStatusHandler {
	return func(topic string, partition int32, status bool) {
		s := UnassignedTopicPartition
		if status {
			s = AssignedTopicPartition
		}
		listener.Listen(&ConsumerGroupStatus{Time: time.Now(), Topic: topic, Status: s, Partition: partition, Offset: -2})
	}
}

func (listener *ConsumerGroupStatusListener) listenConsumerStart() {
	go func() {
		for {
			count := listener.consumerGroupStatusMap.Count()
			if count == 0 {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			allStarted := true
			listener.consumerGroupStatusMap.Range(func(key string, status *ConsumerGroupStatus) bool {
				if !status.IsStarted() {
					allStarted = false
					return true
				}
				return false
			})
			if allStarted {
				listener.startedChan <- true
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
}

func (listener *ConsumerGroupStatusListener) listenConsumerStop() {
	go func() {
		for {
			allStopped := false
			if listener.consumerGroupStatusMap.Count() == 0 {
				listener.stoppedChan <- true
				return
			}
			listener.consumerGroupStatusMap.Range(func(key string, status *ConsumerGroupStatus) bool {
				if status.IsStarted() {
					allStopped = false
					return true
				}
				allStopped = true
				return false
			})
			if allStopped {
				listener.stoppedChan <- true
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
}

func newConsumerGroupStatusListener() *ConsumerGroupStatusListener {
	return &ConsumerGroupStatusListener{
		consumerGroupStatusMap: csmap.Create[string, *ConsumerGroupStatus](0),
		startedChan:            make(chan bool),
		stoppedChan:            make(chan bool),
	}
}
