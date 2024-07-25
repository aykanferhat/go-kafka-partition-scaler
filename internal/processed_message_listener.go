package internal

import (
	"sort"
	"sync"
	"time"

	"github.com/Trendyol/go-kafka-partition-scaler/pkg/kafka"
)

type ProcessedMessageListener interface {
	ResetTicker(duration time.Duration)
	SetFirstConsumedMessage(topic string, partition int32, offset int64)
	Publish(message *ConsumerMessage)
	IsChannelClosed() bool
	Close()
	LastCommittedOffset() int64
}

type processedMessageListener struct {
	commitMessageFunc             kafka.CommitMessageFunc
	messageChan                   chan *processedMessage
	firstConsumedMessage          *firstConsumedMessage
	lastCommittedMessage          *lastCommittedMessage
	processMessageMutex           *sync.Mutex
	processMessageTicker          *time.Ticker
	waitGroup                     *sync.WaitGroup
	closeOnce                     *sync.Once
	topic                         string
	messages                      []*processedMessage
	partition                     int32
	messageChanClosed             bool
	firstConsumedMessageProcessed bool
}

type firstConsumedMessage struct {
	topic     string
	partition int32
	offset    int64
}

type lastCommittedMessage struct {
	date   time.Time
	offset int64
}

func (listener *processedMessageListener) SetFirstConsumedMessage(topic string, partition int32, offset int64) {
	listener.firstConsumedMessage = &firstConsumedMessage{topic: topic, partition: partition, offset: offset}
}

func (listener *processedMessageListener) ResetTicker(duration time.Duration) {
	listener.processMessageTicker.Reset(duration)
}

func (listener *processedMessageListener) Publish(message *ConsumerMessage) {
	if listener.messageChanClosed {
		return
	}
	listener.waitGroup.Add(1)
	listener.messageChan <- newProcessedMessage(message)
}

func (listener *processedMessageListener) IsChannelClosed() bool {
	return listener.messageChanClosed
}

func (listener *processedMessageListener) listen() {
	go func() {
		for message := range listener.messageChan {
			listener.processMessageMutex.Lock()
			listener.messages = append(listener.messages, message)
			listener.processMessageMutex.Unlock()
			listener.waitGroup.Done()
		}
	}()
}

func (listener *processedMessageListener) scheduleProcessedMessagesForCommit() {
	go func() {
		for range listener.processMessageTicker.C {
			listener.processMessageTicker.Reset(1 * time.Second)
			listener.processMessageMutex.Lock()
			listener.commitMessages()
			listener.processMessageMutex.Unlock()
		}
	}()
}

func (listener *processedMessageListener) LastCommittedOffset() int64 {
	return listener.lastCommittedMessage.offset
}

func (listener *processedMessageListener) commitMessages() {
	sort.Slice(listener.messages, func(i, j int) bool {
		return listener.messages[i].Offset < listener.messages[j].Offset
	})
	committedMessageIndex := 0
	committed := false
	for i, message := range listener.messages {
		if !listener.firstConsumedMessageProcessed {
			if listener.firstConsumedMessage == nil || listener.firstConsumedMessage.offset != message.Offset {
				break
			}
			listener.firstConsumedMessageProcessed = true
		}
		if message.Offset-listener.lastCommittedMessage.offset > 1 {
			diff := time.Since(listener.lastCommittedMessage.date)
			if diff.Nanoseconds() < 1*time.Minute.Nanoseconds() {
				break
			}
		}
		listener.commitMessage(message)
		committedMessageIndex = i
		committed = true
	}
	if !committed {
		return
	}
	listener.messages = listener.messages[committedMessageIndex+1:]
}

func (listener *processedMessageListener) commitMessage(message *processedMessage) {
	listener.commitMessageFunc(message.Topic, message.Partition, message.Offset)
	listener.lastCommittedMessage.offset = message.Offset
	listener.lastCommittedMessage.date = time.Now()
}

func (listener *processedMessageListener) Close() {
	listener.closeOnce.Do(func() {
		listener.waitGroup.Wait()

		listener.messageChanClosed = true
		close(listener.messageChan)
		listener.firstConsumedMessage = nil

		// commit processed message before close
		listener.processMessageMutex.Lock()
		listener.commitMessages()
		listener.processMessageMutex.Unlock()

		listener.processMessageTicker.Stop()
	})
}

func NewProcessedMessageListener(
	topic string,
	partition int32,
	virtualPartition int,
	commitMessageFunc kafka.CommitMessageFunc,
) ProcessedMessageListener {
	listener := &processedMessageListener{
		topic:                topic,
		partition:            partition,
		commitMessageFunc:    commitMessageFunc,
		messageChan:          make(chan *processedMessage, virtualPartition*10),
		messages:             []*processedMessage{},
		processMessageTicker: time.NewTicker(1 * time.Second),
		processMessageMutex:  &sync.Mutex{},
		waitGroup:            &sync.WaitGroup{},
		closeOnce:            &sync.Once{},
		lastCommittedMessage: &lastCommittedMessage{},
	}
	listener.listen()
	listener.scheduleProcessedMessagesForCommit()
	return listener
}
