package testdata

import "fmt"

type TestProducerMessage struct {
	Id      int64  `json:"id"`
	Name    string `json:"name"`
	Reason  string `json:"reason"`
	Version int    `json:"version"`
}

func (t *TestProducerMessage) GetConfigName() string {
	return "topic" // topic config name
}

func (t *TestProducerMessage) GetKey() string {
	return fmt.Sprint(t.Id)
}

type TestWrongTypeProducerMessage struct {
	Id      string `json:"id"` // type from int to string
	Name    string `json:"name"`
	Reason  string `json:"reason"`
	Version int    `json:"version"`
}

func (t *TestWrongTypeProducerMessage) GetConfigName() string {
	return "topic" // topic config name
}

func (t *TestWrongTypeProducerMessage) GetKey() string {
	return t.Id
}
