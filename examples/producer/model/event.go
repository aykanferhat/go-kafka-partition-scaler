package model

import "fmt"

type Event struct {
	EventType string `json:"eventType"`
	Id        int    `json:"id"`
}

func (t *Event) GetConfigName() string {
	return "producerTopic"
}

func (t *Event) GetKey() string {
	return fmt.Sprint(t.Id)
}
