package internal

import (
	"fmt"
	"time"
)

type KafkaPartitionScalerErr struct {
	Instant time.Time `json:"instant"`
	Detail  string    `json:"detail"`
}

func (err KafkaPartitionScalerErr) Error() string {
	return err.Detail
}

func NewErrWithArgs(detail string, a ...any) error {
	return NewErr(fmt.Sprintf(detail, a...))
}

func NewErr(detail string) error {
	return &KafkaPartitionScalerErr{
		Detail:  detail,
		Instant: time.Now(),
	}
}
