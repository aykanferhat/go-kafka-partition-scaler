package common

import "context"

type ContextKey string

func (c ContextKey) String() string {
	return string(c)
}

const (
	RetryTopicCountKey ContextKey = "X-RetryCount"
	ErrorTopicCountKey ContextKey = "X-ErrorCount"
	TargetTopicKey     ContextKey = "X-TargetTopic"
	ErrorMessageKey    ContextKey = "X-ErrorMessage"

	GroupID                  ContextKey = "Z-GroupID"
	Topic                    ContextKey = "Z-Topic"
	Partition                ContextKey = "Z-Partition"
	MessageConsumedTimestamp ContextKey = "Z-Timestamp"
)

func AddToContext(ctx context.Context, key ContextKey, value any) context.Context {
	values := ctx.Value(key)
	if values == nil {
		return context.WithValue(ctx, key, value)
	}
	return ctx
}

func GetFromContext[T any](ctx context.Context, key ContextKey) T {
	values := ctx.Value(key)
	var result T
	if values == nil {
		return result
	}
	return values.(T)
}
