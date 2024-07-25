package internal

import (
	"context"
)

type ConsumerErrorInterceptor interface {
	OnError(ctx context.Context, message *ConsumerMessage, err error)
}
