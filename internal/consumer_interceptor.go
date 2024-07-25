package internal

import (
	"context"
)

type ConsumerInterceptor interface {
	OnConsume(ctx context.Context, message *ConsumerMessage) context.Context
}
