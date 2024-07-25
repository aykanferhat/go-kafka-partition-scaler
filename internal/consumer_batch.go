package internal

import (
	"context"
)

type BatchConsumer interface {
	Consume(ctx context.Context, messages []*ConsumerMessage) map[*ConsumerMessage]error
}
