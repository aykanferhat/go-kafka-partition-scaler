package internal

import (
	"context"
)

type ProducerInterceptor interface {
	OnProduce(ctx context.Context, message *ProducerMessage)
}
