package internal

import (
	"context"

	"github.com/Trendyol/go-kafka-partition-scaler/common"
)

type EndFunc = common.EndFunc

type Tracer interface {
	Start(ctx context.Context, tracerName string) (context.Context, EndFunc)
}

type Segment interface {
	StartSegment(ctx context.Context, tracerName string) EndFunc
}
