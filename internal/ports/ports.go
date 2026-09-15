package ports

import (
	"context"

	"github.com/aatishgupta25/kafka-consumer-proxy/internal/model"
)

type Consumer interface {
	Fetch(context.Context) (model.Record, error)
	CommitOffset(context.Context, string, int, int64) error
}

type Worker interface {
	Process(context.Context, model.Record) error
}

type DeadLetterWriter interface {
	Write(context.Context, model.Record, error) error
}
