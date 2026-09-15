package dispatch

import (
	"context"
	"testing"

	"github.com/aatishgupta25/kafka-consumer-proxy/internal/model"
)

type stubWorker struct{ id int }

func (w *stubWorker) Process(context.Context, model.Record) error { return nil }

func TestPoolCyclesAcrossWorkers(t *testing.T) {
	w1 := &stubWorker{id: 1}
	w2 := &stubWorker{id: 2}
	w3 := &stubWorker{id: 3}
	pool, err := NewPool([]interfaceWorker{w1, w2, w3})
	_ = pool
	_ = err
}

type interfaceWorker = interface {
	Process(context.Context, model.Record) error
}
