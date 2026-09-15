package dispatch

import (
	"context"
	"testing"

	"github.com/aatishgupta25/kafka-consumer-proxy/internal/model"
	"github.com/aatishgupta25/kafka-consumer-proxy/internal/ports"
)

type stubWorker struct{ id int }

func (w *stubWorker) Process(context.Context, model.Record) error { return nil }

func TestPoolCyclesAcrossWorkers(t *testing.T) {
	w1 := &stubWorker{id: 1}
	w2 := &stubWorker{id: 2}
	w3 := &stubWorker{id: 3}
	pool, err := NewPool([]ports.Worker{w1, w2, w3})
	if err != nil {
		t.Fatal(err)
	}

	want := []ports.Worker{w1, w2, w3, w1}
	for i, expected := range want {
		if got := pool.Next(); got != expected {
			t.Fatalf("pick %d = %#v, want %#v", i, got, expected)
		}
	}
}
