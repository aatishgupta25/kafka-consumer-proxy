package dispatch

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aatishgupta25/kafka-consumer-proxy/internal/failure"
	"github.com/aatishgupta25/kafka-consumer-proxy/internal/model"
	"github.com/aatishgupta25/kafka-consumer-proxy/internal/ports"
)

type fakeConsumer struct {
	mu      sync.Mutex
	commits []int64
}

func (c *fakeConsumer) Fetch(context.Context) (model.Record, error) {
	return model.Record{}, errors.New("not used")
}
func (c *fakeConsumer) CommitOffset(_ context.Context, _ string, _ int, offset int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.commits = append(c.commits, offset)
	return nil
}

type delayWorker struct{}

func (delayWorker) Process(_ context.Context, record model.Record) error {
	if record.Offset == 10 {
		time.Sleep(20 * time.Millisecond)
	}
	return nil
}

type failingWorker struct{}

func (failingWorker) Process(context.Context, model.Record) error { return errors.New("poison") }

type blockingWorker struct{}

func (blockingWorker) Process(ctx context.Context, _ model.Record) error {
	<-ctx.Done()
	return ctx.Err()
}

type fakeDLQ struct {
	mu      sync.Mutex
	offsets []int64
}

func (d *fakeDLQ) Write(_ context.Context, record model.Record, _ error) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.offsets = append(d.offsets, record.Offset)
	return nil
}

func TestSlowRecordDoesNotPermitCommitGap(t *testing.T) {
	consumer := &fakeConsumer{}
	pool, _ := NewPool([]ports.Worker{delayWorker{}})
	d := NewDispatcher(consumer, pool, nil, failure.NewBreaker(3, time.Millisecond), 1, 4)

	first := model.Record{Topic: "events", Partition: 0, Offset: 10}
	second := model.Record{Topic: "events", Partition: 0, Offset: 11}
	d.partition(first)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = d.handle(context.Background(), first) }()
	go func() { defer wg.Done(); _ = d.handle(context.Background(), second) }()
	wg.Wait()

	if len(consumer.commits) != 1 || consumer.commits[0] != 12 {
		t.Fatalf("commits = %v, want [12]", consumer.commits)
	}
}

func TestPoisonRecordMovesToDLQAndAdvancesOffset(t *testing.T) {
	consumer := &fakeConsumer{}
	dlq := &fakeDLQ{}
	pool, _ := NewPool([]ports.Worker{failingWorker{}})
	d := NewDispatcher(consumer, pool, dlq, failure.NewBreaker(10, time.Millisecond), 2, 1)
	record := model.Record{Topic: "events", Partition: 0, Offset: 5}

	if err := d.handle(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if len(dlq.offsets) != 1 || dlq.offsets[0] != 5 {
		t.Fatalf("dlq offsets = %v, want [5]", dlq.offsets)
	}
	if len(consumer.commits) != 1 || consumer.commits[0] != 6 {
		t.Fatalf("commits = %v, want [6]", consumer.commits)
	}
}

func TestWorkerTimeoutMovesRecordToDLQ(t *testing.T) {
	consumer := &fakeConsumer{}
	dlq := &fakeDLQ{}
	pool, _ := NewPool([]ports.Worker{blockingWorker{}})
	d := NewDispatcher(consumer, pool, dlq, failure.NewBreaker(10, time.Millisecond), 1, 1)
	d.SetWorkerTimeout(5 * time.Millisecond)
	record := model.Record{Topic: "events", Partition: 0, Offset: 7}

	if err := d.handle(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if len(dlq.offsets) != 1 || dlq.offsets[0] != 7 {
		t.Fatalf("dlq offsets = %v, want [7]", dlq.offsets)
	}
	if len(consumer.commits) != 1 || consumer.commits[0] != 8 {
		t.Fatalf("commits = %v, want [8]", consumer.commits)
	}
}


type drainingConsumer struct {
	record  model.Record
	once    sync.Once
	mu      sync.Mutex
	commits []int64
}

func (c *drainingConsumer) Fetch(ctx context.Context) (model.Record, error) {
	first := false
	c.once.Do(func() { first = true })
	if first {
		return c.record, nil
	}
	<-ctx.Done()
	return model.Record{}, ctx.Err()
}

func (c *drainingConsumer) CommitOffset(_ context.Context, _ string, _ int, offset int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.commits = append(c.commits, offset)
	return nil
}

type drainingWorker struct {
	started chan struct{}
	release chan struct{}
}

func (w drainingWorker) Process(ctx context.Context, _ model.Record) error {
	close(w.started)
	select {
	case <-w.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestShutdownDrainsInflightWorkBeforeCancelingWorkers(t *testing.T) {
	consumer := &drainingConsumer{
		record: model.Record{Topic: "events", Partition: 0, Offset: 21},
	}
	worker := drainingWorker{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	pool, _ := NewPool([]ports.Worker{worker})
	d := NewDispatcher(consumer, pool, nil, failure.NewBreaker(3, time.Millisecond), 1, 1)
	d.SetDrainTimeout(250 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()

	<-worker.started
	cancel()

	time.Sleep(10 * time.Millisecond)
	close(worker.release)

	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}

	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	if len(consumer.commits) != 1 || consumer.commits[0] != 22 {
		t.Fatalf("commits = %v, want [22]", consumer.commits)
	}
}
