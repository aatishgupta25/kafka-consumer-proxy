package dispatch

import (
	"context"
	"fmt"
	"sync"

	committracker "github.com/aatishgupta25/kafka-consumer-proxy/internal/commit"
	"github.com/aatishgupta25/kafka-consumer-proxy/internal/failure"
	"github.com/aatishgupta25/kafka-consumer-proxy/internal/model"
	"github.com/aatishgupta25/kafka-consumer-proxy/internal/ports"
)

type partitionKey struct {
	topic     string
	partition int
}

type partitionState struct {
	mu      sync.Mutex
	tracker *committracker.Tracker
}

type Dispatcher struct {
	consumer    ports.Consumer
	workers     *Pool
	dlq         ports.DeadLetterWriter
	breaker     *failure.Breaker
	maxAttempts int
	maxInFlight int

	mu         sync.Mutex
	partitions map[partitionKey]*partitionState
}

func NewDispatcher(consumer ports.Consumer, workers *Pool, dlq ports.DeadLetterWriter, breaker *failure.Breaker, maxAttempts, maxInFlight int) *Dispatcher {
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	if maxInFlight < 1 {
		maxInFlight = 1
	}
	return &Dispatcher{
		consumer: consumer, workers: workers, dlq: dlq, breaker: breaker,
		maxAttempts: maxAttempts, maxInFlight: maxInFlight,
		partitions: make(map[partitionKey]*partitionState),
	}
}

func (d *Dispatcher) Run(ctx context.Context) error {
	sem := make(chan struct{}, d.maxInFlight)
	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		if err := d.breaker.Wait(ctx); err != nil {
			return err
		}

		record, err := d.consumer.Fetch(ctx)
		if err != nil {
			return err
		}

		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}

		wg.Add(1)
		go func(record model.Record) {
			defer wg.Done()
			defer func() { <-sem }()
			_ = d.handle(ctx, record)
		}(record)
	}
}

func (d *Dispatcher) handle(ctx context.Context, record model.Record) error {
	var lastErr error
	for attempt := 0; attempt < d.maxAttempts; attempt++ {
		if err := d.workers.Next().Process(ctx, record); err == nil {
			d.breaker.Success()
			return d.ack(ctx, record)
		} else {
			lastErr = err
			d.breaker.Failure()
		}
	}

	if d.dlq == nil {
		return fmt.Errorf("record %d exhausted retries: %w", record.Offset, lastErr)
	}
	if err := d.dlq.Write(ctx, record, lastErr); err != nil {
		return fmt.Errorf("write record %d to dlq: %w", record.Offset, err)
	}
	return d.ack(ctx, record)
}

func (d *Dispatcher) ack(ctx context.Context, record model.Record) error {
	state := d.partition(record)
	state.mu.Lock()
	defer state.mu.Unlock()

	nextOffset, advanced := state.tracker.Ack(record.Offset)
	if !advanced {
		return nil
	}
	return d.consumer.CommitOffset(ctx, record.Topic, record.Partition, nextOffset)
}

func (d *Dispatcher) partition(record model.Record) *partitionState {
	key := partitionKey{topic: record.Topic, partition: record.Partition}
	d.mu.Lock()
	defer d.mu.Unlock()
	if state, ok := d.partitions[key]; ok {
		return state
	}
	state := &partitionState{tracker: committracker.NewTracker(record.Offset)}
	d.partitions[key] = state
	return state
}
