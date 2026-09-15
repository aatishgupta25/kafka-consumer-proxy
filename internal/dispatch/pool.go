package dispatch

import (
	"errors"
	"sync/atomic"

	"github.com/aatishgupta25/kafka-consumer-proxy/internal/ports"
)

type Pool struct {
	workers []ports.Worker
	next    atomic.Uint64
}

func NewPool(workers []ports.Worker) (*Pool, error) {
	if len(workers) == 0 {
		return nil, errors.New("worker pool requires at least one worker")
	}
	return &Pool{workers: workers}, nil
}

func (p *Pool) Next() ports.Worker {
	i := p.next.Add(1) - 1
	return p.workers[i%uint64(len(p.workers))]
}
