package failure

import (
	"context"
	"sync"
	"time"
)

type Breaker struct {
	mu          sync.Mutex
	threshold   int
	cooldown    time.Duration
	consecutive int
	openUntil   time.Time
}

func NewBreaker(threshold int, cooldown time.Duration) *Breaker {
	return &Breaker{threshold: threshold, cooldown: cooldown}
}

func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consecutive = 0
	b.openUntil = time.Time{}
}

func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.consecutive++
	if b.threshold > 0 && b.consecutive >= b.threshold {
		b.openUntil = time.Now().Add(b.cooldown)
		b.consecutive = 0
	}
}

func (b *Breaker) Wait(ctx context.Context) error {
	b.mu.Lock()
	remaining := time.Until(b.openUntil)
	b.mu.Unlock()

	if remaining <= 0 {
		return nil
	}

	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
