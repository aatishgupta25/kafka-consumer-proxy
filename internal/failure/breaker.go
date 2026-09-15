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
	de b.mu.Unlock()
}
