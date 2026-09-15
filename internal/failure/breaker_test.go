package failure

import (
	"context"
	"testing"
	"time"
)

func TestBreakerBlocksAfterThreshold(t *testing.T) {
	breaker := NewBreaker(2, 20*time.Millisecond)
	breaker.Failure()
	breaker.Failure()

	start := time.Now()
	if err := breaker.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 15*time.Millisecond {
		t.Fatal("breaker did not delay dispatch")
	}
}

func TestSuccessResetsBreaker(t *testing.T) {
	breaker := NewBreaker(2, time.Second)
	breaker.Failure()
	breaker.Success()
	breaker.Failure()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := breaker.Wait(ctx); err != nil {
		t.Fatalf("breaker opened after reset: %v", err)
	}
}
