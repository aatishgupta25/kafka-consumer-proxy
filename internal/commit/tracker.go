package commit

import "sync"

// Tracker advances only across the contiguous acknowledged prefix.
type Tracker struct {
	mu      sync.Mutex
	next    int64
	acked   map[int64]struct{}
}

func NewTracker(firstOffset int64) *Tracker {
	return &Tracker{next: firstOffset, acked: make(map[int64]struct{})}
}

// Ack records completion and returns the next Kafka offset that is safe to commit.
// A false second return means the contiguous prefix did not advance.
func (t *Tracker) Ack(offset int64) (int64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if offset < t.next {
		return t.next, false
	}
	t.acked[offset] = struct{}{}
	start := t.next
	for {
		if _, ok := t.acked[t.next]; !ok {
			break
		}
		delete(t.acked, t.next)
		t.next++
	}
	return t.next, t.next != start
}

func (t *Tracker) Next() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.next
}
