package commit

import "testing"

func TestTrackerWaitsForContiguousPrefix(t *testing.T) {
	tracker := NewTracker(10)

	if next, advanced := tracker.Ack(11); advanced || next != 10 {
		t.Fatalf("ack 11 = (%d, %v), want (10, false)", next, advanced)
	}
	if next, advanced := tracker.Ack(12); advanced || next != 10 {
		t.Fatalf("ack 12 = (%d, %v), want (10, false)", next, advanced)
	}
	if next, advanced := tracker.Ack(10); !advanced || next != 13 {
		t.Fatalf("ack 10 = (%d, %v), want (13, true)", next, advanced)
	}
}

func TestTrackerIgnoresDuplicateAck(t *testing.T) {
	tracker := NewTracker(4)
	tracker.Ack(4)
	if next, advanced := tracker.Ack(4); advanced || next != 5 {
		t.Fatalf("duplicate ack = (%d, %v), want (5, false)", next, advanced)
	}
}
