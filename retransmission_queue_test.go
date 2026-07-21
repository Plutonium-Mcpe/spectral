package spectral

import (
	"testing"
	"time"
)

func TestShiftLostPacketThreshold(t *testing.T) {
	q := newRetransmissionQueue()
	now := time.Now()
	for i := uint32(1); i <= 10; i++ {
		q.add(now, i, []byte{byte(i)})
	}

	lost := q.shiftLost(now, 10, 3)
	if len(lost) != 7 {
		t.Fatalf("expected 7 lost (seq 1..7), got %d", len(lost))
	}
	for _, l := range lost {
		if l.gaveUp {
			t.Fatal("unexpected gaveUp on fresh loss")
		}
	}
	if len(q.queue) != 10 {
		t.Fatalf("all 10 packets should remain queued (7 rescheduled + 3 in-flight), got %d", len(q.queue))
	}
	for _, entry := range q.queue {
		if entry.sequenceID <= 7 && entry.attempts != 1 {
			t.Fatalf("lost seq %d should have 1 attempt, got %d", entry.sequenceID, entry.attempts)
		}
		if entry.sequenceID > 7 && entry.attempts != 0 {
			t.Fatalf("in-flight seq %d should have 0 attempts, got %d", entry.sequenceID, entry.attempts)
		}
	}
}

func TestShiftLostBelowThreshold(t *testing.T) {
	q := newRetransmissionQueue()
	now := time.Now()
	q.add(now, 1, []byte{1})
	q.add(now, 2, []byte{2})
	if lost := q.shiftLost(now, 2, 3); len(lost) != 0 {
		t.Fatalf("no loss expected when largestAcked <= threshold, got %d", len(lost))
	}
	if len(q.queue) != 2 {
		t.Fatalf("queue should be untouched, got %d", len(q.queue))
	}
}

func TestShiftLostGaveUp(t *testing.T) {
	q := newRetransmissionQueue()
	now := time.Now()
	q.add(now, 1, []byte{1})
	q.queue[0].attempts = retransmissionAttempts - 1

	lost := q.shiftLost(now, 100, 3)
	if len(lost) != 1 || !lost[0].gaveUp {
		t.Fatalf("expected 1 gaveUp packet, got %+v", lost)
	}
	if len(q.queue) != 0 {
		t.Fatalf("gaveUp packet must be removed from the queue, len=%d", len(q.queue))
	}
}
