package spectral

import "testing"

func TestReceiveQueueAllowsExpectedWhenFull(t *testing.T) {
	q := newReceiveQueue()

	for sequenceID := uint32(2); sequenceID <= maxReceiveQueueEntries+1; sequenceID++ {
		if q.add(sequenceID) != receiveAccepted {
			t.Fatalf("expected sequence %d to be queued", sequenceID)
		}
	}

	if len(q.queue) != maxReceiveQueueEntries {
		t.Fatalf("expected queue length %d, got %d", maxReceiveQueueEntries, len(q.queue))
	}

	if q.add(1) != receiveAccepted {
		t.Fatal("expected missing sequence to be admitted when queue is full")
	}

	if want, got := uint32(maxReceiveQueueEntries+2), q.expected; got != want {
		t.Fatalf("expected next sequence %d, got %d", want, got)
	}

	if len(q.queue) != 0 {
		t.Fatalf("expected queue to be drained, got %d entries", len(q.queue))
	}
}
