package spectral

const maxReceiveQueueEntries = 8192

type receiveResult int

const (
	// receiveAccepted: fresh packet, deliver and acknowledge.
	receiveAccepted receiveResult = iota
	// receiveDuplicate: already delivered. Re-acknowledge (our previous ack
	// may have been lost) but do not deliver again.
	receiveDuplicate
	// receiveRejected: fresh packet outside the receive window (or queue
	// full). Must NOT be acknowledged: acking makes the sender drop it from
	// its retransmission queue and the data is permanently lost. Dropping it
	// unacked lets the sender retransmit once the window advances.
	receiveRejected
)

type receiveQueue struct {
	expected uint32
	queue    map[uint32]bool
}

func newReceiveQueue() *receiveQueue {
	return &receiveQueue{
		expected: 1,
		queue:    make(map[uint32]bool),
	}
}

func (r *receiveQueue) add(sequenceID uint32) receiveResult {
	if r.exists(sequenceID) {
		return receiveDuplicate
	}

	if sequenceID > r.expected+maxReceiveQueueEntries {
		return receiveRejected
	}

	// Always admit the next expected sequence ID, even when the queue is full,
	// so merge() can progress and free queued entries.
	if len(r.queue) >= maxReceiveQueueEntries && sequenceID != r.expected {
		return receiveRejected
	}

	r.queue[sequenceID] = true
	r.merge()
	return receiveAccepted
}

func (r *receiveQueue) exists(sequenceID uint32) bool {
	if r.expected > sequenceID {
		return true
	}
	_, ok := r.queue[sequenceID]
	return ok
}

func (r *receiveQueue) merge() {
	for {
		if _, ok := r.queue[r.expected]; !ok {
			break
		}
		delete(r.queue, r.expected)
		r.expected++
	}
}
