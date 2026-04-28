package spectral

const maxReceiveQueueEntries = 8192

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

func (r *receiveQueue) add(sequenceID uint32) bool {
	if r.exists(sequenceID) {
		return false
	}

	if sequenceID > r.expected+maxReceiveQueueEntries {
		return false
	}

	// Always admit the next expected sequence ID, even when the queue is full,
	// so merge() can progress and free queued entries.
	if len(r.queue) >= maxReceiveQueueEntries && sequenceID != r.expected {
		return false
	}

	r.queue[sequenceID] = true
	r.merge()
	return true
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
