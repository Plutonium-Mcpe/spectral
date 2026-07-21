package spectral

import (
	"slices"
	"sync"
	"time"
)

const (
	retransmissionAttempts = 20
	maxBackoff             = 1 * time.Second
)

type retransmissionEntry struct {
	sequenceID uint32
	payload    []byte
	sent       time.Time
	attempts   int
	backoff    time.Duration
}

type lostPacket struct {
	payload []byte
	sent    time.Time
	gaveUp  bool
}

type retransmissionQueue struct {
	queue []*retransmissionEntry
	mu    sync.RWMutex
}

func newRetransmissionQueue() *retransmissionQueue {
	return &retransmissionQueue{}
}

func (r *retransmissionQueue) add(now time.Time, sequenceID uint32, p []byte) {
	r.mu.Lock()
	r.queue = append(r.queue, &retransmissionEntry{sequenceID: sequenceID, payload: p, sent: now})
	r.sort()
	r.mu.Unlock()
}

func (r *retransmissionQueue) remove(sequenceID uint32) *retransmissionEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	index := slices.IndexFunc(r.queue, func(e *retransmissionEntry) bool { return e.sequenceID == sequenceID })
	if index >= 0 {
		entry := r.queue[index]
		r.queue = slices.Delete(r.queue, index, index+1)
		return entry
	}
	return nil
}

func (r *retransmissionQueue) next(rto time.Duration) (t time.Time) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.queue) > 0 {
		entry := r.queue[0]
		waitFor := entry.backoff
		if waitFor == 0 {
			waitFor = rto
		}
		return entry.sent.Add(waitFor)
	}
	return
}

// shift returns the next packet due for retransmission. gaveUp is true when a
// reliable packet has exhausted retransmissionAttempts: the connection can no
// longer guarantee ordered delivery (the peer's streams have a permanent gap)
// and the caller must tear the connection down loudly instead of silently
// dropping the packet — the previous behavior left streams stalled forever
// while the connection stayed "healthy".
func (r *retransmissionQueue) shift(now time.Time, rto time.Duration) (p []byte, t time.Time, gaveUp bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.queue) == 0 {
		return
	}

	entry := r.queue[0]
	waitFor := entry.backoff
	if waitFor == 0 {
		waitFor = rto
	}

	if now.Sub(entry.sent) >= waitFor {
		sent := entry.sent
		entry.sent = now
		entry.attempts++

		next := waitFor * 2
		if next > maxBackoff {
			next = maxBackoff
		}
		entry.backoff = next

		if entry.attempts >= retransmissionAttempts {
			r.queue[0] = nil
			r.queue = r.queue[1:]
			return entry.payload, sent, true
		}
		r.queue = append(r.queue[1:], entry)
		r.sort()
		return entry.payload, sent, false
	}
	return
}

func (r *retransmissionQueue) shiftLost(now time.Time, largestAcked, threshold uint32) (lost []lostPacket) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if largestAcked <= threshold || len(r.queue) == 0 {
		return
	}

	limit := largestAcked - threshold
	remaining := r.queue[:0]
	for _, entry := range r.queue {
		if entry.sequenceID > limit {
			remaining = append(remaining, entry)
			continue
		}

		sent := entry.sent
		entry.sent = now
		entry.attempts++
		if entry.attempts >= retransmissionAttempts {
			lost = append(lost, lostPacket{payload: entry.payload, sent: sent, gaveUp: true})
			continue
		}
		lost = append(lost, lostPacket{payload: entry.payload, sent: sent})
		remaining = append(remaining, entry)
	}
	r.queue = remaining
	r.sort()
	return
}

func (r *retransmissionQueue) clear() {
	r.mu.Lock()
	for i, entry := range r.queue {
		entry.payload = entry.payload[:0]
		entry.payload = nil
		r.queue[i] = nil
	}
	r.queue = r.queue[:0]
	r.queue = nil
	r.mu.Unlock()
}

func (r *retransmissionQueue) sort() {
	if len(r.queue) > 1 {
		slices.SortFunc(r.queue, func(a, b *retransmissionEntry) int {
			if a.sent.Before(b.sent) {
				return -1
			} else if b.sent.Before(a.sent) {
				return 1
			}
			return 0
		})
	}
}
