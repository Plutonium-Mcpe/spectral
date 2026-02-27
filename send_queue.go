package spectral

import (
	"sync"

	"github.com/cooldogedev/spectral/internal/protocol"
)

const maxSendQueueBytes = 32 * 1024 * 1024

type sendQueue struct {
	queue          [][]byte
	pk             []byte
	queuedBytes    uint64
	maxSegmentSize uint64
	mu             sync.RWMutex
}

func newSendQueue() *sendQueue {
	return &sendQueue{
		pk:             make([]byte, 0, protocol.MaxPacketSize),
		maxSegmentSize: protocol.MinPacketSize,
	}
}

func (s *sendQueue) available() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.queue) > 0 || len(s.pk) > 0
}

func (s *sendQueue) mss() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.maxSegmentSize
}

func (s *sendQueue) setMSS(mss uint64) {
	s.mu.Lock()
	s.maxSegmentSize = mss
	s.mu.Unlock()
}

func (s *sendQueue) add(p []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.queuedBytes+uint64(len(p)) > maxSendQueueBytes {
		return false
	}

	s.queue = append(s.queue, p)
	s.queuedBytes += uint64(len(p))
	return true
}

func (s *sendQueue) pack(window uint64) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.queue) == 0 && len(s.pk) == 0 {
		return nil
	}

	size := int(min(window, s.maxSegmentSize))
	for len(s.queue) > 0 {
		entry := s.queue[0]
		if len(s.pk)+len(entry) > size {
			break
		}
		s.queue[0] = nil
		s.queue = s.queue[1:]
		s.queuedBytes -= uint64(len(entry))
		s.pk = append(s.pk, entry...)
	}
	return s.pk
}

func (s *sendQueue) flush() {
	s.mu.Lock()
	s.pk = s.pk[:0]
	s.mu.Unlock()
}

func (s *sendQueue) clear() {
	s.mu.Lock()
	for i := range s.queue {
		s.queue[i] = nil
	}
	s.queue = s.queue[:0]
	s.queue = nil
	s.queuedBytes = 0
	s.pk = s.pk[:0]
	s.pk = nil
	s.mu.Unlock()
}
