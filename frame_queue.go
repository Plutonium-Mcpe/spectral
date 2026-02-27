package spectral

import "sort"

const maxFrameQueueEntries = 4096

type frameEntry struct {
	sequenceID uint32
	payload    []byte
}

type frameQueue struct {
	queue    []*frameEntry
	expected uint32
}

func newFrameQueue() *frameQueue {
	return &frameQueue{}
}

func (f *frameQueue) top() *frameEntry {
	if len(f.queue) > 0 && f.queue[0].sequenceID == f.expected {
		return f.queue[0]
	}
	return nil
}

func (f *frameQueue) enqueue(sequenceID uint32, p []byte) bool {
	if sequenceID < f.expected {
		return true
	}

	for _, entry := range f.queue {
		if entry.sequenceID == sequenceID {
			return true
		}
	}

	if len(f.queue) >= maxFrameQueueEntries {
		return false
	}

	f.queue = append(f.queue, &frameEntry{sequenceID: sequenceID, payload: append([]byte(nil), p...)})
	sort.Slice(f.queue, func(i, j int) bool { return f.queue[i].sequenceID < f.queue[j].sequenceID })
	return true
}

func (f *frameQueue) dequeue() {
	entry := f.queue[0]
	entry.payload = entry.payload[:0]
	entry.payload = nil
	f.queue[0] = nil
	f.queue = f.queue[1:]
	f.expected++
}

func (f *frameQueue) clear() {
	for i, entry := range f.queue {
		entry.payload = entry.payload[:0]
		entry.payload = nil
		f.queue[i] = nil
	}
	f.queue = f.queue[:0]
	f.queue = nil
}
