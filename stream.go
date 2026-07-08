package spectral

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/cooldogedev/spectral/internal"
	"github.com/cooldogedev/spectral/internal/frame"
	"github.com/cooldogedev/spectral/internal/log"
	"github.com/cooldogedev/spectral/internal/protocol"
)

var streamDataPool = sync.Pool{New: func() any { return &frame.StreamData{} }}

const streamBufferSize = 1024 * 1024

type Stream struct {
	ctx        context.Context
	cancelFunc context.CancelCauseFunc
	streamID   protocol.StreamID
	wake       func()
	closer     func()
	sendQueue  *sendQueue
	frame      *frameQueue
	buffer     *internal.RingBuffer[byte]
	available  chan struct{}
	sequenceID atomic.Uint32
	logger     log.Logger
	writeMu    sync.Mutex
	mu         sync.Mutex
	once       sync.Once
}

func newStream(streamID protocol.StreamID, parentCtx context.Context, sendQueue *sendQueue, wake func(), closer func(), logger log.Logger) *Stream {
	ctx, cancelFunc := context.WithCancelCause(parentCtx)
	return &Stream{
		ctx:        ctx,
		cancelFunc: cancelFunc,
		streamID:   streamID,
		wake:       wake,
		closer:     closer,
		sendQueue:  sendQueue,
		frame:      newFrameQueue(),
		buffer:     internal.NewRingBuffer[byte](streamBufferSize),
		available:  make(chan struct{}, 1),
		logger:     logger,
	}
}

func (s *Stream) Read(p []byte) (int, error) {
	for {
		if n := s.read(p); n > 0 {
			return n, nil
		}

		select {
		case <-s.ctx.Done():
			return 0, context.Cause(s.ctx)
		case <-s.available:
		}
	}
}

// Write may return a partial write with an error when the connection send queue is full.
func (s *Stream) Write(p []byte) (int, error) {
	select {
	case <-s.ctx.Done():
		return 0, context.Cause(s.ctx)
	default:
	}
	if len(p) == 0 {
		return 0, nil
	}

	mss := int(s.sendQueue.mss()) - 20
	if mss <= 0 {
		return 0, errors.New("invalid maximum segment size")
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	nextSequence := s.sequenceID.Load()
	written := 0
	fr := streamDataPool.Get().(*frame.StreamData)
	fr.StreamID = s.streamID
	for payload := range slices.Chunk(p, mss) {
		fr.SequenceID = nextSequence
		fr.Payload = payload
		if !s.sendQueue.add(frame.PackSingle(fr)) {
			fr.Payload = fr.Payload[:0]
			streamDataPool.Put(fr)
			if written == 0 {
				return 0, errors.New("send queue full")
			}
			s.wake()
			return written, errors.New("send queue full")
		}
		nextSequence++
		s.sequenceID.Store(nextSequence)
		written += len(payload)
	}
	fr.Payload = fr.Payload[:0]
	streamDataPool.Put(fr)
	if written > 0 {
		s.wake()
	}
	return written, nil
}

func (s *Stream) Context() context.Context {
	return s.ctx
}

func (s *Stream) Close() error {
	s.logger.Log("stream_close_application", "streamID", s.streamID)
	return s.internalClose("closed by application")
}

func (s *Stream) internalClose(message string) error {
	s.once.Do(func() {
		s.cancelFunc(errors.New(message))
		s.closer()
		s.logger.Log("stream_close", "streamID", s.streamID)
		s.cleanup()
	})
	return nil
}

func (s *Stream) cleanup() {
	<-s.ctx.Done()
	s.mu.Lock()
	s.buffer.Reset()
	s.frame.clear()
	s.mu.Unlock()
}

func (s *Stream) receive(sequenceID uint32, p []byte) {
	select {
	case <-s.ctx.Done():
		return
	default:
	}
	if len(p) > streamBufferSize {
		_ = s.internalClose("stream payload exceeds buffer")
		return
	}

	overflow := false
	s.mu.Lock()
	if s.frame.expected == sequenceID && s.buffer.Free() >= len(p) {
		s.frame.expected++
		_, _ = s.buffer.Write(p)
	} else {
		overflow = !s.frame.enqueue(sequenceID, p)
	}
	s.processFrames()
	s.mu.Unlock()
	if overflow {
		_ = s.internalClose("stream frame queue full")
	}
}

func (s *Stream) processFrames() {
	for {
		entry := s.frame.top()
		if entry == nil || len(entry.payload) > s.buffer.Free() {
			break
		}

		if _, err := s.buffer.Write(entry.payload); err != nil {
			break
		}
		s.frame.dequeue()
	}

	if s.buffer.Len() > 0 {
		select {
		case s.available <- struct{}{}:
		default:
		}
	}
}

func (s *Stream) read(p []byte) (n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.buffer.Len() > 0 {
		n = s.buffer.Read(p)
		// Draining the buffer may have freed room for frames parked in the
		// reorder queue. processFrames is otherwise only invoked from
		// receive(), i.e. when a NEW frame arrives — with an idle sender
		// (request/response protocols) parked in-order frames would never be
		// delivered and Read would block forever on s.available.
		s.processFrames()
	}
	return
}
