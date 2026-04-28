package spectral

import (
	"context"
	"testing"

	"github.com/cooldogedev/spectral/internal/log"
)

func TestStreamWriteDoesNotSkipSequenceOnQueueFull(t *testing.T) {
	sendQueue := newSendQueue()
	sendQueue.setMSS(25) // payload chunk size becomes 5 bytes.

	padding := make([]byte, maxSendQueueBytes-25)
	if !sendQueue.add(padding) {
		t.Fatal("failed to prefill send queue")
	}

	stream := newStream(1, context.Background(), sendQueue, func() {}, func() {}, log.NopLogger{})
	written, err := stream.Write(make([]byte, 10)) // 2 chunks, second should fail.
	if err == nil {
		t.Fatal("expected send queue full error")
	}
	if written != 5 {
		t.Fatalf("expected partial write of 5 bytes, got %d", written)
	}

	if got := stream.sequenceID.Load(); got != 1 {
		t.Fatalf("expected sequence ID to advance by successful chunks only, got %d", got)
	}
}
