package frame

import (
	"encoding/binary"
	"testing"

	"github.com/cooldogedev/spectral/internal/protocol"
)

func TestMTURequestDecodeConsumesEncodedSize(t *testing.T) {
	encoded := (&MTURequest{MTU: 1200}).Encode()

	var fr MTURequest
	n, err := fr.Decode(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if n != 1200 {
		t.Fatalf("expected decoded size 1200, got %d", n)
	}
	if fr.MTU != 1200 {
		t.Fatalf("expected decoded mtu 1200, got %d", fr.MTU)
	}
}

func TestMTURequestEncodeClampsToProtocolMax(t *testing.T) {
	encoded := (&MTURequest{MTU: protocol.MaxPacketSize + 100}).Encode()
	if len(encoded) != int(protocol.MaxPacketSize) {
		t.Fatalf("expected encoded length %d, got %d", protocol.MaxPacketSize, len(encoded))
	}
	if mtu := binary.LittleEndian.Uint64(encoded[:8]); mtu != protocol.MaxPacketSize {
		t.Fatalf("expected encoded mtu %d, got %d", protocol.MaxPacketSize, mtu)
	}

	var fr MTURequest
	n, err := fr.Decode(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if n != int(protocol.MaxPacketSize) {
		t.Fatalf("expected decoded size %d, got %d", protocol.MaxPacketSize, n)
	}
}

func TestMTURequestDecodeRejectsInvalidSizes(t *testing.T) {
	tooSmall := make([]byte, 8)
	binary.LittleEndian.PutUint64(tooSmall, 7)
	if _, err := (&MTURequest{}).Decode(tooSmall); err == nil {
		t.Fatal("expected error for mtu below minimum")
	}

	insufficient := make([]byte, 8)
	binary.LittleEndian.PutUint64(insufficient, 1200)
	if _, err := (&MTURequest{}).Decode(insufficient); err == nil {
		t.Fatal("expected error for insufficient mtu payload bytes")
	}
}
