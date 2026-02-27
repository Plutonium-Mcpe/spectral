package frame

import (
	"encoding/binary"
	"errors"

	"github.com/cooldogedev/spectral/internal/protocol"
)

type MTURequest struct {
	MTU uint64
}

func (fr *MTURequest) ID() uint32 {
	return IDMTURequest
}

func (fr *MTURequest) Encode() []byte {
	mtu := fr.MTU
	if mtu < 8 {
		mtu = 8
	}
	if mtu > protocol.MaxPacketSize {
		mtu = protocol.MaxPacketSize
	}
	p := make([]byte, mtu)
	binary.LittleEndian.PutUint64(p[0:8], mtu)
	return p
}

func (fr *MTURequest) Decode(p []byte) (int, error) {
	if len(p) < 8 {
		return 0, errors.New("not enough data to decode")
	}

	mtu := binary.LittleEndian.Uint64(p[0:8])
	if mtu < 8 || mtu > protocol.MaxPacketSize {
		return 0, errors.New("invalid mtu size")
	}
	if len(p) < int(mtu) {
		return 0, errors.New("not enough data to decode mtu payload")
	}

	fr.MTU = mtu
	return int(mtu), nil
}

func (fr *MTURequest) Reset() {}
