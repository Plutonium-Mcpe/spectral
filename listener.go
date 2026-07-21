package spectral

import (
	"context"
	"errors"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/cooldogedev/spectral/internal/frame"
	"github.com/cooldogedev/spectral/internal/protocol"
)

type Listener struct {
	conn                *udpConn
	connections         map[protocol.ConnectionID]*ServerConnection
	connectionsMu       sync.RWMutex
	connectionID        protocol.ConnectionID
	incomingConnections chan *ServerConnection
	ctx                 context.Context
	cancelFunc          context.CancelFunc
	once                sync.Once
}

func newListener(conn *udpConn) *Listener {
	listener := &Listener{
		conn:                conn,
		connections:         make(map[protocol.ConnectionID]*ServerConnection),
		incomingConnections: make(chan *ServerConnection, 100),
	}
	listener.ctx, listener.cancelFunc = context.WithCancel(context.Background())
	go conn.Read(func(dgram *datagram) (err error) {
		defer dgram.reset()
		connectionID, sequenceID, frames, err := frame.Unpack(dgram.b)
		if err != nil {
			return nil
		}

		var (
			c             *ServerConnection
			newConnection bool
		)
		listener.connectionsMu.Lock()
		c, ok := listener.connections[connectionID]
		if ok && !udpAddrEqual(c.peerAddr, dgram.peerAddr) {
			listener.connectionsMu.Unlock()
			releaseFrames(frames)
			return nil
		}

		if !ok && slices.ContainsFunc(frames, func(fr frame.Frame) bool { return fr.ID() == frame.IDConnectionRequest }) {
			assignedID := listener.connectionID
			c = newServerConnection(conn, dgram.peerAddr, assignedID, listener.ctx)
			c.logger.Log("connection_accepted", "addr", dgram.peerAddr.String())
			listener.connections[assignedID] = c
			listener.connectionID++
			newConnection = true
			go func() {
				<-c.ctx.Done()
				listener.connectionsMu.Lock()
				delete(listener.connections, assignedID)
				listener.connectionsMu.Unlock()
			}()
		}
		listener.connectionsMu.Unlock()

		if c == nil {
			releaseFrames(frames)
			return
		}

		if newConnection {
			select {
			case listener.incomingConnections <- c:
			default:
				releaseFrames(frames)
				_ = c.CloseWithError(frame.ConnectionCloseInternal, "connection accept queue full")
				return nil
			}
		}

		select {
		case <-listener.ctx.Done():
			releaseFrames(frames)
			return context.Cause(listener.ctx)
		case <-c.ctx.Done():
			releaseFrames(frames)
		case c.packets <- &receivedPacket{sequenceID, frames, time.Now()}:
		default:
			releaseFrames(frames)
			c.logger.Log("packet_dropped", "reason", "receive queue full")
		}
		return
	})
	return listener
}

func Listen(address string) (*Listener, error) {
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, err
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}

	c, err := newUDPConn(conn, true)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return newListener(c), nil
}

func (l *Listener) Accept(ctx context.Context) (Connection, error) {
	select {
	case <-l.ctx.Done():
		return nil, errors.New("listener closed")
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	case conn := <-l.incomingConnections:
		return conn, nil
	}
}

func (l *Listener) Close() (err error) {
	l.once.Do(func() {
		l.connectionsMu.Lock()
		conns := make([]*ServerConnection, 0, len(l.connections))
		for i, conn := range l.connections {
			conns = append(conns, conn)
			delete(l.connections, i)
		}
		l.connectionsMu.Unlock()

		for _, conn := range conns {
			_ = conn.CloseWithError(frame.ConnectionCloseGraceful, "closed listener")
		}
		l.cancelFunc()
		_ = l.conn.closeListener()
	})
	return
}

func udpAddrEqual(a, b *net.UDPAddr) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Port == b.Port && a.Zone == b.Zone && a.IP.Equal(b.IP)
}

func releaseFrames(frames []frame.Frame) {
	for _, fr := range frames {
		frame.PutFrame(fr)
	}
}
