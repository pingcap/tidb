package ddl

import (
	"context"
	"sync"

	"github.com/pingcap/kvproto/pkg/debugpb"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
)

// pkdbDebugClientPool caches TiKV debug gRPC connections per store address.
// The lifecycle is bound to one saveRaftIndex task, so we can re-dial on errors
// by deleting an entry.
type pkdbDebugClientPool struct {
	dialOptions []grpc.DialOption

	mu      sync.Mutex
	conns   map[string]*grpc.ClientConn
	clients map[string]debugpb.DebugClient

	dialGroup singleflight.Group
}

func newPkdbDebugClientPool(dialOptions []grpc.DialOption) *pkdbDebugClientPool {
	return &pkdbDebugClientPool{
		dialOptions: dialOptions,
		conns:       make(map[string]*grpc.ClientConn),
		clients:     make(map[string]debugpb.DebugClient),
	}
}

func (p *pkdbDebugClientPool) Get(ctx context.Context, addr string) (debugpb.DebugClient, error) {
	p.mu.Lock()
	if cli := p.clients[addr]; cli != nil {
		p.mu.Unlock()
		return cli, nil
	}
	p.mu.Unlock()

	v, err, _ := p.dialGroup.Do(addr, func() (any, error) {
		p.mu.Lock()
		if cli := p.clients[addr]; cli != nil {
			p.mu.Unlock()
			return cli, nil
		}
		p.mu.Unlock()

		conn, err := grpc.DialContext(ctx, addr, p.dialOptions...)
		if err != nil {
			return nil, err
		}

		cli := debugpb.NewDebugClient(conn)

		p.mu.Lock()
		if existing := p.clients[addr]; existing != nil {
			p.mu.Unlock()
			_ = conn.Close()
			return existing, nil
		}
		p.conns[addr] = conn
		p.clients[addr] = cli
		p.mu.Unlock()
		return cli, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(debugpb.DebugClient), nil
}

func (p *pkdbDebugClientPool) Delete(addr string) {
	p.mu.Lock()
	conn := p.conns[addr]
	delete(p.conns, addr)
	delete(p.clients, addr)
	p.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

func (p *pkdbDebugClientPool) Close() {
	p.mu.Lock()
	conns := make([]*grpc.ClientConn, 0, len(p.conns))
	for _, c := range p.conns {
		conns = append(conns, c)
	}
	p.conns = make(map[string]*grpc.ClientConn)
	p.clients = make(map[string]debugpb.DebugClient)
	p.mu.Unlock()

	for _, c := range conns {
		_ = c.Close()
	}
}
