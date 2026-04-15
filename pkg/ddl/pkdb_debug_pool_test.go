// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/debugpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

func TestPkdbDebugClientPoolCacheByAddress(t *testing.T) {
	addr1 := startTestDebugServer(t)
	addr2 := startTestDebugServer(t)

	var dialCount atomic.Int32
	pool := newTestPkdbDebugClientPool(&dialCount, 0)
	t.Cleanup(func() {
		closeTestDebugClientPool(t, pool)
	})

	cli1 := mustGetTestDebugClient(t, pool, addr1)
	cli1Again := mustGetTestDebugClient(t, pool, addr1)
	cli2 := mustGetTestDebugClient(t, pool, addr2)

	require.Equal(t, cli1, cli1Again)
	require.NotEqual(t, cli1, cli2)
	require.Len(t, pool.clients, 2)
	require.Len(t, pool.conns, 2)
	require.Equal(t, int32(2), dialCount.Load())

	mustCallRegionInfo(t, cli1)
	mustCallRegionInfo(t, cli2)
}

func TestPkdbDebugClientPoolSingleflight(t *testing.T) {
	addr := startTestDebugServer(t)

	var dialCount atomic.Int32
	pool := newTestPkdbDebugClientPool(&dialCount, 50*time.Millisecond)
	t.Cleanup(func() {
		closeTestDebugClientPool(t, pool)
	})

	const callers = 16
	start := make(chan struct{})
	clients := make([]debugpb.DebugClient, callers)
	errCh := make(chan error, callers)

	var wg sync.WaitGroup
	wg.Add(callers)
	for idx := range callers {
		go func() {
			defer wg.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cli, err := pool.Get(ctx, addr)
			if err != nil {
				errCh <- err
				return
			}
			clients[idx] = cli
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		require.NoError(t, err)
	}
	for i := 1; i < len(clients); i++ {
		require.Equal(t, clients[0], clients[i])
	}
	require.Len(t, pool.clients, 1)
	require.Len(t, pool.conns, 1)
	require.Equal(t, int32(1), dialCount.Load())
}

func TestPkdbDebugClientPoolDelete(t *testing.T) {
	addr := startTestDebugServer(t)

	var dialCount atomic.Int32
	pool := newTestPkdbDebugClientPool(&dialCount, 0)
	t.Cleanup(func() {
		closeTestDebugClientPool(t, pool)
	})

	cli1 := mustGetTestDebugClient(t, pool, addr)
	mustCallRegionInfo(t, cli1)

	conn1 := pool.conns[addr]
	require.NotNil(t, conn1)

	pool.Delete(addr)
	require.Nil(t, pool.clients[addr])
	require.Nil(t, pool.conns[addr])
	require.Eventually(t, func() bool {
		return conn1.GetState() == connectivity.Shutdown
	}, 3*time.Second, 10*time.Millisecond)

	cli2 := mustGetTestDebugClient(t, pool, addr)
	conn2 := pool.conns[addr]
	require.NotNil(t, conn2)
	require.NotEqual(t, cli1, cli2)
	require.NotEqual(t, conn1, conn2)
	require.Equal(t, int32(2), dialCount.Load())
}

func TestPkdbDebugClientPoolCloseClosesAllConnections(t *testing.T) {
	addr1 := startTestDebugServer(t)
	addr2 := startTestDebugServer(t)

	var dialCount atomic.Int32
	pool := newTestPkdbDebugClientPool(&dialCount, 0)
	t.Cleanup(func() {
		closeTestDebugClientPool(t, pool)
	})

	_ = mustGetTestDebugClient(t, pool, addr1)
	_ = mustGetTestDebugClient(t, pool, addr2)
	conn1 := pool.conns[addr1]
	conn2 := pool.conns[addr2]
	require.NotNil(t, conn1)
	require.NotNil(t, conn2)

	pool.Close()
	require.Empty(t, pool.clients)
	require.Empty(t, pool.conns)
	require.Eventually(t, func() bool {
		return conn1.GetState() == connectivity.Shutdown && conn2.GetState() == connectivity.Shutdown
	}, 3*time.Second, 10*time.Millisecond)

	// Close should be idempotent.
	pool.Close()
}

func newTestPkdbDebugClientPool(dialCount *atomic.Int32, dialDelay time.Duration) *pkdbDebugClientPool {
	dialer := net.Dialer{}
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithContextDialer(func(ctx context.Context, target string) (net.Conn, error) {
			dialCount.Add(1)
			if dialDelay > 0 {
				timer := time.NewTimer(dialDelay)
				defer timer.Stop()
				select {
				case <-timer.C:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return dialer.DialContext(ctx, "tcp", target)
		}),
	}
	return newPkdbDebugClientPool(opts)
}

func mustGetTestDebugClient(t *testing.T, pool *pkdbDebugClientPool, addr string) debugpb.DebugClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cli, err := pool.Get(ctx, addr)
	require.NoError(t, err)
	return cli
}

func mustCallRegionInfo(t *testing.T, client debugpb.DebugClient) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.RegionInfo(ctx, &debugpb.RegionInfoRequest{RegionId: 1})
	require.NoError(t, err)
}

func startTestDebugServer(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := grpc.NewServer()
	debugpb.RegisterDebugServer(server, &testDebugServer{})
	serveErrCh := make(chan error, 1)
	serveDone := make(chan struct{})

	go func() {
		defer close(serveDone)
		err := server.Serve(listener)
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			serveErrCh <- err
		}
	}()

	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		select {
		case <-serveDone:
		case <-time.After(3 * time.Second):
			t.Error("debug test server did not exit in time")
		}
		select {
		case err := <-serveErrCh:
			require.NoError(t, err)
		default:
		}
	})

	return listener.Addr().String()
}

type testDebugServer struct {
	debugpb.UnimplementedDebugServer
}

func (*testDebugServer) RegionInfo(context.Context, *debugpb.RegionInfoRequest) (*debugpb.RegionInfoResponse, error) {
	return &debugpb.RegionInfoResponse{}, nil
}

func closeTestDebugClientPool(t *testing.T, pool *pkdbDebugClientPool) {
	t.Helper()

	pool.mu.Lock()
	conns := make([]*grpc.ClientConn, 0, len(pool.conns))
	for _, conn := range pool.conns {
		conns = append(conns, conn)
	}
	pool.mu.Unlock()

	pool.Close()
	for _, conn := range conns {
		require.Eventually(t, func() bool {
			return conn.GetState() == connectivity.Shutdown
		}, 3*time.Second, 10*time.Millisecond)
	}
}
