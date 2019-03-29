// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/debugpb"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/store/tikv/debugrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// DebugClient is the client for debug protobuf
type DebugClient struct {
	sync.RWMutex
	cIndex   map[string]*uint32
	conns    map[string][]*grpc.ClientConn
	isClosed bool
	security config.Security
}

func newDebugClient(security config.Security) *DebugClient {
	return &DebugClient{
		security: security,
	}
}

func (c *DebugClient) getConn(addr string) (*grpc.ClientConn, error) {
	c.RLock()
	if c.isClosed {
		c.RUnlock()
		return nil, errors.Errorf("rpcClient is closed")
	}
	_, ok := c.conns[addr]
	c.RUnlock()
	if !ok {
		err := c.createConns(addr)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	next := atomic.AddUint32(c.cIndex[addr], 1) % uint32(len(c.conns[addr]))
	return c.conns[addr][next], nil
}

func (c *DebugClient) createConns(addr string) error {
	opt := grpc.WithInsecure()
	if len(c.security.ClusterSSLCA) != 0 {
		tlsConfig, err := c.security.ToTLSConfig()
		if err != nil {
			return errors.Trace(err)
		}
		opt = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}

	cfg := config.GetGlobalConfig()
	keepAlive := cfg.TiKVClient.GrpcKeepAliveTime
	keepAliveTimeout := cfg.TiKVClient.GrpcKeepAliveTimeout

	c.Lock()
	_, ok := c.conns[addr]
	if !ok {
		c.conns[addr] = make([]*grpc.ClientConn, config.GetGlobalConfig().TiKVClient.GrpcConnectionCount)
		c.cIndex[addr] = new(uint32)
		for i := range c.conns[addr] {
			ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
			conn, err := grpc.DialContext(
				ctx,
				addr,
				opt,
				grpc.WithInitialWindowSize(grpcInitialWindowSize),
				grpc.WithInitialConnWindowSize(grpcInitialConnWindowSize),
				grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxCallMsgSize)),
				grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(MaxSendMsgSize)),
				grpc.WithBackoffMaxDelay(time.Second*3),
				grpc.WithKeepaliveParams(keepalive.ClientParameters{
					Time:                time.Duration(keepAlive) * time.Second,
					Timeout:             time.Duration(keepAliveTimeout) * time.Second,
					PermitWithoutStream: true,
				}),
			)
			cancel()
			if err != nil {
				// Cleanup if the initialization fails.
				// a.Close()
				return errors.Trace(err)
			}
			c.conns[addr][i] = conn
		}
	}
	c.Unlock()
	return nil
}

// SendRequest send debug request
func (c *DebugClient) SendRequest(ctx context.Context, addr string, req *debugrpc.Request, timeout time.Duration) (*debugrpc.Response, error) {
	conn, err := c.getConn(addr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	client := debugpb.NewDebugClient(conn)
	ctx1, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return debugrpc.CallRPC(ctx1, client, req)
}
