// Copyright 2016 PingCAP, Inc.
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

// Package tikv provides tcp connection to kvserver.
package tikv

import (
	"context"
	"io"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/debugpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// MaxRecvMsgSize set max gRPC receive message size received from server. If any message size is larger than
// current value, an error will be reported from gRPC.
var MaxRecvMsgSize = math.MaxInt64

// Timeout durations.
const (
	dialTimeout               = 5 * time.Second
	readTimeoutShort          = 20 * time.Second  // For requests that read/write several key-values.
	ReadTimeoutMedium         = 60 * time.Second  // For requests that may need scan region.
	ReadTimeoutLong           = 150 * time.Second // For requests that may need scan region multiple times.
	GCTimeout                 = 5 * time.Minute
	UnsafeDestroyRangeTimeout = 5 * time.Minute

	grpcInitialWindowSize     = 1 << 30
	grpcInitialConnWindowSize = 1 << 30
)

// Client is a client that sends RPC.
// It should not be used after calling Close().
type Client interface {
	// Close should release all data.
	Close() error
	// SendRequest sends Request.
	SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error)
}

type connArray struct {
	// The target host.
	target string

	index uint32
	v     []*grpc.ClientConn
	// streamTimeout binds with a background goroutine to process coprocessor streaming timeout.
	streamTimeout chan *tikvrpc.Lease
	// batchConn is not null when batch is enabled.
	*batchConn

	closed int32
}

func newConnArray(cfg config.TiKVClient, addr string, security config.Security, idleNotify chan<- string) (*connArray, error) {
	a := &connArray{
		index:         0,
		v:             make([]*grpc.ClientConn, cfg.GrpcConnectionCount),
		streamTimeout: make(chan *tikvrpc.Lease, 1024),
	}
	if err := a.Init(cfg, addr, security, idleNotify); err != nil {
		return nil, err
	}
	return a, nil
}

func (a *connArray) Init(cfg config.TiKVClient, addr string, security config.Security, idleNotify chan<- string) error {
	a.target = addr

	opt := grpc.WithInsecure()
	if len(security.ClusterSSLCA) != 0 {
		tlsConfig, err := security.ToTLSConfig()
		if err != nil {
			return errors.Trace(err)
		}
		opt = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}

	var (
		unaryInterceptor  grpc.UnaryClientInterceptor
		streamInterceptor grpc.StreamClientInterceptor
	)
	if config.GetGlobalConfig().OpenTracing.Enable {
		unaryInterceptor = grpc_opentracing.UnaryClientInterceptor()
		streamInterceptor = grpc_opentracing.StreamClientInterceptor()
	}

	for i := range a.v {
		ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
		conn, err := grpc.DialContext(
			ctx,
			addr,
			opt,
			grpc.WithInitialWindowSize(grpcInitialWindowSize),
			grpc.WithInitialConnWindowSize(grpcInitialConnWindowSize),
			grpc.WithUnaryInterceptor(unaryInterceptor),
			grpc.WithStreamInterceptor(streamInterceptor),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxRecvMsgSize)),
			grpc.WithBackoffMaxDelay(time.Second*3),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                time.Duration(cfg.GrpcKeepAliveTime) * time.Second,
				Timeout:             time.Duration(cfg.GrpcKeepAliveTimeout) * time.Second,
				PermitWithoutStream: true,
			}),
		)
		cancel()
		if err != nil {
			// Cleanup if the initialization fails.
			a.Close()
			return errors.Trace(err)
		}
		a.v[i] = conn
	}
	go tikvrpc.CheckStreamTimeoutLoop(a.streamTimeout)

	if cfg.MaxBatchSize > 0 {
		bconn, err := newBatchConn(a, cfg, idleNotify)
		if err != nil {
			a.Close()
			return err
		}
		a.batchConn = bconn
	}

	return nil
}

func (a *connArray) Get() *grpc.ClientConn {
	next := atomic.AddUint32(&a.index, 1) % uint32(len(a.v))
	return a.v[next]
}

func (a *connArray) Close() {
	if atomic.CompareAndSwapInt32(&a.closed, 0, 1) {
		if a.batchConn != nil {
			a.batchConn.Close()
		}
		for i, c := range a.v {
			if c != nil {
				err := c.Close()
				terror.Log(errors.Trace(err))
				a.v[i] = nil
			}
		}
		close(a.streamTimeout)
	}
}

// rpcClient is RPC client struct.
// TODO: Add flow control between RPC clients in TiDB ond RPC servers in TiKV.
// Since we use shared client connection to communicate to the same TiKV, it's possible
// that there are too many concurrent requests which overload the service of TiKV.
type rpcClient struct {
	sync.RWMutex
	cfg      config.TiKVClient
	isClosed bool
	conns    map[string]*connArray
	security config.Security

	// Implement background cleanup.
	// Check whether there is any connection that is idle and then close and remove these idle connections.
	idleNotify           chan string
	idleCollectorCloseCh chan struct{}
}

func newRPCClient(tikvclient config.TiKVClient, security config.Security) *rpcClient {
	client := &rpcClient{
		cfg:                  tikvclient,
		conns:                make(map[string]*connArray),
		security:             security,
		idleNotify:           make(chan string, 16),
		idleCollectorCloseCh: make(chan struct{}, 1),
	}
	go func() {
		select {
		case addr := <-client.idleNotify:
			client.Lock()
			conn, ok := client.conns[addr]
			delete(client.conns, addr)
			client.Unlock()
			if ok {
				conn.Close()
				logutil.BgLogger().Info("recycle idle connection",
					zap.String("target", addr))
			}
		case <-client.idleCollectorCloseCh:
			return
		}
	}()
	return client
}

// NewTestRPCClient is for some external tests.
func NewTestRPCClient() Client {
	clientConfig := config.GetGlobalConfig().TiKVClient
	return newRPCClient(clientConfig, config.Security{})
}

func (c *rpcClient) getConnArray(addr string) (*connArray, error) {
	c.RLock()
	if c.isClosed {
		c.RUnlock()
		return nil, errors.Errorf("rpcClient is closed")
	}
	array, ok := c.conns[addr]
	c.RUnlock()
	if !ok {
		var err error
		array, err = c.createConnArray(addr)
		if err != nil {
			return nil, err
		}
	}
	return array, nil
}

func (c *rpcClient) createConnArray(addr string) (*connArray, error) {
	c.Lock()
	defer c.Unlock()
	array, ok := c.conns[addr]
	if !ok {
		var err error
		array, err = newConnArray(c.cfg, addr, c.security, c.idleNotify)
		if err != nil {
			return nil, err
		}
		c.conns[addr] = array
	}
	return array, nil
}

func (c *rpcClient) closeConns() {
}

// SendRequest sends a Request to server and receives Response.
func (c *rpcClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	start := time.Now()
	reqType := req.Type.String()
	storeID := strconv.FormatUint(req.Context.GetPeer().GetStoreId(), 10)
	defer func() {
		metrics.TiKVSendReqHistogram.WithLabelValues(reqType, storeID).Observe(time.Since(start).Seconds())
	}()

	connArray, err := c.getConnArray(addr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if c.cfg.MaxBatchSize > 0 {
		if batchReq := req.ToBatchCommandsRequest(); batchReq != nil {
			return doRPCForBatchRequest(ctx, addr, connArray.batchConn, batchReq, timeout)
		}
	}

	if req.IsDebugReq() {
		client := debugpb.NewDebugClient(connArray.Get())
		ctx1, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return tikvrpc.CallDebugRPC(ctx1, client, req)
	}

	client := tikvpb.NewTikvClient(connArray.Get())

	if req.Type != tikvrpc.CmdCopStream {
		ctx1, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return tikvrpc.CallRPC(ctx1, client, req)
	}

	// Coprocessor streaming request.
	// Use context to support timeout for grpc streaming client.
	ctx1, cancel := context.WithCancel(ctx)
	defer cancel()
	resp, err := tikvrpc.CallRPC(ctx1, client, req)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Put the lease object to the timeout channel, so it would be checked periodically.
	copStream := resp.Resp.(*tikvrpc.CopStreamResponse)
	copStream.Timeout = timeout
	copStream.Lease.Cancel = cancel
	connArray.streamTimeout <- &copStream.Lease

	// Read the first streaming response to get CopStreamResponse.
	// This can make error handling much easier, because SendReq() retry on
	// region error automatically.
	var first *coprocessor.Response
	first, err = copStream.Recv()
	if err != nil {
		if errors.Cause(err) != io.EOF {
			return nil, errors.Trace(err)
		}
		logutil.BgLogger().Debug("copstream returns nothing for the request.")
	}
	copStream.Response = first
	return resp, nil
}

func (c *rpcClient) Close() error {
	c.Lock()
	defer c.Unlock()
	if !c.isClosed {
		c.isClosed = true
		// close all connections
		for _, array := range c.conns {
			array.Close()
		}
		close(c.idleCollectorCloseCh)
	}
	return nil
}
