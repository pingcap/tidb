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
	"go.uber.org/zap"
	"io"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/debugpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// MaxRecvMsgSize set max gRPC receive message size received from server. If any message size is larger than
// current value, an error will be reported from gRPC.
var MaxRecvMsgSize = math.MaxInt64

// Timeout durations.
var (
	dialTimeout               = 5 * time.Second
	readTimeoutShort          = 20 * time.Second  // For requests that read/write several key-values.
	ReadTimeoutMedium         = 60 * time.Second  // For requests that may need scan region.
	ReadTimeoutLong           = 150 * time.Second // For requests that may need scan region multiple times.
	GCTimeout                 = 5 * time.Minute
	UnsafeDestroyRangeTimeout = 5 * time.Minute
	AccessLockObserverTimeout = 10 * time.Second
)

const (
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
	done chan struct{}
}

func newConnArray(maxSize uint, addr string, security config.Security, dieNotify *uint32, enableBatch bool) (*connArray, error) {
	a := &connArray{
		index:         0,
		v:             make([]*grpc.ClientConn, maxSize),
		streamTimeout: make(chan *tikvrpc.Lease, 1024),
		done:          make(chan struct{}),
	}
	if err := a.Init(addr, security, dieNotify, enableBatch); err != nil {
		return nil, err
	}
	return a, nil
}

func (a *connArray) Init(addr string, security config.Security, dieNotify *uint32, enableBatch bool) error {
	a.target = addr

	opt := grpc.WithInsecure()
	if len(security.ClusterSSLCA) != 0 {
		tlsConfig, err := security.ToTLSConfig()
		if err != nil {
			return errors.Trace(err)
		}
		opt = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}

	cfg := config.GetGlobalConfig()
	var (
		unaryInterceptor  grpc.UnaryClientInterceptor
		streamInterceptor grpc.StreamClientInterceptor
	)
	if cfg.OpenTracing.Enable {
		unaryInterceptor = grpc_opentracing.UnaryClientInterceptor()
		streamInterceptor = grpc_opentracing.StreamClientInterceptor()
	}

	allowBatch := (cfg.TiKVClient.MaxBatchSize > 0) && enableBatch
	if allowBatch {
		a.batchConn = newBatchConn(uint(len(a.v)), cfg.TiKVClient.MaxBatchSize, dieNotify)
		a.pendingRequests = metrics.TiKVPendingBatchRequests.WithLabelValues(a.target)
	}
	keepAlive := cfg.TiKVClient.GrpcKeepAliveTime
	keepAliveTimeout := cfg.TiKVClient.GrpcKeepAliveTimeout
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
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  100 * time.Millisecond, // Default was 1s.
					Multiplier: 1.6,                    // Default
					Jitter:     0.2,                    // Default
					MaxDelay:   3 * time.Second,        // Default was 120s.
				},
				MinConnectTimeout: dialTimeout,
			}),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                time.Duration(keepAlive) * time.Second,
				Timeout:             time.Duration(keepAliveTimeout) * time.Second,
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

		if allowBatch {
			batchClient := &batchCommandsClient{
				target:        a.target,
				conn:          conn,
				batched:       sync.Map{},
				idAlloc:       0,
				closed:        0,
				tikvClientCfg: cfg.TiKVClient,
				tikvLoad:      &a.tikvTransportLayerLoad,
				dieNotify:     a.dieNotify,
				dieFlag:       &a.die,
			}
			a.batchCommandsClients = append(a.batchCommandsClients, batchClient)
		}
	}
	go tikvrpc.CheckStreamTimeoutLoop(a.streamTimeout, a.done)
	if allowBatch {
		go a.batchSendLoop(cfg.TiKVClient)
	}

	return nil
}

func (a *connArray) Get() *grpc.ClientConn {
	next := atomic.AddUint32(&a.index, 1) % uint32(len(a.v))
	return a.v[next]
}

func (a *connArray) Close() {
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

	close(a.done)
}

// rpcClient is RPC client struct.
// TODO: Add flow control between RPC clients in TiDB ond RPC servers in TiKV.
// Since we use shared client connection to communicate to the same TiKV, it's possible
// that there are too many concurrent requests which overload the service of TiKV.
type rpcClient struct {
	sync.RWMutex

	conns    map[string]*connArray
	security config.Security

	dieNotify uint32
	// Periodically check whether there is any connection that was die and then close and remove these connections.
	// Implement background cleanup.
	isClosed         bool
	dieEventListener func(addr []string)
}

func newRPCClient(security config.Security) *rpcClient {
	return &rpcClient{
		conns:    make(map[string]*connArray),
		security: security,
	}
}

// NewTestRPCClient is for some external tests.
func NewTestRPCClient(security config.Security) Client {
	return newRPCClient(security)
}

func (c *rpcClient) getConnArray(addr string, enableBatch bool) (*connArray, error) {
	c.RLock()
	if c.isClosed {
		c.RUnlock()
		return nil, errors.Errorf("rpcClient is closed")
	}
	array, ok := c.conns[addr]
	c.RUnlock()
	if !ok {
		var err error
		array, err = c.createConnArray(addr, enableBatch)
		if err != nil {
			return nil, err
		}
	}
	return array, nil
}

func (c *rpcClient) createConnArray(addr string, enableBatch bool) (*connArray, error) {
	c.Lock()
	defer c.Unlock()
	array, ok := c.conns[addr]
	if !ok {
		var err error
		connCount := config.GetGlobalConfig().TiKVClient.GrpcConnectionCount
		array, err = newConnArray(connCount, addr, c.security, &c.dieNotify, enableBatch)
		if err != nil {
			return nil, err
		}
		c.conns[addr] = array
	}
	return array, nil
}

func (c *rpcClient) closeConns() {
	c.Lock()
	if !c.isClosed {
		c.isClosed = true
		// close all connections
		for _, array := range c.conns {
			array.Close()
		}
	}
	c.Unlock()
}

var sendReqHistCache sync.Map

type sendReqHistCacheKey struct {
	tp tikvrpc.CmdType
	id uint64
}

func (c *rpcClient) updateTiKVSendReqHistogram(req *tikvrpc.Request, start time.Time) {
	key := sendReqHistCacheKey{
		req.Type,
		req.Context.GetPeer().GetStoreId(),
	}

	v, ok := sendReqHistCache.Load(key)
	if !ok {
		reqType := req.Type.String()
		storeID := strconv.FormatUint(req.Context.GetPeer().GetStoreId(), 10)
		v = metrics.TiKVSendReqHistogram.WithLabelValues(reqType, storeID)
		sendReqHistCache.Store(key, v)
	}

	v.(prometheus.Observer).Observe(time.Since(start).Seconds())
}

// SendRequest sends a Request to server and receives Response.
func (c *rpcClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("rpcClient.SendRequest", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	start := time.Now()
	defer c.updateTiKVSendReqHistogram(req, start)

	if atomic.CompareAndSwapUint32(&c.dieNotify, 1, 0) {
		c.recycleDieConnArray()
	}

	enableBatch := req.StoreTp != kv.TiDB
	connArray, err := c.getConnArray(addr, enableBatch)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// TiDB RPC server supports batch RPC, but batch connection will send heart beat, It's not necessary since
	// request to TiDB is not high frequency.
	if config.GetGlobalConfig().TiKVClient.MaxBatchSize > 0 && enableBatch {
		if batchReq := req.ToBatchCommandsRequest(); batchReq != nil {
			return sendBatchRequest(ctx, addr, connArray.batchConn, batchReq, timeout)
		}
	}

	clientConn := connArray.Get()
	if state := clientConn.GetState(); state == connectivity.TransientFailure {
		storeID := strconv.FormatUint(req.Context.GetPeer().GetStoreId(), 10)
		metrics.GRPCConnTransientFailureCounter.WithLabelValues(addr, storeID).Inc()
	}

	if req.IsDebugReq() {
		client := debugpb.NewDebugClient(clientConn)
		ctx1, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return tikvrpc.CallDebugRPC(ctx1, client, req)
	}

	client := tikvpb.NewTikvClient(clientConn)

	if req.Type == tikvrpc.CmdBatchCop {
		logutil.BgLogger().Debug("send query to ", zap.String("store addr", addr))
		return c.getBatchCopStreamResponse(ctx, client, req, timeout, connArray)

	}

	if req.Type == tikvrpc.CmdCopStream {
		return c.getCopStreamResponse(ctx, client, req, timeout, connArray)
	}

	ctx1, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return tikvrpc.CallRPC(ctx1, client, req)
}

func (c *rpcClient) getCopStreamResponse(ctx context.Context, client tikvpb.TikvClient, req *tikvrpc.Request, timeout time.Duration, connArray *connArray) (*tikvrpc.Response, error) {
	// Coprocessor streaming request.
	// Use context to support timeout for grpc streaming client.
	ctx1, cancel := context.WithCancel(ctx)
	// Should NOT call defer cancel() here because it will cancel further stream.Recv()
	// We put it in copStream.Lease.Cancel call this cancel at copStream.Close
	// TODO: add unit test for SendRequest.
	resp, err := tikvrpc.CallRPC(ctx1, client, req)
	if err != nil {
		cancel()
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

func (c *rpcClient) getBatchCopStreamResponse(ctx context.Context, client tikvpb.TikvClient, req *tikvrpc.Request, timeout time.Duration, connArray *connArray) (*tikvrpc.Response, error) {
	// Coprocessor streaming request.
	// Use context to support timeout for grpc streaming client.
	ctx1, cancel := context.WithCancel(ctx)
	// Should NOT call defer cancel() here because it will cancel further stream.Recv()
	// We put it in copStream.Lease.Cancel call this cancel at copStream.Close
	// TODO: add unit test for SendRequest.
	resp, err := tikvrpc.CallRPC(ctx1, client, req)
	if err != nil {
		cancel()
		return nil, errors.Trace(err)
	}

	// Put the lease object to the timeout channel, so it would be checked periodically.
	copStream := resp.Resp.(*tikvrpc.BatchCopStreamResponse)
	copStream.Timeout = timeout
	copStream.Lease.Cancel = cancel
	connArray.streamTimeout <- &copStream.Lease

	// Read the first streaming response to get CopStreamResponse.
	// This can make error handling much easier, because SendReq() retry on
	// region error automatically.
	var first *coprocessor.BatchResponse
	first, err = copStream.Recv()
	if err != nil {
		if errors.Cause(err) != io.EOF {
			return nil, errors.Trace(err)
		}
		logutil.BgLogger().Debug("copstream returns nothing for the request.")
	}
	copStream.BatchResponse = first
	return resp, nil

}

func (c *rpcClient) Close() error {
	// TODO: add a unit test for SendRequest After Closed
	c.closeConns()
	return nil
}
