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
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/terror"
	goctx "golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// MaxConnectionCount is the max gRPC connections that will be established with
// each tikv-server.
var MaxConnectionCount = 16

// Timeout durations.
const (
	dialTimeout       = 5 * time.Second
	readTimeoutShort  = 20 * time.Second  // For requests that read/write several key-values.
	ReadTimeoutMedium = 60 * time.Second  // For requests that may need scan region.
	ReadTimeoutLong   = 150 * time.Second // For requests that may need scan region multiple times.

	grpcInitialWindowSize     = 1 << 30
	grpcInitialConnWindowSize = 1 << 30
)

// Client is a client that sends RPC.
// It should not be used after calling Close().
type Client interface {
	// Close should release all data.
	Close() error
	// SendReq sends Request.
	SendReq(ctx goctx.Context, addr string, req *tikvrpc.Request) (*tikvrpc.Response, error)
}

type connArray struct {
	index uint32
	v     []*grpc.ClientConn
}

func newConnArray(maxSize int, addr string, security config.Security) (*connArray, error) {
	a := &connArray{
		index: 0,
		v:     make([]*grpc.ClientConn, maxSize),
	}
	if err := a.Init(addr, security); err != nil {
		return nil, err
	}
	return a, nil
}

func (a *connArray) Init(addr string, security config.Security) error {
	opt := grpc.WithInsecure()
	if len(security.ClusterSSLCA) != 0 {
		tlsConfig, err := security.ToTLSConfig()
		if err != nil {
			return errors.Trace(err)
		}
		opt = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}

	for i := range a.v {
		unaryInterceptor := grpc_middleware.ChainUnaryClient(
			grpc_prometheus.UnaryClientInterceptor,
			grpc_opentracing.UnaryClientInterceptor(),
		)
		streamInterceptor := grpc_middleware.ChainStreamClient(
			grpc_prometheus.StreamClientInterceptor,
			grpc_opentracing.StreamClientInterceptor(),
		)
		conn, err := grpc.Dial(
			addr,
			opt,
			grpc.WithTimeout(dialTimeout),
			grpc.WithInitialWindowSize(grpcInitialWindowSize),
			grpc.WithInitialConnWindowSize(grpcInitialConnWindowSize),
			grpc.WithUnaryInterceptor(unaryInterceptor),
			grpc.WithStreamInterceptor(streamInterceptor),
		)

		if err != nil {
			// Cleanup if the initialization fails.
			a.Close()
			return errors.Trace(err)
		}
		a.v[i] = conn
	}
	return nil
}

func (a *connArray) Get() *grpc.ClientConn {
	next := atomic.AddUint32(&a.index, 1) % uint32(len(a.v))
	return a.v[next]
}

func (a *connArray) Close() {
	for i, c := range a.v {
		if c != nil {
			err := c.Close()
			terror.Log(errors.Trace(err))
			a.v[i] = nil
		}
	}
}

// rpcClient is RPC client struct.
// TODO: Add flow control between RPC clients in TiDB ond RPC servers in TiKV.
// Since we use shared client connection to communicate to the same TiKV, it's possible
// that there are too many concurrent requests which overload the service of TiKV.
// TODO: Implement background cleanup. It adds a background goroutine to periodically check
// whether there is any connection is idle and then close and remove these idle connections.
type rpcClient struct {
	sync.RWMutex
	isClosed bool
	conns    map[string]*connArray
	security config.Security
}

func newRPCClient(security config.Security) *rpcClient {
	return &rpcClient{
		conns:    make(map[string]*connArray),
		security: security,
	}
}

func (c *rpcClient) getConn(addr string) (*grpc.ClientConn, error) {
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
	return array.Get(), nil
}

func (c *rpcClient) createConnArray(addr string) (*connArray, error) {
	c.Lock()
	defer c.Unlock()
	array, ok := c.conns[addr]
	if !ok {
		var err error
		array, err = newConnArray(MaxConnectionCount, addr, c.security)
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

// SendReq sends a Request to server and receives Response.
func (c *rpcClient) SendReq(ctx goctx.Context, addr string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
	start := time.Now()
	reqType := req.Type.String()
	storeID := strconv.FormatUint(req.Context.GetPeer().GetStoreId(), 10)
	defer func() { sendReqHistogram.WithLabelValues(reqType, storeID).Observe(time.Since(start).Seconds()) }()

	conn, err := c.getConn(addr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	client := tikvpb.NewTikvClient(conn)
	resp, err := c.callRPC(ctx, client, req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

func (c *rpcClient) callRPC(ctx goctx.Context, client tikvpb.TikvClient, req *tikvrpc.Request) (*tikvrpc.Response, error) {
	resp := &tikvrpc.Response{}
	resp.Type = req.Type
	var err error
	switch req.Type {
	case tikvrpc.CmdGet:
		resp.Get, err = client.KvGet(ctx, req.Get)
	case tikvrpc.CmdScan:
		resp.Scan, err = client.KvScan(ctx, req.Scan)
	case tikvrpc.CmdPrewrite:
		resp.Prewrite, err = client.KvPrewrite(ctx, req.Prewrite)
	case tikvrpc.CmdCommit:
		resp.Commit, err = client.KvCommit(ctx, req.Commit)
	case tikvrpc.CmdCleanup:
		resp.Cleanup, err = client.KvCleanup(ctx, req.Cleanup)
	case tikvrpc.CmdBatchGet:
		resp.BatchGet, err = client.KvBatchGet(ctx, req.BatchGet)
	case tikvrpc.CmdBatchRollback:
		resp.BatchRollback, err = client.KvBatchRollback(ctx, req.BatchRollback)
	case tikvrpc.CmdScanLock:
		resp.ScanLock, err = client.KvScanLock(ctx, req.ScanLock)
	case tikvrpc.CmdResolveLock:
		resp.ResolveLock, err = client.KvResolveLock(ctx, req.ResolveLock)
	case tikvrpc.CmdGC:
		resp.GC, err = client.KvGC(ctx, req.GC)
	case tikvrpc.CmdDeleteRange:
		resp.DeleteRange, err = client.KvDeleteRange(ctx, req.DeleteRange)
	case tikvrpc.CmdRawGet:
		resp.RawGet, err = client.RawGet(ctx, req.RawGet)
	case tikvrpc.CmdRawPut:
		resp.RawPut, err = client.RawPut(ctx, req.RawPut)
	case tikvrpc.CmdRawDelete:
		resp.RawDelete, err = client.RawDelete(ctx, req.RawDelete)
	case tikvrpc.CmdRawScan:
		resp.RawScan, err = client.RawScan(ctx, req.RawScan)
	case tikvrpc.CmdCop:
		resp.Cop, err = client.Coprocessor(ctx, req.Cop)
	case tikvrpc.CmdCopStream:
		resp.CopStream, err = client.CoprocessorStream(ctx, req.Cop)
	case tikvrpc.CmdMvccGetByKey:
		resp.MvccGetByKey, err = client.MvccGetByKey(ctx, req.MvccGetByKey)
	case tikvrpc.CmdMvccGetByStartTs:
		resp.MvccGetByStartTS, err = client.MvccGetByStartTs(ctx, req.MvccGetByStartTs)
	case tikvrpc.CmdSplitRegion:
		resp.SplitRegion, err = client.SplitRegion(ctx, req.SplitRegion)
	default:
		return nil, errors.Errorf("invalid request type: %v", req.Type)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

func (c *rpcClient) Close() error {
	c.closeConns()
	return nil
}
