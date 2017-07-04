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
	"sync"
	"sync/atomic"
	"time"

	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	goctx "golang.org/x/net/context"
	"google.golang.org/grpc"
)

const (
	maxConnectionNumber = 16
	dialTimeout         = 5 * time.Second
	readTimeoutShort    = 20 * time.Second  // For requests that read/write several key-values.
	readTimeoutMedium   = 60 * time.Second  // For requests that may need scan region.
	readTimeoutLong     = 150 * time.Second // For requests that may need scan region multiple times.

	grpcInitialWindowSize     = 1 << 30
	grpcInitialConnWindowSize = 1 << 30

	rpcLabelKV  = "kv"
	rpcLabelCop = "cop"
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

func newConnArray(maxSize uint32, addr string) (*connArray, error) {
	a := &connArray{
		index: 0,
		v:     make([]*grpc.ClientConn, maxSize),
	}
	if err := a.Init(addr); err != nil {
		return nil, err
	}
	return a, nil
}

func (a *connArray) Init(addr string) error {
	for i := range a.v {
		conn, err := grpc.Dial(
			addr,
			grpc.WithInsecure(),
			grpc.WithTimeout(dialTimeout),
			grpc.WithInitialWindowSize(grpcInitialWindowSize),
			grpc.WithInitialConnWindowSize(grpcInitialConnWindowSize),
			grpc.WithUnaryInterceptor(grpc_prometheus.UnaryClientInterceptor),
			grpc.WithStreamInterceptor(grpc_prometheus.StreamClientInterceptor))
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
			c.Close()
			a.v[i] = nil
		}
	}
}

// TODO: Add flow control between RPC clients in TiDB ond RPC servers in TiKV.
// Since we use shared client connection to communicate to the same TiKV, it's possible
// that there are too many concurrent requests which overload the service of TiKV.
// TODO: Implement background cleanup. It adds a background goroutine to periodically check
// whether there is any connection is idle and then close and remove these idle connections.
type rpcClient struct {
	sync.RWMutex
	isClosed bool
	conns    map[string]*connArray
}

func newRPCClient() *rpcClient {
	return &rpcClient{
		conns: make(map[string]*connArray),
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
		array, err = newConnArray(maxConnectionNumber, addr)
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
	var label = rpcLabelKV
	if req.Type == tikvrpc.CmdCop {
		label = rpcLabelCop
	}
	defer func() { sendReqHistogram.WithLabelValues(label).Observe(time.Since(start).Seconds()) }()

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
	switch req.Type {
	case tikvrpc.CmdGet:
		r, err := client.KvGet(ctx, req.Get)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp.Get = r
		return resp, nil
	case tikvrpc.CmdScan:
		r, err := client.KvScan(ctx, req.Scan)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp.Scan = r
		return resp, nil
	case tikvrpc.CmdPrewrite:
		r, err := client.KvPrewrite(ctx, req.Prewrite)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp.Prewrite = r
		return resp, nil
	case tikvrpc.CmdCommit:
		r, err := client.KvCommit(ctx, req.Commit)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp.Commit = r
		return resp, nil
	case tikvrpc.CmdCleanup:
		r, err := client.KvCleanup(ctx, req.Cleanup)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp.Cleanup = r
		return resp, nil
	case tikvrpc.CmdBatchGet:
		r, err := client.KvBatchGet(ctx, req.BatchGet)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp.BatchGet = r
		return resp, nil
	case tikvrpc.CmdBatchRollback:
		r, err := client.KvBatchRollback(ctx, req.BatchRollback)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp.BatchRollback = r
		return resp, nil
	case tikvrpc.CmdScanLock:
		r, err := client.KvScanLock(ctx, req.ScanLock)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp.ScanLock = r
		return resp, nil
	case tikvrpc.CmdResolveLock:
		r, err := client.KvResolveLock(ctx, req.ResolveLock)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp.ResolveLock = r
		return resp, nil
	case tikvrpc.CmdGC:
		r, err := client.KvGC(ctx, req.GC)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp.GC = r
		return resp, nil
	case tikvrpc.CmdRawGet:
		r, err := client.RawGet(ctx, req.RawGet)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp.RawGet = r
		return resp, nil
	case tikvrpc.CmdRawPut:
		r, err := client.RawPut(ctx, req.RawPut)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp.RawPut = r
		return resp, nil
	case tikvrpc.CmdRawDelete:
		r, err := client.RawDelete(ctx, req.RawDelete)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp.RawDelete = r
		return resp, nil
	case tikvrpc.CmdCop:
		r, err := client.Coprocessor(ctx, req.Cop)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp.Cop = r
		return resp, nil
	default:
		return nil, errors.Errorf("invalid request type: %v", req.Type)
	}
}

func (c *rpcClient) Close() error {
	c.closeConns()
	return nil
}
