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
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	goctx "golang.org/x/net/context"
)

// Client is a client that sends RPC.
// It should not be used after calling Close().
type Client interface {
	KvGet(ctx goctx.Context, addr string, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error)
	KvScan(ctx goctx.Context, addr string, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error)
	KvPrewrite(ctx goctx.Context, addr string, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error)
	KvCommit(ctx goctx.Context, addr string, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error)
	KvCleanup(ctx goctx.Context, addr string, req *kvrpcpb.CleanupRequest) (*kvrpcpb.CleanupResponse, error)
	KvBatchGet(ctx goctx.Context, addr string, req *kvrpcpb.BatchGetRequest) (*kvrpcpb.BatchGetResponse, error)
	KvBatchRollback(ctx goctx.Context, addr string, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error)
	KvScanLock(ctx goctx.Context, addr string, req *kvrpcpb.ScanLockRequest) (*kvrpcpb.ScanLockResponse, error)
	KvResolveLock(ctx goctx.Context, addr string, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error)
	KvGC(ctx goctx.Context, addr string, req *kvrpcpb.GCRequest) (*kvrpcpb.GCResponse, error)

	RawGet(ctx goctx.Context, addr string, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error)
	RawPut(ctx goctx.Context, addr string, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error)
	RawDelete(ctx goctx.Context, addr string, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error)

	Coprocessor(ctx goctx.Context, addr string, req *coprocessor.Request) (*coprocessor.Response, error)

	// Close should release all data.
	Close() error
}

const (
	maxConnection     = 150
	dialTimeout       = 5 * time.Second
	writeTimeout      = 10 * time.Second
	readTimeoutShort  = 20 * time.Second  // For requests that read/write several key-values.
	readTimeoutMedium = 60 * time.Second  // For requests that may need scan region.
	readTimeoutLong   = 150 * time.Second // For requests that may need scan region multiple times.
)

type rpcClient struct {
	p *Pools
}

func newRPCClient() *rpcClient {
	return &rpcClient{
		p: NewPools(maxConnection, func(addr string) (*Conn, error) {
			return NewConnection(addr, dialTimeout)
		}),
	}
}

func (c *rpcClient) callRpcFunc(addr string, f func(conn *Conn), label string) error {
	startTime := time.Now()
	defer func() { sendReqHistogram.WithLabelValues(label).Observe(time.Since(startTime).Seconds()) }()

	conn, err := c.p.GetConn(addr)
	if err != nil {
		return err
	}
	defer c.p.PutConn(conn)
	f(conn)
	return nil
}

func (c *rpcClient) KvGet(ctx goctx.Context, addr string, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	var resp *kvrpcpb.GetResponse
	var err error
	f := func(conn *Conn) {
		client := tikvpb.NewTiKVClient(conn.ClientConn)
		resp, err = client.KvGet(ctx, req)
		if err != nil {
			conn.Close()
			err = errors.Trace(err)
		}
	}
	if e := c.callRpcFunc(addr, f, "kv"); e != nil {
		return nil, errors.Trace(err)
	}
	return resp, err
}

func (c *rpcClient) KvScan(ctx goctx.Context, addr string, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	var resp *kvrpcpb.ScanResponse
	var err error
	f := func(conn *Conn) {
		client := tikvpb.NewTiKVClient(conn.ClientConn)
		resp, err = client.KvScan(ctx, req)
		if err != nil {
			conn.Close()
			err = errors.Trace(err)
		}
	}
	if e := c.callRpcFunc(addr, f, "kv"); e != nil {
		return nil, errors.Trace(err)
	}
	return resp, err
}

func (c *rpcClient) KvPrewrite(ctx goctx.Context, addr string, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	var resp *kvrpcpb.PrewriteResponse
	var err error
	f := func(conn *Conn) {
		client := tikvpb.NewTiKVClient(conn.ClientConn)
		resp, err = client.KvPrewrite(ctx, req)
		if err != nil {
			conn.Close()
			err = errors.Trace(err)
		}
	}
	if e := c.callRpcFunc(addr, f, "kv"); e != nil {
		return nil, errors.Trace(err)
	}
	return resp, err
}

func (c *rpcClient) KvCommit(ctx goctx.Context, addr string, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	var resp *kvrpcpb.CommitResponse
	var err error
	f := func(conn *Conn) {
		client := tikvpb.NewTiKVClient(conn.ClientConn)
		resp, err = client.KvCommit(ctx, req)
		if err != nil {
			conn.Close()
			err = errors.Trace(err)
		}
	}
	if e := c.callRpcFunc(addr, f, "kv"); e != nil {
		return nil, errors.Trace(err)
	}
	return resp, err
}

func (c *rpcClient) KvCleanup(ctx goctx.Context, addr string, req *kvrpcpb.CleanupRequest) (*kvrpcpb.CleanupResponse, error) {
	var resp *kvrpcpb.CleanupResponse
	var err error
	f := func(conn *Conn) {
		client := tikvpb.NewTiKVClient(conn.ClientConn)
		resp, err = client.KvCleanup(ctx, req)
		if err != nil {
			conn.Close()
			err = errors.Trace(err)
		}
	}
	if e := c.callRpcFunc(addr, f, "kv"); e != nil {
		return nil, errors.Trace(err)
	}
	return resp, err
}

func (c *rpcClient) KvBatchGet(ctx goctx.Context, addr string, req *kvrpcpb.BatchGetRequest) (*kvrpcpb.BatchGetResponse, error) {
	var resp *kvrpcpb.BatchGetResponse
	var err error
	f := func(conn *Conn) {
		client := tikvpb.NewTiKVClient(conn.ClientConn)
		resp, err = client.KvBatchGet(ctx, req)
		if err != nil {
			conn.Close()
			err = errors.Trace(err)
		}
	}
	if e := c.callRpcFunc(addr, f, "kv"); e != nil {
		return nil, errors.Trace(err)
	}
	return resp, err
}

func (c *rpcClient) KvBatchRollback(ctx goctx.Context, addr string, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	var resp *kvrpcpb.BatchRollbackResponse
	var err error
	f := func(conn *Conn) {
		client := tikvpb.NewTiKVClient(conn.ClientConn)
		resp, err = client.KvBatchRollback(ctx, req)
		if err != nil {
			conn.Close()
			err = errors.Trace(err)
		}
	}
	if e := c.callRpcFunc(addr, f, "kv"); e != nil {
		return nil, errors.Trace(err)
	}
	return resp, err
}

func (c *rpcClient) KvScanLock(ctx goctx.Context, addr string, req *kvrpcpb.ScanLockRequest) (*kvrpcpb.ScanLockResponse, error) {
	var resp *kvrpcpb.ScanLockResponse
	var err error
	f := func(conn *Conn) {
		client := tikvpb.NewTiKVClient(conn.ClientConn)
		resp, err = client.KvScanLock(ctx, req)
		if err != nil {
			conn.Close()
			err = errors.Trace(err)
		}
	}
	if e := c.callRpcFunc(addr, f, "kv"); e != nil {
		return nil, errors.Trace(err)
	}
	return resp, err
}

func (c *rpcClient) KvResolveLock(ctx goctx.Context, addr string, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	var resp *kvrpcpb.ResolveLockResponse
	var err error
	f := func(conn *Conn) {
		client := tikvpb.NewTiKVClient(conn.ClientConn)
		resp, err = client.KvResolveLock(ctx, req)
		if err != nil {
			conn.Close()
			err = errors.Trace(err)
		}
	}
	if e := c.callRpcFunc(addr, f, "kv"); e != nil {
		return nil, errors.Trace(err)
	}
	return resp, err
}

func (c *rpcClient) KvGC(ctx goctx.Context, addr string, req *kvrpcpb.GCRequest) (*kvrpcpb.GCResponse, error) {
	var resp *kvrpcpb.GCResponse
	var err error
	f := func(conn *Conn) {
		client := tikvpb.NewTiKVClient(conn.ClientConn)
		resp, err = client.KvGC(ctx, req)
		if err != nil {
			conn.Close()
			err = errors.Trace(err)
		}
	}
	if e := c.callRpcFunc(addr, f, "kv"); e != nil {
		return nil, errors.Trace(err)
	}
	return resp, err
}

func (c *rpcClient) RawGet(ctx goctx.Context, addr string, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	var resp *kvrpcpb.RawGetResponse
	var err error
	f := func(conn *Conn) {
		client := tikvpb.NewTiKVClient(conn.ClientConn)
		resp, err = client.RawGet(ctx, req)
		if err != nil {
			conn.Close()
			err = errors.Trace(err)
		}
	}
	if e := c.callRpcFunc(addr, f, "kv"); e != nil {
		return nil, errors.Trace(err)
	}
	return resp, err
}

func (c *rpcClient) RawPut(ctx goctx.Context, addr string, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	var resp *kvrpcpb.RawPutResponse
	var err error
	f := func(conn *Conn) {
		client := tikvpb.NewTiKVClient(conn.ClientConn)
		resp, err = client.RawPut(ctx, req)
		if err != nil {
			conn.Close()
			err = errors.Trace(err)
		}
	}
	if e := c.callRpcFunc(addr, f, "kv"); e != nil {
		return nil, errors.Trace(err)
	}
	return resp, err
}

func (c *rpcClient) RawDelete(ctx goctx.Context, addr string, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	var resp *kvrpcpb.RawDeleteResponse
	var err error
	f := func(conn *Conn) {
		client := tikvpb.NewTiKVClient(conn.ClientConn)
		resp, err = client.RawDelete(ctx, req)
		if err != nil {
			conn.Close()
			err = errors.Trace(err)
		}
	}
	if e := c.callRpcFunc(addr, f, "kv"); e != nil {
		return nil, errors.Trace(err)
	}
	return resp, err
}

func (c *rpcClient) Coprocessor(ctx goctx.Context, addr string, req *coprocessor.Request) (*coprocessor.Response, error) {
	var resp *coprocessor.Response
	var err error
	f := func(conn *Conn) {
		client := tikvpb.NewTiKVClient(conn.ClientConn)
		resp, err = client.Coprocessor(ctx, req)
		if err != nil {
			conn.Close()
			err = errors.Trace(err)
		}
	}
	if e := c.callRpcFunc(addr, f, "cop"); e != nil {
		return nil, errors.Trace(err)
	}
	return resp, err
}

func (c *rpcClient) Close() error {
	c.p.Close()
	return nil
}
