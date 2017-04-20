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

	"google.golang.org/grpc"

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
	dialTimeout       = 5 * time.Second
	writeTimeout      = 10 * time.Second
	readTimeoutShort  = 20 * time.Second  // For requests that read/write several key-values.
	readTimeoutMedium = 60 * time.Second  // For requests that may need scan region.
	readTimeoutLong   = 150 * time.Second // For requests that may need scan region multiple times.

	rpcLabelKV  = "kv"
	rpcLabelCop = "cop"
)

// TODO Add flow control between RPC clients in TiDB ond RPC servers in TiKV.
// Since we use shared client connection to communicate to the same TiKV, it's possible
// that there are too many concurrent requests which overload the service of TiKV.
type rpcClient struct {
	p *ConnPool
}

func newRPCClient() *rpcClient {
	return &rpcClient{
		p: NewConnPool(func(addr string) (*grpc.ClientConn, error) {
			return grpc.Dial(addr, grpc.WithInsecure(), grpc.WithTimeout(dialTimeout))
		}),
	}
}

func (c *rpcClient) callFunc(addr string, f func(conn *grpc.ClientConn) error, label string) error {
	startTime := time.Now()
	defer func() { sendReqHistogram.WithLabelValues(label).Observe(time.Since(startTime).Seconds()) }()

	conn, err := c.p.Get(addr)
	if err != nil {
		return err
	}
	defer c.p.Put(addr, conn)
	return f(conn)
}

func (c *rpcClient) KvGet(ctx goctx.Context, addr string, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	var resp *kvrpcpb.GetResponse
	f := func(conn *grpc.ClientConn) error {
		client := tikvpb.NewTiKVClient(conn)
		var err error
		resp, err = client.KvGet(ctx, req)
		return err
	}
	if err := c.callFunc(addr, f, rpcLabelKV); err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

func (c *rpcClient) KvScan(ctx goctx.Context, addr string, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	var resp *kvrpcpb.ScanResponse
	f := func(conn *grpc.ClientConn) error {
		client := tikvpb.NewTiKVClient(conn)
		var err error
		resp, err = client.KvScan(ctx, req)
		return err
	}
	if err := c.callFunc(addr, f, rpcLabelKV); err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

func (c *rpcClient) KvPrewrite(ctx goctx.Context, addr string, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	var resp *kvrpcpb.PrewriteResponse
	f := func(conn *grpc.ClientConn) error {
		client := tikvpb.NewTiKVClient(conn)
		var err error
		resp, err = client.KvPrewrite(ctx, req)
		return err
	}
	if err := c.callFunc(addr, f, rpcLabelKV); err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

func (c *rpcClient) KvCommit(ctx goctx.Context, addr string, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	var resp *kvrpcpb.CommitResponse
	f := func(conn *grpc.ClientConn) error {
		client := tikvpb.NewTiKVClient(conn)
		var err error
		resp, err = client.KvCommit(ctx, req)
		return err
	}
	if err := c.callFunc(addr, f, rpcLabelKV); err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

func (c *rpcClient) KvCleanup(ctx goctx.Context, addr string, req *kvrpcpb.CleanupRequest) (*kvrpcpb.CleanupResponse, error) {
	var resp *kvrpcpb.CleanupResponse
	f := func(conn *grpc.ClientConn) error {
		client := tikvpb.NewTiKVClient(conn)
		var err error
		resp, err = client.KvCleanup(ctx, req)
		return err
	}
	if err := c.callFunc(addr, f, rpcLabelKV); err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

func (c *rpcClient) KvBatchGet(ctx goctx.Context, addr string, req *kvrpcpb.BatchGetRequest) (*kvrpcpb.BatchGetResponse, error) {
	var resp *kvrpcpb.BatchGetResponse
	f := func(conn *grpc.ClientConn) error {
		client := tikvpb.NewTiKVClient(conn)
		var err error
		resp, err = client.KvBatchGet(ctx, req)
		return err
	}
	if err := c.callFunc(addr, f, rpcLabelKV); err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

func (c *rpcClient) KvBatchRollback(ctx goctx.Context, addr string, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	var resp *kvrpcpb.BatchRollbackResponse
	f := func(conn *grpc.ClientConn) error {
		client := tikvpb.NewTiKVClient(conn)
		var err error
		resp, err = client.KvBatchRollback(ctx, req)
		return err
	}
	if err := c.callFunc(addr, f, rpcLabelKV); err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

func (c *rpcClient) KvScanLock(ctx goctx.Context, addr string, req *kvrpcpb.ScanLockRequest) (*kvrpcpb.ScanLockResponse, error) {
	var resp *kvrpcpb.ScanLockResponse
	f := func(conn *grpc.ClientConn) error {
		client := tikvpb.NewTiKVClient(conn)
		var err error
		resp, err = client.KvScanLock(ctx, req)
		return err
	}
	if err := c.callFunc(addr, f, rpcLabelKV); err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

func (c *rpcClient) KvResolveLock(ctx goctx.Context, addr string, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	var resp *kvrpcpb.ResolveLockResponse
	f := func(conn *grpc.ClientConn) error {
		client := tikvpb.NewTiKVClient(conn)
		var err error
		resp, err = client.KvResolveLock(ctx, req)
		return err
	}
	if err := c.callFunc(addr, f, rpcLabelKV); err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

func (c *rpcClient) KvGC(ctx goctx.Context, addr string, req *kvrpcpb.GCRequest) (*kvrpcpb.GCResponse, error) {
	var resp *kvrpcpb.GCResponse
	f := func(conn *grpc.ClientConn) error {
		client := tikvpb.NewTiKVClient(conn)
		var err error
		resp, err = client.KvGC(ctx, req)
		return err
	}
	if err := c.callFunc(addr, f, rpcLabelKV); err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

func (c *rpcClient) RawGet(ctx goctx.Context, addr string, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	var resp *kvrpcpb.RawGetResponse
	f := func(conn *grpc.ClientConn) error {
		client := tikvpb.NewTiKVClient(conn)
		var err error
		resp, err = client.RawGet(ctx, req)
		return err
	}
	if err := c.callFunc(addr, f, rpcLabelKV); err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

func (c *rpcClient) RawPut(ctx goctx.Context, addr string, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	var resp *kvrpcpb.RawPutResponse
	f := func(conn *grpc.ClientConn) error {
		client := tikvpb.NewTiKVClient(conn)
		var err error
		resp, err = client.RawPut(ctx, req)
		return err
	}
	if err := c.callFunc(addr, f, rpcLabelKV); err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

func (c *rpcClient) RawDelete(ctx goctx.Context, addr string, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	var resp *kvrpcpb.RawDeleteResponse
	f := func(conn *grpc.ClientConn) error {
		client := tikvpb.NewTiKVClient(conn)
		var err error
		resp, err = client.RawDelete(ctx, req)
		return err
	}
	if err := c.callFunc(addr, f, rpcLabelKV); err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

func (c *rpcClient) Coprocessor(ctx goctx.Context, addr string, req *coprocessor.Request) (*coprocessor.Response, error) {
	var resp *coprocessor.Response
	f := func(conn *grpc.ClientConn) error {
		client := tikvpb.NewTiKVClient(conn)
		var err error
		resp, err = client.Coprocessor(ctx, req)
		return err
	}
	if err := c.callFunc(addr, f, rpcLabelCop); err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

func (c *rpcClient) Close() error {
	c.p.Close()
	return nil
}
