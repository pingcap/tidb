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

package tikv

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/tidb/store/tikv/config"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"google.golang.org/grpc/metadata"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

type testClientSuite struct {
	OneByOneSuite
}

type testClientSerialSuite struct {
	OneByOneSuite
}

var _ = Suite(&testClientSuite{})
var _ = SerialSuites(&testClientFailSuite{})
var _ = SerialSuites(&testClientSerialSuite{})

func (s *testClientSerialSuite) TestConn(c *C) {
	defer config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.MaxBatchSize = 0
	})()

	client := NewRPCClient(config.Security{})

	addr := "127.0.0.1:6379"
	conn1, err := client.getConnArray(addr, true)
	c.Assert(err, IsNil)

	conn2, err := client.getConnArray(addr, true)
	c.Assert(err, IsNil)
	c.Assert(conn2.Get(), Not(Equals), conn1.Get())

	client.Close()
	conn3, err := client.getConnArray(addr, true)
	c.Assert(err, NotNil)
	c.Assert(conn3, IsNil)
}

func (s *testClientSuite) TestCancelTimeoutRetErr(c *C) {
	req := new(tikvpb.BatchCommandsRequest_Request)
	a := newBatchConn(1, 1, nil)

	ctx, cancel := context.WithCancel(context.TODO())
	cancel()
	_, err := sendBatchRequest(ctx, "", "", a, req, 2*time.Second)
	c.Assert(errors.Cause(err), Equals, context.Canceled)

	_, err = sendBatchRequest(context.Background(), "", "", a, req, 0)
	c.Assert(errors.Cause(err), Equals, context.DeadlineExceeded)
}

func (s *testClientSuite) TestSendWhenReconnect(c *C) {
	server, port := startMockTikvService()
	c.Assert(port > 0, IsTrue)

	rpcClient := NewRPCClient(config.Security{})
	addr := fmt.Sprintf("%s:%d", "127.0.0.1", port)
	conn, err := rpcClient.getConnArray(addr, true)
	c.Assert(err, IsNil)

	// Suppose all connections are re-establishing.
	for _, client := range conn.batchConn.batchCommandsClients {
		client.lockForRecreate()
	}

	req := tikvrpc.NewRequest(tikvrpc.CmdEmpty, &tikvpb.BatchCommandsEmptyRequest{})
	_, err = rpcClient.SendRequest(context.Background(), addr, req, 100*time.Second)
	c.Assert(err.Error() == "no available connections", IsTrue)
	conn.Close()
	server.Stop()
}

// chanClient sends received requests to the channel.
type chanClient struct {
	wg *sync.WaitGroup
	ch chan<- *tikvrpc.Request
}

func (c *chanClient) Close() error {
	return nil
}

func (c *chanClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	c.wg.Wait()
	c.ch <- req
	return nil, nil
}

func (s *testClientSuite) TestCollapseResolveLock(c *C) {
	buildResolveLockReq := func(regionID uint64, startTS uint64, commitTS uint64, keys [][]byte) *tikvrpc.Request {
		region := &metapb.Region{Id: regionID}
		req := tikvrpc.NewRequest(tikvrpc.CmdResolveLock, &kvrpcpb.ResolveLockRequest{
			StartVersion:  startTS,
			CommitVersion: commitTS,
			Keys:          keys,
		})
		tikvrpc.SetContext(req, region, nil)
		return req
	}
	buildBatchResolveLockReq := func(regionID uint64, txnInfos []*kvrpcpb.TxnInfo) *tikvrpc.Request {
		region := &metapb.Region{Id: regionID}
		req := tikvrpc.NewRequest(tikvrpc.CmdResolveLock, &kvrpcpb.ResolveLockRequest{
			TxnInfos: txnInfos,
		})
		tikvrpc.SetContext(req, region, nil)
		return req
	}

	var wg sync.WaitGroup
	reqCh := make(chan *tikvrpc.Request)
	client := reqCollapse{&chanClient{wg: &wg, ch: reqCh}}
	ctx := context.Background()

	// Collapse ResolveLock.
	resolveLockReq := buildResolveLockReq(1, 10, 20, nil)
	wg.Add(1)
	go client.SendRequest(ctx, "", resolveLockReq, time.Second)
	go client.SendRequest(ctx, "", resolveLockReq, time.Second)
	time.Sleep(300 * time.Millisecond)
	wg.Done()
	req := <-reqCh
	c.Assert(*req, DeepEquals, *resolveLockReq)
	select {
	case <-reqCh:
		c.Fatal("fail to collapse ResolveLock")
	default:
	}

	// Don't collapse ResolveLockLite.
	resolveLockLiteReq := buildResolveLockReq(1, 10, 20, [][]byte{[]byte("foo")})
	wg.Add(1)
	go client.SendRequest(ctx, "", resolveLockLiteReq, time.Second)
	go client.SendRequest(ctx, "", resolveLockLiteReq, time.Second)
	time.Sleep(300 * time.Millisecond)
	wg.Done()
	for i := 0; i < 2; i++ {
		req := <-reqCh
		c.Assert(*req, DeepEquals, *resolveLockLiteReq)
	}

	// Don't collapse BatchResolveLock.
	batchResolveLockReq := buildBatchResolveLockReq(1, []*kvrpcpb.TxnInfo{
		{Txn: 10, Status: 20},
	})
	wg.Add(1)
	go client.SendRequest(ctx, "", batchResolveLockReq, time.Second)
	go client.SendRequest(ctx, "", batchResolveLockReq, time.Second)
	time.Sleep(300 * time.Millisecond)
	wg.Done()
	for i := 0; i < 2; i++ {
		req := <-reqCh
		c.Assert(*req, DeepEquals, *batchResolveLockReq)
	}

	// Mixed
	wg.Add(1)
	go client.SendRequest(ctx, "", resolveLockReq, time.Second)
	go client.SendRequest(ctx, "", resolveLockLiteReq, time.Second)
	go client.SendRequest(ctx, "", batchResolveLockReq, time.Second)
	time.Sleep(300 * time.Millisecond)
	wg.Done()
	for i := 0; i < 3; i++ {
		<-reqCh
	}
	select {
	case <-reqCh:
		c.Fatal("unexpected request")
	default:
	}
}

func (s *testClientSerialSuite) TestForwardMetadataByUnaryCall(c *C) {
	server, port := startMockTikvService()
	c.Assert(port > 0, IsTrue)
	defer server.Stop()
	addr := fmt.Sprintf("%s:%d", "127.0.0.1", port)

	// Disable batch.
	defer config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.MaxBatchSize = 0
		conf.TiKVClient.GrpcConnectionCount = 1
	})()
	rpcClient := NewRPCClient(config.Security{})
	defer rpcClient.closeConns()

	var checkCnt uint64
	// Check no corresponding metadata if ForwardedHost is empty.
	server.setMetaChecker(func(ctx context.Context) error {
		atomic.AddUint64(&checkCnt, 1)
		// gRPC may set some metadata by default, e.g. "context-type".
		md, ok := metadata.FromIncomingContext(ctx)
		if ok {
			vals := md.Get(forwardMetadataKey)
			c.Assert(len(vals), Equals, 0)
		}
		return nil
	})

	// Prewrite represents unary-unary call.
	prewriteReq := tikvrpc.NewRequest(tikvrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{})
	for i := 0; i < 3; i++ {
		_, err := rpcClient.SendRequest(context.Background(), addr, prewriteReq, 10*time.Second)
		c.Assert(err, IsNil)
	}
	c.Assert(atomic.LoadUint64(&checkCnt), Equals, uint64(3))

	// CopStream represents unary-stream call.
	copStreamReq := tikvrpc.NewRequest(tikvrpc.CmdCopStream, &coprocessor.Request{})
	_, err := rpcClient.SendRequest(context.Background(), addr, copStreamReq, 10*time.Second)
	c.Assert(err, IsNil)
	c.Assert(atomic.LoadUint64(&checkCnt), Equals, uint64(4))

	checkCnt = 0
	forwardedHost := "127.0.0.1:6666"
	// Check the metadata exists.
	server.setMetaChecker(func(ctx context.Context) error {
		atomic.AddUint64(&checkCnt, 1)
		// gRPC may set some metadata by default, e.g. "context-type".
		md, ok := metadata.FromIncomingContext(ctx)
		c.Assert(ok, IsTrue)
		vals := md.Get(forwardMetadataKey)
		c.Assert(vals, DeepEquals, []string{forwardedHost})
		return nil
	})

	prewriteReq.ForwardedHost = forwardedHost
	for i := 0; i < 3; i++ {
		_, err = rpcClient.SendRequest(context.Background(), addr, prewriteReq, 10*time.Second)
		c.Assert(err, IsNil)
	}
	// checkCnt should be 3 because we don't use BatchCommands for redirection for now.
	c.Assert(atomic.LoadUint64(&checkCnt), Equals, uint64(3))

	copStreamReq.ForwardedHost = forwardedHost
	_, err = rpcClient.SendRequest(context.Background(), addr, copStreamReq, 10*time.Second)
	c.Assert(err, IsNil)
	c.Assert(atomic.LoadUint64(&checkCnt), Equals, uint64(4))
}

func (s *testClientSerialSuite) TestForwardMetadataByBatchCommands(c *C) {
	server, port := startMockTikvService()
	c.Assert(port > 0, IsTrue)
	defer server.Stop()
	addr := fmt.Sprintf("%s:%d", "127.0.0.1", port)

	// Enable batch and limit the connection count to 1 so that
	// there is only one BatchCommands stream for each host or forwarded host.
	defer config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.MaxBatchSize = 128
		conf.TiKVClient.GrpcConnectionCount = 1
	})()
	rpcClient := NewRPCClient(config.Security{})
	defer rpcClient.closeConns()

	var checkCnt uint64
	setCheckHandler := func(forwardedHost string) {
		server.setMetaChecker(func(ctx context.Context) error {
			atomic.AddUint64(&checkCnt, 1)
			md, ok := metadata.FromIncomingContext(ctx)
			if forwardedHost == "" {
				if ok {
					vals := md.Get(forwardMetadataKey)
					c.Assert(len(vals), Equals, 0)
				}
			} else {
				c.Assert(ok, IsTrue)
				vals := md.Get(forwardMetadataKey)
				c.Assert(vals, DeepEquals, []string{forwardedHost})

			}
			return nil
		})
	}

	prewriteReq := tikvrpc.NewRequest(tikvrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{})
	forwardedHosts := []string{"", "127.0.0.1:6666", "127.0.0.1:7777", "127.0.0.1:8888"}
	for i, forwardedHost := range forwardedHosts {
		setCheckHandler(forwardedHost)
		prewriteReq.ForwardedHost = forwardedHost
		for i := 0; i < 3; i++ {
			_, err := rpcClient.SendRequest(context.Background(), addr, prewriteReq, 10*time.Second)
			c.Assert(err, IsNil)
		}
		// checkCnt should be i because there is a stream for each forwardedHost.
		c.Assert(atomic.LoadUint64(&checkCnt), Equals, 1+uint64(i))
	}

	checkCnt = 0
	// CopStream is a unary-stream call which doesn't support batch.
	copStreamReq := tikvrpc.NewRequest(tikvrpc.CmdCopStream, &coprocessor.Request{})
	// Check no corresponding metadata if forwardedHost is empty.
	setCheckHandler("")
	_, err := rpcClient.SendRequest(context.Background(), addr, copStreamReq, 10*time.Second)
	c.Assert(err, IsNil)
	c.Assert(atomic.LoadUint64(&checkCnt), Equals, uint64(1))

	copStreamReq.ForwardedHost = "127.0.0.1:6666"
	// Check the metadata exists.
	setCheckHandler(copStreamReq.ForwardedHost)
	_, err = rpcClient.SendRequest(context.Background(), addr, copStreamReq, 10*time.Second)
	c.Assert(err, IsNil)
	c.Assert(atomic.LoadUint64(&checkCnt), Equals, uint64(2))
}

func (s *testClientSuite) TestBatchCommandsBuilder(c *C) {
	builder := newBatchCommandsBuilder(128)

	// Test no forwarding requests.
	builder.reset()
	req := new(tikvpb.BatchCommandsRequest_Request)
	for i := 0; i < 10; i++ {
		builder.push(&batchCommandsEntry{req: req})
		c.Assert(builder.len(), Equals, i+1)
	}
	entryMap := make(map[uint64]*batchCommandsEntry)
	batchedReq, forwardingReqs := builder.build(func(id uint64, e *batchCommandsEntry) {
		entryMap[id] = e
	})
	c.Assert(len(batchedReq.GetRequests()), Equals, 10)
	c.Assert(len(batchedReq.GetRequestIds()), Equals, 10)
	c.Assert(len(entryMap), Equals, 10)
	for i, id := range batchedReq.GetRequestIds() {
		c.Assert(id, Equals, uint64(i))
		c.Assert(entryMap[id].req, Equals, batchedReq.GetRequests()[i])
	}
	c.Assert(len(forwardingReqs), Equals, 0)
	c.Assert(builder.idAlloc, Equals, uint64(10))

	// Test collecting forwarding requests.
	builder.reset()
	forwardedHosts := []string{"", "127.0.0.1:6666", "127.0.0.1:7777", "127.0.0.1:8888"}
	for i := range forwardedHosts {
		for j, host := range forwardedHosts {
			// Each forwarded host has incremental count of requests
			// and interleaves with each other.
			if i <= j {
				builder.push(&batchCommandsEntry{req: req, forwardedHost: host})
			}
		}
	}
	entryMap = make(map[uint64]*batchCommandsEntry)
	batchedReq, forwardingReqs = builder.build(func(id uint64, e *batchCommandsEntry) {
		entryMap[id] = e
	})
	c.Assert(len(batchedReq.GetRequests()), Equals, 1)
	c.Assert(len(batchedReq.GetRequestIds()), Equals, 1)
	c.Assert(len(forwardingReqs), Equals, 3)
	for i, host := range forwardedHosts[1:] {
		c.Assert(len(forwardingReqs[host].GetRequests()), Equals, i+2)
		c.Assert(len(forwardingReqs[host].GetRequestIds()), Equals, i+2)
	}
	c.Assert(builder.idAlloc, Equals, uint64(10+builder.len()))
	c.Assert(len(entryMap), Equals, builder.len())
	for host, forwardingReq := range forwardingReqs {
		for i, id := range forwardingReq.GetRequestIds() {
			c.Assert(entryMap[id].req, Equals, forwardingReq.GetRequests()[i])
			c.Assert(entryMap[id].forwardedHost, Equals, host)
		}
	}

	// Test not collecting canceled requests
	builder.reset()
	entries := []*batchCommandsEntry{
		{canceled: 1, req: req},
		{canceled: 0, req: req},
		{canceled: 1, req: req},
		{canceled: 1, req: req},
		{canceled: 0, req: req},
	}
	for _, entry := range entries {
		builder.push(entry)
	}
	entryMap = make(map[uint64]*batchCommandsEntry)
	batchedReq, forwardingReqs = builder.build(func(id uint64, e *batchCommandsEntry) {
		entryMap[id] = e
	})
	c.Assert(len(batchedReq.GetRequests()), Equals, 2)
	c.Assert(len(batchedReq.GetRequestIds()), Equals, 2)
	c.Assert(len(forwardingReqs), Equals, 0)
	c.Assert(len(entryMap), Equals, 2)
	for i, id := range batchedReq.GetRequestIds() {
		c.Assert(entryMap[id].req, Equals, batchedReq.GetRequests()[i])
		c.Assert(entryMap[id].isCanceled(), IsFalse)
	}

	// Test canceling all requests
	builder.reset()
	entries = entries[:0]
	for i := 0; i < 3; i++ {
		entry := &batchCommandsEntry{req: req, res: make(chan *tikvpb.BatchCommandsResponse_Response, 1)}
		entries = append(entries, entry)
		builder.push(entry)
	}
	err := errors.New("error")
	builder.cancel(err)
	for _, entry := range entries {
		_, ok := <-entry.res
		c.Assert(ok, IsFalse)
		c.Assert(entry.err, Equals, err)
	}

	// Test reset
	builder.reset()
	c.Assert(builder.len(), Equals, 0)
	c.Assert(len(builder.entries), Equals, 0)
	c.Assert(len(builder.requests), Equals, 0)
	c.Assert(len(builder.requestIDs), Equals, 0)
	c.Assert(len(builder.forwardingReqs), Equals, 0)
	c.Assert(builder.idAlloc, Not(Equals), 0)
}
