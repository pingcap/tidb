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
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

type testClientSuite struct {
	OneByOneSuite
}

var _ = Suite(&testClientSuite{})

func (s *testClientSuite) TestConnWithoutBatch(c *C) {
	cfg := config.GetGlobalConfig().TiKVClient
	cfg.MaxBatchSize = 0
	client := newRPCClient(cfg, config.Security{})

	addr := "127.0.0.1:6379"
	conn1, err := client.getConnArray(addr)
	c.Assert(err, IsNil)

	conn2, err := client.getConnArray(addr)
	c.Assert(err, IsNil)
	c.Assert(conn2.Get(), Not(Equals), conn1.Get())

	client.Close()
	conn3, err := client.getConnArray(addr)
	c.Assert(err, NotNil)
	c.Assert(conn3, IsNil)
}

func (s *testClientSuite) TestRemoveCanceledRequests(c *C) {
	req := new(tikvpb.BatchCommandsRequest_Request)
	entries := []*batchCommandsEntry{
		{canceled: 1, req: req},
		{canceled: 0, req: req},
		{canceled: 1, req: req},
		{canceled: 1, req: req},
		{canceled: 0, req: req},
	}
	entryPtr := &entries[0]
	requests := make([]*tikvpb.BatchCommandsRequest_Request, len(entries))
	for i := range entries {
		requests[i] = entries[i].req
	}
	entries, requests = removeCanceledRequests(entries, requests)
	c.Assert(len(entries), Equals, 2)
	for _, e := range entries {
		c.Assert(e.isCanceled(), IsFalse)
	}
	c.Assert(len(requests), Equals, 2)
	newEntryPtr := &entries[0]
	c.Assert(entryPtr, Equals, newEntryPtr)
}

func (s *testClientSuite) TestConnectToNone(c *C) {
	cfg := config.GetGlobalConfig().TiKVClient
	cfg.GrpcConnectionCount = 1
	fmt.Printf("cfg: %v\n", cfg)
	rpcClient := newRPCClient(cfg, config.Security{})

	addr := fmt.Sprintf("%s:%d", "127.0.0.1", 44444)
	conn, err := rpcClient.getConnArray(addr)
	c.Assert(err, NotNil)
	c.Assert(conn, IsNil)
}

func (s *testClientSuite) TestSendAfterClose(c *C) {
	server, port := startMockTikvService()
	c.Assert(port > 0, IsTrue)

	cfg := config.GetGlobalConfig().TiKVClient
	cfg.GrpcConnectionCount = 1
	rpcClient := newRPCClient(cfg, config.Security{})

	addr := fmt.Sprintf("%s:%d", "127.0.0.1", port)
	conn, err := rpcClient.getConnArray(addr)
	c.Assert(err, IsNil)

	conn.Close()

	req := &tikvrpc.Request{
		Type:  tikvrpc.CmdEmpty,
		Empty: &tikvpb.BatchCommandsEmptyRequest{},
	}
	batchReq := req.ToBatchCommandsRequest()
	c.Assert(batchReq, NotNil)

	ch := make(chan error, 1)
	go func() {
		_, err = doRPCForBatchRequest(context.Background(), addr, conn.batchConn, batchReq, 100*time.Second)
		ch <- err
	}()

	select {
	case err := <-ch:
		c.Assert(err.Error() == "send to closed transport", IsTrue)
	case <-time.NewTimer(3 * time.Second).C:
		panic("doRPCForBatchRequest should retrun immediately")
	}

	server.Stop()
}

func (s *testClientSuite) TestDoubleClose(c *C) {
	server, port := startMockTikvService()
	c.Assert(port > 0, IsTrue)

	cfg := config.GetGlobalConfig().TiKVClient
	cfg.GrpcConnectionCount = 1
	fmt.Printf("cfg: %v\n", cfg)
	rpcClient := newRPCClient(cfg, config.Security{})

	addr := fmt.Sprintf("%s:%d", "127.0.0.1", port)
	conn, err := rpcClient.getConnArray(addr)
	c.Assert(err, IsNil)
	c.Assert(conn, NotNil)

	conn.Close()
	conn.Close()
	rpcClient.Close()
	rpcClient.Close()
	server.Stop()
}
