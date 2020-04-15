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
	"sync/atomic"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/tidb/v4/config"
	"github.com/pingcap/tidb/v4/store/tikv/tikvrpc"
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

func setMaxBatchSize(size uint) {
	newConf := config.NewConfig()
	newConf.TiKVClient.MaxBatchSize = size
	config.StoreGlobalConfig(newConf)
}

func (s *testClientSerialSuite) TestConn(c *C) {
	maxBatchSize := config.GetGlobalConfig().TiKVClient.MaxBatchSize
	setMaxBatchSize(0)

	client := newRPCClient(config.Security{})

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
	setMaxBatchSize(maxBatchSize)
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

func (s *testClientSuite) TestCancelTimeoutRetErr(c *C) {
	req := new(tikvpb.BatchCommandsRequest_Request)
	a := newBatchConn(1, 1, nil)

	ctx, cancel := context.WithCancel(context.TODO())
	cancel()
	_, err := sendBatchRequest(ctx, "", a, req, 2*time.Second)
	c.Assert(errors.Cause(err), Equals, context.Canceled)

	_, err = sendBatchRequest(context.Background(), "", a, req, 0)
	c.Assert(errors.Cause(err), Equals, context.DeadlineExceeded)
}

func (s *testClientSuite) TestSendWhenReconnect(c *C) {
	server, port := startMockTikvService()
	c.Assert(port > 0, IsTrue)

	rpcClient := newRPCClient(config.Security{})
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

func (s *testClientSuite) TestIdleHeartbeat(c *C) {
	server, port := startMockTikvService()
	c.Assert(port > 0, IsTrue)
	defer server.Stop()

	rpcClient := newRPCClient(config.Security{})
	addr := fmt.Sprintf("%s:%d", "127.0.0.1", port)
	conn, err := rpcClient.getConnArray(addr, true)
	c.Assert(err, IsNil)

	sendIdleReq := "github.com/pingcap/tidb/v4/store/tikv/sendIdleHeartbeatReq"
	noStripResp := "github.com/pingcap/tidb/v4/store/tikv/forceReturnIdleHeartbeatResp"
	noAvConn := "github.com/pingcap/tidb/v4/store/tikv/noAvConn"
	failBeforeSend := "github.com/pingcap/tidb/v4/store/tikv/failBeforeSend"

	c.Assert(failpoint.Enable(sendIdleReq, `return()`), IsNil)
	c.Assert(failpoint.Enable(noStripResp, `return()`), IsNil)
	c.Assert(failpoint.Enable(noAvConn, `return()`), IsNil)
	c.Assert(failpoint.Enable(failBeforeSend, `return()`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable(sendIdleReq), IsNil)
		c.Assert(failpoint.Disable(noStripResp), IsNil)
		c.Assert(failpoint.Disable(noAvConn), IsNil)
		c.Assert(failpoint.Disable(failBeforeSend), IsNil)
	}()

	// 1. test trigger idle heartbeat and return success by a live store.
	ctx := failpoint.WithHook(context.TODO(), func(ctx context.Context, fpname string) bool {
		if fpname == sendIdleReq || fpname == noStripResp {
			return true
		}
		return false
	})
	req := tikvrpc.NewRequest(tikvrpc.CmdEmpty, &tikvpb.BatchCommandsEmptyRequest{}).ToBatchCommandsRequest()
	_, err = sendBatchRequest(ctx, addr, conn.batchConn, req, 100*time.Second)
	c.Assert(err, IsNil)

	// 2. test trigger idle heartbeat and cannot found any conn.
	ctx = failpoint.WithHook(context.TODO(), func(ctx context.Context, fpname string) bool {
		if fpname == sendIdleReq || fpname == noStripResp || fpname == noAvConn {
			return true
		}
		return false
	})
	var dieNode []string
	rpcClient.dieEventListener = func(addr []string) {
		dieNode = append(dieNode, addr...)
	}
	_, err = sendBatchRequest(ctx, addr, conn.batchConn, req, 100*time.Second)
	c.Assert(err, NotNil) // no available connections
	c.Assert(conn.batchConn.isDie(), IsTrue)
	c.Assert(atomic.LoadUint32(conn.batchConn.dieNotify), Equals, uint32(1))
	rpcClient.recycleDieConnArray()
	c.Assert(len(dieNode), Equals, 1)
	c.Assert(dieNode[0], Equals, addr)

	// 3. test trigger idle heartbeat and send fail before send.
	conn, err = rpcClient.getConnArray(addr, true)
	c.Assert(err, IsNil)
	ctx = failpoint.WithHook(context.TODO(), func(ctx context.Context, fpname string) bool {
		if fpname == sendIdleReq || fpname == noStripResp || fpname == failBeforeSend {
			return true
		}
		return false
	})
	dieNode = dieNode[:0]
	rpcClient.dieEventListener = func(addr []string) {
		dieNode = append(dieNode, addr...)
	}
	_, err = sendBatchRequest(ctx, addr, conn.batchConn, req, 100*time.Second)
	c.Assert(err, NotNil) // no available connections
	c.Assert(conn.batchConn.isDie(), IsTrue)
	c.Assert(atomic.LoadUint32(conn.batchConn.dieNotify), Equals, uint32(1))
	rpcClient.recycleDieConnArray()
	c.Assert(len(dieNode), Greater, 0)
	c.Assert(dieNode[0], Equals, addr)
	rpcClient.recycleDieConnArray()
	c.Assert(len(dieNode), Equals, 1)
	c.Assert(dieNode[0], Equals, addr)
}
