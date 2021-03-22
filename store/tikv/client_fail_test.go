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
	"fmt"
	"sync/atomic"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/tidb/store/tikv/config"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
)

type testClientFailSuite struct {
	OneByOneSuite
}

func (s *testClientFailSuite) SetUpSuite(_ *C) {
	// This lock make testClientFailSuite runs exclusively.
	withTiKVGlobalLock.Lock()
}

func (s testClientFailSuite) TearDownSuite(_ *C) {
	withTiKVGlobalLock.Unlock()
}

func (s *testClientFailSuite) TestPanicInRecvLoop(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/panicInFailPendingRequests", `panic`), IsNil)
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/gotErrorInRecvLoop", `return("0")`), IsNil)

	server, port := startMockTikvService()
	c.Assert(port > 0, IsTrue)
	defer server.Stop()

	addr := fmt.Sprintf("%s:%d", "127.0.0.1", port)
	rpcClient := NewRPCClient(config.Security{}, func(c *RPCClient) {
		c.dialTimeout = time.Second / 3
	})

	// Start batchRecvLoop, and it should panic in `failPendingRequests`.
	_, err := rpcClient.getConnArray(addr, true, func(cfg *config.TiKVClient) { cfg.GrpcConnectionCount = 1 })
	c.Assert(err, IsNil, Commentf("cannot establish local connection due to env problems(e.g. heavy load in test machine), please retry again"))

	req := tikvrpc.NewRequest(tikvrpc.CmdEmpty, &tikvpb.BatchCommandsEmptyRequest{})
	_, err = rpcClient.SendRequest(context.Background(), addr, req, time.Second/2)
	c.Assert(err, NotNil)

	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/gotErrorInRecvLoop"), IsNil)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/panicInFailPendingRequests"), IsNil)
	time.Sleep(time.Second * 2)

	req = tikvrpc.NewRequest(tikvrpc.CmdEmpty, &tikvpb.BatchCommandsEmptyRequest{})
	_, err = rpcClient.SendRequest(context.Background(), addr, req, time.Second*4)
	c.Assert(err, IsNil)
}

func (s *testClientFailSuite) TestRecvErrorInMultipleRecvLoops(c *C) {
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

	// Create 4 BatchCommands streams.
	prewriteReq := tikvrpc.NewRequest(tikvrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{})
	forwardedHosts := []string{"", "127.0.0.1:6666", "127.0.0.1:7777", "127.0.0.1:8888"}
	for _, forwardedHost := range forwardedHosts {
		prewriteReq.ForwardedHost = forwardedHost
		_, err := rpcClient.SendRequest(context.Background(), addr, prewriteReq, 10*time.Second)
		c.Assert(err, IsNil)
	}
	connArray, err := rpcClient.getConnArray(addr, true)
	c.Assert(connArray, NotNil)
	c.Assert(err, IsNil)
	batchConn := connArray.batchConn
	c.Assert(batchConn, NotNil)
	c.Assert(len(batchConn.batchCommandsClients), Equals, 1)
	batchClient := batchConn.batchCommandsClients[0]
	c.Assert(batchClient.client, NotNil)
	c.Assert(batchClient.client.forwardedHost, Equals, "")
	c.Assert(len(batchClient.forwardedClients), Equals, 3)
	for _, forwardedHosts := range forwardedHosts[1:] {
		c.Assert(batchClient.forwardedClients[forwardedHosts].forwardedHost, Equals, forwardedHosts)
	}

	// Save all streams
	clientSave := batchClient.client.Tikv_BatchCommandsClient
	forwardedClientsSave := make(map[string]tikvpb.Tikv_BatchCommandsClient)
	for host, client := range batchClient.forwardedClients {
		forwardedClientsSave[host] = client.Tikv_BatchCommandsClient
	}
	epoch := atomic.LoadUint64(&batchClient.epoch)

	fp := "github.com/pingcap/tidb/store/tikv/gotErrorInRecvLoop"
	// Send a request to each stream to trigger reconnection.
	for _, forwardedHost := range forwardedHosts {
		c.Assert(failpoint.Enable(fp, `1*return("0")`), IsNil)
		prewriteReq.ForwardedHost = forwardedHost
		_, err := rpcClient.SendRequest(context.Background(), addr, prewriteReq, 10*time.Second)
		c.Assert(err, IsNil)
		time.Sleep(100 * time.Millisecond)
		c.Assert(failpoint.Disable(fp), IsNil)
	}

	// Wait for finishing reconnection.
	for {
		batchClient.lockForRecreate()
		if atomic.LoadUint64(&batchClient.epoch) != epoch {
			batchClient.unlockForRecreate()
			break
		}
		batchClient.unlockForRecreate()
		time.Sleep(time.Millisecond * 100)
	}

	// send request after reconnection.
	for _, forwardedHost := range forwardedHosts {
		prewriteReq.ForwardedHost = forwardedHost
		_, err := rpcClient.SendRequest(context.Background(), addr, prewriteReq, 10*time.Second)
		c.Assert(err, IsNil)
	}
	// Should only reconnect once.
	c.Assert(atomic.LoadUint64(&batchClient.epoch), Equals, epoch+1)
	// All streams are refreshed.
	c.Assert(batchClient.client.Tikv_BatchCommandsClient, Not(Equals), clientSave)
	c.Assert(len(batchClient.forwardedClients), Equals, len(forwardedClientsSave))
	for host, clientSave := range forwardedClientsSave {
		c.Assert(batchClient.forwardedClients[host].Tikv_BatchCommandsClient, Not(Equals), clientSave)
	}
}
