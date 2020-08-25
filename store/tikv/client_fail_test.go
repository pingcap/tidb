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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
)

type testClientFailSuite struct {
	OneByOneSuite
}

func (s *testClientFailSuite) SetUpSuite(c *C) {
	// This lock make testClientFailSuite runs exclusively.
	withTiKVGlobalLock.Lock()
}

func (s testClientFailSuite) TearDownSuite(c *C) {
	withTiKVGlobalLock.Unlock()
}

func (s *testClientFailSuite) TestPanicInRecvLoop(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/panicInFailPendingRequests", `panic`), IsNil)
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/gotErrorInRecvLoop", `return("0")`), IsNil)

	server, port := startMockTikvService()
	c.Assert(port > 0, IsTrue)
	defer server.Stop()

	addr := fmt.Sprintf("%s:%d", "127.0.0.1", port)
	rpcClient := newRPCClient(config.Security{}, func(c *rpcClient) {
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
	_, err = rpcClient.SendRequest(context.Background(), addr, req, time.Second)
	c.Assert(err, IsNil)
}
