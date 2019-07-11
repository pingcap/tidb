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

func (s *testClientSuite) TestPanicInRecvLoop(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/panicInFailPendingRequests", `panic`), IsNil)
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/gotErrorInRecvLoop", `return("0")`), IsNil)

	server, port := startMockTikvService()
	c.Assert(port > 0, IsTrue)

	cfg := config.GetGlobalConfig().TiKVClient
	cfg.GrpcConnectionCount = 1
	rpcClient := newRPCClient(cfg, config.Security{})

	// Start batchRecvLoop, and it should panic in `failPendingRequests`.
	addr := fmt.Sprintf("%s:%d", "127.0.0.1", port)
	_, err := rpcClient.getConnArray(addr)
	c.Assert(err, IsNil)

	time.Sleep(time.Second)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/panicInFailPendingRequests"), IsNil)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/gotErrorInRecvLoop"), IsNil)
	time.Sleep(time.Second)

	req := &tikvrpc.Request{
		Type:  tikvrpc.CmdEmpty,
		Empty: &tikvpb.BatchCommandsEmptyRequest{},
	}
	_, err = rpcClient.SendRequest(context.Background(), addr, req, time.Second)
	c.Assert(err, IsNil)
	server.Stop()
}
