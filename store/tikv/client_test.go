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
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/tidb/config"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

type testClientSuite struct {
	OneByOneSuite
}

var _ = Suite(&testClientSuite{})

func setMaxBatchSize(size uint) {
	config.GetGlobalConfig().TiKVClient.MaxBatchSize = size
}

func (s *testClientSuite) TestConn(c *C) {
	maxBatchSize := config.GetGlobalConfig().TiKVClient.MaxBatchSize
	setMaxBatchSize(0)

	client := newRPCClient(config.Security{})

	addr := "127.0.0.1:6379"
	conn1, err := client.getConnArray(addr)
	c.Assert(err, IsNil)

	conn2, err := client.getConnArray(addr)
	c.Assert(err, IsNil)
	c.Assert(conn2.Get(), Not(Equals), conn1.Get())

	client.recycleIdleConnArray()

	client.Close()
	conn3, err := client.getConnArray(addr)
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
	length := removeCanceledRequests(&entries, &requests)
	c.Assert(length, Equals, 2)
	for _, e := range entries {
		c.Assert(e.isCanceled(), IsFalse)
	}
	c.Assert(len(requests), Equals, 2)
	newEntryPtr := &entries[0]
	c.Assert(entryPtr, Equals, newEntryPtr)
}

func (s *testClientSuite) TestCancelTimeoutRetErr(c *C) {
	req := new(tikvpb.BatchCommandsRequest_Request)
	a := &connArray{batchCommandsCh: make(chan *batchCommandsEntry, 1)}

	ctx, cancel := context.WithCancel(context.TODO())
	cancel()
	_, err := sendBatchRequest(ctx, "", a, req, 2*time.Second)
	c.Assert(errors.Cause(err), Equals, context.Canceled)

	_, err = sendBatchRequest(context.Background(), "", a, req, 0)
	c.Assert(errors.Cause(err), Equals, context.DeadlineExceeded)
}
