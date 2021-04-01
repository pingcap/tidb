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

package tikv_test

import (
	"context"
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/kv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/oracle/oracles"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
)

type testStoreSuite struct {
	testStoreSuiteBase
}

type testStoreSerialSuite struct {
	testStoreSuiteBase
}

type testStoreSuiteBase struct {
	OneByOneSuite
	store tikv.StoreProbe
}

var _ = Suite(&testStoreSuite{})
var _ = SerialSuites(&testStoreSerialSuite{})

func (s *testStoreSuiteBase) SetUpTest(c *C) {
	s.store = tikv.StoreProbe{KVStore: NewTestStore(c)}
}

func (s *testStoreSuiteBase) TearDownTest(c *C) {
	c.Assert(s.store.Close(), IsNil)
}

func (s *testStoreSuite) TestOracle(c *C) {
	o := &oracles.MockOracle{}
	s.store.SetOracle(o)

	ctx := context.Background()
	t1, err := s.store.GetTimestampWithRetry(tikv.NewBackofferWithVars(ctx, 100, nil), oracle.GlobalTxnScope)
	c.Assert(err, IsNil)
	t2, err := s.store.GetTimestampWithRetry(tikv.NewBackofferWithVars(ctx, 100, nil), oracle.GlobalTxnScope)
	c.Assert(err, IsNil)
	c.Assert(t1, Less, t2)

	t1, err = o.GetLowResolutionTimestamp(ctx, &oracle.Option{})
	c.Assert(err, IsNil)
	t2, err = o.GetLowResolutionTimestamp(ctx, &oracle.Option{})
	c.Assert(err, IsNil)
	c.Assert(t1, Less, t2)
	f := o.GetLowResolutionTimestampAsync(ctx, &oracle.Option{})
	c.Assert(f, NotNil)
	_ = o.UntilExpired(0, 0, &oracle.Option{})

	// Check retry.
	var wg sync.WaitGroup
	wg.Add(2)

	o.Disable()
	go func() {
		defer wg.Done()
		time.Sleep(time.Millisecond * 100)
		o.Enable()
	}()

	go func() {
		defer wg.Done()
		t3, err := s.store.GetTimestampWithRetry(tikv.NewBackofferWithVars(ctx, 5000, nil), oracle.GlobalTxnScope)
		c.Assert(err, IsNil)
		c.Assert(t2, Less, t3)
		expired := s.store.GetOracle().IsExpired(t2, 50, &oracle.Option{})
		c.Assert(expired, IsTrue)
	}()

	wg.Wait()
}

type checkRequestClient struct {
	tikv.Client
	priority pb.CommandPri
}

func (c *checkRequestClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	resp, err := c.Client.SendRequest(ctx, addr, req, timeout)
	if c.priority != req.Priority {
		if resp.Resp != nil {
			if getResp, ok := resp.Resp.(*pb.GetResponse); ok {
				getResp.Error = &pb.KeyError{
					Abort: "request check error",
				}
			}
		}
	}
	return resp, err
}

func (s *testStoreSuite) TestRequestPriority(c *C) {
	client := &checkRequestClient{
		Client: s.store.GetTiKVClient(),
	}
	s.store.SetTiKVClient(client)

	// Cover 2PC commit.
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	client.priority = pb.CommandPri_High
	txn.SetOption(kv.Priority, kv.PriorityHigh)
	err = txn.Set([]byte("key"), []byte("value"))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	// Cover the basic Get request.
	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	client.priority = pb.CommandPri_Low
	txn.SetOption(kv.Priority, kv.PriorityLow)
	_, err = txn.Get(context.TODO(), []byte("key"))
	c.Assert(err, IsNil)

	// A counter example.
	client.priority = pb.CommandPri_Low
	txn.SetOption(kv.Priority, kv.PriorityNormal)
	_, err = txn.Get(context.TODO(), []byte("key"))
	// err is translated to "try again later" by backoffer, so doesn't check error value here.
	c.Assert(err, NotNil)

	// Cover Seek request.
	client.priority = pb.CommandPri_High
	txn.SetOption(kv.Priority, kv.PriorityHigh)
	iter, err := txn.Iter([]byte("key"), nil)
	c.Assert(err, IsNil)
	for iter.Valid() {
		c.Assert(iter.Next(), IsNil)
	}
	iter.Close()
}

func (s *testStoreSerialSuite) TestOracleChangeByFailpoint(c *C) {
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/store/tikv/oracle/changeTSFromPD")
	}()
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/oracle/changeTSFromPD",
		"return(10000)"), IsNil)
	o := &oracles.MockOracle{}
	s.store.SetOracle(o)
	ctx := context.Background()
	t1, err := s.store.GetTimestampWithRetry(tikv.NewBackofferWithVars(ctx, 100, nil), oracle.GlobalTxnScope)
	c.Assert(err, IsNil)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/oracle/changeTSFromPD"), IsNil)
	t2, err := s.store.GetTimestampWithRetry(tikv.NewBackofferWithVars(ctx, 100, nil), oracle.GlobalTxnScope)
	c.Assert(err, IsNil)
	c.Assert(t1, Greater, t2)
}
