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
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	pd "github.com/pingcap/pd/v3/client"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockoracle"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
)

var errStopped = errors.New("stopped")

type testStoreSuite struct {
	OneByOneSuite
	store *tikvStore
}

var _ = Suite(&testStoreSuite{})

func (s *testStoreSuite) SetUpTest(c *C) {
	s.store = NewTestStore(c).(*tikvStore)
}

func (s *testStoreSuite) TearDownTest(c *C) {
	c.Assert(s.store.Close(), IsNil)
}

func (s *testStoreSuite) TestParsePath(c *C) {
	etcdAddrs, disableGC, err := parsePath("tikv://node1:2379,node2:2379")
	c.Assert(err, IsNil)
	c.Assert(etcdAddrs, DeepEquals, []string{"node1:2379", "node2:2379"})
	c.Assert(disableGC, IsFalse)

	_, _, err = parsePath("tikv://node1:2379")
	c.Assert(err, IsNil)
	_, disableGC, err = parsePath("tikv://node1:2379?disableGC=true")
	c.Assert(err, IsNil)
	c.Assert(disableGC, IsTrue)
}

func (s *testStoreSuite) TestOracle(c *C) {
	o := &mockoracle.MockOracle{}
	s.store.oracle = o

	ctx := context.Background()
	t1, err := s.store.getTimestampWithRetry(NewBackoffer(ctx, 100))
	c.Assert(err, IsNil)
	t2, err := s.store.getTimestampWithRetry(NewBackoffer(ctx, 100))
	c.Assert(err, IsNil)
	c.Assert(t1, Less, t2)

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
		t3, err := s.store.getTimestampWithRetry(NewBackoffer(ctx, tsoMaxBackoff))
		c.Assert(err, IsNil)
		c.Assert(t2, Less, t3)
		expired := s.store.oracle.IsExpired(t2, 50)
		c.Assert(expired, IsTrue)
	}()

	wg.Wait()
}

var _ pd.Client = &mockPDClient{}

type mockPDClient struct {
	sync.RWMutex
	client pd.Client
	stop   bool
}

func (c *mockPDClient) ConfigClient() pd.ConfigClient {
	return nil
}

func (c *mockPDClient) enable() {
	c.Lock()
	defer c.Unlock()
	c.stop = false
}

func (c *mockPDClient) disable() {
	c.Lock()
	defer c.Unlock()
	c.stop = true
}

func (c *mockPDClient) GetClusterID(context.Context) uint64 {
	return 1
}

func (c *mockPDClient) GetTS(ctx context.Context) (int64, int64, error) {
	c.RLock()
	defer c.RUnlock()

	if c.stop {
		return 0, 0, errors.Trace(errStopped)
	}
	return c.client.GetTS(ctx)
}

func (c *mockPDClient) GetTSAsync(ctx context.Context) pd.TSFuture {
	return nil
}

func (c *mockPDClient) GetRegion(ctx context.Context, key []byte) (*metapb.Region, *metapb.Peer, error) {
	c.RLock()
	defer c.RUnlock()

	if c.stop {
		return nil, nil, errors.Trace(errStopped)
	}
	return c.client.GetRegion(ctx, key)
}

func (c *mockPDClient) GetPrevRegion(ctx context.Context, key []byte) (*metapb.Region, *metapb.Peer, error) {
	c.RLock()
	defer c.RUnlock()

	if c.stop {
		return nil, nil, errors.Trace(errStopped)
	}
	return c.client.GetPrevRegion(ctx, key)
}

func (c *mockPDClient) GetRegionByID(ctx context.Context, regionID uint64) (*metapb.Region, *metapb.Peer, error) {
	c.RLock()
	defer c.RUnlock()

	if c.stop {
		return nil, nil, errors.Trace(errStopped)
	}
	return c.client.GetRegionByID(ctx, regionID)
}

func (c *mockPDClient) ScanRegions(ctx context.Context, startKey []byte, endKey []byte, limit int) ([]*metapb.Region, []*metapb.Peer, error) {
	c.RLock()
	defer c.RUnlock()

	if c.stop {
		return nil, nil, errors.Trace(errStopped)
	}
	return c.client.ScanRegions(ctx, startKey, endKey, limit)
}

func (c *mockPDClient) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	c.RLock()
	defer c.RUnlock()

	if c.stop {
		return nil, errors.Trace(errStopped)
	}
	return c.client.GetStore(ctx, storeID)
}

func (c *mockPDClient) GetAllStores(ctx context.Context, opts ...pd.GetStoreOption) ([]*metapb.Store, error) {
	c.RLock()
	defer c.Unlock()

	if c.stop {
		return nil, errors.Trace(errStopped)
	}
	return c.client.GetAllStores(ctx)
}

func (c *mockPDClient) UpdateGCSafePoint(ctx context.Context, safePoint uint64) (uint64, error) {
	panic("unimplemented")
}

func (c *mockPDClient) Close() {}

func (c *mockPDClient) ScatterRegion(ctx context.Context, regionID uint64) error {
	return nil
}

func (c *mockPDClient) GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error) {
	return &pdpb.GetOperatorResponse{Status: pdpb.OperatorStatus_SUCCESS}, nil
}

func (c *mockPDClient) GetLeaderAddr() string { return "mockpd" }

type checkRequestClient struct {
	Client
	priority pb.CommandPri
}

func (c *checkRequestClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	resp, err := c.Client.SendRequest(ctx, addr, req, timeout)
	if c.priority != req.Priority {
		if resp.Get != nil {
			resp.Get.Error = &pb.KeyError{
				Abort: "request check error",
			}
		}
	}
	return resp, err
}

func (s *testStoreSuite) TestRequestPriority(c *C) {
	client := &checkRequestClient{
		Client: s.store.client,
	}
	s.store.client = client

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
	_, err = txn.Get([]byte("key"))
	c.Assert(err, IsNil)

	// A counter example.
	client.priority = pb.CommandPri_Low
	txn.SetOption(kv.Priority, kv.PriorityNormal)
	_, err = txn.Get([]byte("key"))
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
