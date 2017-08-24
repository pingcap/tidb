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
	"sync"
	"time"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/errorpb"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/pd-client"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	goctx "golang.org/x/net/context"
)

type testStoreSuite struct {
	store *tikvStore
}

var _ = Suite(&testStoreSuite{})

func (s *testStoreSuite) SetUpTest(c *C) {
	store, err := NewMockTikvStore()
	c.Check(err, IsNil)
	s.store = store.(*tikvStore)
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
	o := &mockOracle{}
	s.store.oracle = o

	ctx := goctx.Background()
	t1, err := s.store.getTimestampWithRetry(NewBackoffer(100, ctx))
	c.Assert(err, IsNil)
	t2, err := s.store.getTimestampWithRetry(NewBackoffer(100, ctx))
	c.Assert(err, IsNil)
	c.Assert(t1, Less, t2)

	// Check retry.
	var wg sync.WaitGroup
	wg.Add(2)

	o.disable()
	go func() {
		defer wg.Done()
		time.Sleep(time.Millisecond * 100)
		o.enable()
	}()

	go func() {
		defer wg.Done()
		t3, err := s.store.getTimestampWithRetry(NewBackoffer(tsoMaxBackoff, ctx))
		c.Assert(err, IsNil)
		c.Assert(t2, Less, t3)
		expired := s.store.oracle.IsExpired(t2, 50)
		c.Assert(expired, IsTrue)
	}()

	wg.Wait()
}

func (s *testStoreSuite) TestBusyServerKV(c *C) {
	client := newBusyClient(s.store.client)
	s.store.client = client

	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	err = txn.Set([]byte("key"), []byte("value"))
	c.Assert(err, IsNil)
	err = txn.Commit()
	c.Assert(err, IsNil)

	var wg sync.WaitGroup
	wg.Add(2)

	client.setBusy(true)
	go func() {
		defer wg.Done()
		time.Sleep(time.Millisecond * 100)
		client.setBusy(false)
	}()

	go func() {
		defer wg.Done()
		txn, err := s.store.Begin()
		c.Assert(err, IsNil)
		val, err := txn.Get([]byte("key"))
		c.Assert(err, IsNil)
		c.Assert(val, BytesEquals, []byte("value"))
	}()

	wg.Wait()
}

func (s *testStoreSuite) TestBusyServerCop(c *C) {
	client := newBusyClient(s.store.client)
	s.store.client = client
	_, err := tidb.BootstrapSession(s.store)
	c.Assert(err, IsNil)

	session, err := tidb.CreateSession(s.store)
	c.Assert(err, IsNil)

	var wg sync.WaitGroup
	wg.Add(2)

	client.setBusy(true)
	go func() {
		defer wg.Done()
		time.Sleep(time.Millisecond * 100)
		client.setBusy(false)
	}()

	go func() {
		defer wg.Done()
		rs, err := session.Execute(`SELECT variable_value FROM mysql.tidb WHERE variable_name="bootstrapped"`)
		c.Assert(err, IsNil)
		row, err := rs[0].Next()
		c.Assert(err, IsNil)
		c.Assert(row, NotNil)
		c.Assert(row.Data[0].GetString(), Equals, "True")
	}()

	wg.Wait()
}

var errStopped = errors.New("stopped")

type mockOracle struct {
	sync.RWMutex
	stop   bool
	offset time.Duration
	lastTS uint64
}

func (o *mockOracle) enable() {
	o.Lock()
	defer o.Unlock()
	o.stop = false
}

func (o *mockOracle) disable() {
	o.Lock()
	defer o.Unlock()
	o.stop = true
}

func (o *mockOracle) setOffset(offset time.Duration) {
	o.Lock()
	defer o.Unlock()

	o.offset = offset
}

func (o *mockOracle) addOffset(d time.Duration) {
	o.Lock()
	defer o.Unlock()

	o.offset += d
}

func (o *mockOracle) GetTimestamp(goctx.Context) (uint64, error) {
	o.Lock()
	defer o.Unlock()

	if o.stop {
		return 0, errors.Trace(errStopped)
	}
	physical := oracle.GetPhysical(time.Now().Add(o.offset))
	ts := oracle.ComposeTS(physical, 0)
	if oracle.ExtractPhysical(o.lastTS) == physical {
		ts = o.lastTS + 1
	}
	o.lastTS = ts
	return ts, nil
}

type mockOracleFuture struct {
	o   *mockOracle
	ctx goctx.Context
}

func (m *mockOracleFuture) Wait() (uint64, error) {
	return m.o.GetTimestamp(m.ctx)
}

func (o *mockOracle) GetTimestampAsync(ctx goctx.Context) oracle.Future {
	return &mockOracleFuture{o, ctx}
}

func (o *mockOracle) IsExpired(lockTimestamp uint64, TTL uint64) bool {
	o.RLock()
	defer o.RUnlock()

	return oracle.GetPhysical(time.Now().Add(o.offset)) >= oracle.ExtractPhysical(lockTimestamp)+int64(TTL)
}

func (o *mockOracle) Close() {

}

type busyClient struct {
	client Client
	mu     struct {
		sync.RWMutex
		isBusy bool
	}
}

func newBusyClient(client Client) *busyClient {
	return &busyClient{
		client: client,
	}
}

func (c *busyClient) setBusy(busy bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.mu.isBusy = busy
}

func (c *busyClient) Close() error {
	return c.client.Close()
}

func (c *busyClient) SendReq(ctx goctx.Context, addr string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.mu.isBusy {
		return tikvrpc.GenRegionErrorResp(req, &errorpb.Error{ServerIsBusy: &errorpb.ServerIsBusy{}})
	}
	return c.client.SendReq(ctx, addr, req)
}

type mockPDClient struct {
	sync.RWMutex
	client pd.Client
	stop   bool
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

func (c *mockPDClient) GetClusterID(goctx.Context) uint64 {
	return 1
}

func (c *mockPDClient) GetTS(ctx goctx.Context) (int64, int64, error) {
	c.RLock()
	defer c.RUnlock()

	if c.stop {
		return 0, 0, errors.Trace(errStopped)
	}
	return c.client.GetTS(ctx)
}

func (c *mockPDClient) GetTSAsync(ctx goctx.Context) pd.TSFuture {
	return nil
}

func (c *mockPDClient) GetRegion(ctx goctx.Context, key []byte) (*metapb.Region, *metapb.Peer, error) {
	c.RLock()
	defer c.RUnlock()

	if c.stop {
		return nil, nil, errors.Trace(errStopped)
	}
	return c.client.GetRegion(ctx, key)
}

func (c *mockPDClient) GetRegionByID(ctx goctx.Context, regionID uint64) (*metapb.Region, *metapb.Peer, error) {
	c.RLock()
	defer c.RUnlock()

	if c.stop {
		return nil, nil, errors.Trace(errStopped)
	}
	return c.client.GetRegionByID(ctx, regionID)
}

func (c *mockPDClient) GetStore(ctx goctx.Context, storeID uint64) (*metapb.Store, error) {
	c.RLock()
	defer c.RUnlock()

	if c.stop {
		return nil, errors.Trace(errStopped)
	}
	return c.client.GetStore(ctx, storeID)
}

func (c *mockPDClient) Close() {}

type checkRequestClient struct {
	Client
	priority pb.CommandPri
}

func (c *checkRequestClient) SendReq(ctx goctx.Context, addr string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
	resp, err := c.Client.SendReq(ctx, addr, req)
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
	err = txn.Commit()
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
	iter, err := txn.Seek([]byte("key"))
	c.Assert(err, IsNil)
	for iter.Valid() {
		c.Assert(iter.Next(), IsNil)
	}
	iter.Close()
}
