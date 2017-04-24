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
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	pd "github.com/pingcap/pd/pd-client"
	"github.com/pingcap/tidb"
	mocktikv "github.com/pingcap/tidb/store/tikv/mock-tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	goctx "golang.org/x/net/context"
)

type testStoreSuite struct {
	cluster *mocktikv.Cluster
	store   *tikvStore
}

var _ = Suite(&testStoreSuite{})

func (s *testStoreSuite) SetUpTest(c *C) {
	s.cluster = mocktikv.NewCluster()
	mocktikv.BootstrapWithSingleStore(s.cluster)
	mvccStore := mocktikv.NewMvccStore()
	clientFactory := mocktikv.NewRPCClient(s.cluster, mvccStore)
	pdCli := &codecPDClient{mocktikv.NewPDClient(s.cluster)}
	store, err := newTikvStore("mock-tikv-store", pdCli, clientFactory, false)
	c.Assert(err, IsNil)
	s.store = store
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

func (c *busyClient) KvGet(ctx goctx.Context, addr string, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.mu.isBusy {
		return &kvrpcpb.GetResponse{
			RegionError: &errorpb.Error{
				ServerIsBusy: &errorpb.ServerIsBusy{},
			},
		}, nil
	}
	return c.client.KvGet(ctx, addr, req)
}

func (c *busyClient) KvScan(ctx goctx.Context, addr string, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.mu.isBusy {
		return &kvrpcpb.ScanResponse{
			RegionError: &errorpb.Error{
				ServerIsBusy: &errorpb.ServerIsBusy{},
			},
		}, nil
	}
	return c.client.KvScan(ctx, addr, req)
}

func (c *busyClient) KvPrewrite(ctx goctx.Context, addr string, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.mu.isBusy {
		return &kvrpcpb.PrewriteResponse{
			RegionError: &errorpb.Error{
				ServerIsBusy: &errorpb.ServerIsBusy{},
			},
		}, nil
	}
	return c.client.KvPrewrite(ctx, addr, req)
}

func (c *busyClient) KvCommit(ctx goctx.Context, addr string, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.mu.isBusy {
		return &kvrpcpb.CommitResponse{
			RegionError: &errorpb.Error{
				ServerIsBusy: &errorpb.ServerIsBusy{},
			},
		}, nil
	}
	return c.client.KvCommit(ctx, addr, req)
}

func (c *busyClient) KvCleanup(ctx goctx.Context, addr string, req *kvrpcpb.CleanupRequest) (*kvrpcpb.CleanupResponse, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.mu.isBusy {
		return &kvrpcpb.CleanupResponse{
			RegionError: &errorpb.Error{
				ServerIsBusy: &errorpb.ServerIsBusy{},
			},
		}, nil
	}
	return c.client.KvCleanup(ctx, addr, req)
}

func (c *busyClient) KvBatchGet(ctx goctx.Context, addr string, req *kvrpcpb.BatchGetRequest) (*kvrpcpb.BatchGetResponse, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.mu.isBusy {
		return &kvrpcpb.BatchGetResponse{
			RegionError: &errorpb.Error{
				ServerIsBusy: &errorpb.ServerIsBusy{},
			},
		}, nil
	}
	return c.client.KvBatchGet(ctx, addr, req)
}

func (c *busyClient) KvBatchRollback(ctx goctx.Context, addr string, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.mu.isBusy {
		return &kvrpcpb.BatchRollbackResponse{
			RegionError: &errorpb.Error{
				ServerIsBusy: &errorpb.ServerIsBusy{},
			},
		}, nil
	}
	return c.client.KvBatchRollback(ctx, addr, req)
}

func (c *busyClient) KvScanLock(ctx goctx.Context, addr string, req *kvrpcpb.ScanLockRequest) (*kvrpcpb.ScanLockResponse, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.mu.isBusy {
		return &kvrpcpb.ScanLockResponse{
			RegionError: &errorpb.Error{
				ServerIsBusy: &errorpb.ServerIsBusy{},
			},
		}, nil
	}
	return c.client.KvScanLock(ctx, addr, req)
}

func (c *busyClient) KvResolveLock(ctx goctx.Context, addr string, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.mu.isBusy {
		return &kvrpcpb.ResolveLockResponse{
			RegionError: &errorpb.Error{
				ServerIsBusy: &errorpb.ServerIsBusy{},
			},
		}, nil
	}
	return c.client.KvResolveLock(ctx, addr, req)
}

func (c *busyClient) KvGC(ctx goctx.Context, addr string, req *kvrpcpb.GCRequest) (*kvrpcpb.GCResponse, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.mu.isBusy {
		return &kvrpcpb.GCResponse{
			RegionError: &errorpb.Error{
				ServerIsBusy: &errorpb.ServerIsBusy{},
			},
		}, nil
	}
	return c.client.KvGC(ctx, addr, req)
}

func (c *busyClient) RawGet(ctx goctx.Context, addr string, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.mu.isBusy {
		return &kvrpcpb.RawGetResponse{
			RegionError: &errorpb.Error{
				ServerIsBusy: &errorpb.ServerIsBusy{},
			},
		}, nil
	}
	return c.client.RawGet(ctx, addr, req)
}

func (c *busyClient) RawPut(ctx goctx.Context, addr string, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.mu.isBusy {
		return &kvrpcpb.RawPutResponse{
			RegionError: &errorpb.Error{
				ServerIsBusy: &errorpb.ServerIsBusy{},
			},
		}, nil
	}
	return c.client.RawPut(ctx, addr, req)
}

func (c *busyClient) RawDelete(ctx goctx.Context, addr string, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.mu.isBusy {
		return &kvrpcpb.RawDeleteResponse{
			RegionError: &errorpb.Error{
				ServerIsBusy: &errorpb.ServerIsBusy{},
			},
		}, nil
	}
	return c.client.RawDelete(ctx, addr, req)
}

func (c *busyClient) Coprocessor(ctx goctx.Context, addr string, req *coprocessor.Request) (*coprocessor.Response, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.mu.isBusy {
		return &coprocessor.Response{
			RegionError: &errorpb.Error{
				ServerIsBusy: &errorpb.ServerIsBusy{},
			},
		}, nil
	}
	return c.client.Coprocessor(ctx, addr, req)
}

func (c *busyClient) Close() error {
	return c.client.Close()
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
