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
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/store/tikv/mock-tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
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
	store, err := newTikvStore("mock-tikv-store", mocktikv.NewPDClient(s.cluster), clientFactory, false)
	c.Assert(err, IsNil)
	s.store = store
}

func (s *testStoreSuite) TestOracle(c *C) {
	o := &mockOracle{}
	s.store.oracle = o

	t1, err := s.store.getTimestampWithRetry(NewBackoffer(100))
	c.Assert(err, IsNil)
	t2, err := s.store.getTimestampWithRetry(NewBackoffer(100))
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
		t3, err := s.store.getTimestampWithRetry(NewBackoffer(tsoMaxBackoff))
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

func (o *mockOracle) GetTimestamp() (uint64, error) {
	o.RLock()
	defer o.RUnlock()

	if o.stop {
		return 0, errors.New("stopped")
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

func (c *busyClient) Close() error {
	return c.client.Close()
}

func (c *busyClient) SendKVReq(addr string, req *kvrpcpb.Request, timeout time.Duration) (*kvrpcpb.Response, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.mu.isBusy {
		return &kvrpcpb.Response{
			RegionError: &errorpb.Error{
				ServerIsBusy: &errorpb.ServerIsBusy{},
			},
		}, nil
	}
	return c.client.SendKVReq(addr, req, timeout)
}

func (c *busyClient) SendCopReq(addr string, req *coprocessor.Request, timeout time.Duration) (*coprocessor.Response, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.mu.isBusy {
		return &coprocessor.Response{
			RegionError: &errorpb.Error{
				ServerIsBusy: &errorpb.ServerIsBusy{},
			},
		}, nil
	}
	return c.client.SendCopReq(addr, req, timeout)
}
