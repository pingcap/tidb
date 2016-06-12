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
	clientFactory := mockClientFactory(s.cluster, mvccStore)
	s.store = newTikvStore("mock-tikv-store", mocktikv.NewPDClient(s.cluster), clientFactory)
}

func (s *testStoreSuite) TestOracle(c *C) {
	o := newMockOracle(s.store.oracle)
	s.store.oracle = o

	t1, err := s.store.getTimestampWithRetry()
	c.Assert(err, IsNil)
	t2, err := s.store.getTimestampWithRetry()
	c.Assert(err, IsNil)
	c.Assert(t1, Less, t2)

	// Check retry.
	var wg sync.WaitGroup
	wg.Add(3)

	o.disable()
	go func() {
		time.Sleep(time.Second)
		o.enable()
		wg.Done()
	}()

	go func() {
		t3, err := s.store.getTimestampWithRetry()
		c.Assert(err, IsNil)
		c.Assert(t2, Less, t3)
		wg.Done()
	}()

	go func() {
		expired, err := s.store.checkTimestampExpiredWithRetry(t2, 500)
		c.Assert(err, IsNil)
		c.Assert(expired, IsTrue)
		wg.Done()
	}()

	wg.Wait()
}

type mockOracle struct {
	oracle.Oracle
	mu   sync.RWMutex
	stop bool
}

func newMockOracle(oracle oracle.Oracle) *mockOracle {
	return &mockOracle{Oracle: oracle}
}

func (o *mockOracle) enable() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.stop = false
}

func (o *mockOracle) disable() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.stop = true
}

func (o *mockOracle) GetTimestamp() (uint64, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	if o.stop {
		return 0, errors.New("stopped")
	}
	return o.Oracle.GetTimestamp()
}

func (o *mockOracle) IsExpired(lockTimestamp uint64, TTL uint64) (bool, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	if o.stop {
		return false, errors.New("stopped")
	}
	return o.Oracle.IsExpired(lockTimestamp, TTL)
}
