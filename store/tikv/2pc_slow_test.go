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

// +build !race

package tikv

import (
	"context"
	"sync/atomic"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
)

// TestCommitMultipleRegions tests commit multiple regions.
// The test takes too long under the race detector.
func (s *testCommitterSuite) TestCommitMultipleRegions(c *C) {
	m := make(map[string]string)
	for i := 0; i < 100; i++ {
		k, v := randKV(10, 10)
		m[k] = v
	}
	s.mustCommit(c, m)

	// Test big values.
	m = make(map[string]string)
	for i := 0; i < 50; i++ {
		k, v := randKV(11, txnCommitBatchSize/7)
		m[k] = v
	}
	s.mustCommit(c, m)
}

func (s *testTiclientSuite) TestSplitRegionIn2PC(c *C) {
	if *WithTiKV {
		c.Skip("scatter will timeout with single node TiKV")
	}
	const preSplitThresholdInTest = 500
	old := atomic.LoadUint32(&preSplitDetectThreshold)
	defer atomic.StoreUint32(&preSplitDetectThreshold, old)
	atomic.StoreUint32(&preSplitDetectThreshold, preSplitThresholdInTest)

	old = atomic.LoadUint32(&preSplitSizeThreshold)
	defer atomic.StoreUint32(&preSplitSizeThreshold, old)
	atomic.StoreUint32(&preSplitSizeThreshold, 5000)

	bo := NewBackofferWithVars(context.Background(), 1, nil)
	checkKeyRegion := func(bo *Backoffer, start, end []byte, checker Checker) {
		// Check regions after split.
		loc1, err := s.store.regionCache.LocateKey(bo, start)
		c.Assert(err, IsNil)
		loc2, err := s.store.regionCache.LocateKey(bo, end)
		c.Assert(err, IsNil)
		c.Assert(loc1.Region.id, checker, loc2.Region.id)
	}
	mode := []string{"optimistic", "pessimistic"}
	var (
		startKey []byte
		endKey   []byte
	)
	ctx := context.Background()
	for _, m := range mode {
		if m == "optimistic" {
			startKey = encodeKey(s.prefix, s08d("key", 0))
			endKey = encodeKey(s.prefix, s08d("key", preSplitThresholdInTest))
		} else {
			startKey = encodeKey(s.prefix, s08d("pkey", 0))
			endKey = encodeKey(s.prefix, s08d("pkey", preSplitThresholdInTest))
		}
		// Check before test.
		checkKeyRegion(bo, startKey, endKey, Equals)
		txn := s.beginTxn(c)
		if m == "pessimistic" {
			txn.SetOption(kv.Pessimistic, true)
			lockCtx := &kv.LockCtx{}
			lockCtx.ForUpdateTS = txn.startTS
			keys := make([]kv.Key, 0, preSplitThresholdInTest)
			for i := 0; i < preSplitThresholdInTest; i++ {
				keys = append(keys, encodeKey(s.prefix, s08d("pkey", i)))
			}
			err := txn.LockKeys(ctx, lockCtx, keys...)
			c.Assert(err, IsNil)
			checkKeyRegion(bo, startKey, endKey, Not(Equals))
		}
		var err error
		for i := 0; i < preSplitThresholdInTest; i++ {
			if m == "optimistic" {
				err = txn.Set(encodeKey(s.prefix, s08d("key", i)), valueBytes(i))
			} else {
				err = txn.Set(encodeKey(s.prefix, s08d("pkey", i)), valueBytes(i))
			}
			c.Assert(err, IsNil)
		}
		err = txn.Commit(context.Background())
		c.Assert(err, IsNil)
		// Check region split after test.
		checkKeyRegion(bo, startKey, endKey, Not(Equals))
	}
}
