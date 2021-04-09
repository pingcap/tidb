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

package tikv_test

import (
	"context"

	. "github.com/pingcap/check"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/kv"
)

func (s *testTiclientSuite) TestSplitRegionIn2PC(c *C) {
	if *WithTiKV {
		c.Skip("scatter will timeout with single node TiKV")
	}
	config := tikv.ConfigProbe{}
	const preSplitThresholdInTest = 500
	old := config.LoadPreSplitDetectThreshold()
	defer config.StorePreSplitDetectThreshold(old)
	config.StorePreSplitDetectThreshold(preSplitThresholdInTest)

	old = config.LoadPreSplitSizeThreshold()
	defer config.StorePreSplitSizeThreshold(old)
	config.StorePreSplitSizeThreshold(5000)

	bo := tikv.NewBackofferWithVars(context.Background(), 1, nil)
	checkKeyRegion := func(bo *tikv.Backoffer, start, end []byte, checker Checker) {
		// Check regions after split.
		loc1, err := s.store.GetRegionCache().LocateKey(bo, start)
		c.Assert(err, IsNil)
		loc2, err := s.store.GetRegionCache().LocateKey(bo, end)
		c.Assert(err, IsNil)
		c.Assert(loc1.Region.GetID(), checker, loc2.Region.GetID())
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
			lockCtx := &tidbkv.LockCtx{}
			lockCtx.ForUpdateTS = txn.StartTS()
			keys := make([]tidbkv.Key, 0, preSplitThresholdInTest)
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
