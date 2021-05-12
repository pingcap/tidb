// Copyright 2021 PingCAP, Inc.
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

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore/unistore"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
)

type extractStartTsSuite struct {
	store *KVStore
}

var _ = Suite(&extractStartTsSuite{})

func (s *extractStartTsSuite) SetUpTest(c *C) {
	client, pdClient, cluster, err := unistore.New("")
	c.Assert(err, IsNil)
	unistore.BootstrapWithSingleStore(cluster)
	store, err := NewTestTiKVStore(client, pdClient, nil, nil, 0)
	c.Assert(err, IsNil)
	store.regionCache.storeMu.stores[2] = &Store{
		storeID:   2,
		storeType: tikvrpc.TiKV,
		state:     uint64(resolved),
		labels: []*metapb.StoreLabel{
			{
				Key:   DCLabelKey,
				Value: oracle.LocalTxnScope,
			},
		},
	}
	store.regionCache.storeMu.stores[3] = &Store{
		storeID:   3,
		storeType: tikvrpc.TiKV,
		state:     uint64(resolved),
		labels: []*metapb.StoreLabel{{
			Key:   DCLabelKey,
			Value: "Some Random Label",
		}},
	}
	store.setSafeTS(2, 102)
	store.setSafeTS(3, 101)
	s.store = store
}

func (s *extractStartTsSuite) TestExtractStartTs(c *C) {
	i := uint64(100)
	bo := NewBackofferWithVars(context.Background(), tsoMaxBackoff, nil)
	stalenessTimestamp, _ := s.store.getStalenessTimestamp(bo, oracle.GlobalTxnScope, 100)

	cases := []struct {
		expectedTS uint64
		option     kv.TransactionOption
	}{
		// StartTS setted
		{100, kv.TransactionOption{TxnScope: oracle.GlobalTxnScope, StartTS: &i, PrevSec: nil, MinStartTS: nil, MaxPrevSec: nil}},
		// PrevSec setted
		{stalenessTimestamp, kv.TransactionOption{TxnScope: oracle.GlobalTxnScope, StartTS: nil, PrevSec: &i, MinStartTS: nil, MaxPrevSec: nil}},
		// MinStartTS setted, global
		{101, kv.TransactionOption{TxnScope: oracle.GlobalTxnScope, StartTS: nil, PrevSec: nil, MinStartTS: &i, MaxPrevSec: nil}},
		// MinStartTS setted, local
		{102, kv.TransactionOption{TxnScope: oracle.LocalTxnScope, StartTS: nil, PrevSec: nil, MinStartTS: &i, MaxPrevSec: nil}},
		// MaxPrevSec setted
		// however we need to add more cases to check the behavior when it fall backs to MinStartTS setted
		// see `TestMaxPrevSecFallback`
		{stalenessTimestamp, kv.TransactionOption{TxnScope: oracle.GlobalTxnScope, StartTS: nil, PrevSec: nil, MinStartTS: nil, MaxPrevSec: &i}},
		// nothing setted
		{0, kv.TransactionOption{TxnScope: oracle.GlobalTxnScope, StartTS: nil, PrevSec: nil, MinStartTS: nil, MaxPrevSec: nil}},
	}
	for _, cs := range cases {
		expected := cs.expectedTS
		result, _ := extractStartTs(s.store, cs.option)
		if expected == 0 {
			c.Assert(result, Greater, stalenessTimestamp)
		} else if expected == stalenessTimestamp {
			// "stalenessTimestamp" fetched by extractStartTs can be later than stalenessTimestamp fetched in this function
			// because it *is* created in later physical time
			c.Assert(result, GreaterEqual, expected)
			// but it should not be late too much
			maxStalenessTimestamp := oracle.ComposeTS(oracle.ExtractPhysical(stalenessTimestamp)+50, oracle.ExtractLogical(stalenessTimestamp))
			c.Assert(result, Less, maxStalenessTimestamp)
		} else {
			c.Assert(result, Equals, expected)
		}
	}
}

func (s *extractStartTsSuite) TestMaxPrevSecFallback(c *C) {
	s.store.setSafeTS(2, 0x8000000000000002)
	s.store.setSafeTS(3, 0x8000000000000001)
	i := uint64(100)
	cases := []struct {
		expectedTS uint64
		option     kv.TransactionOption
	}{
		{0x8000000000000001, kv.TransactionOption{TxnScope: oracle.GlobalTxnScope, StartTS: nil, PrevSec: nil, MinStartTS: nil, MaxPrevSec: &i}},
		{0x8000000000000002, kv.TransactionOption{TxnScope: oracle.LocalTxnScope, StartTS: nil, PrevSec: nil, MinStartTS: nil, MaxPrevSec: &i}},
	}
	for _, cs := range cases {
		result, _ := extractStartTs(s.store, cs.option)
		c.Assert(result, Equals, cs.expectedTS)
	}
}
