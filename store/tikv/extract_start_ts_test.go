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
	store.resolveTSMu.resolveTS[2] = 102
	store.resolveTSMu.resolveTS[3] = 101
	s.store = store
}

func (s *extractStartTsSuite) TestExtractStartTs(c *C) {
	i := uint64(100)
	cases := []kv.TransactionOption{
		// StartTS setted
		{TxnScope: oracle.GlobalTxnScope, StartTS: &i, PrevSec: nil, MinStartTS: nil, MaxPrevSec: nil},
		// PrevSec setted
		{TxnScope: oracle.GlobalTxnScope, StartTS: nil, PrevSec: &i, MinStartTS: nil, MaxPrevSec: nil},
		// MinStartTS setted, global
		{TxnScope: oracle.GlobalTxnScope, StartTS: nil, PrevSec: nil, MinStartTS: &i, MaxPrevSec: nil},
		// MinStartTS setted, local
		{TxnScope: oracle.LocalTxnScope, StartTS: nil, PrevSec: nil, MinStartTS: &i, MaxPrevSec: nil},
		// MaxPrevSec setted
		// however we need to add more cases to check the behavior when it fall backs to MinStartTS setted
		// see `TestMaxPrevSecFallback`
		{TxnScope: oracle.GlobalTxnScope, StartTS: nil, PrevSec: nil, MinStartTS: nil, MaxPrevSec: &i},
		// nothing setted
		{TxnScope: oracle.GlobalTxnScope, StartTS: nil, PrevSec: nil, MinStartTS: nil, MaxPrevSec: nil},
	}
	bo := NewBackofferWithVars(context.Background(), tsoMaxBackoff, nil)
	stalenessTimestamp, _ := s.store.getStalenessTimestamp(bo, oracle.GlobalTxnScope, 100)
	expectedTs := []uint64{
		100,
		stalenessTimestamp,

		101,
		102,

		stalenessTimestamp,
		// it's too hard to figure out the value `getTimestampWithRetry` returns
		// so we just check whether it is greater than stalenessTimestamp
		0,
	}
	for i, cs := range cases {
		expected := expectedTs[i]
		result, _ := extractStartTs(s.store, cs)
		if expected == 0 {
			c.Assert(result, Greater, stalenessTimestamp)
		} else {
			c.Assert(result, Equals, expected)
		}
	}
}

func (s *extractStartTsSuite) TestMaxPrevSecFallback(c *C) {
	s.store.resolveTSMu.resolveTS[2] = 0x8000000000000002
	s.store.resolveTSMu.resolveTS[3] = 0x8000000000000001

	i := uint64(100)
	cases := []kv.TransactionOption{
		{TxnScope: oracle.GlobalTxnScope, StartTS: nil, PrevSec: nil, MinStartTS: nil, MaxPrevSec: &i},
		{TxnScope: oracle.LocalTxnScope, StartTS: nil, PrevSec: nil, MinStartTS: nil, MaxPrevSec: &i},
	}
	expectedTs := []uint64{0x8000000000000001, 0x8000000000000002}
	for i, cs := range cases {
		expected := expectedTs[i]
		result, _ := extractStartTs(s.store, cs)
		c.Assert(result, Equals, expected)
	}
}
