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
	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/store/mockstore/unistore"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
)

type extractStartTsSuite struct {
	store *KVStore
}

var _ = SerialSuites(&extractStartTsSuite{})

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
	// to prevent time change during test case execution
	// we use failpoint to make it "fixed"
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/MockStalenessTimestamp", "return(200)"), IsNil)
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/MockCurrentTimestamp", `return(300)`), IsNil)

	cases := []struct {
		expectedTS uint64
		option     TransactionOption
	}{
		// StartTS setted
		{100, TransactionOption{TxnScope: oracle.GlobalTxnScope, StartTS: &i, PrevSec: nil, MinStartTS: nil, MaxPrevSec: nil}},
		// PrevSec setted
		{200, TransactionOption{TxnScope: oracle.GlobalTxnScope, StartTS: nil, PrevSec: &i, MinStartTS: nil, MaxPrevSec: nil}},
		// MinStartTS setted, global
		{101, TransactionOption{TxnScope: oracle.GlobalTxnScope, StartTS: nil, PrevSec: nil, MinStartTS: &i, MaxPrevSec: nil}},
		// MinStartTS setted, local
		{102, TransactionOption{TxnScope: oracle.LocalTxnScope, StartTS: nil, PrevSec: nil, MinStartTS: &i, MaxPrevSec: nil}},
		// MaxPrevSec setted
		// however we need to add more cases to check the behavior when it fall backs to MinStartTS setted
		// see `TestMaxPrevSecFallback`
		{200, TransactionOption{TxnScope: oracle.GlobalTxnScope, StartTS: nil, PrevSec: nil, MinStartTS: nil, MaxPrevSec: &i}},
		// nothing setted
		{300, TransactionOption{TxnScope: oracle.GlobalTxnScope, StartTS: nil, PrevSec: nil, MinStartTS: nil, MaxPrevSec: nil}},
	}
	for _, cs := range cases {
		expected := cs.expectedTS
		result, _ := extractStartTs(s.store, cs.option)
		c.Assert(result, Equals, expected)
	}

	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/MockStalenessTimestamp"), IsNil)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/MockCurrentTimestamp"), IsNil)
}

func (s *extractStartTsSuite) TestMaxPrevSecFallback(c *C) {
	s.store.setSafeTS(2, 0x8000000000000002)
	s.store.setSafeTS(3, 0x8000000000000001)
	i := uint64(100)
	cases := []struct {
		expectedTS uint64
		option     TransactionOption
	}{
		{0x8000000000000001, TransactionOption{TxnScope: oracle.GlobalTxnScope, StartTS: nil, PrevSec: nil, MinStartTS: nil, MaxPrevSec: &i}},
		{0x8000000000000002, TransactionOption{TxnScope: oracle.LocalTxnScope, StartTS: nil, PrevSec: nil, MinStartTS: nil, MaxPrevSec: &i}},
	}
	for _, cs := range cases {
		result, _ := extractStartTs(s.store, cs.option)
		c.Assert(result, Equals, cs.expectedTS)
	}
}
