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

package tikv_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/store/mockstore/unistore"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
)

type extractStartTsSuite struct {
	store *tikv.KVStore
}

var _ = SerialSuites(&extractStartTsSuite{})

func (s *extractStartTsSuite) SetUpTest(c *C) {
	client, pdClient, cluster, err := unistore.New("")
	c.Assert(err, IsNil)
	unistore.BootstrapWithSingleStore(cluster)
	store, err := tikv.NewTestTiKVStore(client, pdClient, nil, nil, 0)
	c.Assert(err, IsNil)
	probe := tikv.StoreProbe{KVStore: store}
	probe.SetRegionCacheStore(2, tikvrpc.TiKV, 1, []*metapb.StoreLabel{
		{
			Key:   tikv.DCLabelKey,
			Value: oracle.LocalTxnScope,
		},
	})
	probe.SetRegionCacheStore(3, tikvrpc.TiKV, 1, []*metapb.StoreLabel{
		{
			Key:   tikv.DCLabelKey,
			Value: "Some Random Label",
		},
	})
	probe.SetSafeTS(2, 102)
	probe.SetSafeTS(3, 101)
	s.store = probe.KVStore
}

func (s *extractStartTsSuite) TestExtractStartTs(c *C) {
	i := uint64(100)
	// to prevent time change during test case execution
	// we use failpoint to make it "fixed"
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/MockStalenessTimestamp", "return(200)"), IsNil)
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/MockCurrentTimestamp", `return(300)`), IsNil)

	cases := []struct {
		expectedTS uint64
		option     tikv.StartTSOption
	}{
		// StartTS setted
		{100, tikv.StartTSOption{TxnScope: oracle.GlobalTxnScope, StartTS: &i, PrevSec: nil, MinStartTS: nil, MaxPrevSec: nil}},
		// PrevSec setted
		{200, tikv.StartTSOption{TxnScope: oracle.GlobalTxnScope, StartTS: nil, PrevSec: &i, MinStartTS: nil, MaxPrevSec: nil}},
		// MinStartTS setted, global
		{101, tikv.StartTSOption{TxnScope: oracle.GlobalTxnScope, StartTS: nil, PrevSec: nil, MinStartTS: &i, MaxPrevSec: nil}},
		// MinStartTS setted, local
		{102, tikv.StartTSOption{TxnScope: oracle.LocalTxnScope, StartTS: nil, PrevSec: nil, MinStartTS: &i, MaxPrevSec: nil}},
		// MaxPrevSec setted
		// however we need to add more cases to check the behavior when it fall backs to MinStartTS setted
		// see `TestMaxPrevSecFallback`
		{200, tikv.StartTSOption{TxnScope: oracle.GlobalTxnScope, StartTS: nil, PrevSec: nil, MinStartTS: nil, MaxPrevSec: &i}},
		// nothing setted
		{300, tikv.StartTSOption{TxnScope: oracle.GlobalTxnScope, StartTS: nil, PrevSec: nil, MinStartTS: nil, MaxPrevSec: nil}},
	}
	for _, cs := range cases {
		expected := cs.expectedTS
		result, _ := tikv.ExtractStartTs(s.store, cs.option)
		c.Assert(result, Equals, expected)
	}

	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/MockStalenessTimestamp"), IsNil)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/MockCurrentTimestamp"), IsNil)
}

func (s *extractStartTsSuite) TestMaxPrevSecFallback(c *C) {
	probe := tikv.StoreProbe{KVStore: s.store}
	probe.SetSafeTS(2, 0x8000000000000002)
	probe.SetSafeTS(3, 0x8000000000000001)
	i := uint64(100)
	cases := []struct {
		expectedTS uint64
		option     tikv.StartTSOption
	}{
		{0x8000000000000001, tikv.StartTSOption{TxnScope: oracle.GlobalTxnScope, StartTS: nil, PrevSec: nil, MinStartTS: nil, MaxPrevSec: &i}},
		{0x8000000000000002, tikv.StartTSOption{TxnScope: oracle.LocalTxnScope, StartTS: nil, PrevSec: nil, MinStartTS: nil, MaxPrevSec: &i}},
	}
	for _, cs := range cases {
		result, _ := tikv.ExtractStartTs(s.store, cs.option)
		c.Assert(result, Equals, cs.expectedTS)
	}
}
