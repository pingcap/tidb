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
			Value: "local1",
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

func (s *extractStartTsSuite) TestExtractStartTS(c *C) {
	i := uint64(100)
	// to prevent time change during test case execution
	// we use failpoint to make it "fixed"
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/MockCurrentTimestamp", `return(300)`), IsNil)
	cases := []struct {
		expectedTS uint64
		option     tikv.StartTSOption
	}{
		// StartTS setted
		{100, tikv.StartTSOption{TxnScope: oracle.GlobalTxnScope, StartTS: &i}},
		// nothing setted
		{300, tikv.StartTSOption{TxnScope: oracle.GlobalTxnScope, StartTS: nil}},
	}
	for _, cs := range cases {
		expected := cs.expectedTS
		result, _ := tikv.ExtractStartTS(s.store, cs.option)
		c.Assert(result, Equals, expected)
	}
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/MockCurrentTimestamp"), IsNil)
}
