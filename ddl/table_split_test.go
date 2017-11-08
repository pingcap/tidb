// Copyright 2017 PingCAP, Inc.
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

package ddl_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/tablecodec"
	"golang.org/x/net/context"
)

type testDDLTableSplitSuite struct{}

var _ = Suite(&testDDLTableSplitSuite{})

func (s *testDDLTableSplitSuite) TestTableSplit(c *C) {
	store, err := tikv.NewMockTikvStore()
	c.Assert(err, IsNil)
	tidb.SetSchemaLease(0)
	tidb.SetStatsLease(0)
	ddl.EnableSplitTableRegion = true
	dom, err := tidb.BootstrapSession(store)
	c.Assert(err, IsNil)
	ddl.EnableSplitTableRegion = false
	infoSchema := dom.InfoSchema()
	c.Assert(infoSchema, NotNil)
	t, err := infoSchema.TableByName(model.NewCIStr("mysql"), model.NewCIStr("tidb"))
	c.Assert(err, IsNil)
	regionStartKey := tablecodec.EncodeTablePrefix(t.Meta().ID)

	type kvStore interface {
		GetRegionCache() *tikv.RegionCache
	}
	cache := store.(kvStore).GetRegionCache()
	loc, err := cache.LocateKey(tikv.NewBackoffer(5000, context.Background()), regionStartKey)
	c.Assert(err, IsNil)
	c.Assert(loc.StartKey, BytesEquals, []byte(regionStartKey))
}
