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
	"context"
	"sync/atomic"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/tikv/client-go/v2/tikv"
)

type testDDLTableSplitSuite struct{}

var _ = Suite(&testDDLTableSplitSuite{})

func (s *testDDLTableSplitSuite) TestTableSplit(c *C) {
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	defer func() {
		err := store.Close()
		c.Assert(err, IsNil)
	}()
	session.SetSchemaLease(100 * time.Millisecond)
	session.DisableStats4Test()
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	// Synced split table region.
	tk.MustExec("set global tidb_scatter_region = 1")
	tk.MustExec(`create table t_part (a int key) partition by range(a) (
		partition p0 values less than (10),
		partition p1 values less than (20)
	)`)
	defer dom.Close()
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 0)
	infoSchema := dom.InfoSchema()
	c.Assert(infoSchema, NotNil)
	t, err := infoSchema.TableByName(model.NewCIStr("mysql"), model.NewCIStr("tidb"))
	c.Assert(err, IsNil)
	checkRegionStartWithTableID(c, t.Meta().ID, store.(kvStore))

	t, err = infoSchema.TableByName(model.NewCIStr("test"), model.NewCIStr("t_part"))
	c.Assert(err, IsNil)
	pi := t.Meta().GetPartitionInfo()
	c.Assert(pi, NotNil)
	for _, def := range pi.Definitions {
		checkRegionStartWithTableID(c, def.ID, store.(kvStore))
	}
}

type kvStore interface {
	GetRegionCache() *tikv.RegionCache
}

func checkRegionStartWithTableID(c *C, id int64, store kvStore) {
	regionStartKey := tablecodec.EncodeTablePrefix(id)
	var loc *tikv.KeyLocation
	var err error
	cache := store.GetRegionCache()
	loc, err = cache.LocateKey(tikv.NewBackoffer(context.Background(), 5000), regionStartKey)
	c.Assert(err, IsNil)
	// Region cache may be out of date, so we need to drop this expired region and load it again.
	cache.InvalidateCachedRegion(loc.Region)
	c.Assert(loc.StartKey, BytesEquals, []byte(regionStartKey))
}
