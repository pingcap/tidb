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
	"bytes"
	"context"
	"sync/atomic"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/tablecodec"
)

type testDDLTableSplitSuite struct{}

var _ = Suite(&testDDLTableSplitSuite{})

func (s *testDDLTableSplitSuite) TestTableSplit(c *C) {
	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	defer store.Close()
	session.SetSchemaLease(0)
	session.SetStatsLease(0)
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	defer dom.Close()
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 0)
	infoSchema := dom.InfoSchema()
	c.Assert(infoSchema, NotNil)
	t, err := infoSchema.TableByName(model.NewCIStr("mysql"), model.NewCIStr("tidb"))
	c.Assert(err, IsNil)
	regionStartKey := tablecodec.EncodeTablePrefix(t.Meta().ID)

	type kvStore interface {
		GetRegionCache() *tikv.RegionCache
	}
	var loc *tikv.KeyLocation
	for i := 0; i < 10; i++ {
		cache := store.(kvStore).GetRegionCache()
		loc, err = cache.LocateKey(tikv.NewBackoffer(context.Background(), 5000), regionStartKey)
		c.Assert(err, IsNil)

		// Region cache may be out of date, so we need to drop this expired region and load it again.
		cache.DropRegion(loc.Region)
		if bytes.Equal(loc.StartKey, []byte(regionStartKey)) {
			return
		}
		time.Sleep(3 * time.Millisecond)
	}
	c.Assert(loc.StartKey, BytesEquals, []byte(regionStartKey))
}
