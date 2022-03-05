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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/testkit"

	"github.com/tikv/client-go/v2/tikv"

	"github.com/stretchr/testify/require"
)

func TestTableSplit(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()
	session.SetSchemaLease(100 * time.Millisecond)
	session.DisableStats4Test()
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)
	tk := testkit.NewTestKit(t, store)
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
	require.NotNil(t, infoSchema)
	tbl, err := infoSchema.TableByName(model.NewCIStr("mysql"), model.NewCIStr("tidb"))
	require.NoError(t, err)
	checkRegionStartWithTableID(t, tbl.Meta().ID, store.(kvStore))

	tbl, err = infoSchema.TableByName(model.NewCIStr("test"), model.NewCIStr("t_part"))
	require.NoError(t, err)
	pi := tbl.Meta().GetPartitionInfo()
	require.NotNil(t, pi)
	for _, def := range pi.Definitions {
		checkRegionStartWithTableID(t, def.ID, store.(kvStore))
	}
}

type kvStore interface {
	GetRegionCache() *tikv.RegionCache
}

func checkRegionStartWithTableID(t *testing.T, id int64, store kvStore) {
	regionStartKey := tablecodec.EncodeTablePrefix(id)
	var loc *tikv.KeyLocation
	var err error
	cache := store.GetRegionCache()
	loc, err = cache.LocateKey(tikv.NewBackoffer(context.Background(), 5000), regionStartKey)
	require.NoError(t, err)
	// Region cache may be out of date, so we need to drop this expired region and load it again.
	cache.InvalidateCachedRegion(loc.Region)
	require.Equal(t, []byte(regionStartKey), loc.StartKey)
}
