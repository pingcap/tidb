// Copyright 2025 PingCAP, Inc.
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

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func TestTableSplit(t *testing.T) {
	store, err := mockstore.NewMockStore(mockstore.WithStoreType(mockstore.EmbedUnistore))
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()
	vardef.SetSchemaLease(100 * time.Millisecond)
	session.DisableStats4Test()
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// Synced split table region.
	tk.MustExec("set @@session.tidb_scatter_region = 'table'")
	tk.MustExec(`create table t_part (a int key) partition by range(a) (
		partition p0 values less than (10),
		partition p1 values less than (20)
	)`)
	tk.MustQuery("select @@global.tidb_scatter_region;").Check(testkit.Rows(""))
	tk.MustExec("set @@global.tidb_scatter_region = 'table'")
	tk = testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t_part_2 (a int key) partition by range(a) (
		partition p0 values less than (10),
		partition p1 values less than (20)
	)`)
	defer dom.Close()
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 0)
	infoSchema := dom.InfoSchema()
	require.NotNil(t, infoSchema)
	tbl, err := infoSchema.TableByName(context.Background(), ast.NewCIStr("mysql"), ast.NewCIStr("tidb"))
	require.NoError(t, err)
	checkRegionStartWithTableID(t, tbl.Meta().ID, store.(kvStore))

	tbl, err = infoSchema.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_part"))
	require.NoError(t, err)
	pi := tbl.Meta().GetPartitionInfo()
	require.NotNil(t, pi)
	for _, def := range pi.Definitions {
		checkRegionStartWithTableID(t, def.ID, store.(kvStore))
	}
	tbl, err = infoSchema.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_part_2"))
	require.NoError(t, err)
	pi = tbl.Meta().GetPartitionInfo()
	require.NotNil(t, pi)
	for _, def := range pi.Definitions {
		checkRegionStartWithTableID(t, def.ID, store.(kvStore))
	}
}

// TestScatterRegion test the behavior of the tidb_scatter_region system variable, for verifying:
// 1. The variable can be set and queried correctly at both session and global levels.
// 2. Changes to the global variable affect new sessions but not existing ones.
// 3. The variable only accepts valid values (”, 'table', 'global').
// 4. Attempts to set invalid values result in appropriate error messages.
func TestScatterRegion(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)

	tk.MustQuery("select @@tidb_scatter_region;").Check(testkit.Rows(""))
	tk.MustExec("set @@tidb_scatter_region = 'table';")
	tk.MustQuery("select @@tidb_scatter_region;").Check(testkit.Rows("table"))
	tk.MustExec("set @@tidb_scatter_region = 'global';")
	tk.MustQuery("select @@tidb_scatter_region;").Check(testkit.Rows("global"))
	tk.MustExec("set @@tidb_scatter_region = 'TABLE';")
	tk.MustQuery("select @@tidb_scatter_region;").Check(testkit.Rows("table"))
	tk.MustExec("set @@tidb_scatter_region = 'GLOBAL';")
	tk.MustQuery("select @@tidb_scatter_region;").Check(testkit.Rows("global"))
	tk.MustExec("set @@tidb_scatter_region = '';")
	tk.MustQuery("select @@tidb_scatter_region;").Check(testkit.Rows(""))

	tk.MustExec("set global tidb_scatter_region = 'table';")
	tk.MustQuery("select @@global.tidb_scatter_region;").Check(testkit.Rows("table"))
	tk.MustQuery("select @@tidb_scatter_region;").Check(testkit.Rows(""))
	tk2.MustQuery("select @@tidb_scatter_region;").Check(testkit.Rows(""))
	tk2 = testkit.NewTestKit(t, store)
	tk2.MustQuery("select @@tidb_scatter_region;").Check(testkit.Rows("table"))

	tk.MustExec("set global tidb_scatter_region = 'global';")
	tk.MustQuery("select @@global.tidb_scatter_region;").Check(testkit.Rows("global"))
	tk.MustExec("set global tidb_scatter_region = '';")
	tk.MustQuery("select @@global.tidb_scatter_region;").Check(testkit.Rows(""))
	tk2 = testkit.NewTestKit(t, store)
	tk2.MustQuery("select @@tidb_scatter_region;").Check(testkit.Rows(""))

	tk.MustExec("set global tidb_scatter_region = 'TABLE';")
	tk.MustQuery("select @@global.tidb_scatter_region;").Check(testkit.Rows("table"))
	tk.MustQuery("select @@tidb_scatter_region;").Check(testkit.Rows(""))
	tk2 = testkit.NewTestKit(t, store)
	tk2.MustQuery("select @@tidb_scatter_region;").Check(testkit.Rows("table"))

	tk.MustExec("set global tidb_scatter_region = 'GLOBAL';")
	tk.MustQuery("select @@global.tidb_scatter_region;").Check(testkit.Rows("global"))
	tk.MustExec("set global tidb_scatter_region = '';")
	tk.MustQuery("select @@global.tidb_scatter_region;").Check(testkit.Rows(""))

	err := tk.ExecToErr("set @@tidb_scatter_region = 'test';")
	require.ErrorContains(t, err, "invalid value for 'test', it should be either '', 'table' or 'global'")
	err = tk.ExecToErr("set @@tidb_scatter_region = 'te st';")
	require.ErrorContains(t, err, "invalid value for 'te st', it should be either '', 'table' or 'global'")
	err = tk.ExecToErr("set @@tidb_scatter_region = '1';")
	require.ErrorContains(t, err, "invalid value for '1', it should be either '', 'table' or 'global'")
	err = tk.ExecToErr("set @@tidb_scatter_region = 0;")
	require.ErrorContains(t, err, "invalid value for '0', it should be either '', 'table' or 'global'")

	tk.MustQuery("select @@tidb_scatter_region;").Check(testkit.Rows(""))
	tk.MustExec("set @@tidb_scatter_region = 'TaBlE';")
	tk.MustQuery("select @@tidb_scatter_region;").Check(testkit.Rows("table"))
	tk.MustExec("set @@tidb_scatter_region = 'gLoBaL';")
	tk.MustQuery("select @@tidb_scatter_region;").Check(testkit.Rows("global"))
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

func TestTableSplitPolicy(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_scatter_region = 'table'")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (id bigint primary key, name varchar(100))")
	tk.MustExec("alter table t1 split between (0) and (1000000) regions 4")

	tbl := external.GetTableByName(t, tk, "test", "t1")
	require.NotNil(t, tbl.Meta().TableSplitPolicy)
	require.Equal(t, int64(4), tbl.Meta().TableSplitPolicy.Regions)
	require.Equal(t, []string{"0"}, tbl.Meta().TableSplitPolicy.Lower)
	require.Equal(t, []string{"1000000"}, tbl.Meta().TableSplitPolicy.Upper)

	tk.MustExec("alter table t1 add index idx_name (name)")
	tk.MustExec("alter table t1 split index idx_name between ('a') and ('z') regions 3")

	tbl = external.GetTableByName(t, tk, "test", "t1")
	var idxInfo *model.IndexInfo
	for _, idx := range tbl.Meta().Indices {
		if idx.Name.L == "idx_name" {
			idxInfo = idx
			break
		}
	}
	require.NotNil(t, idxInfo)
	require.NotNil(t, idxInfo.RegionSplitPolicy)
	require.Equal(t, int64(3), idxInfo.RegionSplitPolicy.Regions)

	tk.MustExec("admin check table t1")

	tk.MustExec("drop table if exists t2")
	tk.MustExec(`create table t2 (
		id bigint primary key,
		user_id bigint,
		index idx_user (user_id)
	) split between (0) and (1000000) regions 4
	  split index idx_user between (100) and (100000) regions 3`)

	tbl = external.GetTableByName(t, tk, "test", "t2")
	require.NotNil(t, tbl.Meta().TableSplitPolicy)
	require.Equal(t, int64(4), tbl.Meta().TableSplitPolicy.Regions)

	for _, idx := range tbl.Meta().Indices {
		if idx.Name.L == "idx_user" {
			require.NotNil(t, idx.RegionSplitPolicy)
			require.Equal(t, int64(3), idx.RegionSplitPolicy.Regions)
		}
	}

	tk.MustExec("admin check table t2")
}

func TestTableSplitPolicyForPartitionedTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_scatter_region = 'table'")

	tk.MustExec("drop table if exists t_part")
	tk.MustExec(`create table t_part (
		id bigint primary key,
		val bigint,
		index idx_val (val)
	) partition by range (id) (
		partition p0 values less than (1000),
		partition p1 values less than (2000),
		partition p2 values less than (maxvalue)
	) split between (0) and (10000) regions 5
	  split index idx_val between (0) and (10000) regions 3`)

	tbl := external.GetTableByName(t, tk, "test", "t_part")
	require.NotNil(t, tbl.Meta().TableSplitPolicy)
	require.Equal(t, int64(5), tbl.Meta().TableSplitPolicy.Regions)

	for _, idx := range tbl.Meta().Indices {
		if idx.Name.L == "idx_val" {
			require.NotNil(t, idx.RegionSplitPolicy)
			require.Equal(t, int64(3), idx.RegionSplitPolicy.Regions)
		}
	}

	tk.MustExec("admin check table t_part")

	tk.MustExec("drop table if exists t_part2")
	tk.MustExec(`create table t_part2 (
		id bigint primary key,
		val bigint
	) partition by range (id) (
		partition p0 values less than (1000),
		partition p1 values less than (2000)
	)`)

	tk.MustExec("alter table t_part2 split between (0) and (10000) regions 5")
	tbl = external.GetTableByName(t, tk, "test", "t_part2")
	require.NotNil(t, tbl.Meta().TableSplitPolicy)
	require.Equal(t, int64(5), tbl.Meta().TableSplitPolicy.Regions)

	tk.MustExec("admin check table t_part2")
}

func TestTableSplitPolicyWarning(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t_warn")
	tk.MustExec("create table t_warn (id bigint primary key, user_id bigint, status varchar(10))")
	tk.MustExec("alter table t_warn add index idx_user_id (user_id)")
	tk.MustExec("alter table t_warn split index idx_user_id between (0) and (10000) regions 5")

	tbl := external.GetTableByName(t, tk, "test", "t_warn")
	var hasPolicy bool
	for _, idx := range tbl.Meta().Indices {
		if idx.Name.L == "idx_user_id" && idx.RegionSplitPolicy != nil {
			hasPolicy = true
			break
		}
	}
	require.True(t, hasPolicy)

	time.Sleep(time.Second)

	tk.MustExec("alter table t_warn add index idx_status (status)")
	warnings := tk.Session().GetSessionVars().StmtCtx.GetWarnings()
	foundWarning := false
	for _, warn := range warnings {
		if warn.Level == "Warning" {
			foundWarning = true
			require.Contains(t, warn.Err.Error(), "region split strategy")
			require.Contains(t, warn.Err.Error(), "idx_status")
			break
		}
	}
	require.True(t, foundWarning)

	tk.MustExec("admin check table t_warn")
}

func TestTableSplitPolicyMultipleIndexes(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_scatter_region = 'table'")

	tk.MustExec("drop table if exists t_multi")
	tk.MustExec(`create table t_multi (
		id bigint primary key,
		user_id bigint,
		status varchar(10),
		created_at bigint,
		index idx_user (user_id),
		index idx_status (status),
		index idx_created (created_at)
	) split between (0) and (1000000) regions 4
	  split index idx_user between (100) and (100000) regions 3
	  split index idx_status between ('a') and ('z') regions 2`)

	tbl := external.GetTableByName(t, tk, "test", "t_multi")
	require.NotNil(t, tbl.Meta().TableSplitPolicy)
	require.Equal(t, int64(4), tbl.Meta().TableSplitPolicy.Regions)

	indexPolicies := make(map[string]*model.RegionSplitPolicy)
	for _, idx := range tbl.Meta().Indices {
		if idx.RegionSplitPolicy != nil {
			indexPolicies[idx.Name.L] = idx.RegionSplitPolicy
		}
	}

	require.NotNil(t, indexPolicies["idx_user"])
	require.Equal(t, int64(3), indexPolicies["idx_user"].Regions)
	require.NotNil(t, indexPolicies["idx_status"])
	require.Equal(t, int64(2), indexPolicies["idx_status"].Regions)
	require.Nil(t, indexPolicies["idx_created"])

	tk.MustExec("alter table t_multi split index idx_created between (0) and (1000000000) regions 5")
	tbl = external.GetTableByName(t, tk, "test", "t_multi")
	for _, idx := range tbl.Meta().Indices {
		if idx.Name.L == "idx_created" {
			require.NotNil(t, idx.RegionSplitPolicy)
			require.Equal(t, int64(5), idx.RegionSplitPolicy.Regions)
		}
	}

	tk.MustExec("admin check table t_multi")
}
