// Copyright 2024 PingCAP, Inc.
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

package initstats

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/statistics/handle"
	"github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/stretchr/testify/require"
)

func maxPhysicalTableID(h *handle.Handle, is infoschema.InfoSchema) int64 {
	var maxID int64
	for _, statsTbl := range h.StatsCache.Values() {
		table, ok := h.TableInfoByID(is, statsTbl.PhysicalID)
		if !ok {
			continue
		}
		dbInfo, ok := is.SchemaByID(table.Meta().DBID)
		if !ok {
			continue
		}
		if filter.IsSystemSchema(dbInfo.Name.L) {
			continue
		}
		maxID = max(maxID, statsTbl.PhysicalID)
	}
	return maxID
}

func TestLiteInitStatsWithTableIDs(t *testing.T) {
	store, dom := session.CreateStoreAndBootstrap(t)
	defer store.Close()
	se := session.CreateSessionAndSetID(t, store)
	session.MustExec(t, se, "use test")
	session.MustExec(t, se, "create table t1( id int, a int, b int, index idx(id, a));")
	session.MustExec(t, se, "create table t2( id int, a int, b int, index idx(id, a));")
	session.MustExec(t, se, "create table t3( id int, a int, b int, index idx(id, a));")
	session.MustExec(t, se, "insert into t1 values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, 5);")
	session.MustExec(t, se, "insert into t2 values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, 5);")
	session.MustExec(t, se, "insert into t3 values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, 5);")
	session.MustExec(t, se, "analyze table t1, t2, t3 all columns;")
	is := dom.InfoSchema()
	tbl1, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	tbl2, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t2"))
	require.NoError(t, err)
	tbl3, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t3"))
	require.NoError(t, err)

	dom.Close()

	vardef.SetStatsLease(-1)
	dom, err = session.BootstrapSession(store)
	require.NoError(t, err)
	h := dom.StatsHandle()
	_, ok := h.Get(tbl1.Meta().ID)
	require.False(t, ok)
	require.NoError(t, h.InitStatsLite(context.Background(), tbl1.Meta().ID))
	_, ok = h.Get(tbl1.Meta().ID)
	require.True(t, ok)
	_, ok = h.Get(tbl2.Meta().ID)
	require.False(t, ok)
	_, ok = h.Get(tbl3.Meta().ID)
	require.False(t, ok)

	// Make sure it can be loaded multiple times.
	require.NoError(t, h.InitStatsLite(context.Background(), tbl1.Meta().ID, tbl2.Meta().ID))
	_, ok = h.Get(tbl1.Meta().ID)
	require.True(t, ok)
	_, ok = h.Get(tbl2.Meta().ID)
	require.True(t, ok)
	_, ok = h.Get(tbl3.Meta().ID)
	require.False(t, ok)

	require.NoError(t, h.InitStatsLite(context.Background()))
	_, ok = h.Get(tbl1.Meta().ID)
	require.True(t, ok)
	_, ok = h.Get(tbl2.Meta().ID)
	require.True(t, ok)
	_, ok = h.Get(tbl3.Meta().ID)
	require.True(t, ok)

	dom.Close()
}

func TestNonLiteInitStatsWithTableIDs(t *testing.T) {
	store, dom := session.CreateStoreAndBootstrap(t)
	defer store.Close()
	se := session.CreateSessionAndSetID(t, store)
	session.MustExec(t, se, "use test")
	session.MustExec(t, se, "create table t1( id int, a int, b int, index idx(id, a));")
	session.MustExec(t, se, "create table t2( id int, a int, b int, index idx(id, a));")
	session.MustExec(t, se, "create table t3( id int, a int, b int, index idx(id, a));")
	session.MustExec(t, se, "insert into t1 values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, 5);")
	session.MustExec(t, se, "insert into t2 values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, 5);")
	session.MustExec(t, se, "insert into t3 values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, 5);")
	session.MustExec(t, se, "analyze table t1, t2, t3 all columns with 1 topn, 10 buckets;")
	is := dom.InfoSchema()
	tbl1, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	tbl2, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t2"))
	require.NoError(t, err)
	tbl3, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t3"))
	require.NoError(t, err)

	dom.Close()

	vardef.SetStatsLease(-1)
	dom, err = session.BootstrapSession(store)
	require.NoError(t, err)
	is = dom.InfoSchema()
	h := dom.StatsHandle()
	_, ok := h.Get(tbl1.Meta().ID)
	require.False(t, ok)
	require.NoError(t, h.InitStats(context.Background(), is, tbl1.Meta().ID))
	stats1, ok := h.Get(tbl1.Meta().ID)
	require.True(t, ok)
	require.True(t, stats1.GetIdx(1).IsFullLoad())
	_, ok = h.Get(tbl2.Meta().ID)
	require.False(t, ok)
	_, ok = h.Get(tbl3.Meta().ID)
	require.False(t, ok)

	// Make sure it can be loaded multiple times.
	require.NoError(t, h.InitStats(context.Background(), is, tbl1.Meta().ID, tbl2.Meta().ID))
	stats1, ok = h.Get(tbl1.Meta().ID)
	require.True(t, ok)
	require.True(t, stats1.GetIdx(1).IsFullLoad())
	stats2, ok := h.Get(tbl2.Meta().ID)
	require.True(t, ok)
	require.True(t, stats2.GetIdx(1).IsFullLoad())
	_, ok = h.Get(tbl3.Meta().ID)
	require.False(t, ok)

	require.NoError(t, h.InitStats(context.Background(), is))
	stats1, ok = h.Get(tbl1.Meta().ID)
	require.True(t, ok)
	require.True(t, stats1.GetIdx(1).IsFullLoad())
	stats2, ok = h.Get(tbl2.Meta().ID)
	require.True(t, ok)
	require.True(t, stats2.GetIdx(1).IsFullLoad())
	stats3, ok := h.Get(tbl3.Meta().ID)
	require.True(t, ok)
	require.True(t, stats3.GetIdx(1).IsFullLoad())

	dom.Close()
}

func TestConcurrentlyInitStatsWithMemoryLimit(t *testing.T) {
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Performance.LiteInitStats = false
	})
	handle.IsFullCacheFunc = func(cache types.StatsCache, total uint64) bool {
		return true
	}
	testConcurrentlyInitStats(t)
}

func TestConcurrentlyInitStatsWithoutMemoryLimit(t *testing.T) {
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Performance.LiteInitStats = false
	})
	handle.IsFullCacheFunc = func(cache types.StatsCache, total uint64) bool {
		return false
	}
	testConcurrentlyInitStats(t)
}

func testConcurrentlyInitStats(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_analyze_column_options='ALL'")
	tk.MustExec("create table t1 (a int, b int, c int, primary key(c))")
	tk.MustExec("insert into t1 values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,7,8)")
	tk.MustExec("analyze table t1")
	for i := 2; i < 10; i++ {
		tk.MustExec(fmt.Sprintf("create table t%v (a int, b int, c int, primary key(c))", i))
		tk.MustExec(fmt.Sprintf("insert into t%v select * from t1", i))
		tk.MustExec(fmt.Sprintf("analyze table t%v all columns", i))
	}
	h := dom.StatsHandle()
	is := dom.InfoSchema()
	h.Clear()
	require.Equal(t, h.MemConsumed(), int64(0))
	require.NoError(t, h.InitStats(context.Background(), is))
	for i := 1; i < 10; i++ {
		tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr(fmt.Sprintf("t%v", i)))
		require.NoError(t, err)
		stats, ok := h.StatsCache.Get(tbl.Meta().ID)
		require.True(t, ok)
		for _, col := range stats.GetColSlice() {
			require.True(t, col.IsAllEvicted())
			require.False(t, col.IsFullLoad())
		}
	}
	for i := 1; i < 10; i++ {
		tk.MustQuery(fmt.Sprintf("explain select * from t%v where a = 1", i)).CheckNotContain("pseudo")
	}
	for i := 1; i < 10; i++ {
		tk.MustQuery(fmt.Sprintf("explain select * from t%v where b = 1", i)).CheckNotContain("pseudo")
	}
	for i := 1; i < 10; i++ {
		tk.MustQuery(fmt.Sprintf("explain select * from t%v where c >= 1", i)).CheckNotContain("pseudo")
	}
	for i := 1; i < 10; i++ {
		tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr(fmt.Sprintf("t%v", i)))
		require.NoError(t, err)
		stats, ok := h.StatsCache.Get(tbl.Meta().ID)
		require.True(t, ok)
		for _, col := range stats.GetColSlice() {
			require.True(t, col.IsFullLoad())
			require.False(t, col.IsAllEvicted())
		}
	}
	maxID := maxPhysicalTableID(h, is)
	if kerneltype.IsClassic() {
		require.Equal(t, int64(132), maxID)
	} else {
		// In next-gen, the table ID is different from classic because the system table IDs and the regular table IDs are different,
		// so the next-gen table ID will be ahead of the classic table ID.
		require.Equal(t, int64(23), maxID)
	}
}

func TestDropTableBeforeConcurrentlyInitStats(t *testing.T) {
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Performance.LiteInitStats = false
	})
	testDropTableBeforeInitStats(t)
}

func TestDropTableBeforeNonLiteInitStats(t *testing.T) {
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Performance.LiteInitStats = false
	})
	testDropTableBeforeInitStats(t)
}

func testDropTableBeforeInitStats(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("create table t( id int, a int, b int, index idx(id, a));")
	tk.MustExec("insert into t values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, 5);")
	tk.MustExec("insert into t select * from t where id<>2;")
	tk.MustExec("insert into t select * from t where id<>2;")
	tk.MustExec("insert into t select * from t where id<>2;")
	tk.MustExec("insert into t select * from t where id<>2;")
	tk.MustExec("analyze table t all columns;")
	tk.MustExec("drop table t")
	h := dom.StatsHandle()
	is := dom.InfoSchema()
	require.NoError(t, h.InitStats(context.Background(), is))
}

func TestSkipStatsInitWithSkipInitStats(t *testing.T) {
	config.GetGlobalConfig().Performance.SkipInitStats = true
	defer func() {
		config.GetGlobalConfig().Performance.SkipInitStats = false
	}()

	store, dom := session.CreateStoreAndBootstrap(t)
	defer store.Close()
	se := session.CreateSessionAndSetID(t, store)
	session.MustExec(t, se, "use test")
	session.MustExec(t, se, "create table t( id int, a int, b int, index idx(id, a));")
	session.MustExec(t, se, "insert into t values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, 5);")
	session.MustExec(t, se, "analyze table t all columns;")
	dom.Close()

	vardef.SetStatsLease(3)
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)
	h := dom.StatsHandle()
	<-h.InitStatsDone
	is := dom.InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	_, ok := h.StatsCache.Get(tbl.Meta().ID)
	require.False(t, ok)
	dom.Close()
}

func TestNonLiteInitStatsAndCheckTheLastTableStats(t *testing.T) {
	store, dom := session.CreateStoreAndBootstrap(t)
	defer store.Close()
	se := session.CreateSessionAndSetID(t, store)
	session.MustExec(t, se, "use test")
	session.MustExec(t, se, "create table t1( id int, a int, b int, index idx(id, a));")
	session.MustExec(t, se, "create table t2( id int, a int, b int, index idx(id, a));")
	session.MustExec(t, se, "create table t3( id int, a int, b int, index idx(id, a));")
	session.MustExec(t, se, "insert into t1 values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, 5);")
	session.MustExec(t, se, "insert into t2 values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, 5);")
	session.MustExec(t, se, "insert into t3 values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, 5);")
	session.MustExec(t, se, "analyze table t1, t2, t3 all columns with 1 topn, 10 buckets;")
	is := dom.InfoSchema()
	tbl1, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	tbl2, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t2"))
	require.NoError(t, err)
	tbl3, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t3"))
	require.NoError(t, err)

	dom.Close()

	vardef.SetStatsLease(-1)
	dom, err = session.BootstrapSession(store)
	require.NoError(t, err)
	is = dom.InfoSchema()
	h := dom.StatsHandle()
	_, ok := h.Get(tbl1.Meta().ID)
	require.False(t, ok)
	require.NoError(t, h.InitStats(context.Background(), is))
	stats1, ok := h.Get(tbl1.Meta().ID)
	require.True(t, ok)
	require.True(t, stats1.GetIdx(1).IsFullLoad())
	stats2, ok := h.Get(tbl2.Meta().ID)
	require.True(t, ok)
	require.True(t, stats2.GetIdx(1).IsFullLoad())
	stats3, ok := h.Get(tbl3.Meta().ID)
	require.True(t, ok)
	require.True(t, stats3.GetIdx(1).IsFullLoad())

	dom.Close()
}
