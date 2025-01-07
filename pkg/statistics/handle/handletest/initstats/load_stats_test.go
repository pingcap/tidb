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
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/statistics/handle"
	"github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestConcurrentlyInitStatsWithMemoryLimit(t *testing.T) {
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Performance.LiteInitStats = false
		conf.Performance.ConcurrentlyInitStats = true
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
		conf.Performance.ConcurrentlyInitStats = true
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
	require.Equal(t, int64(126), handle.GetMaxTidRecordForTest())
}

func TestDropTableBeforeConcurrentlyInitStats(t *testing.T) {
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Performance.LiteInitStats = false
		conf.Performance.ConcurrentlyInitStats = true
	})
	testDropTableBeforeInitStats(t)
}

func TestDropTableBeforeNonLiteInitStats(t *testing.T) {
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Performance.LiteInitStats = false
		conf.Performance.ConcurrentlyInitStats = false
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
