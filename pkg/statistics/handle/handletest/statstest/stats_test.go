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

package statstest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/statistics"
	statstestutil "github.com/pingcap/tidb/pkg/statistics/handle/ddl/testutil"
	"github.com/pingcap/tidb/pkg/statistics/handle/internal"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/analyzehelper"
	"github.com/stretchr/testify/require"
)

// Helper functions for checking stats properties

// checkAnalyzedTableBasicMeta checks the basic metadata for an analyzed table
func checkAnalyzedTableBasicMeta(t *testing.T, tableStats *statistics.Table, expectedRealtimeCount int64) {
	require.True(t, tableStats.IsAnalyzed(), "table should be marked as analyzed")
	require.Equal(t, int64(0), tableStats.ModifyCount, "modify count should be 0 for freshly analyzed table")
	require.Equal(t, expectedRealtimeCount, tableStats.RealtimeCount, "realtime count should match expected count")
	require.Equal(t, statistics.Version2, tableStats.StatsVer, "stats version should be Version2 for analyzed table")
}

// checkAnalyzedIndexStats checks all index stats for an analyzed table
func checkAnalyzedIndexStats(t *testing.T, tableStats *statistics.Table, tableInfo *model.TableInfo, expectedTopNCount uint64, expectedTotalRowCount float64, expectedHistLen int) {
	require.Equal(t, len(tableInfo.Indices), tableStats.IdxNum(), "index count should match table info")
	tableStats.ForEachIndexImmutable(func(_ int64, idx *statistics.Index) bool {
		require.True(t, tableStats.ColAndIdxExistenceMap.Has(idx.ID, true), "analyzed index %d should exist in existence map", idx.ID)
		require.True(t, tableStats.ColAndIdxExistenceMap.HasAnalyzed(idx.ID, true), "analyzed index %d should be marked as analyzed", idx.ID)
		require.True(t, idx.IsStatsInitialized(), "analyzed index %d stats should be initialized", idx.ID)
		require.True(t, idx.IsFullLoad(), "analyzed index %d should be fully loaded", idx.ID)
		require.Equal(t, expectedTopNCount, idx.TopN.TotalCount(), "analyzed index %d TopN count should match expected", idx.ID)
		require.Equal(t, expectedTotalRowCount, idx.TotalRowCount(), "analyzed index %d total row count should match expected", idx.ID)
		require.Equal(t, expectedHistLen, idx.Histogram.Len(), "analyzed index %d histogram length should match expected", idx.ID)
		return false
	})
}

// checkAnalyzedColumnStatsAllEvicted checks column stats for analyzed tables where columns are evicted
func checkAnalyzedColumnStatsAllEvicted(t *testing.T, tableStats *statistics.Table, expectedNDV int64) {
	tableStats.ForEachColumnImmutable(func(_ int64, col *statistics.Column) bool {
		require.True(t, tableStats.ColAndIdxExistenceMap.Has(col.ID, false), "evicted column %s should exist in existence map", col.Info.Name.L)
		require.True(t, tableStats.ColAndIdxExistenceMap.HasAnalyzed(col.ID, false), "evicted column %s should be marked as analyzed", col.Info.Name.L)
		require.True(t, col.IsStatsInitialized(), "evicted column %s stats should be initialized", col.Info.Name.L)
		require.True(t, col.IsAllEvicted(), "column %s should be marked as all evicted", col.Info.Name.L)
		require.Equal(t, expectedNDV, col.NDV, "evicted column %s NDV should match expected", col.Info.Name.L)
		require.Equal(t, int64(0), col.NullCount, "evicted column %s null count should be 0", col.Info.Name.L)

		require.Nil(t, col.TopN, "evicted column %s TopN should be nil (evicted)", col.Info.Name.L)
		require.Equal(t, uint64(0), col.TopN.TotalCount(), "evicted column %s TopN total count should be 0 (evicted)", col.Info.Name.L)
		require.Equal(t, float64(0), col.TotalRowCount(), "evicted column %s total row count should be 0 (evicted)", col.Info.Name.L)
		require.Equal(t, 0, col.Histogram.Len(), "evicted column %s histogram length should be 0 (evicted)", col.Info.Name.L)
		return false
	})
}

// checkNonAnalyzedTableBasicMeta checks the basic metadata for a non-analyzed table
func checkNonAnalyzedTableBasicMeta(t *testing.T, tableStats *statistics.Table, expectedCount int64) {
	require.False(t, tableStats.Pseudo, "table stats should not be pseudo for non-analyzed table")
	require.False(t, tableStats.IsAnalyzed(), "table should not be marked as analyzed")
	require.Equal(t, expectedCount, tableStats.ModifyCount, "modify count should match expected count")
	require.Equal(t, expectedCount, tableStats.RealtimeCount, "realtime count should match expected count")
	require.Equal(t, statistics.Version0, tableStats.StatsVer, "stats version should be Version0 for non-analyzed table")
}

// checkNonAnalyzedIndexStats checks all index stats for a non-analyzed table
func checkNonAnalyzedIndexStats(t *testing.T, tableStats *statistics.Table, tableInfo *model.TableInfo) {
	require.Equal(t, len(tableInfo.Indices), tableStats.IdxNum(), "index count should match table info")
	tableStats.ForEachIndexImmutable(func(_ int64, idx *statistics.Index) bool {
		require.True(t, tableStats.ColAndIdxExistenceMap.Has(idx.ID, true), "index %d should exist in existence map", idx.ID)
		require.False(t, tableStats.ColAndIdxExistenceMap.HasAnalyzed(idx.ID, true), "index %d should not be marked as analyzed", idx.ID)
		require.False(t, idx.IsStatsInitialized(), "index %d stats should not be initialized", idx.ID)
		require.False(t, idx.IsAllEvicted(), "index %d should not be marked as evicted", idx.ID)
		require.False(t, idx.IsFullLoad(), "index %d should not be fully loaded", idx.ID)
		require.Equal(t, int64(0), idx.NDV, "index %d NDV should be 0", idx.ID)
		require.Equal(t, int64(0), idx.NullCount, "index %d null count should be 0", idx.ID)
		require.Nil(t, idx.TopN, "index %d TopN should be nil", idx.ID)
		require.Equal(t, uint64(0), idx.TopN.TotalCount(), "index %d TopN total count should be 0", idx.ID)
		require.Equal(t, float64(0), idx.TotalRowCount(), "index %d total row count should be 0", idx.ID)
		require.Equal(t, 0, idx.Histogram.Len(), "index %d histogram length should be 0", idx.ID)
		return false
	})
}

// checkNonAnalyzedColumnStats checks column stats for a non-analyzed table
func checkNonAnalyzedColumnStats(t *testing.T, tableStats *statistics.Table, tableInfo *model.TableInfo) {
	require.Equal(t, len(tableInfo.Columns), tableStats.ColNum(), "column count should match table info")
	tableStats.ForEachColumnImmutable(func(_ int64, col *statistics.Column) bool {
		require.True(t, tableStats.ColAndIdxExistenceMap.Has(col.ID, false), "column %s should exist in existence map", col.Info.Name.L)
		require.False(t, tableStats.ColAndIdxExistenceMap.HasAnalyzed(col.ID, false), "column %s should not be marked as analyzed", col.Info.Name.L)
		require.False(t, col.IsStatsInitialized(), "column %s stats should not be initialized", col.Info.Name.L)
		require.False(t, col.IsAllEvicted(), "column %s should not be marked as evicted", col.Info.Name.L)
		require.False(t, col.IsFullLoad(), "column %s should not be fully loaded", col.Info.Name.L)
		require.Equal(t, int64(0), col.NDV, "column %s NDV should be 0", col.Info.Name.L)
		require.Equal(t, int64(0), col.NullCount, "column %s null count should be 0", col.Info.Name.L)
		require.Nil(t, col.TopN, "column %s TopN should be nil", col.Info.Name.L)
		require.Equal(t, uint64(0), col.TopN.TotalCount(), "column %s TopN total count should be 0", col.Info.Name.L)
		require.Equal(t, float64(0), col.TotalRowCount(), "column %s total row count should be 0", col.Info.Name.L)
		require.Equal(t, 0, col.Histogram.Len(), "column %s histogram length should be 0", col.Info.Name.L)
		return false
	})
}

// checkPredicateColumnStats checks column stats for tables analyzed with predicate columns
// nonPredicateCols specifies which columns should NOT be analyzed, while others should be analyzed but evicted
func checkPredicateColumnStats(t *testing.T, tableStats *statistics.Table, tableInfo *model.TableInfo, nonPredicateCols []string, expectedNDV int64) {
	nonPredicateMap := make(map[string]bool, len(nonPredicateCols))
	for _, col := range nonPredicateCols {
		nonPredicateMap[col] = true
	}

	require.Equal(t, len(tableInfo.Columns), tableStats.ColNum(), "column count should match table info")
	tableStats.ForEachColumnImmutable(func(_ int64, col *statistics.Column) bool {
		// Check if this column is in the non-predicate columns list
		isNonPredicate := nonPredicateMap[col.Info.Name.L]

		if isNonPredicate {
			// Non-predicate column: should not be analyzed
			require.True(t, tableStats.ColAndIdxExistenceMap.Has(col.ID, false), "non-predicate column %s should exist in existence map", col.Info.Name.L)
			require.False(t, tableStats.ColAndIdxExistenceMap.HasAnalyzed(col.ID, false), "non-predicate column %s should not be marked as analyzed", col.Info.Name.L)
			require.False(t, col.IsStatsInitialized(), "non-predicate column %s stats should not be initialized", col.Info.Name.L)
			require.False(t, col.IsAllEvicted(), "non-predicate column %s should not be marked as evicted", col.Info.Name.L)
			require.False(t, col.IsFullLoad(), "non-predicate column %s should not be fully loaded", col.Info.Name.L)
			require.Equal(t, int64(0), col.NDV, "non-predicate column %s NDV should be 0", col.Info.Name.L)
			require.Equal(t, int64(0), col.NullCount, "non-predicate column %s null count should be 0", col.Info.Name.L)
			require.Nil(t, col.TopN, "non-predicate column %s TopN should be nil", col.Info.Name.L)
			require.Equal(t, uint64(0), col.TopN.TotalCount(), "non-predicate column %s TopN total count should be 0", col.Info.Name.L)
			require.Equal(t, float64(0), col.TotalRowCount(), "non-predicate column %s total row count should be 0", col.Info.Name.L)
			require.Equal(t, 0, col.Histogram.Len(), "non-predicate column %s histogram length should be 0", col.Info.Name.L)
			return false
		}

		// Predicate column: should be analyzed but evicted
		require.True(t, tableStats.ColAndIdxExistenceMap.Has(col.ID, false), "predicate column %s should exist in existence map", col.Info.Name.L)
		require.True(t, tableStats.ColAndIdxExistenceMap.HasAnalyzed(col.ID, false), "predicate column %s should be marked as analyzed", col.Info.Name.L)
		require.True(t, col.IsStatsInitialized(), "predicate column %s stats should be initialized", col.Info.Name.L)
		require.True(t, col.IsAllEvicted(), "predicate column %s should be marked as evicted", col.Info.Name.L)
		require.False(t, col.IsFullLoad(), "predicate column %s should not be fully loaded", col.Info.Name.L)
		require.Equal(t, expectedNDV, col.NDV, "predicate column %s NDV should match expected", col.Info.Name.L)
		require.Equal(t, int64(0), col.NullCount, "predicate column %s null count should be 0", col.Info.Name.L)
		require.Nil(t, col.TopN, "predicate column %s TopN should be nil (evicted)", col.Info.Name.L)
		require.Equal(t, uint64(0), col.TopN.TotalCount(), "predicate column %s TopN total count should be 0 (evicted)", col.Info.Name.L)
		require.Equal(t, float64(0), col.TotalRowCount(), "predicate column %s total row count should be 0 (evicted)", col.Info.Name.L)
		require.Equal(t, 0, col.Histogram.Len(), "predicate column %s histogram length should be 0 (evicted)", col.Info.Name.L)
		return false
	})
}

func TestStatsCacheProcess(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 int)")
	testKit.MustExec("insert into t values(1, 2)")
	analyzehelper.TriggerPredicateColumnsCollection(t, testKit, store, "t", "c1", "c2")
	do := dom
	is := do.InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	statsTbl := do.StatsHandle().GetPhysicalTableStats(tableInfo.ID, tableInfo)
	require.True(t, statsTbl.Pseudo)
	require.Zero(t, statsTbl.Version)
	currentVersion := do.StatsHandle().MaxTableStatsVersion()
	testKit.MustExec("analyze table t")
	statsTbl = do.StatsHandle().GetPhysicalTableStats(tableInfo.ID, tableInfo)
	require.False(t, statsTbl.Pseudo)
	require.NotZero(t, statsTbl.Version)
	require.Equal(t, currentVersion, do.StatsHandle().MaxTableStatsVersion())
	newVersion := do.StatsHandle().GetNextCheckVersionWithOffset()
	require.Equal(t, currentVersion, newVersion, "analyze should not move forward the stats cache version")

	// Insert more rows
	testKit.MustExec("insert into t values(2, 3)")
	require.NoError(t, do.StatsHandle().DumpStatsDeltaToKV(true))
	require.NoError(t, do.StatsHandle().Update(context.Background(), is))
	newVersion = do.StatsHandle().MaxTableStatsVersion()
	require.NotEqual(t, currentVersion, newVersion, "update with no table should move forward the stats cache version")
}

func TestStatsCache(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 int)")
	testKit.MustExec("insert into t values(1, 2)")
	analyzehelper.TriggerPredicateColumnsCollection(t, testKit, store, "t", "c1", "c2")
	do := dom
	is := do.InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	statsTbl := do.StatsHandle().GetPhysicalTableStats(tableInfo.ID, tableInfo)
	require.True(t, statsTbl.Pseudo)
	testKit.MustExec("analyze table t")
	statsTbl = do.StatsHandle().GetPhysicalTableStats(tableInfo.ID, tableInfo)
	require.False(t, statsTbl.Pseudo)
	testKit.MustExec("create index idx_t on t(c1)")
	do.InfoSchema()
	statsTbl = do.StatsHandle().GetPhysicalTableStats(tableInfo.ID, tableInfo)
	// If index is build, but stats is not updated. statsTbl can also work.
	require.False(t, statsTbl.Pseudo)
	// But the added index will not work.
	require.Nil(t, statsTbl.GetIdx(int64(1)))

	testKit.MustExec("analyze table t")
	statsTbl = do.StatsHandle().GetPhysicalTableStats(tableInfo.ID, tableInfo)
	require.False(t, statsTbl.Pseudo)
	// If the new schema drop a column, the table stats can still work.
	testKit.MustExec("alter table t drop column c2")
	is = do.InfoSchema()
	do.StatsHandle().Clear()
	err = do.StatsHandle().Update(context.Background(), is)
	require.NoError(t, err)
	statsTbl = do.StatsHandle().GetPhysicalTableStats(tableInfo.ID, tableInfo)
	require.False(t, statsTbl.Pseudo)

	// If the new schema add a column, the table stats can still work.
	testKit.MustExec("alter table t add column c10 int")
	is = do.InfoSchema()

	do.StatsHandle().Clear()
	err = do.StatsHandle().Update(context.Background(), is)
	require.NoError(t, err)
	statsTbl = do.StatsHandle().GetPhysicalTableStats(tableInfo.ID, tableInfo)
	require.False(t, statsTbl.Pseudo)
}

func TestStatsCacheMemTracker(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 int, c3 int)")
	analyzehelper.TriggerPredicateColumnsCollection(t, testKit, store, "t", "c1", "c2", "c3")
	testKit.MustExec("insert into t values(1, 2, 3)")
	do := dom
	is := do.InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	statsTbl := do.StatsHandle().GetPhysicalTableStats(tableInfo.ID, tableInfo)
	require.True(t, statsTbl.MemoryUsage().TotalMemUsage == 0)
	require.True(t, statsTbl.Pseudo)

	testKit.MustExec("analyze table t")
	statsTbl = do.StatsHandle().GetPhysicalTableStats(tableInfo.ID, tableInfo)

	require.False(t, statsTbl.Pseudo)
	testKit.MustExec("create index idx_t on t(c1)")
	do.InfoSchema()
	statsTbl = do.StatsHandle().GetPhysicalTableStats(tableInfo.ID, tableInfo)

	// If index is build, but stats is not updated. statsTbl can also work.
	require.False(t, statsTbl.Pseudo)
	// But the added index will not work.
	require.Nil(t, statsTbl.GetIdx(int64(1)))

	testKit.MustExec("analyze table t")
	statsTbl = do.StatsHandle().GetPhysicalTableStats(tableInfo.ID, tableInfo)

	require.False(t, statsTbl.Pseudo)

	// If the new schema drop a column, the table stats can still work.
	testKit.MustExec("alter table t drop column c2")
	is = do.InfoSchema()
	do.StatsHandle().Clear()
	err = do.StatsHandle().Update(context.Background(), is)
	require.NoError(t, err)

	statsTbl = do.StatsHandle().GetPhysicalTableStats(tableInfo.ID, tableInfo)
	require.True(t, statsTbl.MemoryUsage().TotalMemUsage > 0)
	require.False(t, statsTbl.Pseudo)

	// If the new schema add a column, the table stats can still work.
	testKit.MustExec("alter table t add column c10 int")
	is = do.InfoSchema()

	do.StatsHandle().Clear()
	err = do.StatsHandle().Update(context.Background(), is)
	require.NoError(t, err)
	statsTbl = do.StatsHandle().GetPhysicalTableStats(tableInfo.ID, tableInfo)
	require.False(t, statsTbl.Pseudo)
}

func TestStatsStoreAndLoad(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 int)")
	recordCount := 1000
	for i := range recordCount {
		testKit.MustExec("insert into t values (?, ?)", i, i+1)
	}
	testKit.MustExec("create index idx_t on t(c2)")
	analyzehelper.TriggerPredicateColumnsCollection(t, testKit, store, "t", "c1")
	do := dom
	is := do.InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()

	testKit.MustExec("analyze table t")
	statsTbl1 := do.StatsHandle().GetPhysicalTableStats(tableInfo.ID, tableInfo)

	do.StatsHandle().Clear()
	err = do.StatsHandle().Update(context.Background(), is)
	require.NoError(t, err)
	statsTbl2 := do.StatsHandle().GetPhysicalTableStats(tableInfo.ID, tableInfo)
	require.False(t, statsTbl2.Pseudo)
	require.Equal(t, int64(recordCount), statsTbl2.RealtimeCount)
	internal.AssertTableEqual(t, statsTbl1, statsTbl2)
}

func testInitStatsMemTrace(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int, b int, c int, primary key(a), key idx(b))")
	tk.MustExec("insert into t1 values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,7,8)")
	tk.MustExec("analyze table t1")
	for i := 2; i < 10; i++ {
		tk.MustExec(fmt.Sprintf("create table t%v (a int, b int, c int, primary key(a), key idx(b))", i))
		tk.MustExec(fmt.Sprintf("insert into t%v select * from t1", i))
		tk.MustExec(fmt.Sprintf("analyze table t%v", i))
	}
	h := dom.StatsHandle()
	is := dom.InfoSchema()
	h.Clear()
	require.Equal(t, h.MemConsumed(), int64(0))
	require.NoError(t, h.InitStats(context.Background(), is))

	var memCostTot int64
	for i := 1; i < 10; i++ {
		tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr(fmt.Sprintf("t%v", i)))
		require.NoError(t, err)
		tStats := h.GetPhysicalTableStats(tbl.Meta().ID, tbl.Meta())
		memCostTot += tStats.MemoryUsage().TotalMemUsage
	}
	tables := h.StatsCache.Values()
	for _, tt := range tables {
		tbl, ok := h.StatsCache.Get(tt.PhysicalID)
		require.True(t, ok)
		require.Equal(t, tbl.PhysicalID, tt.PhysicalID)
	}

	require.Equal(t, h.MemConsumed(), memCostTot)
}

func TestInitStatsMemTraceWithLite(t *testing.T) {
	restore := config.RestoreFunc()
	defer restore()
	testInitStatsMemTraceFunc(t, true)
}

func TestInitStatsMemTraceWithoutLite(t *testing.T) {
	restore := config.RestoreFunc()
	defer restore()
	testInitStatsMemTraceFunc(t, false)
}

func TestInitStatsMemTraceWithConcurrentLite(t *testing.T) {
	restore := config.RestoreFunc()
	defer restore()
	testInitStatsMemTraceFunc(t, true)
}

func TestInitStatsMemTraceWithoutConcurrentLite(t *testing.T) {
	restore := config.RestoreFunc()
	defer restore()
	testInitStatsMemTraceFunc(t, false)
}

func testInitStatsMemTraceFunc(t *testing.T, liteInitStats bool) {
	originValue := config.GetGlobalConfig().Performance.LiteInitStats
	defer func() {
		config.GetGlobalConfig().Performance.LiteInitStats = originValue
	}()
	config.GetGlobalConfig().Performance.LiteInitStats = liteInitStats
	testInitStatsMemTrace(t)
}

func TestInitStatsWithAnalyzeVersion1(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("analyze V1 cannot support in the next gen")
	}
	originValue := config.GetGlobalConfig().Performance.LiteInitStats
	defer func() {
		config.GetGlobalConfig().Performance.LiteInitStats = originValue
	}()
	config.GetGlobalConfig().Performance.LiteInitStats = false
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("set @@session.tidb_analyze_version = 1")
	testKit.MustExec("create table t(a int, b int, c int, primary key(a), key idx(b))")
	testKit.MustExec("insert into t values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,7,8)")
	testKit.MustExec("analyze table t")
	h := dom.StatsHandle()
	is := dom.InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	// `Update` will not use load by need strategy when `Lease` is 0, and `InitStats` is only called when
	// `Lease` is not 0, so here we just change it.
	h.SetLease(time.Millisecond)

	h.Clear()
	require.NoError(t, h.InitStats(context.Background(), is))
	table0 := h.GetPhysicalTableStats(tbl.Meta().ID, tbl.Meta())
	h.Clear()
	require.NoError(t, h.Update(context.Background(), is))
	// Index and pk are loaded.
	needed := fmt.Sprintf(`Table:%v RealtimeCount:6
column:1 ndv:6 totColSize:0
column:2 ndv:6 totColSize:6
column:3 ndv:6 totColSize:6
index:1 ndv:6
num: 1 lower_bound: 1 upper_bound: 1 repeats: 1 ndv: 0
num: 1 lower_bound: 2 upper_bound: 2 repeats: 1 ndv: 0
num: 1 lower_bound: 3 upper_bound: 3 repeats: 1 ndv: 0
num: 1 lower_bound: 4 upper_bound: 4 repeats: 1 ndv: 0
num: 1 lower_bound: 5 upper_bound: 5 repeats: 1 ndv: 0
num: 1 lower_bound: 7 upper_bound: 7 repeats: 1 ndv: 0`, tbl.Meta().ID)
	require.Equal(t, needed, table0.String())
	h.SetLease(0)
}

func TestInitStats(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, c int, primary key(a), key idx(b))")
	h := dom.StatsHandle()
	is := dom.InfoSchema()
	err := statstestutil.HandleNextDDLEventWithTxn(h)
	require.NoError(t, err)
	tk.MustExec("insert into t values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,7,8)")
	// With all columns.
	tk.MustExec("analyze table t all columns with 2 topn, 2 buckets")

	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)

	require.NoError(t, h.InitStats(context.Background(), is))
	tableStats := h.GetPhysicalTableStats(tbl.Meta().ID, tbl.Meta())
	// Basic meta info check
	checkAnalyzedTableBasicMeta(t, tableStats, 6)
	// Check index stats (TopN + Histogram)
	checkAnalyzedIndexStats(t, tableStats, tbl.Meta(), 2, 6, 2)
	// Check column stats (Only Basic Info, no TopN and Histogram)
	checkAnalyzedColumnStatsAllEvicted(t, tableStats, 6)

	// Another table with no analyze
	tk.MustExec("create table t1(a int, b int, c int, primary key(a), key idx(b))")
	tk.MustExec("insert into t1 values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,7,8)")
	h = dom.StatsHandle()
	is = dom.InfoSchema()
	// Handle DDL event to init the stats meta and histogram meta.
	err = statstestutil.HandleNextDDLEventWithTxn(h)
	require.NoError(t, err)
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(context.Background(), is))
	tbl1, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.NoError(t, err)

	require.NoError(t, h.InitStats(context.Background(), is))
	tableStats1 := h.GetPhysicalTableStats(tbl1.Meta().ID, tbl1.Meta())
	checkNonAnalyzedTableBasicMeta(t, tableStats1, 6)

	// Check index stats
	checkNonAnalyzedIndexStats(t, tableStats1, tbl1.Meta())
	// Check column stats
	checkNonAnalyzedColumnStats(t, tableStats1, tbl1.Meta())

	// Another table with predicate columns
	tk.MustExec("create table t2(a int, b int, c int, primary key(a), key idx(b))")
	tk.MustExec("insert into t2 values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,7,8)")
	h = dom.StatsHandle()
	is = dom.InfoSchema()
	// Handle DDL event to init the stats meta and histogram meta.
	err = statstestutil.HandleNextDDLEventWithTxn(h)
	require.NoError(t, err)
	tk.MustExec("analyze table t2 predicate columns with 2 topn, 2 buckets")
	tbl2, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t2"))
	require.NoError(t, err)

	require.NoError(t, h.InitStats(context.Background(), is))
	tableStats2 := h.GetPhysicalTableStats(tbl2.Meta().ID, tbl2.Meta())
	checkAnalyzedTableBasicMeta(t, tableStats2, 6)
	// Check index stats
	checkAnalyzedIndexStats(t, tableStats2, tbl2.Meta(), 2, 6, 2)
	// Check column stats
	// For column c, it is not in the predicate columns, so its stats has not been collected.
	checkPredicateColumnStats(t, tableStats2, tbl2.Meta(), []string{"c"}, 6)
}

// TestInitStatsForPartitionedTable tests the InitStats function for partitioned tables.
func TestInitStatsForPartitionedTable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, c int, d int, primary key(a), key idx(b), key gidx(d) global) partition by range(a) (partition p0 values less than (10), partition p1 values less than (20))")
	h := dom.StatsHandle()
	is := dom.InfoSchema()
	err := statstestutil.HandleNextDDLEventWithTxn(h)
	require.NoError(t, err)
	tk.MustExec("insert into t values (1,1,1,1),(2,2,2,2),(3,3,3,3),(11,11,11,11),(12,12,12,12),(13,13,13,13)")
	// With all columns.
	tk.MustExec("analyze table t all columns with 2 topn, 2 buckets")

	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)

	// Get partition IDs
	globalID := tbl.Meta().ID
	p0ID := tbl.Meta().GetPartitionInfo().Definitions[0].ID
	p1ID := tbl.Meta().GetPartitionInfo().Definitions[1].ID

	require.NoError(t, h.InitStats(context.Background(), is))

	// Check global stats
	globalStats := h.GetPhysicalTableStats(globalID, tbl.Meta())
	checkAnalyzedTableBasicMeta(t, globalStats, 6)
	// Check index stats (TopN + Histogram)
	checkAnalyzedIndexStats(t, globalStats, tbl.Meta(), 2, 6, 2)
	// Check column stats (Only Basic Info, no TopN and Histogram)
	checkAnalyzedColumnStatsAllEvicted(t, globalStats, 6)

	// Check partition p0 stats
	p0Stats := h.GetPhysicalTableStats(p0ID, tbl.Meta())
	checkAnalyzedTableBasicMeta(t, p0Stats, 3)
	// Check index stats (TopN + Histogram)
	checkAnalyzedIndexStats(t, p0Stats, tbl.Meta(), 2, 3, 1)
	// Check column stats (Only Basic Info, no TopN and Histogram)
	checkAnalyzedColumnStatsAllEvicted(t, p0Stats, 3)

	// Check partition p1 stats
	p1Stats := h.GetPhysicalTableStats(p1ID, tbl.Meta())
	checkAnalyzedTableBasicMeta(t, p1Stats, 3)
	// Check index stats (TopN + Histogram)
	checkAnalyzedIndexStats(t, p1Stats, tbl.Meta(), 2, 3, 1)
	// Check column stats (Only Basic Info, no TopN and Histogram)
	checkAnalyzedColumnStatsAllEvicted(t, p1Stats, 3)

	// Another partitioned table with no analyze
	tk.MustExec("create table t1(a int, b int, c int, d int, primary key(a), key idx(b), key gidx(d) global) partition by range(a) (partition p0 values less than (10), partition p1 values less than (20))")
	tk.MustExec("insert into t1 values (1,1,1,1),(2,2,2,2),(3,3,3,3),(11,11,11,11),(12,12,12,12),(13,13,13,13)")
	h = dom.StatsHandle()
	is = dom.InfoSchema()
	// Handle DDL event to init the stats meta and histogram meta.
	err = statstestutil.HandleNextDDLEventWithTxn(h)
	require.NoError(t, err)
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(context.Background(), is))
	tbl1, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.NoError(t, err)

	// Get partition IDs for t1
	globalT1ID := tbl1.Meta().ID
	t1p0ID := tbl1.Meta().GetPartitionInfo().Definitions[0].ID
	t1p1ID := tbl1.Meta().GetPartitionInfo().Definitions[1].ID

	require.NoError(t, h.InitStats(context.Background(), is))

	// Check global stats (no analyze)
	t1GlobalStats := h.GetPhysicalTableStats(globalT1ID, tbl1.Meta())
	checkNonAnalyzedTableBasicMeta(t, t1GlobalStats, 6)
	// Check index stats
	checkNonAnalyzedIndexStats(t, t1GlobalStats, tbl1.Meta())
	// Check column stats
	checkNonAnalyzedColumnStats(t, t1GlobalStats, tbl1.Meta())

	// Check partition p0 stats (no analyze)
	t1p0Stats := h.GetPhysicalTableStats(t1p0ID, tbl1.Meta())
	checkNonAnalyzedTableBasicMeta(t, t1p0Stats, 3)
	// Check index stats
	checkNonAnalyzedIndexStats(t, t1p0Stats, tbl1.Meta())
	// Check column stats
	checkNonAnalyzedColumnStats(t, t1p0Stats, tbl1.Meta())

	// Check partition p1 stats (no analyze)
	t1p1Stats := h.GetPhysicalTableStats(t1p1ID, tbl1.Meta())
	checkNonAnalyzedTableBasicMeta(t, t1p1Stats, 3)
	// Check index stats
	checkNonAnalyzedIndexStats(t, t1p1Stats, tbl1.Meta())
	// Check column stats
	checkNonAnalyzedColumnStats(t, t1p1Stats, tbl1.Meta())

	// Another partitioned table with predicate columns
	tk.MustExec("create table t2(a int, b int, c int, d int, primary key(a), key idx(b), key gidx(d) global) partition by range(a) (partition p0 values less than (10), partition p1 values less than (20))")
	tk.MustExec("insert into t2 values (1,1,1,1),(2,2,2,2),(3,3,3,3),(11,11,11,11),(12,12,12,12),(13,13,13,13)")
	h = dom.StatsHandle()
	is = dom.InfoSchema()
	// Handle DDL event to init the stats meta and histogram meta.
	err = statstestutil.HandleNextDDLEventWithTxn(h)
	require.NoError(t, err)
	tk.MustExec("analyze table t2 predicate columns with 2 topn, 2 buckets")
	tbl2, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t2"))
	require.NoError(t, err)

	// Get partition IDs for t2
	globalT2ID := tbl2.Meta().ID
	t2p0ID := tbl2.Meta().GetPartitionInfo().Definitions[0].ID
	t2p1ID := tbl2.Meta().GetPartitionInfo().Definitions[1].ID

	require.NoError(t, h.InitStats(context.Background(), is))

	// Check global stats (predicate columns)
	t2GlobalStats := h.GetPhysicalTableStats(globalT2ID, tbl2.Meta())
	checkAnalyzedTableBasicMeta(t, t2GlobalStats, 6)
	// Check index stats
	checkAnalyzedIndexStats(t, t2GlobalStats, tbl2.Meta(), 2, 6, 2)
	// Check column stats
	// For column c, it is not in the predicate columns, so its stats has not been collected.
	checkPredicateColumnStats(t, t2GlobalStats, tbl2.Meta(), []string{"c"}, 6)

	// Check partition p0 stats (predicate columns)
	t2p0Stats := h.GetPhysicalTableStats(t2p0ID, tbl2.Meta())
	checkAnalyzedTableBasicMeta(t, t2p0Stats, 3)
	// Check index stats
	checkAnalyzedIndexStats(t, t2p0Stats, tbl2.Meta(), 2, 3, 1)
	// Check column stats
	// For column c, it is not in the predicate columns, so its stats has not been collected.
	checkPredicateColumnStats(t, t2p0Stats, tbl2.Meta(), []string{"c"}, 3)

	// Check partition p1 stats (predicate columns)
	t2p1Stats := h.GetPhysicalTableStats(t2p1ID, tbl2.Meta())
	checkAnalyzedTableBasicMeta(t, t2p1Stats, 3)
	// Check index stats
	checkAnalyzedIndexStats(t, t2p1Stats, tbl2.Meta(), 2, 3, 1)
	// Check column stats
	// For column c, it is not in the predicate columns, so its stats has not been collected.
	checkPredicateColumnStats(t, t2p1Stats, tbl2.Meta(), []string{"c"}, 3)
}

// TestInitStatsWithoutHandlingDDLEvent tests the scenario that stats
// meta exists but no histogram meta exists because no analyze has been done
// and no DDL event has been handled.
// TODO: this test is incomplete because we should figure out what
// is the real impact to sync load and async load in this scenario.
func TestInitStatsWithoutHandlingDDLEvent(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, c int, primary key(a), key idx(b))")
	h := dom.StatsHandle()
	is := dom.InfoSchema()
	tk.MustExec("insert into t values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,7,8)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))

	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)

	require.NoError(t, h.InitStats(context.Background(), is))
	tableStats := h.GetPhysicalTableStats(tbl.Meta().ID, tbl.Meta())
	require.False(t, tableStats.Pseudo)
	require.False(t, tableStats.IsAnalyzed())
	// Basic meta info check
	require.Equal(t, int64(6), tableStats.ModifyCount)
	require.Equal(t, int64(6), tableStats.RealtimeCount)
	require.Equal(t, statistics.Version0, tableStats.StatsVer)
	// Check index stats
	require.Equal(t, 0, tableStats.IdxNum())
	idxID := tbl.Meta().Indices[0].ID
	idx := tableStats.GetIdx(idxID)
	require.Nil(t, idx)
	require.False(t, tableStats.ColAndIdxExistenceMap.Has(idxID, true))
	require.False(t, tableStats.ColAndIdxExistenceMap.HasAnalyzed(idxID, true))
	// Check column stats
	for _, colInfo := range tbl.Meta().Columns {
		col := tableStats.GetCol(colInfo.ID)
		require.Nil(t, col)
		require.False(t, tableStats.ColAndIdxExistenceMap.Has(colInfo.ID, false))
		require.False(t, tableStats.ColAndIdxExistenceMap.HasAnalyzed(colInfo.ID, false))
	}
}

func TestInitStats51358(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("analyze V1 cannot support in the next gen")
	}
	originValue := config.GetGlobalConfig().Performance.LiteInitStats
	defer func() {
		config.GetGlobalConfig().Performance.LiteInitStats = originValue
	}()
	config.GetGlobalConfig().Performance.LiteInitStats = false
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("set @@session.tidb_analyze_version = 1")
	testKit.MustExec("create table t(a int, b int, c int, primary key(a), key idx(b))")
	testKit.MustExec("insert into t values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,7,8)")
	testKit.MustExec("analyze table t")
	h := dom.StatsHandle()
	is := dom.InfoSchema()
	// `Update` will not use load by need strategy when `Lease` is 0, and `InitStats` is only called when
	// `Lease` is not 0, so here we just change it.
	h.SetLease(time.Millisecond)

	h.Clear()
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/statistics/handle/cache/StatsCacheGetNil", "return()"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/statistics/handle/cache/StatsCacheGetNil"))
	}()
	require.NoError(t, h.InitStats(context.Background(), is))
	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	stats := h.GetPhysicalTableStats(tbl.Meta().ID, tbl.Meta())
	stats.ForEachColumnImmutable(func(_ int64, column *statistics.Column) bool {
		if mysql.HasPriKeyFlag(column.Info.GetFlag()) {
			// primary key column has no stats info, because primary key's is_index is false. so it cannot load the topn
			require.Nil(t, column.TopN)
		}
		require.False(t, column.IsFullLoad())
		return false
	})
}

func TestInitStatsVer2(t *testing.T) {
	originValue := config.GetGlobalConfig().Performance.LiteInitStats
	defer func() {
		config.GetGlobalConfig().Performance.LiteInitStats = originValue
	}()
	config.GetGlobalConfig().Performance.LiteInitStats = false
	initStatsVer2(t)
}

func initStatsVer2(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version=2")
	tk.MustExec("create table t(a int, b int, c int, d int, index idx(a), index idxab(a, b))")
	h := dom.StatsHandle()
	err := statstestutil.HandleNextDDLEventWithTxn(h)
	require.NoError(t, err)
	analyzehelper.TriggerPredicateColumnsCollection(t, tk, store, "t", "c")
	tk.MustExec("insert into t values(1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3), (4, 4, 4, 4), (4, 4, 4, 4), (4, 4, 4, 4)")
	tk.MustExec("analyze table t predicate columns with 2 topn, 3 buckets")
	tk.MustExec("alter table t add column e int default 1")
	err = statstestutil.HandleNextDDLEventWithTxn(h)
	require.NoError(t, err)
	is := dom.InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	// `Update` will not use load by need strategy when `Lease` is 0, and `InitStats` is only called when
	// `Lease` is not 0, so here we just change it.
	h.SetLease(time.Millisecond)

	h.Clear()
	require.NoError(t, h.InitStats(context.Background(), is))
	table0 := h.GetPhysicalTableStats(tbl.Meta().ID, tbl.Meta())
	require.Equal(t, 5, table0.ColNum())
	require.True(t, table0.GetCol(1).IsAllEvicted())
	require.True(t, table0.GetCol(2).IsAllEvicted())
	require.True(t, table0.GetCol(3).IsAllEvicted())
	require.True(t, !table0.GetCol(4).IsStatsInitialized())
	require.True(t, table0.GetCol(5).IsStatsInitialized())
	require.Equal(t, 2, table0.IdxNum())
	h.Clear()
	require.NoError(t, h.InitStats(context.Background(), is))
	table1 := h.GetPhysicalTableStats(tbl.Meta().ID, tbl.Meta())
	internal.AssertTableEqual(t, table0, table1)
	h.SetLease(0)
}

func TestInitStatsIssue41938(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_analyze_version=1")
	tk.MustExec("set @@session.tidb_analyze_version=1")
	tk.MustExec("create table t1 (a timestamp primary key)")
	tk.MustExec("insert into t1 values ('2023-03-07 14:24:30'), ('2023-03-07 14:24:31'), ('2023-03-07 14:24:32'), ('2023-03-07 14:24:33')")
	tk.MustExec("analyze table t1 with 0 topn")
	h := dom.StatsHandle()
	// `InitStats` is only called when `Lease` is not 0, so here we just change it.
	h.SetLease(time.Millisecond)
	h.Clear()
	require.NoError(t, h.InitStats(context.Background(), dom.InfoSchema()))
	h.SetLease(0)
}

func TestDumpStatsDeltaInBatch(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t1 (c1 int, c2 int)")
	testKit.MustExec("insert into t1 values (1, 1), (2, 2), (3, 3)")
	testKit.MustExec("create table t2 (c1 int, c2 int)")
	testKit.MustExec("insert into t2 values (1, 1), (2, 2), (3, 3)")

	// Dump stats delta in one batch.
	handle := dom.StatsHandle()
	require.NoError(t, handle.DumpStatsDeltaToKV(true))

	// Check the mysql.stats_meta table.
	rows := testKit.MustQuery("select modify_count, count, version from mysql.stats_meta order by table_id").Rows()
	require.Len(t, rows, 2)

	require.Equal(t, "3", rows[0][0])
	require.Equal(t, "3", rows[0][1])
	require.Equal(t, "3", rows[1][0])
	require.Equal(t, "3", rows[1][1])
	require.Equal(
		t,
		rows[0][2],
		rows[1][2],
		"The version of two tables should be the same because they are dumped in the same transaction.",
	)
}

func TestInitStatsForTableWithTopNButNoBuckets(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t2(a int, b int, c int, primary key(a), key idx(b))")
	tk.MustExec("insert into t2 values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,7,8)")
	h := dom.StatsHandle()
	is := dom.InfoSchema()
	analyzehelper.TriggerPredicateColumnsCollection(t, tk, store, "t2", "c")
	tk.MustExec("analyze table t2")
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t2"))
	require.NoError(t, err)

	h.Clear()
	require.NoError(t, h.InitStats(context.Background(), is))
	tableStats2 := h.GetPhysicalTableStats(tbl.Meta().ID, tbl.Meta())
	require.True(t, tableStats2.IsAnalyzed())
	// Check index stats
	require.Equal(t, 1, tableStats2.IdxNum())
	idx := tableStats2.GetIdx(tbl.Meta().Indices[0].ID)
	require.NotNil(t, idx)
	require.True(t, tableStats2.ColAndIdxExistenceMap.Has(idx.ID, true))
	require.True(t, tableStats2.ColAndIdxExistenceMap.HasAnalyzed(idx.ID, true))
	require.True(t, idx.IsStatsInitialized())
	require.True(t, idx.IsFullLoad())
	require.Equal(t, uint64(6), idx.TopN.TotalCount())
	require.Equal(t, float64(6), idx.TotalRowCount())
	require.Equal(t, 0, idx.Len())
}

// TestInitStatsMemoryFullBlocksBucketsButKeepsTopN tests a scenario where:
// - Table has both TopN and buckets in storage
// - Memory becomes full after TopN load
// - TopN should be loaded but buckets should be blocked
func TestInitStatsMemoryFullBlocksBucketsButKeepsTopN(t *testing.T) {
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Performance.LiteInitStats = false
	})

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// Create a table with enough data to generate both TopN and buckets
	tk.MustExec("create table t(a int, b int, c int, primary key(a), key idx(b))")
	// Insert more data to ensure buckets are generated (not just TopN)
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d, %d)", i, i, i))
	}
	h := dom.StatsHandle()
	is := dom.InfoSchema()
	analyzehelper.TriggerPredicateColumnsCollection(t, tk, store, "t", "c")
	tk.MustExec("analyze table t with 2 topn, 3 buckets")
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)

	// Verify the table has buckets before testing
	rows := tk.MustQuery("select count(*) from mysql.stats_buckets where table_id = " +
		fmt.Sprintf("%d", tbl.Meta().ID) + " and is_index = 1").Rows()
	bucketCount := rows[0][0].(string)
	require.NotEqual(t, "0", bucketCount, "table should have buckets for this test")

	// Simulate memory becoming full before buckets are loaded, so buckets are blocked.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/statistics/handle/mockBucketsLoadMemoryLimit", "return(1)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/statistics/handle/mockBucketsLoadMemoryLimit"))
	}()

	h.Clear()
	require.NoError(t, h.InitStats(context.Background(), is))
	tableStats := h.GetPhysicalTableStats(tbl.Meta().ID, tbl.Meta())
	require.True(t, tableStats.IsAnalyzed())

	// Check index stats - TopN should be loaded while buckets are blocked.
	require.Equal(t, 1, tableStats.IdxNum())
	idx := tableStats.GetIdx(tbl.Meta().Indices[0].ID)
	require.NotNil(t, idx)
	require.True(t, tableStats.ColAndIdxExistenceMap.Has(idx.ID, true))
	require.True(t, tableStats.ColAndIdxExistenceMap.HasAnalyzed(idx.ID, true))
	require.True(t, idx.IsStatsInitialized())
	require.False(t, idx.IsFullLoad(), "index should not be FullLoad because buckets are blocked")
	// TopN should be loaded, but buckets should not be loaded
	require.NotNil(t, idx.TopN, "TopN should be loaded before buckets are blocked")
	require.Greater(t, idx.TopN.TotalCount(), uint64(0), "TopN should have entries")
	require.Greater(t, idx.TotalRowCount(), float64(0), "TotalRowCount should be populated by TopN")
	require.Equal(t, 0, idx.Len(), "histogram should have no buckets loaded")
}
