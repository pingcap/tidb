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
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/planner/cardinality"
	"github.com/pingcap/tidb/statistics/handle/internal"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func TestStatsCache(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 int)")
	testKit.MustExec("insert into t values(1, 2)")
	do := dom
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	statsTbl := do.StatsHandle().GetTableStats(tableInfo)
	require.True(t, statsTbl.Pseudo)
	testKit.MustExec("analyze table t")
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	require.False(t, statsTbl.Pseudo)
	testKit.MustExec("create index idx_t on t(c1)")
	do.InfoSchema()
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	// If index is build, but stats is not updated. statsTbl can also work.
	require.False(t, statsTbl.Pseudo)
	// But the added index will not work.
	require.Nil(t, statsTbl.Indices[int64(1)])

	testKit.MustExec("analyze table t")
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	require.False(t, statsTbl.Pseudo)
	// If the new schema drop a column, the table stats can still work.
	testKit.MustExec("alter table t drop column c2")
	is = do.InfoSchema()
	do.StatsHandle().Clear()
	err = do.StatsHandle().Update(is)
	require.NoError(t, err)
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	require.False(t, statsTbl.Pseudo)

	// If the new schema add a column, the table stats can still work.
	testKit.MustExec("alter table t add column c10 int")
	is = do.InfoSchema()

	do.StatsHandle().Clear()
	err = do.StatsHandle().Update(is)
	require.NoError(t, err)
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	require.False(t, statsTbl.Pseudo)
}

func TestStatsCacheMemTracker(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 int,c3 int)")
	testKit.MustExec("insert into t values(1, 2, 3)")
	do := dom
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	statsTbl := do.StatsHandle().GetTableStats(tableInfo)
	require.True(t, statsTbl.MemoryUsage().TotalMemUsage == 0)
	require.True(t, statsTbl.Pseudo)

	testKit.MustExec("analyze table t")
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)

	require.False(t, statsTbl.Pseudo)
	testKit.MustExec("create index idx_t on t(c1)")
	do.InfoSchema()
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)

	// If index is build, but stats is not updated. statsTbl can also work.
	require.False(t, statsTbl.Pseudo)
	// But the added index will not work.
	require.Nil(t, statsTbl.Indices[int64(1)])

	testKit.MustExec("analyze table t")
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)

	require.False(t, statsTbl.Pseudo)

	// If the new schema drop a column, the table stats can still work.
	testKit.MustExec("alter table t drop column c2")
	is = do.InfoSchema()
	do.StatsHandle().Clear()
	err = do.StatsHandle().Update(is)
	require.NoError(t, err)

	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	require.True(t, statsTbl.MemoryUsage().TotalMemUsage > 0)
	require.False(t, statsTbl.Pseudo)

	// If the new schema add a column, the table stats can still work.
	testKit.MustExec("alter table t add column c10 int")
	is = do.InfoSchema()

	do.StatsHandle().Clear()
	err = do.StatsHandle().Update(is)
	require.NoError(t, err)
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	require.False(t, statsTbl.Pseudo)
}

func TestStatsStoreAndLoad(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 int)")
	recordCount := 1000
	for i := 0; i < recordCount; i++ {
		testKit.MustExec("insert into t values (?, ?)", i, i+1)
	}
	testKit.MustExec("create index idx_t on t(c2)")
	do := dom
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()

	testKit.MustExec("analyze table t")
	statsTbl1 := do.StatsHandle().GetTableStats(tableInfo)

	do.StatsHandle().Clear()
	err = do.StatsHandle().Update(is)
	require.NoError(t, err)
	statsTbl2 := do.StatsHandle().GetTableStats(tableInfo)
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
	require.Equal(t, h.GetMemConsumed(), int64(0))
	require.NoError(t, h.InitStats(is))

	var memCostTot int64
	for i := 1; i < 10; i++ {
		tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr(fmt.Sprintf("t%v", i)))
		require.NoError(t, err)
		tStats := h.GetTableStats(tbl.Meta())
		memCostTot += tStats.MemoryUsage().TotalMemUsage
	}

	require.Equal(t, h.GetMemConsumed(), memCostTot)
}

func TestInitStatsMemTraceWithLite(t *testing.T) {
	testInitStatsMemTraceFunc(t, true)
}

func TestInitStatsMemTraceWithoutLite(t *testing.T) {
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

func TestInitStats(t *testing.T) {
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
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	// `Update` will not use load by need strategy when `Lease` is 0, and `InitStats` is only called when
	// `Lease` is not 0, so here we just change it.
	h.SetLease(time.Millisecond)

	h.Clear()
	require.NoError(t, h.InitStats(is))
	table0 := h.GetTableStats(tbl.Meta())
	cols := table0.Columns
	require.Equal(t, uint8(0x36), cols[1].LastAnalyzePos.GetBytes()[0])
	require.Equal(t, uint8(0x37), cols[2].LastAnalyzePos.GetBytes()[0])
	require.Equal(t, uint8(0x38), cols[3].LastAnalyzePos.GetBytes()[0])
	h.Clear()
	require.NoError(t, h.Update(is))
	table1 := h.GetTableStats(tbl.Meta())
	internal.AssertTableEqual(t, table0, table1)
	h.SetLease(0)
}

func TestInitStatsVer2(t *testing.T) {
	originValue := config.GetGlobalConfig().Performance.LiteInitStats
	defer func() {
		config.GetGlobalConfig().Performance.LiteInitStats = originValue
	}()
	config.GetGlobalConfig().Performance.LiteInitStats = false
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version=2")
	tk.MustExec("create table t(a int, b int, c int, index idx(a), index idxab(a, b))")
	tk.MustExec("insert into t values(1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (4, 4, 4), (4, 4, 4)")
	tk.MustExec("analyze table t with 2 topn, 3 buckets")
	h := dom.StatsHandle()
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	// `Update` will not use load by need strategy when `Lease` is 0, and `InitStats` is only called when
	// `Lease` is not 0, so here we just change it.
	h.SetLease(time.Millisecond)

	h.Clear()
	require.NoError(t, h.InitStats(is))
	table0 := h.GetTableStats(tbl.Meta())
	cols := table0.Columns
	require.Equal(t, uint8(0x33), cols[1].LastAnalyzePos.GetBytes()[0])
	require.Equal(t, uint8(0x33), cols[2].LastAnalyzePos.GetBytes()[0])
	require.Equal(t, uint8(0x33), cols[3].LastAnalyzePos.GetBytes()[0])
	h.Clear()
	require.NoError(t, h.InitStats(is))
	table1 := h.GetTableStats(tbl.Meta())
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
	require.NoError(t, h.InitStats(dom.InfoSchema()))
	h.SetLease(0)
}

func TestLoadStats(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("set @@session.tidb_analyze_version=1")
	testKit.MustExec("create table t(a int, b int, c int, primary key(a), key idx(b))")
	testKit.MustExec("insert into t values (1,1,1),(2,2,2),(3,3,3)")

	oriLease := dom.StatsHandle().Lease()
	dom.StatsHandle().SetLease(1)
	defer func() {
		dom.StatsHandle().SetLease(oriLease)
	}()
	testKit.MustExec("analyze table t")

	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	colAID := tableInfo.Columns[0].ID
	colCID := tableInfo.Columns[2].ID
	idxBID := tableInfo.Indices[0].ID
	h := dom.StatsHandle()

	// Index/column stats are not be loaded after analyze.
	stat := h.GetTableStats(tableInfo)
	require.True(t, stat.Columns[colAID].IsAllEvicted())
	hg := stat.Columns[colAID].Histogram
	require.Equal(t, hg.Len(), 0)
	cms := stat.Columns[colAID].CMSketch
	require.Nil(t, cms)
	require.True(t, stat.Indices[idxBID].IsAllEvicted())
	hg = stat.Indices[idxBID].Histogram
	require.Equal(t, hg.Len(), 0)
	cms = stat.Indices[idxBID].CMSketch
	topN := stat.Indices[idxBID].TopN
	require.Equal(t, cms.TotalCount()+topN.TotalCount(), uint64(0))
	require.True(t, stat.Columns[colCID].IsAllEvicted())
	hg = stat.Columns[colCID].Histogram
	require.Equal(t, 0, hg.Len())
	cms = stat.Columns[colCID].CMSketch
	require.Nil(t, cms)

	// Column stats are loaded after they are needed.
	_, err = cardinality.ColumnEqualRowCount(testKit.Session(), stat, types.NewIntDatum(1), colAID)
	require.NoError(t, err)
	_, err = cardinality.ColumnEqualRowCount(testKit.Session(), stat, types.NewIntDatum(1), colCID)
	require.NoError(t, err)
	require.NoError(t, h.LoadNeededHistograms())
	stat = h.GetTableStats(tableInfo)
	require.True(t, stat.Columns[colAID].IsFullLoad())
	hg = stat.Columns[colAID].Histogram
	require.Greater(t, hg.Len(), 0)
	// We don't maintain cmsketch for pk.
	cms = stat.Columns[colAID].CMSketch
	require.Nil(t, cms)
	require.True(t, stat.Columns[colCID].IsFullLoad())
	hg = stat.Columns[colCID].Histogram
	require.Greater(t, hg.Len(), 0)
	cms = stat.Columns[colCID].CMSketch
	require.NotNil(t, cms)

	// Index stats are loaded after they are needed.
	idx := stat.Indices[idxBID]
	hg = idx.Histogram
	cms = idx.CMSketch
	topN = idx.TopN
	require.Equal(t, float64(cms.TotalCount()+topN.TotalCount())+hg.TotalRowCount(), float64(0))
	require.False(t, idx.IsEssentialStatsLoaded())
	// IsInvalid adds the index to HistogramNeededItems.
	idx.IsInvalid(testKit.Session(), false)
	require.NoError(t, h.LoadNeededHistograms())
	stat = h.GetTableStats(tableInfo)
	idx = stat.Indices[tableInfo.Indices[0].ID]
	hg = idx.Histogram
	cms = idx.CMSketch
	topN = idx.TopN
	require.Greater(t, float64(cms.TotalCount()+topN.TotalCount())+hg.TotalRowCount(), float64(0))
	require.True(t, idx.IsFullLoad())

	// Following test tests whether the LoadNeededHistograms would panic.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/statistics/mockGetStatsReaderFail", `return(true)`))
	err = h.LoadNeededHistograms()
	require.Error(t, err)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/statistics/mockGetStatsReaderFail"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/statistics/mockGetStatsReaderPanic", "panic"))
	err = h.LoadNeededHistograms()
	require.Error(t, err)
	require.Regexp(t, ".*getStatsReader panic.*", err.Error())
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/statistics/mockGetStatsReaderPanic"))
	err = h.LoadNeededHistograms()
	require.NoError(t, err)
}
