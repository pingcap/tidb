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

package handletest

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestEmptyTable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 int, key cc1(c1), key cc2(c2))")
	testKit.MustExec("analyze table t")
	do := dom
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	statsTbl := do.StatsHandle().GetTableStats(tableInfo)
	count := cardinality.ColumnGreaterRowCount(mock.NewContext(), statsTbl, types.NewDatum(1), tableInfo.Columns[0].ID)
	require.Equal(t, 0.0, count)
}

func TestColumnIDs(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 int)")
	testKit.MustExec("insert into t values(1, 2)")
	testKit.MustExec("analyze table t")
	do := dom
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	statsTbl := do.StatsHandle().GetTableStats(tableInfo)
	sctx := mock.NewContext()
	ran := &ranger.Range{
		LowVal:      []types.Datum{types.MinNotNullDatum()},
		HighVal:     []types.Datum{types.NewIntDatum(2)},
		LowExclude:  false,
		HighExclude: true,
		Collators:   collate.GetBinaryCollatorSlice(1),
	}
	count, err := cardinality.GetRowCountByColumnRanges(sctx, &statsTbl.HistColl, tableInfo.Columns[0].ID, []*ranger.Range{ran})
	require.NoError(t, err)
	require.Equal(t, float64(1), count)

	// Drop a column and the offset changed,
	testKit.MustExec("alter table t drop column c1")
	is = do.InfoSchema()
	do.StatsHandle().Clear()
	err = do.StatsHandle().Update(is)
	require.NoError(t, err)
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo = tbl.Meta()
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	// At that time, we should get c2's stats instead of c1's.
	count, err = cardinality.GetRowCountByColumnRanges(sctx, &statsTbl.HistColl, tableInfo.Columns[0].ID, []*ranger.Range{ran})
	require.NoError(t, err)
	require.Equal(t, 0.0, count)
}

func TestDurationToTS(t *testing.T) {
	tests := []time.Duration{time.Millisecond, time.Second, time.Minute, time.Hour}
	for _, test := range tests {
		ts := util.DurationToTS(test)
		require.Equal(t, int64(test), oracle.ExtractPhysical(ts)*int64(time.Millisecond))
	}
}

func TestVersion(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit2 := testkit.NewTestKit(t, store)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t1 (c1 int, c2 int)")
	testKit.MustExec("analyze table t1")
	do := dom
	is := do.InfoSchema()
	tbl1, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	tableInfo1 := tbl1.Meta()
	h, err := handle.NewHandle(testKit.Session(), testKit2.Session(), time.Millisecond, do.SysSessionPool(), do.SysProcTracker(), do.GetAutoAnalyzeProcID)
	defer func() {
		h.Close()
	}()
	require.NoError(t, err)
	unit := oracle.ComposeTS(1, 0)
	testKit.MustExec("update mysql.stats_meta set version = ? where table_id = ?", 2*unit, tableInfo1.ID)

	require.NoError(t, h.Update(is))
	require.Equal(t, 2*unit, h.MaxTableStatsVersion())
	statsTbl1 := h.GetTableStats(tableInfo1)
	require.False(t, statsTbl1.Pseudo)

	testKit.MustExec("create table t2 (c1 int, c2 int)")
	testKit.MustExec("analyze table t2")
	is = do.InfoSchema()
	tbl2, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	require.NoError(t, err)
	tableInfo2 := tbl2.Meta()
	// A smaller version write, and we can still read it.
	testKit.MustExec("update mysql.stats_meta set version = ? where table_id = ?", unit, tableInfo2.ID)
	require.NoError(t, h.Update(is))
	require.Equal(t, 2*unit, h.MaxTableStatsVersion())
	statsTbl2 := h.GetTableStats(tableInfo2)
	require.False(t, statsTbl2.Pseudo)

	testKit.MustExec("insert t1 values(1,2)")
	testKit.MustExec("analyze table t1")
	offset := 3 * unit
	testKit.MustExec("update mysql.stats_meta set version = ? where table_id = ?", offset+4, tableInfo1.ID)
	require.NoError(t, h.Update(is))
	require.Equal(t, offset+uint64(4), h.MaxTableStatsVersion())
	statsTbl1 = h.GetTableStats(tableInfo1)
	require.Equal(t, int64(1), statsTbl1.RealtimeCount)

	testKit.MustExec("insert t2 values(1,2)")
	testKit.MustExec("analyze table t2")
	// A smaller version write, and we can still read it.
	testKit.MustExec("update mysql.stats_meta set version = ? where table_id = ?", offset+3, tableInfo2.ID)
	require.NoError(t, h.Update(is))
	require.Equal(t, offset+uint64(4), h.MaxTableStatsVersion())
	statsTbl2 = h.GetTableStats(tableInfo2)
	require.Equal(t, int64(1), statsTbl2.RealtimeCount)

	testKit.MustExec("insert t2 values(1,2)")
	testKit.MustExec("analyze table t2")
	// A smaller version write, and we cannot read it. Because at this time, lastThree Version is 4.
	testKit.MustExec("update mysql.stats_meta set version = 1 where table_id = ?", tableInfo2.ID)
	require.NoError(t, h.Update(is))
	require.Equal(t, offset+uint64(4), h.MaxTableStatsVersion())
	statsTbl2 = h.GetTableStats(tableInfo2)
	require.Equal(t, int64(1), statsTbl2.RealtimeCount)

	// We add an index and analyze it, but DDL doesn't load.
	testKit.MustExec("alter table t2 add column c3 int")
	testKit.MustExec("analyze table t2")
	// load it with old schema.
	require.NoError(t, h.Update(is))
	statsTbl2 = h.GetTableStats(tableInfo2)
	require.False(t, statsTbl2.Pseudo)
	require.Nil(t, statsTbl2.Columns[int64(3)])
	// Next time DDL updated.
	is = do.InfoSchema()
	require.NoError(t, h.Update(is))
	statsTbl2 = h.GetTableStats(tableInfo2)
	require.False(t, statsTbl2.Pseudo)
	require.Nil(t, statsTbl2.Columns[int64(3)])
	tbl2, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	require.NoError(t, err)
	tableInfo2 = tbl2.Meta()
	statsTbl2, err = h.TableStatsFromStorage(tableInfo2, tableInfo2.ID, true, 0)
	require.NoError(t, err)
	require.NotNil(t, statsTbl2.Columns[int64(3)])
}

func TestLoadHist(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 varchar(12), c2 char(12))")
	do := dom
	h := do.StatsHandle()
	err := h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)
	rowCount := 10
	for i := 0; i < rowCount; i++ {
		testKit.MustExec("insert into t values('a','ddd')")
	}
	testKit.MustExec("analyze table t")
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	oldStatsTbl := h.GetTableStats(tableInfo)
	for i := 0; i < rowCount; i++ {
		testKit.MustExec("insert into t values('bb','sdfga')")
	}
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	err = h.Update(do.InfoSchema())
	require.NoError(t, err)
	newStatsTbl := h.GetTableStats(tableInfo)
	// The stats table is updated.
	require.False(t, oldStatsTbl == newStatsTbl)
	// Only the TotColSize of histograms is updated.
	for id, hist := range oldStatsTbl.Columns {
		require.Less(t, hist.TotColSize, newStatsTbl.Columns[id].TotColSize)

		temp := hist.TotColSize
		hist.TotColSize = newStatsTbl.Columns[id].TotColSize
		require.True(t, statistics.HistogramEqual(&hist.Histogram, &newStatsTbl.Columns[id].Histogram, false))
		hist.TotColSize = temp

		require.True(t, hist.CMSketch.Equal(newStatsTbl.Columns[id].CMSketch))
		require.Equal(t, newStatsTbl.Columns[id].Info, hist.Info)
	}
	// Add column c3, we only update c3.
	testKit.MustExec("alter table t add column c3 int")
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)
	is = do.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo = tbl.Meta()
	require.NoError(t, h.Update(is))
	newStatsTbl2 := h.GetTableStats(tableInfo)
	require.False(t, newStatsTbl2 == newStatsTbl)
	// The histograms is not updated.
	for id, hist := range newStatsTbl.Columns {
		require.Equal(t, newStatsTbl2.Columns[id], hist)
	}
	require.Greater(t, newStatsTbl2.Columns[int64(3)].LastUpdateVersion, newStatsTbl2.Columns[int64(1)].LastUpdateVersion)
}

func TestCorrelation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t(c1 int primary key, c2 int)")
	testKit.MustExec("select * from t where c1 > 10 and c2 > 10")
	testKit.MustExec("insert into t values(1,1),(3,12),(4,20),(2,7),(5,21)")
	testKit.MustExec("set @@session.tidb_analyze_version=1")
	testKit.MustExec("analyze table t")
	result := testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	require.Len(t, result.Rows(), 2)
	require.Equal(t, "0", result.Rows()[0][9])
	require.Equal(t, "1", result.Rows()[1][9])
	testKit.MustExec("set @@session.tidb_analyze_version=2")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	require.Len(t, result.Rows(), 2)
	require.Equal(t, "1", result.Rows()[0][9])
	require.Equal(t, "1", result.Rows()[1][9])
	testKit.MustExec("insert into t values(8,18)")
	testKit.MustExec("set @@session.tidb_analyze_version=1")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	require.Len(t, result.Rows(), 2)
	require.Equal(t, "0", result.Rows()[0][9])
	require.Equal(t, "0.8285714285714286", result.Rows()[1][9])
	testKit.MustExec("set @@session.tidb_analyze_version=2")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	require.Len(t, result.Rows(), 2)
	require.Equal(t, "1", result.Rows()[0][9])
	require.Equal(t, "0.8285714285714286", result.Rows()[1][9])

	testKit.MustExec("truncate table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	require.Len(t, result.Rows(), 0)
	testKit.MustExec("insert into t values(1,21),(3,12),(4,7),(2,20),(5,1)")
	testKit.MustExec("set @@session.tidb_analyze_version=1")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	require.Len(t, result.Rows(), 2)
	require.Equal(t, "0", result.Rows()[0][9])
	require.Equal(t, "-1", result.Rows()[1][9])
	testKit.MustExec("set @@session.tidb_analyze_version=2")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	require.Len(t, result.Rows(), 2)
	require.Equal(t, "1", result.Rows()[0][9])
	require.Equal(t, "-1", result.Rows()[1][9])
	testKit.MustExec("insert into t values(8,4)")
	testKit.MustExec("set @@session.tidb_analyze_version=1")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	require.Len(t, result.Rows(), 2)
	require.Equal(t, "0", result.Rows()[0][9])
	require.Equal(t, "-0.9428571428571428", result.Rows()[1][9])
	testKit.MustExec("set @@session.tidb_analyze_version=2")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	require.Len(t, result.Rows(), 2)
	require.Equal(t, "1", result.Rows()[0][9])
	require.Equal(t, "-0.9428571428571428", result.Rows()[1][9])

	testKit.MustExec("truncate table t")
	testKit.MustExec("insert into t values (1,1),(2,1),(3,1),(4,1),(5,1),(6,1),(7,1),(8,1),(9,1),(10,1),(11,1),(12,1),(13,1),(14,1),(15,1),(16,1),(17,1),(18,1),(19,1),(20,2),(21,2),(22,2),(23,2),(24,2),(25,2)")
	testKit.MustExec("set @@session.tidb_analyze_version=1")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	require.Len(t, result.Rows(), 2)
	require.Equal(t, "0", result.Rows()[0][9])
	require.Equal(t, "1", result.Rows()[1][9])
	testKit.MustExec("set @@session.tidb_analyze_version=2")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	require.Len(t, result.Rows(), 2)
	require.Equal(t, "1", result.Rows()[0][9])
	require.Equal(t, "1", result.Rows()[1][9])

	testKit.MustExec("drop table t")
	testKit.MustExec("create table t(c1 int, c2 int)")
	testKit.MustExec("insert into t values(1,1),(2,7),(3,12),(4,20),(5,21),(8,18)")
	testKit.MustExec("set @@session.tidb_analyze_version=1")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	require.Len(t, result.Rows(), 2)
	require.Equal(t, "1", result.Rows()[0][9])
	require.Equal(t, "0.8285714285714286", result.Rows()[1][9])
	testKit.MustExec("set @@session.tidb_analyze_version=2")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	require.Len(t, result.Rows(), 2)
	require.Equal(t, "1", result.Rows()[0][9])
	require.Equal(t, "0.8285714285714286", result.Rows()[1][9])

	testKit.MustExec("truncate table t")
	testKit.MustExec("insert into t values(1,1),(2,7),(3,12),(8,18),(4,20),(5,21)")
	testKit.MustExec("set @@session.tidb_analyze_version=1")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	require.Len(t, result.Rows(), 2)
	require.Equal(t, "0.8285714285714286", result.Rows()[0][9])
	require.Equal(t, "1", result.Rows()[1][9])
	testKit.MustExec("set @@session.tidb_analyze_version=2")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	require.Len(t, result.Rows(), 2)
	require.Equal(t, "0.8285714285714286", result.Rows()[0][9])
	require.Equal(t, "1", result.Rows()[1][9])

	testKit.MustExec("drop table t")
	testKit.MustExec("create table t(c1 int primary key, c2 int, c3 int, key idx_c2(c2))")
	testKit.MustExec("insert into t values(1,1,1),(2,2,2),(3,3,3)")
	testKit.MustExec("set @@session.tidb_analyze_version=1")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't' and Is_index = 0").Sort()
	require.Len(t, result.Rows(), 3)
	require.Equal(t, "0", result.Rows()[0][9])
	require.Equal(t, "1", result.Rows()[1][9])
	require.Equal(t, "1", result.Rows()[2][9])
	result = testKit.MustQuery("show stats_histograms where Table_name = 't' and Is_index = 1").Sort()
	require.Len(t, result.Rows(), 1)
	require.Equal(t, "0", result.Rows()[0][9])
	testKit.MustExec("set @@tidb_analyze_version=2")
	testKit.MustExec("analyze table t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't' and Is_index = 0").Sort()
	require.Len(t, result.Rows(), 3)
	require.Equal(t, "1", result.Rows()[0][9])
	require.Equal(t, "1", result.Rows()[1][9])
	require.Equal(t, "1", result.Rows()[2][9])
	result = testKit.MustQuery("show stats_histograms where Table_name = 't' and Is_index = 1").Sort()
	require.Len(t, result.Rows(), 1)
	require.Equal(t, "0", result.Rows()[0][9])
}

func TestMergeGlobalTopN(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("set @@session.tidb_analyze_version=2;")
	tk.MustExec("set @@session.tidb_partition_prune_mode='dynamic';")
	tk.MustExec(`create table t (a int, b int, key(b)) partition by range (a) (
		partition p0 values less than (10),
		partition p1 values less than (20)
	);`)
	tk.MustExec("insert into t values(1, 1), (1, 1), (1, 1), (1, 1), (2, 2), (2, 2), (3, 3), (3, 3), (3, 3), " +
		"(11, 11), (11, 11), (11, 11), (12, 12), (12, 12), (12, 12), (13, 3), (13, 3);")
	tk.MustExec("analyze table t with 2 topn;")
	// The top2 values in partition p0 are 1(count = 4) and 3(count = 3).
	tk.MustQuery("show stats_topn where table_name = 't' and column_name = 'b' and partition_name = 'p0';").Check(testkit.Rows(
		("test t p0 b 0 1 4"),
		("test t p0 b 0 3 3"),
		("test t p0 b 1 1 4"),
		("test t p0 b 1 3 3")))
	// The top2 values in partition p1 are 11(count = 3) and 12(count = 3).
	tk.MustQuery("show stats_topn where table_name = 't' and column_name = 'b' and partition_name = 'p1';").Check(testkit.Rows(
		("test t p1 b 0 11 3"),
		("test t p1 b 0 12 3"),
		("test t p1 b 1 11 3"),
		("test t p1 b 1 12 3")))
	// The top2 values in global are 1(count = 4) and 3(count = 5).
	// Notice: The value 3 does not appear in the topN structure of partition one.
	// But we can still use the histogram to calculate its accurate value.
	tk.MustQuery("show stats_topn where table_name = 't' and column_name = 'b' and partition_name = 'global';").Check(testkit.Rows(
		("test t global b 0 1 4"),
		("test t global b 0 3 5"),
		("test t global b 1 1 4"),
		("test t global b 1 3 5")))
}

func TestExtendedStatsOps(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set session tidb_enable_extended_stats = on")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int primary key, b int, c int, d int)")
	tk.MustExec("insert into t values(1,1,5,1),(2,2,4,2),(3,3,3,3),(4,4,2,4),(5,5,1,5)")
	tk.MustExec("analyze table t")
	err := tk.ExecToErr("alter table not_exist_db.t add stats_extended s1 correlation(b,c)")
	require.Equal(t, "[schema:1146]Table 'not_exist_db.t' doesn't exist", err.Error())
	err = tk.ExecToErr("alter table not_exist_tbl add stats_extended s1 correlation(b,c)")
	require.Equal(t, "[schema:1146]Table 'test.not_exist_tbl' doesn't exist", err.Error())
	err = tk.ExecToErr("alter table t add stats_extended s1 correlation(b,e)")
	require.Equal(t, "[schema:1054]Unknown column 'e' in 't'", err.Error())
	tk.MustExec("alter table t add stats_extended s1 correlation(a,b)")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1105 No need to create correlation statistics on the integer primary key column",
	))
	tk.MustQuery("select type, column_ids, stats, status from mysql.stats_extended where name = 's1'").Check(testkit.Rows())
	err = tk.ExecToErr("alter table t add stats_extended s1 correlation(b,c,d)")
	require.Equal(t, "Only support Correlation and Dependency statistics types on 2 columns", err.Error())

	tk.MustQuery("select type, column_ids, stats, status from mysql.stats_extended where name = 's1'").Check(testkit.Rows())
	tk.MustExec("alter table t add stats_extended s1 correlation(b,c)")
	tk.MustQuery("select type, column_ids, stats, status from mysql.stats_extended where name = 's1'").Check(testkit.Rows(
		"2 [2,3] <nil> 0",
	))
	do := dom
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	err = do.StatsHandle().Update(is)
	require.NoError(t, err)
	statsTbl := do.StatsHandle().GetTableStats(tableInfo)
	require.NotNil(t, statsTbl)
	require.NotNil(t, statsTbl.ExtendedStats)
	require.Len(t, statsTbl.ExtendedStats.Stats, 0)

	tk.MustExec("update mysql.stats_extended set status = 1 where name = 's1'")
	do.StatsHandle().Clear()
	err = do.StatsHandle().Update(is)
	require.NoError(t, err)
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	require.NotNil(t, statsTbl)
	require.NotNil(t, statsTbl.ExtendedStats)
	require.Len(t, statsTbl.ExtendedStats.Stats, 1)

	tk.MustExec("alter table t drop stats_extended s1")
	tk.MustQuery("select type, column_ids, stats, status from mysql.stats_extended where name = 's1'").Check(testkit.Rows(
		"2 [2,3] <nil> 2",
	))
	err = do.StatsHandle().Update(is)
	require.NoError(t, err)
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	require.NotNil(t, statsTbl.ExtendedStats)
	require.Len(t, statsTbl.ExtendedStats.Stats, 0)
}

func TestAdminReloadStatistics1(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set session tidb_enable_extended_stats = on")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int primary key, b int, c int, d int)")
	tk.MustExec("insert into t values(1,1,5,1),(2,2,4,2),(3,3,3,3),(4,4,2,4),(5,5,1,5)")
	tk.MustExec("analyze table t")
	tk.MustExec("alter table t add stats_extended s1 correlation(b,c)")
	tk.MustQuery("select type, column_ids, stats, status from mysql.stats_extended where name = 's1'").Check(testkit.Rows(
		"2 [2,3] <nil> 0",
	))
	do := dom
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	err = do.StatsHandle().Update(is)
	require.NoError(t, err)
	statsTbl := do.StatsHandle().GetTableStats(tableInfo)
	require.NotNil(t, statsTbl)
	require.NotNil(t, statsTbl.ExtendedStats)
	require.Len(t, statsTbl.ExtendedStats.Stats, 0)

	tk.MustExec("update mysql.stats_extended set status = 1 where name = 's1'")
	do.StatsHandle().Clear()
	err = do.StatsHandle().Update(is)
	require.NoError(t, err)
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	require.NotNil(t, statsTbl)
	require.NotNil(t, statsTbl.ExtendedStats)
	require.Len(t, statsTbl.ExtendedStats.Stats, 1)

	tk.MustExec("delete from mysql.stats_extended where name = 's1'")
	err = do.StatsHandle().Update(is)
	require.NoError(t, err)
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	require.NotNil(t, statsTbl.ExtendedStats)
	require.Len(t, statsTbl.ExtendedStats.Stats, 1)

	tk.MustExec("admin reload stats_extended")
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	require.NotNil(t, statsTbl.ExtendedStats)
	require.Len(t, statsTbl.ExtendedStats.Stats, 0)
}

func TestAdminReloadStatistics2(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set session tidb_enable_extended_stats = on")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1,1),(2,2),(3,3)")
	tk.MustExec("alter table t add stats_extended s1 correlation(a,b)")
	tk.MustExec("analyze table t")
	tk.MustQuery("select stats, status from mysql.stats_extended where name = 's1'").Check(testkit.Rows(
		"1.000000 1",
	))
	rows := tk.MustQuery("show stats_extended where stats_name = 's1'").Rows()
	require.Len(t, rows, 1)

	tk.MustExec("delete from mysql.stats_extended where name = 's1'")
	is := dom.InfoSchema()
	dom.StatsHandle().Update(is)
	tk.MustQuery("select stats, status from mysql.stats_extended where name = 's1'").Check(testkit.Rows())
	rows = tk.MustQuery("show stats_extended where stats_name = 's1'").Rows()
	require.Len(t, rows, 1)

	tk.MustExec("admin reload stats_extended")
	tk.MustQuery("select stats, status from mysql.stats_extended where name = 's1'").Check(testkit.Rows())
	rows = tk.MustQuery("show stats_extended where stats_name = 's1'").Rows()
	require.Len(t, rows, 0)
}

func TestCorrelationStatsCompute(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set session tidb_enable_extended_stats = on")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, c int)")
	tk.MustExec("insert into t values(1,1,5),(2,2,4),(3,3,3),(4,4,2),(5,5,1)")
	tk.MustExec("analyze table t")
	tk.MustQuery("select type, column_ids, stats, status from mysql.stats_extended").Check(testkit.Rows())
	tk.MustExec("alter table t add stats_extended s1 correlation(a,b)")
	tk.MustExec("alter table t add stats_extended s2 correlation(a,c)")
	tk.MustQuery("select type, column_ids, stats, status from mysql.stats_extended").Sort().Check(testkit.Rows(
		"2 [1,2] <nil> 0",
		"2 [1,3] <nil> 0",
	))
	do := dom
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	err = do.StatsHandle().Update(is)
	require.NoError(t, err)
	statsTbl := do.StatsHandle().GetTableStats(tableInfo)
	require.NotNil(t, statsTbl)
	require.NotNil(t, statsTbl.ExtendedStats)
	require.Len(t, statsTbl.ExtendedStats.Stats, 0)

	tk.MustExec("analyze table t")
	tk.MustQuery("select type, column_ids, stats, status from mysql.stats_extended").Sort().Check(testkit.Rows(
		"2 [1,2] 1.000000 1",
		"2 [1,3] -1.000000 1",
	))
	tk.MustExec("set @@session.tidb_analyze_version=2")
	tk.MustExec("analyze table t")
	tk.MustQuery("select type, column_ids, stats, status from mysql.stats_extended").Sort().Check(testkit.Rows(
		"2 [1,2] 1.000000 1",
		"2 [1,3] -1.000000 1",
	))
	err = do.StatsHandle().Update(is)
	require.NoError(t, err)
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	require.NotNil(t, statsTbl)
	require.NotNil(t, statsTbl.ExtendedStats)
	require.Len(t, statsTbl.ExtendedStats.Stats, 2)
	foundS1, foundS2 := false, false
	for name, item := range statsTbl.ExtendedStats.Stats {
		switch name {
		case "s1":
			foundS1 = true
			require.Equal(t, float64(1), item.ScalarVals)
		case "s2":
			foundS2 = true
			require.Equal(t, float64(-1), item.ScalarVals)
		default:
			require.FailNow(t, "Unexpected extended stats in cache")
		}
	}
	require.True(t, foundS1 && foundS2)

	// Check that table with NULLs won't cause panic
	tk.MustExec("delete from t")
	tk.MustExec("insert into t values(1,null,2), (2,null,null)")
	tk.MustExec("set @@session.tidb_analyze_version=1")
	tk.MustExec("analyze table t")
	tk.MustQuery("select type, column_ids, stats, status from mysql.stats_extended").Sort().Check(testkit.Rows(
		"2 [1,2] 0.000000 1",
		"2 [1,3] 1.000000 1",
	))
	tk.MustExec("set @@session.tidb_analyze_version=2")
	tk.MustExec("analyze table t")
	tk.MustQuery("select type, column_ids, stats, status from mysql.stats_extended").Sort().Check(testkit.Rows(
		"2 [1,2] 0.000000 1",
		"2 [1,3] 1.000000 1",
	))
	tk.MustExec("insert into t values(3,3,3)")
	tk.MustExec("set @@session.tidb_analyze_version=1")
	tk.MustExec("analyze table t")
	tk.MustQuery("select type, column_ids, stats, status from mysql.stats_extended").Sort().Check(testkit.Rows(
		"2 [1,2] 1.000000 1",
		"2 [1,3] 1.000000 1",
	))
	tk.MustExec("set @@session.tidb_analyze_version=2")
	tk.MustExec("analyze table t")
	tk.MustQuery("select type, column_ids, stats, status from mysql.stats_extended").Sort().Check(testkit.Rows(
		"2 [1,2] 1.000000 1",
		"2 [1,3] 1.000000 1",
	))
}

func TestSyncStatsExtendedRemoval(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set session tidb_enable_extended_stats = on")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1,1),(2,2),(3,3)")
	tk.MustExec("alter table t add stats_extended s1 correlation(a,b)")
	tk.MustExec("analyze table t")
	do := dom
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	statsTbl := do.StatsHandle().GetTableStats(tableInfo)
	require.NotNil(t, statsTbl)
	require.NotNil(t, statsTbl.ExtendedStats)
	require.Len(t, statsTbl.ExtendedStats.Stats, 1)
	item := statsTbl.ExtendedStats.Stats["s1"]
	require.NotNil(t, item)
	result := tk.MustQuery("show stats_extended where db_name = 'test' and table_name = 't'")
	require.Len(t, result.Rows(), 1)

	tk.MustExec("alter table t drop stats_extended s1")
	statsTbl = do.StatsHandle().GetTableStats(tableInfo)
	require.NotNil(t, statsTbl)
	require.NotNil(t, statsTbl.ExtendedStats)
	require.Len(t, statsTbl.ExtendedStats.Stats, 0)
	result = tk.MustQuery("show stats_extended where db_name = 'test' and table_name = 't'")
	require.Len(t, result.Rows(), 0)
}

func TestStaticPartitionPruneMode(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_partition_prune_mode='" + string(variable.Static) + "'")
	tk.MustExec("use test")
	tk.MustExec(`create table t (a int, key(a)) partition by range(a)
					(partition p0 values less than (10),
					partition p1 values less than (22))`)
	tk.MustExec(`insert into t values (1), (2), (3), (10), (11)`)
	tk.MustExec(`analyze table t`)
	require.True(t, tk.MustNoGlobalStats("t"))
	tk.MustExec("set @@tidb_partition_prune_mode='" + string(variable.Dynamic) + "'")
	require.True(t, tk.MustNoGlobalStats("t"))

	tk.MustExec("set @@tidb_partition_prune_mode='" + string(variable.Static) + "'")
	tk.MustExec(`insert into t values (4), (5), (6)`)
	tk.MustExec(`analyze table t partition p0`)
	require.True(t, tk.MustNoGlobalStats("t"))
	tk.MustExec("set @@tidb_partition_prune_mode='" + string(variable.Dynamic) + "'")
	require.True(t, tk.MustNoGlobalStats("t"))
	tk.MustExec("set @@tidb_partition_prune_mode='" + string(variable.Static) + "'")
}

func TestMergeIdxHist(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_partition_prune_mode='" + string(variable.Dynamic) + "'")
	defer tk.MustExec("set @@tidb_partition_prune_mode='" + string(variable.Static) + "'")
	tk.MustExec("use test")
	tk.MustExec(`
		create table t (a int, key(a))
		partition by range (a) (
			partition p0 values less than (10),
			partition p1 values less than (20))`)
	tk.MustExec("set @@tidb_analyze_version=2")
	defer tk.MustExec("set @@tidb_analyze_version=1")
	tk.MustExec("insert into t values (1), (2), (3), (4), (5), (6), (6), (null), (11), (12), (13), (14), (15), (16), (17), (18), (19), (19)")

	tk.MustExec("analyze table t with 2 topn, 2 buckets")
	rows := tk.MustQuery("show stats_buckets where partition_name like 'global'")
	require.Len(t, rows.Rows(), 4)
}

func TestPartitionPruneModeSessionVariable(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune")

	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("set tidb_cost_model_version=1")
	tk1.MustExec("set @@tidb_partition_prune_mode = '" + string(variable.Dynamic) + "'")
	tk1.MustExec(`set @@tidb_analyze_version=2`)

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk2.MustExec("set tidb_cost_model_version=1")
	tk2.MustExec("set @@tidb_partition_prune_mode = '" + string(variable.Static) + "'")
	tk2.MustExec(`set @@tidb_analyze_version=2`)

	tk1.MustExec(`create table t (a int, key(a)) partition by range(a)
					(partition p0 values less than (10),
					partition p1 values less than (22))`)

	tk1.MustQuery("explain format = 'brief' select * from t").Check(testkit.Rows(
		"TableReader 10000.00 root partition:all data:TableFullScan",
		"└─TableFullScan 10000.00 cop[tikv] table:t keep order:false, stats:pseudo",
	))
	tk2.MustQuery("explain format = 'brief' select * from t").Check(testkit.Rows(
		"PartitionUnion 20000.00 root  ",
		"├─TableReader 10000.00 root  data:TableFullScan",
		"│ └─TableFullScan 10000.00 cop[tikv] table:t, partition:p0 keep order:false, stats:pseudo",
		"└─TableReader 10000.00 root  data:TableFullScan",
		"  └─TableFullScan 10000.00 cop[tikv] table:t, partition:p1 keep order:false, stats:pseudo",
	))

	tk1.MustExec(`insert into t values (1), (2), (3), (10), (11)`)
	tk1.MustExec(`analyze table t with 1 topn, 2 buckets`)
	tk1.MustQuery("explain format = 'brief' select * from t").Check(testkit.Rows(
		"TableReader 5.00 root partition:all data:TableFullScan",
		"└─TableFullScan 5.00 cop[tikv] table:t keep order:false",
	))
	tk2.MustQuery("explain format = 'brief' select * from t").Check(testkit.Rows(
		"PartitionUnion 5.00 root  ",
		"├─TableReader 3.00 root  data:TableFullScan",
		"│ └─TableFullScan 3.00 cop[tikv] table:t, partition:p0 keep order:false",
		"└─TableReader 2.00 root  data:TableFullScan",
		"  └─TableFullScan 2.00 cop[tikv] table:t, partition:p1 keep order:false",
	))

	tk1.MustExec("set @@tidb_partition_prune_mode = '" + string(variable.Static) + "'")
	tk1.MustQuery("explain format = 'brief' select * from t").Check(testkit.Rows(
		"PartitionUnion 5.00 root  ",
		"├─TableReader 3.00 root  data:TableFullScan",
		"│ └─TableFullScan 3.00 cop[tikv] table:t, partition:p0 keep order:false",
		"└─TableReader 2.00 root  data:TableFullScan",
		"  └─TableFullScan 2.00 cop[tikv] table:t, partition:p1 keep order:false",
	))
	tk2.MustExec("set @@tidb_partition_prune_mode = '" + string(variable.Dynamic) + "'")
	tk2.MustQuery("explain format = 'brief' select * from t").Check(testkit.Rows(
		"TableReader 5.00 root partition:all data:TableFullScan",
		"└─TableFullScan 5.00 cop[tikv] table:t keep order:false",
	))
}

func TestRepetitiveAddDropExtendedStats(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set session tidb_enable_extended_stats = on")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1,1),(2,2),(3,3)")
	tk.MustExec("alter table t add stats_extended s1 correlation(a,b)")
	tk.MustQuery("select name, status from mysql.stats_extended where name = 's1'").Sort().Check(testkit.Rows(
		"s1 0",
	))
	result := tk.MustQuery("show stats_extended where db_name = 'test' and table_name = 't'")
	require.Len(t, result.Rows(), 0)
	tk.MustExec("analyze table t")
	tk.MustQuery("select name, status from mysql.stats_extended where name = 's1'").Sort().Check(testkit.Rows(
		"s1 1",
	))
	result = tk.MustQuery("show stats_extended where db_name = 'test' and table_name = 't'")
	require.Len(t, result.Rows(), 1)
	tk.MustExec("alter table t drop stats_extended s1")
	tk.MustQuery("select name, status from mysql.stats_extended where name = 's1'").Sort().Check(testkit.Rows(
		"s1 2",
	))
	result = tk.MustQuery("show stats_extended where db_name = 'test' and table_name = 't'")
	require.Len(t, result.Rows(), 0)
	tk.MustExec("alter table t add stats_extended s1 correlation(a,b)")
	tk.MustQuery("select name, status from mysql.stats_extended where name = 's1'").Sort().Check(testkit.Rows(
		"s1 0",
	))
	result = tk.MustQuery("show stats_extended where db_name = 'test' and table_name = 't'")
	require.Len(t, result.Rows(), 0)
	tk.MustExec("analyze table t")
	tk.MustQuery("select name, status from mysql.stats_extended where name = 's1'").Sort().Check(testkit.Rows(
		"s1 1",
	))
	result = tk.MustQuery("show stats_extended where db_name = 'test' and table_name = 't'")
	require.Len(t, result.Rows(), 1)
}

func TestDuplicateFMSketch(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	defer tk.MustExec("set @@tidb_partition_prune_mode='static'")
	tk.MustExec("create table t(a int, b int, c int) partition by hash(a) partitions 3")
	tk.MustExec("insert into t values (1, 1, 1)")
	tk.MustExec("analyze table t")
	tk.MustQuery("select count(*) from mysql.stats_fm_sketch").Check(testkit.Rows("9"))
	tk.MustExec("analyze table t")
	tk.MustQuery("select count(*) from mysql.stats_fm_sketch").Check(testkit.Rows("9"))

	tk.MustExec("alter table t drop column b")
	require.NoError(t, dom.StatsHandle().GCStats(dom.InfoSchema(), time.Duration(0)))
	tk.MustQuery("select count(*) from mysql.stats_fm_sketch").Check(testkit.Rows("6"))
}

func TestIndexFMSketch(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version = 1")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, index ia(a), index ibc(b, c)) partition by hash(a) partitions 3")
	tk.MustExec("insert into t values (1, 1, 1)")
	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	defer tk.MustExec("set @@tidb_partition_prune_mode='static'")
	tk.MustExec("analyze table t index ia")
	tk.MustQuery("select count(*) from mysql.stats_fm_sketch").Check(testkit.Rows("3"))
	tk.MustExec("analyze table t index ibc")
	tk.MustQuery("select count(*) from mysql.stats_fm_sketch").Check(testkit.Rows("6"))
	tk.MustExec("analyze table t")
	tk.MustQuery("select count(*) from mysql.stats_fm_sketch").Check(testkit.Rows("15"))
	tk.MustExec("drop table if exists t")
	require.NoError(t, dom.StatsHandle().GCStats(dom.InfoSchema(), 0))

	// clustered index
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@tidb_enable_clustered_index=ON")
	tk.MustExec("create table t (a datetime, b datetime, primary key (a)) partition by hash(year(a)) partitions 3")
	tk.MustExec("insert into t values ('2000-01-01', '2000-01-01')")
	tk.MustExec("analyze table t")
	tk.MustQuery("select count(*) from mysql.stats_fm_sketch").Check(testkit.Rows("6"))
	tk.MustExec("drop table if exists t")
	require.NoError(t, dom.StatsHandle().GCStats(dom.InfoSchema(), 0))

	// test NDV
	checkNDV := func(rows, ndv int) {
		tk.MustExec("analyze table t")
		rs := tk.MustQuery("select value from mysql.stats_fm_sketch").Rows()
		require.Len(t, rs, rows)
		for i := range rs {
			fm, err := statistics.DecodeFMSketch([]byte(rs[i][0].(string)))
			require.NoError(t, err)
			require.Equal(t, int64(ndv), fm.NDV())
		}
	}

	tk.MustExec("set @@tidb_enable_clustered_index=OFF")
	tk.MustExec("create table t(a int, key(a)) partition by hash(a) partitions 3")
	tk.MustExec("insert into t values (1), (2), (2), (3)")
	checkNDV(6, 1)
	tk.MustExec("insert into t values (4), (5), (6)")
	checkNDV(6, 2)
	tk.MustExec("insert into t values (2), (5)")
	checkNDV(6, 2)
	tk.MustExec("drop table if exists t")
	require.NoError(t, dom.StatsHandle().GCStats(dom.InfoSchema(), 0))

	// clustered index
	tk.MustExec("set @@tidb_enable_clustered_index=ON")
	tk.MustExec("create table t (a datetime, b datetime, primary key (a)) partition by hash(year(a)) partitions 3")
	tk.MustExec("insert into t values ('2000-01-01', '2001-01-01'), ('2001-01-01', '2001-01-01'), ('2002-01-01', '2001-01-01')")
	checkNDV(6, 1)
	tk.MustExec("insert into t values ('1999-01-01', '1998-01-01'), ('1997-01-02', '1999-01-02'), ('1998-01-03', '1999-01-03')")
	checkNDV(6, 2)
}

func TestShowExtendedStats4DropColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set session tidb_enable_extended_stats = on")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, c int)")
	tk.MustExec("insert into t values(1,1,1),(2,2,2),(3,3,3)")
	tk.MustExec("alter table t add stats_extended s1 correlation(a,b)")
	tk.MustExec("alter table t add stats_extended s2 correlation(b,c)")
	tk.MustExec("analyze table t")
	rows := tk.MustQuery("show stats_extended").Sort().Rows()
	require.Len(t, rows, 2)
	require.Equal(t, "s1", rows[0][2])
	require.Equal(t, "[a,b]", rows[0][3])
	require.Equal(t, "s2", rows[1][2])
	require.Equal(t, "[b,c]", rows[1][3])

	tk.MustExec("alter table t drop column b")
	rows = tk.MustQuery("show stats_extended").Rows()
	require.Len(t, rows, 0)

	// Previously registered extended stats should be invalid for re-created columns.
	tk.MustExec("alter table t add column b int")
	rows = tk.MustQuery("show stats_extended").Rows()
	require.Len(t, rows, 0)
}

func TestExtStatsOnReCreatedTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set session tidb_enable_extended_stats = on")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1,1),(2,2),(3,3)")
	tk.MustExec("alter table t add stats_extended s1 correlation(a,b)")
	tk.MustExec("analyze table t")
	rows := tk.MustQuery("select table_id, stats from mysql.stats_extended where name = 's1'").Rows()
	require.Len(t, rows, 1)
	tableID1 := rows[0][0]
	require.Equal(t, "1.000000", rows[0][1])
	rows = tk.MustQuery("show stats_extended where stats_name = 's1'").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "1.000000", rows[0][5])

	tk.MustExec("drop table t")
	rows = tk.MustQuery("show stats_extended where stats_name = 's1'").Rows()
	require.Len(t, rows, 0)

	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1,3),(2,2),(3,1)")
	tk.MustExec("alter table t add stats_extended s1 correlation(a,b)")
	tk.MustExec("analyze table t")
	rows = tk.MustQuery("select table_id, stats from mysql.stats_extended where name = 's1' order by stats").Rows()
	require.Len(t, rows, 2)
	tableID2 := rows[0][0]
	require.NotEqual(t, tableID1, tableID2)
	require.Equal(t, tableID1, rows[1][0])
	require.Equal(t, "-1.000000", rows[0][1])
	require.Equal(t, "1.000000", rows[1][1])
	rows = tk.MustQuery("show stats_extended where stats_name = 's1'").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "-1.000000", rows[0][5])
}

func TestExtStatsOnReCreatedColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set session tidb_enable_extended_stats = on")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1,1),(2,2),(3,3)")
	tk.MustExec("alter table t add stats_extended s1 correlation(a,b)")
	tk.MustExec("analyze table t")
	tk.MustQuery("select column_ids, stats from mysql.stats_extended where name = 's1'").Check(testkit.Rows(
		"[1,2] 1.000000",
	))
	rows := tk.MustQuery("show stats_extended where stats_name = 's1'").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "[a,b]", rows[0][3])
	require.Equal(t, "1.000000", rows[0][5])

	tk.MustExec("alter table t drop column b")
	tk.MustExec("alter table t add column b int")
	tk.MustQuery("select * from t").Sort().Check(testkit.Rows(
		"1 <nil>",
		"2 <nil>",
		"3 <nil>",
	))
	tk.MustExec("update t set b = 3 where a = 1")
	tk.MustExec("update t set b = 2 where a = 2")
	tk.MustExec("update t set b = 1 where a = 3")
	tk.MustQuery("select * from t").Sort().Check(testkit.Rows(
		"1 3",
		"2 2",
		"3 1",
	))
	tk.MustExec("analyze table t")
	// Previous extended stats would not be collected and would not take effect anymore, it will be removed by stats GC.
	tk.MustQuery("select column_ids, stats from mysql.stats_extended where name = 's1'").Check(testkit.Rows(
		"[1,2] 1.000000",
	))
	rows = tk.MustQuery("show stats_extended where stats_name = 's1'").Rows()
	require.Len(t, rows, 0)
}

func TestExtStatsOnRenamedColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set session tidb_enable_extended_stats = on")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1,1),(2,2),(3,3)")
	tk.MustExec("alter table t add stats_extended s1 correlation(a,b)")
	tk.MustExec("analyze table t")
	tk.MustQuery("select column_ids, stats from mysql.stats_extended where name = 's1'").Check(testkit.Rows(
		"[1,2] 1.000000",
	))
	rows := tk.MustQuery("show stats_extended where stats_name = 's1'").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "[a,b]", rows[0][3])
	require.Equal(t, "1.000000", rows[0][5])

	tk.MustExec("alter table t rename column b to c")
	tk.MustExec("update t set c = 3 where a = 1")
	tk.MustExec("update t set c = 2 where a = 2")
	tk.MustExec("update t set c = 1 where a = 3")
	tk.MustQuery("select * from t").Sort().Check(testkit.Rows(
		"1 3",
		"2 2",
		"3 1",
	))
	tk.MustExec("analyze table t")
	// Previous extended stats would still be collected and take effect.
	tk.MustQuery("select column_ids, stats from mysql.stats_extended where name = 's1'").Check(testkit.Rows(
		"[1,2] -1.000000",
	))
	rows = tk.MustQuery("show stats_extended where stats_name = 's1'").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "[a,c]", rows[0][3])
	require.Equal(t, "-1.000000", rows[0][5])
}

func TestExtStatsOnModifiedColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set session tidb_enable_extended_stats = on")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1,1),(2,2),(3,3)")
	tk.MustExec("alter table t add stats_extended s1 correlation(a,b)")
	tk.MustExec("analyze table t")
	tk.MustQuery("select column_ids, stats from mysql.stats_extended where name = 's1'").Check(testkit.Rows(
		"[1,2] 1.000000",
	))
	rows := tk.MustQuery("show stats_extended where stats_name = 's1'").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "[a,b]", rows[0][3])
	require.Equal(t, "1.000000", rows[0][5])

	tk.MustExec("alter table t modify column b bigint")
	tk.MustExec("update t set b = 3 where a = 1")
	tk.MustExec("update t set b = 2 where a = 2")
	tk.MustExec("update t set b = 1 where a = 3")
	tk.MustQuery("select * from t").Sort().Check(testkit.Rows(
		"1 3",
		"2 2",
		"3 1",
	))
	tk.MustExec("analyze table t")
	// Previous extended stats would still be collected and take effect.
	tk.MustQuery("select column_ids, stats from mysql.stats_extended where name = 's1'").Check(testkit.Rows(
		"[1,2] -1.000000",
	))
	rows = tk.MustQuery("show stats_extended where stats_name = 's1'").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "[a,b]", rows[0][3])
	require.Equal(t, "-1.000000", rows[0][5])
}

func TestCorrelationWithDefinedCollate(t *testing.T) {
	store := testkit.CreateMockStore(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t(a int primary key, b varchar(8) character set utf8mb4 collate utf8mb4_general_ci, c varchar(8) character set utf8mb4 collate utf8mb4_bin)")
	testKit.MustExec("insert into t values(1,'aa','aa'),(2,'Cb','Cb'),(3,'CC','CC')")
	testKit.MustExec("analyze table t")
	testKit.MustQuery("select a from t order by b").Check(testkit.Rows(
		"1",
		"2",
		"3",
	))
	testKit.MustQuery("select a from t order by c").Check(testkit.Rows(
		"3",
		"2",
		"1",
	))
	rows := testKit.MustQuery("show stats_histograms where table_name = 't'").Sort().Rows()
	require.Len(t, rows, 3)
	require.Equal(t, "1", rows[1][9])
	require.Equal(t, "-1", rows[2][9])
	testKit.MustExec("set session tidb_enable_extended_stats = on")
	testKit.MustExec("alter table t add stats_extended s1 correlation(b,c)")
	testKit.MustExec("analyze table t")
	rows = testKit.MustQuery("show stats_extended where stats_name = 's1'").Sort().Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "[b,c]", rows[0][3])
	require.Equal(t, "-1.000000", rows[0][5])
}

func TestLoadHistogramWithCollate(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t(a varchar(10) collate utf8mb4_unicode_ci);")
	testKit.MustExec("insert into t values('abcdefghij');")
	testKit.MustExec("insert into t values('abcdufghij');")
	testKit.MustExec("analyze table t with 0 topn;")
	do := dom
	h := do.StatsHandle()
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	_, err = h.TableStatsFromStorage(tblInfo, tblInfo.ID, true, 0)
	require.NoError(t, err)
}

func TestStatsCacheUpdateSkip(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	do := dom
	h := do.StatsHandle()
	testKit.MustExec("use test")
	testKit.MustExec("create table t (c1 int, c2 int)")
	testKit.MustExec("insert into t values(1, 2)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	testKit.MustExec("analyze table t")
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	statsTbl1 := h.GetTableStats(tableInfo)
	require.False(t, statsTbl1.Pseudo)
	h.Update(is)
	statsTbl2 := h.GetTableStats(tableInfo)
	require.Equal(t, statsTbl2, statsTbl1)
}

func testIncrementalModifyCountUpdateHelper(analyzeSnapshot bool) func(*testing.T) {
	return func(t *testing.T) {
		store, dom := testkit.CreateMockStoreAndDomain(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		if analyzeSnapshot {
			tk.MustExec("set @@session.tidb_enable_analyze_snapshot = on")
		} else {
			tk.MustExec("set @@session.tidb_enable_analyze_snapshot = off")
		}
		tk.MustExec("create table t(a int)")
		tk.MustExec("set @@session.tidb_analyze_version = 2")
		h := dom.StatsHandle()
		err := h.HandleDDLEvent(<-h.DDLEventCh())
		require.NoError(t, err)
		tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
		require.NoError(t, err)
		tblInfo := tbl.Meta()
		tid := tblInfo.ID

		tk.MustExec("insert into t values(1),(2),(3)")
		require.NoError(t, h.DumpStatsDeltaToKV(true))
		err = h.Update(dom.InfoSchema())
		require.NoError(t, err)
		tk.MustExec("analyze table t")
		tk.MustQuery(fmt.Sprintf("select count, modify_count from mysql.stats_meta where table_id = %d", tid)).Check(testkit.Rows(
			"3 0",
		))

		tk.MustExec("begin")
		txn, err := tk.Session().Txn(false)
		require.NoError(t, err)
		startTS := txn.StartTS()
		tk.MustExec("commit")

		tk.MustExec("insert into t values(4),(5),(6)")
		require.NoError(t, h.DumpStatsDeltaToKV(true))
		err = h.Update(dom.InfoSchema())
		require.NoError(t, err)

		// Simulate that the analyze would start before and finish after the second insert.
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/injectAnalyzeSnapshot", fmt.Sprintf("return(%d)", startTS)))
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/injectBaseCount", "return(3)"))
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/injectBaseModifyCount", "return(0)"))
		tk.MustExec("analyze table t")
		if analyzeSnapshot {
			// Check the count / modify_count changes during the analyze are not lost.
			tk.MustQuery(fmt.Sprintf("select count, modify_count from mysql.stats_meta where table_id = %d", tid)).Check(testkit.Rows(
				"6 3",
			))
			// Check the histogram is correct for the snapshot analyze.
			tk.MustQuery(fmt.Sprintf("select distinct_count from mysql.stats_histograms where table_id = %d", tid)).Check(testkit.Rows(
				"3",
			))
		} else {
			// Since analyze use max ts to read data, it finds the row count is 6 and directly set count to 6 rather than incrementally update it.
			// But it still incrementally updates modify_count.
			tk.MustQuery(fmt.Sprintf("select count, modify_count from mysql.stats_meta where table_id = %d", tid)).Check(testkit.Rows(
				"6 3",
			))
			// Check the histogram is collected from the latest data rather than the snapshot at startTS.
			tk.MustQuery(fmt.Sprintf("select distinct_count from mysql.stats_histograms where table_id = %d", tid)).Check(testkit.Rows(
				"6",
			))
		}
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/injectAnalyzeSnapshot"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/injectBaseCount"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/injectBaseModifyCount"))
	}
}

func TestIncrementalModifyCountUpdate(t *testing.T) {
	for _, analyzeSnapshot := range []bool{true, false} {
		t.Run(fmt.Sprintf("%s-%t", t.Name(), analyzeSnapshot), testIncrementalModifyCountUpdateHelper(analyzeSnapshot))
	}
}

func TestRecordHistoricalStatsToStorage(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_analyze_version = 2")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b varchar(10))")
	tk.MustExec("insert into t value(1, 'aaa'), (3, 'aab'), (5, 'bba'), (2, 'bbb'), (4, 'cca'), (6, 'ccc')")
	// mark column stats as needed
	tk.MustExec("select * from t where a = 3")
	tk.MustExec("select * from t where b = 'bbb'")
	tk.MustExec("alter table t add index single(a)")
	tk.MustExec("alter table t add index multi(a, b)")
	tk.MustExec("analyze table t with 2 topn")

	tableInfo, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	version, err := dom.StatsHandle().RecordHistoricalStatsToStorage("t", tableInfo.Meta(), tableInfo.Meta().ID, false)
	require.NoError(t, err)

	rows := tk.MustQuery(fmt.Sprintf("select count(*) from mysql.stats_history where version = '%d'", version)).Rows()
	num, _ := strconv.Atoi(rows[0][0].(string))
	require.GreaterOrEqual(t, num, 1)
}

func TestEvictedColumnLoadedStatus(t *testing.T) {
	t.Skip("skip this test because it is useless")
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Performance.EnableStatsCacheMemQuota = true
	})
	store, dom := testkit.CreateMockStoreAndDomain(t)
	dom.StatsHandle().SetLease(0)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_analyze_version = 1")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("analyze table test.t")
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.Nil(t, err)
	tblStats := domain.GetDomain(tk.Session()).StatsHandle().GetTableStats(tbl.Meta())
	for _, col := range tblStats.Columns {
		require.True(t, col.IsStatsInitialized())
	}

	domain.GetDomain(tk.Session()).StatsHandle().SetStatsCacheCapacity(1)
	tblStats = domain.GetDomain(tk.Session()).StatsHandle().GetTableStats(tbl.Meta())
	for _, col := range tblStats.Columns {
		require.True(t, col.IsStatsInitialized())
	}
}

func TestUninitializedStatsStatus(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	dom.StatsHandle().SetLease(0)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, index idx_a(a))")
	h := dom.StatsHandle()
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	tk.MustExec("insert into t values (1,2,2), (3,4,4), (5,6,6), (7,8,8), (9,10,10)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	is := dom.InfoSchema()
	require.NoError(t, h.Update(is))
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	tblStats := h.GetTableStats(tblInfo)
	for _, col := range tblStats.Columns {
		require.False(t, col.IsStatsInitialized())
	}
	for _, idx := range tblStats.Indices {
		require.False(t, idx.IsStatsInitialized())
	}
	tk.MustQuery("show stats_histograms where db_name = 'test' and table_name = 't'").Check(testkit.Rows())
	checkStatsPseudo := func() {
		rows := tk.MustQuery("explain select * from t").Rows()
		operatorInfo := rows[len(rows)-1][4].(string)
		require.True(t, strings.Contains(operatorInfo, "stats:pseudo"))
	}
	tk.MustExec("set @@tidb_enable_pseudo_for_outdated_stats = true")
	checkStatsPseudo()
	tk.MustExec("set @@tidb_enable_pseudo_for_outdated_stats = false")
	checkStatsPseudo()
}

func TestIssue39336(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`
create table t1 (
    a datetime(3) default null,
    b int
) partition by range (b) (
    partition p0 values less than (1000),
    partition p1 values less than (maxvalue)
)`)
	tk.MustExec("set @@sql_mode=''")
	tk.MustExec("set @@tidb_analyze_version=2")
	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	tk.MustExec(`
insert into t1 values
('1000-00-09 00:00:00.000',    1),
('1000-00-06 00:00:00.000',    1),
('1000-00-06 00:00:00.000',    1),
('2022-11-23 14:24:30.000',    1),
('2022-11-23 14:24:32.000',    1),
('2022-11-23 14:24:33.000',    1),
('2022-11-23 14:24:35.000',    1),
('2022-11-23 14:25:08.000', 1001),
('2022-11-23 14:25:09.000', 1001)`)
	tk.MustExec("analyze table t1 with 0 topn")
	rows := tk.MustQuery("show analyze status where job_info like 'merge global stats%'").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "finished", rows[0][7])
}

func checkAllEvicted(t *testing.T, statsTbl *statistics.Table) {
	for _, col := range statsTbl.Columns {
		require.True(t, col.IsAllEvicted())
	}
	for _, idx := range statsTbl.Indices {
		require.True(t, idx.IsAllEvicted())
	}
}

func TestInitStatsLite(t *testing.T) {
	oriVal := config.GetGlobalConfig().Performance.LiteInitStats
	config.GetGlobalConfig().Performance.LiteInitStats = true
	defer func() {
		config.GetGlobalConfig().Performance.LiteInitStats = oriVal
	}()

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, c int, primary key(a), key idxb(b), key idxc(c))")
	tk.MustExec("insert into t values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9)")

	h := dom.StatsHandle()
	// set lease > 0 to trigger on-demand stats load.
	h.SetLease(time.Millisecond)
	defer func() {
		h.SetLease(0)
	}()

	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	colBID := tblInfo.Columns[1].ID
	colCID := tblInfo.Columns[2].ID
	idxBID := tblInfo.Indices[0].ID
	idxCID := tblInfo.Indices[1].ID

	tk.MustExec("analyze table t with 2 topn, 2 buckets")
	statsTbl0 := h.GetTableStats(tblInfo)
	checkAllEvicted(t, statsTbl0)

	h.Clear()
	require.NoError(t, h.InitStatsLite(is))
	statsTbl1 := h.GetTableStats(tblInfo)
	checkAllEvicted(t, statsTbl1)
	{
		// internal.AssertTableEqual(t, statsTbl0, statsTbl1)
		// statsTbl0 is loaded when the cache has pseudo table.
		// TODO: We haven't optimize the pseudo table's memory usage yet. So here the two will be different.
		require.True(t, len(statsTbl0.Columns) > 0)
		require.True(t, len(statsTbl0.Indices) > 0)
		require.True(t, len(statsTbl1.Columns) == 0)
		require.True(t, len(statsTbl1.Indices) == 0)
	}

	// async stats load
	tk.MustExec("set @@tidb_stats_load_sync_wait = 0")
	tk.MustExec("explain select * from t where b > 1")
	require.NoError(t, h.LoadNeededHistograms())
	statsTbl2 := h.GetTableStats(tblInfo)
	colBStats1 := statsTbl2.Columns[colBID]
	colCStats := statsTbl2.Columns[colCID]
	require.True(t, colBStats1.IsFullLoad())
	idxBStats1 := statsTbl2.Indices[idxBID]
	require.True(t, idxBStats1.IsFullLoad())
	require.True(t, colCStats.IsAllEvicted())

	// sync stats load
	tk.MustExec("set @@tidb_stats_load_sync_wait = 60000")
	tk.MustExec("explain select * from t where c > 1")
	statsTbl3 := h.GetTableStats(tblInfo)
	colCStats1 := statsTbl3.Columns[colCID]
	require.True(t, colCStats1.IsFullLoad())
	idxCStats1 := statsTbl3.Indices[idxCID]
	require.True(t, idxCStats1.IsFullLoad())

	// update stats
	tk.MustExec("analyze table t with 1 topn, 3 buckets")
	statsTbl4 := h.GetTableStats(tblInfo)
	colBStats2 := statsTbl4.Columns[colBID]
	require.True(t, colBStats2.IsFullLoad())
	require.Greater(t, colBStats2.LastUpdateVersion, colBStats1.LastUpdateVersion)
	idxBStats2 := statsTbl4.Indices[idxBID]
	require.True(t, idxBStats2.IsFullLoad())
	require.Greater(t, idxBStats2.LastUpdateVersion, idxBStats1.LastUpdateVersion)
	colCStats2 := statsTbl4.Columns[colCID]
	require.True(t, colCStats2.IsFullLoad())
	require.Greater(t, colCStats2.LastUpdateVersion, colCStats1.LastUpdateVersion)
	idxCStats2 := statsTbl4.Indices[idxCID]
	require.True(t, idxCStats2.IsFullLoad())
	require.Greater(t, idxCStats2.LastUpdateVersion, idxCStats1.LastUpdateVersion)
}

func TestSkipMissingPartitionStats(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec("set @@tidb_skip_missing_partition_stats = 1")
	tk.MustExec("create table t (a int, b int, c int, index idx_b(b)) partition by range (a) (partition p0 values less than (100), partition p1 values less than (200), partition p2 values less than (300))")
	tk.MustExec("insert into t values (1,1,1), (2,2,2), (101,101,101), (102,102,102), (201,201,201), (202,202,202)")
	h := dom.StatsHandle()
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	tk.MustExec("analyze table t partition p0, p1")
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	globalStats := h.GetTableStats(tblInfo)
	require.Equal(t, 6, int(globalStats.RealtimeCount))
	require.Equal(t, 2, int(globalStats.ModifyCount))
	for _, col := range globalStats.Columns {
		require.True(t, col.IsStatsInitialized())
	}
	for _, idx := range globalStats.Indices {
		require.True(t, idx.IsStatsInitialized())
	}
}
