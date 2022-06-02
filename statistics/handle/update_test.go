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

package handle_test

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/ranger"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestSingleSessionInsert(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("set @@session.tidb_analyze_version = 1")
	testKit.MustExec("create table t1 (c1 int, c2 int)")
	testKit.MustExec("create table t2 (c1 int, c2 int)")

	rowCount1 := 10
	rowCount2 := 20
	for i := 0; i < rowCount1; i++ {
		testKit.MustExec("insert into t1 values(1, 2)")
	}
	for i := 0; i < rowCount2; i++ {
		testKit.MustExec("insert into t2 values(1, 2)")
	}

	is := dom.InfoSchema()
	tbl1, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	tableInfo1 := tbl1.Meta()
	h := dom.StatsHandle()

	err = h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)

	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.Update(is))
	stats1 := h.GetTableStats(tableInfo1)
	require.Equal(t, int64(rowCount1), stats1.Count)

	tbl2, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	require.NoError(t, err)
	tableInfo2 := tbl2.Meta()
	stats2 := h.GetTableStats(tableInfo2)
	require.Equal(t, int64(rowCount2), stats2.Count)

	testKit.MustExec("analyze table t1")
	// Test update in a txn.
	for i := 0; i < rowCount1; i++ {
		testKit.MustExec("insert into t1 values(1, 2)")
	}
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.Update(is))
	stats1 = h.GetTableStats(tableInfo1)
	require.Equal(t, int64(rowCount1*2), stats1.Count)

	// Test IncreaseFactor.
	count, err := stats1.ColumnEqualRowCount(testKit.Session(), types.NewIntDatum(1), tableInfo1.Columns[0].ID)
	require.NoError(t, err)
	require.Equal(t, float64(rowCount1*2), count)

	testKit.MustExec("begin")
	for i := 0; i < rowCount1; i++ {
		testKit.MustExec("insert into t1 values(1, 2)")
	}
	testKit.MustExec("commit")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.Update(is))
	stats1 = h.GetTableStats(tableInfo1)
	require.Equal(t, int64(rowCount1*3), stats1.Count)

	testKit.MustExec("begin")
	for i := 0; i < rowCount1; i++ {
		testKit.MustExec("insert into t1 values(1, 2)")
	}
	for i := 0; i < rowCount1; i++ {
		testKit.MustExec("delete from t1 limit 1")
	}
	for i := 0; i < rowCount2; i++ {
		testKit.MustExec("update t2 set c2 = c1")
	}
	testKit.MustExec("commit")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.Update(is))
	stats1 = h.GetTableStats(tableInfo1)
	require.Equal(t, int64(rowCount1*3), stats1.Count)
	stats2 = h.GetTableStats(tableInfo2)
	require.Equal(t, int64(rowCount2), stats2.Count)

	testKit.MustExec("begin")
	testKit.MustExec("delete from t1")
	testKit.MustExec("commit")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.Update(is))
	stats1 = h.GetTableStats(tableInfo1)
	require.Equal(t, int64(0), stats1.Count)

	rs := testKit.MustQuery("select modify_count from mysql.stats_meta")
	rs.Check(testkit.Rows("40", "70"))

	rs = testKit.MustQuery("select tot_col_size from mysql.stats_histograms").Sort()
	rs.Check(testkit.Rows("0", "0", "20", "20"))

	// test dump delta only when `modify count / count` is greater than the ratio.
	originValue := handle.DumpStatsDeltaRatio
	handle.DumpStatsDeltaRatio = 0.5
	defer func() {
		handle.DumpStatsDeltaRatio = originValue
	}()
	handle.DumpStatsDeltaRatio = 0.5
	for i := 0; i < rowCount1; i++ {
		testKit.MustExec("insert into t1 values (1,2)")
	}
	err = h.DumpStatsDeltaToKV(handle.DumpDelta)
	require.NoError(t, err)
	require.NoError(t, h.Update(is))
	stats1 = h.GetTableStats(tableInfo1)
	require.Equal(t, int64(rowCount1), stats1.Count)

	// not dumped
	testKit.MustExec("insert into t1 values (1,2)")
	err = h.DumpStatsDeltaToKV(handle.DumpDelta)
	require.NoError(t, err)
	require.NoError(t, h.Update(is))
	stats1 = h.GetTableStats(tableInfo1)
	require.Equal(t, int64(rowCount1), stats1.Count)

	h.FlushStats()
	require.NoError(t, h.Update(is))
	stats1 = h.GetTableStats(tableInfo1)
	require.Equal(t, int64(rowCount1+1), stats1.Count)
}

func TestRollback(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (a int, b int)")
	testKit.MustExec("begin")
	testKit.MustExec("insert into t values (1,2)")
	testKit.MustExec("rollback")

	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	h := dom.StatsHandle()
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.Update(is))

	stats := h.GetTableStats(tableInfo)
	require.Equal(t, int64(0), stats.Count)
	require.Equal(t, int64(0), stats.ModifyCount)
}

func TestMultiSession(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t1 (c1 int, c2 int)")

	rowCount1 := 10
	for i := 0; i < rowCount1; i++ {
		testKit.MustExec("insert into t1 values(1, 2)")
	}

	testKit1 := testkit.NewTestKit(t, store)
	for i := 0; i < rowCount1; i++ {
		testKit1.MustExec("insert into test.t1 values(1, 2)")
	}
	testKit2 := testkit.NewTestKit(t, store)
	for i := 0; i < rowCount1; i++ {
		testKit2.MustExec("delete from test.t1 limit 1")
	}
	is := dom.InfoSchema()
	tbl1, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	tableInfo1 := tbl1.Meta()
	h := dom.StatsHandle()

	err = h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)

	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.Update(is))
	stats1 := h.GetTableStats(tableInfo1)
	require.Equal(t, int64(rowCount1), stats1.Count)

	for i := 0; i < rowCount1; i++ {
		testKit.MustExec("insert into t1 values(1, 2)")
	}

	for i := 0; i < rowCount1; i++ {
		testKit1.MustExec("insert into test.t1 values(1, 2)")
	}

	for i := 0; i < rowCount1; i++ {
		testKit2.MustExec("delete from test.t1 limit 1")
	}

	testKit.Session().Close()
	testKit2.Session().Close()

	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.Update(is))
	stats1 = h.GetTableStats(tableInfo1)
	require.Equal(t, int64(rowCount1*2), stats1.Count)
	testKit.RefreshSession()
	rs := testKit.MustQuery("select modify_count from mysql.stats_meta")
	rs.Check(testkit.Rows("60"))
}

func TestTxnWithFailure(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t1 (c1 int primary key, c2 int)")

	is := dom.InfoSchema()
	tbl1, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	tableInfo1 := tbl1.Meta()
	h := dom.StatsHandle()

	err = h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)

	rowCount1 := 10
	testKit.MustExec("begin")
	for i := 0; i < rowCount1; i++ {
		testKit.MustExec("insert into t1 values(?, 2)", i)
	}
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.Update(is))
	stats1 := h.GetTableStats(tableInfo1)
	// have not commit
	require.Equal(t, int64(0), stats1.Count)
	testKit.MustExec("commit")

	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.Update(is))
	stats1 = h.GetTableStats(tableInfo1)
	require.Equal(t, int64(rowCount1), stats1.Count)

	_, err = testKit.Exec("insert into t1 values(0, 2)")
	require.Error(t, err)

	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.Update(is))
	stats1 = h.GetTableStats(tableInfo1)
	require.Equal(t, int64(rowCount1), stats1.Count)

	testKit.MustExec("insert into t1 values(-1, 2)")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.Update(is))
	stats1 = h.GetTableStats(tableInfo1)
	require.Equal(t, int64(rowCount1+1), stats1.Count)
}

func TestUpdatePartition(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)
	testKit.MustQuery("select @@tidb_partition_prune_mode").Check(testkit.Rows(string(dom.StatsHandle().CurrentPruneMode())))
	testKit.MustExec("use test")
	testkit.WithPruneMode(testKit, variable.Static, func() {
		err := dom.StatsHandle().RefreshVars()
		require.NoError(t, err)
		testKit.MustExec("drop table if exists t")
		createTable := `CREATE TABLE t (a int, b char(5)) PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (6),PARTITION p1 VALUES LESS THAN (11))`
		testKit.MustExec(createTable)
		do := dom
		is := do.InfoSchema()
		tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
		require.NoError(t, err)
		tableInfo := tbl.Meta()
		h := do.StatsHandle()
		err = h.HandleDDLEvent(<-h.DDLEventCh())
		require.NoError(t, err)
		pi := tableInfo.GetPartitionInfo()
		require.Len(t, pi.Definitions, 2)
		bColID := tableInfo.Columns[1].ID

		testKit.MustExec(`insert into t values (1, "a"), (7, "a")`)
		require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
		require.NoError(t, h.Update(is))
		for _, def := range pi.Definitions {
			statsTbl := h.GetPartitionStats(tableInfo, def.ID)
			require.Equal(t, int64(1), statsTbl.ModifyCount)
			require.Equal(t, int64(1), statsTbl.Count)
			require.Equal(t, int64(2), statsTbl.Columns[bColID].TotColSize)
		}

		testKit.MustExec(`update t set a = a + 1, b = "aa"`)
		require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
		require.NoError(t, h.Update(is))
		for _, def := range pi.Definitions {
			statsTbl := h.GetPartitionStats(tableInfo, def.ID)
			require.Equal(t, int64(2), statsTbl.ModifyCount)
			require.Equal(t, int64(1), statsTbl.Count)
			require.Equal(t, int64(3), statsTbl.Columns[bColID].TotColSize)
		}

		testKit.MustExec("delete from t")
		require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
		require.NoError(t, h.Update(is))
		for _, def := range pi.Definitions {
			statsTbl := h.GetPartitionStats(tableInfo, def.ID)
			require.Equal(t, int64(3), statsTbl.ModifyCount)
			require.Equal(t, int64(0), statsTbl.Count)
			require.Equal(t, int64(0), statsTbl.Columns[bColID].TotColSize)
		}
		// assert WithGetTableStatsByQuery get the same result
		for _, def := range pi.Definitions {
			statsTbl := h.GetPartitionStats(tableInfo, def.ID, handle.WithTableStatsByQuery())
			require.Equal(t, int64(3), statsTbl.ModifyCount)
			require.Equal(t, int64(0), statsTbl.Count)
			require.Equal(t, int64(0), statsTbl.Columns[bColID].TotColSize)
		}
	})
}

func TestAutoUpdate(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)
	testkit.WithPruneMode(testKit, variable.Static, func() {
		testKit.MustExec("use test")
		testKit.MustExec("create table t (a varchar(20))")

		handle.AutoAnalyzeMinCnt = 0
		testKit.MustExec("set global tidb_auto_analyze_ratio = 0.2")
		defer func() {
			handle.AutoAnalyzeMinCnt = 1000
			testKit.MustExec("set global tidb_auto_analyze_ratio = 0.0")
		}()

		do := dom
		is := do.InfoSchema()
		tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
		require.NoError(t, err)
		tableInfo := tbl.Meta()
		h := do.StatsHandle()

		err = h.HandleDDLEvent(<-h.DDLEventCh())
		require.NoError(t, err)
		require.NoError(t, h.Update(is))
		stats := h.GetTableStats(tableInfo)
		require.Equal(t, int64(0), stats.Count)

		_, err = testKit.Exec("insert into t values ('ss'), ('ss'), ('ss'), ('ss'), ('ss')")
		require.NoError(t, err)
		require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
		require.NoError(t, h.Update(is))
		h.HandleAutoAnalyze(is)
		require.NoError(t, h.Update(is))
		stats = h.GetTableStats(tableInfo)
		require.Equal(t, int64(5), stats.Count)
		require.Equal(t, int64(0), stats.ModifyCount)
		for _, item := range stats.Columns {
			// TotColSize = 5*(2(length of 'ss') + 1(size of len byte)).
			require.Equal(t, int64(15), item.TotColSize)
			break
		}

		// Test that even if the table is recently modified, we can still analyze the table.
		h.SetLease(time.Second)
		defer func() { h.SetLease(0) }()
		_, err = testKit.Exec("insert into t values ('fff')")
		require.NoError(t, err)
		require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
		require.NoError(t, h.Update(is))
		h.HandleAutoAnalyze(is)
		require.NoError(t, h.Update(is))
		stats = h.GetTableStats(tableInfo)
		require.Equal(t, int64(6), stats.Count)
		require.Equal(t, int64(1), stats.ModifyCount)

		_, err = testKit.Exec("insert into t values ('fff')")
		require.NoError(t, err)
		require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
		require.NoError(t, h.Update(is))
		h.HandleAutoAnalyze(is)
		require.NoError(t, h.Update(is))
		stats = h.GetTableStats(tableInfo)
		require.Equal(t, int64(7), stats.Count)
		require.Equal(t, int64(0), stats.ModifyCount)

		_, err = testKit.Exec("insert into t values ('eee')")
		require.NoError(t, err)
		require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
		require.NoError(t, h.Update(is))
		h.HandleAutoAnalyze(is)
		require.NoError(t, h.Update(is))
		stats = h.GetTableStats(tableInfo)
		require.Equal(t, int64(8), stats.Count)
		// Modify count is non-zero means that we do not analyze the table.
		require.Equal(t, int64(1), stats.ModifyCount)
		for _, item := range stats.Columns {
			// TotColSize = 27, because the table has not been analyzed, and insert statement will add 3(length of 'eee') to TotColSize.
			require.Equal(t, int64(27), item.TotColSize)
			break
		}

		testKit.MustExec("analyze table t")
		_, err = testKit.Exec("create index idx on t(a)")
		require.NoError(t, err)
		is = do.InfoSchema()
		tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
		require.NoError(t, err)
		tableInfo = tbl.Meta()
		h.HandleAutoAnalyze(is)
		require.NoError(t, h.Update(is))
		stats = h.GetTableStats(tableInfo)
		require.Equal(t, int64(8), stats.Count)
		require.Equal(t, int64(0), stats.ModifyCount)
		hg, ok := stats.Indices[tableInfo.Indices[0].ID]
		require.True(t, ok)
		require.Equal(t, int64(3), hg.NDV)
		require.Equal(t, 0, hg.Len())
		require.Equal(t, 3, hg.TopN.Num())
	})
}

func TestAutoUpdatePartition(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)
	testkit.WithPruneMode(testKit, variable.Static, func() {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t")
		testKit.MustExec("create table t (a int) PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (6))")
		testKit.MustExec("analyze table t")

		handle.AutoAnalyzeMinCnt = 0
		testKit.MustExec("set global tidb_auto_analyze_ratio = 0.6")
		defer func() {
			handle.AutoAnalyzeMinCnt = 1000
			testKit.MustExec("set global tidb_auto_analyze_ratio = 0.0")
		}()

		do := dom
		is := do.InfoSchema()
		tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
		require.NoError(t, err)
		tableInfo := tbl.Meta()
		pi := tableInfo.GetPartitionInfo()
		h := do.StatsHandle()
		require.NoError(t, h.RefreshVars())

		require.NoError(t, h.Update(is))
		stats := h.GetPartitionStats(tableInfo, pi.Definitions[0].ID)
		require.Equal(t, int64(0), stats.Count)

		testKit.MustExec("insert into t values (1)")
		require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
		require.NoError(t, h.Update(is))
		h.HandleAutoAnalyze(is)
		stats = h.GetPartitionStats(tableInfo, pi.Definitions[0].ID)
		require.Equal(t, int64(1), stats.Count)
		require.Equal(t, int64(0), stats.ModifyCount)
	})
}

func TestAutoAnalyzeOnEmptyTable(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	oriStart := tk.MustQuery("select @@tidb_auto_analyze_start_time").Rows()[0][0].(string)
	oriEnd := tk.MustQuery("select @@tidb_auto_analyze_end_time").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_start_time='%v'", oriStart))
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_end_time='%v'", oriEnd))
	}()

	tt := time.Now().Add(-1 * time.Minute)
	h, m := tt.Hour(), tt.Minute()
	start, end := fmt.Sprintf("%02d:%02d +0000", h, m), fmt.Sprintf("%02d:%02d +0000", h, m)
	tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_start_time='%v'", start))
	tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_end_time='%v'", end))
	dom.StatsHandle().HandleAutoAnalyze(dom.InfoSchema())

	tk.MustExec("use test")
	tk.MustExec("create table t (a int, index idx(a))")
	// to pass the stats.Pseudo check in autoAnalyzeTable
	tk.MustExec("analyze table t")
	// to pass the AutoAnalyzeMinCnt check in autoAnalyzeTable
	tk.MustExec("insert into t values (1)" + strings.Repeat(", (1)", int(handle.AutoAnalyzeMinCnt)))
	require.NoError(t, dom.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, dom.StatsHandle().Update(dom.InfoSchema()))

	// test if it will be limited by the time range
	require.False(t, dom.StatsHandle().HandleAutoAnalyze(dom.InfoSchema()))

	tk.MustExec("set global tidb_auto_analyze_start_time='00:00 +0000'")
	tk.MustExec("set global tidb_auto_analyze_end_time='23:59 +0000'")
	require.True(t, dom.StatsHandle().HandleAutoAnalyze(dom.InfoSchema()))
}

func TestAutoAnalyzeOutOfSpecifiedTime(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	oriStart := tk.MustQuery("select @@tidb_auto_analyze_start_time").Rows()[0][0].(string)
	oriEnd := tk.MustQuery("select @@tidb_auto_analyze_end_time").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_start_time='%v'", oriStart))
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_end_time='%v'", oriEnd))
	}()

	tt := time.Now().Add(-1 * time.Minute)
	h, m := tt.Hour(), tt.Minute()
	start, end := fmt.Sprintf("%02d:%02d +0000", h, m), fmt.Sprintf("%02d:%02d +0000", h, m)
	tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_start_time='%v'", start))
	tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_end_time='%v'", end))
	dom.StatsHandle().HandleAutoAnalyze(dom.InfoSchema())

	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	// to pass the stats.Pseudo check in autoAnalyzeTable
	tk.MustExec("analyze table t")
	// to pass the AutoAnalyzeMinCnt check in autoAnalyzeTable
	tk.MustExec("insert into t values (1)" + strings.Repeat(", (1)", int(handle.AutoAnalyzeMinCnt)))
	require.NoError(t, dom.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, dom.StatsHandle().Update(dom.InfoSchema()))

	require.False(t, dom.StatsHandle().HandleAutoAnalyze(dom.InfoSchema()))
	tk.MustExec("analyze table t")

	tk.MustExec("alter table t add index ia(a)")
	require.False(t, dom.StatsHandle().HandleAutoAnalyze(dom.InfoSchema()))

	tk.MustExec("set global tidb_auto_analyze_start_time='00:00 +0000'")
	tk.MustExec("set global tidb_auto_analyze_end_time='23:59 +0000'")
	require.True(t, dom.StatsHandle().HandleAutoAnalyze(dom.InfoSchema()))
}

func TestIssue25700(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	oriStart := tk.MustQuery("select @@tidb_auto_analyze_start_time").Rows()[0][0].(string)
	oriEnd := tk.MustQuery("select @@tidb_auto_analyze_end_time").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_start_time='%v'", oriStart))
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_end_time='%v'", oriEnd))
	}()
	tk.MustExec("set global tidb_auto_analyze_start_time='00:00 +0000'")
	tk.MustExec("set global tidb_auto_analyze_end_time='23:59 +0000'")

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE `t` ( `ldecimal` decimal(32,4) DEFAULT NULL, `rdecimal` decimal(32,4) DEFAULT NULL, `gen_col` decimal(36,4) GENERATED ALWAYS AS (`ldecimal` + `rdecimal`) VIRTUAL, `col_timestamp` timestamp(3) NULL DEFAULT NULL ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("analyze table t")
	tk.MustExec("INSERT INTO `t` (`ldecimal`, `rdecimal`, `col_timestamp`) VALUES (2265.2200, 9843.4100, '1999-12-31 16:00:00')" + strings.Repeat(", (2265.2200, 9843.4100, '1999-12-31 16:00:00')", int(handle.AutoAnalyzeMinCnt)))
	require.NoError(t, dom.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, dom.StatsHandle().Update(dom.InfoSchema()))

	require.True(t, dom.StatsHandle().HandleAutoAnalyze(dom.InfoSchema()))
	require.Equal(t, "finished", tk.MustQuery("show analyze status").Rows()[1][7])
}

func TestAutoAnalyzeOnChangeAnalyzeVer(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, index idx(a))")
	tk.MustExec("insert into t values(1)")
	tk.MustExec("set @@global.tidb_analyze_version = 1")
	do := dom
	handle.AutoAnalyzeMinCnt = 0
	defer func() {
		handle.AutoAnalyzeMinCnt = 1000
	}()
	h := do.StatsHandle()
	err := h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	is := do.InfoSchema()
	err = h.UpdateSessionVar()
	require.NoError(t, err)
	require.NoError(t, h.Update(is))
	// Auto analyze when global ver is 1.
	h.HandleAutoAnalyze(is)
	require.NoError(t, h.Update(is))
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	statsTbl1 := h.GetTableStats(tbl.Meta())
	// Check that all the version of t's stats are 1.
	for _, col := range statsTbl1.Columns {
		require.Equal(t, int64(1), col.StatsVer)
	}
	for _, idx := range statsTbl1.Indices {
		require.Equal(t, int64(1), idx.StatsVer)
	}
	tk.MustExec("set @@global.tidb_analyze_version = 2")
	err = h.UpdateSessionVar()
	require.NoError(t, err)
	tk.MustExec("insert into t values(1), (2), (3), (4)")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.Update(is))
	// Auto analyze t whose version is 1 after setting global ver to 2.
	h.HandleAutoAnalyze(is)
	require.NoError(t, h.Update(is))
	statsTbl1 = h.GetTableStats(tbl.Meta())
	require.Equal(t, int64(5), statsTbl1.Count)
	// All of its statistics should still be version 1.
	for _, col := range statsTbl1.Columns {
		require.Equal(t, int64(1), col.StatsVer)
	}
	for _, idx := range statsTbl1.Indices {
		require.Equal(t, int64(1), idx.StatsVer)
	}
	// Add a new table after the analyze version set to 2.
	tk.MustExec("create table tt(a int, index idx(a))")
	tk.MustExec("insert into tt values(1), (2), (3), (4), (5)")
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	is = do.InfoSchema()
	tbl2, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("tt"))
	require.NoError(t, err)
	require.NoError(t, h.Update(is))
	h.HandleAutoAnalyze(is)
	require.NoError(t, h.Update(is))
	statsTbl2 := h.GetTableStats(tbl2.Meta())
	// Since it's a newly created table. Auto analyze should analyze it's statistics to version2.
	for _, idx := range statsTbl2.Indices {
		require.Equal(t, int64(2), idx.StatsVer)
	}
	for _, col := range statsTbl2.Columns {
		require.Equal(t, int64(2), col.StatsVer)
	}
	tk.MustExec("set @@global.tidb_analyze_version = 1")
}

func TestTableAnalyzed(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (a int)")
	testKit.MustExec("insert into t values (1)")

	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	h := dom.StatsHandle()

	require.NoError(t, h.Update(is))
	statsTbl := h.GetTableStats(tableInfo)
	require.False(t, handle.TableAnalyzed(statsTbl))

	testKit.MustExec("analyze table t")
	require.NoError(t, h.Update(is))
	statsTbl = h.GetTableStats(tableInfo)
	require.True(t, handle.TableAnalyzed(statsTbl))

	h.Clear()
	oriLease := h.Lease()
	// set it to non-zero so we will use load by need strategy
	h.SetLease(1)
	defer func() {
		h.SetLease(oriLease)
	}()
	require.NoError(t, h.Update(is))
	statsTbl = h.GetTableStats(tableInfo)
	require.True(t, handle.TableAnalyzed(statsTbl))
}

func TestUpdateErrorRate(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	h := dom.StatsHandle()
	is := dom.InfoSchema()
	h.SetLease(0)
	require.NoError(t, h.Update(is))
	oriProbability := statistics.FeedbackProbability.Load()
	oriMinLogCount := handle.MinLogScanCount.Load()
	oriErrorRate := handle.MinLogErrorRate.Load()
	defer func() {
		statistics.FeedbackProbability.Store(oriProbability)
		handle.MinLogScanCount.Store(oriMinLogCount)
		handle.MinLogErrorRate.Store(oriErrorRate)
	}()
	statistics.FeedbackProbability.Store(1)
	handle.MinLogScanCount.Store(0)
	handle.MinLogErrorRate.Store(0)

	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("set @@session.tidb_analyze_version = 0")
	testKit.MustExec("create table t (a bigint(64), b bigint(64), primary key(a), index idx(b))")
	err := h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)

	testKit.MustExec("insert into t values (1, 3)")

	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	testKit.MustExec("analyze table t")

	testKit.MustExec("insert into t values (2, 3)")
	testKit.MustExec("insert into t values (5, 3)")
	testKit.MustExec("insert into t values (8, 3)")
	testKit.MustExec("insert into t values (12, 3)")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	is = dom.InfoSchema()
	require.NoError(t, h.Update(is))

	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblInfo := table.Meta()
	tbl := h.GetTableStats(tblInfo)
	aID := tblInfo.Columns[0].ID
	bID := tblInfo.Indices[0].ID

	// The statistic table is outdated now.
	require.True(t, tbl.Columns[aID].NotAccurate())

	testKit.MustQuery("select * from t where a between 1 and 10")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.DumpStatsFeedbackToKV())
	require.NoError(t, h.HandleUpdateStats(is))
	h.UpdateErrorRate(is)
	require.NoError(t, h.Update(is))
	tbl = h.GetTableStats(tblInfo)

	// The error rate of this column is not larger than MaxErrorRate now.
	require.False(t, tbl.Columns[aID].NotAccurate())

	require.True(t, tbl.Indices[bID].NotAccurate())
	testKit.MustQuery("select * from t where b between 2 and 10")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.DumpStatsFeedbackToKV())
	require.NoError(t, h.HandleUpdateStats(is))
	h.UpdateErrorRate(is)
	require.NoError(t, h.Update(is))
	tbl = h.GetTableStats(tblInfo)
	require.False(t, tbl.Indices[bID].NotAccurate())
	require.Equal(t, int64(1), tbl.Indices[bID].QueryTotal)

	testKit.MustExec("analyze table t")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.Update(is))
	tbl = h.GetTableStats(tblInfo)
	require.Equal(t, int64(0), tbl.Indices[bID].QueryTotal)
}

func TestUpdatePartitionErrorRate(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	h := dom.StatsHandle()
	is := dom.InfoSchema()
	h.SetLease(0)
	require.NoError(t, h.Update(is))
	oriProbability := statistics.FeedbackProbability.Load()
	oriMinLogCount := handle.MinLogScanCount.Load()
	oriErrorRate := handle.MinLogErrorRate.Load()
	defer func() {
		statistics.FeedbackProbability.Store(oriProbability)
		handle.MinLogScanCount.Store(oriMinLogCount)
		handle.MinLogErrorRate.Store(oriErrorRate)
	}()
	statistics.FeedbackProbability.Store(1)
	handle.MinLogScanCount.Store(0)
	handle.MinLogErrorRate.Store(0)

	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec(`set @@tidb_partition_prune_mode='` + string(variable.Static) + `'`)
	testKit.MustExec("create table t (a bigint(64), primary key(a)) partition by range (a) (partition p0 values less than (30))")
	err := h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)

	testKit.MustExec("insert into t values (1)")

	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	testKit.MustExec("analyze table t")

	testKit.MustExec("insert into t values (2)")
	testKit.MustExec("insert into t values (5)")
	testKit.MustExec("insert into t values (8)")
	testKit.MustExec("insert into t values (12)")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	is = dom.InfoSchema()
	require.NoError(t, h.Update(is))

	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblInfo := table.Meta()
	pid := tblInfo.Partition.Definitions[0].ID
	tbl := h.GetPartitionStats(tblInfo, pid)
	aID := tblInfo.Columns[0].ID

	// The statistic table is outdated now.
	require.True(t, tbl.Columns[aID].NotAccurate())

	testKit.MustQuery("select * from t where a between 1 and 10")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.DumpStatsFeedbackToKV())
	require.NoError(t, h.HandleUpdateStats(is))
	h.UpdateErrorRate(is)
	require.NoError(t, h.Update(is))
	tbl = h.GetPartitionStats(tblInfo, pid)

	// Feedback will not take effect under partition table.
	require.True(t, tbl.Columns[aID].NotAccurate())
}

func appendBucket(h *statistics.Histogram, l, r int64) {
	lower, upper := types.NewIntDatum(l), types.NewIntDatum(r)
	h.AppendBucket(&lower, &upper, 0, 0)
}

func TestSplitRange(t *testing.T) {
	h := statistics.NewHistogram(0, 0, 0, 0, types.NewFieldType(mysql.TypeLong), 5, 0)
	appendBucket(h, 1, 1)
	appendBucket(h, 2, 5)
	appendBucket(h, 7, 7)
	appendBucket(h, 8, 8)
	appendBucket(h, 10, 13)

	tests := []struct {
		points  []int64
		exclude []bool
		result  string
	}{
		{
			points:  []int64{1, 1},
			exclude: []bool{false, false},
			result:  "[1,1]",
		},
		{
			points:  []int64{0, 1, 3, 8, 8, 20},
			exclude: []bool{true, false, true, false, true, false},
			result:  "(0,1],(3,7),[7,8),[8,8],(8,10),[10,20]",
		},
		{
			points:  []int64{8, 10, 20, 30},
			exclude: []bool{false, false, true, true},
			result:  "[8,10),[10,10],(20,30)",
		},
		{
			// test remove invalid range
			points:  []int64{8, 9},
			exclude: []bool{false, true},
			result:  "[8,9)",
		},
	}
	for _, test := range tests {
		ranges := make([]*ranger.Range, 0, len(test.points)/2)
		for i := 0; i < len(test.points); i += 2 {
			ranges = append(ranges, &ranger.Range{
				LowVal:      []types.Datum{types.NewIntDatum(test.points[i])},
				LowExclude:  test.exclude[i],
				HighVal:     []types.Datum{types.NewIntDatum(test.points[i+1])},
				HighExclude: test.exclude[i+1],
				Collators:   collate.GetBinaryCollatorSlice(1),
			})
		}
		ranges, _ = h.SplitRange(nil, ranges, false)
		var ranStrs []string
		for _, ran := range ranges {
			ranStrs = append(ranStrs, ran.String())
		}
		require.Equal(t, test.result, strings.Join(ranStrs, ","))
	}
}

func TestQueryFeedback(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("set @@session.tidb_analyze_version = 0")
	testKit.MustExec("create table t (a bigint(64), b bigint(64), primary key(a), index idx(b))")
	testKit.MustExec("insert into t values (1,2),(2,2),(4,5)")
	testKit.MustExec("analyze table t with 0 topn")
	testKit.MustExec("insert into t values (3,4)")

	h := dom.StatsHandle()
	oriProbability := statistics.FeedbackProbability.Load()
	oriNumber := statistics.MaxNumberOfRanges
	oriMinLogCount := handle.MinLogScanCount.Load()
	oriErrorRate := handle.MinLogErrorRate.Load()
	defer func() {
		statistics.FeedbackProbability.Store(oriProbability)
		statistics.MaxNumberOfRanges = oriNumber
		handle.MinLogScanCount.Store(oriMinLogCount)
		handle.MinLogErrorRate.Store(oriErrorRate)
	}()
	statistics.FeedbackProbability.Store(1)
	handle.MinLogScanCount.Store(0)
	handle.MinLogErrorRate.Store(0)
	tests := []struct {
		sql     string
		hist    string
		idxCols int
	}{
		{
			// test primary key feedback
			sql: "select * from t where t.a <= 5 order by a desc",
			hist: "column:1 ndv:4 totColSize:0\n" +
				"num: 1 lower_bound: -9223372036854775808 upper_bound: 2 repeats: 0 ndv: 0\n" +
				"num: 2 lower_bound: 2 upper_bound: 4 repeats: 0 ndv: 0\n" +
				"num: 1 lower_bound: 4 upper_bound: 4 repeats: 1 ndv: 0",
			idxCols: 0,
		},
		{
			// test index feedback by double read
			sql: "select * from t use index(idx) where t.b <= 5",
			hist: "index:1 ndv:2\n" +
				"num: 3 lower_bound: -inf upper_bound: 5 repeats: 0 ndv: 0\n" +
				"num: 1 lower_bound: 5 upper_bound: 5 repeats: 1 ndv: 0",
			idxCols: 1,
		},
		{
			// test index feedback by single read
			sql: "select b from t use index(idx) where t.b <= 5",
			hist: "index:1 ndv:2\n" +
				"num: 3 lower_bound: -inf upper_bound: 5 repeats: 0 ndv: 0\n" +
				"num: 1 lower_bound: 5 upper_bound: 5 repeats: 1 ndv: 0",
			idxCols: 1,
		},
	}
	is := dom.InfoSchema()
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	for i, test := range tests {
		testKit.MustQuery(test.sql)
		require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
		require.NoError(t, h.DumpStatsFeedbackToKV())
		require.NoError(t, h.HandleUpdateStats(dom.InfoSchema()))
		require.NoError(t, err)
		require.NoError(t, h.Update(is))
		tblInfo := table.Meta()
		tbl := h.GetTableStats(tblInfo)
		if test.idxCols == 0 {
			require.Equal(t, tests[i].hist, tbl.Columns[tblInfo.Columns[0].ID].ToString(0))
		} else {
			require.Equal(t, tests[i].hist, tbl.Indices[tblInfo.Indices[0].ID].ToString(1))
		}
	}

	// Feedback from limit executor may not be accurate.
	testKit.MustQuery("select * from t where t.a <= 5 limit 1")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	feedback := h.GetQueryFeedback()
	require.Equal(t, 0, feedback.Size)

	// Test only collect for max number of Ranges.
	statistics.MaxNumberOfRanges = 0
	for _, test := range tests {
		testKit.MustQuery(test.sql)
		require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
		feedback := h.GetQueryFeedback()
		require.Equal(t, 0, feedback.Size)
	}

	// Test collect feedback by probability.
	statistics.FeedbackProbability.Store(0)
	statistics.MaxNumberOfRanges = oriNumber
	for _, test := range tests {
		testKit.MustQuery(test.sql)
		require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
		feedback := h.GetQueryFeedback()
		require.Equal(t, 0, feedback.Size)
	}

	// Test that after drop stats, the feedback won't cause panic.
	statistics.FeedbackProbability.Store(1)
	for _, test := range tests {
		testKit.MustQuery(test.sql)
	}
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.DumpStatsFeedbackToKV())
	testKit.MustExec("drop stats t")
	require.NoError(t, h.HandleUpdateStats(dom.InfoSchema()))

	// Test that the outdated feedback won't cause panic.
	testKit.MustExec("analyze table t")
	for _, test := range tests {
		testKit.MustQuery(test.sql)
	}
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.DumpStatsFeedbackToKV())
	testKit.MustExec("drop table t")
	require.NoError(t, h.HandleUpdateStats(dom.InfoSchema()))
}

func TestQueryFeedbackForPartition(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("set @@session.tidb_analyze_version = 0")
	testKit.MustExec(`set @@tidb_partition_prune_mode='` + string(variable.Static) + `'`)
	testKit.MustExec(`create table t (a bigint(64), b bigint(64), primary key(a), index idx(b))
			    partition by range (a) (
			    partition p0 values less than (3),
			    partition p1 values less than (6))`)
	testKit.MustExec("insert into t values (1,2),(2,2),(3,4),(4,1),(5,6)")
	testKit.MustExec("analyze table t")

	oriProbability := statistics.FeedbackProbability.Load()
	oriMinLogCount := handle.MinLogScanCount.Load()
	oriErrorRate := handle.MinLogErrorRate.Load()
	defer func() {
		statistics.FeedbackProbability.Store(oriProbability)
		handle.MinLogScanCount.Store(oriMinLogCount)
		handle.MinLogErrorRate.Store(oriErrorRate)
	}()
	statistics.FeedbackProbability.Store(1)
	handle.MinLogScanCount.Store(0)
	handle.MinLogErrorRate.Store(0)

	h := dom.StatsHandle()
	// Feedback will not take effect under partition table.
	tests := []struct {
		sql     string
		hist    string
		idxCols int
	}{
		{
			// test primary key feedback
			sql: "select * from t where t.a <= 5",
			hist: "column:1 ndv:2 totColSize:2\n" +
				"num: 1 lower_bound: 1 upper_bound: 1 repeats: 1 ndv: 0\n" +
				"num: 1 lower_bound: 2 upper_bound: 2 repeats: 1 ndv: 0",
			idxCols: 0,
		},
		{
			// test index feedback by double read
			sql: "select * from t use index(idx) where t.b <= 5",
			hist: "index:1 ndv:1\n" +
				"num: 2 lower_bound: 2 upper_bound: 2 repeats: 2 ndv: 0",
			idxCols: 1,
		},
		{
			// test index feedback by single read
			sql: "select b from t use index(idx) where t.b <= 5",
			hist: "index:1 ndv:1\n" +
				"num: 2 lower_bound: 2 upper_bound: 2 repeats: 2 ndv: 0",
			idxCols: 1,
		},
	}
	is := dom.InfoSchema()
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblInfo := table.Meta()
	pi := tblInfo.GetPartitionInfo()
	require.NotNil(t, pi)

	// This test will check the result of partition p0.
	var pid int64
	for _, def := range pi.Definitions {
		if def.Name.L == "p0" {
			pid = def.ID
			break
		}
	}

	for i, test := range tests {
		testKit.MustQuery(test.sql)
		require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
		require.NoError(t, h.DumpStatsFeedbackToKV())
		require.NoError(t, h.HandleUpdateStats(dom.InfoSchema()))
		require.NoError(t, err)
		require.NoError(t, h.Update(is))
		tbl := h.GetPartitionStats(tblInfo, pid)
		if test.idxCols == 0 {
			require.Equal(t, tests[i].hist, tbl.Columns[tblInfo.Columns[0].ID].ToString(0))
		} else {
			require.Equal(t, tests[i].hist, tbl.Indices[tblInfo.Indices[0].ID].ToString(1))
		}
	}
	testKit.MustExec("drop table t")
}

func TestUpdateSystemTable(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (a int, b int)")
	testKit.MustExec("insert into t values (1,2)")
	testKit.MustExec("analyze table t")
	testKit.MustExec("analyze table mysql.stats_histograms")
	h := dom.StatsHandle()
	require.NoError(t, h.Update(dom.InfoSchema()))
	feedback := h.GetQueryFeedback()
	// We may have query feedback for system tables, but we do not need to store them.
	require.Equal(t, 0, feedback.Size)
}

func TestOutOfOrderUpdate(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (a int, b int)")
	testKit.MustExec("insert into t values (1,2)")

	do := dom
	is := do.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	h := do.StatsHandle()
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)

	// Simulate the case that another tidb has inserted some value, but delta info has not been dumped to kv yet.
	testKit.MustExec("insert into t values (2,2),(4,5)")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	testKit.MustExec(fmt.Sprintf("update mysql.stats_meta set count = 1 where table_id = %d", tableInfo.ID))

	testKit.MustExec("delete from t")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	testKit.MustQuery("select count from mysql.stats_meta").Check(testkit.Rows("1"))

	// Now another tidb has updated the delta info.
	testKit.MustExec(fmt.Sprintf("update mysql.stats_meta set count = 3 where table_id = %d", tableInfo.ID))

	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	testKit.MustQuery("select count from mysql.stats_meta").Check(testkit.Rows("0"))
}

func TestUpdateStatsByLocalFeedback(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("set @@session.tidb_analyze_version = 0")
	testKit.MustExec(`set @@tidb_partition_prune_mode='` + string(variable.Static) + `'`)
	testKit.MustExec("create table t (a bigint(64), b bigint(64), primary key(a), index idx(b))")
	testKit.MustExec("insert into t values (1,2),(2,2),(4,5)")
	testKit.MustExec("analyze table t with 0 topn")
	testKit.MustExec("insert into t values (3,5)")
	h := dom.StatsHandle()
	oriProbability := statistics.FeedbackProbability.Load()
	oriMinLogCount := handle.MinLogScanCount.Load()
	oriErrorRate := handle.MinLogErrorRate.Load()
	oriNumber := statistics.MaxNumberOfRanges
	defer func() {
		statistics.FeedbackProbability.Store(oriProbability)
		handle.MinLogScanCount.Store(oriMinLogCount)
		handle.MinLogErrorRate.Store(oriErrorRate)
		statistics.MaxNumberOfRanges = oriNumber
	}()
	statistics.FeedbackProbability.Store(1)
	handle.MinLogScanCount.Store(0)
	handle.MinLogErrorRate.Store(0)

	is := dom.InfoSchema()
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)

	tblInfo := table.Meta()
	h.GetTableStats(tblInfo)

	testKit.MustQuery("select * from t use index(idx) where b <= 5")
	testKit.MustQuery("select * from t where a > 1")
	testKit.MustQuery("select * from t use index(idx) where b = 5")

	h.UpdateStatsByLocalFeedback(dom.InfoSchema())
	tbl := h.GetTableStats(tblInfo)

	require.Equal(t, "column:1 ndv:3 totColSize:0\n"+
		"num: 1 lower_bound: 1 upper_bound: 1 repeats: 1 ndv: 0\n"+
		"num: 2 lower_bound: 2 upper_bound: 4 repeats: 0 ndv: 0\n"+
		"num: 1 lower_bound: 4 upper_bound: 9223372036854775807 repeats: 0 ndv: 0", tbl.Columns[tblInfo.Columns[0].ID].ToString(0))
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	low, err := codec.EncodeKey(sc, nil, types.NewIntDatum(5))
	require.NoError(t, err)

	require.Equal(t, uint64(2), tbl.Indices[tblInfo.Indices[0].ID].CMSketch.QueryBytes(low))

	require.Equal(t, "index:1 ndv:2\n"+
		"num: 2 lower_bound: -inf upper_bound: 5 repeats: 0 ndv: 0\n"+
		"num: 1 lower_bound: 5 upper_bound: 5 repeats: 1 ndv: 0", tbl.Indices[tblInfo.Indices[0].ID].ToString(1))

	// Test that it won't cause panic after update.
	testKit.MustQuery("select * from t use index(idx) where b > 0")

	// Test that after drop stats, it won't cause panic.
	testKit.MustExec("drop stats t")
	h.UpdateStatsByLocalFeedback(dom.InfoSchema())
}

func TestUpdatePartitionStatsByLocalFeedback(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("set @@session.tidb_analyze_version = 0")
	testKit.MustExec(`set @@tidb_partition_prune_mode='` + string(variable.Static) + `'`)
	testKit.MustExec("create table t (a bigint(64), b bigint(64), primary key(a)) partition by range (a) (partition p0 values less than (6))")
	testKit.MustExec("insert into t values (1,2),(2,2),(4,5)")
	testKit.MustExec("analyze table t")
	testKit.MustExec("insert into t values (3,5)")
	h := dom.StatsHandle()
	oriProbability := statistics.FeedbackProbability.Load()
	oriMinLogCount := handle.MinLogScanCount.Load()
	oriErrorRate := handle.MinLogErrorRate.Load()
	defer func() {
		statistics.FeedbackProbability.Store(oriProbability)
		handle.MinLogScanCount.Store(oriMinLogCount)
		handle.MinLogErrorRate.Store(oriErrorRate)
	}()
	statistics.FeedbackProbability.Store(1)
	handle.MinLogScanCount.Store(0)
	handle.MinLogErrorRate.Store(0)

	is := dom.InfoSchema()
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)

	testKit.MustQuery("select * from t where a > 1").Check(testkit.Rows("2 2", "3 5", "4 5"))

	h.UpdateStatsByLocalFeedback(dom.InfoSchema())

	tblInfo := table.Meta()
	pid := tblInfo.Partition.Definitions[0].ID
	tbl := h.GetPartitionStats(tblInfo, pid)

	// Feedback will not take effect under partition table.
	require.Equal(t, "column:1 ndv:3 totColSize:0\n"+
		"num: 1 lower_bound: 1 upper_bound: 1 repeats: 1 ndv: 0\n"+
		"num: 1 lower_bound: 2 upper_bound: 2 repeats: 1 ndv: 0\n"+
		"num: 1 lower_bound: 4 upper_bound: 4 repeats: 1 ndv: 0", tbl.Columns[tblInfo.Columns[0].ID].ToString(0))
}

func TestFeedbackWithStatsVer2(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("set global tidb_analyze_version = 1")
	testKit.MustExec("set @@tidb_analyze_version = 1")

	oriProbability := statistics.FeedbackProbability.Load()
	oriNumber := statistics.MaxNumberOfRanges
	oriMinLogCount := handle.MinLogScanCount.Load()
	oriErrorRate := handle.MinLogErrorRate.Load()
	defer func() {
		statistics.FeedbackProbability.Store(oriProbability)
		statistics.MaxNumberOfRanges = oriNumber
		handle.MinLogScanCount.Store(oriMinLogCount)
		handle.MinLogErrorRate.Store(oriErrorRate)
	}()
	// Case 1: You can't set tidb_analyze_version to 2 if feedback is enabled.
	statistics.FeedbackProbability.Store(1)
	testKit.MustQuery("select @@tidb_analyze_version").Check(testkit.Rows("1"))
	testKit.MustExec("set @@tidb_analyze_version = 2")
	testKit.MustQuery("show warnings").Check(testkit.Rows(`Error 1105 variable tidb_analyze_version not updated because analyze version 2 is incompatible with query feedback. Please consider setting feedback-probability to 0.0 in config file to disable query feedback`))
	testKit.MustQuery("select @@tidb_analyze_version").Check(testkit.Rows("1"))

	// Case 2: Feedback wouldn't be applied on version 2 statistics.
	statistics.FeedbackProbability.Store(0)
	testKit.MustExec("set @@tidb_analyze_version = 2")
	testKit.MustQuery("select @@tidb_analyze_version").Check(testkit.Rows("2"))
	testKit.MustExec("create table t (a bigint(64), b bigint(64), index idx(b))")
	for i := 0; i < 200; i++ {
		testKit.MustExec("insert into t values (1,2),(2,2),(4,5),(2,3),(3,4)")
	}
	testKit.MustExec("analyze table t with 0 topn")
	h := dom.StatsHandle()
	is := dom.InfoSchema()
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblInfo := table.Meta()
	testKit.MustExec("analyze table t")
	err = h.Update(dom.InfoSchema())
	require.NoError(t, err)
	statsTblBefore := h.GetTableStats(tblInfo)
	statistics.FeedbackProbability.Store(1)
	// make the statistics inaccurate.
	for i := 0; i < 200; i++ {
		testKit.MustExec("insert into t values (3,4), (3,4), (3,4), (3,4), (3,4)")
	}
	// trigger feedback
	testKit.MustExec("select * from t where t.a <= 5 order by a desc")
	testKit.MustExec("select b from t use index(idx) where t.b <= 5")

	h.UpdateStatsByLocalFeedback(dom.InfoSchema())
	err = h.DumpStatsFeedbackToKV()
	require.NoError(t, err)
	err = h.HandleUpdateStats(dom.InfoSchema())
	require.NoError(t, err)
	statsTblAfter := h.GetTableStats(tblInfo)
	// assert that statistics not changed
	assertTableEqual(t, statsTblBefore, statsTblAfter)

	// Case 3: Feedback is still effective on version 1 statistics.
	testKit.MustExec("set tidb_analyze_version = 1")
	testKit.MustExec("create table t1 (a bigint(64), b bigint(64), index idx(b))")
	for i := 0; i < 200; i++ {
		testKit.MustExec("insert into t1 values (1,2),(2,2),(4,5),(2,3),(3,4)")
	}
	testKit.MustExec("analyze table t1 with 0 topn")
	// make the statistics inaccurate.
	for i := 0; i < 200; i++ {
		testKit.MustExec("insert into t1 values (3,4), (3,4), (3,4), (3,4), (3,4)")
	}
	is = dom.InfoSchema()
	table, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	tblInfo = table.Meta()
	statsTblBefore = h.GetTableStats(tblInfo)
	// trigger feedback
	testKit.MustExec("select b from t1 use index(idx) where t1.b <= 5")

	h.UpdateStatsByLocalFeedback(dom.InfoSchema())
	err = h.DumpStatsFeedbackToKV()
	require.NoError(t, err)
	err = h.HandleUpdateStats(dom.InfoSchema())
	require.NoError(t, err)
	statsTblAfter = h.GetTableStats(tblInfo)
	// assert that statistics changed(feedback worked)
	require.False(t, statistics.HistogramEqual(&statsTblBefore.Indices[1].Histogram, &statsTblAfter.Indices[1].Histogram, false))

	// Case 4: When existing version 1 stats + tidb_analyze_version=2 + feedback enabled, explicitly running `analyze table` still results in version 1 stats.
	statistics.FeedbackProbability.Store(0)
	testKit.MustExec("set tidb_analyze_version = 2")
	statistics.FeedbackProbability.Store(1)
	testKit.MustExec("analyze table t1 with 0 topn")
	testKit.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1105 Use analyze version 1 on table `t1` because this table already has version 1 statistics and query feedback is also enabled." +
			" If you want to switch to version 2 statistics, please first disable query feedback by setting feedback-probability to 0.0 in the config file."))
	testKit.MustQuery(fmt.Sprintf("select stats_ver from mysql.stats_histograms where table_id = %d", tblInfo.ID)).Check(testkit.Rows("1", "1", "1"))

	testKit.MustExec("set global tidb_analyze_version = 1")
}

func TestNeedAnalyzeTable(t *testing.T) {
	columns := map[int64]*statistics.Column{}
	columns[1] = &statistics.Column{Count: 1}
	tests := []struct {
		tbl    *statistics.Table
		ratio  float64
		limit  time.Duration
		result bool
		reason string
	}{
		// table was never analyzed and has reach the limit
		{
			tbl:    &statistics.Table{Version: oracle.GoTimeToTS(time.Now())},
			limit:  0,
			ratio:  0,
			result: true,
			reason: "table unanalyzed",
		},
		// table was never analyzed but has not reached the limit
		{
			tbl:    &statistics.Table{Version: oracle.GoTimeToTS(time.Now())},
			limit:  time.Hour,
			ratio:  0,
			result: false,
			reason: "",
		},
		// table was already analyzed but auto analyze is disabled
		{
			tbl:    &statistics.Table{HistColl: statistics.HistColl{Columns: columns, ModifyCount: 1, Count: 1}},
			limit:  0,
			ratio:  0,
			result: false,
			reason: "",
		},
		// table was already analyzed but modify count is small
		{
			tbl:    &statistics.Table{HistColl: statistics.HistColl{Columns: columns, ModifyCount: 0, Count: 1}},
			limit:  0,
			ratio:  0.3,
			result: false,
			reason: "",
		},
		// table was already analyzed
		{
			tbl:    &statistics.Table{HistColl: statistics.HistColl{Columns: columns, ModifyCount: 1, Count: 1}},
			limit:  0,
			ratio:  0.3,
			result: true,
			reason: "too many modifications",
		},
		// table was already analyzed
		{
			tbl:    &statistics.Table{HistColl: statistics.HistColl{Columns: columns, ModifyCount: 1, Count: 1}},
			limit:  0,
			ratio:  0.3,
			result: true,
			reason: "too many modifications",
		},
		// table was already analyzed
		{
			tbl:    &statistics.Table{HistColl: statistics.HistColl{Columns: columns, ModifyCount: 1, Count: 1}},
			limit:  0,
			ratio:  0.3,
			result: true,
			reason: "too many modifications",
		},
		// table was already analyzed
		{
			tbl:    &statistics.Table{HistColl: statistics.HistColl{Columns: columns, ModifyCount: 1, Count: 1}},
			limit:  0,
			ratio:  0.3,
			result: true,
			reason: "too many modifications",
		},
	}
	for _, test := range tests {
		needAnalyze, reason := handle.NeedAnalyzeTable(test.tbl, test.limit, test.ratio)
		require.Equal(t, test.result, needAnalyze)
		require.True(t, strings.HasPrefix(reason, test.reason))
	}
}

func TestIndexQueryFeedback(t *testing.T) {
	t.Skip("support update the topn of index equal conditions")
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)

	oriProbability := statistics.FeedbackProbability.Load()
	defer func() {
		statistics.FeedbackProbability.Store(oriProbability)
	}()
	statistics.FeedbackProbability.Store(1)

	testKit.MustExec("use test")
	testKit.MustExec("create table t (a bigint(64), b bigint(64), c bigint(64), d float, e double, f decimal(17,2), " +
		"g time, h date, index idx_b(b), index idx_ab(a,b), index idx_ac(a,c), index idx_ad(a, d), index idx_ae(a, e), index idx_af(a, f)," +
		" index idx_ag(a, g), index idx_ah(a, h))")
	for i := 0; i < 20; i++ {
		testKit.MustExec(fmt.Sprintf(`insert into t values (1, %d, %d, %d, %d, %d, %d, "%s")`, i, i, i, i, i, i, fmt.Sprintf("1000-01-%02d", i+1)))
	}
	h := dom.StatsHandle()
	err := h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	testKit.MustExec("analyze table t with 3 buckets")
	for i := 0; i < 20; i++ {
		testKit.MustExec(fmt.Sprintf(`insert into t values (1, %d, %d, %d, %d, %d, %d, "%s")`, i, i, i, i, i, i, fmt.Sprintf("1000-01-%02d", i+1)))
	}
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	is := dom.InfoSchema()
	require.NoError(t, h.Update(is))
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblInfo := table.Meta()
	tests := []struct {
		sql     string
		hist    string
		idxCols int
		rangeID int64
		idxID   int64
		eqCount uint32
	}{
		{
			sql: "select * from t use index(idx_ab) where a = 1 and b < 21",
			hist: "index:1 ndv:20\n" +
				"num: 16 lower_bound: -inf upper_bound: 7 repeats: 0\n" +
				"num: 16 lower_bound: 8 upper_bound: 15 repeats: 0\n" +
				"num: 9 lower_bound: 16 upper_bound: 21 repeats: 0",
			rangeID: tblInfo.Indices[0].ID,
			idxID:   tblInfo.Indices[1].ID,
			idxCols: 1,
			eqCount: 32,
		},
		{
			sql: "select * from t use index(idx_ac) where a = 1 and c < 21",
			hist: "column:3 ndv:20 totColSize:40\n" +
				"num: 13 lower_bound: -9223372036854775808 upper_bound: 6 repeats: 0\n" +
				"num: 13 lower_bound: 7 upper_bound: 13 repeats: 0\n" +
				"num: 12 lower_bound: 14 upper_bound: 21 repeats: 0",
			rangeID: tblInfo.Columns[2].ID,
			idxID:   tblInfo.Indices[2].ID,
			idxCols: 0,
			eqCount: 32,
		},
		{
			sql: "select * from t use index(idx_ad) where a = 1 and d < 21",
			hist: "column:4 ndv:20 totColSize:320\n" +
				"num: 13 lower_bound: -10000000000000 upper_bound: 6 repeats: 0\n" +
				"num: 12 lower_bound: 7 upper_bound: 13 repeats: 0\n" +
				"num: 10 lower_bound: 14 upper_bound: 21 repeats: 0",
			rangeID: tblInfo.Columns[3].ID,
			idxID:   tblInfo.Indices[3].ID,
			idxCols: 0,
			eqCount: 32,
		},
		{
			sql: "select * from t use index(idx_ae) where a = 1 and e < 21",
			hist: "column:5 ndv:20 totColSize:320\n" +
				"num: 13 lower_bound: -100000000000000000000000 upper_bound: 6 repeats: 0\n" +
				"num: 12 lower_bound: 7 upper_bound: 13 repeats: 0\n" +
				"num: 10 lower_bound: 14 upper_bound: 21 repeats: 0",
			rangeID: tblInfo.Columns[4].ID,
			idxID:   tblInfo.Indices[4].ID,
			idxCols: 0,
			eqCount: 32,
		},
		{
			sql: "select * from t use index(idx_af) where a = 1 and f < 21",
			hist: "column:6 ndv:20 totColSize:400\n" +
				"num: 13 lower_bound: -999999999999999.99 upper_bound: 6.00 repeats: 0\n" +
				"num: 12 lower_bound: 7.00 upper_bound: 13.00 repeats: 0\n" +
				"num: 10 lower_bound: 14.00 upper_bound: 21.00 repeats: 0",
			rangeID: tblInfo.Columns[5].ID,
			idxID:   tblInfo.Indices[5].ID,
			idxCols: 0,
			eqCount: 32,
		},
		{
			sql: "select * from t use index(idx_ag) where a = 1 and g < 21",
			hist: "column:7 ndv:20 totColSize:196\n" +
				"num: 13 lower_bound: -838:59:59 upper_bound: 00:00:06 repeats: 0\n" +
				"num: 12 lower_bound: 00:00:07 upper_bound: 00:00:13 repeats: 0\n" +
				"num: 10 lower_bound: 00:00:14 upper_bound: 00:00:21 repeats: 0",
			rangeID: tblInfo.Columns[6].ID,
			idxID:   tblInfo.Indices[6].ID,
			idxCols: 0,
			eqCount: 30,
		},
		{
			sql: `select * from t use index(idx_ah) where a = 1 and h < "1000-01-21"`,
			hist: "column:8 ndv:20 totColSize:360\n" +
				"num: 13 lower_bound: 1000-01-01 upper_bound: 1000-01-07 repeats: 0\n" +
				"num: 12 lower_bound: 1000-01-08 upper_bound: 1000-01-14 repeats: 0\n" +
				"num: 10 lower_bound: 1000-01-15 upper_bound: 1000-01-21 repeats: 0",
			rangeID: tblInfo.Columns[7].ID,
			idxID:   tblInfo.Indices[7].ID,
			idxCols: 0,
			eqCount: 32,
		},
	}
	for i, test := range tests {
		testKit.MustQuery(test.sql)
		require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
		require.NoError(t, h.DumpStatsFeedbackToKV())
		require.NoError(t, h.HandleUpdateStats(dom.InfoSchema()))
		require.NoError(t, h.Update(is))
		tbl := h.GetTableStats(tblInfo)
		if test.idxCols == 0 {
			require.Equal(t, tests[i].hist, tbl.Columns[test.rangeID].ToString(0))
		} else {
			require.Equal(t, tests[i].hist, tbl.Indices[test.rangeID].ToString(1))
		}
		val, err := codec.EncodeKey(testKit.Session().GetSessionVars().StmtCtx, nil, types.NewIntDatum(1))
		require.NoError(t, err)
		require.Equal(t, uint64(test.eqCount), tbl.Indices[test.idxID].CMSketch.QueryBytes(val))
	}
}

func TestIndexQueryFeedback4TopN(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)

	oriProbability := statistics.FeedbackProbability.Load()
	oriMinLogCount := handle.MinLogScanCount.Load()
	oriErrorRate := handle.MinLogErrorRate.Load()
	defer func() {
		statistics.FeedbackProbability.Store(oriProbability)
		handle.MinLogScanCount.Store(oriMinLogCount)
		handle.MinLogErrorRate.Store(oriErrorRate)
	}()
	statistics.FeedbackProbability.Store(1)
	handle.MinLogScanCount.Store(0)
	handle.MinLogErrorRate.Store(0)

	testKit.MustExec("use test")
	testKit.MustExec("set @@session.tidb_analyze_version = 0")
	testKit.MustExec("create table t (a bigint(64), index idx(a))")
	for i := 0; i < 20; i++ {
		testKit.MustExec(`insert into t values (1)`)
	}
	h := dom.StatsHandle()
	err := h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	testKit.MustExec("set @@session.tidb_analyze_version = 1")
	testKit.MustExec("set @@tidb_enable_fast_analyze = 1")
	testKit.MustExec("analyze table t with 3 buckets")
	for i := 0; i < 20; i++ {
		testKit.MustExec(`insert into t values (1)`)
	}
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	is := dom.InfoSchema()
	require.NoError(t, h.Update(is))
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblInfo := table.Meta()

	testKit.MustQuery("select * from t use index(idx) where a = 1")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.DumpStatsFeedbackToKV())
	require.NoError(t, h.HandleUpdateStats(dom.InfoSchema()))
	require.NoError(t, h.Update(is))
	tbl := h.GetTableStats(tblInfo)
	val, err := codec.EncodeKey(testKit.Session().GetSessionVars().StmtCtx, nil, types.NewIntDatum(1))
	require.NoError(t, err)
	require.Equal(t, uint64(40), tbl.Indices[1].CMSketch.QueryBytes(val))
}

func TestAbnormalIndexFeedback(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)

	oriProbability := statistics.FeedbackProbability.Load()
	oriMinLogCount := handle.MinLogScanCount.Load()
	oriErrorRate := handle.MinLogErrorRate.Load()
	defer func() {
		statistics.FeedbackProbability.Store(oriProbability)
		handle.MinLogScanCount.Store(oriMinLogCount)
		handle.MinLogErrorRate.Store(oriErrorRate)
	}()
	statistics.FeedbackProbability.Store(1)
	handle.MinLogScanCount.Store(0)
	handle.MinLogErrorRate.Store(0)

	testKit.MustExec("use test")
	testKit.MustExec("create table t (a bigint(64), b bigint(64), index idx_ab(a,b))")
	for i := 0; i < 20; i++ {
		testKit.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i/5, i))
	}
	testKit.MustExec("set @@session.tidb_analyze_version = 1")
	testKit.MustExec("analyze table t with 3 buckets, 0 topn")
	testKit.MustExec("delete from t where a = 1")
	testKit.MustExec("delete from t where b > 10")
	is := dom.InfoSchema()
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblInfo := table.Meta()
	h := dom.StatsHandle()
	tests := []struct {
		sql     string
		hist    string
		rangeID int64
		idxID   int64
		eqCount uint32
	}{
		{
			// The real count of `a = 1` is 0.
			sql: "select * from t where a = 1 and b < 21",
			hist: "column:2 ndv:20 totColSize:20\n" +
				"num: 5 lower_bound: -9223372036854775808 upper_bound: 7 repeats: 0 ndv: 0\n" +
				"num: 4 lower_bound: 7 upper_bound: 14 repeats: 0 ndv: 0\n" +
				"num: 4 lower_bound: 14 upper_bound: 21 repeats: 0 ndv: 0",
			rangeID: tblInfo.Columns[1].ID,
			idxID:   tblInfo.Indices[0].ID,
			eqCount: 3,
		},
		{
			// The real count of `b > 10` is 0.
			sql: "select * from t where a = 2 and b > 10",
			hist: "column:2 ndv:20 totColSize:20\n" +
				"num: 5 lower_bound: -9223372036854775808 upper_bound: 7 repeats: 0 ndv: 0\n" +
				"num: 6 lower_bound: 7 upper_bound: 14 repeats: 0 ndv: 0\n" +
				"num: 8 lower_bound: 14 upper_bound: 9223372036854775807 repeats: 0 ndv: 0",
			rangeID: tblInfo.Columns[1].ID,
			idxID:   tblInfo.Indices[0].ID,
			eqCount: 3,
		},
	}
	for i, test := range tests {
		testKit.MustQuery(test.sql)
		require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
		require.NoError(t, h.DumpStatsFeedbackToKV())
		require.NoError(t, h.HandleUpdateStats(dom.InfoSchema()))
		require.NoError(t, h.Update(is))
		tbl := h.GetTableStats(tblInfo)
		require.Equal(t, tests[i].hist, tbl.Columns[test.rangeID].ToString(0))
		val, err := codec.EncodeKey(testKit.Session().GetSessionVars().StmtCtx, nil, types.NewIntDatum(1))
		require.NoError(t, err)
		require.Equal(t, uint64(test.eqCount), tbl.Indices[test.idxID].CMSketch.QueryBytes(val))
	}
}

func TestFeedbackRanges(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)
	h := dom.StatsHandle()
	oriProbability := statistics.FeedbackProbability.Load()
	oriNumber := statistics.MaxNumberOfRanges
	oriMinLogCount := handle.MinLogScanCount.Load()
	oriErrorRate := handle.MinLogErrorRate.Load()
	defer func() {
		statistics.FeedbackProbability.Store(oriProbability)
		statistics.MaxNumberOfRanges = oriNumber
		handle.MinLogScanCount.Store(oriMinLogCount)
		handle.MinLogErrorRate.Store(oriErrorRate)
	}()
	statistics.FeedbackProbability.Store(1)
	handle.MinLogScanCount.Store(0)
	handle.MinLogErrorRate.Store(0)

	testKit.MustExec("use test")
	testKit.MustExec("create table t (a tinyint, b tinyint, primary key(a), index idx(a, b))")
	for i := 0; i < 20; i++ {
		testKit.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i))
	}
	err := h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	testKit.MustExec("set @@session.tidb_analyze_version=1")
	testKit.MustExec("analyze table t with 3 buckets")
	for i := 30; i < 40; i++ {
		testKit.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i))
	}
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	tests := []struct {
		sql   string
		hist  string
		colID int64
	}{
		{
			sql: "select * from t where a <= 50 or (a > 130 and a < 140)",
			hist: "column:1 ndv:30 totColSize:0\n" +
				"num: 8 lower_bound: -128 upper_bound: 8 repeats: 0 ndv: 0\n" +
				"num: 8 lower_bound: 8 upper_bound: 16 repeats: 0 ndv: 0\n" +
				"num: 14 lower_bound: 16 upper_bound: 50 repeats: 0 ndv: 0",
			colID: 1,
		},
		{
			sql: "select * from t where a >= 10",
			hist: "column:1 ndv:30 totColSize:0\n" +
				"num: 8 lower_bound: -128 upper_bound: 8 repeats: 0 ndv: 0\n" +
				"num: 8 lower_bound: 8 upper_bound: 16 repeats: 0 ndv: 0\n" +
				"num: 14 lower_bound: 16 upper_bound: 127 repeats: 0 ndv: 0",
			colID: 1,
		},
		{
			sql: "select * from t use index(idx) where a = 1 and (b <= 50 or (b > 130 and b < 140))",
			hist: "column:2 ndv:20 totColSize:30\n" +
				"num: 8 lower_bound: -128 upper_bound: 7 repeats: 0 ndv: 0\n" +
				"num: 8 lower_bound: 7 upper_bound: 14 repeats: 0 ndv: 0\n" +
				"num: 7 lower_bound: 14 upper_bound: 51 repeats: 0 ndv: 0",
			colID: 2,
		},
	}
	is := dom.InfoSchema()
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	for _, test := range tests {
		testKit.MustQuery(test.sql)
		require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
		require.NoError(t, h.DumpStatsFeedbackToKV())
		require.NoError(t, h.HandleUpdateStats(dom.InfoSchema()))
		require.NoError(t, err)
		require.NoError(t, h.Update(is))
		tblInfo := table.Meta()
		tbl := h.GetTableStats(tblInfo)
		require.Equal(t, test.hist, tbl.Columns[test.colID].ToString(0))
	}
}

func TestUnsignedFeedbackRanges(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)
	h := dom.StatsHandle()

	oriProbability := statistics.FeedbackProbability.Load()
	oriMinLogCount := handle.MinLogScanCount.Load()
	oriErrorRate := handle.MinLogErrorRate.Load()
	oriNumber := statistics.MaxNumberOfRanges
	defer func() {
		statistics.FeedbackProbability.Store(oriProbability)
		handle.MinLogScanCount.Store(oriMinLogCount)
		handle.MinLogErrorRate.Store(oriErrorRate)
		statistics.MaxNumberOfRanges = oriNumber
	}()
	statistics.FeedbackProbability.Store(1)
	handle.MinLogScanCount.Store(0)
	handle.MinLogErrorRate.Store(0)

	testKit.MustExec("use test")
	testKit.MustExec("set @@session.tidb_analyze_version = 0")
	testKit.MustExec("create table t (a tinyint unsigned, primary key(a))")
	testKit.MustExec("create table t1 (a bigint unsigned, primary key(a))")
	for i := 0; i < 20; i++ {
		testKit.MustExec(fmt.Sprintf("insert into t values (%d)", i))
		testKit.MustExec(fmt.Sprintf("insert into t1 values (%d)", i))
	}
	err := h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	testKit.MustExec("analyze table t, t1 with 3 buckets")
	for i := 30; i < 40; i++ {
		testKit.MustExec(fmt.Sprintf("insert into t values (%d)", i))
		testKit.MustExec(fmt.Sprintf("insert into t1 values (%d)", i))
	}
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	tests := []struct {
		sql     string
		hist    string
		tblName string
	}{
		{
			sql: "select * from t where a <= 50",
			hist: "column:1 ndv:30 totColSize:10\n" +
				"num: 8 lower_bound: 0 upper_bound: 8 repeats: 0 ndv: 0\n" +
				"num: 8 lower_bound: 8 upper_bound: 16 repeats: 0 ndv: 0\n" +
				"num: 14 lower_bound: 16 upper_bound: 50 repeats: 0 ndv: 0",
			tblName: "t",
		},
		{
			sql: "select count(*) from t",
			hist: "column:1 ndv:30 totColSize:10\n" +
				"num: 8 lower_bound: 0 upper_bound: 8 repeats: 0 ndv: 0\n" +
				"num: 8 lower_bound: 8 upper_bound: 16 repeats: 0 ndv: 0\n" +
				"num: 14 lower_bound: 16 upper_bound: 255 repeats: 0 ndv: 0",
			tblName: "t",
		},
		{
			sql: "select * from t1 where a <= 50",
			hist: "column:1 ndv:30 totColSize:10\n" +
				"num: 8 lower_bound: 0 upper_bound: 8 repeats: 0 ndv: 0\n" +
				"num: 8 lower_bound: 8 upper_bound: 16 repeats: 0 ndv: 0\n" +
				"num: 14 lower_bound: 16 upper_bound: 50 repeats: 0 ndv: 0",
			tblName: "t1",
		},
		{
			sql: "select count(*) from t1",
			hist: "column:1 ndv:30 totColSize:10\n" +
				"num: 8 lower_bound: 0 upper_bound: 8 repeats: 0 ndv: 0\n" +
				"num: 8 lower_bound: 8 upper_bound: 16 repeats: 0 ndv: 0\n" +
				"num: 14 lower_bound: 16 upper_bound: 18446744073709551615 repeats: 0 ndv: 0",
			tblName: "t1",
		},
	}
	is := dom.InfoSchema()
	require.NoError(t, h.Update(is))
	for _, test := range tests {
		table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr(test.tblName))
		require.NoError(t, err)
		testKit.MustQuery(test.sql)
		require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
		require.NoError(t, h.DumpStatsFeedbackToKV())
		require.NoError(t, h.HandleUpdateStats(dom.InfoSchema()))
		require.NoError(t, err)
		require.NoError(t, h.Update(is))
		tblInfo := table.Meta()
		tbl := h.GetTableStats(tblInfo)
		require.Equal(t, test.hist, tbl.Columns[1].ToString(0))
	}
}

func TestLoadHistCorrelation(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)
	h := dom.StatsHandle()
	origLease := h.Lease()
	h.SetLease(time.Second)
	defer func() { h.SetLease(origLease) }()
	testKit.MustExec("use test")
	testKit.MustExec("create table t(c int)")
	testKit.MustExec("insert into t values(1),(2),(3),(4),(5)")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	testKit.MustExec("analyze table t")
	h.Clear()
	require.NoError(t, h.Update(dom.InfoSchema()))
	result := testKit.MustQuery("show stats_histograms where Table_name = 't'")
	require.Len(t, result.Rows(), 0)
	testKit.MustExec("explain select * from t where c = 1")
	require.NoError(t, h.LoadNeededHistograms())
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'")
	require.Len(t, result.Rows(), 1)
	require.Equal(t, "1", result.Rows()[0][9])
}

func TestDeleteUpdateFeedback(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)

	oriProbability := statistics.FeedbackProbability.Load()
	defer func() {
		statistics.FeedbackProbability.Store(oriProbability)
	}()
	statistics.FeedbackProbability.Store(1)

	h := dom.StatsHandle()
	testKit.MustExec("use test")
	testKit.MustExec("create table t (a bigint(64), b bigint(64), index idx_ab(a,b))")
	for i := 0; i < 20; i++ {
		testKit.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i/5, i))
	}
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	testKit.MustExec("analyze table t with 3 buckets")

	testKit.MustExec("delete from t where a = 1")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.Equal(t, 0, h.GetQueryFeedback().Size)
	testKit.MustExec("update t set a = 6 where a = 2")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.Equal(t, 0, h.GetQueryFeedback().Size)
	testKit.MustExec("explain analyze delete from t where a = 3")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.Equal(t, 0, h.GetQueryFeedback().Size)
}

func BenchmarkHandleAutoAnalyze(b *testing.B) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(b)
	defer clean()
	testKit := testkit.NewTestKit(b, store)
	testKit.MustExec("use test")
	h := dom.StatsHandle()
	is := dom.InfoSchema()
	for i := 0; i < b.N; i++ {
		h.HandleAutoAnalyze(is)
	}
}

// subtraction parses the number for counter and returns new - old.
// string for counter will be `label:<name:"type" value:"ok" > counter:<value:0 > `
func subtraction(newMetric *dto.Metric, oldMetric *dto.Metric) int {
	newStr := newMetric.String()
	oldStr := oldMetric.String()
	newIdx := strings.LastIndex(newStr, ":")
	newNum, _ := strconv.Atoi(newStr[newIdx+1 : len(newStr)-3])
	oldIdx := strings.LastIndex(oldStr, ":")
	oldNum, _ := strconv.Atoi(oldStr[oldIdx+1 : len(oldStr)-3])
	return newNum - oldNum
}

func TestDisableFeedback(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)

	oriProbability := statistics.FeedbackProbability.Load()
	defer func() {
		statistics.FeedbackProbability.Store(oriProbability)
	}()
	statistics.FeedbackProbability.Store(0.0)
	oldNum := &dto.Metric{}
	err := metrics.StoreQueryFeedbackCounter.WithLabelValues(metrics.LblOK).Write(oldNum)
	require.NoError(t, err)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (a int, b int, index idx_a(a))")
	testKit.MustExec("insert into t values (1, 1), (2, 2), (3, 3), (5, 5)")
	testKit.MustExec("analyze table t with 0 topn")
	for i := 0; i < 20; i++ {
		testKit.MustQuery("select /*+ use_index(t, idx_a) */ * from t where a < 4")
	}

	newNum := &dto.Metric{}
	err = metrics.StoreQueryFeedbackCounter.WithLabelValues(metrics.LblOK).Write(newNum)
	require.NoError(t, err)
	require.Equal(t, 0, subtraction(newNum, oldNum))
}

func TestFeedbackCounter(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)

	oriProbability := statistics.FeedbackProbability.Load()
	defer func() {
		statistics.FeedbackProbability.Store(oriProbability)
	}()
	statistics.FeedbackProbability.Store(1)
	oldNum := &dto.Metric{}
	err := metrics.StoreQueryFeedbackCounter.WithLabelValues(metrics.LblOK).Write(oldNum)
	require.NoError(t, err)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (a int, b int, index idx_a(a))")
	testKit.MustExec("insert into t values (1, 1), (2, 2), (3, 3), (5, 5)")
	testKit.MustExec("analyze table t with 0 topn")
	for i := 0; i < 20; i++ {
		testKit.MustQuery("select /*+ use_index(t, idx_a) */ * from t where a < 4")
	}

	newNum := &dto.Metric{}
	err = metrics.StoreQueryFeedbackCounter.WithLabelValues(metrics.LblOK).Write(newNum)
	require.NoError(t, err)
	require.Equal(t, 20, subtraction(newNum, oldNum))
}

func TestMergeTopN(t *testing.T) {
	// Move this test to here to avoid race test.
	tests := []struct {
		topnNum    int
		n          int
		maxTopNVal int
		maxTopNCnt int
	}{
		{
			topnNum:    10,
			n:          5,
			maxTopNVal: 50,
			maxTopNCnt: 100,
		},
		{
			topnNum:    1,
			n:          5,
			maxTopNVal: 50,
			maxTopNCnt: 100,
		},
		{
			topnNum:    5,
			n:          5,
			maxTopNVal: 5,
			maxTopNCnt: 100,
		},
		{
			topnNum:    5,
			n:          5,
			maxTopNVal: 10,
			maxTopNCnt: 100,
		},
	}
	for _, test := range tests {
		topnNum, n := test.topnNum, test.n
		maxTopNVal, maxTopNCnt := test.maxTopNVal, test.maxTopNCnt

		// the number of maxTopNVal should be bigger than n.
		ok := maxTopNVal >= n
		require.Equal(t, true, ok)

		topNs := make([]*statistics.TopN, 0, topnNum)
		res := make(map[int]uint64)
		rand.Seed(time.Now().Unix())
		for i := 0; i < topnNum; i++ {
			topN := statistics.NewTopN(n)
			occur := make(map[int]bool)
			for j := 0; j < n; j++ {
				// The range of numbers in the topn structure is in [0, maxTopNVal)
				// But there cannot be repeated occurrences of value in a topN structure.
				randNum := rand.Intn(maxTopNVal)
				for occur[randNum] {
					randNum = rand.Intn(maxTopNVal)
				}
				occur[randNum] = true
				tString := []byte(fmt.Sprintf("%d", randNum))
				// The range of the number of occurrences in the topn structure is in [0, maxTopNCnt)
				randCnt := uint64(rand.Intn(maxTopNCnt))
				res[randNum] += randCnt
				topNMeta := statistics.TopNMeta{Encoded: tString, Count: randCnt}
				topN.TopN = append(topN.TopN, topNMeta)
			}
			topNs = append(topNs, topN)
		}
		topN, remainTopN := statistics.MergeTopN(topNs, uint32(n))
		cnt := len(topN.TopN)
		var minTopNCnt uint64
		for _, topNMeta := range topN.TopN {
			val, err := strconv.Atoi(string(topNMeta.Encoded))
			require.NoError(t, err)
			require.Equal(t, res[val], topNMeta.Count)
			minTopNCnt = topNMeta.Count
		}
		if remainTopN != nil {
			cnt += len(remainTopN)
			for _, remainTopNMeta := range remainTopN {
				val, err := strconv.Atoi(string(remainTopNMeta.Encoded))
				require.NoError(t, err)
				require.Equal(t, res[val], remainTopNMeta.Count)
				// The count of value in remainTopN may equal to the min count of value in TopN.
				ok = minTopNCnt >= remainTopNMeta.Count
				require.Equal(t, true, ok)
			}
		}
		require.Equal(t, len(res), cnt)
	}
}

func TestAutoUpdatePartitionInDynamicOnlyMode(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	testKit := testkit.NewTestKit(t, store)
	testkit.WithPruneMode(testKit, variable.DynamicOnly, func() {
		testKit.MustExec("use test")
		testKit.MustExec("set @@tidb_analyze_version = 2;")
		testKit.MustExec("drop table if exists t")
		testKit.MustExec(`create table t (a int, b varchar(10), index idx_ab(a, b))
					partition by range (a) (
					partition p0 values less than (10),
					partition p1 values less than (20),
					partition p2 values less than (30))`)

		do := dom
		is := do.InfoSchema()
		h := do.StatsHandle()
		require.NoError(t, h.RefreshVars())
		require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))

		testKit.MustExec("insert into t values (1, 'a'), (2, 'b'), (11, 'c'), (12, 'd'), (21, 'e'), (22, 'f')")
		require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
		testKit.MustExec("set @@tidb_analyze_version = 2")
		testKit.MustExec("analyze table t")

		handle.AutoAnalyzeMinCnt = 0
		testKit.MustExec("set global tidb_auto_analyze_ratio = 0.1")
		defer func() {
			handle.AutoAnalyzeMinCnt = 1000
			testKit.MustExec("set global tidb_auto_analyze_ratio = 0.0")
		}()

		require.NoError(t, h.Update(is))
		tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
		require.NoError(t, err)
		tableInfo := tbl.Meta()
		pi := tableInfo.GetPartitionInfo()
		globalStats := h.GetTableStats(tableInfo)
		partitionStats := h.GetPartitionStats(tableInfo, pi.Definitions[0].ID)
		require.Equal(t, int64(6), globalStats.Count)
		require.Equal(t, int64(0), globalStats.ModifyCount)
		require.Equal(t, int64(2), partitionStats.Count)
		require.Equal(t, int64(0), partitionStats.ModifyCount)

		testKit.MustExec("insert into t values (3, 'g')")
		require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
		require.NoError(t, h.Update(is))
		globalStats = h.GetTableStats(tableInfo)
		partitionStats = h.GetPartitionStats(tableInfo, pi.Definitions[0].ID)
		require.Equal(t, int64(7), globalStats.Count)
		require.Equal(t, int64(1), globalStats.ModifyCount)
		require.Equal(t, int64(3), partitionStats.Count)
		require.Equal(t, int64(1), partitionStats.ModifyCount)

		h.HandleAutoAnalyze(is)
		require.NoError(t, h.Update(is))
		globalStats = h.GetTableStats(tableInfo)
		partitionStats = h.GetPartitionStats(tableInfo, pi.Definitions[0].ID)
		require.Equal(t, int64(7), globalStats.Count)
		require.Equal(t, int64(0), globalStats.ModifyCount)
		require.Equal(t, int64(3), partitionStats.Count)
		require.Equal(t, int64(0), partitionStats.ModifyCount)
	})
}

func TestAutoAnalyzeRatio(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	oriStart := tk.MustQuery("select @@tidb_auto_analyze_start_time").Rows()[0][0].(string)
	oriEnd := tk.MustQuery("select @@tidb_auto_analyze_end_time").Rows()[0][0].(string)
	handle.AutoAnalyzeMinCnt = 0
	defer func() {
		handle.AutoAnalyzeMinCnt = 1000
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_start_time='%v'", oriStart))
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_end_time='%v'", oriEnd))
	}()

	h := dom.StatsHandle()
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	tk.MustExec("insert into t values (1)" + strings.Repeat(", (1)", 19))
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	is := dom.InfoSchema()
	require.NoError(t, h.Update(is))
	// To pass the stats.Pseudo check in autoAnalyzeTable
	tk.MustExec("analyze table t")
	tk.MustExec("explain select * from t where a = 1")
	require.NoError(t, h.LoadNeededHistograms())
	tk.MustExec("set global tidb_auto_analyze_start_time='00:00 +0000'")
	tk.MustExec("set global tidb_auto_analyze_end_time='23:59 +0000'")

	tk.MustExec("insert into t values (1)" + strings.Repeat(", (1)", 10))
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.Update(is))
	require.True(t, h.HandleAutoAnalyze(is))

	tk.MustExec("delete from t limit 12")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.Update(is))
	require.False(t, h.HandleAutoAnalyze(is))

	tk.MustExec("delete from t limit 4")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.Update(is))
	require.True(t, h.HandleAutoAnalyze(dom.InfoSchema()))
}

func TestDumpColumnStatsUsage(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	originalVal := tk.MustQuery("select @@tidb_enable_column_tracking").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_enable_column_tracking = %v", originalVal))
	}()
	tk.MustExec("set global tidb_enable_column_tracking = 1")

	h := dom.StatsHandle()
	tk.MustExec("use test")
	tk.MustExec("create table t1(a int, b int)")
	tk.MustExec("create table t2(a int, b int)")
	tk.MustExec("create table t3(a int, b int) partition by range(a) (partition p0 values less than (10), partition p1 values less than maxvalue)")
	tk.MustExec("insert into t1 values (1, 2), (3, 4)")
	tk.MustExec("insert into t2 values (5, 6), (7, 8)")
	tk.MustExec("insert into t3 values (1, 2), (3, 4), (11, 12), (13, 14)")
	tk.MustExec("select * from t1 where a > 1")
	tk.MustExec("select * from t2 where b < 10")
	require.NoError(t, h.DumpColStatsUsageToKV())
	// t1.a is collected as predicate column
	rows := tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't1'").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, []interface{}{"test", "t1", "", "a"}, rows[0][:4])
	require.True(t, rows[0][4].(string) != "<nil>")
	require.True(t, rows[0][5].(string) == "<nil>")
	rows = tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't2'").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, []interface{}{"test", "t2", "", "b"}, rows[0][:4])
	require.True(t, rows[0][4].(string) != "<nil>")
	require.True(t, rows[0][5].(string) == "<nil>")

	tk.MustExec("analyze table t1")
	tk.MustExec("select * from t1 where b > 1")
	require.NoError(t, h.DumpColStatsUsageToKV())
	// t1.a updates last_used_at first and then updates last_analyzed_at while t1.b updates last_analyzed_at first and then updates last_used_at.
	// Check both of them behave as expected.
	rows = tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't1'").Rows()
	require.Len(t, rows, 2)
	require.Equal(t, []interface{}{"test", "t1", "", "a"}, rows[0][:4])
	require.True(t, rows[0][4].(string) != "<nil>")
	require.True(t, rows[0][5].(string) != "<nil>")
	require.Equal(t, []interface{}{"test", "t1", "", "b"}, rows[1][:4])
	require.True(t, rows[1][4].(string) != "<nil>")
	require.True(t, rows[1][5].(string) != "<nil>")

	// Test partition table.
	// No matter whether it is static or dynamic pruning mode, we record predicate columns using table ID rather than partition ID.
	for _, val := range []string{string(variable.Static), string(variable.Dynamic)} {
		tk.MustExec(fmt.Sprintf("set @@tidb_partition_prune_mode = '%v'", val))
		tk.MustExec("delete from mysql.column_stats_usage")
		tk.MustExec("select * from t3 where a < 5")
		require.NoError(t, h.DumpColStatsUsageToKV())
		rows = tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't3'").Rows()
		require.Len(t, rows, 1)
		require.Equal(t, []interface{}{"test", "t3", "global", "a"}, rows[0][:4])
		require.True(t, rows[0][4].(string) != "<nil>")
		require.True(t, rows[0][5].(string) == "<nil>")
	}

	// Test non-correlated subquery.
	// Non-correlated subquery will be executed during the plan building phase, which cannot be done by mock in (*testPlanSuite).TestCollectPredicateColumns.
	// Hence we put the test of collecting predicate columns for non-correlated subquery here.
	tk.MustExec("delete from mysql.column_stats_usage")
	tk.MustExec("select * from t2 where t2.a > (select count(*) from t1 where t1.b > 1)")
	require.NoError(t, h.DumpColStatsUsageToKV())
	rows = tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't1'").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, []interface{}{"test", "t1", "", "b"}, rows[0][:4])
	require.True(t, rows[0][4].(string) != "<nil>")
	require.True(t, rows[0][5].(string) == "<nil>")
	rows = tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't2'").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, []interface{}{"test", "t2", "", "a"}, rows[0][:4])
	require.True(t, rows[0][4].(string) != "<nil>")
	require.True(t, rows[0][5].(string) == "<nil>")
}

func TestCollectPredicateColumnsFromExecute(t *testing.T) {
	for _, val := range []bool{false, true} {
		func(planCache bool) {
			store, dom, clean := testkit.CreateMockStoreAndDomain(t)
			defer clean()
			tmp := testkit.NewTestKit(t, store)
			defer tmp.MustExec("set global tidb_enable_prepared_plan_cache=" + variable.BoolToOnOff(variable.EnablePreparedPlanCache.Load()))
			tmp.MustExec("set global tidb_enable_prepared_plan_cache=" + variable.BoolToOnOff(planCache))
			tk := testkit.NewTestKit(t, store)

			originalVal2 := tk.MustQuery("select @@tidb_enable_column_tracking").Rows()[0][0].(string)
			defer func() {
				tk.MustExec(fmt.Sprintf("set global tidb_enable_column_tracking = %v", originalVal2))
			}()
			tk.MustExec("set global tidb_enable_column_tracking = 1")

			h := dom.StatsHandle()
			tk.MustExec("use test")
			tk.MustExec("create table t1(a int, b int)")
			tk.MustExec("prepare stmt from 'select * from t1 where a > ?'")
			require.NoError(t, h.DumpColStatsUsageToKV())
			// Prepare only converts sql string to ast and doesn't do optimization, so no predicate column is collected.
			tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't1'").Check(testkit.Rows())
			tk.MustExec("set @p1 = 1")
			tk.MustExec("execute stmt using @p1")
			require.NoError(t, h.DumpColStatsUsageToKV())
			rows := tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't1'").Rows()
			require.Len(t, rows, 1)
			require.Equal(t, []interface{}{"test", "t1", "", "a"}, rows[0][:4])
			require.True(t, rows[0][4].(string) != "<nil>")
			require.True(t, rows[0][5].(string) == "<nil>")

			tk.MustExec("delete from mysql.column_stats_usage")
			tk.MustExec("set @p2 = 2")
			tk.MustExec("execute stmt using @p2")
			if planCache {
				tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
				require.NoError(t, h.DumpColStatsUsageToKV())
				// If the second execution uses the cached plan, no predicate column is collected.
				tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't1'").Check(testkit.Rows())
			} else {
				tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
				require.NoError(t, h.DumpColStatsUsageToKV())
				rows = tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't1'").Rows()
				require.Len(t, rows, 1)
				require.Equal(t, []interface{}{"test", "t1", "", "a"}, rows[0][:4])
				require.True(t, rows[0][4].(string) != "<nil>")
				require.True(t, rows[0][5].(string) == "<nil>")
			}
		}(val)
	}
}

func TestEnableAndDisableColumnTracking(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	h := dom.StatsHandle()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int)")

	originalVal := tk.MustQuery("select @@tidb_enable_column_tracking").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_enable_column_tracking = %v", originalVal))
	}()

	tk.MustExec("set global tidb_enable_column_tracking = 1")
	tk.MustExec("select * from t where b > 1")
	require.NoError(t, h.DumpColStatsUsageToKV())
	rows := tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't' and last_used_at is not null").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "b", rows[0][3])

	tk.MustExec("set global tidb_enable_column_tracking = 0")
	// After tidb_enable_column_tracking is set to 0, the predicate columns collected before are invalidated.
	tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't' and last_used_at is not null").Check(testkit.Rows())

	// Sleep for 1.5s to let `last_used_at` be larger than `tidb_disable_tracking_time`.
	time.Sleep(1500 * time.Millisecond)
	tk.MustExec("select * from t where a > 1")
	require.NoError(t, h.DumpColStatsUsageToKV())
	// We don't collect predicate columns when tidb_enable_column_tracking = 0
	tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't' and last_used_at is not null").Check(testkit.Rows())

	tk.MustExec("set global tidb_enable_column_tracking = 1")
	tk.MustExec("select * from t where b < 1 and c > 1")
	require.NoError(t, h.DumpColStatsUsageToKV())
	rows = tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't' and last_used_at is not null").Sort().Rows()
	require.Len(t, rows, 2)
	require.Equal(t, "b", rows[0][3])
	require.Equal(t, "c", rows[1][3])

	// Test invalidating predicate columns again in order to check that tidb_disable_tracking_time can be updated.
	tk.MustExec("set global tidb_enable_column_tracking = 0")
	tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't' and last_used_at is not null").Check(testkit.Rows())
}
