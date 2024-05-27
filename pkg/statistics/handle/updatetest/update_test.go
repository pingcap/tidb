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

package updatetest

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/exec"
	"github.com/pingcap/tidb/pkg/statistics/handle/usage"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/ranger"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestSingleSessionInsert(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
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

	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(is))
	stats1 := h.GetTableStats(tableInfo1)
	require.Equal(t, int64(rowCount1), stats1.RealtimeCount)

	tbl2, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	require.NoError(t, err)
	tableInfo2 := tbl2.Meta()
	stats2 := h.GetTableStats(tableInfo2)
	require.Equal(t, int64(rowCount2), stats2.RealtimeCount)

	testKit.MustExec("analyze table t1")
	// Test update in a txn.
	for i := 0; i < rowCount1; i++ {
		testKit.MustExec("insert into t1 values(1, 2)")
	}
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(is))
	stats1 = h.GetTableStats(tableInfo1)
	require.Equal(t, int64(rowCount1*2), stats1.RealtimeCount)

	// Test IncreaseFactor.
	count, err := cardinality.ColumnEqualRowCount(testKit.Session().GetPlanCtx(), stats1, types.NewIntDatum(1), tableInfo1.Columns[0].ID)
	require.NoError(t, err)
	require.Equal(t, float64(rowCount1*2), count)

	testKit.MustExec("begin")
	for i := 0; i < rowCount1; i++ {
		testKit.MustExec("insert into t1 values(1, 2)")
	}
	testKit.MustExec("commit")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(is))
	stats1 = h.GetTableStats(tableInfo1)
	require.Equal(t, int64(rowCount1*3), stats1.RealtimeCount)

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
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(is))
	stats1 = h.GetTableStats(tableInfo1)
	require.Equal(t, int64(rowCount1*3), stats1.RealtimeCount)
	stats2 = h.GetTableStats(tableInfo2)
	require.Equal(t, int64(rowCount2), stats2.RealtimeCount)

	testKit.MustExec("begin")
	testKit.MustExec("delete from t1")
	testKit.MustExec("commit")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(is))
	stats1 = h.GetTableStats(tableInfo1)
	require.Equal(t, int64(0), stats1.RealtimeCount)

	rs := testKit.MustQuery("select modify_count from mysql.stats_meta")
	rs.Check(testkit.Rows("40", "70"))

	rs = testKit.MustQuery("select tot_col_size from mysql.stats_histograms").Sort()
	rs.Check(testkit.Rows("0", "0", "20", "20"))

	// test dump delta only when `modify count / count` is greater than the ratio.
	originValue := usage.DumpStatsDeltaRatio
	usage.DumpStatsDeltaRatio = 0.5
	defer func() {
		usage.DumpStatsDeltaRatio = originValue
	}()
	usage.DumpStatsDeltaRatio = 0.5
	for i := 0; i < rowCount1; i++ {
		testKit.MustExec("insert into t1 values (1,2)")
	}
	err = h.DumpStatsDeltaToKV(false)
	require.NoError(t, err)
	require.NoError(t, h.Update(is))
	stats1 = h.GetTableStats(tableInfo1)
	require.Equal(t, int64(rowCount1), stats1.RealtimeCount)

	// not dumped
	testKit.MustExec("insert into t1 values (1,2)")
	err = h.DumpStatsDeltaToKV(false)
	require.NoError(t, err)
	require.NoError(t, h.Update(is))
	stats1 = h.GetTableStats(tableInfo1)
	require.Equal(t, int64(rowCount1), stats1.RealtimeCount)

	h.FlushStats()
	require.NoError(t, h.Update(is))
	stats1 = h.GetTableStats(tableInfo1)
	require.Equal(t, int64(rowCount1+1), stats1.RealtimeCount)
}

func TestRollback(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
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
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(is))

	stats := h.GetTableStats(tableInfo)
	require.Equal(t, int64(0), stats.RealtimeCount)
	require.Equal(t, int64(0), stats.ModifyCount)
}

func TestMultiSession(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
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

	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(is))
	stats1 := h.GetTableStats(tableInfo1)
	require.Equal(t, int64(rowCount1), stats1.RealtimeCount)

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

	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(is))
	stats1 = h.GetTableStats(tableInfo1)
	require.Equal(t, int64(rowCount1*2), stats1.RealtimeCount)
	testKit.RefreshSession()
	rs := testKit.MustQuery("select modify_count from mysql.stats_meta")
	rs.Check(testkit.Rows("60"))
}

func TestTxnWithFailure(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
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
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(is))
	stats1 := h.GetTableStats(tableInfo1)
	// have not commit
	require.Equal(t, int64(0), stats1.RealtimeCount)
	testKit.MustExec("commit")

	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(is))
	stats1 = h.GetTableStats(tableInfo1)
	require.Equal(t, int64(rowCount1), stats1.RealtimeCount)

	_, err = testKit.Exec("insert into t1 values(0, 2)")
	require.Error(t, err)

	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(is))
	stats1 = h.GetTableStats(tableInfo1)
	require.Equal(t, int64(rowCount1), stats1.RealtimeCount)

	testKit.MustExec("insert into t1 values(-1, 2)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(is))
	stats1 = h.GetTableStats(tableInfo1)
	require.Equal(t, int64(rowCount1+1), stats1.RealtimeCount)
}

func TestUpdatePartition(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	pruneMode, err := util.GetCurrentPruneMode(dom.StatsHandle().SPool())
	require.NoError(t, err)
	testKit.MustQuery("select @@tidb_partition_prune_mode").Check(testkit.Rows(pruneMode))
	testKit.MustExec("use test")
	testkit.WithPruneMode(testKit, variable.Static, func() {
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
		require.NoError(t, h.DumpStatsDeltaToKV(true))
		require.NoError(t, h.Update(is))
		for _, def := range pi.Definitions {
			statsTbl := h.GetPartitionStats(tableInfo, def.ID)
			require.Equal(t, int64(1), statsTbl.ModifyCount)
			require.Equal(t, int64(1), statsTbl.RealtimeCount)
			require.Equal(t, int64(2), statsTbl.Columns[bColID].TotColSize)
		}

		testKit.MustExec(`update t set a = a + 1, b = "aa"`)
		require.NoError(t, h.DumpStatsDeltaToKV(true))
		require.NoError(t, h.Update(is))
		for _, def := range pi.Definitions {
			statsTbl := h.GetPartitionStats(tableInfo, def.ID)
			require.Equal(t, int64(2), statsTbl.ModifyCount)
			require.Equal(t, int64(1), statsTbl.RealtimeCount)
			require.Equal(t, int64(3), statsTbl.Columns[bColID].TotColSize)
		}

		testKit.MustExec("delete from t")
		require.NoError(t, h.DumpStatsDeltaToKV(true))
		require.NoError(t, h.Update(is))
		for _, def := range pi.Definitions {
			statsTbl := h.GetPartitionStats(tableInfo, def.ID)
			require.Equal(t, int64(3), statsTbl.ModifyCount)
			require.Equal(t, int64(0), statsTbl.RealtimeCount)
			require.Equal(t, int64(0), statsTbl.Columns[bColID].TotColSize)
		}
		// assert WithGetTableStatsByQuery get the same result
		for _, def := range pi.Definitions {
			statsTbl := h.GetPartitionStats(tableInfo, def.ID)
			require.Equal(t, int64(3), statsTbl.ModifyCount)
			require.Equal(t, int64(0), statsTbl.RealtimeCount)
			require.Equal(t, int64(0), statsTbl.Columns[bColID].TotColSize)
		}
	})
}

func TestAutoUpdate(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testkit.WithPruneMode(testKit, variable.Static, func() {
		testKit.MustExec("use test")
		testKit.MustExec("create table t (a varchar(20))")

		exec.AutoAnalyzeMinCnt = 0
		testKit.MustExec("set global tidb_auto_analyze_ratio = 0.2")
		defer func() {
			exec.AutoAnalyzeMinCnt = 1000
			testKit.MustExec("set global tidb_auto_analyze_ratio = 0.5")
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
		require.Equal(t, int64(0), stats.RealtimeCount)

		_, err = testKit.Exec("insert into t values ('ss'), ('ss'), ('ss'), ('ss'), ('ss')")
		require.NoError(t, err)
		require.NoError(t, h.DumpStatsDeltaToKV(true))
		require.NoError(t, h.Update(is))
		h.HandleAutoAnalyze()
		require.NoError(t, h.Update(is))
		stats = h.GetTableStats(tableInfo)
		require.Equal(t, int64(5), stats.RealtimeCount)
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
		require.NoError(t, h.DumpStatsDeltaToKV(true))
		require.NoError(t, h.Update(is))
		h.HandleAutoAnalyze()
		require.NoError(t, h.Update(is))
		stats = h.GetTableStats(tableInfo)
		require.Equal(t, int64(6), stats.RealtimeCount)
		require.Equal(t, int64(1), stats.ModifyCount)

		_, err = testKit.Exec("insert into t values ('fff')")
		require.NoError(t, err)
		require.NoError(t, h.DumpStatsDeltaToKV(true))
		require.NoError(t, h.Update(is))
		h.HandleAutoAnalyze()
		require.NoError(t, h.Update(is))
		stats = h.GetTableStats(tableInfo)
		require.Equal(t, int64(7), stats.RealtimeCount)
		require.Equal(t, int64(0), stats.ModifyCount)

		_, err = testKit.Exec("insert into t values ('eee')")
		require.NoError(t, err)
		require.NoError(t, h.DumpStatsDeltaToKV(true))
		require.NoError(t, h.Update(is))
		h.HandleAutoAnalyze()
		require.NoError(t, h.Update(is))
		stats = h.GetTableStats(tableInfo)
		require.Equal(t, int64(8), stats.RealtimeCount)
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
		h.HandleAutoAnalyze()
		require.NoError(t, h.Update(is))
		testKit.MustExec("explain select * from t where a > 'a'")
		require.NoError(t, h.LoadNeededHistograms())
		stats = h.GetTableStats(tableInfo)
		require.Equal(t, int64(8), stats.RealtimeCount)
		require.Equal(t, int64(0), stats.ModifyCount)
		hg, ok := stats.Indices[tableInfo.Indices[0].ID]
		require.True(t, ok)
		require.Equal(t, int64(3), hg.NDV)
		require.Equal(t, 0, hg.Len())
		require.Equal(t, 3, hg.TopN.Num())
	})
}

func TestAutoUpdatePartition(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testkit.WithPruneMode(testKit, variable.Static, func() {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t")
		testKit.MustExec("create table t (a int) PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (6))")
		testKit.MustExec("analyze table t")

		exec.AutoAnalyzeMinCnt = 0
		testKit.MustExec("set global tidb_auto_analyze_ratio = 0.6")
		defer func() {
			exec.AutoAnalyzeMinCnt = 1000
			testKit.MustExec("set global tidb_auto_analyze_ratio = 0.5")
		}()

		do := dom
		is := do.InfoSchema()
		tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
		require.NoError(t, err)
		tableInfo := tbl.Meta()
		pi := tableInfo.GetPartitionInfo()
		h := do.StatsHandle()

		require.NoError(t, h.Update(is))
		stats := h.GetPartitionStats(tableInfo, pi.Definitions[0].ID)
		require.Equal(t, int64(0), stats.RealtimeCount)

		testKit.MustExec("insert into t values (1)")
		require.NoError(t, h.DumpStatsDeltaToKV(true))
		require.NoError(t, h.Update(is))
		h.HandleAutoAnalyze()
		stats = h.GetPartitionStats(tableInfo, pi.Definitions[0].ID)
		require.Equal(t, int64(1), stats.RealtimeCount)
		require.Equal(t, int64(0), stats.ModifyCount)
	})
}

func TestIssue25700(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
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
	tk.MustExec("INSERT INTO `t` (`ldecimal`, `rdecimal`, `col_timestamp`) VALUES (2265.2200, 9843.4100, '1999-12-31 16:00:00')" + strings.Repeat(", (2265.2200, 9843.4100, '1999-12-31 16:00:00')", int(exec.AutoAnalyzeMinCnt)))
	require.NoError(t, dom.StatsHandle().DumpStatsDeltaToKV(true))
	require.NoError(t, dom.StatsHandle().Update(dom.InfoSchema()))

	require.True(t, dom.StatsHandle().HandleAutoAnalyze())
	require.Equal(t, "finished", tk.MustQuery("show analyze status").Rows()[1][7])
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
	sc := new(stmtctx.StatementContext)
	sc.SetTimeZone(time.UTC)
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
		ranges, _ = h.SplitRange(sc, ranges, false)
		var ranStrs []string
		for _, ran := range ranges {
			ranStrs = append(ranStrs, ran.String())
		}
		require.Equal(t, test.result, strings.Join(ranStrs, ","))
	}
}

func TestOutOfOrderUpdate(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
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
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	testKit.MustExec(fmt.Sprintf("update mysql.stats_meta set count = 1 where table_id = %d", tableInfo.ID))

	testKit.MustExec("delete from t")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	// If count < -Delta, then update count to 0.
	// Check https://github.com/pingcap/tidb/pull/38301#discussion_r1094050951 for details.
	testKit.MustQuery(fmt.Sprintf("select count from mysql.stats_meta where table_id = %d", tableInfo.ID)).Check(testkit.Rows("0"))

	// Now another tidb has updated the delta info.
	testKit.MustExec(fmt.Sprintf("update mysql.stats_meta set count = 3 where table_id = %d", tableInfo.ID))

	require.NoError(t, h.DumpStatsDeltaToKV(true))
	testKit.MustQuery(fmt.Sprintf("select count from mysql.stats_meta where table_id = %d", tableInfo.ID)).Check(testkit.Rows("3"))
}

func TestLoadHistCorrelation(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	h := dom.StatsHandle()
	origLease := h.Lease()
	h.SetLease(time.Second)
	defer func() { h.SetLease(origLease) }()
	testKit.MustExec("use test")
	testKit.MustExec("create table t(c int)")
	testKit.MustExec("insert into t values(1),(2),(3),(4),(5)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
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

func BenchmarkHandleAutoAnalyze(b *testing.B) {
	store, dom := testkit.CreateMockStoreAndDomain(b)
	testKit := testkit.NewTestKit(b, store)
	testKit.MustExec("use test")
	h := dom.StatsHandle()
	for i := 0; i < b.N; i++ {
		h.HandleAutoAnalyze()
	}
}

// subtraction parses the number for counter and returns new - old.
// string for counter will be `label:<name:"type" value:"ok" > counter:<value:0 > `
func subtraction(newMetric *dto.Metric, oldMetric *dto.Metric) int {
	return int(*(newMetric.Counter.Value) - *(oldMetric.Counter.Value))
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

func TestStatsVariables(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	h := dom.StatsHandle()
	sctx := tk.Session().(sessionctx.Context)

	pruneMode, err := util.GetCurrentPruneMode(h.SPool())
	require.NoError(t, err)
	require.Equal(t, string(variable.Dynamic), pruneMode)
	err = util.UpdateSCtxVarsForStats(sctx)
	require.NoError(t, err)
	require.Equal(t, 2, sctx.GetSessionVars().AnalyzeVersion)
	require.Equal(t, false, sctx.GetSessionVars().EnableHistoricalStats)
	require.Equal(t, string(variable.Dynamic), sctx.GetSessionVars().PartitionPruneMode.Load())
	require.Equal(t, false, sctx.GetSessionVars().EnableAnalyzeSnapshot)
	require.Equal(t, true, sctx.GetSessionVars().SkipMissingPartitionStats)

	tk.MustExec(`set global tidb_analyze_version=1`)
	tk.MustExec(`set global tidb_partition_prune_mode='static'`)
	tk.MustExec(`set global tidb_enable_historical_stats=1`)
	tk.MustExec(`set global tidb_enable_analyze_snapshot=1`)
	tk.MustExec(`set global tidb_skip_missing_partition_stats=0`)

	pruneMode, err = util.GetCurrentPruneMode(h.SPool())
	require.NoError(t, err)
	require.Equal(t, string(variable.Static), pruneMode)
	err = util.UpdateSCtxVarsForStats(sctx)
	require.NoError(t, err)
	require.Equal(t, 1, sctx.GetSessionVars().AnalyzeVersion)
	require.Equal(t, true, sctx.GetSessionVars().EnableHistoricalStats)
	require.Equal(t, string(variable.Static), sctx.GetSessionVars().PartitionPruneMode.Load())
	require.Equal(t, true, sctx.GetSessionVars().EnableAnalyzeSnapshot)
	require.Equal(t, false, sctx.GetSessionVars().SkipMissingPartitionStats)
}

func TestAutoUpdatePartitionInDynamicOnlyMode(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
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
		require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))

		testKit.MustExec("insert into t values (1, 'a'), (2, 'b'), (11, 'c'), (12, 'd'), (21, 'e'), (22, 'f')")
		require.NoError(t, h.DumpStatsDeltaToKV(true))
		testKit.MustExec("set @@tidb_analyze_version = 2")
		testKit.MustExec("analyze table t")

		exec.AutoAnalyzeMinCnt = 0
		testKit.MustExec("set global tidb_auto_analyze_ratio = 0.1")
		defer func() {
			exec.AutoAnalyzeMinCnt = 1000
			testKit.MustExec("set global tidb_auto_analyze_ratio = 0.5")
		}()

		require.NoError(t, h.Update(is))
		tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
		require.NoError(t, err)
		tableInfo := tbl.Meta()
		pi := tableInfo.GetPartitionInfo()
		globalStats := h.GetTableStats(tableInfo)
		partitionStats := h.GetPartitionStats(tableInfo, pi.Definitions[0].ID)
		require.Equal(t, int64(6), globalStats.RealtimeCount)
		require.Equal(t, int64(0), globalStats.ModifyCount)
		require.Equal(t, int64(2), partitionStats.RealtimeCount)
		require.Equal(t, int64(0), partitionStats.ModifyCount)

		testKit.MustExec("insert into t values (3, 'g')")
		require.NoError(t, h.DumpStatsDeltaToKV(true))
		require.NoError(t, h.Update(is))
		globalStats = h.GetTableStats(tableInfo)
		partitionStats = h.GetPartitionStats(tableInfo, pi.Definitions[0].ID)
		require.Equal(t, int64(7), globalStats.RealtimeCount)
		require.Equal(t, int64(1), globalStats.ModifyCount)
		require.Equal(t, int64(3), partitionStats.RealtimeCount)
		require.Equal(t, int64(1), partitionStats.ModifyCount)

		h.HandleAutoAnalyze()
		require.NoError(t, h.Update(is))
		globalStats = h.GetTableStats(tableInfo)
		partitionStats = h.GetPartitionStats(tableInfo, pi.Definitions[0].ID)
		require.Equal(t, int64(7), globalStats.RealtimeCount)
		require.Equal(t, int64(0), globalStats.ModifyCount)
		require.Equal(t, int64(3), partitionStats.RealtimeCount)
		require.Equal(t, int64(0), partitionStats.ModifyCount)
	})
}

func TestAutoAnalyzeRatio(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	oriStart := tk.MustQuery("select @@tidb_auto_analyze_start_time").Rows()[0][0].(string)
	oriEnd := tk.MustQuery("select @@tidb_auto_analyze_end_time").Rows()[0][0].(string)
	exec.AutoAnalyzeMinCnt = 0
	defer func() {
		exec.AutoAnalyzeMinCnt = 1000
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_start_time='%v'", oriStart))
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_end_time='%v'", oriEnd))
	}()

	h := dom.StatsHandle()
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	tk.MustExec("insert into t values (1)" + strings.Repeat(", (1)", 19))
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	is := dom.InfoSchema()
	require.NoError(t, h.Update(is))
	// To pass the stats.Pseudo check in autoAnalyzeTable
	tk.MustExec("analyze table t")
	tk.MustExec("explain select * from t where a = 1")
	require.NoError(t, h.LoadNeededHistograms())
	tk.MustExec("set global tidb_auto_analyze_start_time='00:00 +0000'")
	tk.MustExec("set global tidb_auto_analyze_end_time='23:59 +0000'")

	getStatsHealthy := func() int {
		rows := tk.MustQuery("show stats_healthy where db_name = 'test' and table_name = 't'").Rows()
		require.Len(t, rows, 1)
		healthy, err := strconv.Atoi(rows[0][3].(string))
		require.NoError(t, err)
		return healthy
	}

	tk.MustExec("insert into t values (1)" + strings.Repeat(", (1)", 10))
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(is))
	require.Equal(t, getStatsHealthy(), 44)
	require.True(t, h.HandleAutoAnalyze())

	tk.MustExec("delete from t limit 12")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(is))
	require.Equal(t, getStatsHealthy(), 61)
	require.False(t, h.HandleAutoAnalyze())

	tk.MustExec("delete from t limit 4")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(is))
	require.Equal(t, getStatsHealthy(), 48)
	require.True(t, h.HandleAutoAnalyze())
}

func TestDumpColumnStatsUsage(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
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
	require.Equal(t, []any{"test", "t1", "", "a"}, rows[0][:4])
	require.True(t, rows[0][4].(string) != "<nil>")
	require.True(t, rows[0][5].(string) == "<nil>")
	rows = tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't2'").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, []any{"test", "t2", "", "b"}, rows[0][:4])
	require.True(t, rows[0][4].(string) != "<nil>")
	require.True(t, rows[0][5].(string) == "<nil>")

	tk.MustExec("analyze table t1")
	tk.MustExec("select * from t1 where b > 1")
	require.NoError(t, h.DumpColStatsUsageToKV())
	// t1.a updates last_used_at first and then updates last_analyzed_at while t1.b updates last_analyzed_at first and then updates last_used_at.
	// Check both of them behave as expected.
	rows = tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't1'").Rows()
	require.Len(t, rows, 2)
	require.Equal(t, []any{"test", "t1", "", "a"}, rows[0][:4])
	require.True(t, rows[0][4].(string) != "<nil>")
	require.True(t, rows[0][5].(string) != "<nil>")
	require.Equal(t, []any{"test", "t1", "", "b"}, rows[1][:4])
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
		require.Equal(t, []any{"test", "t3", "global", "a"}, rows[0][:4])
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
	require.Equal(t, []any{"test", "t1", "", "b"}, rows[0][:4])
	require.True(t, rows[0][4].(string) != "<nil>")
	require.True(t, rows[0][5].(string) == "<nil>")
	rows = tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't2'").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, []any{"test", "t2", "", "a"}, rows[0][:4])
	require.True(t, rows[0][4].(string) != "<nil>")
	require.True(t, rows[0][5].(string) == "<nil>")
}

func TestCollectPredicateColumnsFromExecute(t *testing.T) {
	for _, val := range []bool{false, true} {
		func(planCache bool) {
			store, dom := testkit.CreateMockStoreAndDomain(t)
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("set tidb_enable_prepared_plan_cache=" + variable.BoolToOnOff(planCache))

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
			require.Equal(t, []any{"test", "t1", "", "a"}, rows[0][:4])
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
				require.Equal(t, []any{"test", "t1", "", "a"}, rows[0][:4])
				require.True(t, rows[0][4].(string) != "<nil>")
				require.True(t, rows[0][5].(string) == "<nil>")
			}
		}(val)
	}
}

func TestEnableAndDisableColumnTracking(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
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

func TestStatsLockUnlockForAutoAnalyze(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	oriStart := tk.MustQuery("select @@tidb_auto_analyze_start_time").Rows()[0][0].(string)
	oriEnd := tk.MustQuery("select @@tidb_auto_analyze_end_time").Rows()[0][0].(string)
	exec.AutoAnalyzeMinCnt = 0
	defer func() {
		exec.AutoAnalyzeMinCnt = 1000
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_start_time='%v'", oriStart))
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_end_time='%v'", oriEnd))
	}()

	h := dom.StatsHandle()
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	tk.MustExec("insert into t values (1)" + strings.Repeat(", (1)", 19))
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	is := dom.InfoSchema()
	require.NoError(t, h.Update(is))
	// To pass the stats.Pseudo check in autoAnalyzeTable
	tk.MustExec("analyze table t")
	tk.MustExec("explain select * from t where a = 1")
	require.NoError(t, h.LoadNeededHistograms())
	tk.MustExec("set global tidb_auto_analyze_start_time='00:00 +0000'")
	tk.MustExec("set global tidb_auto_analyze_end_time='23:59 +0000'")

	tk.MustExec("insert into t values (1)" + strings.Repeat(", (1)", 10))
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(is))
	require.True(t, h.HandleAutoAnalyze())

	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.Nil(t, err)

	tblStats := h.GetTableStats(tbl.Meta())
	for _, col := range tblStats.Columns {
		require.True(t, col.IsStatsInitialized())
	}

	tk.MustExec("lock stats t")

	tk.MustExec("delete from t limit 12")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(is))
	require.False(t, h.HandleAutoAnalyze())

	tblStats1 := h.GetTableStats(tbl.Meta())
	require.Equal(t, tblStats, tblStats1)

	tk.MustExec("unlock stats t")

	tk.MustExec("delete from t limit 4")

	rows := tk.MustQuery("select count(*) from t").Rows()
	num, _ := strconv.Atoi(rows[0][0].(string))
	require.Equal(t, num, 15)

	tk.MustExec("analyze table t")

	tblStats2 := h.GetTableStats(tbl.Meta())
	require.Equal(t, int64(15), tblStats2.RealtimeCount)
}

func TestStatsLockForDelta(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("set @@session.tidb_analyze_version = 1")
	testKit.MustExec("create table t1 (c1 int, c2 int)")
	testKit.MustExec("create table t2 (c1 int, c2 int)")

	is := dom.InfoSchema()
	tbl1, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	tableInfo1 := tbl1.Meta()
	h := dom.StatsHandle()

	testKit.MustExec("lock stats t1")

	rowCount1 := 10
	rowCount2 := 20
	for i := 0; i < rowCount1; i++ {
		testKit.MustExec("insert into t1 values(1, 2)")
	}
	for i := 0; i < rowCount2; i++ {
		testKit.MustExec("insert into t2 values(1, 2)")
	}

	err = h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)

	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(is))
	stats1 := h.GetTableStats(tableInfo1)
	require.Equal(t, stats1.RealtimeCount, int64(0))

	tbl2, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	require.NoError(t, err)
	tableInfo2 := tbl2.Meta()
	stats2 := h.GetTableStats(tableInfo2)
	require.Equal(t, int64(rowCount2), stats2.RealtimeCount)

	testKit.MustExec("analyze table t1")
	for i := 0; i < rowCount1; i++ {
		testKit.MustExec("insert into t1 values(1, 2)")
	}
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(is))
	stats1 = h.GetTableStats(tableInfo1)
	require.Equal(t, stats1.RealtimeCount, int64(0))

	testKit.MustExec("unlock stats t1")

	testKit.MustExec("analyze table t1")
	stats1 = h.GetTableStats(tableInfo1)
	require.Equal(t, int64(20), stats1.RealtimeCount)

	for i := 0; i < rowCount1; i++ {
		testKit.MustExec("insert into t1 values(1, 2)")
	}
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(is))
	stats1 = h.GetTableStats(tableInfo1)
	require.Equal(t, int64(30), stats1.RealtimeCount)
}

func TestFillMissingStatsMeta(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int, b int)")
	tk.MustExec("create table t2 (a int, b int) partition by range (a) (partition p0 values less than (10), partition p1 values less than (maxvalue))")

	tk.MustQuery("select * from mysql.stats_meta").Check(testkit.Rows())

	is := dom.InfoSchema()
	tbl1, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	tbl1ID := tbl1.Meta().ID
	tbl2, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	require.NoError(t, err)
	tbl2Info := tbl2.Meta()
	tbl2ID := tbl2Info.ID
	require.Len(t, tbl2Info.Partition.Definitions, 2)
	p0ID := tbl2Info.Partition.Definitions[0].ID
	p1ID := tbl2Info.Partition.Definitions[1].ID
	h := dom.StatsHandle()

	checkStatsMeta := func(id int64, expectedModifyCount, expectedCount string) int64 {
		rows := tk.MustQuery(fmt.Sprintf("select version, modify_count, count from mysql.stats_meta where table_id = %v", id)).Rows()
		require.Len(t, rows, 1)
		ver, err := strconv.ParseInt(rows[0][0].(string), 10, 64)
		require.NoError(t, err)
		require.Equal(t, expectedModifyCount, rows[0][1])
		require.Equal(t, expectedCount, rows[0][2])
		return ver
	}

	tk.MustExec("insert into t1 values (1, 2), (3, 4)")
	require.NoError(t, h.DumpStatsDeltaToKV(false))
	require.NoError(t, h.Update(is))
	ver1 := checkStatsMeta(tbl1ID, "2", "2")
	tk.MustExec("delete from t1 where a = 1")
	require.NoError(t, h.DumpStatsDeltaToKV(false))
	require.NoError(t, h.Update(is))
	ver2 := checkStatsMeta(tbl1ID, "3", "1")
	require.Greater(t, ver2, ver1)

	tk.MustExec("insert into t2 values (1, 2), (3, 4)")
	require.NoError(t, h.DumpStatsDeltaToKV(false))
	require.NoError(t, h.Update(is))
	checkStatsMeta(p0ID, "2", "2")
	globalVer1 := checkStatsMeta(tbl2ID, "2", "2")
	tk.MustExec("insert into t2 values (11, 12)")
	require.NoError(t, h.DumpStatsDeltaToKV(false))
	require.NoError(t, h.Update(is))
	checkStatsMeta(p1ID, "1", "1")
	globalVer2 := checkStatsMeta(tbl2ID, "3", "3")
	require.Greater(t, globalVer2, globalVer1)
}

func TestNotDumpSysTable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int, b int)")
	h := dom.StatsHandle()
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	tk.MustQuery("select count(1) from mysql.stats_meta").Check(testkit.Rows("1"))
	// After executing `delete from mysql.stats_meta`, a delta for mysql.stats_meta is created but it would not be dumped.
	tk.MustExec("delete from mysql.stats_meta")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("mysql"), model.NewCIStr("stats_meta"))
	require.NoError(t, err)
	tblID := tbl.Meta().ID
	tk.MustQuery(fmt.Sprintf("select * from mysql.stats_meta where table_id = %v", tblID)).Check(testkit.Rows())
}

func TestAutoAnalyzePartitionTableAfterAddingIndex(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	oriMinCnt := exec.AutoAnalyzeMinCnt
	oriStart := tk.MustQuery("select @@tidb_auto_analyze_start_time").Rows()[0][0].(string)
	oriEnd := tk.MustQuery("select @@tidb_auto_analyze_end_time").Rows()[0][0].(string)
	defer func() {
		exec.AutoAnalyzeMinCnt = oriMinCnt
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_start_time='%v'", oriStart))
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_end_time='%v'", oriEnd))
	}()
	exec.AutoAnalyzeMinCnt = 0
	tk.MustExec("set global tidb_auto_analyze_start_time='00:00 +0000'")
	tk.MustExec("set global tidb_auto_analyze_end_time='23:59 +0000'")
	tk.MustExec("set global tidb_analyze_version = 2")
	tk.MustExec("set global tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int) partition by range (a) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN MAXVALUE)")
	h := dom.StatsHandle()
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	tk.MustExec("insert into t values (1,2), (3,4), (11,12),(13,14)")
	tk.MustExec("set session tidb_analyze_version = 2")
	tk.MustExec("set session tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec("analyze table t")
	require.False(t, h.HandleAutoAnalyze())
	tk.MustExec("alter table t add index idx(a)")
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	idxInfo := tblInfo.Indices[0]
	require.Nil(t, h.GetTableStats(tblInfo).Indices[idxInfo.ID])
	require.True(t, h.HandleAutoAnalyze())
	require.NotNil(t, h.GetTableStats(tblInfo).Indices[idxInfo.ID])
}
