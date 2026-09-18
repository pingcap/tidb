// Copyright 2023 PingCAP, Inc.
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

package analyze

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	statstestutil "github.com/pingcap/tidb/pkg/statistics/handle/ddl/testutil"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/analyzehelper"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// nolint:unused
func checkForGlobalStatsWithOpts(t *testing.T, dom *domain.Domain, db, tt, pp string, topn, buckets int) {
	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr(db), ast.NewCIStr(tt))
	require.NoError(t, err)

	tblInfo := tbl.Meta()
	physicalID := tblInfo.ID
	if pp != "global" {
		for _, def := range tbl.Meta().GetPartitionInfo().Definitions {
			if def.Name.L == pp {
				physicalID = def.ID
			}
		}
	}
	tblStats, err := dom.StatsHandle().TableStatsFromStorage(tblInfo, physicalID, true, 0)
	require.NoError(t, err)

	delta := buckets/2 + 10
	tblStats.ForEachIndexImmutable(func(_ int64, idxStats *statistics.Index) bool {
		if len(idxStats.Buckets) == 0 {
			return false // it's not loaded
		}
		numTopN := idxStats.TopN.Num()
		numBuckets := len(idxStats.Buckets)
		// since the hist-building algorithm doesn't stipulate the final bucket number to be equal to the expected number exactly,
		// we have to check the results by a range here.
		require.Equal(t, topn, numTopN)
		require.GreaterOrEqual(t, numBuckets, buckets-delta)
		require.LessOrEqual(t, numBuckets, buckets+delta)
		return false
	})
	tblStats.ForEachColumnImmutable(func(_ int64, colStats *statistics.Column) bool {
		if len(colStats.Buckets) == 0 {
			return false // it's not loaded
		}
		numTopN := colStats.TopN.Num()
		numBuckets := len(colStats.Buckets)
		require.Equal(t, topn, numTopN)
		require.GreaterOrEqual(t, numBuckets, buckets-delta)
		require.LessOrEqual(t, numBuckets, buckets+delta)
		return false
	})
}

// nolint:unused
func prepareForGlobalStatsWithOptsV2(t *testing.T, dom *domain.Domain, tk *testkit.TestKit, tblName, dbName string) {
	tk.MustExec("create database if not exists " + dbName)
	tk.MustExec("use " + dbName)
	tk.MustExec("drop table if exists " + tblName)
	tk.MustExec(` create table ` + tblName + ` (a int, key(a)) partition by range (a) ` +
		`(partition p0 values less than (100000), partition p1 values less than (200000))`)
	buf1 := bytes.NewBufferString("insert into " + tblName + " values (0)")
	buf2 := bytes.NewBufferString("insert into " + tblName + " values (100000)")
	for range 1000 {
		buf1.WriteString(fmt.Sprintf(", (%v)", 2))
		buf2.WriteString(fmt.Sprintf(", (%v)", 100002))
		buf1.WriteString(fmt.Sprintf(", (%v)", 1))
		buf2.WriteString(fmt.Sprintf(", (%v)", 100001))
		buf1.WriteString(fmt.Sprintf(", (%v)", 0))
		buf2.WriteString(fmt.Sprintf(", (%v)", 100000))
	}
	for i := 0; i < 5000; i += 3 {
		buf1.WriteString(fmt.Sprintf(", (%v)", i))
		buf2.WriteString(fmt.Sprintf(", (%v)", 100000+i))
	}
	tk.MustExec(buf1.String())
	tk.MustExec(buf2.String())
	tk.MustExec("set @@tidb_analyze_version=2")
	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	tk.MustExec("flush stats_delta *.*")
}

// nolint:unused
func prepareForGlobalStatsWithOpts(t *testing.T, dom *domain.Domain, tk *testkit.TestKit, tblName, dbName string) {
	tk.MustExec("create database if not exists " + dbName)
	tk.MustExec("use " + dbName)
	tk.MustExec("drop table if exists " + tblName)
	tk.MustExec(` create table ` + tblName + ` (a int, key(a)) partition by range (a) ` +
		`(partition p0 values less than (100000), partition p1 values less than (200000))`)
	buf1 := bytes.NewBufferString("insert into " + tblName + " values (0)")
	buf2 := bytes.NewBufferString("insert into " + tblName + " values (100000)")
	for i := 0; i < 5000; i += 3 {
		buf1.WriteString(fmt.Sprintf(", (%v)", i))
		buf2.WriteString(fmt.Sprintf(", (%v)", 100000+i))
	}
	for range 1000 {
		buf1.WriteString(fmt.Sprintf(", (%v)", 0))
		buf2.WriteString(fmt.Sprintf(", (%v)", 100000))
	}
	tk.MustExec(buf1.String())
	tk.MustExec(buf2.String())
	tk.MustExec("set @@tidb_analyze_version=2")
	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	tk.MustExec("flush stats_delta *.*")
}

func TestAnalyzeVirtualCol(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int generated always as (-a) virtual, c int generated always as (-a) stored, index (c))")
	tk.MustExec("insert into t(a) values(2),(1),(1),(3),(NULL)")
	analyzehelper.TriggerPredicateColumnsCollection(t, tk, store, "t", "a", "b", "c")
	tk.MustExec("set @@tidb_analyze_version = 2")
	tk.MustExec("analyze table t")
	require.Len(t, tk.MustQuery("show stats_histograms where table_name ='t'").Rows(), 3)
}

func TestAnalyzeGlobalStatsWithOpts1(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	prepareForGlobalStatsWithOpts(t, dom, tk, "test_gstats_opt", "test_gstats_opt")

	// nolint:unused
	type opt struct {
		topn    int
		buckets int
		err     bool
	}

	cases := []opt{
		{1, 37, false},
		{2, 47, false},
		{10, 77, false},
		{77, 219, false},
		{-31, 222, true},
		{10, -77, true},
		{100001, 47, true},
		{77, 100001, true},
	}
	for _, ca := range cases {
		sql := fmt.Sprintf("analyze table test_gstats_opt with %v topn, %v buckets", ca.topn, ca.buckets)
		if !ca.err {
			tk.MustExec(sql)
			checkForGlobalStatsWithOpts(t, dom, "test_gstats_opt", "test_gstats_opt", "global", ca.topn, ca.buckets)
			checkForGlobalStatsWithOpts(t, dom, "test_gstats_opt", "test_gstats_opt", "p0", ca.topn, ca.buckets)
			checkForGlobalStatsWithOpts(t, dom, "test_gstats_opt", "test_gstats_opt", "p1", ca.topn, ca.buckets)
		} else {
			err := tk.ExecToErr(sql)
			require.Error(t, err)
		}
	}
}

func TestAnalyzeGlobalStatsWithOpts2(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	originalVal1 := tk.MustQuery("select @@tidb_persist_analyze_options").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_persist_analyze_options = %v", originalVal1))
	}()
	tk.MustExec("set global tidb_persist_analyze_options=false")
	prepareForGlobalStatsWithOptsV2(t, dom, tk, "test_gstats_opt2", "test_gstats_opt2")

	tk.MustExec("analyze table test_gstats_opt2 with 2 topn, 10 buckets, 1000 samples")
	checkForGlobalStatsWithOpts(t, dom, "test_gstats_opt2", "test_gstats_opt2", "global", 2, 10)
	checkForGlobalStatsWithOpts(t, dom, "test_gstats_opt2", "test_gstats_opt2", "p0", 2, 10)
	checkForGlobalStatsWithOpts(t, dom, "test_gstats_opt2", "test_gstats_opt2", "p1", 2, 10)

	// analyze a partition to let its options be different with others'
	tk.MustExec("analyze table test_gstats_opt2 partition p0 with 3 topn, 20 buckets")
	checkForGlobalStatsWithOpts(t, dom, "test_gstats_opt2", "test_gstats_opt2", "global", 3, 20) // use new options
	checkForGlobalStatsWithOpts(t, dom, "test_gstats_opt2", "test_gstats_opt2", "p0", 3, 20)
	checkForGlobalStatsWithOpts(t, dom, "test_gstats_opt2", "test_gstats_opt2", "p1", 2, 10)

	tk.MustExec("analyze table test_gstats_opt2 partition p1 with 1 topn, 15 buckets")
	checkForGlobalStatsWithOpts(t, dom, "test_gstats_opt2", "test_gstats_opt2", "global", 1, 15)
	checkForGlobalStatsWithOpts(t, dom, "test_gstats_opt2", "test_gstats_opt2", "p0", 3, 20)
	checkForGlobalStatsWithOpts(t, dom, "test_gstats_opt2", "test_gstats_opt2", "p1", 1, 15)

	tk.MustExec("analyze table test_gstats_opt2 partition p0 with 2 topn, 10 buckets") // change back to 2 topn
	checkForGlobalStatsWithOpts(t, dom, "test_gstats_opt2", "test_gstats_opt2", "global", 2, 10)
	checkForGlobalStatsWithOpts(t, dom, "test_gstats_opt2", "test_gstats_opt2", "p0", 2, 10)
	checkForGlobalStatsWithOpts(t, dom, "test_gstats_opt2", "test_gstats_opt2", "p1", 1, 15)
}

func TestAnalyzeWithDynamicPartitionPruneMode(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_partition_prune_mode = '" + string(variable.Dynamic) + "'")
	tk.MustExec("set @@tidb_analyze_version = 2")
	tk.MustExec("set @@global.tidb_enable_auto_analyze='OFF'")
	tk.MustExec(`create table t (a int, key(a)) partition by range(a)
					(partition p0 values less than (10),
					partition p1 values less than (22))`)
	tk.MustExec(`insert into t values (1), (2), (3), (10), (11)`)
	tk.MustExec(`analyze table t with 1 topn, 2 buckets`)
	rows := tk.MustQuery("show stats_buckets where partition_name = 'global' and is_index=1").Rows()
	require.Len(t, rows, 2)
	require.Equal(t, "4", rows[1][6])
	tk.MustExec("insert into t values (1), (2), (2)")
	tk.MustExec("analyze table t partition p0 with 1 topn, 2 buckets")
	rows = tk.MustQuery("show stats_buckets where partition_name = 'global' and is_index=1").Rows()
	require.Len(t, rows, 2)
	require.Equal(t, "5", rows[1][6])
	tk.MustExec("insert into t values (3)")
	tk.MustExec("analyze table t partition p0 index a with 1 topn, 2 buckets")
	rows = tk.MustQuery("show stats_buckets where partition_name = 'global' and is_index=1").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "6", rows[0][6])
	tk.MustExec("set @@global.tidb_enable_auto_analyze=DEFAULT")
}

func TestFMSWithAnalyzePartition(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_partition_prune_mode = '" + string(variable.Dynamic) + "'")
	tk.MustExec("set @@tidb_analyze_version = 2")
	tk.MustExec(`create table t (a int, key(a)) partition by range(a)
					(partition p0 values less than (10),
					partition p1 values less than (22))`)
	tk.MustExec(`insert into t values (1), (2), (3), (10), (11)`)
	tk.MustQuery("select count(*) from mysql.stats_fm_sketch").Check(testkit.Rows("0"))
	tk.MustExec("analyze table t partition p0 with 1 topn, 2 buckets")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
		"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t's partition p0, reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\"",
		"Warning 1105 Ignore columns and options when analyze partition in dynamic mode",
	))
	tk.MustQuery("select count(*) from mysql.stats_fm_sketch").Check(testkit.Rows("2"))
}

func TestAnalyzeMetricsCounters(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	readCounter := func(counter prometheus.Counter) float64 {
		var metric dto.Metric
		require.NoError(t, counter.Write(&metric))
		return metric.Counter.GetValue()
	}

	manualSucc := metrics.ManualAnalyzeCounter.WithLabelValues("succ")
	manualFail := metrics.ManualAnalyzeCounter.WithLabelValues("failed")
	autoSucc := metrics.AutoAnalyzeCounter.WithLabelValues("succ")
	autoFail := metrics.AutoAnalyzeCounter.WithLabelValues("failed")

	h := dom.StatsHandle()

	tk.MustExec("create table t_metrics_manual(a int)")
	require.NoError(t, statstestutil.HandleNextDDLEventWithTxn(h))
	tk.MustExec("insert into t_metrics_manual values (1),(2)")

	beforeManualSucc := readCounter(manualSucc)
	beforeManualFail := readCounter(manualFail)

	tk.MustExec("analyze table t_metrics_manual")

	require.Equal(t, beforeManualSucc+1, readCounter(manualSucc))
	require.Equal(t, beforeManualFail, readCounter(manualFail))

	tk.MustExec("create table t_metrics_auto(a int)")
	require.NoError(t, statstestutil.HandleNextDDLEventWithTxn(h))
	tk.MustExec("insert into t_metrics_auto values (1)")
	tk.MustExec("flush stats_delta *.*")
	require.NoError(t, h.Update(context.Background(), dom.InfoSchema()))

	origMinCnt := statistics.AutoAnalyzeMinCnt
	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = origMinCnt
	}()

	// Keep this test scoped to the single unanalyzed table created above. The
	// global default may schedule multiple auto-analyze jobs in parallel.
	autoAnalyzeConcurrencySysVar := variable.GetSysVar("tidb_auto_analyze_concurrency")
	require.NoError(t, autoAnalyzeConcurrencySysVar.SetGlobalFromHook(context.Background(), tk.Session().GetSessionVars(), "1", false))
	beforeAutoSucc := readCounter(autoSucc)
	beforeAutoFail := readCounter(autoFail)

	require.True(t, h.HandleAutoAnalyze())

	require.Equal(t, beforeAutoSucc+1, readCounter(autoSucc))
	require.Equal(t, beforeAutoFail, readCounter(autoFail))
}

// TestTimestampStatsAreTimeZoneIndependent is a regression test for issue #52429: the histogram
// bucket bounds of a TIMESTAMP column used to be stored as a naked datetime string in the time zone
// of the session that ran ANALYZE, while TopN and index bounds were stored in UTC. Both the output
// of SHOW STATS_BUCKETS and the row count estimation were therefore wrong for any session whose
// time zone differed from the one that collected the statistics.
func TestTimestampStatsAreTimeZoneIndependent(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@time_zone = '+08:00'")
	tk.MustExec("create table t (a timestamp)")
	// Four distinct values, 512 rows each, so that they end up in the histogram and not in TopN.
	tk.MustExec(`insert into t values ('2024-04-08 10:00:01'), ('2024-04-08 10:00:02'),
		('2024-04-08 10:00:03'), ('2024-04-08 10:00:04')`)
	for range 9 {
		tk.MustExec("insert into t select * from t")
	}
	tk.MustExec("analyze table t with 0 topn")

	// The bounds are stored in UTC, 8 hours behind the session that ran ANALYZE.
	tblID := external.GetTableByName(t, tk, "test", "t").Meta().ID
	tk.MustQuery("select cast(lower_bound as char), cast(upper_bound as char) from mysql.stats_buckets" +
		" where table_id = " + strconv.FormatInt(tblID, 10) + " and is_index = 0 order by bucket_id").
		Check(testkit.Rows(
			"2024-04-08 02:00:01 2024-04-08 02:00:01",
			"2024-04-08 02:00:02 2024-04-08 02:00:02",
			"2024-04-08 02:00:03 2024-04-08 02:00:03",
			"2024-04-08 02:00:04 2024-04-08 02:00:04",
		))

	// SHOW STATS_BUCKETS renders them in the session time zone, like the column value itself.
	showBounds := func(timeZone string) [][]any {
		tk.MustExec("set @@time_zone = '" + timeZone + "'")
		rows := tk.MustQuery("show stats_buckets where db_name = 'test' and table_name = 't'").Rows()
		bounds := make([][]any, 0, len(rows))
		for _, row := range rows {
			bounds = append(bounds, []any{row[8], row[9]})
		}
		return bounds
	}
	require.Equal(t, [][]any{
		{"2024-04-08 10:00:01", "2024-04-08 10:00:01"},
		{"2024-04-08 10:00:02", "2024-04-08 10:00:02"},
		{"2024-04-08 10:00:03", "2024-04-08 10:00:03"},
		{"2024-04-08 10:00:04", "2024-04-08 10:00:04"},
	}, showBounds("+08:00"))
	require.Equal(t, [][]any{
		{"2024-04-08 02:00:01", "2024-04-08 02:00:01"},
		{"2024-04-08 02:00:02", "2024-04-08 02:00:02"},
		{"2024-04-08 02:00:03", "2024-04-08 02:00:03"},
		{"2024-04-08 02:00:04", "2024-04-08 02:00:04"},
	}, showBounds("+00:00"))

	// The same instant has to produce the same estimate no matter which time zone spells it out.
	estimate := func(timeZone, value string) string {
		tk.MustExec("set @@time_zone = '" + timeZone + "'")
		return tk.MustQuery("explain format = 'brief' select * from t where a > '" + value + "'").Rows()[0][1].(string)
	}
	// About half of the 2048 rows are greater than the second of the four distinct values.
	require.Equal(t, "1044.48", estimate("+00:00", "2024-04-08 02:00:02"))
	require.Equal(t, "1044.48", estimate("+08:00", "2024-04-08 10:00:02"))
	require.Equal(t, "1044.48", estimate("-05:00", "2024-04-07 21:00:02"))
}
