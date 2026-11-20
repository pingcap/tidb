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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	statstestutil "github.com/pingcap/tidb/pkg/statistics/handle/ddl/testutil"
	statsutil "github.com/pingcap/tidb/pkg/statistics/util"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/analyzehelper"
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

// timestampStats is a table whose TIMESTAMP column a holds four distinct instants, 512 rows each,
// analyzed in UTC so the column histogram bounds are UTC. markUTC marks them as such.
type timestampStats struct {
	t   *testing.T
	tk  *testkit.TestKit
	dom *domain.Domain
	id  int64
}

func newTimestampStats(t *testing.T, createTable string) *timestampStats {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(createTable)
	tk.MustExec("set @@time_zone = 'UTC'")
	tk.MustExec(`insert into t (a) values ('2024-04-08 02:00:01'), ('2024-04-08 02:00:02'),
		('2024-04-08 02:00:03'), ('2024-04-08 02:00:04')`)
	for range 9 {
		tk.MustExec("insert into t (a) select a from t")
	}
	tk.MustExec("analyze table t with 0 topn")
	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	s := &timestampStats{t: t, tk: tk, dom: dom, id: tbl.Meta().ID}
	require.NoError(t, dom.StatsHandle().Update(context.Background(), dom.InfoSchema()))
	return s
}

// markUTC sets the marker in storage. Like any write of a histogram it moves the versions forward,
// or the stats cache would keep the histogram it has.
func (s *timestampStats) markUTC() {
	s.tk.MustExec(fmt.Sprintf("update mysql.stats_histograms set flag = flag | %d, version = version + 1 where table_id = %d and is_index = 0 and hist_id = 1",
		statistics.HistogramFlagTimestampBoundsInUTC, s.id))
	s.tk.MustExec(fmt.Sprintf("update mysql.stats_meta set last_stats_histograms_version = version + 1, version = version + 1 where table_id = %d", s.id))
	require.NoError(s.t, s.dom.StatsHandle().Update(context.Background(), s.dom.InfoSchema()))
}

// estRows returns the estimated row count of the first plan operator, in the given time zone.
func (s *timestampStats) estRows(timeZone, query string) string {
	s.tk.MustExec("set @@time_zone = '" + timeZone + "'")
	return s.tk.MustQuery("explain format = 'brief' " + query).Rows()[0][1].(string)
}

// sameInstant requires the same estimate for one query spelled out in UTC, +08:00 and -05:00.
func (s *timestampStats) sameInstant(utc, plus8, minus5 string) string {
	want := s.estRows("+00:00", utc)
	require.Equal(s.t, want, s.estRows("+08:00", plus8), plus8)
	require.Equal(s.t, want, s.estRows("-05:00", minus5), minus5)
	return want
}

func (s *timestampStats) showBounds(timeZone string) [][]any {
	s.tk.MustExec("set @@time_zone = '" + timeZone + "'")
	rows := s.tk.MustQuery("show stats_buckets where db_name = 'test' and table_name = 't' and column_name = 'a' and is_index = 0").Rows()
	bounds := make([][]any, 0, len(rows))
	for _, row := range rows {
		bounds = append(bounds, []any{row[8], row[9]})
	}
	return bounds
}

// About half of the 2048 rows are greater than the second of the four distinct instants.
const halfOfTimestampStats = "1044.48"

// TestTimestampBoundsInUTCFlag covers the marker for UTC TIMESTAMP bounds in
// mysql.stats_histograms.flag, see issue #52429. A histogram that carries it has its predicate
// shifted to UTC, so every session sees the same estimate for the same instant. A histogram without
// it is compared with the predicate as written.
func TestTimestampBoundsInUTCFlag(t *testing.T) {
	// The index is on b: an index on a would be used for the estimate instead, and index bounds are
	// encoded in UTC.
	s := newTimestampStats(t, "create table t (a timestamp, b int, key(b))")
	flags := func() [][]any {
		return s.tk.MustQuery(fmt.Sprintf("select is_index, hist_id, flag from mysql.stats_histograms where table_id = %d order by is_index, hist_id", s.id)).Rows()
	}
	const greater = "select * from t where a > '%s'"
	utcBounds := [][]any{
		{"2024-04-08 02:00:01", "2024-04-08 02:00:01"},
		{"2024-04-08 02:00:02", "2024-04-08 02:00:02"},
		{"2024-04-08 02:00:03", "2024-04-08 02:00:03"},
		{"2024-04-08 02:00:04", "2024-04-08 02:00:04"},
	}
	plus8Bounds := [][]any{
		{"2024-04-08 10:00:01", "2024-04-08 10:00:01"},
		{"2024-04-08 10:00:02", "2024-04-08 10:00:02"},
		{"2024-04-08 10:00:03", "2024-04-08 10:00:03"},
		{"2024-04-08 10:00:04", "2024-04-08 10:00:04"},
	}
	checkLegacy := func() {
		// The predicate is compared as written, so only the wall clock the bounds hold matches.
		require.Equal(t, halfOfTimestampStats, s.estRows("+08:00", fmt.Sprintf(greater, "2024-04-08 02:00:02")))
		require.NotEqual(t, halfOfTimestampStats, s.estRows("+08:00", fmt.Sprintf(greater, "2024-04-08 10:00:02")))
		// And the bounds are shown as stored.
		require.Equal(t, utcBounds, s.showBounds("+08:00"))
	}
	checkMarked := func() {
		require.Equal(t, halfOfTimestampStats, s.sameInstant(
			fmt.Sprintf(greater, "2024-04-08 02:00:02"),
			fmt.Sprintf(greater, "2024-04-08 10:00:02"),
			fmt.Sprintf(greater, "2024-04-07 21:00:02")))
		// The bounds are shown in the session time zone, like the column value itself.
		require.Equal(t, plus8Bounds, s.showBounds("+08:00"))
		require.Equal(t, utcBounds, s.showBounds("+00:00"))
	}

	require.Equal(t, testkit.Rows("0 1 0", "0 2 0", "1 1 0"), flags())
	checkLegacy()

	marked := testkit.Rows("0 1 2", "0 2 0", "1 1 0")
	s.markUTC()
	require.Equal(t, marked, flags())
	checkMarked()

	// LOAD STATS keeps the marker. The statement only registers the load; the server then sends the
	// file content to LoadStatsInfo.Update, which is done by hand here.
	loadStats := func(jsonTbl *statsutil.JSONTable) {
		data, err := json.Marshal(jsonTbl)
		require.NoError(t, err)
		path := filepath.Join(t.TempDir(), "stats.json")
		require.NoError(t, os.WriteFile(path, data, 0o644))
		s.tk.MustExec("drop stats t")
		require.Equal(t, testkit.Rows("0 1 0", "0 2 0", "1 1 0"), flags())
		s.tk.MustExec(fmt.Sprintf("load stats '%s'", path))
		info, ok := s.tk.Session().Value(executor.LoadStatsVarKey).(*executor.LoadStatsInfo)
		require.True(t, ok)
		s.tk.Session().SetValue(executor.LoadStatsVarKey, nil)
		content, err := os.ReadFile(info.Path)
		require.NoError(t, err)
		require.NoError(t, info.Update(content))
		require.NoError(t, s.dom.StatsHandle().Update(context.Background(), s.dom.InfoSchema()))
	}
	tbl, ok := s.dom.InfoSchema().TableByID(context.Background(), s.id)
	require.True(t, ok)
	jsonTbl, err := s.dom.StatsHandle().DumpStatsToJSON("test", tbl.Meta(), nil, true)
	require.NoError(t, err)
	require.Equal(t, statistics.HistogramFlagTimestampBoundsInUTC, jsonTbl.Columns["a"].Flag)
	require.Zero(t, jsonTbl.Columns["b"].Flag)
	loadStats(jsonTbl)
	require.Equal(t, marked, flags())
	checkMarked()

	// A file without a flag field loads unmarked.
	jsonTbl.Columns["a"].Flag = 0
	data, err := json.Marshal(jsonTbl)
	require.NoError(t, err)
	require.NotContains(t, string(data), `"flag"`)
	loadStats(jsonTbl)
	require.Equal(t, testkit.Rows("0 1 0", "0 2 0", "1 1 0"), flags())
	checkLegacy()
}

// TestTimestampBoundsInUTCFlagIndexRange covers a range on a single-column index over the TIMESTAMP
// column. Out of the index's range, the estimate borrows the column histogram, whose bounds are
// what the marker describes, while the index bounds are encoded in UTC.
func TestTimestampBoundsInUTCFlagIndexRange(t *testing.T) {
	s := newTimestampStats(t, "create table t (a timestamp, key ia(a))")
	// Rows added since ANALYZE are what the out-of-range estimate distributes.
	s.tk.MustExec("insert into t (a) select a from t limit 512")
	require.NoError(t, s.dom.StatsHandle().DumpStatsDeltaToKV(true))
	s.markUTC()
	require.Equal(t, testkit.Rows("2560 512"),
		s.tk.MustQuery(fmt.Sprintf("select count, modify_count from mysql.stats_meta where table_id = %d", s.id)).Rows())

	const between = "select * from t use index(ia) where a between '%s' and '%s'"
	// Entirely past the last instant, 02:00:04 UTC.
	s.sameInstant(
		fmt.Sprintf(between, "2024-04-08 02:00:05", "2024-04-08 02:30:00"),
		fmt.Sprintf(between, "2024-04-08 10:00:05", "2024-04-08 10:30:00"),
		fmt.Sprintf(between, "2024-04-07 21:00:05", "2024-04-07 21:30:00"))
	// Straddling the last instant.
	s.sameInstant(
		fmt.Sprintf(between, "2024-04-08 02:00:03", "2024-04-08 02:00:10"),
		fmt.Sprintf(between, "2024-04-08 10:00:03", "2024-04-08 10:00:10"),
		fmt.Sprintf(between, "2024-04-07 21:00:03", "2024-04-07 21:00:10"))
}

// TestTimestampBoundsInUTCFlagSyncLoad covers the marker on the path production takes: with a stats
// lease, column statistics are not loaded up front but on demand by the query that needs them.
func TestTimestampBoundsInUTCFlagSyncLoad(t *testing.T) {
	s := newTimestampStats(t, "create table t (a timestamp)")
	s.markUTC()

	h := s.dom.StatsHandle()
	oriLease := h.Lease()
	h.SetLease(1)
	defer h.SetLease(oriLease)
	// Start from an empty cache; with a lease, Update only records that the statistics exist.
	h.Clear()
	require.NoError(t, h.Update(context.Background(), s.dom.InfoSchema()))
	tbl, ok := s.dom.InfoSchema().TableByID(context.Background(), s.id)
	require.True(t, ok)
	colID := tbl.Meta().Columns[0].ID
	col := h.GetPhysicalTableStats(s.id, tbl.Meta()).GetCol(colID)
	require.True(t, col == nil || col.Histogram.Len() == 0, "the column histogram must not be loaded yet")

	// The first query loads the histogram, and has to see the marker already.
	require.Equal(t, halfOfTimestampStats, s.estRows("+08:00", "select * from t where a > '2024-04-08 10:00:02'"))
	col = h.GetPhysicalTableStats(s.id, tbl.Meta()).GetCol(colID)
	require.NotNil(t, col)
	require.Positive(t, col.Histogram.Len())
	require.True(t, col.Histogram.TimestampBoundsInUTC())
}
