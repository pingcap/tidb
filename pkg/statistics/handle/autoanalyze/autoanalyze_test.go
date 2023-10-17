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

package autoanalyze_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestAutoAnalyzeOnChangeAnalyzeVer(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, index idx(a))")
	tk.MustExec("insert into t values(1)")
	tk.MustExec("set @@global.tidb_analyze_version = 1")
	do := dom
	autoanalyze.AutoAnalyzeMinCnt = 0
	defer func() {
		autoanalyze.AutoAnalyzeMinCnt = 1000
	}()
	h := do.StatsHandle()
	err := h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	is := do.InfoSchema()
	require.NoError(t, h.Update(is))
	// Auto analyze when global ver is 1.
	h.HandleAutoAnalyze(is)
	require.NoError(t, h.Update(is))
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	statsTbl1 := h.GetTableStats(tbl.Meta())
	// Check that all the version of t's stats are 1.
	for _, col := range statsTbl1.Columns {
		require.Equal(t, int64(1), col.GetStatsVer())
	}
	for _, idx := range statsTbl1.Indices {
		require.Equal(t, int64(1), idx.GetStatsVer())
	}
	tk.MustExec("set @@global.tidb_analyze_version = 2")
	tk.MustExec("insert into t values(1), (2), (3), (4)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(is))
	// Auto analyze t whose version is 1 after setting global ver to 2.
	h.HandleAutoAnalyze(is)
	require.NoError(t, h.Update(is))
	statsTbl1 = h.GetTableStats(tbl.Meta())
	require.Equal(t, int64(5), statsTbl1.RealtimeCount)
	// All of its statistics should still be version 1.
	for _, col := range statsTbl1.Columns {
		require.Equal(t, int64(1), col.GetStatsVer())
	}
	for _, idx := range statsTbl1.Indices {
		require.Equal(t, int64(1), idx.GetStatsVer())
	}
	// Add a new table after the analyze version set to 2.
	tk.MustExec("create table tt(a int, index idx(a))")
	tk.MustExec("insert into tt values(1), (2), (3), (4), (5)")
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	is = do.InfoSchema()
	tbl2, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("tt"))
	require.NoError(t, err)
	require.NoError(t, h.Update(is))
	h.HandleAutoAnalyze(is)
	require.NoError(t, h.Update(is))
	statsTbl2 := h.GetTableStats(tbl2.Meta())
	// Since it's a newly created table. Auto analyze should analyze it's statistics to version2.
	for _, idx := range statsTbl2.Indices {
		require.Equal(t, int64(2), idx.GetStatsVer())
	}
	for _, col := range statsTbl2.Columns {
		require.Equal(t, int64(2), col.GetStatsVer())
	}
	tk.MustExec("set @@global.tidb_analyze_version = 1")
}

func TestTableAnalyzed(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
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
	require.False(t, autoanalyze.TableAnalyzed(statsTbl))

	testKit.MustExec("analyze table t")
	require.NoError(t, h.Update(is))
	statsTbl = h.GetTableStats(tableInfo)
	require.True(t, autoanalyze.TableAnalyzed(statsTbl))

	h.Clear()
	oriLease := h.Lease()
	// set it to non-zero so we will use load by need strategy
	h.SetLease(1)
	defer func() {
		h.SetLease(oriLease)
	}()
	require.NoError(t, h.Update(is))
	statsTbl = h.GetTableStats(tableInfo)
	require.True(t, autoanalyze.TableAnalyzed(statsTbl))
}

func TestNeedAnalyzeTable(t *testing.T) {
	columns := map[int64]*statistics.Column{}
	columns[1] = &statistics.Column{StatsVer: statistics.Version2}
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
			result: true,
			reason: "table unanalyzed",
		},
		// table was already analyzed but auto analyze is disabled
		{
			tbl:    &statistics.Table{HistColl: statistics.HistColl{Columns: columns, ModifyCount: 1, RealtimeCount: 1}},
			limit:  0,
			ratio:  0,
			result: false,
			reason: "",
		},
		// table was already analyzed but modify count is small
		{
			tbl:    &statistics.Table{HistColl: statistics.HistColl{Columns: columns, ModifyCount: 0, RealtimeCount: 1}},
			limit:  0,
			ratio:  0.3,
			result: false,
			reason: "",
		},
		// table was already analyzed
		{
			tbl:    &statistics.Table{HistColl: statistics.HistColl{Columns: columns, ModifyCount: 1, RealtimeCount: 1}},
			limit:  0,
			ratio:  0.3,
			result: true,
			reason: "too many modifications",
		},
		// table was already analyzed
		{
			tbl:    &statistics.Table{HistColl: statistics.HistColl{Columns: columns, ModifyCount: 1, RealtimeCount: 1}},
			limit:  0,
			ratio:  0.3,
			result: true,
			reason: "too many modifications",
		},
		// table was already analyzed
		{
			tbl:    &statistics.Table{HistColl: statistics.HistColl{Columns: columns, ModifyCount: 1, RealtimeCount: 1}},
			limit:  0,
			ratio:  0.3,
			result: true,
			reason: "too many modifications",
		},
		// table was already analyzed
		{
			tbl:    &statistics.Table{HistColl: statistics.HistColl{Columns: columns, ModifyCount: 1, RealtimeCount: 1}},
			limit:  0,
			ratio:  0.3,
			result: true,
			reason: "too many modifications",
		},
	}
	for _, test := range tests {
		needAnalyze, reason := autoanalyze.NeedAnalyzeTable(test.tbl, test.limit, test.ratio)
		require.Equal(t, test.result, needAnalyze)
		require.True(t, strings.HasPrefix(reason, test.reason))
	}
}

func TestAutoAnalyzeSkipColumnTypes(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, c json, d text, e mediumtext, f blob, g mediumblob, index idx(d(10)))")
	tk.MustExec("insert into t values (1, 2, null, 'xxx', 'yyy', null, null)")
	h := dom.StatsHandle()
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(dom.InfoSchema()))
	tk.MustExec("set @@global.tidb_analyze_skip_column_types = 'json,blob,mediumblob,text,mediumtext'")

	originalVal := autoanalyze.AutoAnalyzeMinCnt
	autoanalyze.AutoAnalyzeMinCnt = 0
	defer func() {
		autoanalyze.AutoAnalyzeMinCnt = originalVal
	}()
	require.True(t, h.HandleAutoAnalyze(dom.InfoSchema()))
	tk.MustQuery("select job_info from mysql.analyze_jobs where job_info like '%auto analyze table%'").Check(testkit.Rows("auto analyze table columns a, b, d with 256 buckets, 500 topn, 1 samplerate"))
}

func TestAutoAnalyzeOnEmptyTable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
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
	tk.MustExec("insert into t values (1)" + strings.Repeat(", (1)", int(autoanalyze.AutoAnalyzeMinCnt)))
	require.NoError(t, dom.StatsHandle().DumpStatsDeltaToKV(true))
	require.NoError(t, dom.StatsHandle().Update(dom.InfoSchema()))

	// test if it will be limited by the time range
	require.False(t, dom.StatsHandle().HandleAutoAnalyze(dom.InfoSchema()))

	tk.MustExec("set global tidb_auto_analyze_start_time='00:00 +0000'")
	tk.MustExec("set global tidb_auto_analyze_end_time='23:59 +0000'")
	require.True(t, dom.StatsHandle().HandleAutoAnalyze(dom.InfoSchema()))
}

func TestAutoAnalyzeOutOfSpecifiedTime(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
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
	tk.MustExec("insert into t values (1)" + strings.Repeat(", (1)", int(autoanalyze.AutoAnalyzeMinCnt)))
	require.NoError(t, dom.StatsHandle().DumpStatsDeltaToKV(true))
	require.NoError(t, dom.StatsHandle().Update(dom.InfoSchema()))

	require.False(t, dom.StatsHandle().HandleAutoAnalyze(dom.InfoSchema()))
	tk.MustExec("analyze table t")

	tk.MustExec("alter table t add index ia(a)")
	require.False(t, dom.StatsHandle().HandleAutoAnalyze(dom.InfoSchema()))

	tk.MustExec("set global tidb_auto_analyze_start_time='00:00 +0000'")
	tk.MustExec("set global tidb_auto_analyze_end_time='23:59 +0000'")
	require.True(t, dom.StatsHandle().HandleAutoAnalyze(dom.InfoSchema()))
}
