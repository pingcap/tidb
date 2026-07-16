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

package storage_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/analyzehelper"
	"github.com/stretchr/testify/require"
)

func TestGCStats(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("set @@tidb_analyze_version = 2")
	testKit.MustExec("use test")
	testKit.MustExec("create table t(a int, b int, index idx(a, b), index idx_a(a))")
	testKit.MustExec("insert into t values (1,1),(2,2),(3,3)")
	testKit.MustExec("analyze table t with 0 topn")

	testKit.MustExec("alter table t drop index idx")
	testKit.MustQuery("select count(*) from mysql.stats_histograms").Check(testkit.Rows("4"))
	testKit.MustQuery("select count(*) from mysql.stats_buckets").Check(testkit.Rows("12"))
	h := dom.StatsHandle()
	ddlLease := time.Duration(0)
	require.Nil(t, h.GCStats(dom.InfoSchema(), ddlLease))
	testKit.MustQuery("select count(*) from mysql.stats_histograms").Check(testkit.Rows("3"))
	testKit.MustQuery("select count(*) from mysql.stats_buckets").Check(testkit.Rows("9"))

	testKit.MustExec("alter table t drop index idx_a")
	testKit.MustExec("alter table t drop column a")
	require.Nil(t, h.GCStats(dom.InfoSchema(), ddlLease))
	testKit.MustQuery("select count(*) from mysql.stats_histograms").Check(testkit.Rows("1"))
	testKit.MustQuery("select count(*) from mysql.stats_buckets").Check(testkit.Rows("3"))

	testKit.MustExec("drop table t")
	require.Nil(t, h.GCStats(dom.InfoSchema(), ddlLease))
	testKit.MustQuery("select count(*) from mysql.stats_meta").Check(testkit.Rows("1"))
	testKit.MustQuery("select count(*) from mysql.stats_histograms").Check(testkit.Rows("0"))
	testKit.MustQuery("select count(*) from mysql.stats_buckets").Check(testkit.Rows("0"))
	require.Nil(t, h.GCStats(dom.InfoSchema(), ddlLease))
	testKit.MustQuery("select count(*) from mysql.stats_meta").Check(testkit.Rows("0"))
}

func TestGCPartition(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("set @@tidb_analyze_version = 2")
	testkit.WithPruneMode(testKit, variable.Static, func() {
		testKit.MustExec("use test")
		testKit.MustExec(`create table t (a bigint(64), b bigint(64), index idx(a, b))
			    partition by range (a) (
			    partition p0 values less than (3),
			    partition p1 values less than (6))`)
		testKit.MustExec("insert into t values (1,2),(2,3),(3,4),(4,5),(5,6)")
		testKit.MustExec("analyze table t with 0 topn")

		testKit.MustQuery("select count(*) from mysql.stats_histograms").Check(testkit.Rows("6"))
		testKit.MustQuery("select count(*) from mysql.stats_buckets").Check(testkit.Rows("15"))
		h := dom.StatsHandle()
		ddlLease := time.Duration(0)
		testKit.MustExec("alter table t drop index idx")
		require.Nil(t, h.GCStats(dom.InfoSchema(), ddlLease))
		testKit.MustQuery("select count(*) from mysql.stats_histograms").Check(testkit.Rows("4"))
		testKit.MustQuery("select count(*) from mysql.stats_buckets").Check(testkit.Rows("10"))

		testKit.MustExec("alter table t drop column b")
		require.Nil(t, h.GCStats(dom.InfoSchema(), ddlLease))
		testKit.MustQuery("select count(*) from mysql.stats_histograms").Check(testkit.Rows("2"))
		testKit.MustQuery("select count(*) from mysql.stats_buckets").Check(testkit.Rows("5"))

		testKit.MustExec("drop table t")
		require.Nil(t, h.GCStats(dom.InfoSchema(), ddlLease))
		testKit.MustQuery("select count(*) from mysql.stats_meta").Check(testkit.Rows("3"))
		testKit.MustQuery("select count(*) from mysql.stats_histograms").Check(testkit.Rows("0"))
		testKit.MustQuery("select count(*) from mysql.stats_buckets").Check(testkit.Rows("0"))
		// FIXME(#68076): The remaining row is the logical table's meta-only stats row. The
		// normal GC version-window scan does not revisit it after the table is dropped.
		require.Nil(t, h.GCStats(dom.InfoSchema(), ddlLease))
		testKit.MustQuery("select count(*) from mysql.stats_meta").Check(testkit.Rows("1"))
	})
}

func TestGCColumnStatsUsage(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t(a int, b int, c int)")
	testKit.MustExec("insert into t values (1,1,1),(2,2,2),(3,3,3)")
	analyzehelper.TriggerPredicateColumnsCollection(t, testKit, store, "t", "a", "b", "c")
	testKit.MustExec("analyze table t")
	testKit.MustQuery("select count(*) from mysql.column_stats_usage").Check(testkit.Rows("3"))
	testKit.MustExec("alter table t drop column a")
	testKit.MustQuery("select count(*) from mysql.column_stats_usage").Check(testkit.Rows("3"))
	h := dom.StatsHandle()
	ddlLease := time.Duration(0)
	require.Nil(t, h.GCStats(dom.InfoSchema(), ddlLease))
	testKit.MustQuery("select count(*) from mysql.column_stats_usage").Check(testkit.Rows("2"))
	testKit.MustExec("drop table t")
	testKit.MustQuery("select count(*) from mysql.column_stats_usage").Check(testkit.Rows("2"))
	require.Nil(t, h.GCStats(dom.InfoSchema(), ddlLease))
	testKit.MustQuery("select count(*) from mysql.column_stats_usage").Check(testkit.Rows("0"))
}

func TestDeleteAnalyzeJobs(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t(a int, b int)")
	testKit.MustExec("insert into t values (1,2),(3,4)")
	testKit.MustExec("analyze table t")
	rows := testKit.MustQuery("show analyze status").Rows()
	require.Equal(t, 1, len(rows))
	require.NoError(t, dom.StatsHandle().DeleteAnalyzeJobs(time.Now().Add(time.Second)))
	rows = testKit.MustQuery("show analyze status").Rows()
	require.Equal(t, 0, len(rows))
}

func TestClearOutdatedHistoryStats(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	// The historical stats feature was removed and nothing writes to
	// mysql.stats_history/mysql.stats_meta_history anymore, but rows written
	// before an upgrade may remain, so seed legacy rows directly. Use more
	// than one delete batch (50 rows) of stats_history rows to make sure the
	// drain loops until completion instead of stopping after the first batch.
	var sb strings.Builder
	sb.WriteString("insert into mysql.stats_history (table_id, stats_data, seq_no, version, create_time) values ")
	for i := range 120 {
		if i > 0 {
			sb.WriteString(",")
		}
		fmt.Fprintf(&sb, "(1, 'x', %d, %d, '2020-01-01 00:00:00')", i, i+1)
	}
	tk.MustExec(sb.String())
	tk.MustExec("insert into mysql.stats_meta_history (table_id, modify_count, count, version, source, create_time) values (1, 0, 0, 1, 'test', '2020-01-01 00:00:00')")
	// Rows within the retention window must survive the drain.
	tk.MustExec("insert into mysql.stats_history (table_id, stats_data, seq_no, version, create_time) values (2, 'x', 0, 1, NOW())")
	tk.MustExec("insert into mysql.stats_meta_history (table_id, modify_count, count, version, source, create_time) values (2, 0, 0, 1, 'test', NOW())")

	require.NoError(t, dom.StatsHandle().ClearOutdatedHistoryStats())
	tk.MustQuery("select count(*) from mysql.stats_meta_history").Check(testkit.Rows("1"))
	tk.MustQuery("select count(*) from mysql.stats_history").Check(testkit.Rows("1"))

	// Legacy rows may remain in mysql.stats_history even after
	// mysql.stats_meta_history is fully drained; they must still be cleared.
	tk.MustExec("insert into mysql.stats_history (table_id, stats_data, seq_no, version, create_time) values (3, 'x', 0, 1, '2020-01-01 00:00:00')")
	require.NoError(t, dom.StatsHandle().ClearOutdatedHistoryStats())
	tk.MustQuery("select count(*) from mysql.stats_history where table_id = 3").Check(testkit.Rows("0"))
}

func TestGCHistoryStatsAfterDropTable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values (1,2),(3,4)")
	tk.MustExec("analyze table t")
	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tid := tbl.Meta().ID
	// Seed legacy historical stats rows for the table. Use a recent
	// create_time so only the drop-table GC path (not the retention-based
	// drain) can remove them.
	tk.MustExec(fmt.Sprintf("insert into mysql.stats_history (table_id, stats_data, seq_no, version, create_time) values (%d, 'x', 0, 1, NOW())", tid))
	tk.MustExec(fmt.Sprintf("insert into mysql.stats_meta_history (table_id, modify_count, count, version, source, create_time) values (%d, 0, 0, 1, 'test', NOW())", tid))

	tk.MustExec("drop table t")
	require.NoError(t, dom.StatsHandle().GCStats(dom.InfoSchema(), 0))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.stats_history where table_id = %d", tid)).Check(testkit.Rows("0"))
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.stats_meta_history where table_id = %d", tid)).Check(testkit.Rows("0"))
}

func TestExtremCaseOfGC(t *testing.T) {
	// This case tests that there's no records in mysql.stats_histograms but this table is not deleted in fact.
	// We should not delete the record in mysql.stats_meta.
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("create table t(a int, b int)")
	testKit.MustExec("insert into t values (1,2),(3,4)")
	testKit.MustExec("analyze table t")
	tbl, err := dom.InfoSchema().TableByName(context.TODO(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tid := tbl.Meta().ID
	rs := testKit.MustQuery("select * from mysql.stats_meta where table_id = ?", tid)
	require.Len(t, rs.Rows(), 1)
	rs = testKit.MustQuery("select * from mysql.stats_histograms where table_id = ?", tid)
	require.Len(t, rs.Rows(), 2)
	h := dom.StatsHandle()
	failpoint.Enable("github.com/pingcap/tidb/pkg/statistics/handle/storage/injectGCStatsLastTSOffset", `return(0)`)
	h.GCStats(dom.InfoSchema(), time.Second*3)
	rs = testKit.MustQuery("select * from mysql.stats_meta where table_id = ?", tid)
	require.Len(t, rs.Rows(), 1)
	failpoint.Disable("github.com/pingcap/tidb/pkg/statistics/handle/storage/injectGCStatsLastTSOffset")
}
