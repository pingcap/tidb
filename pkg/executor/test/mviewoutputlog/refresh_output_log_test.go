// Copyright 2026 PingCAP, Inc.
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

package mviewoutputlog

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func TestMaterializedViewRefreshFastWritesMLog(t *testing.T) {
	testMaterializedViewRefreshWritesMLog(t, "fast")
}

func TestMaterializedViewRefreshCompleteDeltaApplyWritesMLog(t *testing.T) {
	testMaterializedViewRefreshWritesMLog(t, "complete delta apply")
}

func testMaterializedViewRefreshWritesMLog(t *testing.T, refreshMethod string) {
	t.Helper()

	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_refresh_mlog (a int not null, b int not null)")
	tk.MustExec("insert into t_mv_refresh_mlog values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t_mv_refresh_mlog (a, b)")
	tk.MustExec("create materialized view mv_refresh_mlog (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_mv_refresh_mlog group by a")
	tk.MustExec("create materialized view log on mv_refresh_mlog (a, s, cnt)")

	// The three base-table mutations make the parent MV update one group, delete one group,
	// and insert one group. The parent MLog must retain the corresponding logical operations.
	tk.MustExec("update t_mv_refresh_mlog set b = 11 where a = 1 and b = 10")
	tk.MustExec("delete from t_mv_refresh_mlog where a = 2")
	tk.MustExec("insert into t_mv_refresh_mlog values (3, 4)")
	tk.MustExec("refresh materialized view mv_refresh_mlog " + refreshMethod)

	tk.MustQuery("select a, s, cnt from mv_refresh_mlog order by a").Check(testkit.Rows(
		"1 16 2",
		"3 4 1",
	))
	tk.MustQuery("select a, s, cnt, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$mv_refresh_mlog` order by a, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW`").Check(testkit.Rows(
		"1 15 2 U -1",
		"1 16 2 U 1",
		"2 7 1 D -1",
		"3 4 1 I 1",
	))

	// A refresh with no source changes must not append a MLog row.
	tk.MustExec("refresh materialized view mv_refresh_mlog " + refreshMethod)
	tk.MustQuery("select count(*) from `$mlog$mv_refresh_mlog`").Check(testkit.Rows("4"))
}

func TestMaterializedViewRefreshSkipsMLogWhenTrackedColumnsAreUnchanged(t *testing.T) {
	for _, refreshMethod := range []string{"fast", "complete delta apply"} {
		t.Run(refreshMethod, func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			tk := newMViewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("create table t_mv_refresh_mlog_tracked (a int not null, b int not null)")
			tk.MustExec("insert into t_mv_refresh_mlog_tracked values (1, 10), (1, 5)")
			tk.MustExec("create materialized view log on t_mv_refresh_mlog_tracked (a, b)")
			tk.MustExec("create materialized view mv_refresh_mlog_tracked (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_mv_refresh_mlog_tracked group by a")
			tk.MustExec("create materialized view log on mv_refresh_mlog_tracked (a)")

			tk.MustExec("update t_mv_refresh_mlog_tracked set b = 11 where a = 1 and b = 10")
			tk.MustExec("refresh materialized view mv_refresh_mlog_tracked " + refreshMethod)

			tk.MustQuery("select a, s, cnt from mv_refresh_mlog_tracked").Check(testkit.Rows("1 16 2"))
			tk.MustQuery("select * from `$mlog$mv_refresh_mlog_tracked`").Check(testkit.Rows())
		})
	}
}

func TestMaterializedViewRefreshMLogWriteFailureRollsBack(t *testing.T) {
	for _, refreshMethod := range []string{"fast", "complete delta apply"} {
		t.Run(refreshMethod, func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			tk := newMViewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("create table t_mv_refresh_mlog_rollback (a int not null, b int not null)")
			tk.MustExec("insert into t_mv_refresh_mlog_rollback values (1, 10), (1, 5)")
			tk.MustExec("create materialized view log on t_mv_refresh_mlog_rollback (a, b)")
			tk.MustExec("create materialized view mv_refresh_mlog_rollback (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_mv_refresh_mlog_rollback group by a")
			tk.MustExec("create materialized view log on mv_refresh_mlog_rollback (a, s, cnt)")
			tk.MustExec("update t_mv_refresh_mlog_rollback set b = 11 where a = 1 and b = 10")

			const failpointName = "github.com/pingcap/tidb/pkg/table/tables/mockMLogWriteError"
			before := tk.MustQuery("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info").Rows()
			testfailpoint.Enable(t, failpointName, `return("mock mlog write failure")`)

			err := tk.ExecToErr("refresh materialized view mv_refresh_mlog_rollback " + refreshMethod)
			require.ErrorContains(t, err, "mock mlog write failure")
			tk.MustQuery("select a, s, cnt from mv_refresh_mlog_rollback").Check(testkit.Rows("1 15 2"))
			tk.MustQuery("select * from `$mlog$mv_refresh_mlog_rollback`").Check(testkit.Rows())
			require.Equal(t, before, tk.MustQuery("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info").Rows())
			testfailpoint.Disable(t, failpointName)
			tk.MustExec("refresh materialized view mv_refresh_mlog_rollback " + refreshMethod)
			tk.MustQuery("select a, s, cnt from mv_refresh_mlog_rollback").Check(testkit.Rows("1 16 2"))
			tk.MustQuery("select count(*) from `$mlog$mv_refresh_mlog_rollback`").Check(testkit.Rows("2"))
		})
	}
}

func TestMaterializedViewRefreshMLogNullTransitions(t *testing.T) {
	for _, method := range []string{"fast", "complete delta apply"} {
		t.Run(method, func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			tk := newMViewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("create table t_null_output (a int not null, b int)")
			tk.MustExec("insert into t_null_output values (1, null), (2, 10)")
			tk.MustExec("create materialized view log on t_null_output (a, b)")
			tk.MustExec("create materialized view mv_null_output (a, s, n, cnt) as select a, sum(b), count(b), count(1) from t_null_output group by a")
			tk.MustExec("create materialized view log on mv_null_output (a, s, n, cnt)")
			tk.MustExec("update t_null_output set b=10 where a=1")
			tk.MustExec("update t_null_output set b=null where a=2")
			tk.MustExec("refresh materialized view mv_null_output " + method)
			tk.MustQuery("select a, ifnull(cast(s as char), 'NULL'), n, cnt, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$mv_null_output` order by a, `_MLOG$_OLD_NEW`").Check(testkit.Rows(
				"1 NULL 0 1 U -1", "1 10 1 1 U 1", "2 10 1 1 U -1", "2 NULL 0 1 U 1",
			))
		})
	}
}

func TestMaterializedViewRefreshMinMaxWritesMLog(t *testing.T) {
	for _, method := range []string{"fast", "complete delta apply"} {
		t.Run(method, func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			tk := newMViewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("create table t_minmax_output (g int not null, b int, key(g))")
			tk.MustExec("insert into t_minmax_output values (1, 10), (1, 20), (2, null)")
			tk.MustExec("create materialized view log on t_minmax_output (g, b)")
			tk.MustExec("create materialized view mv_minmax_output (g, cnt, mn, mx) as select g, count(1), min(b), max(b) from t_minmax_output group by g")
			tk.MustExec("create materialized view log on mv_minmax_output (g, cnt, mn, mx)")
			tk.MustExec("delete from t_minmax_output where g=1 and b=20")
			tk.MustExec("update t_minmax_output set b=5 where g=2")
			tk.MustExec("refresh materialized view mv_minmax_output " + method)
			tk.MustQuery("select g, cnt, ifnull(cast(mn as char), 'NULL'), ifnull(cast(mx as char), 'NULL'), `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `$mlog$mv_minmax_output` order by g, `_MLOG$_OLD_NEW`").Check(testkit.Rows(
				"1 2 10 20 U -1", "1 1 10 10 U 1", "2 1 NULL NULL U -1", "2 1 5 5 U 1",
			))
		})
	}
}
