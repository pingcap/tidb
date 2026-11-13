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

package mviewnested

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func requireRowsContainPrefix(t *testing.T, rows [][]any, prefix string) {
	for _, row := range rows {
		if len(row) == 0 {
			continue
		}
		if strings.HasPrefix(fmt.Sprintf("%v", row[0]), prefix) {
			return
		}
	}
	require.Failf(t, "prefix not found", "prefix=%s rows=%v", prefix, rows)
}

func TestNestedMaterializedViewFastRefresh(t *testing.T) {
	for _, parentRefreshMethod := range []string{"fast", "complete delta apply"} {
		t.Run(parentRefreshMethod, func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			tk := newMViewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("create table t_nested_mv_refresh (a int not null, b int not null, v int)")
			tk.MustExec("insert into t_nested_mv_refresh values (1, 1, 10), (1, 1, 5), (1, 2, 7), (2, 1, 4)")
			tk.MustExec("create materialized view log on t_nested_mv_refresh (a, b, v)")
			tk.MustExec("create materialized view mv_nested_parent (a, b, s, cnt, cnt_v) refresh fast as select a, b, sum(v), count(1), count(v) from t_nested_mv_refresh group by a, b")
			tk.MustExec("create materialized view log on mv_nested_parent (a, b, s, cnt)")
			tk.MustExec("create materialized view mv_nested_child (a, s, cnt, cnt_s) refresh fast as select a, sum(s), count(1), count(s) from mv_nested_parent group by a")

			tk.MustQuery("select a, s, cnt, cnt_s from mv_nested_child order by a").Check(testkit.Rows(
				"1 22 2 2",
				"2 4 1 1",
			))

			// These mutations make the parent update one group, delete another, and insert a third.
			tk.MustExec("update t_nested_mv_refresh set v = 11 where a = 1 and b = 1 and v = 10")
			tk.MustExec("delete from t_nested_mv_refresh where a = 1 and b = 2")
			tk.MustExec("insert into t_nested_mv_refresh values (3, 1, 8)")
			// A child refresh only consumes published parent deltas; it does not refresh its parent.
			tk.MustExec("refresh materialized view mv_nested_child fast")
			tk.MustQuery("select a, s, cnt, cnt_s from mv_nested_child order by a").Check(testkit.Rows(
				"1 22 2 2", "2 4 1 1",
			))
			tk.MustExec("refresh materialized view mv_nested_parent " + parentRefreshMethod)
			tk.MustExec("refresh materialized view mv_nested_child fast")

			tk.MustQuery("select a, b, s, cnt from mv_nested_parent order by a, b").Check(testkit.Rows(
				"1 1 16 2",
				"2 1 4 1",
				"3 1 8 1",
			))
			tk.MustQuery("select a, s, cnt, cnt_s from mv_nested_child order by a").Check(testkit.Rows(
				"1 16 1 1",
				"2 4 1 1",
				"3 8 1 1",
			))

			// Moving a parent group and making all SUM inputs NULL exercises both group deletion
			// and nullable aggregate transitions in the child's delta stream.
			tk.MustExec("update t_nested_mv_refresh set a = 1, b = 3, v = null where a = 3 and b = 1")
			tk.MustExec("update t_nested_mv_refresh set v = null where a = 1 and b = 1")
			tk.MustExec("refresh materialized view mv_nested_parent " + parentRefreshMethod)
			tk.MustExec("refresh materialized view mv_nested_child fast")
			tk.MustQuery("select a, b, s, cnt, cnt_v from mv_nested_parent order by a, b").Check(testkit.Rows(
				"1 1 <nil> 2 0", "1 3 <nil> 1 0", "2 1 4 1 1",
			))
			tk.MustQuery("select a, s, cnt, cnt_s from mv_nested_child order by a").Check(testkit.Rows(
				"1 <nil> 2 0", "2 4 1 1",
			))

			tk.MustExec("update t_nested_mv_refresh set v = 9 where a = 1 and b = 3")
			tk.MustExec("refresh materialized view mv_nested_parent " + parentRefreshMethod)
			tk.MustExec("refresh materialized view mv_nested_child fast")
			tk.MustQuery("select a, s, cnt, cnt_s from mv_nested_child order by a").Check(testkit.Rows(
				"1 9 2 1", "2 4 1 1",
			))
		})
	}
}

func TestNestedMaterializedViewFastRefreshMinMax(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_nested_mv_minmax (a int not null, b int not null, v int not null, key idx_ab (a, b))")
	tk.MustExec("insert into t_nested_mv_minmax values (1, 1, 10), (1, 1, 30), (1, 2, 5), (2, 1, 8)")
	tk.MustExec("create materialized view log on t_nested_mv_minmax (a, b, v)")
	tk.MustExec("create materialized view mv_nested_parent_minmax (a, b, cnt, mx, mn) refresh fast as select a, b, count(1), max(v), min(v) from t_nested_mv_minmax group by a, b")
	tk.MustExec("create materialized view log on mv_nested_parent_minmax (a, b, cnt, mx, mn)")
	tk.MustExec("create materialized view mv_nested_child_minmax (a, cnt, mx, mn) refresh fast as select a, count(1), max(mx), min(mn) from mv_nested_parent_minmax group by a")

	tk.MustQuery("select a, cnt, mx, mn from mv_nested_child_minmax order by a").Check(testkit.Rows(
		"1 2 30 5",
		"2 1 8 8",
	))

	// Removing the parent group's MAX forces the child MAX fallback to probe the parent MV by its
	// group-key index, rather than treating the source as a physical base table.
	tk.MustExec("delete from t_nested_mv_minmax where a = 1 and b = 1 and v = 30")
	tk.MustExec("refresh materialized view mv_nested_parent_minmax fast")
	tk.MustExec("refresh materialized view mv_nested_child_minmax fast")

	tk.MustQuery("select a, b, cnt, mx, mn from mv_nested_parent_minmax order by a, b").Check(testkit.Rows(
		"1 1 1 10 10",
		"1 2 1 5 5",
		"2 1 1 8 8",
	))
	tk.MustQuery("select a, cnt, mx, mn from mv_nested_child_minmax order by a").Check(testkit.Rows(
		"1 2 10 5",
		"2 1 8 8",
	))
}

func TestMaterializedViewRefreshDryRunUsesInternalTxnInfoSchema(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_dry_run (a int not null, b int not null)")
	tk.MustExec("insert into t_mv_dry_run values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t_mv_dry_run (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_dry_run (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_mv_dry_run group by a")
	lastSuccessReadTSO := tk.MustQuery("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info").Rows()
	tk.MustExec("insert into t_mv_dry_run values (1, 5)")

	for _, tc := range []struct {
		mode     string
		operator string
	}{
		{"complete delta apply", "MViewCompleteDeltaApply"},
		{"fast", "MViewDeltaMerge"},
	} {
		// Each statement registers the outer session's MV dependency before planning
		// in an internal session, which must use its own transaction's MDL table set.
		for range 2 {
			rows := tk.MustQuery("refresh materialized view mv_dry_run " + tc.mode + " dry run").Rows()
			requireRowsContainPrefix(t, rows, "  "+tc.operator)
			tk.MustQuery("select * from mv_dry_run order by a").Check(testkit.Rows("1 10 1", "2 20 1"))
			tk.MustQuery("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info").Check(lastSuccessReadTSO)
			tk.MustQuery("select count(*) from mysql.tidb_mview_refresh_hist").Check(testkit.Rows("0"))
		}
	}

	tk.MustExec("refresh materialized view mv_dry_run complete delta apply")
	tk.MustQuery("select * from mv_dry_run order by a").Check(testkit.Rows("1 15 2", "2 20 1"))
}

func TestMaterializedViewRefreshCompleteOutOfPlaceRejectsDependentMaterializedView(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_oop_child_guard (a int not null, b int not null)")
	tk.MustExec("insert into t_oop_child_guard values (1, 10), (2, 20)")
	tk.MustExec("create materialized view log on t_oop_child_guard (a, b)")
	tk.MustExec("create materialized view mv_oop_child_guard (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_oop_child_guard group by a")
	tk.MustExec("create materialized view log on mv_oop_child_guard (a, s, cnt)")
	tk.MustExec("create materialized view mv_oop_child_guard_dependent (a, s, cnt, cnt_s) refresh fast as select a, sum(s), count(1), count(s) from mv_oop_child_guard group by a")

	err := tk.ExecToErr("refresh materialized view mv_oop_child_guard complete out of place")
	require.ErrorContains(t, err, "complete OUT OF PLACE is not supported")
	require.ErrorContains(t, err, "dependent materialized view")
	tk.MustQuery("show tables like '\\_\\_mv\\_shadow\\_%'").Check(testkit.Rows())

	tk.MustExec("drop materialized view mv_oop_child_guard_dependent")
	tk.MustExec("drop materialized view log on mv_oop_child_guard")
	tk.MustExec("drop materialized view mv_oop_child_guard")
	tk.MustExec("drop materialized view log on t_oop_child_guard")
	tk.MustExec("drop table t_oop_child_guard")
}
