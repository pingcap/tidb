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
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestPurgeMaterializedViewLogPreservesNestedChildDelta(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_nested_purge (a int not null, b int not null, v int not null)")
	tk.MustExec("insert into t_nested_purge values (1, 1, 10), (2, 1, 20)")
	tk.MustExec("create materialized view log on t_nested_purge (a, b, v)")
	tk.MustExec("create materialized view mv_nested_purge_parent (a, b, s, cnt) refresh fast as select a, b, sum(v), count(1) from t_nested_purge group by a, b")
	tk.MustExec("create materialized view log on mv_nested_purge_parent (a, b, s, cnt)")
	tk.MustExec("create materialized view mv_nested_purge_child (a, s, cnt, cnt_s) refresh fast as select a, sum(s), count(1), count(s) from mv_nested_purge_parent group by a")

	is := dom.InfoSchema()
	parentMLog, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$mv_nested_purge_parent"))
	require.NoError(t, err)
	parentMLogID := parentMLog.Meta().ID
	childMV, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_nested_purge_child"))
	require.NoError(t, err)
	childReadTSORow := tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", childMV.Meta().ID)).Rows()
	require.Len(t, childReadTSORow, 1)
	childReadTSO, err := strconv.ParseUint(fmt.Sprint(childReadTSORow[0][0]), 10, 64)
	require.NoError(t, err)
	beforeDeltaRows := tk.MustQuery("select count(*) from `$mlog$mv_nested_purge_parent`").Rows()
	require.Len(t, beforeDeltaRows, 1)
	beforeDeltaCount, err := strconv.ParseInt(fmt.Sprint(beforeDeltaRows[0][0]), 10, 64)
	require.NoError(t, err)

	tk.MustExec("insert into t_nested_purge values (1, 2, 7)")
	tk.MustExec("refresh materialized view mv_nested_purge_parent fast")

	afterParentRefreshRows := tk.MustQuery("select count(*) from `$mlog$mv_nested_purge_parent`").Rows()
	require.Len(t, afterParentRefreshRows, 1)
	afterParentRefreshCount, err := strconv.ParseInt(fmt.Sprint(afterParentRefreshRows[0][0]), 10, 64)
	require.NoError(t, err)
	require.Greater(t, afterParentRefreshCount, beforeDeltaCount)

	// The child has not consumed the parent refresh yet. PURGE must stop at the child's
	// LAST_SUCCESS_READ_TSO and retain the parent delta for the subsequent FAST refresh.
	tk.MustExec("purge materialized view log on mv_nested_purge_parent")
	afterPurgeRows := tk.MustQuery("select count(*) from `$mlog$mv_nested_purge_parent`").Rows()
	require.Len(t, afterPurgeRows, 1)
	afterPurgeCount, err := strconv.ParseInt(fmt.Sprint(afterPurgeRows[0][0]), 10, 64)
	require.NoError(t, err)
	require.Equal(t, afterParentRefreshCount, afterPurgeCount)
	tk.MustQuery(fmt.Sprintf("select LAST_PURGED_TSO from mysql.tidb_mlog_purge_info where MLOG_ID = %d", parentMLogID)).
		Check(testkit.Rows(fmt.Sprintf("%d", childReadTSO)))

	tk.MustExec("refresh materialized view mv_nested_purge_child fast")
	tk.MustQuery("select a, s, cnt, cnt_s from mv_nested_purge_child order by a").Check(testkit.Rows(
		"1 17 2 2",
		"2 20 1 1",
	))

	tk.MustExec("drop materialized view mv_nested_purge_child")
	tk.MustExec("drop materialized view log on mv_nested_purge_parent")
	tk.MustExec("drop materialized view mv_nested_purge_parent")
	tk.MustExec("drop materialized view log on t_nested_purge")
	tk.MustExec("drop table t_nested_purge")
}

func TestPurgeMaterializedViewLogUsesSlowestNestedChildCheckpoint(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_nested_purge_multi (a int not null, b int not null, v int not null)")
	tk.MustExec("insert into t_nested_purge_multi values (1, 1, 10), (2, 1, 20)")
	tk.MustExec("create materialized view log on t_nested_purge_multi (a, b, v)")
	tk.MustExec("create materialized view mv_nested_purge_parent_multi (a, b, s, cnt) refresh fast as select a, b, sum(v), count(1) from t_nested_purge_multi group by a, b")
	tk.MustExec("create materialized view log on mv_nested_purge_parent_multi (a, b, s, cnt)")

	childNames := []string{"mv_nested_purge_child_1", "mv_nested_purge_child_2"}
	childIDs := make([]int64, len(childNames))
	for i, name := range childNames {
		tk.MustExec(fmt.Sprintf("create materialized view %s (a, s, cnt, cnt_s) refresh fast as select a, sum(s), count(1), count(s) from mv_nested_purge_parent_multi group by a", name))
		child, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr(name))
		require.NoError(t, err)
		childIDs[i] = child.Meta().ID
	}
	parentMLog, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$mv_nested_purge_parent_multi"))
	require.NoError(t, err)
	readCheckpoint := func(childID int64) uint64 {
		t.Helper()
		rows := tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", childID)).Rows()
		require.Len(t, rows, 1)
		checkpoint, err := strconv.ParseUint(fmt.Sprint(rows[0][0]), 10, 64)
		require.NoError(t, err)
		require.NotZero(t, checkpoint)
		return checkpoint
	}
	purgeAndCheckCheckpoint := func(expected uint64) {
		t.Helper()
		tk.MustExec("purge materialized view log on mv_nested_purge_parent_multi")
		tk.MustQuery(fmt.Sprintf("select LAST_PURGED_TSO from mysql.tidb_mlog_purge_info where MLOG_ID = %d", parentMLog.Meta().ID)).
			Check(testkit.Rows(fmt.Sprint(expected)))
	}

	child1Checkpoint := readCheckpoint(childIDs[0])
	tk.MustExec("insert into t_nested_purge_multi values (1, 2, 7)")
	tk.MustExec("refresh materialized view mv_nested_purge_parent_multi fast")
	tk.MustExec("refresh materialized view mv_nested_purge_child_2 fast")
	child2Checkpoint := readCheckpoint(childIDs[1])
	require.Greater(t, child2Checkpoint, child1Checkpoint)
	tk.MustQuery("select a, s, cnt, cnt_s from mv_nested_purge_child_1 order by a").Check(testkit.Rows("1 10 1 1", "2 20 1 1"))
	tk.MustQuery("select a, s, cnt, cnt_s from mv_nested_purge_child_2 order by a").Check(testkit.Rows("1 17 2 2", "2 20 1 1"))

	// Put one delta before the faster child's checkpoint and one after it. Neither may be
	// purged yet: child 1 still needs both, even though child 2 has consumed the first.
	tk.MustExec("insert into t_nested_purge_multi values (3, 1, 8)")
	tk.MustExec("refresh materialized view mv_nested_purge_parent_multi fast")
	tk.MustQuery(fmt.Sprintf("select count(*) from `$mlog$mv_nested_purge_parent_multi` where _tidb_commit_ts > %d and _tidb_commit_ts <= %d", child1Checkpoint, child2Checkpoint)).Check(testkit.Rows("1"))
	purgeAndCheckCheckpoint(child1Checkpoint)
	tk.MustQuery("select a, b, s, cnt from `$mlog$mv_nested_purge_parent_multi` order by a, b").Check(testkit.Rows("1 2 7 1", "3 1 8 1"))

	// Once child 1 catches up, child 2 becomes the slower consumer. Only the first delta
	// is now disposable; the second must remain available for child 2's next FAST refresh.
	tk.MustExec("refresh materialized view mv_nested_purge_child_1 fast")
	child1Checkpoint = readCheckpoint(childIDs[0])
	require.Greater(t, child1Checkpoint, child2Checkpoint)
	finalRows := testkit.Rows("1 17 2 2", "2 20 1 1", "3 8 1 1")
	tk.MustQuery("select a, s, cnt, cnt_s from mv_nested_purge_child_1 order by a").Check(finalRows)
	purgeAndCheckCheckpoint(child2Checkpoint)
	tk.MustQuery("select a, b, s, cnt from `$mlog$mv_nested_purge_parent_multi` order by a, b").Check(testkit.Rows("3 1 8 1"))

	tk.MustExec("refresh materialized view mv_nested_purge_child_2 fast")
	require.Greater(t, readCheckpoint(childIDs[1]), child1Checkpoint)
	tk.MustQuery("select a, s, cnt, cnt_s from mv_nested_purge_child_2 order by a").Check(finalRows)
	purgeAndCheckCheckpoint(child1Checkpoint)
	tk.MustQuery("select count(*) from `$mlog$mv_nested_purge_parent_multi`").Check(testkit.Rows("0"))

	for _, name := range childNames {
		tk.MustExec("drop materialized view " + name)
	}
	tk.MustExec("drop materialized view log on mv_nested_purge_parent_multi")
	tk.MustExec("drop materialized view mv_nested_purge_parent_multi")
	tk.MustExec("drop materialized view log on t_nested_purge_multi")
	tk.MustExec("drop table t_nested_purge_multi")
}
