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

package autoanalyze_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/dxf/autoanalyze"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/exec"
	statstestutil "github.com/pingcap/tidb/pkg/statistics/handle/ddl/testutil"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func TestAutoAnalyzeThroughDXF(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int, index idx(a))")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3)")
	tk.MustExec("set global tidb_enable_dist_auto_analyze = ON")
	defer tk.MustExec("set global tidb_enable_dist_auto_analyze = OFF")
	// The stats owner has a service scope of its own, which the tasks must not follow.
	vardef.ServiceScope.Store("stats-owner")
	defer vardef.ServiceScope.Store("")
	targetScope := ""
	if kerneltype.IsNextGen() {
		targetScope = handle.NextGenTargetScope
	}

	ctx := context.Background()
	h := dom.StatsHandle()
	require.NoError(t, statstestutil.HandleNextDDLEventWithTxn(h))
	tk.MustExec("flush stats_delta *.*")
	tbl, err := dom.InfoSchema().TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	// setRowCount makes the stats owner see the table or partition with this
	// number of rows. Its next analysis sets the count back to the real one.
	setRowCount := func(physicalID, count int64) {
		version := tk.MustQuery("select max(version) + 1 from mysql.stats_meta").Rows()[0][0]
		tk.MustExec("update mysql.stats_meta set count = ?, version = ? where table_id = ?", count, version, physicalID)
		require.NoError(t, h.Update(ctx, dom.InfoSchema()))
		tblStats, ok := h.GetNonPseudoPhysicalTableStats(physicalID)
		require.True(t, ok)
		require.Equal(t, count, tblStats.RealtimeCount)
	}
	// autoAnalyze runs the statement the way the priority queue does.
	autoAnalyze := func(table exec.AnalyzedTable, sql string, params ...any) bool {
		var ok bool
		require.NoError(t, statsutil.CallWithSCtx(h.SPool(), func(sctx sessionctx.Context) error {
			ok = exec.AutoAnalyze(sctx, h, dom.SysProcTracker(), statistics.Version2, false, table, sql, params...)
			return nil
		}))
		return ok
	}
	const allTasks = "select type, state, concurrency, target_scope from mysql.tidb_global_task union all " +
		"select type, state, concurrency, target_scope from mysql.tidb_global_task_history"
	const finishedJobs = "select count(*) from mysql.analyze_jobs " +
		"where table_name = 't' and job_info like 'auto analyze%' and state = 'finished'"
	succeededTask := fmt.Sprintf("AutoAnalyze succeed 1 %s", targetScope)

	// The statement can run on another node, which does not update the cache of the stats
	// owner. Dropping the cache after the subtask shows the same to the dispatcher.
	const afterSubtask = "github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/afterRunSubtask"
	testfailpoint.EnableCall(t, afterSubtask, func(taskexecutor.TaskExecutor, *error, context.Context) {
		h.Clear()
	})

	setRowCount(tblInfo.ID, autoanalyze.RowLimit)
	require.True(t, h.HandleAutoAnalyze())
	testfailpoint.Disable(t, afterSubtask)

	// The statement went through DXF and succeeded there, with one slot and
	// outside the scope of the stats owner.
	tk.MustQuery(allTasks).Check(testkit.Rows(succeededTask))
	// It is still recorded as an auto analyze job, and the dispatcher has loaded the
	// new stats into the cache of the stats owner.
	tk.MustQuery(finishedJobs).Check(testkit.Rows("1"))
	require.Equal(t, int64(3), h.GetPhysicalTableStats(tblInfo.ID, tblInfo).RealtimeCount)

	// Below the row limit the statement runs on the stats owner. So does the
	// statement of a table whose stats this node does not know, whatever row
	// count the storage has for it. Neither submits a task.
	setRowCount(tblInfo.ID, autoanalyze.RowLimit-1)
	require.True(t, autoAnalyze(exec.AnalyzedTable{ID: tblInfo.ID}, "analyze table %n.%n", "test", "t"))
	setRowCount(tblInfo.ID, autoanalyze.RowLimit)
	h.Clear()
	require.True(t, autoAnalyze(exec.AnalyzedTable{ID: tblInfo.ID}, "analyze table %n.%n", "test", "t"))
	tk.MustQuery(allTasks).Check(testkit.Rows(succeededTask))
	tk.MustQuery(finishedJobs).Check(testkit.Rows("3"))

	// Only the partitions that the statement names count, whatever the rows of the
	// other partitions and the table-level count are.
	tk.MustExec("create table pt (a int, b int, index idx(a)) partition by range (a) " +
		"(partition p0 values less than (2), partition p1 values less than (10))")
	tk.MustExec("insert into pt values (1, 1), (2, 2), (3, 3)")
	require.NoError(t, statstestutil.HandleNextDDLEventWithTxn(h))
	tk.MustExec("flush stats_delta *.*")
	pt, err := dom.InfoSchema().TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("pt"))
	require.NoError(t, err)
	partitions := pt.Meta().GetPartitionInfo().Definitions
	setRowCount(pt.Meta().ID, autoanalyze.RowLimit)
	setRowCount(partitions[0].ID, autoanalyze.RowLimit-1)
	setRowCount(partitions[1].ID, 1)
	require.True(t, autoAnalyze(exec.AnalyzedTable{ID: pt.Meta().ID, PartitionNames: []string{"p0"}},
		"analyze table %n.%n partition %n", "test", "pt", "p0"))
	tk.MustQuery(allTasks).Check(testkit.Rows(succeededTask))
	// The two partitions together have enough rows.
	setRowCount(pt.Meta().ID, autoanalyze.RowLimit-1)
	setRowCount(partitions[0].ID, autoanalyze.RowLimit-1)
	require.True(t, autoAnalyze(exec.AnalyzedTable{ID: pt.Meta().ID, PartitionNames: []string{"p0", "p1"}},
		"analyze table %n.%n partition %n, %n", "test", "pt", "p0", "p1"))
	tk.MustQuery(allTasks).Check(testkit.Rows(succeededTask, succeededTask))

	// Another stats owner has left a task for the table. If it runs the same
	// statement, this statement waits for it and submits nothing. If it runs
	// another statement, this one submits its own task while that one is active.
	keyspace := store.GetKeyspace()
	dxfCtx := kv.WithInternalSourceType(ctx, kv.InternalDistTask)
	const activeTasks = "select count(*) from mysql.tidb_global_task where state in ('pending', 'running')"
	for i, c := range []struct {
		sql     string
		params  []string
		adopted bool
	}{
		{"analyze table %n.%n", []string{"test", "t"}, true},
		{"analyze table %n.%n index %n", []string{"test", "t", "idx"}, false},
	} {
		setRowCount(tblInfo.ID, autoanalyze.RowLimit)
		// The task of the other owner starts when release is closed. The timeout
		// only lets the test fail instead of hang.
		release := make(chan struct{})
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/beforeCallStartTaskExecutor",
			func(*proto.TaskBase) {
				select {
				case <-release:
				case <-time.After(30 * time.Second):
				}
			},
		)
		meta, err := json.Marshal(&autoanalyze.TaskMeta{SQL: c.sql, Params: c.params, TableID: tblInfo.ID})
		require.NoError(t, err)
		tasks := len(tk.MustQuery(allTasks).Rows())
		taskKey := fmt.Sprintf("%s/%s/%d/other-owner-%d", keyspace, proto.AutoAnalyze, tblInfo.ID, i)
		_, err = handle.SubmitTask(dxfCtx, taskKey, proto.AutoAnalyze, keyspace, 1, targetScope, 1, meta)
		require.NoError(t, err)
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/dxf/autoanalyze/afterFindActiveTask", func() {
			if c.adopted {
				close(release)
			}
		})
		newTasks := 1
		if !c.adopted {
			newTasks = 2
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/dxf/framework/handle/afterDXFTaskSubmitted", func() {
				tk.MustQuery(activeTasks).Check(testkit.Rows("2"))
				close(release)
			})
		}
		require.True(t, autoAnalyze(exec.AnalyzedTable{ID: tblInfo.ID}, "analyze table %n.%n", "test", "t"))
		if c.adopted {
			// The statement has waited for the task of the other owner.
			tk.MustQuery(activeTasks).Check(testkit.Rows("0"))
		} else {
			// The statement has waited for its own task only. The other one can still run.
			require.Eventually(t, func() bool {
				return tk.MustQuery(activeTasks).Rows()[0][0] == "0"
			}, 30*time.Second, 50*time.Millisecond)
		}
		tk.MustQuery(allTasks).Check(testkit.Rows(slices.Repeat([]string{succeededTask}, tasks+newTasks)...))
	}

	// The table of a statement can be gone when the dispatcher runs. The statement then runs
	// on the stats owner and fails there, as it does without DXF. It submits no task.
	tasks := tk.MustQuery(allTasks).Rows()
	tk.MustExec("drop table t")
	require.False(t, autoAnalyze(exec.AnalyzedTable{ID: tblInfo.ID}, "analyze table %n.%n", "test", "t"))
	tk.MustQuery(allTasks).Check(tasks)
}
