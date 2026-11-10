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
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/dxf/autoanalyze"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/resourcegroup/runaway"
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
	// The scheduler has placed the task when it is about to create the subtask.
	var placedBeforeSubtask atomic.Int64
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/dxf/framework/scheduler/cancelBeforeUpdateTask", func(int64) {
		placedBeforeSubtask.Store(int64(autoanalyze.PlacedTasks()))
	})

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
	// The scheduler held a place for the task, and has released it.
	require.EqualValues(t, 1, placedBeforeSubtask.Load())
	require.Zero(t, autoanalyze.PlacedTasks())
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

	// The schedulers have released the places of all these tasks.
	require.Zero(t, autoanalyze.PlacedTasks())
	// Tasks that are placed against the same used slots count the places of each
	// other, and spread over the nodes. Without that they would all go to b.
	nodes := []string{"a", "b"}
	usedSlots := map[string]int{"a": 1}
	firstNodes := make([]string, 0, 4)
	for taskID := int64(1); taskID <= 4; taskID++ {
		firstNodes = append(firstNodes, autoanalyze.PlaceTask(taskID, nodes, usedSlots)[0])
	}
	require.Equal(t, []string{"b", "a", "b", "a"}, firstNodes)
	// A task that is placed again does not count its own place.
	require.Equal(t, []string{"a", "b"}, autoanalyze.PlaceTask(4, nodes, usedSlots))
	// A released place no longer counts.
	for taskID := int64(1); taskID <= 4; taskID++ {
		autoanalyze.ReleaseTask(taskID)
	}
	require.Zero(t, autoanalyze.PlacedTasks())

	// The statement goes to the node with the fewest used slots. With equal used
	// slots, tasks with consecutive IDs start at consecutive nodes in host order.
	nodes = []string{"a", "b", "c"}
	require.Equal(t, []string{"a", "b", "c"}, autoanalyze.OrderByUsedSlots(nodes, nil, 3))
	require.Equal(t, []string{"b", "c", "a"}, autoanalyze.OrderByUsedSlots(nodes, nil, 4))
	// Only the nodes with equal used slots rotate. A node without subtasks has
	// none, and the used slots of other nodes do not matter.
	nodes = []string{"a", "b", "c", "d"}
	usedSlots = map[string]int{"a": 3, "c": 5, "d": 0, "other-scope": 1}
	require.Equal(t, []string{"b", "d", "a", "c"}, autoanalyze.OrderByUsedSlots(nodes, usedSlots, 4))
	require.Equal(t, []string{"d", "b", "a", "c"}, autoanalyze.OrderByUsedSlots(nodes, usedSlots, 7))
}

// TestKillDispatchedAutoAnalyze kills the auto-analyze process of the stats
// owner while the statement runs as a DXF task. A kill must cancel the task,
// except a kill by the graceful shutdown of the stats owner.
func TestKillDispatchedAutoAnalyze(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int, index idx(a)) partition by range (a) " +
		"(partition p0 values less than (2), partition p1 values less than (10))")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3)")
	tk.MustExec("set global tidb_enable_dist_auto_analyze = ON")
	defer tk.MustExec("set global tidb_enable_dist_auto_analyze = OFF")
	ctx := context.Background()
	h := dom.StatsHandle()
	require.NoError(t, statstestutil.HandleNextDDLEventWithTxn(h))
	tk.MustExec("flush stats_delta *.*")
	// A partition with enough rows makes the statement that names it go to DXF.
	tbl, err := dom.InfoSchema().TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tk.MustExec("update mysql.stats_meta set count = ? where table_id = ?",
		autoanalyze.RowLimit, tbl.Meta().GetPartitionInfo().Definitions[0].ID)
	require.NoError(t, h.Update(ctx, dom.InfoSchema()))
	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
	}()
	const allTasks = "select type, state from mysql.tidb_global_task union all " +
		"select type, state from mysql.tidb_global_task_history"
	const statement = "analyze table `test`.`t` partition `p0`, `p1`"
	analyze := func(sctx sessionctx.Context) bool {
		return exec.AutoAnalyze(sctx, h, dom.SysProcTracker(), statistics.Version2, false,
			exec.AnalyzedTable{ID: tbl.Meta().ID, PartitionNames: []string{"p0", "p1"}},
			"analyze table %n.%n partition %n, %n", "test", "t", "p0", "p1")
	}

	// The process that waits for a task shows nothing of the last statement of its session: the
	// runaway checker would get the process killed, and the start TS would hold back GC.
	const afterFindActiveTask = "github.com/pingcap/tidb/pkg/dxf/autoanalyze/afterFindActiveTask"
	require.NoError(t, statsutil.CallWithSCtx(h.SPool(), func(sctx sessionctx.Context) error {
		// The last statement of the session had a plan, a start TS and a runaway checker.
		_, _, err := statsutil.ExecRows(sctx, "select count from mysql.stats_meta where table_id = %?", tbl.Meta().ID)
		require.NoError(t, err)
		sctx.GetSessionVars().StmtCtx.RunawayChecker = &runaway.Checker{}
		require.NotNil(t, sctx.ShowProcess().Plan)
		require.NotZero(t, sctx.GetSessionVars().TxnCtx.StartTS)
		testfailpoint.EnableCall(t, afterFindActiveTask, func() {
			processes := dom.SysProcTracker().GetSysProcessList()
			require.Len(t, processes, 1)
			for id, info := range processes {
				require.Equal(t, statement, info.Info)
				require.Nil(t, info.RunawayChecker)
				require.Nil(t, info.Plan)
				require.Zero(t, info.CurTxnStartTS)
				dom.SysProcTracker().KillSysProcess(id)
			}
		})
		// The kill comes between the lookup of an active task and the submit: there is no task.
		require.False(t, analyze(sctx))
		return nil
	}))
	tk.MustQuery(allTasks).Check(testkit.Rows())
	require.Empty(t, dom.SysProcTracker().GetSysProcessList())

	// The process that waits for a task is the only auto-analyze process when it
	// has looked for an active task: no task runs its statement yet.
	var waiting atomic.Uint64
	testfailpoint.EnableCall(t, afterFindActiveTask, func() {
		for id := range dom.SysProcTracker().GetSysProcessList() {
			waiting.Store(id)
		}
	})
	// waitForStatement waits until the statement runs: a second auto-analyze
	// process shows the statement of the waiting one. It returns the waiting one.
	waitForStatement := func() uint64 {
		require.Eventually(t, func() bool {
			processes := dom.SysProcTracker().GetSysProcessList()
			waitingInfo, ok := processes[waiting.Load()]
			if !ok || len(processes) != 2 {
				return false
			}
			for _, info := range processes {
				if info.Info != waitingInfo.Info {
					return false
				}
			}
			return true
		}, 30*time.Second, 50*time.Millisecond)
		return waiting.Load()
	}

	// The statement blocks until it is killed.
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/mockSlowAnalyzeV2", "return")
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.HandleAutoAnalyze()
	}()
	waitingID := waitForStatement()
	// The task has one slot, which does not limit the statement: it analyzes both
	// partitions at once, as tidb_auto_build_stats_concurrency allows.
	const runningTask = "select concurrency from mysql.tidb_global_task where state = 'running'"
	tk.MustQuery(runningTask).Check(testkit.Rows("1"))
	require.Eventually(t, func() bool {
		return tk.MustQuery("select count(*) from mysql.analyze_jobs where state = 'running'").Rows()[0][0] == "2"
	}, 30*time.Second, 50*time.Millisecond)
	dom.SysProcTracker().KillSysProcess(waitingID)

	select {
	case <-done:
	case <-time.After(60 * time.Second):
		require.FailNow(t, "the auto analyze did not stop after it was killed")
	}
	require.Empty(t, dom.SysProcTracker().GetSysProcessList())
	tk.MustQuery(allTasks).Check(testkit.Rows("AutoAnalyze reverted"))

	// The stats owner shuts down gracefully. The test kills only the process that
	// waits for the task, which models a task that runs on another node: the
	// waiting stops and the task runs on.
	analyzed := make(chan bool, 1)
	go func() {
		_ = statsutil.CallWithSCtx(h.SPool(), func(sctx sessionctx.Context) error {
			analyzed <- analyze(sctx)
			return nil
		})
	}()
	waitingID = waitForStatement()
	dom.StopAutoAnalyze()
	dom.SysProcTracker().KillSysProcess(waitingID)
	select {
	case ok := <-analyzed:
		require.False(t, ok)
	case <-time.After(60 * time.Second):
		require.FailNow(t, "the auto analyze did not stop when the stats owner shut down")
	}
	tk.MustQuery(runningTask).Check(testkit.Rows("1"))
	running := dom.SysProcTracker().GetSysProcessList()
	require.Len(t, running, 1)

	// Stop the statement, which blocks until it is killed.
	for id := range running {
		dom.SysProcTracker().KillSysProcess(id)
	}
	require.Eventually(t, func() bool {
		return len(tk.MustQuery(runningTask).Rows()) == 0 && len(dom.SysProcTracker().GetSysProcessList()) == 0
	}, 30*time.Second, 50*time.Millisecond)
}
