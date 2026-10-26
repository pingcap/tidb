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

package executor_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

type refreshRuntimeObserver struct {
	started, ended []string
	onStepEnd      func(executor.MVRefreshStepForTest)
}

func (o *refreshRuntimeObserver) OnStepStart(step executor.MVRefreshStepForTest, _ time.Time) {
	o.started = append(o.started, step.Name)
}
func (o *refreshRuntimeObserver) OnStepEnd(step executor.MVRefreshStepForTest, _ time.Time, _ error) {
	o.ended = append(o.ended, step.Name)
	if o.onStepEnd != nil {
		o.onStepEnd(step)
	}
}
func (*refreshRuntimeObserver) OnStepPlanRows(executor.MVRefreshStepForTest, [][]string) {}

func runRefreshRuntime(t *testing.T, tk *testkit.TestKit, mode string, observer *refreshRuntimeObserver) error {
	t.Helper()
	stmts, err := session.Parse(tk.Session(), "refresh materialized view test.mv_runtime "+mode)
	require.NoError(t, err)
	stmt := stmts[0].(*ast.RefreshMaterializedViewStmt)
	query := tk.Session().Value(sessionctx.QueryString)
	tk.Session().SetValue(sessionctx.QueryString, stmt.Text())
	defer tk.Session().SetValue(sessionctx.QueryString, query)
	require.NoError(t, executor.ResetContextOfStmt(tk.Session(), stmt))
	e := executor.NewRefreshMaterializedViewExecForTest(tk.Session(), stmt, observer)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnMViewMaintenance)
	require.NoError(t, e.Open(ctx))
	defer func() { require.NoError(t, e.Close()) }()
	if err := e.Next(ctx, nil); err != nil {
		return err
	}
	// Utility executors perform their work once even if Next is called again.
	return e.Next(ctx, nil)
}

func TestMaterializedViewRefreshRuntime(t *testing.T) {
	for _, mode := range []string{"fast", "complete in place", "complete delta apply", "complete out of place"} {
		t.Run(mode, func(t *testing.T) {
			store, dom := testkit.CreateMockStoreAndDomain(t)
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("set tidb_mview_enable=on")
			tk.MustExec("use test")
			tk.MustExec("create table src_runtime (id int primary key, g int not null, v int not null)")
			tk.MustExec("insert into src_runtime values (1,1,10),(2,1,20),(3,2,30)")
			tk.MustExec("create materialized view log on src_runtime (id,g,v)")
			tk.MustExec("create materialized view mv_runtime (g,s,cnt) refresh fast next date_add(now(), interval 1 hour) as select g,sum(v),count(*) from src_runtime group by g")
			before, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_runtime"))
			require.NoError(t, err)
			lastRead := tk.MustQuery("select last_success_read_tso from mysql.tidb_mview_refresh_info").Rows()[0][0]
			tk.MustExec("delete from src_runtime where id=2")
			tk.MustExec("update src_runtime set g=3,v=5 where id=3")
			tk.MustExec("insert into src_runtime values (4,4,7)")
			observer := &refreshRuntimeObserver{}
			require.NoError(t, runRefreshRuntime(t, tk, mode, observer))
			tk.MustQuery("select * from mv_runtime order by g").Check(testkit.Rows("1 10 1", "3 5 1", "4 7 1"))
			tk.MustQuery("select refresh_status,refresh_method from mysql.tidb_mview_refresh_hist").Check(testkit.Rows("success " + mode + " manual"))
			newRead := tk.MustQuery("select last_success_read_tso from mysql.tidb_mview_refresh_info").Rows()[0][0]
			oldTS, err := strconv.ParseUint(fmt.Sprint(lastRead), 10, 64)
			require.NoError(t, err)
			newTS, err := strconv.ParseUint(fmt.Sprint(newRead), 10, 64)
			require.NoError(t, err)
			require.Greater(t, newTS, oldTS)
			require.Equal(t, observer.started, observer.ended)
			require.Contains(t, observer.ended, "FINALIZE_HIST")
			after, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_runtime"))
			require.NoError(t, err)
			if mode == "complete out of place" {
				require.NotEqual(t, before.Meta().ID, after.Meta().ID)
				require.Contains(t, observer.ended, "OUT_OF_PLACE_CUTOVER")
			} else {
				require.Equal(t, before.Meta().ID, after.Meta().ID)
				require.Contains(t, observer.ended, "TXN_COMMIT")
			}
			tk.MustQuery("select count(*) from information_schema.tables where table_schema='test' and table_name like '__mv_shadow_%'").Check(testkit.Rows("0"))

			// A failed attempt must roll back data and leave the successful watermark intact.
			tk.MustExec("insert into src_runtime values (5,8,9)")
			point := "mockRefreshMaterializedViewErrorAfterDataChanges"
			if mode == "complete out of place" {
				point = "mockRefreshMaterializedViewOutOfPlaceBuildErrorAfterLoadShadow"
			}
			testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/"+point, `return("runtime failure")`)
			require.ErrorContains(t, runRefreshRuntime(t, tk, mode, &refreshRuntimeObserver{}), "runtime failure")
			tk.MustQuery("select * from mv_runtime order by g").Check(testkit.Rows("1 10 1", "3 5 1", "4 7 1"))
			tk.MustQuery("select last_success_read_tso from mysql.tidb_mview_refresh_info").Check(testkit.Rows(fmt.Sprint(newRead)))
			tk.MustQuery("select refresh_status from mysql.tidb_mview_refresh_hist order by refresh_job_id").Check(testkit.Rows("success", "failed"))
			tk.MustQuery("select count(*) from information_schema.tables where table_schema='test' and table_name like '__mv_shadow_%'").Check(testkit.Rows("0"))
			lockName := fmt.Sprintf("mv_refresh_%d_%d", after.Meta().DBID, after.Meta().ID)
			tk.MustQuery("select is_free_lock('" + lockName + "')").Check(testkit.Rows("1"))
			// The same runtime must remain usable after rollback and session cleanup.
			testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/executor/"+point)
			require.NoError(t, runRefreshRuntime(t, tk, mode, &refreshRuntimeObserver{}))
			tk.MustQuery("select * from mv_runtime order by g").Check(testkit.Rows("1 10 1", "3 5 1", "4 7 1", "8 9 1"))
			tk.MustQuery("select refresh_status from mysql.tidb_mview_refresh_hist order by refresh_job_id").Check(testkit.Rows("success", "failed", "success"))
		})
	}
}

func TestMaterializedViewRefreshSlowLog(t *testing.T) {
	for _, mode := range []string{"fast", "complete in place", "complete delta apply", "complete out of place"} {
		t.Run(mode, func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("set tidb_mview_enable=on")
			tk.MustExec("use test")
			tk.MustExec("create table src_runtime (id int primary key, g int not null, v int not null)")
			tk.MustExec("insert into src_runtime values (1,1,10)")
			tk.MustExec("create materialized view log on src_runtime (id,g,v)")
			tk.MustExec("create materialized view mv_runtime (g,s,cnt) refresh fast as select g,sum(v),count(*) from src_runtime group by g")

			for _, tc := range []struct {
				name      string
				total     time.Duration
				execution time.Duration
				failed    bool
				wantLog   bool
			}{
				{name: "slow execution", total: time.Minute, execution: 59 * time.Second},
				{name: "overhead below threshold", total: time.Minute, execution: 55*time.Second + time.Nanosecond},
				{name: "overhead at threshold", total: time.Minute, execution: 55 * time.Second, wantLog: true},
				{name: "overhead above threshold", total: time.Minute, execution: 54 * time.Second, wantLog: true},
				{name: "failed slow execution", total: time.Minute, execution: 59 * time.Second, failed: true},
				{name: "failed overhead above threshold", total: time.Minute, execution: 54 * time.Second, failed: true, wantLog: true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					core, recorded := observer.New(zap.InfoLevel)
					restoreLog := log.ReplaceGlobals(zap.New(core), &log.ZapProperties{Level: zap.NewAtomicLevelAt(zap.InfoLevel)})
					t.Cleanup(restoreLog)
					var measuredExecution time.Duration
					calls := 0
					testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewSlowLogDurations", func(total, execution *time.Duration) {
						calls++
						measuredExecution = *execution
						// Preserve the real measurement for assertions while controlling elapsed time without sleeps.
						*total, *execution = tc.total, tc.execution
					})
					if tc.failed {
						point := "mockRefreshMaterializedViewErrorAfterDataChanges"
						if mode == "complete out of place" {
							point = "mockRefreshMaterializedViewOutOfPlaceBuildErrorAfterLoadShadow"
						}
						testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/"+point, `return("runtime failure")`)
					}
					err := runRefreshRuntime(t, tk, mode, &refreshRuntimeObserver{})
					if tc.failed {
						require.ErrorContains(t, err, "runtime failure")
					} else {
						require.NoError(t, err)
					}
					require.Equal(t, 1, calls)
					require.Positive(t, measuredExecution, "data execution must be timed in every refresh mode, including failures")
					message := "refresh materialized view is slow"
					executionField := "executeRefreshMaterializedViewDataChanges"
					if mode == "complete out of place" {
						message = "refresh materialized view complete out-of-place is slow"
						executionField = "loadShadow"
						require.Empty(t, recorded.FilterMessage("refresh materialized view is slow").All(), "out-of-place refresh must only use its dedicated log")
					} else {
						require.Empty(t, recorded.FilterMessage("refresh materialized view complete out-of-place is slow").All())
					}
					entries := recorded.FilterMessage(message).All()
					if !tc.wantLog {
						require.Empty(t, entries, "slow data execution alone must not trigger the overhead log")
						return
					}
					require.Len(t, entries, 1)
					fields := entries[0].ContextMap()
					require.Equal(t, tc.total, fields["total"])
					require.Equal(t, tc.execution, fields[executionField])
					require.Equal(t, tc.total-tc.execution, fields["overhead"])
					require.Equal(t, 5*time.Second, fields["slowOverheadThreshold"])
					require.Equal(t, !tc.failed, fields["success"])
					if mode == "complete out of place" {
						for _, field := range []string{"lockRefreshInfoRow", "transactionTotal", "acquireAdvisoryLock", "releaseAdvisoryLock", "readRefreshInfo", "insertHistRunning", "finalizeHist"} {
							require.NotContains(t, fields, field)
						}
						require.Positive(t, fields["createShadow"])
						require.Contains(t, fields, "cutover")
						require.Contains(t, fields, "cleanupShadow")
						if tc.failed {
							require.Zero(t, fields["cutover"])
							require.Positive(t, fields["cleanupShadow"])
						} else {
							require.Positive(t, fields["cutover"])
							require.Zero(t, fields["cleanupShadow"])
						}
					} else {
						require.Contains(t, fields, "lockRefreshInfoRow")
						require.Contains(t, fields, "transactionTotal")
						require.NotContains(t, fields, "createShadow")
					}
				})
			}
		})
	}
}

type refreshShadowCleanupExecutor struct {
	ddl.Executor
	cleanupErr   error
	cleanupCalls int
	shadowName   ast.CIStr
}

func (e *refreshShadowCleanupExecutor) DropMaterializedViewShadowTable(ctx sessionctx.Context, schemaName, shadowName ast.CIStr) error {
	e.cleanupCalls++
	e.shadowName = shadowName
	if e.cleanupErr != nil {
		return e.cleanupErr
	}
	return e.Executor.DropMaterializedViewShadowTable(ctx, schemaName, shadowName)
}

func TestMaterializedViewRefreshOutOfPlacePanicCleanup(t *testing.T) {
	for _, tc := range []struct {
		name          string
		panicStep     string
		cleanupFails  bool
		failAfterLoad bool
		committed     bool
	}{
		{name: "panic after create", panicStep: "OUT_OF_PLACE_CREATE_SHADOW"},
		{name: "panic after load", panicStep: "OUT_OF_PLACE_LOAD_SHADOW"},
		{name: "panic and cleanup error", panicStep: "OUT_OF_PLACE_LOAD_SHADOW", cleanupFails: true},
		{name: "panic after cutover", panicStep: "OUT_OF_PLACE_CUTOVER", committed: true},
		{name: "error and cleanup error", failAfterLoad: true, cleanupFails: true},
		{name: "normal completion", committed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, dom := testkit.CreateMockStoreAndDomain(t)
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("set tidb_mview_enable=on")
			tk.MustExec("use test")
			tk.MustExec("create table src_runtime (id int primary key, g int not null, v int not null)")
			tk.MustExec("insert into src_runtime values (1,1,10)")
			tk.MustExec("create materialized view log on src_runtime (id,g,v)")
			tk.MustExec("create materialized view mv_runtime (g,s,cnt) refresh fast as select g,sum(v),count(*) from src_runtime group by g")
			before, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_runtime"))
			require.NoError(t, err)
			tk.MustExec("insert into src_runtime values (2,1,20)")

			cleanup := &refreshShadowCleanupExecutor{Executor: dom.DDLExecutor()}
			if tc.cleanupFails {
				cleanup.cleanupErr = fmt.Errorf("shadow cleanup failure")
			}
			dom.SetDDL(dom.DDL(), cleanup)
			t.Cleanup(func() { dom.SetDDL(dom.DDL(), cleanup.Executor) })
			core, recorded := observer.New(zap.WarnLevel)
			restoreLog := log.ReplaceGlobals(zap.New(core), &log.ZapProperties{Level: zap.NewAtomicLevelAt(zap.WarnLevel)})
			t.Cleanup(restoreLog)
			panicErr := fmt.Errorf("original refresh panic")
			stepObserver := &refreshRuntimeObserver{onStepEnd: func(step executor.MVRefreshStepForTest) {
				if step.Name == tc.panicStep {
					panic(panicErr)
				}
			}}
			if tc.failAfterLoad {
				testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewOutOfPlaceBuildErrorAfterLoadShadow", `return("runtime failure")`)
			}
			err = runRefreshRuntime(t, tk, "complete out of place", stepObserver)
			switch {
			case tc.panicStep != "":
				require.ErrorIs(t, err, panicErr, "cleanup must preserve the original panic for the outer recover")
			case tc.failAfterLoad:
				require.ErrorContains(t, err, "runtime failure")
				require.ErrorContains(t, err, "shadow cleanup failure")
			default:
				require.NoError(t, err)
			}

			if tc.committed {
				require.Zero(t, cleanup.cleanupCalls, "a successful cutover must disarm cleanup before observer callbacks")
				tk.MustQuery("select * from mv_runtime").Check(testkit.Rows("1 30 2"))
			} else {
				require.Equal(t, 1, cleanup.cleanupCalls)
				tk.MustQuery("select * from mv_runtime").Check(testkit.Rows("1 10 1"))
			}
			after, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_runtime"))
			require.NoError(t, err)
			if tc.committed {
				require.NotEqual(t, before.Meta().ID, after.Meta().ID)
			} else {
				require.Equal(t, before.Meta().ID, after.Meta().ID)
			}
			entries := recorded.FilterMessage("failed to cleanup shadow table after out-of-place complete refresh error").All()
			if tc.cleanupFails {
				require.Len(t, entries, 1)
				fields := entries[0].ContextMap()
				require.Equal(t, "test", fields["schema"])
				require.Equal(t, "mv_runtime", fields["mview"])
				require.Equal(t, cleanup.shadowName.O, fields["shadowTable"])
				require.Equal(t, "shadow cleanup failure", fields["error"])
				tk.MustQuery("select count(*) from information_schema.tables where table_schema='test' and table_name like '__mv_shadow_%'").Check(testkit.Rows("1"))
				require.NoError(t, cleanup.Executor.DropMaterializedViewShadowTable(tk.Session(), ast.NewCIStr("test"), cleanup.shadowName))
			} else {
				require.Empty(t, entries)
			}
			tk.MustQuery("select count(*) from information_schema.tables where table_schema='test' and table_name like '__mv_shadow_%'").Check(testkit.Rows("0"))
		})
	}
}
