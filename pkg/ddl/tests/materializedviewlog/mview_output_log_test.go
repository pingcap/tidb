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

package materializedviewlog

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/stretchr/testify/require"
)

func TestCreateMaterializedViewLogRefreshLockLifetime(t *testing.T) {
	for _, rollback := range []bool{false, true} {
		t.Run(fmt.Sprintf("rollback=%t", rollback), func(t *testing.T) {
			store, dom := testkit.CreateMockStoreAndDomain(t)
			tk := newMViewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("create table t_mlog_lock (a int not null, b int not null)")
			tk.MustExec("create materialized view log on t_mlog_lock (a, b)")
			tk.MustExec("create materialized view mv_mlog_lock (a, s, cnt) as select a, sum(b), count(1) from t_mlog_lock group by a")
			mv, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_mlog_lock"))
			require.NoError(t, err)
			lockName := fmt.Sprintf("mv_refresh_%d_%d", mv.Meta().DBID, mv.Meta().ID)
			paused := make(chan struct{})
			resume := make(chan struct{})
			var signalled atomic.Bool
			release := func() {
				select {
				case <-resume:
				default:
					close(resume)
				}
			}
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeWaitSchemaSynced", func(job *model.Job, _ int64) {
				if job.Type == model.ActionCreateMaterializedViewLog && job.TableName == "$mlog$mv_mlog_lock" && signalled.CompareAndSwap(false, true) {
					close(paused)
					<-resume
				}
			})
			const failurePoint = "github.com/pingcap/tidb/pkg/ddl/mockInsertMLogPurgeTableNotExists"
			if rollback {
				require.NoError(t, failpoint.Enable(failurePoint, "return(true)"))
				defer func() { require.NoError(t, failpoint.Disable(failurePoint)) }()
			}
			ddlDone := make(chan error, 1)
			go func() { ddlDone <- tk.ExecToErr("create materialized view log on mv_mlog_lock (a, s, cnt)") }()
			finished := false
			defer func() {
				release()
				if !finished {
					select {
					case <-ddlDone:
					case <-time.After(15 * time.Second):
						t.Error("timeout cleaning up CREATE MLog")
					}
				}
			}()
			select {
			case <-paused:
			case <-time.After(15 * time.Second):
				t.Fatal("timeout waiting for CREATE MLog schema sync")
			}
			tkLock := newMViewTestKit(t, store)
			tkLock.MustExec("use test")
			// The job must hold the refresh lock after its metadata transaction commits.
			tkLock.MustGetErrCode("refresh materialized view mv_mlog_lock fast", errno.ErrMVAdvisoryLockConflict)
			release()
			select {
			case err = <-ddlDone:
				finished = true
			case <-time.After(15 * time.Second):
				t.Fatal("timeout waiting for CREATE MLog completion")
			}
			if rollback {
				require.ErrorContains(t, err, "tidb_mlog_purge_info")
				require.NoError(t, failpoint.Disable(failurePoint))
				tk.MustQuery("show tables like '$mlog$mv_mlog_lock'").Check(testkit.Rows())
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, tkLock.Session().GetAdvisoryLock(lockName, 1), "the worker must release the lock before reuse")
			require.True(t, tkLock.Session().ReleaseAdvisoryLock(lockName))
			if rollback {
				tk.MustExec("create materialized view log on mv_mlog_lock (a, s, cnt)")
			}
		})
	}
}

func TestAlterMaterializedViewLogAddColumnRefreshConflict(t *testing.T) {
	for _, mode := range []string{"fast", "complete delta apply", "complete out of place"} {
		for _, columns := range []string{"s", "s, cnt"} {
			t.Run(mode+"/"+columns, func(t *testing.T) {
				store, dom := testkit.CreateMockStoreAndDomain(t)
				tk := newMViewTestKit(t, store)
				tk.MustExec("use test")
				tk.MustExec("create table t_alter_mlog (a int not null, b int not null)")
				tk.MustExec("insert into t_alter_mlog values (1, 10)")
				tk.MustExec("create materialized view log on t_alter_mlog (a, b)")
				tk.MustExec("create materialized view mv_alter_mlog (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_alter_mlog group by a")
				tk.MustExec("create materialized view log on mv_alter_mlog (a)")
				tk.MustExec("insert into t_alter_mlog values (2, 20)")

				paused := make(chan struct{})
				resume := make(chan struct{})
				pause := func() { close(paused); <-resume }
				release := func() {
					select {
					case <-resume:
					default:
						close(resume)
					}
				}
				pauseFailpoint := "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewAfterDataChanges"
				if mode == "complete out of place" {
					pauseFailpoint = "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewOutOfPlaceAfterBuildDataLoad"
					testfailpoint.EnableCall(t, pauseFailpoint, func(uint64) { pause() })
				} else {
					testfailpoint.EnableCall(t, pauseFailpoint, pause)
				}
				tkRefresh := newMViewTestKit(t, store)
				tkRefresh.MustExec("use test")
				refreshDone := make(chan error, 1)
				go func() { refreshDone <- tkRefresh.ExecToErr("refresh materialized view mv_alter_mlog " + mode) }()
				refreshFinished := false
				defer func() {
					release()
					if !refreshFinished {
						select {
						case <-refreshDone:
						case <-time.After(15 * time.Second):
							t.Error("timeout cleaning up refresh")
						}
					}
				}()
				select {
				case <-paused:
				case <-time.After(15 * time.Second):
					t.Fatal("timeout waiting for refresh")
				}

				alterSQL := "alter materialized view log on mv_alter_mlog add column (" + columns + ")"
				ddlDone := make(chan error, 1)
				go func() { ddlDone <- tk.ExecToErr(alterSQL) }()
				ddlFinished := false
				defer func() {
					release()
					if !ddlFinished {
						select {
						case <-ddlDone:
						case <-time.After(15 * time.Second):
							t.Error("timeout cleaning up ADD COLUMN")
						}
					}
				}()
				select {
				case err := <-ddlDone:
					ddlFinished = true
					require.Error(t, err, "ADD COLUMN must not complete while refresh holds its lock")
					require.True(t, exeerrors.ErrMVAdvisoryLockConflict.Equal(err), "%v", err)
					require.ErrorContains(t, err, "materialized view refresh is running")
				case <-time.After(15 * time.Second):
					t.Fatal("ADD COLUMN must terminate without blocking refresh cutover")
				}
				mlog, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$mv_alter_mlog"))
				require.NoError(t, err)
				require.Equal(t, []ast.CIStr{ast.NewCIStr("a")}, mlog.Meta().MaterializedViewLog.Columns)
				require.Len(t, mlog.Meta().Columns, 3, "a rejected ADD COLUMN must leave no intermediate columns")

				release()
				select {
				case err := <-refreshDone:
					refreshFinished = true
					require.NoError(t, err)
				case <-time.After(15 * time.Second):
					t.Fatal("timeout waiting for refresh completion")
				}
				tk.MustExec(alterSQL)
				tk.MustExec("insert into t_alter_mlog values (3, 30)")
				testfailpoint.Disable(t, pauseFailpoint)
				tk.MustExec("refresh materialized view mv_alter_mlog fast")
				tk.MustQuery("select a, s from `$mlog$mv_alter_mlog` where a = 3").Check(testkit.Rows("3 30"))
				tk.MustExec("update t_alter_mlog set b = 31 where a = 3")
				tk.MustExec("refresh materialized view mv_alter_mlog fast")
				tk.MustQuery("select s from `$mlog$mv_alter_mlog` where a = 3 and `_MLOG$_DML_TYPE` = 'U'").Sort().Check(testkit.Rows("30", "31"))
			})
		}
	}
}

func TestAlterMaterializedViewLogAddColumnRefreshLockLifetime(t *testing.T) {
	for _, columns := range []string{"s", "s, cnt"} {
		for _, rollback := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/rollback=%t", columns, rollback), func(t *testing.T) {
				store, dom := testkit.CreateMockStoreAndDomain(t)
				tk := newMViewTestKit(t, store)
				tk.MustExec("use test")
				tk.MustExec("create table t_alter_mlog_lock (a int not null, b int not null)")
				tk.MustExec("create materialized view log on t_alter_mlog_lock (a, b)")
				tk.MustExec("create materialized view mv_alter_mlog_lock (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_alter_mlog_lock group by a")
				tk.MustExec("create materialized view log on mv_alter_mlog_lock (a)")
				mv, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_alter_mlog_lock"))
				require.NoError(t, err)
				lockName := fmt.Sprintf("mv_refresh_%d_%d", mv.Meta().DBID, mv.Meta().ID)

				if rollback {
					cancelTK := newMViewTestKit(t, store)
					var cancelled atomic.Bool
					testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced", func(job *model.Job) {
						if job.TableName != "$mlog$mv_alter_mlog_lock" || !cancelled.CompareAndSwap(false, true) {
							return
						}
						errs, err := ddl.CancelJobs(context.Background(), cancelTK.Session(), []int64{job.ID})
						require.NoError(t, err)
						require.Len(t, errs, 1)
						require.NoError(t, errs[0])
					})
				}
				paused := make(chan struct{})
				resume := make(chan struct{})
				var signalled atomic.Bool
				testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeWaitSchemaSynced", func(job *model.Job, _ int64) {
					if job.TableName == "$mlog$mv_alter_mlog_lock" &&
						(job.IsDone() || job.IsRollbackDone()) && signalled.CompareAndSwap(false, true) {
						close(paused)
						<-resume
					}
				})
				ddlDone := make(chan error, 1)
				go func() {
					ddlDone <- tk.ExecToErr("alter materialized view log on mv_alter_mlog_lock add column (" + columns + ")")
				}()
				finished := false
				release := func() {
					select {
					case <-resume:
					default:
						close(resume)
					}
				}
				defer func() {
					release()
					if !finished {
						select {
						case <-ddlDone:
						case <-time.After(15 * time.Second):
							t.Error("timeout cleaning up ADD COLUMN")
						}
					}
				}()
				select {
				case <-paused:
				case <-time.After(15 * time.Second):
					t.Fatal("timeout waiting for ADD COLUMN schema sync")
				}
				tkLock := newMViewTestKit(t, store)
				tkLock.MustExec("use test")
				tkLock.MustGetErrCode("refresh materialized view mv_alter_mlog_lock fast", errno.ErrMVAdvisoryLockConflict)
				release()
				select {
				case err = <-ddlDone:
					finished = true
				case <-time.After(15 * time.Second):
					t.Fatal("timeout waiting for ADD COLUMN completion")
				}
				if rollback {
					require.ErrorContains(t, err, "Cancelled DDL job")
				} else {
					require.NoError(t, err)
				}
				require.NoError(t, tkLock.Session().GetAdvisoryLock(lockName, 1), "the worker must release the lock before reuse")
				require.True(t, tkLock.Session().ReleaseAdvisoryLock(lockName))
				tkLock.MustExec("refresh materialized view mv_alter_mlog_lock fast")
			})
		}
	}
}

func TestAlterMaterializedViewLogAddColumnPausedRejectsOutOfPlaceRefresh(t *testing.T) {
	for _, tc := range []struct {
		name           string
		columns        string
		state          model.SchemaState
		stale          bool
		refreshMode    string
		betweenSubJobs bool
	}{
		{"delete_only", "s", model.StateDeleteOnly, false, "fast", false},
		{"write_only", "s", model.StateWriteOnly, false, "complete delta apply", false},
		{"write_reorganization", "s", model.StateWriteReorganization, false, "fast", false},
		{"multiple_columns", "s, cnt", model.StateWriteReorganization, false, "fast", false},
		{"stale_schema", "s, cnt", model.StateWriteReorganization, true, "complete delta apply", false},
		{"between_sub_jobs_fast", "s, cnt", model.StateWriteReorganization, false, "fast", true},
		{"between_sub_jobs_delta_apply", "s, cnt", model.StateWriteReorganization, false, "complete delta apply", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, dom := testkit.CreateMockStoreAndDomain(t)
			tk := newMViewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("create table t_paused_mlog (a int not null, b int not null)")
			tk.MustExec("insert into t_paused_mlog values (1, 10)")
			tk.MustExec("create materialized view log on t_paused_mlog (a, b)")
			tk.MustExec("create materialized view mv_paused_mlog (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_paused_mlog group by a")
			tk.MustExec("create materialized view log on mv_paused_mlog (a)")
			tk.MustExec("insert into t_paused_mlog values (2, 20)")
			oldIS := dom.InfoSchema()
			mv, err := oldIS.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_paused_mlog"))
			require.NoError(t, err)
			lockName := fmt.Sprintf("mv_refresh_%d_%d", mv.Meta().DBID, mv.Meta().ID)
			tkCtl := newMViewTestKit(t, store)
			tkCtl.MustExec("use test")
			testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/infoschema/issyncer/disableOnTickReload", "return(true)")

			var jobID atomic.Int64
			var pauseRequested, pauseSignalled atomic.Bool
			pauseResult := make(chan error, 1)
			paused := make(chan struct{})
			lockConflicts := make(chan *model.Job, 3)
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced", func(job *model.Job) {
				if job.TableName != "$mlog$mv_paused_mlog" {
					return
				}
				if job.ID == jobID.Load() && job.Error != nil && exeerrors.ErrMVAdvisoryLockConflict.Equal(job.Error) {
					select {
					case lockConflicts <- job.Clone():
					default:
					}
				}
				if job.MultiSchemaInfo != nil {
					for i, sub := range job.MultiSchemaInfo.SubJobs {
						state := tc.state
						if tc.betweenSubJobs && i > 0 {
							state = model.StateNone
						}
						if sub.SchemaState != state {
							return
						}
					}
				} else if job.SchemaState != tc.state {
					return
				}
				if !pauseRequested.CompareAndSwap(false, true) {
					return
				}
				jobID.Store(job.ID)
				errs, err := ddl.PauseJobs(context.Background(), tkCtl.Session(), []int64{job.ID})
				if err == nil && len(errs) > 0 {
					err = errs[0]
				}
				pauseResult <- err
			})
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterDeliveryJob", func(job *model.JobW) {
				if job != nil && job.ID == jobID.Load() && job.IsPaused() && pauseSignalled.CompareAndSwap(false, true) {
					close(paused)
				}
			})
			ddlDone := make(chan error, 1)
			go func() {
				ddlDone <- tk.ExecToErr("alter materialized view log on mv_paused_mlog add column (" + tc.columns + ")")
			}()
			resumed, finished := false, false
			resumeDDL := func() {
				if resumed || jobID.Load() == 0 {
					return
				}
				errs, err := ddl.ResumeJobs(context.Background(), tkCtl.Session(), []int64{jobID.Load()})
				require.NoError(t, err)
				require.Len(t, errs, 1)
				require.NoError(t, errs[0])
				resumed = true
			}
			defer func() {
				resumeDDL()
				if !finished {
					select {
					case <-ddlDone:
					case <-time.After(15 * time.Second):
						t.Error("timeout cleaning up paused ADD COLUMN")
					}
				}
			}()
			select {
			case err := <-pauseResult:
				require.NoError(t, err)
			case <-time.After(15 * time.Second):
				t.Fatal("timeout requesting ADD COLUMN pause")
			}
			select {
			case <-paused:
			case <-time.After(15 * time.Second):
				t.Fatal("timeout waiting for ADD COLUMN to release its worker")
			}
			require.Eventually(t, func() bool {
				return tkCtl.Session().IsUsedAdvisoryLock(lockName) == 0
			}, 10*time.Second, 10*time.Millisecond)

			// An OOP refresh must reject intermediate output-log metadata before
			// submitting any shadow/cutover DDL that could block a resumed ADD COLUMN.
			var shadowCreated atomic.Bool
			const shadowFailpoint = "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewOutOfPlaceAfterCreateShadow"
			testfailpoint.EnableCall(t, shadowFailpoint, func() {
				shadowCreated.Store(true)
			})
			// Keep the regression bounded before the fix: without the schema check,
			// OOP would wait on shadow DDL while holding the lock needed by resume.
			const beforeHistFailpoint = "github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewErrorBeforeInsertHist"
			testfailpoint.Enable(t, beforeHistFailpoint, `return("OOP passed the intermediate MLog schema")`)
			if tc.stale {
				dom.InfoCache().Upsert(oldIS, 0)
			}
			err = tkCtl.ExecToErr("refresh materialized view mv_paused_mlog complete out of place")
			require.True(t, exeerrors.ErrMVAdvisoryLockConflict.Equal(err), "%v", err)
			require.ErrorContains(t, err, "materialized view log schema change is in progress")
			require.False(t, shadowCreated.Load())
			testfailpoint.Disable(t, shadowFailpoint)
			testfailpoint.Disable(t, beforeHistFailpoint)
			if tc.stale {
				require.Greater(t, dom.InfoSchema().SchemaMetaVersion(), oldIS.SchemaMetaVersion())
			}
			require.NoError(t, tkCtl.Session().GetAdvisoryLock(lockName, 1), "rejected refresh must release its lock")
			require.True(t, tkCtl.Session().ReleaseAdvisoryLock(lockName))
			tkCtl.MustQuery("show tables like '\\_\\_mv\\_shadow\\_%'").Check(testkit.Rows())
			testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/infoschema/issyncer/disableOnTickReload")
			require.NoError(t, dom.Reload())

			originalErrorCountLimit := vardef.GetDDLErrorCountLimit()
			tkCtl.MustExec("set global tidb_ddl_error_count_limit = 1")
			defer tkCtl.MustExec(fmt.Sprintf("set global tidb_ddl_error_count_limit = %d", originalErrorCountLimit))
			refreshPaused := make(chan struct{})
			allowRefresh := make(chan struct{})
			releaseRefresh := func() {
				select {
				case <-allowRefresh:
				default:
					close(allowRefresh)
				}
			}
			const dataChangesFailpoint = "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewAfterDataChanges"
			testfailpoint.EnableCall(t, dataChangesFailpoint, func() {
				close(refreshPaused)
				<-allowRefresh
			})
			tkRefresh := newMViewTestKit(t, store)
			tkRefresh.MustExec("use test")
			refreshDone := make(chan error, 1)
			go func() {
				refreshDone <- tkRefresh.ExecToErr("refresh materialized view mv_paused_mlog " + tc.refreshMode)
			}()
			refreshFinished := false
			defer func() {
				releaseRefresh()
				if !refreshFinished {
					select {
					case <-refreshDone:
					case <-time.After(15 * time.Second):
						t.Error("timeout cleaning up refresh during ADD COLUMN resume")
					}
				}
			}()
			select {
			case <-refreshPaused:
			case <-time.After(15 * time.Second):
				t.Fatal("timeout waiting for refresh data changes")
			}

			// Resuming an existing schema change must wait for refresh without
			// exhausting the DDL failure budget or rolling back intermediate columns.
			resumeDDL()
			for range 3 {
				select {
				case job := <-lockConflicts:
					require.Equal(t, model.JobStateRunning, job.State)
					require.Zero(t, job.ErrorCount, "refresh lock waits must not count as DDL failures")
				case err := <-ddlDone:
					finished = true
					t.Fatalf("ADD COLUMN finished before refresh released its lock: %v", err)
				case <-time.After(15 * time.Second):
					t.Fatal("timeout waiting for resumed ADD COLUMN lock conflict")
				}
			}
			releaseRefresh()
			select {
			case err := <-refreshDone:
				refreshFinished = true
				require.NoError(t, err)
			case <-time.After(15 * time.Second):
				t.Fatal("timeout waiting for refresh completion")
			}
			testfailpoint.Disable(t, dataChangesFailpoint)
			select {
			case err := <-ddlDone:
				finished = true
				require.NoError(t, err)
			case <-time.After(15 * time.Second):
				t.Fatal("timeout waiting for resumed ADD COLUMN")
			}
			tkCtl.MustExec("refresh materialized view mv_paused_mlog complete out of place")
			tkCtl.MustQuery("select * from mv_paused_mlog order by a").Check(testkit.Rows("1 10 1", "2 20 1"))
		})
	}
}

func TestMaterializedViewLogOnReadyMaterializedView(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_mlog_ddl (a int not null, b int not null)")
	tk.MustExec("insert into t_mv_mlog_ddl values (1, 10)")
	tk.MustExec("create materialized view log on t_mv_mlog_ddl (a, b)")
	tk.MustExec("create materialized view mv_mlog_ddl (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_mv_mlog_ddl group by a")
	tk.MustExec("create materialized view log on mv_mlog_ddl (a)")

	mlog, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$mv_mlog_ddl"))
	require.NoError(t, err)
	mlogID := mlog.Meta().ID
	mv, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_mlog_ddl"))
	require.NoError(t, err)
	require.Equal(t, mlogID, mv.Meta().MaterializedViewBase.MLogID)
	require.Equal(t, mv.Meta().ID, mlog.Meta().MaterializedViewLog.BaseTableID)

	showCreate := tk.MustQuery("show create materialized view log on mv_mlog_ddl").Rows()[0][1].(string)
	require.Contains(t, showCreate, "CREATE MATERIALIZED VIEW LOG ON `mv_mlog_ddl` (`a`)")
	tk.MustExec("alter materialized view log on mv_mlog_ddl add column (s, cnt)")
	showCreate = tk.MustQuery("show create materialized view log on mv_mlog_ddl").Rows()[0][1].(string)
	require.Contains(t, showCreate, "CREATE MATERIALIZED VIEW LOG ON `mv_mlog_ddl` (`a`, `s`, `cnt`)")
	tk.MustExec("alter materialized view log on mv_mlog_ddl purge next date_add(now(), interval 1 hour)")
	tk.MustQuery(fmt.Sprintf("select NEXT_PURGE_UNIX_SECONDS is not null from mysql.tidb_mlog_purge_info where MLOG_ID=%d", mlogID)).Check(testkit.Rows("1"))
	tk.MustExec("purge materialized view log on mv_mlog_ddl")

	for _, stmt := range []string{"drop materialized view mv_mlog_ddl", "drop materialized view if exists mv_mlog_ddl"} {
		require.ErrorContains(t, tk.ExecToErr(stmt), "DROP MATERIALIZED VIEW with materialized view log")
	}
	mvAfterDrop, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_mlog_ddl"))
	require.NoError(t, err)
	require.Equal(t, mv.Meta().ID, mvAfterDrop.Meta().ID)
	require.Equal(t, mlogID, mvAfterDrop.Meta().MaterializedViewBase.MLogID)
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_info where MLOG_ID=%d", mlogID)).Check(testkit.Rows("1"))
	tk.MustQuery("select * from mv_mlog_ddl").Check(testkit.Rows("1 10 1"))

	tk.MustExec("drop materialized view log on mv_mlog_ddl")
	mv, err = dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_mlog_ddl"))
	require.NoError(t, err)
	require.True(t, mv.Meta().MaterializedViewBase == nil || mv.Meta().MaterializedViewBase.MLogID == 0)
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_info where MLOG_ID=%d", mlogID)).Check(testkit.Rows("0"))
	tk.MustQuery("select * from mv_mlog_ddl").Check(testkit.Rows("1 10 1"))
	tk.MustExec("drop materialized view mv_mlog_ddl")
	tk.MustQuery("show tables like 'mv_mlog_ddl'").Check(testkit.Rows())
	tk.MustQuery("show tables like '$mlog$mv_mlog_ddl'").Check(testkit.Rows())
	tk.MustExec("drop materialized view log on t_mv_mlog_ddl")
	tk.MustExec("drop table t_mv_mlog_ddl")
}

func TestDropMaterializedViewRechecksOwnedLog(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_drop_mv_mlog (a int)")
	tk.MustExec("create materialized view log on t_drop_mv_mlog (a)")
	tk.MustExec("create materialized view mv_drop_mlog (a, cnt) as select a, count(1) from t_drop_mv_mlog group by a")
	mv, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_drop_mlog"))
	require.NoError(t, err)
	mvID := mv.Meta().ID

	createStarted := make(chan struct{})
	allowCreate := make(chan struct{})
	releaseCreate := func() {
		select {
		case <-allowCreate:
		default:
			close(allowCreate)
		}
	}
	t.Cleanup(releaseCreate)
	var signalled atomic.Bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if job.Type != model.ActionCreateMaterializedViewLog || job.TableName != "$mlog$mv_drop_mlog" {
			return
		}
		if signalled.CompareAndSwap(false, true) {
			close(createStarted)
		}
		<-allowCreate
	})

	tkCreate := newMViewTestKit(t, store)
	tkCreate.MustExec("use test")
	createErrCh := make(chan error, 1)
	go func() { createErrCh <- tkCreate.ExecToErr("create materialized view log on mv_drop_mlog (a, cnt)") }()
	select {
	case <-createStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for CREATE MATERIALIZED VIEW LOG worker")
	}

	// Enqueue DROP with a stale executor precheck, behind the blocked CREATE.
	tkDrop := newMViewTestKit(t, store)
	tkDrop.MustExec("use test")
	dropErrCh := make(chan error, 1)
	go func() { dropErrCh <- tkDrop.ExecToErr("drop materialized view mv_drop_mlog") }()
	require.Eventually(t, func() bool {
		return len(tk.MustQuery("select job_id from mysql.tidb_ddl_job").Rows()) == 2
	}, 10*time.Second, 10*time.Millisecond)
	releaseCreate()
	select {
	case err := <-createErrCh:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for CREATE MATERIALIZED VIEW LOG")
	}
	select {
	case err := <-dropErrCh:
		require.ErrorContains(t, err, "DROP MATERIALIZED VIEW with materialized view log")
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for DROP MATERIALIZED VIEW")
	}

	mv, err = dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_drop_mlog"))
	require.NoError(t, err)
	require.Equal(t, mvID, mv.Meta().ID)
	require.NotNil(t, mv.Meta().MaterializedViewBase)
	mlogID := mv.Meta().MaterializedViewBase.MLogID
	mlog, ok := dom.InfoSchema().TableByID(context.Background(), mlogID)
	require.True(t, ok)
	require.NotNil(t, mlog.Meta().MaterializedViewLog)
	require.Equal(t, mvID, mlog.Meta().MaterializedViewLog.BaseTableID)
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_info where MLOG_ID=%d", mlogID)).Check(testkit.Rows("1"))
	tk.MustExec("drop materialized view log on mv_drop_mlog")
	tk.MustExec("drop materialized view mv_drop_mlog")
}
