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

package ddl_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/dxf/importinto/taskinfo"
	"github.com/pingcap/tidb/pkg/dxf/importinto/taskkey"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
)

func TestCreateMViewBuildImportJob(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set tidb_mview_enable = on")
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int)")
	tk.MustExec("insert into t values (1, 1), (1, 2), (2, 3)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockMViewBuildImportJob", "return")

	var jobsAtReorg [][]any
	var reorgJob *model.Job
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if job.Type == model.ActionCreateMaterializedView && job.SchemaState == model.StateWriteReorganization {
			reorgJob = &model.Job{Version: job.Version, RawArgs: job.RawArgs}
			if jobsAtReorg == nil {
				checkTK := testkit.NewTestKit(t, store)
				jobsAtReorg = checkTK.MustQuery("select count(*) from mysql.tidb_import_jobs").Rows()
			}
		}
	})
	tk.MustExec("create materialized view mv (a, c) refresh fast next date_add(now(), interval 1 hour) as select a, count(1) from t group by a")
	require.Equal(t, testkit.Rows("0"), jobsAtReorg)
	require.NotNil(t, reorgJob)
	args, err := model.GetCreateMaterializedViewArgs(reorgJob)
	require.NoError(t, err)
	require.Positive(t, args.ImportJobID)
	tk.MustQuery("select group_key, status from mysql.tidb_import_jobs where id = ?", args.ImportJobID).Check(testkit.Rows(" pending"))
	tk.MustQuery("select count(*) from mysql.tidb_import_jobs").Check(testkit.Rows("1"))
}

func TestRollbackMViewBuildCancelsImportJob(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set tidb_mview_enable = on")
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int)")
	tk.MustExec("insert into t values (1, 1)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockMViewBuildImportJob", "return")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewBuildErr", "return")
	err := tk.ExecToErr("create materialized view mv (a, c) refresh fast next date_add(now(), interval 1 hour) as select a, count(1) from t group by a")
	require.ErrorContains(t, err, "mock create materialized view build error")
	tk.MustQuery("select status from mysql.tidb_import_jobs").Check(testkit.Rows("cancelled"))
}

type mviewBuildSQLHook func(context.Context, string, []any, sqlexec.SQLExecutor) (sqlexec.RecordSet, error)

type mviewBuildSession struct {
	sessionctx.Context
	sqlexec.SQLExecutor
	hook mviewBuildSQLHook
}

func (s *mviewBuildSession) Close() { s.Context.(pools.Resource).Close() }

func (s *mviewBuildSession) GetSQLExecutor() sqlexec.SQLExecutor { return s }

func (e *mviewBuildSession) ExecuteInternal(ctx context.Context, sql string, args ...any) (sqlexec.RecordSet, error) {
	if e.hook != nil {
		return e.hook(ctx, sql, args, e.SQLExecutor)
	}
	return e.SQLExecutor.ExecuteInternal(ctx, sql, args...)
}

type mviewTaskTest struct {
	store       kv.Storage
	pool        *pools.ResourcePool
	mgr         *storage.TaskManager
	ctx         context.Context
	job         *model.Job
	importJobID int64
	meta        []byte
}

func newMViewTaskTest(t *testing.T, hook mviewBuildSQLHook) *mviewTaskTest {
	t.Helper()
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/domain/MockDisableDistTask", "return(true)")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(8)")
	store := testkit.CreateMockStore(t)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		se := testkit.NewTestKit(t, store).Session()
		return &mviewBuildSession{Context: se, SQLExecutor: se.GetSQLExecutor(), hook: hook}, nil
	}, 4, 4, time.Second)
	t.Cleanup(pool.Close)
	mgr := storage.NewTaskManager(pool)
	previous, _ := storage.GetTaskManager()
	previousSvc, _ := storage.GetDXFSvcTaskMgr()
	storage.SetTaskManager(mgr)
	storage.SetDXFSvcTaskMgr(mgr)
	t.Cleanup(func() { storage.SetTaskManager(previous); storage.SetDXFSvcTaskMgr(previousSvc) })
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalDistTask)
	require.NoError(t, mgr.InitMeta(ctx, ":4000", ""))
	job := &model.Job{ID: 123, TableID: 456, Version: model.JobVersion2}
	var importJobID int64
	require.NoError(t, mgr.WithNewSession(func(se sessionctx.Context) error {
		var err error
		importJobID, err = importer.CreateJob(ctx, se.GetSQLExecutor(), "test", "mv", job.TableID, "", "", &importer.ImportParameters{}, 0)
		return err
	}))
	job.FillArgs(&model.CreateMaterializedViewArgs{ImportJobID: importJobID})
	meta, err := json.Marshal(taskinfo.TaskMeta{JobID: importJobID, Plan: importer.Plan{QuerySource: &importer.QuerySource{ReadTS: 100}}})
	require.NoError(t, err)
	return &mviewTaskTest{store: store, pool: pool, mgr: mgr, ctx: ctx, job: job, importJobID: importJobID, meta: meta}
}

func TestMViewBuildCancelPausedTask(t *testing.T) {
	f := newMViewTaskTest(t, nil)
	worker := ddl.NewMViewBuildWorkerForTest(f.ctx, f.store, f.pool)
	for _, state := range []proto.TaskState{proto.TaskStatePaused, proto.TaskStatePausing, proto.TaskStateResuming, proto.TaskStateModifying} {
		t.Run(string(state), func(t *testing.T) {
			key := taskkey.ForJob(f.importJobID)
			id, err := f.mgr.CreateTask(f.ctx, key, proto.ImportInto, f.store.GetKeyspace(), 1, "", 1, proto.ExtraParams{}, f.meta)
			require.NoError(t, err)
			t.Cleanup(func() {
				_, err := f.mgr.ExecuteSQLWithNewSession(f.ctx, "delete from mysql.tidb_global_task where id = %?", id)
				require.NoError(t, err)
			})
			_, err = f.mgr.ExecuteSQLWithNewSession(f.ctx, "update mysql.tidb_global_task set state = %? where id = %?", state, id)
			require.NoError(t, err)
			stopCtx, cancel := context.WithTimeout(f.ctx, 30*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- worker.StopMViewBuildForTest(stopCtx, f.job) }()
			// Drive the framework's asynchronous transitions in the real task table.
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case err := <-done:
					require.NoError(t, err)
					task, err := f.mgr.GetTaskByID(f.ctx, id)
					require.NoError(t, err)
					require.Equal(t, proto.TaskStateReverted, task.State)
					return
				case <-ticker.C:
					_, err := f.mgr.ExecuteSQLWithNewSession(f.ctx, `update mysql.tidb_global_task set state = case state
      when 'pausing' then 'paused' when 'resuming' then 'running' when 'modifying' then 'running' when 'cancelling' then 'reverted' else state end where id = %?`, id)
					require.NoError(t, err)
				}
			}
		})
	}
}

func TestMViewBuildReconnectRetry(t *testing.T) {
	for _, failure := range []string{"task lookup", "resume"} {
		t.Run(failure, func(t *testing.T) {
			var armed atomic.Bool
			injectedErr := errors.New("transient MV task SQL failure")
			hook := func(ctx context.Context, sql string, args []any, exec sqlexec.SQLExecutor) (sqlexec.RecordSet, error) {
				lower := strings.ToLower(sql)
				match := failure == "task lookup" && strings.Contains(lower, "from mysql.tidb_global_task") ||
					failure == "resume" && strings.Contains(lower, "update mysql.tidb_global_task")
				if match && armed.CompareAndSwap(true, false) {
					return nil, injectedErr
				}
				return exec.ExecuteInternal(ctx, sql, args...)
			}
			f := newMViewTaskTest(t, hook)
			key := taskkey.ForJob(f.importJobID)
			id, err := f.mgr.CreateTask(f.ctx, key, proto.ImportInto, f.store.GetKeyspace(), 1, "", 1, proto.ExtraParams{}, f.meta)
			require.NoError(t, err)
			state := proto.TaskStateSucceed
			if failure == "resume" {
				state = proto.TaskStatePaused
			}
			_, err = f.mgr.ExecuteSQLWithNewSession(f.ctx, "update mysql.tidb_global_task set state = %? where id = %?", state, id)
			require.NoError(t, err)
			worker := ddl.NewMViewBuildWorkerForTest(f.ctx, f.store, f.pool)
			armed.Store(true)
			_, err = worker.ExecuteMViewBuildForTest(f.ctx, f.job, &model.TableInfo{ID: f.job.TableID})
			require.ErrorIs(t, err, injectedErr)
			task, err := f.mgr.GetTaskByID(f.ctx, id)
			require.NoError(t, err)
			require.Equal(t, state, task.State)
			_, err = f.mgr.ExecuteSQLWithNewSession(f.ctx, "update mysql.tidb_global_task set state = %? where id = %?", proto.TaskStateSucceed, id)
			require.NoError(t, err)
			readTS, err := worker.ExecuteMViewBuildForTest(f.ctx, f.job, &model.TableInfo{ID: f.job.TableID})
			require.NoError(t, err)
			require.EqualValues(t, 100, readTS)
		})
	}
}

func TestMViewBuildLostSubmitResponse(t *testing.T) {
	var committed atomic.Bool
	var failedRead atomic.Bool
	injectedErr := errors.New("lost submit response")
	hook := func(ctx context.Context, sql string, args []any, exec sqlexec.SQLExecutor) (sqlexec.RecordSet, error) {
		lower := strings.ToLower(sql)
		if strings.Contains(lower, "insert into mysql.tidb_global_task") {
			rs, err := exec.ExecuteInternal(ctx, sql, args...)
			if err != nil {
				return rs, err
			}
			committed.Store(true)
			if rs != nil {
				_ = rs.Close()
			}
			return nil, injectedErr
		}
		if committed.Load() && strings.Contains(lower, "from mysql.tidb_global_task") && failedRead.CompareAndSwap(false, true) {
			return nil, errors.New("temporary readback failure")
		}
		return exec.ExecuteInternal(ctx, sql, args...)
	}
	f := newMViewTaskTest(t, hook)
	worker := ddl.NewMViewBuildWorkerForTest(f.ctx, f.store, f.pool)
	key := taskkey.ForJob(f.importJobID)
	_, err := worker.SubmitMViewBuildForTest(f.ctx, f.mgr, key, &proto.Task{TaskBase: proto.TaskBase{RequiredSlots: 1, MaxNodeCount: 1}, Meta: f.meta})
	require.ErrorIs(t, err, injectedErr)
	require.True(t, committed.Load())
	require.True(t, failedRead.Load())
	task, err := f.mgr.GetTaskByKeyWithHistory(f.ctx, key)
	require.NoError(t, err)
	_, err = f.mgr.ExecuteSQLWithNewSession(f.ctx, "update mysql.tidb_global_task set state = %? where id = %?", proto.TaskStateSucceed, task.ID)
	require.NoError(t, err)
	readTS, err := worker.ExecuteMViewBuildForTest(f.ctx, f.job, &model.TableInfo{ID: f.job.TableID})
	require.NoError(t, err)
	require.EqualValues(t, 100, readTS)
	rows, err := f.mgr.ExecuteSQLWithNewSession(f.ctx, "select count(*) from mysql.tidb_global_task where task_key = %?", key)
	require.NoError(t, err)
	require.EqualValues(t, 1, rows[0].GetInt64(0))
}

func TestMViewBuildFailedTask(t *testing.T) {
	f := newMViewTaskTest(t, nil)
	key := taskkey.ForJob(f.importJobID)
	id, err := f.mgr.CreateTask(f.ctx, key, proto.ImportInto, f.store.GetKeyspace(), 1, "", 1, proto.ExtraParams{}, f.meta)
	require.NoError(t, err)
	require.NoError(t, f.mgr.FailTask(f.ctx, id, proto.TaskStatePending, plannererrors.ErrWrongValueCountOnRow.GenWithStackByArgs(1)))
	worker := ddl.NewMViewBuildWorkerForTest(f.ctx, f.store, f.pool)
	_, err = worker.ExecuteMViewBuildForTest(f.ctx, f.job, &model.TableInfo{ID: f.job.TableID})
	require.True(t, plannererrors.ErrWrongValueCountOnRow.Equal(err), "%v", err)
	task, err := f.mgr.GetTaskByKeyWithHistory(f.ctx, key)
	require.NoError(t, err)
	require.Equal(t, id, task.ID)
	require.Equal(t, proto.TaskStateFailed, task.State)
}

func TestMViewBuildDDLControl(t *testing.T) {
	for _, paused := range []bool{false, true} {
		name := "cancel"
		cause := dbterror.ErrCancelledDDLJob.GenWithStackByArgs(123)
		requested, finished := proto.TaskStateCancelling, proto.TaskStateReverted
		if paused {
			name = "pause"
			cause = dbterror.ErrPausedDDLJob.GenWithStackByArgs(123)
			requested, finished = proto.TaskStatePausing, proto.TaskStatePaused
		}
		t.Run(name, func(t *testing.T) {
			f := newMViewTaskTest(t, nil)
			key := taskkey.ForJob(f.importJobID)
			id, err := f.mgr.CreateTask(f.ctx, key, proto.ImportInto, f.store.GetKeyspace(), 1, "", 1, proto.ExtraParams{}, f.meta)
			require.NoError(t, err)
			workerCtx, stopWorker := context.WithTimeout(f.ctx, 15*time.Second)
			worker := ddl.NewMViewBuildWorkerForTest(workerCtx, f.store, f.pool)
			stepCtx, cancelStep := context.WithCancelCause(workerCtx)
			cancelStep(cause)
			done := make(chan error, 1)
			exited := make(chan struct{})
			go func() {
				defer close(exited)
				_, err := worker.ExecuteMViewBuildForTest(stepCtx, f.job, &model.TableInfo{ID: f.job.TableID})
				done <- err
			}()
			t.Cleanup(func() {
				stopWorker()
				select {
				case <-exited:
				case <-time.After(5 * time.Second):
					t.Error("MV task waiter did not stop with worker")
				}
			})
			require.Eventually(t, func() bool {
				task, err := f.mgr.GetTaskByID(f.ctx, id)
				return err == nil && task.State == requested
			}, 5*time.Second, 10*time.Millisecond)
			select {
			case err := <-done:
				t.Fatalf("wait returned before DXF finished the %s request: %v", name, err)
			default:
			}
			_, err = f.mgr.ExecuteSQLWithNewSession(f.ctx, "update mysql.tidb_global_task set state = %? where id = %?", finished, id)
			require.NoError(t, err)
			select {
			case err := <-done:
				require.Error(t, err)
				if paused {
					require.True(t, dbterror.ErrPausedDDLJob.Equal(err), "%v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("MV task waiter did not observe DXF completion")
			}
		})
	}
}
