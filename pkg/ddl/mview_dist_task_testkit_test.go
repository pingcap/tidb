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
	"fmt"
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
	"github.com/pingcap/tidb/pkg/importer"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
)

type mviewBuildSQLHook func(context.Context, string, []any, sqlexec.SQLExecutor) (sqlexec.RecordSet, error)

type mviewBuildSession struct {
	sessionctx.Context
	hook mviewBuildSQLHook
}

func (s *mviewBuildSession) Close() { s.Context.(pools.Resource).Close() }

func (s *mviewBuildSession) GetSQLExecutor() sqlexec.SQLExecutor {
	return &mviewBuildSQLExecutor{SQLExecutor: s.Context.GetSQLExecutor(), hook: s.hook}
}

type mviewBuildSQLExecutor struct {
	sqlexec.SQLExecutor
	hook mviewBuildSQLHook
}

func (e *mviewBuildSQLExecutor) ExecuteInternal(ctx context.Context, sql string, args ...any) (sqlexec.RecordSet, error) {
	if e.hook != nil {
		return e.hook(ctx, sql, args, e.SQLExecutor)
	}
	return e.SQLExecutor.ExecuteInternal(ctx, sql, args...)
}

func newMViewTaskTest(t *testing.T, hook mviewBuildSQLHook) (kv.Storage, *pools.ResourcePool, *storage.TaskManager, context.Context, *model.Job, int64, []byte) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/domain/MockDisableDistTask", "return(true)")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(8)")
	store := testkit.CreateMockStore(t)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return &mviewBuildSession{Context: testkit.NewTestKit(t, store).Session(), hook: hook}, nil
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
	job := &model.Job{ID: 123, TableID: 456}
	var importJobID int64
	require.NoError(t, mgr.WithNewSession(func(se sessionctx.Context) error {
		var err error
		importJobID, err = importer.CreateJob(ctx, se.GetSQLExecutor(), "test", "mv", job.TableID, "", fmt.Sprintf("mview-build-%d", job.ID), &importer.ImportParameters{}, 0)
		return err
	}))
	meta, err := json.Marshal(taskinfo.TaskMeta{JobID: importJobID, Plan: importer.Plan{QuerySource: &importer.QuerySource{ReadTS: 100}}})
	require.NoError(t, err)
	return store, pool, mgr, ctx, job, importJobID, meta
}

func TestMViewBuildCancelPausedTask(t *testing.T) {
	store, pool, mgr, ctx, job, importJobID, meta := newMViewTaskTest(t, nil)
	worker := ddl.NewMViewBuildWorkerForTest(ctx, store, pool)
	for _, state := range []proto.TaskState{proto.TaskStatePaused, proto.TaskStatePausing, proto.TaskStateResuming} {
		t.Run(string(state), func(t *testing.T) {
			key := taskkey.ForJob(importJobID)
			id, err := mgr.CreateTask(ctx, key, proto.ImportInto, store.GetKeyspace(), 1, "", 1, proto.ExtraParams{}, meta)
			require.NoError(t, err)
			t.Cleanup(func() {
				_, err := mgr.ExecuteSQLWithNewSession(ctx, "delete from mysql.tidb_global_task where id = %?", id)
				require.NoError(t, err)
			})
			_, err = mgr.ExecuteSQLWithNewSession(ctx, "update mysql.tidb_global_task set state = %? where id = %?", state, id)
			require.NoError(t, err)
			stopCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- worker.StopMViewBuildForTest(stopCtx, job) }()
			// Drive the framework's asynchronous transitions in the real task table.
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case err := <-done:
					require.NoError(t, err)
					task, err := mgr.GetTaskByID(ctx, id)
					require.NoError(t, err)
					require.Equal(t, proto.TaskStateReverted, task.State)
					return
				case <-ticker.C:
					_, err := mgr.ExecuteSQLWithNewSession(ctx, `update mysql.tidb_global_task set state = case state
      when 'pausing' then 'paused' when 'resuming' then 'running' when 'cancelling' then 'reverted' else state end where id = %?`, id)
					require.NoError(t, err)
				}
			}
		})
	}
}

func TestMViewBuildReconnectRetry(t *testing.T) {
	for _, failure := range []string{"import job lookup", "task lookup", "resume"} {
		t.Run(failure, func(t *testing.T) {
			var armed atomic.Bool
			injectedErr := errors.New("transient MV task SQL failure")
			hook := func(ctx context.Context, sql string, args []any, exec sqlexec.SQLExecutor) (sqlexec.RecordSet, error) {
				lower := strings.ToLower(sql)
				match := failure == "import job lookup" && strings.Contains(lower, "from mysql.tidb_import_jobs") ||
					failure == "task lookup" && strings.Contains(lower, "from mysql.tidb_global_task") ||
					failure == "resume" && strings.Contains(lower, "update mysql.tidb_global_task")
				if match && armed.CompareAndSwap(true, false) {
					return nil, injectedErr
				}
				return exec.ExecuteInternal(ctx, sql, args...)
			}
			store, pool, mgr, ctx, job, importJobID, meta := newMViewTaskTest(t, hook)
			key := taskkey.ForJob(importJobID)
			id, err := mgr.CreateTask(ctx, key, proto.ImportInto, store.GetKeyspace(), 1, "", 1, proto.ExtraParams{}, meta)
			require.NoError(t, err)
			state := proto.TaskStateSucceed
			if failure == "resume" {
				state = proto.TaskStatePaused
			}
			_, err = mgr.ExecuteSQLWithNewSession(ctx, "update mysql.tidb_global_task set state = %? where id = %?", state, id)
			require.NoError(t, err)
			worker := ddl.NewMViewBuildWorkerForTest(ctx, store, pool)
			armed.Store(true)
			_, err = worker.ExecuteMViewBuildForTest(ctx, job, &model.TableInfo{ID: job.TableID})
			require.ErrorIs(t, err, injectedErr)
			require.True(t, ddl.IsRetryableMViewBuildErrorForTest(err), "infrastructure errors must not roll back the DDL job")
			task, err := mgr.GetTaskByID(ctx, id)
			require.NoError(t, err)
			require.Equal(t, state, task.State)
			_, err = mgr.ExecuteSQLWithNewSession(ctx, "update mysql.tidb_global_task set state = %? where id = %?", proto.TaskStateSucceed, id)
			require.NoError(t, err)
			readTS, err := worker.ExecuteMViewBuildForTest(ctx, job, &model.TableInfo{ID: job.TableID})
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
	store, pool, mgr, ctx, job, importJobID, meta := newMViewTaskTest(t, hook)
	worker := ddl.NewMViewBuildWorkerForTest(ctx, store, pool)
	key := taskkey.ForJob(importJobID)
	_, err := worker.SubmitMViewBuildForTest(ctx, mgr, key, &proto.Task{TaskBase: proto.TaskBase{RequiredSlots: 1, MaxNodeCount: 1}, Meta: meta})
	require.ErrorIs(t, err, injectedErr)
	require.True(t, committed.Load())
	require.True(t, failedRead.Load())
	require.True(t, ddl.IsRetryableMViewBuildErrorForTest(err), "ambiguous commit must remain retryable")
	task, err := mgr.GetTaskByKeyWithHistory(ctx, key)
	require.NoError(t, err)
	_, err = mgr.ExecuteSQLWithNewSession(ctx, "update mysql.tidb_global_task set state = %? where id = %?", proto.TaskStateSucceed, task.ID)
	require.NoError(t, err)
	readTS, err := worker.ExecuteMViewBuildForTest(ctx, job, &model.TableInfo{ID: job.TableID})
	require.NoError(t, err)
	require.EqualValues(t, 100, readTS)
	rows, err := mgr.ExecuteSQLWithNewSession(ctx, "select count(*) from mysql.tidb_global_task where task_key = %?", key)
	require.NoError(t, err)
	require.EqualValues(t, 1, rows[0].GetInt64(0))
}
