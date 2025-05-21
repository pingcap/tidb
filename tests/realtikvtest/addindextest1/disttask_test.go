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

package addindextest

import (
	"context"
	goerrors "errors"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func init() {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Path = "127.0.0.1:2379"
	})
}

func checkTmpDDLDir(t *testing.T) {
	tmpDir := config.GetGlobalConfig().TempDir
	fmt.Println(tmpDir)
	require.NoError(t, fs.WalkDir(os.DirFS(tmpDir), ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !strings.Contains(path, "/tmp_ddl-") {
			return nil
		}
		// we only checks whether there is a stale file, empty dir is allowed.
		if !d.IsDir() {
			return goerrors.New("stale file found")
		}
		return nil
	}))
}

func TestAddIndexDistBasic(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("might ingest overlapped sst, skip")
	}
	// mock that we only have 1 cpu, add-index task can be scheduled as usual
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", `return(1)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/storage/testSetLastTaskID", `return(true)`))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/storage/testSetLastTaskID"))
	})
	store := realtikvtest.CreateMockStoreAndSetup(t)
	if store.Name() != "TiKV" {
		t.Skip("TiKV store only")
	}

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists test;")
	tk.MustExec("create database test;")
	tk.MustExec("use test;")
	tk.MustExec(`set global tidb_enable_dist_task=1;`)

	bak := vardef.GetDDLReorgWorkerCounter()
	tk.MustExec("set global tidb_ddl_reorg_worker_cnt = 111")
	tk.MustExec("set @@tidb_ddl_reorg_worker_cnt = 111")
	require.Equal(t, int32(111), vardef.GetDDLReorgWorkerCounter())
	tk.MustExec("create table t(a bigint auto_random primary key) partition by hash(a) partitions 20;")
	tk.MustExec("insert into t values (), (), (), (), (), ()")
	tk.MustExec("insert into t values (), (), (), (), (), ()")
	tk.MustExec("insert into t values (), (), (), (), (), ()")
	tk.MustExec("insert into t values (), (), (), (), (), ()")
	tk.MustExec("insert into t values (), (), (), (), (), ()")
	tk.MustExec("split table t between (3) and (8646911284551352360) regions 50;")
	tk.MustExec("alter table t add index idx(a);")
	tk.MustExec("admin check index t idx;")
	taskMgr, err := storage.GetTaskManager()
	require.NoError(t, err)
	ctx := util.WithInternalSourceType(context.Background(), "scheduler")
	task, err := taskMgr.GetTaskByIDWithHistory(ctx, storage.TestLastTaskID.Load())
	require.NoError(t, err)
	require.Equal(t, 1, task.Concurrency)

	tk.MustExec(fmt.Sprintf("set global tidb_ddl_reorg_worker_cnt = %d", bak))
	tk.MustExec(fmt.Sprintf("set @@tidb_ddl_reorg_worker_cnt = %d", bak))
	require.Equal(t, bak, vardef.GetDDLReorgWorkerCounter())

	tk.MustExec("create table t1(a bigint auto_random primary key);")
	tk.MustExec("insert into t1 values (), (), (), (), (), ()")
	tk.MustExec("insert into t1 values (), (), (), (), (), ()")
	tk.MustExec("insert into t1 values (), (), (), (), (), ()")
	tk.MustExec("insert into t1 values (), (), (), (), (), ()")
	tk.MustExec("split table t1 between (3) and (8646911284551352360) regions 50;")
	tk.MustExec("alter table t1 add index idx(a);")
	tk.MustExec("admin check index t1 idx;")

	var counter atomic.Int32
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/afterRunSubtask",
		func(e taskexecutor.TaskExecutor, errP *error, _ context.Context) {
			if counter.Add(1) == 1 {
				*errP = context.Canceled
			}
		},
	)
	tk.MustExec("alter table t1 add index idx1(a);")
	tk.MustExec("admin check index t1 idx1;")
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/afterRunSubtask")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/injectPanicForTableScan", "return()"))
	tk.MustExecToErr("alter table t1 add index idx2(a);")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/injectPanicForTableScan"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/injectPanicForIndexIngest", "return()"))
	tk.MustExecToErr("alter table t1 add index idx2(a);")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/injectPanicForIndexIngest"))

	tk.MustExec(`set global tidb_enable_dist_task=0;`)
	checkTmpDDLDir(t)
}

func TestAddIndexDistCancelWithPartition(t *testing.T) {
	testutil.ReduceCheckInterval(t)
	store := realtikvtest.CreateMockStoreAndSetup(t)
	if store.Name() != "TiKV" {
		t.Skip("TiKV store only")
	}

	tk := testkit.NewTestKit(t, store)
	tk1 := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists test;")
	tk.MustExec("create database test;")
	tk.MustExec("use test;")
	tk.MustExec(`set global tidb_enable_dist_task=1;`)

	tk.MustExec("create table t(a bigint auto_random primary key) partition by hash(a) partitions 8;")
	tk.MustExec("insert into t values (), (), (), (), (), ()")
	tk.MustExec("insert into t values (), (), (), (), (), ()")
	tk.MustExec("insert into t values (), (), (), (), (), ()")
	tk.MustExec("insert into t values (), (), (), (), (), ()")
	tk.MustExec("split table t between (3) and (8646911284551352360) regions 50;")

	var once sync.Once
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/mockDMLExecutionAddIndexSubTaskFinish", func(*local.Backend) {
		once.Do(func() {
			row := tk1.MustQuery("select job_id from mysql.tidb_ddl_job").Rows()
			require.Equal(t, 1, len(row))
			jobID := row[0][0].(string)
			tk1.MustExec("admin cancel ddl jobs " + jobID)
		})
	})

	require.Error(t, tk.ExecToErr("alter table t add index idx(a);"))
	tk.MustExec("admin check table t;")
	tk.MustExec("alter table t add index idx2(a);")
	tk.MustExec("admin check table t;")

	tk.MustExec(`set global tidb_enable_dist_task=0;`)
}

func TestAddIndexDistCancel(t *testing.T) {
	testutil.ReduceCheckInterval(t)
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
	tk.MustExec("create table t (a int, b int);")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3);")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use addindexlit;")
	tk2.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
	tk.MustExec("set @@global.tidb_enable_dist_task = 1;")
	tk2.MustExec("create table t2 (a int, b int);")
	tk2.MustExec("insert into t2 values (1, 1), (2, 2), (3, 3);")

	var counter atomic.Int32
	var enableTrigger atomic.Bool
	var targetJobID atomic.Int64
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterBackfillStateRunningDone", func(job *model.Job) {
		// fail for one index when finish backfill, and check row count right
		if counter.Add(1) == 1 {
			targetJobID.Store(job.ID)
			enableTrigger.Store(true)
		}
	})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterUpdateJobToTable", func(job *model.Job, errP *error) {
		if enableTrigger.Load() && job.ID == targetJobID.Load() {
			*errP = errors.New("mock error")
			enableTrigger.Store(false)
		}
	})
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		tk.MustExec("alter table t add index idx(a);")
		wg.Done()
	}()
	go func() {
		tk2.MustExec("alter table t2 add index idx_b(b);")
		wg.Done()
	}()
	wg.Wait()
	rows := tk.MustQuery("admin show ddl jobs 2;").Rows()
	require.Len(t, rows, 2)
	require.True(t, strings.Contains(rows[0][12].(string) /* comments */, "ingest"))
	require.True(t, strings.Contains(rows[1][12].(string) /* comments */, "ingest"))
	require.Equal(t, "3", rows[0][7].(string) /* row_count */)
	require.Equal(t, "3", rows[1][7].(string) /* row_count */)
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/afterBackfillStateRunningDone")
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/afterUpdateJobToTable")

	// test cancel is timely
	enter := make(chan struct{})
	testfailpoint.EnableCall(
		t,
		"github.com/pingcap/tidb/pkg/lightning/backend/local/beforeExecuteRegionJob",
		func(ctx context.Context) {
			close(enter)
			select {
			case <-time.After(time.Second * 30):
			case <-ctx.Done():
			}
		})
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := tk2.ExecToErr("alter table t add index idx_ba(b, a);")
		require.ErrorContains(t, err, "Cancelled DDL job")
	}()
	<-enter
	jobID := tk.MustQuery("admin show ddl jobs 1;").Rows()[0][0].(string)
	now := time.Now()
	tk.MustExec("admin cancel ddl jobs " + jobID)
	wg.Wait()
	// cancel should be timely
	require.Less(t, time.Since(now).Seconds(), 20.0)
}

func TestAddIndexDistPauseAndResume(t *testing.T) {
	t.Skip("unstable") // TODO(tangenta): fix this unstable test
	store := realtikvtest.CreateMockStoreAndSetup(t)
	if store.Name() != "TiKV" {
		t.Skip("TiKV store only")
	}

	tk := testkit.NewTestKit(t, store)
	tk1 := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists test;")
	tk.MustExec("create database test;")
	tk.MustExec("use test;")

	tk.MustExec("create table t(a bigint auto_random primary key) partition by hash(a) partitions 8;")
	tk.MustExec("insert into t values (), (), (), (), (), ()")
	tk.MustExec("insert into t values (), (), (), (), (), ()")
	tk.MustExec("insert into t values (), (), (), (), (), ()")
	tk.MustExec("insert into t values (), (), (), (), (), ()")
	tk.MustExec("split table t between (3) and (8646911284551352360) regions 50;")

	var syncChan = make(chan struct{})
	var counter atomic.Int32
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/mockDMLExecutionAddIndexSubTaskFinish", func(*local.Backend) {
		if counter.Add(1) <= 3 {
			row := tk1.MustQuery("select job_id from mysql.tidb_ddl_job").Rows()
			require.Equal(t, 1, len(row))
			jobID := row[0][0].(string)
			tk1.MustExec("admin pause ddl jobs " + jobID)
			<-syncChan
		}
	})

	require.NoError(t, failpoint.EnableCall("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockDMLExecutionOnPausedState", func() {
		row := tk1.MustQuery("select job_id from mysql.tidb_ddl_job").Rows()
		require.Equal(t, 1, len(row))
		jobID := row[0][0].(string)
		tk1.MustExec("admin resume ddl jobs " + jobID)
	}))

	require.NoError(t, failpoint.EnableCall("github.com/pingcap/tidb/pkg/ddl/syncDDLTaskPause", func() {
		// make sure the task is paused.
		syncChan <- struct{}{}
	}))
	tk.MustExec(`set global tidb_enable_dist_task=1;`)
	tk.MustExec("alter table t add index idx1(a);")
	tk.MustExec("admin check table t;")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockDMLExecutionAddIndexSubTaskFinish"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockDMLExecutionOnPausedState"))

	// dist task succeed, job paused and resumed.
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced", func(job *model.Job) {
		if job.IsPaused() {
			row := tk1.MustQuery("select job_id from mysql.tidb_ddl_job").Rows()
			require.Equal(t, 1, len(row))
			jobID := row[0][0].(string)
			tk1.MustExec("admin resume ddl jobs " + jobID)
		}
	})
	var once sync.Once
	require.NoError(t, failpoint.EnableCall("github.com/pingcap/tidb/pkg/ddl/pauseAfterDistTaskFinished",
		func() {
			once.Do(func() {
				row := tk1.MustQuery("select job_id from mysql.tidb_ddl_job").Rows()
				require.Equal(t, 1, len(row))
				jobID := row[0][0].(string)
				tk1.MustExec("admin pause ddl jobs " + jobID)
			})
		},
	))
	tk.MustExec("alter table t add index idx3(a);")
	tk.MustExec("admin check table t;")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/pauseAfterDistTaskFinished"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/syncDDLTaskPause"))

	tk.MustExec(`set global tidb_enable_dist_task=0;`)
}

func TestAddIndexInvalidDistTaskVariableSetting(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	t.Cleanup(func() {
		tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
		tk.MustExec("set global tidb_enable_dist_task = off;")
	})
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=off;`)
	tk.MustExec("set global tidb_enable_dist_task = on;")
	tk.MustExec("create table t (a int);")
	tk.MustGetErrCode("alter table t add index idx(a);", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t add column b int, add index idx(a);", errno.ErrUnsupportedDDLOperation)
	tk.MustExec("alter table t add column b int, add column c int;")
}

func TestAddIndexForCurrentTimestampColumn(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	t.Cleanup(func() {
		tk.MustExec("set global tidb_enable_dist_task = off;")
	})
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
	tk.MustExec("set global tidb_enable_dist_task = on;")

	tk.MustExec("create table t (a timestamp default current_timestamp);")
	tk.MustExec("insert into t values ();")
	tk.MustExec("alter table t add index idx(a);")
	tk.MustExec("admin check table t;")
}

func TestAddUKErrorMessage(t *testing.T) {
	ingest.ForceSyncFlagForTest.Store(true)
	t.Cleanup(func() {
		ingest.ForceSyncFlagForTest.Store(false)
	})

	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	t.Cleanup(func() {
		tk.MustExec("set global tidb_enable_dist_task = off;")
	})
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
	tk.MustExec("set global tidb_enable_dist_task = on;")

	tk.MustExec("create table t (a int primary key, b int);")
	tk.MustExec("insert into t values (5, 1), (10005, 1), (20005, 1), (30005, 1);")
	tk.MustExec("split table t between (1) and (100001) regions 10;")
	err := tk.ExecToErr("alter table t add unique index uk(b);")
	require.ErrorContains(t, err, "Duplicate entry '1' for key 't.uk'")
}

func TestAddIndexDistLockAcquireFailed(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_dist_task = on;")
	t.Cleanup(func() {
		tk.MustExec("set global tidb_enable_dist_task = off;")
	})
	tk.MustExec("create table t (a int, b int);")
	tk.MustExec("insert into t values (1, 1);")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/owner/mockAcquireDistLockFailed", "1*return(true)")
	tk.MustExec("alter table t add index idx(b);")
}

func TestAddIndexScheduleAway(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_dist_task = on;")
	t.Cleanup(func() {
		tk.MustExec("set global tidb_enable_dist_task = off;")
	})
	tk.MustExec("create table t (a int, b int);")
	tk.MustExec("insert into t values (1, 1);")

	var jobID atomic.Int64
	// Acquire the job ID.
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if job.Type == model.ActionAddIndex {
			jobID.Store(job.ID)
		}
	})
	// Do not balance subtasks automatically.
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockNoEnoughSlots", "return")
	afterCancel := make(chan struct{})
	// Capture the cancel operation from checkBalanceLoop.
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/afterCancelSubtaskExec", func() {
		close(afterCancel)
	})
	var once sync.Once
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/mockDMLExecutionAddIndexSubTaskFinish", func(*local.Backend) {
		once.Do(func() {
			tk1 := testkit.NewTestKit(t, store)
			tk1.MustExec("use test")
			updateExecID := fmt.Sprintf(`
				update mysql.tidb_background_subtask set exec_id = 'other' where task_key in
					(select id from mysql.tidb_global_task where task_key like '%%%d')`, jobID.Load())
			tk1.MustExec(updateExecID)
			<-afterCancel
			updateExecID = fmt.Sprintf(`
				update mysql.tidb_background_subtask set exec_id = ':4000' where task_key in
					(select id from mysql.tidb_global_task where task_key like '%%%d')`, jobID.Load())
			tk1.MustExec(updateExecID)
		})
	})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/afterRunSubtask",
		func(_ taskexecutor.TaskExecutor, _ *error, ctx context.Context) {
			require.Error(t, ctx.Err())
			require.Equal(t, context.Canceled, context.Cause(ctx))
		},
	)
	tk.MustExec("alter table t add index idx(b);")
	require.NotEqual(t, int64(0), jobID.Load())
}

func TestAddIndexDistCleanUpBlock(t *testing.T) {
	proto.MaxConcurrentTask = 1
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", `return(1)`)
	ch := make(chan struct{})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/disttask/framework/scheduler/doCleanupTask", func() {
		<-ch
	})
	store := realtikvtest.CreateMockStoreAndSetup(t)
	if store.Name() != "TiKV" {
		t.Skip("TiKV store only")
	}

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists test;")
	tk.MustExec("create database test;")
	tk.MustExec("use test;")
	tk.MustExec(`set global tidb_enable_dist_task=1;`)
	var wg sync.WaitGroup
	for i := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tk := testkit.NewTestKit(t, store)
			tk.MustExec(fmt.Sprintf("create table test.t%d (a int, b int);", i))
			tk.MustExec(fmt.Sprintf("insert into test.t%d values (1, 1);", i))
			tk.MustExec(fmt.Sprintf("alter table test.t%d add index idx(b);", i))
		}()
	}
	wg.Wait()
	close(ch)
}
