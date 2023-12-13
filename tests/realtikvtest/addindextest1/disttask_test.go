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
	"fmt"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/util/callback"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func init() {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Path = "127.0.0.1:2379"
	})
}

func TestAddIndexDistBasic(t *testing.T) {
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

	bak := variable.GetDDLReorgWorkerCounter()
	tk.MustExec("set global tidb_ddl_reorg_worker_cnt = 111")
	require.Equal(t, int32(111), variable.GetDDLReorgWorkerCounter())
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
	ctx := util.WithInternalSourceType(context.Background(), "dispatcher")
	task, err := taskMgr.GetTaskByIDWithHistory(ctx, storage.TestLastTaskID.Load())
	require.NoError(t, err)
	require.Equal(t, 1, task.Concurrency)

	tk.MustExec(fmt.Sprintf("set global tidb_ddl_reorg_worker_cnt = %d", bak))
	require.Equal(t, bak, variable.GetDDLReorgWorkerCounter())

	tk.MustExec("create table t1(a bigint auto_random primary key);")
	tk.MustExec("insert into t1 values (), (), (), (), (), ()")
	tk.MustExec("insert into t1 values (), (), (), (), (), ()")
	tk.MustExec("insert into t1 values (), (), (), (), (), ()")
	tk.MustExec("insert into t1 values (), (), (), (), (), ()")
	tk.MustExec("split table t1 between (3) and (8646911284551352360) regions 50;")
	tk.MustExec("alter table t1 add index idx(a);")
	tk.MustExec("admin check index t1 idx;")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/MockRunSubtaskContextCanceled", "1*return(true)"))
	tk.MustExec("alter table t1 add index idx1(a);")
	tk.MustExec("admin check index t1 idx1;")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/MockRunSubtaskContextCanceled"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/injectPanicForTableScan", "return()"))
	tk.MustExecToErr("alter table t1 add index idx2(a);")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/injectPanicForTableScan"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/injectPanicForIndexIngest", "return()"))
	tk.MustExecToErr("alter table t1 add index idx2(a);")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/injectPanicForIndexIngest"))

	tk.MustExec(`set global tidb_enable_dist_task=0;`)
}

func TestAddIndexDistCancel(t *testing.T) {
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

	ddl.MockDMLExecutionAddIndexSubTaskFinish = func() {
		row := tk1.MustQuery("select job_id from mysql.tidb_ddl_job").Rows()
		require.Equal(t, 1, len(row))
		jobID := row[0][0].(string)
		tk1.MustExec("admin cancel ddl jobs " + jobID)
	}

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockDMLExecutionAddIndexSubTaskFinish", "1*return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockDMLExecutionAddIndexSubTaskFinish"))
	}()

	require.Error(t, tk.ExecToErr("alter table t add index idx(a);"))
	tk.MustExec("admin check table t;")
	tk.MustExec("alter table t add index idx2(a);")
	tk.MustExec("admin check table t;")

	tk.MustExec(`set global tidb_enable_dist_task=0;`)
}

func TestAddIndexDistPauseAndResume(t *testing.T) {
	store, dom := realtikvtest.CreateMockStoreAndDomainAndSetup(t)
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

	ddl.MockDMLExecutionAddIndexSubTaskFinish = func() {
		row := tk1.MustQuery("select job_id from mysql.tidb_ddl_job").Rows()
		require.Equal(t, 1, len(row))
		jobID := row[0][0].(string)
		tk1.MustExec("admin pause ddl jobs " + jobID)
		<-ddl.TestSyncChan
	}

	scheduler.MockDMLExecutionOnPausedState = func(task *proto.Task) {
		row := tk1.MustQuery("select job_id from mysql.tidb_ddl_job").Rows()
		require.Equal(t, 1, len(row))
		jobID := row[0][0].(string)
		tk1.MustExec("admin resume ddl jobs " + jobID)
	}

	ddl.MockDMLExecutionOnTaskFinished = func() {
		row := tk1.MustQuery("select job_id from mysql.tidb_ddl_job").Rows()
		require.Equal(t, 1, len(row))
		jobID := row[0][0].(string)
		tk1.MustExec("admin pause ddl jobs " + jobID)
	}

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockDMLExecutionAddIndexSubTaskFinish", "3*return(true)"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockDMLExecutionOnPausedState", "return(true)"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/syncDDLTaskPause", "return()"))
	tk.MustExec(`set global tidb_enable_dist_task=1;`)
	tk.MustExec("alter table t add index idx1(a);")
	tk.MustExec("admin check table t;")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockDMLExecutionAddIndexSubTaskFinish"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockDMLExecutionOnPausedState"))

	// dist task succeed, job paused and resumed.
	var hook = &callback.TestDDLCallback{Do: dom}
	var resumeFunc = func(job *model.Job) {
		if job.IsPaused() {
			row := tk1.MustQuery("select job_id from mysql.tidb_ddl_job").Rows()
			require.Equal(t, 1, len(row))
			jobID := row[0][0].(string)
			tk1.MustExec("admin resume ddl jobs " + jobID)
		}
	}
	hook.OnJobUpdatedExported.Store(&resumeFunc)
	dom.DDL().SetHook(hook.Clone())
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/pauseAfterDistTaskFinished", "1*return(true)"))
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

func TestAddIndexTSErrorWhenResetImportEngine(t *testing.T) {
	store, dom := realtikvtest.CreateMockStoreAndDomainAndSetup(t)
	var tblInfo *model.TableInfo
	var idxInfo *model.IndexInfo
	cb := &callback.TestDDLCallback{}
	interceptFn := func(job *model.Job) {
		if idxInfo == nil {
			tbl, _ := dom.InfoSchema().TableByID(job.TableID)
			tblInfo = tbl.Meta()
			if len(tblInfo.Indices) == 0 {
				return
			}
			idxInfo = tblInfo.Indices[0]
		}
	}
	cb.OnJobUpdatedExported.Store(&interceptFn)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	t.Cleanup(func() {
		tk.MustExec("set global tidb_enable_dist_task = off;")
	})
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=on;`)
	tk.MustExec("set global tidb_enable_dist_task = on;")

	err := failpoint.Enable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/mockAllocateTSErr", `1*return`)
	require.NoError(t, err)
	tk.MustExec("create table t (a int);")
	tk.MustExec("insert into t values (1), (2), (3);")
	dom.DDL().SetHook(cb)
	tk.MustExec("alter table t add index idx(a);")
	err = failpoint.Disable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/mockAllocateTSErr")
	require.NoError(t, err)

	dts := []types.Datum{types.NewIntDatum(1)}
	sctx := tk.Session().GetSessionVars().StmtCtx
	idxKey, _, err := tablecodec.GenIndexKey(sctx.TimeZone(), tblInfo, idxInfo, tblInfo.ID, dts, kv.IntHandle(1), nil)
	require.NoError(t, err)

	tikvStore := dom.Store().(helper.Storage)
	newHelper := helper.NewHelper(tikvStore)
	mvccResp, err := newHelper.GetMvccByEncodedKeyWithTS(idxKey, 0)
	require.NoError(t, err)
	require.NotNil(t, mvccResp)
	require.NotNil(t, mvccResp.Info)
	require.Greater(t, len(mvccResp.Info.Writes), 0)
	require.Greater(t, mvccResp.Info.Writes[0].CommitTs, uint64(0))
}
