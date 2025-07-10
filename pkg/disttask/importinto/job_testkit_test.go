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

package importinto_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/pingcap/tidb/pkg/disttask/importinto"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	kvstore "github.com/pingcap/tidb/pkg/store"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/atomic"
)

func switchTaskStep(
	ctx context.Context, t *testing.T,
	manager *storage.TaskManager, taskID int64, step proto.Step,
) {
	task, err := manager.GetTaskByID(ctx, taskID)
	require.NoError(t, err)
	require.NoError(t, manager.SwitchTaskStep(ctx, task, proto.TaskStateRunning, step, nil))
}

func TestSubmitTaskNextgen(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("This test is only for nextgen")
	}
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/domain/MockDisableDistTask", "return(true)")
	require.NoError(t, kvstore.Register(config.StoreTypeUniStore, mockstore.EmbedUnistoreDriver{}))
	sysKSStore, _ := testkit.CreateMockStoreAndDomainForKS(t, keyspace.System)
	sysKSTK := testkit.NewTestKit(t, sysKSStore)
	// in uni-store, Store instances are completely isolated, even they have the
	// same keyspace name, so we store them here and mock the GetStore
	// TODO use a shared storage for all Store instances.
	storeMap := make(map[string]kv.Storage, 4)
	storeMap[keyspace.System] = sysKSStore
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/beforeGetStore",
		func(fnP *func(string) (store kv.Storage, err error)) {
			*fnP = func(ks string) (store kv.Storage, err error) {
				return storeMap[ks], nil
			}
		},
	)
	userKSStore, _ := testkit.CreateMockStoreAndDomainForKS(t, "ks")
	storeMap["ks"] = userKSStore
	userKSTK := testkit.NewTestKit(t, userKSStore)

	ctx := util.WithInternalSourceType(context.Background(), kv.InternalDistTask)

	manuallyInitFn := func(t *testing.T, currKSStore, sysKSStore kv.Storage) {
		t.Helper()
		// as we have disabled the dist task in domain, we need init the task manager
		// and framework meta manually.
		getPoolFn := func(store kv.Storage) tidbutil.SessionPool {
			pool := pools.NewResourcePool(func() (pools.Resource, error) {
				return testkit.NewTestKit(t, store).Session(), nil
			}, 1, 1, time.Second)
			t.Cleanup(func() {
				pool.Close()
			})
			return pool
		}
		taskMgr := storage.NewTaskManager(getPoolFn(currKSStore))
		storage.SetTaskManager(taskMgr)
		sysKSTaskMgr := taskMgr
		if keyspace.IsRunningOnUser() {
			sysKSTaskMgr = storage.NewTaskManager(getPoolFn(sysKSStore))
			storage.SetDXFSvcTaskMgr(sysKSTaskMgr)
		}
		require.NoError(t, sysKSTaskMgr.InitMeta(ctx, "tidb", "dxf_service"))
	}

	t.Run("submit task in system keyspace", func(t *testing.T) {
		config.UpdateGlobal(func(conf *config.Config) {
			conf.KeyspaceName = keyspace.System
		})
		manuallyInitFn(t, sysKSStore, sysKSStore)
		jobID, task, err := importinto.SubmitTask(ctx, &importer.Plan{
			TableInfo:  &model.TableInfo{},
			Parameters: &importer.ImportParameters{},
		}, "import into t from '/path/to/file'")
		require.NoError(t, err)
		// both inside system keyspace
		sysKSTK.MustQuery("select count(1) from mysql.tidb_import_jobs where id = ?", jobID).Check(testkit.Rows("1"))
		sysKSTK.MustQuery("select count(1) from mysql.tidb_global_task where id = ?", task.ID).Check(testkit.Rows("1"))
		// user keyspace should not have the job
		userKSTK.MustQuery("select count(1) from mysql.tidb_import_jobs where id = ?", jobID).Check(testkit.Rows("0"))
		userKSTK.MustQuery("select count(1) from mysql.tidb_global_task where id = ?", task.ID).Check(testkit.Rows("0"))
	})

	t.Run("submit task in user keyspace", func(t *testing.T) {
		sysKSTK.MustExec("delete from mysql.tidb_import_jobs")
		userKSTK.MustExec("delete from mysql.tidb_global_task")
		config.UpdateGlobal(func(conf *config.Config) {
			conf.KeyspaceName = "ks"
		})
		manuallyInitFn(t, userKSStore, sysKSStore)
		jobID, task, err := importinto.SubmitTask(ctx, &importer.Plan{
			TableInfo:  &model.TableInfo{},
			Parameters: &importer.ImportParameters{},
		}, "import into t from '/path/to/file'")
		require.NoError(t, err)
		// job created in user keyspace, task created in system keyspace
		userKSTK.MustQuery("select count(1) from mysql.tidb_import_jobs where id = ?", jobID).Check(testkit.Rows("1"))
		sysKSTK.MustQuery("select count(1) from mysql.tidb_global_task where id = ?", task.ID).Check(testkit.Rows("1"))
		// the reverse is not true.
		sysKSTK.MustQuery("select count(1) from mysql.tidb_import_jobs where id = ?", jobID).Check(testkit.Rows("0"))
		userKSTK.MustQuery("select count(1) from mysql.tidb_global_task where id = ?", task.ID).Check(testkit.Rows("0"))
	})
}

func TestGetTaskImportedRows(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return tk.Session(), nil
	}, 1, 1, time.Second)
	defer pool.Close()
	ctx := context.WithValue(context.Background(), "etcd", true)
	ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)

	mgr := storage.NewTaskManager(pool)
	storage.SetTaskManager(mgr)
	manager, err := storage.GetTaskManager()
	require.NoError(t, err)

	// local sort
	taskMeta := importinto.TaskMeta{
		Plan: importer.Plan{},
	}
	bytes, err := json.Marshal(taskMeta)
	require.NoError(t, err)
	taskID, err := manager.CreateTask(ctx, importinto.TaskKey(111), proto.ImportInto, 1, "", 0, proto.ExtraParams{}, bytes)
	require.NoError(t, err)
	importStepSummaries := []*execute.SubtaskSummary{
		{
			RowCnt: *atomic.NewInt64(1),
		},
		{
			RowCnt: *atomic.NewInt64(2),
		},
	}
	for _, m := range importStepSummaries {
		testutil.CreateSubTaskWithSummary(t, manager, taskID, proto.ImportStepImport,
			"", nil, m, proto.SubtaskStatePending, proto.ImportInto, 11)
	}

	switchTaskStep(ctx, t, manager, taskID, proto.ImportStepImport)

	runInfo, err := importinto.GetRuntimeInfoForJob(ctx, 111)
	require.NoError(t, err)
	require.EqualValues(t, 3, runInfo.ImportRows)

	// global sort
	taskMeta = importinto.TaskMeta{
		Plan: importer.Plan{
			CloudStorageURI: "s3://test-bucket/test-path",
		},
	}
	bytes, err = json.Marshal(taskMeta)
	require.NoError(t, err)
	taskID, err = manager.CreateTask(ctx, importinto.TaskKey(222), proto.ImportInto, 1, "", 0, proto.ExtraParams{}, bytes)
	require.NoError(t, err)
	ingestStepSummaries := []*execute.SubtaskSummary{
		{
			RowCnt: *atomic.NewInt64(11),
		},
		{
			RowCnt: *atomic.NewInt64(22),
		},
	}
	for _, m := range ingestStepSummaries {
		testutil.CreateSubTaskWithSummary(t, manager, taskID, proto.ImportStepWriteAndIngest,
			"", bytes, m, proto.SubtaskStatePending, proto.ImportInto, 11)
	}

	switchTaskStep(ctx, t, manager, taskID, proto.ImportStepWriteAndIngest)
	runInfo, err = importinto.GetRuntimeInfoForJob(ctx, 222)
	require.NoError(t, err)
	require.EqualValues(t, 33, runInfo.ImportRows)
}
