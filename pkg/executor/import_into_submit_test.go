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

package executor_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/dxf/importinto/taskkey"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	kvstore "github.com/pingcap/tidb/pkg/store"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/etcd"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/util"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
	"go.uber.org/atomic"
)

func TestSubmitImportTaskNextgen(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("This test is only for nextgen")
	}
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/domain/MockDisableDistTask", "return(true)")

	integration.BeforeTestExternal(t)
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 10})
	defer cluster.Terminate(t)
	keyspaceIDs := map[string]uint32{
		keyspace.System: 1,
		"ks":            2,
	}
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/injectETCDCli",
		func(cliP **clientv3.Client, ks string) {
			id, ok := keyspaceIDs[ks]
			require.True(t, ok)
			// one client per ks
			*cliP = cluster.Client(int(id - 1))
			// we will close the client.
			cluster.TakeClient(int(id - 1))
			codec, err := tikv.NewCodecV2(tikv.ModeTxn, &keyspacepb.KeyspaceMeta{Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: id}, Name: ks})
			require.NoError(t, err)
			etcd.SetEtcdCliByNamespace(*cliP, keyspace.MakeKeyspaceEtcdNamespace(codec))
		},
	)
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

	manuallyInitFn := func(t *testing.T, currKSStore, sysKSStore kv.Storage) *storage.TaskManager {
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
		if kv.IsUserKS(currKSStore) {
			sysKSTaskMgr = storage.NewTaskManager(getPoolFn(sysKSStore))
			storage.SetDXFSvcTaskMgr(sysKSTaskMgr)
		}
		require.NoError(t, sysKSTaskMgr.InitMeta(ctx, "tidb", "dxf_service"))
		return sysKSTaskMgr
	}
	setupUserKeyspaceImportJob := func(t *testing.T) (int64, *storage.TaskManager) {
		t.Helper()
		sysKSTK.MustExec("delete from mysql.tidb_import_jobs")
		sysKSTK.MustExec("delete from mysql.tidb_global_task")
		sysKSTK.MustExec("delete from mysql.tidb_global_task_history")
		userKSTK.MustExec("delete from mysql.tidb_import_jobs")
		userKSTK.MustExec("delete from mysql.tidb_global_task")
		userKSTK.MustExec("delete from mysql.tidb_global_task_history")
		config.UpdateGlobal(func(conf *config.Config) {
			conf.KeyspaceName = "ks"
		})
		sysKSTaskMgr := manuallyInitFn(t, userKSStore, sysKSStore)

		conn := userKSTK.Session().GetSQLExecutor()
		jobID, err := importer.CreateJob(ctx, conn, "test", "t", 1,
			userKSTK.Session().GetSessionVars().User.String(), "", &importer.ImportParameters{}, 123)
		require.NoError(t, err)
		return jobID, sysKSTaskMgr
	}

	t.Run("submit task in system keyspace", func(t *testing.T) {
		config.UpdateGlobal(func(conf *config.Config) {
			conf.KeyspaceName = keyspace.System
		})
		manuallyInitFn(t, sysKSStore, sysKSStore)
		jobID, task, err := executor.SubmitImportTask(ctx, &importer.Plan{
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
		jobID, task, err := executor.SubmitImportTask(ctx, &importer.Plan{
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

	t.Run("cancel dangling user keyspace job without DXF task", func(t *testing.T) {
		jobID, _ := setupUserKeyspaceImportJob(t)
		sysKSTK.MustQuery("select count(1) from mysql.tidb_global_task where task_key = ?", taskkey.ForJob(jobID)).
			Check(testkit.Rows("0"))

		userKSTK.MustExec(fmt.Sprintf("cancel import job %d", jobID))

		userKSTK.MustQuery("select status, error_message from mysql.tidb_import_jobs where id = ?", jobID).
			Check(testkit.Rows("cancelled cancelled by user"))
		sysKSTK.MustQuery("select count(1) from mysql.tidb_global_task where task_key = ?", taskkey.ForJob(jobID)).
			Check(testkit.Rows("0"))
	})

	t.Run("cancel user keyspace job with an archived reverted DXF task", func(t *testing.T) {
		jobID, sysKSTaskMgr := setupUserKeyspaceImportJob(t)
		taskID, err := sysKSTaskMgr.CreateTask(ctx, taskkey.ForJob(jobID), proto.ImportInto, "", 1, "", 0, proto.ExtraParams{}, nil)
		require.NoError(t, err)
		// This is a synthetic state, not a normal real-world scheduler outcome:
		// onReverting calls importScheduler.OnDone to fail or cancel the job before
		// RevertedTask marks the DXF task reverted. Bypassing OnDone here isolates
		// the check that an existing terminal task is not treated as a missing task.
		require.NoError(t, sysKSTaskMgr.RevertTask(ctx, taskID, proto.TaskStatePending, fmt.Errorf("already reverted")))
		require.NoError(t, sysKSTaskMgr.RevertedTask(ctx, taskID))
		task, err := sysKSTaskMgr.GetTaskByID(ctx, taskID)
		require.NoError(t, err)
		require.NoError(t, sysKSTaskMgr.TransferTasks2History(ctx, []*proto.Task{task}))
		sysKSTK.MustQuery("select count(1) from mysql.tidb_global_task where id = ?", taskID).
			Check(testkit.Rows("0"))
		sysKSTK.MustQuery("select state from mysql.tidb_global_task_history where id = ?", taskID).
			Check(testkit.Rows(string(proto.TaskStateReverted)))

		userKSTK.MustExec(fmt.Sprintf("cancel import job %d", jobID))

		userKSTK.MustQuery("select status from mysql.tidb_import_jobs where id = ?", jobID).
			Check(testkit.Rows("pending"))
		sysKSTK.MustQuery("select state from mysql.tidb_global_task_history where id = ?", taskID).
			Check(testkit.Rows(string(proto.TaskStateReverted)))
	})

	t.Run("cancel pending user keyspace job when task is created after lookup miss", func(t *testing.T) {
		jobID, sysKSTaskMgr := setupUserKeyspaceImportJob(t)
		sysKSTK.MustQuery("select count(1) from mysql.tidb_global_task where task_key = ?", taskkey.ForJob(jobID)).
			Check(testkit.Rows("0"))

		var lateTaskID atomic.Int64
		callbackErrCh := make(chan error, 1)
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/afterCancelImportTaskProbeMiss",
			func(probeMissJobID int64) {
				if probeMissJobID != jobID {
					callbackErrCh <- fmt.Errorf("unexpected job ID %d", probeMissJobID)
					return
				}
				taskID, err := sysKSTaskMgr.CreateTask(ctx, taskkey.ForJob(jobID), proto.ImportInto, "", 1, "", 0, proto.ExtraParams{}, nil)
				if err != nil {
					callbackErrCh <- err
					return
				}
				lateTaskID.Store(taskID)
			},
		)

		cancelErrCh := make(chan error, 1)
		go func() {
			_, err := userKSTK.Exec(fmt.Sprintf("cancel import job %d", jobID))
			cancelErrCh <- err
		}()

		select {
		case err := <-cancelErrCh:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			if taskID := lateTaskID.Load(); taskID != 0 {
				require.NoError(t, sysKSTaskMgr.FailTask(ctx, taskID, proto.TaskStatePending, fmt.Errorf("unblock timed out cancel")))
				<-cancelErrCh
			}
			require.FailNow(t, "cancel import waited for a task created after the cancel-by-key miss")
		}
		select {
		case err := <-callbackErrCh:
			require.NoError(t, err)
		default:
		}

		userKSTK.MustQuery("select status, error_message from mysql.tidb_import_jobs where id = ?", jobID).
			Check(testkit.Rows("cancelled cancelled by user"))
		sysKSTK.MustQuery("select state from mysql.tidb_global_task where task_key = ?", taskkey.ForJob(jobID)).
			Check(testkit.Rows(string(proto.TaskStatePending)))
	})

	t.Run("submit global-sort task uses async prepare mode", func(t *testing.T) {
		originalMode := deploymode.Get()
		t.Cleanup(func() {
			require.NoError(t, deploymode.Set(originalMode))
		})
		require.NoError(t, deploymode.Set(deploymode.Premium))
		sysKSTK.MustExec("delete from mysql.tidb_import_jobs")
		sysKSTK.MustExec("delete from mysql.tidb_global_task")
		config.UpdateGlobal(func(conf *config.Config) {
			conf.KeyspaceName = keyspace.System
		})
		manuallyInitFn(t, sysKSStore, sysKSStore)

		jobID, task, err := executor.SubmitImportTask(ctx, &importer.Plan{
			TableInfo:       &model.TableInfo{},
			Parameters:      &importer.ImportParameters{},
			ThreadCnt:       16,
			MaxNodeCnt:      8,
			CloudStorageURI: "local:///tmp/prepare-mode-sort",
		}, "import into t from 's3://bucket/test.csv'")
		require.NoError(t, err)
		sysKSTK.MustQuery("select count(1) from mysql.tidb_import_jobs where id = ?", jobID).Check(testkit.Rows("1"))
		sysKSTK.MustQuery(
			"select concurrency, max_node_count, json_extract(extra_params, '$.prepare_mode') "+
				"from mysql.tidb_global_task where id = ?",
			task.ID,
		).Check(testkit.Rows("1 1 1"))
	})

	t.Run("submit global-sort task in Starter uses synchronous prepare", func(t *testing.T) {
		originalMode := deploymode.Get()
		t.Cleanup(func() {
			require.NoError(t, deploymode.Set(originalMode))
		})
		require.NoError(t, deploymode.Set(deploymode.Starter))
		sysKSTK.MustExec("delete from mysql.tidb_import_jobs")
		sysKSTK.MustExec("delete from mysql.tidb_global_task")
		config.UpdateGlobal(func(conf *config.Config) {
			conf.KeyspaceName = keyspace.System
		})
		manuallyInitFn(t, sysKSStore, sysKSStore)

		jobID, task, err := executor.SubmitImportTask(ctx, &importer.Plan{
			TableInfo:       &model.TableInfo{},
			Parameters:      &importer.ImportParameters{},
			ThreadCnt:       4,
			MaxNodeCnt:      2,
			CloudStorageURI: "local:///tmp/prepare-mode-sort-starter",
		}, "import into t from 's3://bucket/test.csv'")
		require.NoError(t, err)
		sysKSTK.MustQuery("select count(1) from mysql.tidb_import_jobs where id = ?", jobID).Check(testkit.Rows("1"))
		sysKSTK.MustQuery(
			"select concurrency, max_node_count, json_extract(extra_params, '$.prepare_mode') is null "+
				"from mysql.tidb_global_task where id = ?",
			task.ID,
		).Check(testkit.Rows("4 2 1"))
	})
}
