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

package importinto_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/domain"
	sqlsvrapimock "github.com/pingcap/tidb/pkg/domain/sqlsvrapi/mock"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/dxf/importinto"
	"github.com/pingcap/tidb/pkg/dxf/importinto/taskinfo"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/etcd"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	tikvutil "github.com/tikv/client-go/v2/util"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
	"go.uber.org/mock/gomock"
)

func TestImportCleanerNextgen(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("This test is only for nextgen")
	}
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/domain/MockDisableDistTask", "return(true)")

	integration.BeforeTestExternal(t)
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 2})
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

	ctx := tikvutil.WithInternalSourceType(context.Background(), kv.InternalDistTask)

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
	t.Run("read-only transition excludes background import", func(t *testing.T) {
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockModifySchemaReadOnlyDDL", "return")
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/mockModifySchemaReadOnlyDDL", "return")
		config.UpdateGlobal(func(conf *config.Config) { conf.KeyspaceName = "ks" })
		sysTaskMgr := manuallyInitFn(t, userKSStore, sysKSStore)
		userKSTK.MustExec("create database read_only_import")
		userKSTK.MustExec("create table read_only_import.t(a int primary key)")
		dom := domain.GetDomain(userKSTK.Session())
		db, ok := dom.InfoSchema().SchemaByName(ast.NewCIStr("read_only_import"))
		require.True(t, ok)
		tbl, err := dom.InfoSchema().TableByName(ctx, db.Name, ast.NewCIStr("t"))
		require.NoError(t, err)
		_, task, err := executor.SubmitImportTask(ctx, &importer.Plan{
			DBID: db.ID, DBName: db.Name.O, TableInfo: tbl.Meta(), Keyspace: "ks", Parameters: &importer.ImportParameters{},
		}, "import into read_only_import.t from '/path/to/file'")
		require.NoError(t, err)
		tbl, err = dom.InfoSchema().TableByName(ctx, db.Name, ast.NewCIStr("t"))
		require.NoError(t, err)
		require.Equal(t, model.TableModeImport, tbl.Meta().Mode)
		userKSTK.MustGetErrCode("alter database read_only_import read only = 1", 8258)
		fullTask, err := sysTaskMgr.GetTaskByID(ctx, task.ID)
		require.NoError(t, err)
		require.NoError(t, (&importinto.ImportCleaner{}).Clean(ctx, fullTask))
		userKSTK.MustExec("update mysql.tidb_import_jobs set status = 'cancelled' where table_id = ?", tbl.Meta().ID)
		_, nextTask, err := executor.SubmitImportTask(ctx, &importer.Plan{
			DBID: db.ID, DBName: db.Name.O, TableInfo: tbl.Meta(), Keyspace: "ks", Parameters: &importer.ImportParameters{},
		}, "import into read_only_import.t from '/path/to/file'")
		require.NoError(t, err)
		require.NoError(t, (&importinto.ImportCleaner{}).Clean(ctx, fullTask), "stale cleanup must preserve the next import")
		current, err := dom.InfoSchema().TableByName(ctx, db.Name, ast.NewCIStr("t"))
		require.NoError(t, err)
		require.Equal(t, model.TableModeImport, current.Meta().Mode)
		nextFullTask, err := sysTaskMgr.GetTaskByID(ctx, nextTask.ID)
		require.NoError(t, err)
		require.NoError(t, (&importinto.ImportCleaner{}).Clean(ctx, nextFullTask))
		sysKSTK.MustExec("delete from mysql.tidb_global_task where id = ?", nextTask.ID)
		userKSTK.MustExec("drop table read_only_import.t")
		require.NoError(t, (&importinto.ImportCleaner{}).Clean(ctx, fullTask), "cleanup must tolerate a dropped table on retry")
		userKSTK.MustExec("alter database read_only_import read only = 1")
		userKSTK.MustExec("alter database read_only_import read only = 0")
		userKSTK.MustExec("drop database read_only_import")
		require.NoError(t, (&importinto.ImportCleaner{}).Clean(ctx, fullTask), "cleanup must tolerate a dropped database on retry")
		userKSTK.MustExec("delete from mysql.tidb_import_jobs")
		sysKSTK.MustExec("delete from mysql.tidb_global_task where id = ?", task.ID)
	})

	t.Run("authorized read-only import restores table mode", func(t *testing.T) {
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockModifySchemaReadOnlyDDL", "return")
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/mockModifySchemaReadOnlyDDL", "return")
		config.UpdateGlobal(func(conf *config.Config) { conf.KeyspaceName = "ks" })
		sysTaskMgr := manuallyInitFn(t, userKSStore, sysKSStore)
		userKSTK.MustExec("create database authorized_import")
		userKSTK.MustExec("create table authorized_import.t(a int primary key)")
		dom := domain.GetDomain(userKSTK.Session())
		db, ok := dom.InfoSchema().SchemaByName(ast.NewCIStr("authorized_import"))
		require.True(t, ok)
		tbl, err := dom.InfoSchema().TableByName(ctx, db.Name, ast.NewCIStr("t"))
		require.NoError(t, err)
		plan := &importer.Plan{DBID: db.ID, DBName: db.Name.O, TableInfo: tbl.Meta(), Keyspace: "ks", Parameters: &importer.ImportParameters{}}
		userKSTK.MustExec("alter database authorized_import read only = 1")
		_, _, err = executor.SubmitImportTask(ctx, plan, "import into authorized_import.t from '/path/to/file'")
		require.ErrorContains(t, err, "read only mode")
		plan.SchemaReadOnlyBypass = true
		_, task, err := executor.SubmitImportTask(ctx, plan, "import into authorized_import.t from '/path/to/file'")
		require.NoError(t, err)
		fullTask, err := sysTaskMgr.GetTaskByID(ctx, task.ID)
		require.NoError(t, err)
		var metadata map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(fullTask.Meta, &metadata))
		require.NotContains(t, string(fullTask.Meta), "SchemaReadOnlyBypass")
		require.NoError(t, (&importinto.ImportCleaner{}).Clean(ctx, fullTask))
		tbl, err = dom.InfoSchema().TableByName(ctx, db.Name, ast.NewCIStr("t"))
		require.NoError(t, err)
		require.Equal(t, model.TableModeNormal, tbl.Meta().Mode)
		userKSTK.MustGetErrCode("insert into authorized_import.t values (1)", 3989)
		userKSTK.MustExec("alter database authorized_import read only = 0")
		userKSTK.MustExec("drop database authorized_import")
		userKSTK.MustExec("delete from mysql.tidb_import_jobs")
		sysKSTK.MustExec("delete from mysql.tidb_global_task where id = ?", task.ID)
	})

	t.Run("cleanup propagates table mode errors", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		runtime := sqlsvrapimock.NewMockKSRuntimeHandle(ctrl)
		server := sqlsvrapimock.NewMockServer(ctrl)
		se := mock.NewContext()
		se.Store = sysKSStore
		se.BindDomainAndSchValidator(server, nil)
		pool := tidbutil.NewSessionPool(1, func() (pools.Resource, error) { return se, nil }, nil, nil, nil)
		defer pool.Close()
		previous, err := storage.GetTaskManager()
		require.NoError(t, err)
		storage.SetTaskManager(storage.NewTaskManager(pool))
		defer storage.SetTaskManager(previous)

		server.EXPECT().AcquireKSRuntime("ks", "DXF/import-cleaner/42").Return(runtime, nil)
		target := model.AlterTableModeTarget{
			SchemaID: 1, SchemaName: ast.NewCIStr("test"),
			TableID: 2, TableName: ast.NewCIStr("t"), TargetMode: model.TableModeNormal,
		}
		cleanupErr := fmt.Errorf("injected table mode cleanup failure")
		runtime.EXPECT().AlterTableMode(gomock.Any(), target).Return(cleanupErr)
		runtime.EXPECT().Release()
		metadata, err := json.Marshal(taskinfo.TaskMeta{Plan: importer.Plan{
			DBID: 1, DBName: "test", TableInfo: &model.TableInfo{ID: 2, Name: ast.NewCIStr("t")},
		}})
		require.NoError(t, err)
		require.ErrorIs(t, (&importinto.ImportCleaner{}).Clean(ctx, &proto.Task{
			TaskBase: proto.TaskBase{ID: 42, Keyspace: "ks"}, Meta: metadata,
		}), cleanupErr)
	})

}
