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
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/sem"
	"github.com/stretchr/testify/require"
)

func TestSecurityEnhancedMode(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	sem.Enable()
	defer sem.Disable()
	tk.MustExec("create table test.t (id int);")

	// When SEM is enabled these features are restricted to all users
	// regardless of what privileges they have available.
	tk.MustGetErrMsg("IMPORT INTO test.t FROM '/file.csv'", "[planner:8132]Feature 'IMPORT INTO from server disk' is not supported when security enhanced mode is enabled")
}

func TestImportIntoValidateColAssignmentsWithEncodeCtx(t *testing.T) {
	cases := []struct {
		exprs []string
		error string
	}{
		{
			exprs: []string{"'x'", "1+@1", "concat('hello', 'world')", "getvar('var1')"},
		},
		{
			exprs: []string{"setvar('a', 'b')"},
			error: "FUNCTION setvar is not supported in IMPORT INTO column assignment, index 0",
		},
		{
			exprs: []string{"current_user()"},
			error: "FUNCTION current_user is not supported in IMPORT INTO column assignment, index 0",
		},
		{
			exprs: []string{"current_role()"},
			error: "FUNCTION current_role is not supported in IMPORT INTO column assignment, index 0",
		},
		{
			exprs: []string{"connection_id()"},
			error: "FUNCTION connection_id is not supported in IMPORT INTO column assignment, index 0",
		},
		{
			exprs: []string{"1", "tidb_is_ddl_owner()"},
			error: "FUNCTION tidb_is_ddl_owner is not supported in IMPORT INTO column assignment, index 1",
		},
		{
			exprs: []string{"sleep(1)"},
			error: "FUNCTION sleep is not supported in IMPORT INTO column assignment, index 0",
		},
		{
			exprs: []string{"LAST_INSERT_ID()"},
			error: "FUNCTION last_insert_id is not supported in IMPORT INTO column assignment, index 0",
		},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("case-%d-%s", i, strings.Join(c.exprs, ",")), func(t *testing.T) {
			assigns := make([]*ast.Assignment, 0, len(c.exprs))
			for _, exprStr := range c.exprs {
				stmt, err := parser.New().ParseOneStmt("select "+exprStr, "", "")
				require.NoError(t, err)
				expr := stmt.(*ast.SelectStmt).Fields.Fields[0].Expr
				assigns = append(assigns, &ast.Assignment{
					Expr: expr,
				})
			}

			err := executor.ValidateImportIntoColAssignmentsWithEncodeCtx(&importer.Plan{}, assigns)
			if c.error == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, c.error)
			}
		})
	}
}
<<<<<<< HEAD
=======

func TestImportIntoChildSessionInheritsMaintenanceFlag(t *testing.T) {
	t.Run("ReadOnlyEntry", func(t *testing.T) {
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockModifySchemaReadOnlyDDL", "return")
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/mockModifySchemaReadOnlyDDL", "return")
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("create database ro_import")
		tk.MustExec("create table ro_import.t(a int)")
		tk.MustExec("alter database ro_import read only = 1")
		tk.MustGetErrCode("import into ro_import.t from ''", 3989)
		tk.MustGetErrCode("import into ro_import.t from select 1 with disable_precheck", 3989)
	})
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table src (a int)")
	tk.MustExec("insert into src values (1), (2)")
	tk.MustExec("create table dst (a int)")
	previousURI := vardef.CloudStorageURI.Load()
	t.Cleanup(func() { vardef.CloudStorageURI.Store(previousURI) })
	vardef.CloudStorageURI.Store("")
	expectedError := "mock import from select setup error"
	if kerneltype.IsNextGen() {
		vardef.CloudStorageURI.Store("s3://maintenance-flag-test")
		tk.MustExec("insert into dst values (0)")
		expectedError = "target table is not empty"
	}

	sessionVars := tk.Session().GetSessionVars()
	origRestricted := sessionVars.InRestrictedSQL
	origMaintenance := sessionVars.InMViewMaintenance
	sessionVars.InRestrictedSQL = true
	sessionVars.InMViewMaintenance = true
	defer func() {
		sessionVars.InRestrictedSQL = origRestricted
		sessionVars.InMViewMaintenance = origMaintenance
	}()

	var invoked, childMaintenance bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/inheritMViewMaintenanceFlagApplied", func(maintenance bool) {
		invoked = true
		childMaintenance = maintenance
	})
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/mockImportFromSelectSetupErr", `return(true)`)

	err := tk.ExecToErr("import into dst from select * from src with disable_precheck")
	require.ErrorContains(t, err, expectedError)
	require.True(t, invoked)
	require.True(t, childMaintenance)
}

func TestImportIntoRejectsMaterializedViewLogBaseTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_mview_enable = on")
	tk.MustExec("create table src (a int)")
	tk.MustExec("create table dst (a int)")
	tk.MustExec("create materialized view log on dst (a)")

	err := tk.ExecToErr("import into dst from select * from src with disable_precheck")
	require.ErrorContains(t, err, "IMPORT INTO on tables with materialized view log")
}

func TestImportQueryRouting(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table query_route_src(id int)")
	tk.MustExec("create table query_route_dst(id int)")
	previousURI := vardef.CloudStorageURI.Load()
	t.Cleanup(func() { vardef.CloudStorageURI.Store(previousURI) })
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/mockImportFromSelectSetupErr", `return(true)`)
	sql := "import into query_route_dst from select id from query_route_src with disable_precheck"
	vars := tk.Session().GetSessionVars()
	if kerneltype.IsNextGen() {
		vardef.CloudStorageURI.Store("s3://query-sort")
		require.ErrorContains(t, tk.ExecToErr(sql), "IMPORT INTO FROM SELECT outside materialized view maintenance")
	}
	vars.InMViewMaintenance = true
	t.Cleanup(func() { vars.InMViewMaintenance = false })
	if kerneltype.IsClassic() {
		// Classic FROM SELECT runs locally.
		require.ErrorContains(t, tk.ExecToErr(sql), "mock import from select setup error")
		return
	}
	vardef.CloudStorageURI.Store("")
	require.ErrorContains(t, tk.ExecToErr(sql), "IMPORT INTO FROM SELECT without global sort storage")
	// With a sort store, the query is captured and the import reaches the prechecks.
	vardef.CloudStorageURI.Store("s3://query-sort")
	tk.MustExec("insert into query_route_dst values (1)")
	require.ErrorContains(t, tk.ExecToErr(sql), "target table is not empty")
}

type importPlanKeyspaceStore struct {
	kv.Storage
}

func (importPlanKeyspaceStore) GetKeyspace() string {
	return ""
}

func TestNewImportPlanMatchesResolvedInput(t *testing.T) {
	ctx := context.Background()
	sctx := mock.NewContext()
	defer sctx.Close()
	sctx.Store = importPlanKeyspaceStore{}
	tblInfo := &model.TableInfo{ID: 42, Name: ast.NewCIStr("t"), State: model.StatePublic}
	tbl := tables.MockTableFromMeta(tblInfo)
	format := importer.DataFormatCSV
	value := &expression.Constant{Value: types.NewIntDatum(2), RetType: types.NewFieldType(mysql.TypeLonglong)}
	for _, sourceType := range []importer.DataSourceType{importer.DataSourceTypeFile, importer.DataSourceTypeQuery} {
		t.Run(sourceType.String(), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "data.csv")
			input := &importer.Input{
				DBName: "test", DBID: 7, TableInfo: tblInfo,
				Path: path, Format: &format, DataSourceType: sourceType,
				Options: []*importer.ImportOption{{Name: "thread", Value: value}},
			}
			physical := &plannercore.ImportInto{
				Table: &resolve.TableNameW{
					TableName: &ast.TableName{Schema: ast.NewCIStr("test"), Name: ast.NewCIStr("t")},
					DBInfo:    &model.DBInfo{ID: 7, Name: ast.NewCIStr("test")},
				},
				Path: path, Format: &format,
				Options: []*plannercore.LoadDataOpt{{Name: "thread", Value: value}},
			}
			if sourceType == importer.DataSourceTypeQuery {
				physical.SelectPlan = &physicalop.PhysicalSelection{}
			}
			fromSQL, err := executor.NewImportPlan(ctx, sctx, physical, tbl)
			require.NoError(t, err)
			require.Equal(t, sourceType, fromSQL.DataSourceType)
			direct, err := importer.NewImportPlanFromInput(ctx, sctx, input)
			require.NoError(t, err)
			require.Equal(t, fromSQL, direct)
			encoded, err := json.Marshal(fromSQL)
			require.NoError(t, err)
			directEncoded, err := json.Marshal(direct)
			require.NoError(t, err)
			require.JSONEq(t, string(encoded), string(directEncoded))

			// Both entrypoints reject the same invalid numeric option.
			bad := &expression.Constant{Value: types.NewStringDatum("bad"), RetType: types.NewFieldType(mysql.TypeVarString)}
			input.Options[0].Value = bad
			physical.Options[0].Value = bad
			_, sqlErr := executor.NewImportPlan(ctx, sctx, physical, tbl)
			_, directErr := importer.NewImportPlanFromInput(ctx, sctx, input)
			require.Error(t, sqlErr)
			require.EqualError(t, directErr, sqlErr.Error())
		})
	}
}

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

// maintenanceImportOpenSpy observes child side effects at the import admission boundary.
type maintenanceImportOpenSpy struct {
	exec.BaseExecutor
	opened bool
}

func (s *maintenanceImportOpenSpy) Open(context.Context) error {
	s.opened = true
	return nil
}

func TestMaintenanceMode(t *testing.T) {
	const ddlUnavailableMessage = "[executor:8277]During the maintenance window, DDL is currently unavailable. It will be available after maintenance finishes"
	const importUnavailableMessage = "[executor:8277]During the maintenance window, IMPORT INTO is currently unavailable. It will be available after maintenance finishes"
	t.Cleanup(config.RestoreFunc())
	config.UpdateGlobal(func(conf *config.Config) {
		conf.MaintenanceMode = true
	})
	// Startup's restricted DDL must work even when user job admission is disabled.
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	if kerneltype.IsClassic() {
		// Bypass config validation to verify the runtime guards are NextGen-only.
		tk.MustExec("create table maintenance_classic (a int)")
		tk.MustExec("alter table maintenance_classic add column b int")
		tk.MustExec("drop table maintenance_classic")
		sctx := mock.NewContext()
		child := &maintenanceImportOpenSpy{BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0)}
		importExec := &executor.ImportIntoExec{BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0, child)}
		require.NoError(t, importExec.Open(context.Background()))
		require.True(t, child.opened)
		return
	}
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)
	for _, sql := range []string{
		"create table test.starter_target (a int)",
		"create table test.starter_source (a int)",
		"alter table test.starter_source add column upgrade_column int",
		"alter table test.starter_source drop column upgrade_column",
	} {
		rs, err := tk.Session().ExecuteInternal(ctx, sql)
		require.NoError(t, err)
		require.Nil(t, rs)
	}

	t.Run("user DDL", func(t *testing.T) {
		for _, sql := range []string{
			"create table starter_rejected (a int)",
			"alter table starter_target add column b int",
			"alter table starter_target add column b int, add column c int",
			"drop table starter_target",
			"create database starter_rejected_db",
			"create global temporary table starter_global_temp (a int) on commit delete rows",
		} {
			require.ErrorContains(t, tk.ExecToErr(sql), ddlUnavailableMessage)
			tk.MustGetErrCode(sql, 8277)
		}
		require.ErrorContains(t, tk.ExecToErr("explain analyze alter table starter_target add column b int"), ddlUnavailableMessage)
		tk.MustQuery("show tables like 'starter_rejected'").Check(testkit.Rows())
		tk.MustQuery("select count(*) from information_schema.columns where table_schema = 'test' and table_name = 'starter_target'").Check(testkit.Rows("1"))
		tk.MustExec("prepare starter_ddl from 'create table starter_prepared (a int)'")
		require.ErrorContains(t, tk.ExecToErr("execute starter_ddl"), ddlUnavailableMessage)
		tk.MustExec("deallocate prepare starter_ddl")
		id, _, _, err := tk.Session().PrepareStmt("create table starter_binary_prepared (a int)")
		require.NoError(t, err)
		_, err = tk.Session().ExecutePreparedStmt(context.Background(), id, nil)
		require.ErrorContains(t, err, ddlUnavailableMessage)
		require.NoError(t, tk.Session().DropPreparedStmt(id))
		tk.MustQuery("show tables like 'starter_prepared'").Check(testkit.Rows())
		tk.MustQuery("show tables like 'starter_binary_prepared'").Check(testkit.Rows())
	})

	t.Run("local temporary DDL and DML", func(t *testing.T) {
		tk.MustExec("create temporary table starter_temp (a int)")
		tk.MustExec("insert into starter_temp values (1)")
		tk.MustQuery("select * from starter_temp").Check(testkit.Rows("1"))
		tk.MustExec("drop temporary table starter_temp")
		tk.MustExec("insert into starter_source values (1)")
		tk.MustExec("update starter_source set a = 2")
		tk.MustQuery("select * from starter_source").Check(testkit.Rows("2"))
		tk.MustExec("delete from starter_source")
	})

	for _, closeSession := range []bool{false, true} {
		t.Run(fmt.Sprintf("release existing table locks/close=%t", closeSession), func(t *testing.T) {
			t.Cleanup(config.RestoreFunc())
			config.UpdateGlobal(func(conf *config.Config) {
				conf.EnableTableLock = true
				conf.DelayCleanTableLock = 0
				conf.MaintenanceMode = false
			})
			lockTK := testkit.NewTestKit(t, store)
			closed := false
			t.Cleanup(func() {
				config.UpdateGlobal(func(conf *config.Config) {
					conf.MaintenanceMode = false
				})
				if !closed {
					lockTK.Session().Close()
				}
			})
			lockTK.MustExec("use test")
			lockTK.MustExec("lock tables starter_target write")
			require.Len(t, lockTK.Session().GetAllTableLocks(), 1)
			require.ErrorContains(t, tk.ExecToErr("insert into starter_target values (1)"), "Table 'starter_target' was locked")
			config.UpdateGlobal(func(conf *config.Config) {
				conf.MaintenanceMode = true
			})
			require.ErrorContains(t, lockTK.ExecToErr("lock tables starter_source write"), ddlUnavailableMessage)
			require.Len(t, lockTK.Session().GetAllTableLocks(), 1)
			if closeSession {
				lockTK.Session().Close()
				closed = true
			} else {
				lockTK.MustExec("unlock tables")
			}
			require.Empty(t, lockTK.Session().GetAllTableLocks())
			tk.MustExec("insert into starter_target values (1)")
			tk.MustExec("delete from starter_target")
		})
	}

	t.Run("rejected table lock preserves session state", func(t *testing.T) {
		t.Cleanup(config.RestoreFunc())
		config.UpdateGlobal(func(conf *config.Config) {
			conf.EnableTableLock = true
		})
		lockTK := testkit.NewTestKit(t, store)
		lockTK.MustExec("use test")
		require.ErrorContains(t, lockTK.ExecToErr("lock tables starter_target write"), ddlUnavailableMessage)
		require.Empty(t, lockTK.Session().GetAllTableLocks())
		lockTK.MustExec("create temporary table starter_lock_temp (a int)")
		lockTK.MustExec("insert into starter_lock_temp values (1)")
		lockTK.MustQuery("select * from starter_lock_temp").Check(testkit.Rows("1"))
		lockTK.MustExec("drop temporary table starter_lock_temp")
		lockTK.MustExec("unlock tables")
	})

	t.Run("IMPORT INTO", func(t *testing.T) {
		// NextGen permits FROM SELECT only for materialized view maintenance.
		vars := tk.Session().GetSessionVars()
		vars.InMViewMaintenance = true
		defer func() { vars.InMViewMaintenance = false }()
		for _, sql := range []string{
			"import into starter_target from 's3://bucket/disabled.csv'",
			"import into starter_target from 's3://bucket/disabled.csv' with detached",
			"import into starter_target from select * from starter_source with disable_precheck",
			"explain analyze import into starter_target from select * from starter_source with disable_precheck",
		} {
			require.ErrorContains(t, tk.ExecToErr(sql), importUnavailableMessage)
			tk.MustGetErrCode(sql, 8277)
		}
		require.NotEmpty(t, tk.MustQuery("explain import into starter_target from select * from starter_source").Rows())
		tk.MustQuery("show import jobs").Check(testkit.Rows())
		tk.MustQuery("select count(*) from mysql.tidb_global_task").Check(testkit.Rows("0"))
		tk.MustQuery("select count(*) from mysql.tidb_global_task_history").Check(testkit.Rows("0"))
	})

	t.Run("import admission precedes child Open", func(t *testing.T) {
		sctx := mock.NewContext()
		child := &maintenanceImportOpenSpy{BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0)}
		importExec := &executor.ImportIntoExec{BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0, child)}
		require.ErrorContains(t, importExec.Open(context.Background()), importUnavailableMessage)
		require.False(t, child.opened)
		sctx.GetSessionVars().InRestrictedSQL = true
		require.NoError(t, importExec.Open(context.Background()))
		require.True(t, child.opened)
	})

	config.UpdateGlobal(func(conf *config.Config) {
		conf.MaintenanceMode = false
	})
	tk.MustExec("create table starter_enabled (a int)")
	tk.MustExec("alter table starter_enabled add column b int")
	tk.MustExec("drop table starter_enabled")
}
>>>>>>> bf712cf67f6 (ddl: support database-level read-only mode (#72912))
