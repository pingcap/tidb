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

package jobsubmit_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/ddl/jobsubmit"
	"github.com/pingcap/tidb/pkg/ddl/serverstate"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/ddl/systable"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type submitTestEnv struct {
	store kv.Storage
	tk    *testkit.TestKit
	opts  jobsubmit.SubmitOptions
}

type fakeServerStateSyncer struct {
	upgrading bool
}

func (*fakeServerStateSyncer) Init(context.Context) error { return nil }

func (s *fakeServerStateSyncer) UpdateGlobalState(_ context.Context, info *serverstate.StateInfo) error {
	s.upgrading = info.State == serverstate.StateUpgrading
	return nil
}

func (s *fakeServerStateSyncer) GetGlobalState(context.Context) (*serverstate.StateInfo, error) {
	if s.upgrading {
		return serverstate.NewStateInfo(serverstate.StateUpgrading), nil
	}
	return serverstate.NewStateInfo(serverstate.StateNormalRunning), nil
}

func (s *fakeServerStateSyncer) IsUpgradingState() bool { return s.upgrading }

func (*fakeServerStateSyncer) WatchChan() clientv3.WatchChan { return nil }

func (*fakeServerStateSyncer) Rewatch(context.Context) {}

func newSubmitTestEnv(t *testing.T) submitTestEnv {
	store := testkit.CreateMockStore(t, mockstore.WithStoreType(mockstore.EmbedUnistore))
	tk := testkit.NewTestKit(t, store)

	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return testkit.NewTestKit(t, store).Session(), nil
	}, 8, 8, time.Second)
	t.Cleanup(func() {
		pool.Close()
	})

	sessPool := sess.NewSessionPool(pool)
	sysTblMgr := systable.NewManager(sessPool)
	return submitTestEnv{
		store: store,
		tk:    tk,
		opts: jobsubmit.SubmitOptions{
			Store:             store,
			SessPool:          sessPool,
			SysTblMgr:         sysTblMgr,
			MinJobIDRefresher: systable.NewMinJobIDRefresher(sysTblMgr),
			ServerStateSyncer: &fakeServerStateSyncer{},
		},
	}
}

func getGlobalIDForSubmitTest(ctx context.Context, t *testing.T, store kv.Storage) int64 {
	var res int64
	require.NoError(t, kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
		m := meta.NewMutator(txn)
		id, err := m.GetGlobalID()
		require.NoError(t, err)
		res = id
		return nil
	}))
	return res
}

func setBDRRoleForSubmitTest(ctx context.Context, t *testing.T, store kv.Storage, role ast.BDRRole) {
	require.NoError(t, kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
		return meta.NewMutator(txn).SetBDRRole(string(role))
	}))
}

func newTableModeSpec(t *testing.T) *jobsubmit.JobSpec {
	t.Helper()
	job := &model.Job{
		Version:        model.JobVersion2,
		SchemaID:       100,
		TableID:        200,
		SchemaName:     "testdb",
		Type:           model.ActionAlterTableMode,
		Query:          "skip",
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: 7,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{
			{
				Database: "testdb",
				Table:    "t1",
			},
		},
	}
	args := &model.AlterTableModeArgs{
		TableMode: model.TableModeImport,
		SchemaID:  100,
		TableID:   200,
	}
	return &jobsubmit.JobSpec{
		Job:         job,
		Args:        args,
		IDAllocated: true,
	}
}

func TestSubmitBatchEnqueuesTableModeJob(t *testing.T) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	env := newSubmitTestEnv(t)
	spec := newTableModeSpec(t)
	initialGID := getGlobalIDForSubmitTest(ctx, t, env.store)

	err := jobsubmit.SubmitBatch(ctx, env.opts, []*jobsubmit.JobSpec{spec})
	require.NoError(t, err)
	require.Greater(t, spec.Job.ID, initialGID)
	require.Equal(t, model.JobStateQueueing, spec.Job.State)
	require.NotZero(t, spec.Job.StartTS)
	require.Equal(t, string(ast.BDRRoleNone), spec.Job.BDRRole)

	jobW, err := env.opts.SysTblMgr.GetJobByID(ctx, spec.Job.ID)
	require.NoError(t, err)
	require.Equal(t, model.ActionAlterTableMode, jobW.Type)
	require.Equal(t, model.JobStateQueueing, jobW.State)
	require.EqualValues(t, 100, jobW.SchemaID)
	require.EqualValues(t, 200, jobW.TableID)
	require.Equal(t, "skip", jobW.Query)
	require.Equal(t, []model.InvolvingSchemaInfo{{
		Database: "testdb",
		Table:    "t1",
	}}, jobW.InvolvingSchemaInfo)
	args, err := model.GetAlterTableModeArgs(jobW.Job)
	require.NoError(t, err)
	require.Equal(t, model.TableModeImport, args.TableMode)

	env.tk.MustQuery(
		fmt.Sprintf("select schema_ids, table_ids, type, processing from mysql.tidb_ddl_job where job_id = %d", spec.Job.ID),
	).Check(testkit.Rows(fmt.Sprintf("100 200 %d 0", model.ActionAlterTableMode)))
}

func TestSubmitBatchAllocatesIDsAndInsertsJob(t *testing.T) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	env := newSubmitTestEnv(t)
	spec := &jobsubmit.JobSpec{
		Job: &model.Job{
			Version:             model.JobVersion2,
			Type:                model.ActionCreateTable,
			SchemaName:          "test",
			InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{Database: "test", Table: "created_by_jobsubmit"}},
		},
		Args: &model.CreateTableArgs{
			TableInfo: &model.TableInfo{
				Name: ast.NewCIStr("created_by_jobsubmit"),
				Partition: &model.PartitionInfo{
					Enable: true,
					Definitions: []model.PartitionDefinition{
						{Name: ast.NewCIStr("p0")},
						{Name: ast.NewCIStr("p1")},
					},
				},
			},
		},
	}
	initialGID := getGlobalIDForSubmitTest(ctx, t, env.store)

	err := jobsubmit.SubmitBatch(ctx, env.opts, []*jobsubmit.JobSpec{spec})
	require.NoError(t, err)
	require.Greater(t, spec.Job.ID, initialGID)
	require.Greater(t, spec.Job.TableID, initialGID)
	createArgs := spec.Args.(*model.CreateTableArgs)
	require.Equal(t, spec.Job.TableID, createArgs.TableInfo.ID)
	require.Len(t, createArgs.TableInfo.Partition.Definitions, 2)
	require.Greater(t, createArgs.TableInfo.Partition.Definitions[0].ID, initialGID)
	require.Greater(t, createArgs.TableInfo.Partition.Definitions[1].ID, initialGID)
	env.tk.MustQuery(
		fmt.Sprintf("select schema_ids, table_ids from mysql.tidb_ddl_job where job_id = %d", spec.Job.ID),
	).Check(testkit.Rows(fmt.Sprintf("0 %d", spec.Job.TableID)))
}

func TestSubmitBatchChecksAndPauseState(t *testing.T) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)

	t.Run("invalid involving schema info", func(t *testing.T) {
		env := newSubmitTestEnv(t)
		spec := newTableModeSpec(t)
		spec.Job.InvolvingSchemaInfo = []model.InvolvingSchemaInfo{{Database: "test"}}
		err := jobsubmit.SubmitBatch(ctx, env.opts, []*jobsubmit.JobSpec{spec})
		require.ErrorContains(t, err, "must have non-empty name")
		env.tk.MustQuery("select count(*) from mysql.tidb_ddl_job").Check(testkit.Rows("0"))
	})

	t.Run("flashback cluster job blocks submit", func(t *testing.T) {
		env := newSubmitTestEnv(t)
		env.tk.MustExec(fmt.Sprintf(`insert into mysql.tidb_ddl_job(job_id, reorg, schema_ids, table_ids, job_meta, type, processing)
			values(123, 0, '1', '1', '{"id":123}', %d, 0)`, model.ActionFlashbackCluster))
		err := jobsubmit.SubmitBatch(ctx, env.opts, []*jobsubmit.JobSpec{newTableModeSpec(t)})
		require.ErrorContains(t, err, "have flashback cluster job")
	})

	t.Run("BDR restricted DDL is denied", func(t *testing.T) {
		env := newSubmitTestEnv(t)
		setBDRRoleForSubmitTest(ctx, t, env.store, ast.BDRRolePrimary)
		spec := newTableModeSpec(t)
		spec.Job.CDCWriteSource = 0
		err := jobsubmit.SubmitBatch(ctx, env.opts, []*jobsubmit.JobSpec{spec})
		require.ErrorContains(t, err, "bdr role")
		env.tk.MustQuery("select count(*) from mysql.tidb_ddl_job").Check(testkit.Rows("0"))
	})

	t.Run("upgrade state pauses non-system DDL before insert", func(t *testing.T) {
		env := newSubmitTestEnv(t)
		env.opts.ServerStateSyncer.(*fakeServerStateSyncer).upgrading = true
		spec := newTableModeSpec(t)
		err := jobsubmit.SubmitBatch(ctx, env.opts, []*jobsubmit.JobSpec{spec})
		require.NoError(t, err)
		require.Equal(t, model.JobStatePausing, spec.Job.State)
		require.Equal(t, model.AdminCommandBySystem, spec.Job.AdminOperator)
		jobW, err := env.opts.SysTblMgr.GetJobByID(ctx, spec.Job.ID)
		require.NoError(t, err)
		require.Equal(t, model.JobStatePausing, jobW.State)
		require.Equal(t, model.AdminCommandBySystem, jobW.AdminOperator)
	})
}

func TestSubmitBatchRetryCleanup(t *testing.T) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	env := newSubmitTestEnv(t)
	spec := &jobsubmit.JobSpec{
		Job: &model.Job{
			Version:             model.JobVersion2,
			Type:                model.ActionCreateTable,
			SchemaName:          "test",
			InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{Database: "test", Table: "retry_cleanup"}},
		},
		Args: &model.CreateTableArgs{
			TableInfo: &model.TableInfo{Name: ast.NewCIStr("retry_cleanup")},
		},
	}

	var assignedIDs []int64
	var cleanupIDs []int64
	env.opts.BeforeInsertWithAssignedIDs = func(specs []*jobsubmit.JobSpec) func() {
		require.Len(t, specs, 1)
		id := specs[0].Job.ID
		require.NotZero(t, id)
		assignedIDs = append(assignedIDs, id)
		return func() {
			cleanupIDs = append(cleanupIDs, id)
		}
	}
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/jobsubmit/mockGenGIDRetryableError", `1*return(true)`)

	err := jobsubmit.SubmitBatch(ctx, env.opts, []*jobsubmit.JobSpec{spec})
	require.NoError(t, err)
	require.Len(t, assignedIDs, 2)
	require.Len(t, cleanupIDs, 1)
	require.Equal(t, assignedIDs[0], cleanupIDs[0])
	require.Equal(t, assignedIDs[1], spec.Job.ID)
	env.tk.MustQuery("select count(*) from mysql.tidb_ddl_job").Check(testkit.Rows("1"))
}
