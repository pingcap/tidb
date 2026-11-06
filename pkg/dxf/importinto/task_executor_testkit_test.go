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
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	tidbconfig "github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/dxf/importinto"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/ingestor/globalsort"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func TestPostProcessStepExecutor(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	asInt := func(s string) int {
		v, err := strconv.Atoi(s)
		require.NoError(t, err)
		return v
	}

	tk.MustExec("use test")
	tk.MustExec("create table t (a int primary key, b int)")
	tk.MustExec("insert into t values (1, 2), (3, 4)")
	res := tk.MustQuery("admin checksum table t").Rows()
	stepMeta := &importinto.PostProcessStepMeta{
		Checksum: map[int64]importinto.Checksum{
			-1: {
				Sum:  uint64(asInt(res[0][2].(string))),
				KVs:  uint64(asInt(res[0][3].(string))),
				Size: uint64(asInt(res[0][4].(string))),
			},
		},
	}

	dom, err := session.GetDomain(store)
	require.NoError(t, err)
	db, ok := dom.InfoSchema().SchemaByName(ast.NewCIStr("test"))
	require.True(t, ok)
	table, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	taskMeta := &importinto.TaskMeta{
		Plan: importer.Plan{
			DBID:             db.ID,
			Checksum:         config.OpLevelRequired,
			TableInfo:        table.Meta(),
			DesiredTableInfo: table.Meta(),
			DBName:           "test",
		},
	}

	bytes, err := json.Marshal(stepMeta)
	require.NoError(t, err)
	var taskKS string
	if kerneltype.IsNextGen() {
		taskKS = keyspace.System
	}
	taskMgr, err := storage.GetTaskManager()
	require.NoError(t, err)
	executor := importinto.NewPostProcessStepExecutor(1, store, taskMgr, taskMeta, taskKS, zap.NewExample())
	err = executor.RunSubtask(context.Background(), &proto.Subtask{Meta: bytes})
	require.NoError(t, err)

	tmp := stepMeta.Checksum[-1]
	tmp.Sum += 1
	stepMeta.Checksum[-1] = tmp
	bytes, err = json.Marshal(stepMeta)
	require.NoError(t, err)
	executor = importinto.NewPostProcessStepExecutor(1, store, taskMgr, taskMeta, taskKS, zap.NewExample())
	err = executor.RunSubtask(context.Background(), &proto.Subtask{Meta: bytes})
	require.ErrorContains(t, err, "checksum mismatched remote vs local")

	taskMeta.Plan.Checksum = config.OpLevelOptional
	executor = importinto.NewPostProcessStepExecutor(1, store, taskMgr, taskMeta, taskKS, zap.NewExample())
	err = executor.RunSubtask(context.Background(), &proto.Subtask{Meta: bytes})
	require.NoError(t, err)

	taskMeta.Plan.Checksum = config.OpLevelOff
	executor = importinto.NewPostProcessStepExecutor(1, store, taskMgr, taskMeta, taskKS, zap.NewExample())
	err = executor.RunSubtask(context.Background(), &proto.Subtask{Meta: bytes})
	require.NoError(t, err)
}

func TestImportStepExecutorCleanupAllLocalEnginesOnRetry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	if kerneltype.IsNextGen() {
		t.Skip("next-gen doesn't support local sort, skip this test")
	}
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key, v int)")

	dom, err := session.GetDomain(store)
	require.NoError(t, err)
	dbInfo, ok := dom.InfoSchema().SchemaByName(ast.NewCIStr("test"))
	require.True(t, ok)
	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)

	dataPath := filepath.ToSlash(filepath.Join(t.TempDir(), "data.csv"))
	require.NoError(t, os.WriteFile(dataPath, []byte("1,1\n"), 0o600))

	taskMetaBytes, err := json.Marshal(importinto.TaskMeta{
		Plan: importer.Plan{
			DBID:             dbInfo.ID,
			DBName:           "test",
			TableInfo:        tbl.Meta(),
			DesiredTableInfo: tbl.Meta(),
			ThreadCnt:        1,
			InImportInto:     true,
			Format:           importer.DataFormatCSV,
		},
		Stmt: fmt.Sprintf("import into test.t from '%s'", dataPath),
	})
	require.NoError(t, err)

	task := &proto.Task{
		TaskBase: proto.TaskBase{
			ID:   1,
			Type: proto.ImportInto,
			Step: proto.ImportStepImport,
		},
		Meta: taskMetaBytes,
	}
	param := taskexecutor.NewParamForTest(nil, nil, nil, ":4000")
	param.TaskRuntime = newImportTestRuntime(ctrl, store, nil)
	taskExecutor := importinto.NewImportExecutor(
		context.Background(),
		task,
		param,
	)
	t.Cleanup(taskExecutor.Close)

	stepExecutorFactory := taskExecutor.(interface {
		GetStepExecutor(task *proto.Task) (execute.StepExecutor, error)
	})
	stepExecutor, err := stepExecutorFactory.GetStepExecutor(task)
	require.NoError(t, err)

	resource := &proto.StepResource{
		CPU: proto.NewAllocatable(4),
		Mem: proto.NewAllocatable(8 << 30),
	}
	execute.SetFrameworkInfo(stepExecutor, task, resource, nil, nil)
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/dxf/importinto/createTableImporterForTest", `return(true)`)
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/dxf/importinto/errorWhenSortChunk", `return(true)`)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	require.NoError(t, stepExecutor.Init(ctx))

	subtaskMetaBytes, err := json.Marshal(importinto.ImportStepMeta{
		ID: 1,
		Chunks: []importer.Chunk{
			{
				Path:        dataPath,
				Type:        mydump.SourceTypeCSV,
				Compression: mydump.CompressionNone,
			},
		},
	})
	require.NoError(t, err)
	subtask := &proto.Subtask{
		SubtaskBase: proto.SubtaskBase{
			ID: 1,
		},
		Meta: subtaskMetaBytes,
	}

	err = stepExecutor.RunSubtask(ctx, subtask)
	require.ErrorContains(t, err, "occur an error when sort chunk")
	require.NotContains(t, err.Error(), "already exists")

	err = stepExecutor.RunSubtask(ctx, subtask)
	require.ErrorContains(t, err, "occur an error when sort chunk")
	require.NotContains(t, err.Error(), "already exists")
}

const queryStepTaskID = 1

// newQueryStepExecutor submits the IMPORT INTO statement sql like the
// executor does, and returns the executor of its query step, and the storage
// of global sort, which is a local directory.
func newQueryStepExecutor(t *testing.T, tk *testkit.TestKit, sql string) (execute.StepExecutor, storeapi.Storage) {
	ctx := context.Background()
	sortDir := t.TempDir()
	nodes, err := tk.Session().Parse(ctx, sql)
	require.NoError(t, err)
	require.NoError(t, executor.ResetContextOfStmt(tk.Session(), nodes[0]))
	require.NoError(t, tk.Session().PrepareTxnCtx(ctx, nodes[0]))
	stmt, err := (&executor.Compiler{Ctx: tk.Session()}).Compile(ctx, nodes[0])
	require.NoError(t, err)
	importPlan := stmt.Plan.(*core.ImportInto)
	dom, err := session.GetDomain(tk.Session().GetStore())
	require.NoError(t, err)
	tbl, err := dom.InfoSchema().TableByName(ctx, importPlan.Table.Schema, importPlan.Table.Name)
	require.NoError(t, err)
	plan, err := importer.NewImportPlan(ctx, tk.Session(), importPlan, tbl)
	require.NoError(t, err)
	plan.CloudStorageURI = sortDir
	plan.QuerySource, err = importer.CaptureQuerySource(tk.Session(), sql)
	require.NoError(t, err)

	meta, err := json.Marshal(importinto.TaskMeta{Plan: *plan, Stmt: sql})
	require.NoError(t, err)
	task := &proto.Task{
		TaskBase: proto.TaskBase{ID: queryStepTaskID, Type: proto.ImportInto, Step: proto.ImportStepQuery, RequiredSlots: 2},
		Meta:     meta,
	}
	param := taskexecutor.NewParamForTest(nil, nil, nil, ":4000")
	// The query runs in a session of the runtime, like on a worker.
	param.TaskRuntime = dom.GetRuntime()
	taskExecutor := importinto.NewImportExecutor(ctx, task, param)
	t.Cleanup(taskExecutor.Close)
	step, err := taskExecutor.(interface {
		GetStepExecutor(*proto.Task) (execute.StepExecutor, error)
	}).GetStepExecutor(task)
	require.NoError(t, err)
	execute.SetFrameworkInfo(step, task, &proto.StepResource{CPU: proto.NewAllocatable(2), Mem: proto.NewAllocatable(64 << 20)}, nil, nil)
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/dxf/importinto/createTableImporterForTest", `return(true)`)
	require.NoError(t, step.Init(ctx))
	t.Cleanup(func() { require.NoError(t, step.Cleanup(ctx)) })

	backend, err := objstore.ParseBackend(sortDir, nil)
	require.NoError(t, err)
	sortStore, err := objstore.NewWithDefaultOpt(ctx, backend)
	require.NoError(t, err)
	t.Cleanup(sortStore.Close)
	return step, sortStore
}

// newQuerySubtask returns a subtask of the query step, whose meta the planner
// puts in the storage of global sort.
func newQuerySubtask(t *testing.T, sortStore storeapi.Storage, id int64) *proto.Subtask {
	input := importinto.ImportStepMeta{ID: int32(id)}
	input.ExternalPath = globalsort.PlanMetaPath(queryStepTaskID, proto.Step2Str(proto.ImportInto, proto.ImportStepQuery), int(id))
	require.NoError(t, input.WriteJSONToExternalStorage(context.Background(), sortStore, &input))
	meta, err := json.Marshal(input)
	require.NoError(t, err)
	return &proto.Subtask{SubtaskBase: proto.SubtaskBase{ID: id, TaskID: queryStepTaskID}, Meta: meta}
}

func TestImportQueryStep(t *testing.T) {
	t.Cleanup(tidbconfig.RestoreFunc())
	tidbconfig.UpdateGlobal(func(c *tidbconfig.Config) { c.TempDir = t.TempDir() })
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table query_src(g bigint not null, v bigint not null, key idx_g(g))")
	tk.MustExec("insert into query_src values (1,10),(1,20),(2,30)")
	tk.MustExec("create table query_dst(g bigint primary key, c bigint, s decimal(42,0))")
	sql := "import into query_dst from (select /*+ STREAM_AGG() */ g, count(*), sum(v) from query_src use index(idx_g) group by g) with thread=2"
	require.NoError(t, importer.PlanImportQueryForTest(tk.Session(), sql))
	step, sortStore := newQueryStepExecutor(t, tk, sql)
	ctx := context.Background()

	t.Run("encode the query result", func(t *testing.T) {
		subtask := newQuerySubtask(t, sortStore, 1)
		require.NoError(t, step.RunSubtask(ctx, subtask))
		var output importinto.ImportStepMeta
		require.NoError(t, json.Unmarshal(subtask.Meta, &output))
		require.Equal(t, globalsort.SubtaskMetaPath(queryStepTaskID, subtask.ID), output.ExternalPath)
		require.NoError(t, output.ReadJSONFromExternalStorage(ctx, sortStore, &output))
		// One KV for each of the 2 groups.
		require.EqualValues(t, 2, output.SortedDataMeta.TotalKVCnt)
		// The step only encodes and sorts, it does not ingest.
		tk.MustQuery("select count(*) from query_dst").Check(testkit.Rows("0"))
	})

	t.Run("query failure", func(t *testing.T) {
		// The SELECT fails only when the worker runs it, after the capture.
		tk.MustExec("create table query_bad(g bigint not null, j varchar(16))")
		tk.MustExec("insert into query_bad values (1, 'not json')")
		tk.MustExec("create table query_bad_dst(g bigint primary key, j json)")
		badSQL := "import into query_bad_dst from (select g, cast(j as json) from query_bad) with thread=2"
		require.NoError(t, importer.PlanImportQueryForTest(tk.Session(), badSQL))
		badStep, badStore := newQueryStepExecutor(t, tk, badSQL)
		runCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		subtask := newQuerySubtask(t, badStore, 3)
		inputMeta := append([]byte(nil), subtask.Meta...)
		require.ErrorContains(t, badStep.RunSubtask(runCtx, subtask), "Invalid JSON text")
		require.Equal(t, inputMeta, subtask.Meta)
		exists, err := badStore.FileExists(ctx, globalsort.SubtaskMetaPath(queryStepTaskID, subtask.ID))
		require.NoError(t, err)
		require.False(t, exists)
	})

	// Many rows make the query send many chunks to the encoders.
	tk.MustExec("create table query_many(v bigint not null)")
	tk.MustExec("insert into query_many values (1)")
	for range 11 {
		tk.MustExec("insert into query_many select v + (select count(*) from query_many) from query_many")
	}
	tk.MustExec("create table query_many_dst(v bigint)")
	tk.MustExec("set tidb_max_chunk_size=32")
	defer tk.MustExec("set tidb_max_chunk_size=default")

	t.Run("encode many chunks", func(t *testing.T) {
		manySQL := "import into query_many_dst from (select v from query_many) with thread=2"
		require.NoError(t, importer.PlanImportQueryForTest(tk.Session(), manySQL))
		manyStep, manyStore := newQueryStepExecutor(t, tk, manySQL)
		subtask := newQuerySubtask(t, manyStore, 4)
		require.NoError(t, manyStep.RunSubtask(ctx, subtask))
		var output importinto.ImportStepMeta
		require.NoError(t, json.Unmarshal(subtask.Meta, &output))
		require.NoError(t, output.ReadJSONFromExternalStorage(ctx, manyStore, &output))
		require.EqualValues(t, 2048, output.SortedDataMeta.TotalKVCnt)
		// The rows of different chunks get different row IDs.
		eg, egCtx := util.NewErrorGroupWithRecoverWithCtx(ctx)
		handles := make(map[int64]struct{}, 2048)
		for kv := range globalsort.ReadKVFilesAsync(egCtx, eg, manyStore, output.SortedDataMeta.GetDataFiles()) {
			h, err := tablecodec.DecodeRowKey(kv.Key)
			require.NoError(t, err)
			handles[h.IntValue()] = struct{}{}
		}
		require.NoError(t, eg.Wait())
		require.Len(t, handles, 2048)
	})

	t.Run("empty query result", func(t *testing.T) {
		emptySQL := "import into query_many_dst from (select v from query_many where v < 0) with thread=2"
		require.NoError(t, importer.PlanImportQueryForTest(tk.Session(), emptySQL))
		emptyStep, emptyStore := newQueryStepExecutor(t, tk, emptySQL)
		subtask := newQuerySubtask(t, emptyStore, 5)
		require.NoError(t, emptyStep.RunSubtask(ctx, subtask))
		var output importinto.ImportStepMeta
		require.NoError(t, json.Unmarshal(subtask.Meta, &output))
		require.NoError(t, output.ReadJSONFromExternalStorage(ctx, emptyStore, &output))
		require.Zero(t, output.SortedDataMeta.TotalKVCnt)
	})

	t.Run("encode failure stops the query", func(t *testing.T) {
		manySQL := "import into query_many_dst from (select v from query_many) with thread=2"
		require.NoError(t, importer.PlanImportQueryForTest(tk.Session(), manySQL))
		manyStep, manyStore := newQueryStepExecutor(t, tk, manySQL)
		// The encoders fail before reading any chunk, so the query blocks on
		// sending until the pipeline is canceled.
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/dxf/importinto/errorWhenSortChunk", `return(true)`)
		subtask := newQuerySubtask(t, manyStore, 6)
		inputMeta := append([]byte(nil), subtask.Meta...)
		done := make(chan error, 1)
		go func() { done <- manyStep.RunSubtask(ctx, subtask) }()
		select {
		case err := <-done:
			require.ErrorContains(t, err, "occur an error when sort chunk")
		case <-time.After(10 * time.Second):
			require.FailNow(t, "the query step does not stop after the encoders fail")
		}
		// A failed subtask keeps its input meta and writes no output meta.
		require.Equal(t, inputMeta, subtask.Meta)
		exists, err := manyStore.FileExists(ctx, globalsort.SubtaskMetaPath(queryStepTaskID, subtask.ID))
		require.NoError(t, err)
		require.False(t, exists)
	})
}
