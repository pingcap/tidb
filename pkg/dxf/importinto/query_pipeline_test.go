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
	"os"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/dxf/framework/planner"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/dxf/importinto"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/ingestor/simplesst"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	"golang.org/x/sync/errgroup"
)

func TestImportQueryEncodeS3(t *testing.T) {
	uri := os.Getenv("TIDB_IMPORT_QUERY_S3_URI")
	if uri == "" {
		t.Skip("requires TIDB_IMPORT_QUERY_S3_URI")
	}
	t.Cleanup(config.RestoreFunc())
	config.UpdateGlobal(func(c *config.Config) { c.TempDir = t.TempDir() })
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table query_src(g bigint not null,v bigint not null)")
	tk.MustExec("insert into query_src values (1,10),(1,20),(2,30)")
	tk.MustExec("analyze table query_src all columns")
	tk.MustExec("create table query_dst(g bigint primary key,c bigint,s decimal(42,0))")
	previousURI := vardef.CloudStorageURI.Load()
	vardef.CloudStorageURI.Store(uri)
	t.Cleanup(func() { vardef.CloudStorageURI.Store(previousURI) })
	sql := "import into query_dst from (select g,count(*),sum(v) from query_src group by g) with thread=1"
	ctx := context.Background()
	nodes, err := tk.Session().Parse(ctx, sql)
	require.NoError(t, err)
	require.NoError(t, executor.ResetContextOfStmt(tk.Session(), nodes[0]))
	require.NoError(t, tk.Session().PrepareTxnCtx(ctx, nodes[0]))
	stmt, err := (&executor.Compiler{Ctx: tk.Session()}).Compile(ctx, nodes[0])
	require.NoError(t, err)
	logical := stmt.Plan.(*core.ImportInto)
	tbl, err := dom.InfoSchema().TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("query_dst"))
	require.NoError(t, err)
	plan, err := importer.NewImportPlan(ctx, tk.Session(), logical, tbl)
	require.NoError(t, err)
	plan.Query, _, err = executor.CaptureImportQuery(tk.Session(), sql)
	require.NoError(t, err)
	meta, err := json.Marshal(importinto.TaskMeta{Plan: *plan, Stmt: sql})
	require.NoError(t, err)
	task := &proto.Task{TaskBase: proto.TaskBase{ID: 891, Type: proto.ImportInto, Step: proto.ImportStepQuery, RequiredSlots: 1}, Meta: meta}
	param := taskexecutor.NewParamForTest(nil, nil, nil, ":4000")
	param.TaskRuntime = dom.GetRuntime()
	taskExecutor := importinto.NewImportExecutor(ctx, task, param)
	defer taskExecutor.Close()
	factory := taskExecutor.(interface {
		GetStepExecutor(*proto.Task) (execute.StepExecutor, error)
	})
	step, err := factory.GetStepExecutor(task)
	require.NoError(t, err)
	execute.SetFrameworkInfo(step, task, &proto.StepResource{CPU: proto.NewAllocatable(1), Mem: proto.NewAllocatable(64 << 20)}, nil, nil)
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/dxf/importinto/createTableImporterForTest", `return(true)`)
	require.NoError(t, step.Init(ctx))
	defer func() { require.NoError(t, step.Cleanup(ctx)) }()
	backend, err := objstore.ParseBackend(plan.CloudStorageURI, nil)
	require.NoError(t, err)
	s3, err := objstore.NewWithDefaultOpt(ctx, backend)
	require.NoError(t, err)
	defer s3.Close()
	input := importinto.ImportStepMeta{ID: 1}
	input.ExternalPath = "891/plan/query/1/meta.json"
	require.NoError(t, input.WriteJSONToExternalStorage(ctx, s3, &input))
	subMeta, err := json.Marshal(input)
	require.NoError(t, err)
	sub := &proto.Subtask{SubtaskBase: proto.SubtaskBase{ID: 1, TaskID: 891}, Meta: subMeta}
	require.NoError(t, step.RunSubtask(ctx, sub))
	var result importinto.ImportStepMeta
	require.NoError(t, json.Unmarshal(sub.Meta, &result))
	require.Equal(t, "891/1/meta.json", result.ExternalPath)
	require.NoError(t, result.ReadJSONFromExternalStorage(ctx, s3, &result))
	require.NotNil(t, result.SortedDataMeta)
	require.EqualValues(t, 2, result.SortedDataMeta.TotalKVCnt)
	// Encoding alone must not ingest anything into the destination.
	tk.MustQuery("select count(*) from query_dst").Check(testkit.Rows("0"))

	for i, tc := range []struct{ name, failpoint, message string }{
		{"query failure", "github.com/pingcap/tidb/pkg/executor/failAfterImportQueryOptimize", "injected failure after import query optimization"},
		{"encode failure", "github.com/pingcap/tidb/pkg/dxf/importinto/errorWhenSortChunk", "occur an error when sort chunk"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testfailpoint.Enable(t, tc.failpoint, `return(true)`)
			runCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			failed := &proto.Subtask{SubtaskBase: proto.SubtaskBase{ID: int64(i + 2), TaskID: 891}, Meta: append([]byte(nil), subMeta...)}
			require.ErrorContains(t, step.RunSubtask(runCtx, failed), tc.message)
			require.Equal(t, subMeta, failed.Meta)
			exists, err := s3.FileExists(ctx, fmt.Sprintf("891/%d/meta.json", failed.ID))
			require.NoError(t, err)
			require.False(t, exists)
		})
	}

	// Statistics loading errors must abort the Query step before ingest.
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/statistics/handle/util/ExecRowsTimeout", `return(true)`)
	sub.Meta = subMeta
	require.ErrorContains(t, step.RunSubtask(ctx, sub), "inject timeout error")
	tk.MustQuery("select count(*) from query_dst").Check(testkit.Rows("0"))
}

func TestImportQueryRangesEncodeS3(t *testing.T) {
	uri := os.Getenv("TIDB_IMPORT_QUERY_S3_URI")
	if !kerneltype.IsNextGen() || uri == "" {
		t.Skip("requires nextgen and TIDB_IMPORT_QUERY_S3_URI")
	}
	t.Cleanup(config.RestoreFunc())
	config.UpdateGlobal(func(c *config.Config) { c.TempDir = t.TempDir() })
	var cluster testutils.Cluster
	store, dom := testkit.CreateMockStoreAndDomain(t, mockstore.WithClusterInspector(func(c testutils.Cluster) {
		mockstore.BootstrapWithSingleStore(c)
		cluster = c
	}))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_isolation_read_engines='tikv'")
	tk.MustExec("create table range_src(id bigint primary key clustered,v bigint)")
	tk.MustExec("insert into range_src values (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8)")
	source, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("range_src"))
	require.NoError(t, err)
	start := tablecodec.GenTableRecordPrefix(source.Meta().ID)
	cluster.SplitKeys(store.GetCodec().EncodeKey(start), store.GetCodec().EncodeKey(start.PrefixNext()), 4)
	tk.MustExec("analyze table range_src all columns")
	tk.MustExec("create table range_dst(id bigint primary key nonclustered,v bigint)")
	old := vardef.CloudStorageURI.Load()
	vardef.CloudStorageURI.Store(uri)
	t.Cleanup(func() { vardef.CloudStorageURI.Store(old) })
	sql := "import into range_dst from (select id,v+1 from range_src where v>=2) with thread=1"
	ctx := context.Background()
	nodes, err := tk.Session().Parse(ctx, sql)
	require.NoError(t, err)
	require.NoError(t, executor.ResetContextOfStmt(tk.Session(), nodes[0]))
	require.NoError(t, tk.Session().PrepareTxnCtx(ctx, nodes[0]))
	stmt, err := (&executor.Compiler{Ctx: tk.Session()}).Compile(ctx, nodes[0])
	require.NoError(t, err)
	logical := stmt.Plan.(*core.ImportInto)
	tbl, err := dom.InfoSchema().TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("range_dst"))
	require.NoError(t, err)
	plan, err := importer.NewImportPlan(ctx, tk.Session(), logical, tbl)
	require.NoError(t, err)
	var queryNode ast.StmtNode
	plan.Query, queryNode, err = executor.CaptureImportQuery(tk.Session(), sql)
	require.NoError(t, err)
	require.NoError(t, executor.PrepareImportQueryRanges(ctx, tk.Session(), queryNode, logical.SelectPlan, plan))
	require.NotNil(t, plan.Query.Scan)
	lp := &importinto.LogicalPlan{Plan: *plan, Stmt: sql}
	physical, err := lp.ToPhysicalPlan(planner.PlanCtx{SourceStep: proto.ImportStepQuery, Ctx: ctx, TaskID: 892, GlobalSort: true, NextTaskStep: proto.ImportStepQuery})
	require.NoError(t, err)
	require.Greater(t, len(physical.Processors), 1)
	meta, err := lp.ToTaskMeta()
	require.NoError(t, err)
	task := &proto.Task{TaskBase: proto.TaskBase{ID: 892, Type: proto.ImportInto, Step: proto.ImportStepQuery, RequiredSlots: 1}, Meta: meta}
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/dxf/importinto/createTableImporterForTest", `return(true)`)
	workers := make([]execute.StepExecutor, plan.MaxNodeCnt)
	for i := range workers {
		param := taskexecutor.NewParamForTest(nil, nil, nil, fmt.Sprintf(":%d", 4000+i))
		param.TaskRuntime = dom.GetRuntime()
		te := importinto.NewImportExecutor(ctx, task, param)
		t.Cleanup(te.Close)
		factory := te.(interface {
			GetStepExecutor(*proto.Task) (execute.StepExecutor, error)
		})
		workers[i], err = factory.GetStepExecutor(task)
		require.NoError(t, err)
		execute.SetFrameworkInfo(workers[i], task, &proto.StepResource{CPU: proto.NewAllocatable(1), Mem: proto.NewAllocatable(64 << 20)}, nil, nil)
		require.NoError(t, workers[i].Init(ctx))
		t.Cleanup(func() { require.NoError(t, workers[i].Cleanup(ctx)) })
	}
	subtasks := make([]*proto.Subtask, len(physical.Processors))
	for i, p := range physical.Processors {
		data, err := p.Pipeline.ToSubtaskMeta(planner.PlanCtx{SourceStep: proto.ImportStepQuery, GlobalSort: true})
		require.NoError(t, err)
		subtasks[i] = &proto.Subtask{SubtaskBase: proto.SubtaskBase{ID: int64(i + 1), TaskID: 892}, Meta: data}
	}
	var group errgroup.Group
	for i, worker := range workers {
		group.Go(func() error {
			for j := i; j < len(subtasks); j += len(workers) {
				if err := worker.RunSubtask(ctx, subtasks[j]); err != nil {
					return err
				}
			}
			return nil
		})
	}
	require.NoError(t, group.Wait())
	s3, err := importer.GetSortStore(ctx, plan.CloudStorageURI)
	require.NoError(t, err)
	defer s3.Close()
	var files []string
	var total uint64
	for _, sub := range subtasks {
		var result importinto.ImportStepMeta
		require.NoError(t, json.Unmarshal(sub.Meta, &result))
		require.NoError(t, result.ReadJSONFromExternalStorage(ctx, s3, &result))
		total += result.SortedDataMeta.TotalKVCnt
		files = append(files, result.SortedDataMeta.GetDataFiles()...)
	}
	require.EqualValues(t, 7, total)
	iter, err := simplesst.NewMergeKVIter(ctx, files, make([]uint64, len(files)), s3, simplesst.DefaultReadBufferSize, false, 1)
	require.NoError(t, err)
	defer func() { require.NoError(t, iter.Close()) }()
	seen := make(map[string]bool)
	for iter.Next() {
		key := string(iter.Key())
		require.False(t, seen[key])
		seen[key] = true
	}
	require.NoError(t, iter.Error())
	require.Len(t, seen, 7)
	tk.MustQuery("select count(*) from range_dst").Check(testkit.Rows("0"))
}
