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

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/dxf/importinto"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
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
	taskExecutor := importinto.NewImportExecutor(
		context.Background(),
		task,
		taskexecutor.NewParamForTest(nil, nil, nil, ":4000", store),
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
