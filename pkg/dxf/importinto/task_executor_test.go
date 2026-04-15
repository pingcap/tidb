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

package importinto

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"testing"

	tidbconfig "github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/ingestor/engineapi"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

type StoreWithKS struct {
	kv.Storage
	ks string
}

func (s *StoreWithKS) GetKeyspace() string {
	return s.ks
}

func TestImportTaskExecutor(t *testing.T) {
	ctx := context.Background()
	executor := NewImportExecutor(
		ctx,
		&proto.Task{
			TaskBase: proto.TaskBase{ID: 1},
		},
		taskexecutor.NewParamForTest(nil, nil, nil, ":4000", &StoreWithKS{}),
	).(*importExecutor)

	require.NotNil(t, executor.BaseTaskExecutor.Extension)
	require.True(t, executor.IsIdempotent(&proto.Subtask{}))

	taskMeta := []byte(`{"Plan":{"TableInfo":{}}}`)
	for _, step := range []proto.Step{
		proto.ImportStepImport,
		proto.ImportStepEncodeAndSort,
		proto.ImportStepMergeSort,
		proto.ImportStepWriteAndIngest,
		proto.ImportStepPostProcess,
		proto.ImportStepCollectConflicts,
		proto.ImportStepConflictResolution,
	} {
		exe, err := executor.GetStepExecutor(&proto.Task{TaskBase: proto.TaskBase{Step: step}, Meta: taskMeta})
		require.NoError(t, err)
		require.NotNil(t, exe)
	}
	_, err := executor.GetStepExecutor(&proto.Task{TaskBase: proto.TaskBase{Step: proto.StepInit}, Meta: taskMeta})
	require.Error(t, err)
	_, err = executor.GetStepExecutor(&proto.Task{TaskBase: proto.TaskBase{Step: proto.ImportStepImport}, Meta: []byte("")})
	require.Error(t, err)
}

func TestTiCITaskIDForImportIntoUsesTaskKey(t *testing.T) {
	jobID := int64(12345)
	require.Equal(t, TaskKey(jobID), ticiTaskIDForImportInto(jobID))
}

func TestGetTableImporterSetsTiCITaskID(t *testing.T) {
	ctx := context.Background()
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	cfg := tidbconfig.GetGlobalConfig()
	originalTempDir := cfg.TempDir
	cfg.TempDir = t.TempDir()
	t.Cleanup(func() {
		cfg.TempDir = originalTempDir
	})

	tableInfo := &model.TableInfo{
		ID:    2,
		Name:  ast.NewCIStr("t"),
		State: model.StatePublic,
		Columns: []*model.ColumnInfo{{
			ID:        1,
			Name:      ast.NewCIStr("a"),
			Offset:    0,
			State:     model.StatePublic,
			FieldType: *types.NewFieldType(mysql.TypeLonglong),
		}},
	}

	path := filepath.Join(t.TempDir(), "input.csv")
	taskMeta := &TaskMeta{
		JobID: 12345,
		Plan: importer.Plan{
			DBID:             1,
			DBName:           "test",
			TableInfo:        tableInfo,
			DesiredTableInfo: tableInfo,
			Path:             path,
			Format:           importer.DataFormatCSV,
			InImportInto:     true,
			DataSourceType:   importer.DataSourceTypeFile,
		},
		Stmt: fmt.Sprintf("IMPORT INTO test.t FROM '%s'", path),
	}

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/dxf/importinto/createTableImporterForTest", `return(true)`)
	tableImporter, err := getTableImporter(ctx, 99, taskMeta, store, zap.NewNop())
	require.NoError(t, err)
	require.Equal(t, TaskKey(taskMeta.JobID), tableImporter.LoadDataController.TiDBTaskIDForTiCI)
	tableImporter.LoadDataController.Close()
	tableImporter.Backend().CloseEngineMgr()
}

func TestGetOnDupForKVGroup(t *testing.T) {
	t.Run("data-kv-group", func(t *testing.T) {
		onDup, err := getOnDupForKVGroup(nil, external.DataKVGroup)
		require.NoError(t, err)
		require.Equal(t, engineapi.OnDuplicateKeyRecord, onDup)
	})

	indicesGenKV := map[int64]importer.GenKVIndex{
		1: {Unique: true},
		2: {Unique: false},
	}

	t.Run("unique-index", func(t *testing.T) {
		onDup, err := getOnDupForKVGroup(indicesGenKV, external.IndexID2KVGroup(1))
		require.NoError(t, err)
		require.Equal(t, engineapi.OnDuplicateKeyRecord, onDup)
	})

	t.Run("non-unique-index", func(t *testing.T) {
		onDup, err := getOnDupForKVGroup(indicesGenKV, external.IndexID2KVGroup(2))
		require.NoError(t, err)
		require.Equal(t, engineapi.OnDuplicateKeyRemove, onDup)
	})

	t.Run("unknown-index", func(t *testing.T) {
		onDup, err := getOnDupForKVGroup(indicesGenKV, external.IndexID2KVGroup(3))
		require.Error(t, err)
		require.Equal(t, engineapi.OnDuplicateKeyIgnore, onDup)
		require.ErrorContains(t, err, "unknown index 3")
	})

	t.Run("invalid-kv-group", func(t *testing.T) {
		onDup, err := getOnDupForKVGroup(indicesGenKV, "not-a-number")
		require.Error(t, err)
		require.Equal(t, engineapi.OnDuplicateKeyIgnore, onDup)
	})
}

func TestDecideTiCIWriteEnabled(t *testing.T) {
	makePlan := func(withFullText bool) *importer.Plan {
		indexInfo := &model.IndexInfo{
			ID:   101,
			Name: ast.NewCIStr("idx_fulltext"),
		}
		if withFullText {
			indexInfo.FullTextInfo = &model.FullTextIndexInfo{}
		}
		tableInfo := &model.TableInfo{
			ID:      1,
			Name:    ast.NewCIStr("t"),
			Indices: []*model.IndexInfo{indexInfo},
		}
		return &importer.Plan{
			DBName:    "test",
			TableInfo: tableInfo,
		}
	}

	t.Run("tici index enabled logs details", func(t *testing.T) {
		core, recorded := observer.New(zap.InfoLevel)
		logger := zap.New(core)
		plan := makePlan(true)

		enabled := decideTiCIWriteEnabled(logger, 10, 20, strconv.FormatInt(101, 10), plan)
		require.True(t, enabled)

		entries := recorded.FilterMessage("TiCI write decision for index engine").All()
		require.Len(t, entries, 1)
		fields := entries[0].ContextMap()
		require.Equal(t, int64(10), fields["task-id"])
		require.Equal(t, int64(20), fields["subtask-id"])
		require.Equal(t, "test", fields["schema-name"])
		require.Equal(t, "t", fields["table-name"])
		require.Equal(t, "idx_fulltext", fields["index-name"])
		require.Equal(t, true, fields["tici-write-enabled"])
	})

	t.Run("non tici index logs disabled", func(t *testing.T) {
		core, recorded := observer.New(zap.InfoLevel)
		logger := zap.New(core)
		plan := makePlan(false)

		enabled := decideTiCIWriteEnabled(logger, 10, 21, strconv.FormatInt(101, 10), plan)
		require.False(t, enabled)

		entries := recorded.FilterMessage("TiCI write decision for index engine").All()
		require.Len(t, entries, 1)
		fields := entries[0].ContextMap()
		require.Equal(t, false, fields["tici-write-enabled"])
	})
}
