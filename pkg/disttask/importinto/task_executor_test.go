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
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/ngaut/pools"
	tidbconfig "github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestImportTaskExecutor(t *testing.T) {
	ctx := context.Background()
	executor := NewImportExecutor(
		ctx,
		&proto.Task{
			TaskBase: proto.TaskBase{ID: 1},
		},
		taskexecutor.NewParamForTest(nil, nil, nil, ":4000"),
		nil,
	).(*importExecutor)

	require.NotNil(t, executor.BaseTaskExecutor.Extension)
	require.True(t, executor.IsIdempotent(&proto.Subtask{}))

	taskMeta := `{"Plan": {"TableInfo": {}}}`
	for _, step := range []proto.Step{
		proto.ImportStepImport,
		proto.ImportStepEncodeAndSort,
		proto.ImportStepMergeSort,
		proto.ImportStepWriteAndIngest,
		proto.ImportStepPostProcess,
	} {
		exe, err := executor.GetStepExecutor(&proto.Task{TaskBase: proto.TaskBase{Step: step}, Meta: []byte(taskMeta)})
		require.NoError(t, err)
		require.NotNil(t, exe)
	}
	_, err := executor.GetStepExecutor(&proto.Task{TaskBase: proto.TaskBase{Step: proto.StepInit}, Meta: []byte(taskMeta)})
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
		Name:  pmodel.NewCIStr("t"),
		State: model.StatePublic,
		Columns: []*model.ColumnInfo{{
			ID:        1,
			Name:      pmodel.NewCIStr("a"),
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

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/disttask/importinto/createTableImporterForTest", `return(true)`)
	tableImporter, err := getTableImporter(ctx, 99, taskMeta, store)
	require.NoError(t, err)
	require.Equal(t, TaskKey(taskMeta.JobID), tableImporter.LoadDataController.TiDBTaskIDForTiCI)
	tableImporter.Backend().CloseEngineMgr()
}

func TestDecideTiCIWriteEnabled(t *testing.T) {
	makePlan := func(withFullText bool) *importer.Plan {
		indexInfo := &model.IndexInfo{
			ID:   101,
			Name: pmodel.NewCIStr("idx_fulltext"),
		}
		if withFullText {
			indexInfo.FullTextInfo = &model.FullTextIndexInfo{}
		}
		tableInfo := &model.TableInfo{
			ID:      1,
			Name:    pmodel.NewCIStr("t"),
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

func TestFinishTiCIIndexUploadForPostProcess(t *testing.T) {
	originFinishTiCIIndexUpload := finishTiCIIndexUpload
	t.Cleanup(func() {
		finishTiCIIndexUpload = originFinishTiCIIndexUpload
	})

	makePlan := func(withTiCI bool) *importer.Plan {
		indexInfo := &model.IndexInfo{
			ID:   101,
			Name: pmodel.NewCIStr("idx_fulltext"),
		}
		if withTiCI {
			indexInfo.FullTextInfo = &model.FullTextIndexInfo{}
		}
		tableInfo := &model.TableInfo{
			ID:         1,
			Name:       pmodel.NewCIStr("t"),
			PKIsHandle: true,
			Indices:    []*model.IndexInfo{indexInfo},
		}
		return &importer.Plan{
			DBName:           "test",
			TableInfo:        tableInfo,
			DesiredTableInfo: tableInfo,
		}
	}

	finishErr := errors.New("finish failed")
	jobID := int64(456)
	tests := []struct {
		name        string
		plan        *importer.Plan
		finishErr   error
		wantTaskIDs []string
		wantWarn    bool
		wantInfo    bool
	}{
		{
			name: "nil plan skips finish",
		},
		{
			name: "nil table info skips finish",
			plan: &importer.Plan{},
		},
		{
			name: "no tici index skips finish",
			plan: makePlan(false),
		},
		{
			name:        "tici index finishes upload",
			plan:        makePlan(true),
			wantTaskIDs: []string{TaskKey(jobID)},
			wantInfo:    true,
		},
		{
			name:        "finish failure only warns",
			plan:        makePlan(true),
			finishErr:   finishErr,
			wantTaskIDs: []string{TaskKey(jobID)},
			wantWarn:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotTaskIDs []string
			finishTiCIIndexUpload = func(_ context.Context, _ kv.Storage, taskID string) error {
				gotTaskIDs = append(gotTaskIDs, taskID)
				return tt.finishErr
			}
			core, recorded := observer.New(zap.DebugLevel)
			logger := zap.New(core)

			finishTiCIIndexUploadForPostProcess(context.Background(), nil, 123, jobID, tt.plan, logger)

			require.Equal(t, tt.wantTaskIDs, gotTaskIDs)
			warns := recorded.FilterMessage("failed to finish TiCI index upload for post process").All()
			if tt.wantWarn {
				require.Len(t, warns, 1)
				fields := warns[0].ContextMap()
				require.Equal(t, int64(123), fields["task-id"])
				require.Equal(t, []any{int64(101)}, fields["tici-index-ids"])
				require.Equal(t, finishErr.Error(), fields["error"])
			} else {
				require.Empty(t, warns)
			}
			infos := recorded.FilterMessage("finished TiCI index upload for post process").All()
			if tt.wantInfo {
				require.Len(t, infos, 1)
				fields := infos[0].ContextMap()
				require.Equal(t, int64(123), fields["task-id"])
				require.Equal(t, []any{int64(101)}, fields["tici-index-ids"])
			} else {
				require.Empty(t, infos)
			}
		})
	}
}

func TestPostProcessTiCIFinishFailureDoesNotAbort(t *testing.T) {
	originFinishTiCIIndexUpload := finishTiCIIndexUpload
	t.Cleanup(func() {
		finishTiCIIndexUpload = originFinishTiCIIndexUpload
	})
	finishTiCIIndexUpload = func(_ context.Context, _ kv.Storage, _ string) error {
		return errors.New("finish failed")
	}

	indexInfo := &model.IndexInfo{
		ID:           101,
		Name:         pmodel.NewCIStr("idx_fulltext"),
		FullTextInfo: &model.FullTextIndexInfo{},
		State:        model.StatePublic,
	}
	tableInfo := &model.TableInfo{
		ID:         1,
		Name:       pmodel.NewCIStr("t"),
		PKIsHandle: true,
		Indices:    []*model.IndexInfo{indexInfo},
	}
	core, recorded := observer.New(zap.DebugLevel)
	logger := zap.New(core)
	taskMeta := &TaskMeta{JobID: 456, Plan: importer.Plan{
		DBName: "test", TableInfo: tableInfo, DesiredTableInfo: tableInfo,
	}}
	err := postProcess(context.Background(), 123, nil, taskMeta, &PostProcessStepMeta{TooManyConflictsFromIndex: true}, logger)
	require.NoError(t, err)
	require.Len(t, recorded.FilterMessage("failed to finish TiCI index upload for post process").All(), 1)

	pool := pools.NewResourcePool(func() (pools.Resource, error) { return mock.NewContext(), nil }, 1, 1, time.Second)
	t.Cleanup(pool.Close)
	previous, _ := storage.GetTaskManager()
	storage.SetTaskManager(storage.NewTaskManager(pool))
	t.Cleanup(func() { storage.SetTaskManager(previous) })
	core, recorded = observer.New(zap.DebugLevel)
	logger = zap.New(core)
	taskMeta.Plan.Checksum = config.OpLevelOff
	err = postProcess(context.Background(), 123, nil, taskMeta, &PostProcessStepMeta{}, logger)
	require.NoError(t, err)
	require.Len(t, recorded.FilterMessage("failed to finish TiCI index upload for post process").All(), 1)
}
