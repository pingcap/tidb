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
	"strconv"
	"testing"

	frameworkmock "github.com/pingcap/tidb/pkg/dxf/framework/mock"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/ingestor/engineapi"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
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

func TestFinishTiCIIndexUploadForPostProcess(t *testing.T) {
	originFinishTiCIIndexUpload := finishTiCIIndexUpload
	t.Cleanup(func() {
		finishTiCIIndexUpload = originFinishTiCIIndexUpload
	})

	makePlan := func(withTiCI bool) *importer.Plan {
		indexInfo := &model.IndexInfo{
			ID:   101,
			Name: ast.NewCIStr("idx_fulltext"),
		}
		if withTiCI {
			indexInfo.FullTextInfo = &model.FullTextIndexInfo{}
		}
		tableInfo := &model.TableInfo{
			ID:         1,
			Name:       ast.NewCIStr("t"),
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
			wantTaskIDs: []string{"123"},
			wantInfo:    true,
		},
		{
			name:        "finish failure only warns",
			plan:        makePlan(true),
			finishErr:   finishErr,
			wantTaskIDs: []string{"123"},
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

			finishTiCIIndexUploadForPostProcess(context.Background(), nil, 123, tt.plan, logger)

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
		Name:         ast.NewCIStr("idx_fulltext"),
		FullTextInfo: &model.FullTextIndexInfo{},
		State:        model.StatePublic,
	}
	tableInfo := &model.TableInfo{
		ID:         1,
		Name:       ast.NewCIStr("t"),
		PKIsHandle: true,
		Indices:    []*model.IndexInfo{indexInfo},
	}
	core, recorded := observer.New(zap.DebugLevel)
	logger := zap.New(core)
	executor := &postProcessStepExecutor{
		taskID: 123,
		taskMeta: &TaskMeta{
			Plan: importer.Plan{
				DBName:           "test",
				TableInfo:        tableInfo,
				DesiredTableInfo: tableInfo,
			},
		},
		logger: logger,
	}

	err := executor.postProcess(context.Background(), &PostProcessStepMeta{TooManyConflictsFromIndex: true}, logger)
	require.NoError(t, err)
	require.Len(t, recorded.FilterMessage("failed to finish TiCI index upload for post process").All(), 1)

	ctrl := gomock.NewController(t)
	taskTbl := frameworkmock.NewMockTaskTable(ctrl)
	taskTbl.EXPECT().WithNewSession(gomock.Any()).AnyTimes().DoAndReturn(func(fn func(sessionctx.Context) error) error {
		return fn(nil)
	})
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/dxf/importinto/skipPostProcessAlterTableMode", "return(true)")
	core, recorded = observer.New(zap.DebugLevel)
	logger = zap.New(core)
	executor.taskTbl = taskTbl
	executor.logger = logger
	executor.taskMeta.Plan.Checksum = config.OpLevelOff

	err = executor.postProcess(context.Background(), &PostProcessStepMeta{}, logger)
	require.NoError(t, err)
	require.Len(t, recorded.FilterMessage("failed to finish TiCI index upload for post process").All(), 1)
}
