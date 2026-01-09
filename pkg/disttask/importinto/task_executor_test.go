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
	"strconv"
	"testing"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

type StoreWithoutKS struct {
	kv.Storage
}

func (*StoreWithoutKS) GetKeyspace() string {
	return ""
}

func TestImportTaskExecutor(t *testing.T) {
	ctx := context.Background()
	executor := NewImportExecutor(
		ctx,
		&proto.Task{
			TaskBase: proto.TaskBase{ID: 1},
		},
		taskexecutor.NewParamForTest(nil, nil, nil, ":4000", &StoreWithoutKS{}),
	).(*importExecutor)

	require.NotNil(t, executor.BaseTaskExecutor.Extension)
	require.True(t, executor.IsIdempotent(&proto.Subtask{}))

	for _, step := range []proto.Step{
		proto.ImportStepImport,
		proto.ImportStepEncodeAndSort,
		proto.ImportStepMergeSort,
		proto.ImportStepWriteAndIngest,
		proto.ImportStepPostProcess,
	} {
		exe, err := executor.GetStepExecutor(&proto.Task{TaskBase: proto.TaskBase{Step: step}, Meta: []byte("{}")})
		require.NoError(t, err)
		require.NotNil(t, exe)
	}
	_, err := executor.GetStepExecutor(&proto.Task{TaskBase: proto.TaskBase{Step: proto.StepInit}, Meta: []byte("{}")})
	require.Error(t, err)
	_, err = executor.GetStepExecutor(&proto.Task{TaskBase: proto.TaskBase{Step: proto.ImportStepImport}, Meta: []byte("")})
	require.Error(t, err)
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
