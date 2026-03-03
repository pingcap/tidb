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
	stderrors "errors"
	"testing"

	"github.com/docker/go-units"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/ingestor/engineapi"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
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

func newImportStepExecutorForEstimateTest(cpuCapacity, memCapacity int64) *importStepExecutor {
	s := &importStepExecutor{
		taskMeta:         &TaskMeta{},
		concurrency:      int(cpuCapacity),
		BaseStepExecutor: taskexecutor.BaseStepExecutor{},
		logger:           zap.NewNop(),
	}
	execute.SetFrameworkInfo(
		s,
		&proto.Task{TaskBase: proto.TaskBase{ID: 1, Step: proto.ImportStepImport}},
		&proto.StepResource{
			CPU: proto.NewAllocatable(cpuCapacity),
			Mem: proto.NewAllocatable(memCapacity),
		},
		nil,
		nil,
	)
	return s
}

func TestEstimateAndSetConcurrency(t *testing.T) {
	oldEstimator := estimateParquetReaderMemory
	t.Cleanup(func() {
		estimateParquetReaderMemory = oldEstimator
	})

	store := objstore.NewMemStorage()
	t.Cleanup(store.Close)
	chunks := []importer.Chunk{
		{Path: "small.parquet", FileSize: 1, Type: mydump.SourceTypeParquet},
		{Path: "large.parquet", FileSize: 2, Type: mydump.SourceTypeParquet},
	}

	t.Run("estimator error fallback", func(t *testing.T) {
		var path string
		estimateParquetReaderMemory = func(_ context.Context, _ storeapi.Storage, p string) (int64, error) {
			path = p
			return 0, stderrors.New("mock estimate error")
		}

		exec := newImportStepExecutorForEstimateTest(8, int64(16*units.GiB))
		exec.estimateAndSetConcurrency(context.Background(), store, chunks)
		require.Equal(t, "large.parquet", path)
		require.Equal(t, 8, exec.concurrency)
	})

	t.Run("peak memory non-positive", func(t *testing.T) {
		estimateParquetReaderMemory = func(_ context.Context, _ storeapi.Storage, _ string) (int64, error) {
			return 0, nil
		}

		exec := newImportStepExecutorForEstimateTest(8, int64(16*units.GiB))
		exec.estimateAndSetConcurrency(context.Background(), store, chunks)
		require.Equal(t, 8, exec.concurrency)
	})

	t.Run("downscale concurrency by memory budget", func(t *testing.T) {
		const peakMem = int64(3 * units.GiB)
		estimateParquetReaderMemory = func(_ context.Context, _ storeapi.Storage, p string) (int64, error) {
			require.Equal(t, "large.parquet", p)
			return peakMem, nil
		}

		totalMem := int64(16 * units.GiB)
		exec := newImportStepExecutorForEstimateTest(8, totalMem)
		exec.estimateAndSetConcurrency(context.Background(), store, chunks)

		expected := max(1, int((float64(totalMem)*readerMemBudgetRatio)/float64(peakMem)))
		require.Equal(t, expected, exec.concurrency)
	})
}

func TestEstimateAndSetConcurrencyOnce(t *testing.T) {
	oldEstimator := estimateParquetReaderMemory
	t.Cleanup(func() {
		estimateParquetReaderMemory = oldEstimator
	})

	callCount := 0
	estimateParquetReaderMemory = func(_ context.Context, _ storeapi.Storage, path string) (int64, error) {
		callCount++
		require.Equal(t, "largest-task-file.parquet", path)
		return int64(4 * units.GiB), nil
	}

	store := objstore.NewMemStorage()
	t.Cleanup(store.Close)

	exec := newImportStepExecutorForEstimateTest(8, int64(64*units.GiB))
	exec.taskMeta.ChunkMap = map[int32][]importer.Chunk{
		1: {
			{Path: "small-subtask-file.parquet", FileSize: 1, Type: mydump.SourceTypeParquet},
		},
		2: {
			{Path: "largest-task-file.parquet", FileSize: 9, Type: mydump.SourceTypeParquet},
		},
	}

	exec.estimateAndSetConcurrencyOnce(context.Background(), store, []importer.Chunk{
		{Path: "small-subtask-file.parquet", FileSize: 1, Type: mydump.SourceTypeParquet},
	})
	exec.estimateAndSetConcurrencyOnce(context.Background(), store, []importer.Chunk{
		{Path: "ignored.parquet", FileSize: 10, Type: mydump.SourceTypeParquet},
	})

	require.Equal(t, 1, callCount)
	require.Equal(t, 4, exec.concurrency)
}
