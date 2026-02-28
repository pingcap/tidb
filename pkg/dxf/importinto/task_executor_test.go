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
	"testing"

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

func TestMaybeEstimateAndSetConcurrencyRetryOnError(t *testing.T) {
	originalEstimator := estimateParquetReaderMemory
	t.Cleanup(func() {
		estimateParquetReaderMemory = originalEstimator
	})

	var estimateCalls int
	estimateParquetReaderMemory = func(context.Context, storeapi.Storage, string) (int64, error) {
		estimateCalls++
		if estimateCalls == 1 {
			return 0, errors.New("transient parquet read error")
		}
		return 10, nil
	}

	store, err := objstore.NewLocalStorage(t.TempDir())
	require.NoError(t, err)
	exec := &importStepExecutor{
		logger:      zap.NewNop(),
		concurrency: 8,
	}
	execute.SetFrameworkInfo(exec, &proto.Task{TaskBase: proto.TaskBase{ID: 1}}, &proto.StepResource{
		CPU: proto.NewAllocatable(8),
		Mem: proto.NewAllocatable(100),
	}, nil, nil)

	chunks := []importer.Chunk{{
		Path:     "sample.parquet",
		FileSize: 1,
		Type:     mydump.SourceTypeParquet,
	}}

	exec.maybeEstimateAndSetConcurrency(context.Background(), store, chunks)
	require.Equal(t, 1, estimateCalls)
	require.False(t, exec.estimateConcDone)
	require.Equal(t, 8, exec.concurrency)

	exec.maybeEstimateAndSetConcurrency(context.Background(), store, chunks)
	require.Equal(t, 2, estimateCalls)
	require.True(t, exec.estimateConcDone)
	require.Equal(t, 3, exec.concurrency)

	exec.maybeEstimateAndSetConcurrency(context.Background(), store, chunks)
	require.Equal(t, 2, estimateCalls)
}

func TestEstimateAndSetConcurrencyChooseLargestChunk(t *testing.T) {
	originalEstimator := estimateParquetReaderMemory
	t.Cleanup(func() {
		estimateParquetReaderMemory = originalEstimator
	})

	var estimatedPath string
	estimateParquetReaderMemory = func(_ context.Context, _ storeapi.Storage, path string) (int64, error) {
		estimatedPath = path
		return 10, nil
	}

	store, err := objstore.NewLocalStorage(t.TempDir())
	require.NoError(t, err)
	exec := &importStepExecutor{
		logger:      zap.NewNop(),
		concurrency: 8,
	}
	execute.SetFrameworkInfo(exec, &proto.Task{TaskBase: proto.TaskBase{ID: 1}}, &proto.StepResource{
		CPU: proto.NewAllocatable(8),
		Mem: proto.NewAllocatable(100),
	}, nil, nil)

	done := exec.estimateAndSetConcurrency(context.Background(), store, []importer.Chunk{
		{Path: "small.parquet", FileSize: 1, Type: mydump.SourceTypeParquet},
		{Path: "large.parquet", FileSize: 10, Type: mydump.SourceTypeParquet},
	})
	require.True(t, done)
	require.Equal(t, "large.parquet", estimatedPath)
}
