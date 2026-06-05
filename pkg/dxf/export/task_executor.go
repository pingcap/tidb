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

package export

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

type exportTaskExecutor struct {
	*taskexecutor.BaseTaskExecutor
	store kv.Storage
}

var _ taskexecutor.TaskExecutor = (*exportTaskExecutor)(nil)

// NewExportTaskExecutor creates a task executor for the export task.
func NewExportTaskExecutor(ctx context.Context, task *proto.Task, param taskexecutor.Param) taskexecutor.TaskExecutor {
	e := &exportTaskExecutor{
		BaseTaskExecutor: taskexecutor.NewBaseTaskExecutor(ctx, task, param),
		store:            param.Store,
	}
	e.BaseTaskExecutor.Extension = e
	return e
}

// IsIdempotent implements taskexecutor.Extension. A fixed range at a fixed
// snapshot TS produces the same files, so a retry overwrites them.
func (*exportTaskExecutor) IsIdempotent(*proto.Subtask) bool {
	return true
}

// IsRetryableError implements taskexecutor.Extension.
func (*exportTaskExecutor) IsRetryableError(error) bool {
	return false
}

// GetStepExecutor implements taskexecutor.Extension.
func (e *exportTaskExecutor) GetStepExecutor(task *proto.Task) (execute.StepExecutor, error) {
	taskMeta := &TaskMeta{}
	if err := json.Unmarshal(task.Meta, taskMeta); err != nil {
		return nil, errors.Annotate(err, "unmarshal export task meta failed")
	}
	// on nextgen the executor runs on DXF service nodes in the SYSTEM
	// keyspace, the table data lives in the task's keyspace.
	store := e.store
	if store.GetKeyspace() != task.Keyspace {
		var err error
		if err2 := e.GetTaskTable().WithNewSession(func(se sessionctx.Context) error {
			store, err = se.GetSQLServer().GetKSStore(task.Keyspace)
			return err
		}); err2 != nil {
			return nil, err2
		}
	}
	switch task.Step {
	case proto.ExportStepDump:
		return &dumpStepExecutor{
			taskMeta: taskMeta,
			store:    store,
			logger:   logutil.BgLogger().With(zap.Int64("task-id", task.ID), zap.String("step", "dump")),
		}, nil
	default:
		return nil, errors.Errorf("unknown export step %d", task.Step)
	}
}

type dumpStepExecutor struct {
	taskexecutor.BaseStepExecutor
	taskMeta *TaskMeta
	store    kv.Storage
	logger   *zap.Logger

	objStore storeapi.Storage
	colInfos []*model.ColumnInfo
	fieldTps []*types.FieldType
	summary  execute.SubtaskSummary
	// bufPool recycles encoded buffers between encoders and writers.
	bufPool sync.Pool
	// chunkPool recycles read chunks between encoders and readers.
	chunkPool sync.Pool
}

func (e *dumpStepExecutor) getChunk() *chunk.Chunk {
	if chk, ok := e.chunkPool.Get().(*chunk.Chunk); ok {
		chk.Reset()
		return chk
	}
	return chunk.NewChunkWithCapacity(e.fieldTps, readChunkSize)
}

var _ execute.StepExecutor = (*dumpStepExecutor)(nil)

// Init implements execute.StepExecutor.
func (e *dumpStepExecutor) Init(ctx context.Context) error {
	objStore, err := objstore.NewFromURL(ctx, e.taskMeta.Dest)
	if err != nil {
		return errors.Trace(err)
	}
	e.objStore = objStore
	e.colInfos, e.fieldTps = exportColumns(e.taskMeta.TableInfo)
	return nil
}

// RunSubtask implements execute.StepExecutor.
func (e *dumpStepExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) error {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)
	stMeta := &SubtaskMeta{}
	if err := json.Unmarshal(subtask.Meta, stMeta); err != nil {
		return errors.Annotate(err, "unmarshal export subtask meta failed")
	}
	// sub-range bounds were fixed at schedule time, so a retry rewrites
	// exactly the same files.
	bounds := make([]kv.Key, 0, len(stMeta.WriterBounds))
	for _, k := range stMeta.WriterBounds {
		bounds = append(bounds, k)
	}
	e.logger.Info("run export dump subtask",
		zap.Int64("subtask-id", subtask.ID),
		zap.Int("ordinal", subtask.Ordinal),
		zap.Int("concurrency", subtask.Concurrency),
		zap.Int("writer-cnt", len(bounds)-1))
	return e.runPipeline(ctx, stMeta.PhysicalID, subtask.Ordinal, bounds)
}

// RealtimeSummary implements execute.StepExecutor.
func (e *dumpStepExecutor) RealtimeSummary() *execute.SubtaskSummary {
	return &e.summary
}

// Cleanup implements execute.StepExecutor.
func (e *dumpStepExecutor) Cleanup(context.Context) error {
	if e.objStore != nil {
		e.objStore.Close()
	}
	return nil
}
