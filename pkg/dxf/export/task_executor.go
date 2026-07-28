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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type exportTaskExecutor struct {
	*taskexecutor.BaseTaskExecutor
}

var _ taskexecutor.TaskExecutor = (*exportTaskExecutor)(nil)

// NewExportTaskExecutor creates a task executor for the export task.
func NewExportTaskExecutor(ctx context.Context, task *proto.Task, param taskexecutor.Param) taskexecutor.TaskExecutor {
	e := &exportTaskExecutor{
		BaseTaskExecutor: taskexecutor.NewBaseTaskExecutor(ctx, task, param),
	}
	e.BaseTaskExecutor.Extension = e
	return e
}

// IsIdempotent implements taskexecutor.Extension. A subtask's chunks have fixed
// key ranges and file names at a fixed snapshot, so a retry overwrites the same
// files.
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
	switch task.Step {
	case proto.ExportStepDump:
		return &dumpStepExecutor{
			taskMeta: taskMeta,
			store:    e.TaskRuntime.Store(),
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
	summary  execute.SubtaskSummary
}

var _ execute.StepExecutor = (*dumpStepExecutor)(nil)

// Init implements execute.StepExecutor.
func (e *dumpStepExecutor) Init(ctx context.Context) error {
	objStore, err := objstore.NewFromURL(ctx, e.taskMeta.Dest)
	if err != nil {
		return errors.Trace(err)
	}
	e.objStore = objStore
	return nil
}

// RunSubtask implements execute.StepExecutor. It runs a worker pool that pulls
// the subtask's chunks from a queue and exports each, mirroring IMPORT INTO:
// the concurrency is the subtask's allocated CPU capacity, and because a chunk's
// file names are fixed at split time the worker count never affects the output.
func (e *dumpStepExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) error {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)
	stMeta := &SubtaskMeta{}
	if err := json.Unmarshal(subtask.Meta, stMeta); err != nil {
		return errors.Annotate(err, "unmarshal export subtask meta failed")
	}
	concurrency := max(1, int(e.GetResource().CPU.Capacity()))
	e.logger.Info("run export dump subtask",
		zap.Int64("subtask-id", subtask.ID),
		zap.Int("chunk-cnt", len(stMeta.Chunks)),
		zap.Int("concurrency", concurrency))

	chunkCh := make(chan Chunk)
	eg, egCtx := errgroup.WithContext(ctx)
	for range concurrency {
		eg.Go(func() error {
			for c := range chunkCh {
				if err := e.exportChunk(egCtx, c); err != nil {
					return err
				}
			}
			return nil
		})
	}
	eg.Go(func() error {
		defer close(chunkCh)
		for _, c := range stMeta.Chunks {
			select {
			case chunkCh <- c:
			case <-egCtx.Done():
				return egCtx.Err()
			}
		}
		return nil
	})
	return eg.Wait()
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
