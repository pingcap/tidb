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
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/planner/extstore"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// countCapMultiplier sizes the hard cap on concurrently running chunks
// relative to node CPU: small chunks are PUT-latency-bound rather than
// bandwidth-bound, so they can usefully run at well above 1x cores before
// hitting diminishing returns. Aliased from scheduler.ExportDumpConcurrencyMultiplier
// (the single source of truth, since CalcRequiredSlotsForExport also derives
// its table-count bound from it) so call sites below don't need the package
// qualifier.
const countCapMultiplier = scheduler.ExportDumpConcurrencyMultiplier

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

// IsIdempotent implements taskexecutor.Extension. Chunk file names are fixed at
// split, so a retry overwrites the same files.
func (*exportTaskExecutor) IsIdempotent(*proto.Subtask) bool {
	return true
}

// IsRetryableError implements taskexecutor.Extension.
func (*exportTaskExecutor) IsRetryableError(err error) bool {
	return common.IsRetryableError(err)
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
	case proto.ExportStepSchema:
		return &schemaStepExecutor{
			taskMeta: taskMeta,
			store:    e.TaskRuntime.Store(),
			taskTbl:  e.GetTaskTable(),
			logger:   logutil.BgLogger().With(zap.Int64("task-id", task.ID), zap.String("step", "schema")),
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

	objStore  storeapi.Storage
	tableRefs []tableRef
	summary   execute.SubtaskSummary
}

var _ execute.StepExecutor = (*dumpStepExecutor)(nil)

// Init implements execute.StepExecutor.
func (e *dumpStepExecutor) Init(ctx context.Context) error {
	objStore, err := objstore.NewFromURL(ctx, e.taskMeta.DestURI)
	if err != nil {
		return errors.Trace(err)
	}
	e.objStore = objStore

	tableInfos, _, err := snapshotTableInfos(e.store, e.taskMeta)
	if err != nil {
		return err
	}
	refs, err := e.taskMeta.tableRefs(tableInfos)
	if err != nil {
		return err
	}
	e.tableRefs = refs
	return nil
}

// decodeSubtaskMeta unmarshals the subtask row and, since marshalSubtasks
// offloads Chunks to external storage, hydrates it from there when needed.
func decodeSubtaskMeta(ctx context.Context, subtask *proto.Subtask) (*SubtaskMeta, error) {
	stMeta := &SubtaskMeta{}
	if err := json.Unmarshal(subtask.Meta, stMeta); err != nil {
		return nil, errors.Annotate(err, "unmarshal export subtask meta failed")
	}
	if stMeta.ExternalPath != "" {
		metaStore, err := extstore.GetGlobalExtStorage(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if err := stMeta.ReadJSONFromExternalStorage(ctx, metaStore, stMeta); err != nil {
			return nil, errors.Trace(err)
		}
	}
	return stMeta, nil
}

// RunSubtask implements execute.StepExecutor. A subtask's chunks mix
// bandwidth-bound regular chunks and latency-bound irregular ones (see
// packSubtasks), so a single fixed worker count either over-concurrents big
// chunks (bandwidth contention) or under-uses small ones (idle capacity while
// latency-bound). Instead, each chunk is admitted against two caps sized to
// the node's CPU: a byte-weighted cap (weightCap = nodeCPU * FileSize, one
// FileSize-sized stream per CPU — a single chunk writer already saturates
// roughly one core's share of egress bandwidth once it's writing a full
// output file, so more concurrent big chunks than that just burns memory for
// no extra throughput) that a chunk at or above FileSize consumes almost
// entirely by itself, and a flat count cap (countCap) that bounds how many
// small, near-zero-weight chunks can run at once regardless of how little
// bandwidth they use. This assumes each open chunk writer's own memory stays
// well under nodeMem/nodeCPU (see uploadConcurrency/uploadPartSize) — the
// weight budget doesn't itself account for memory, it relies on that holding.
func (e *dumpStepExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) error {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)
	stMeta, err := decodeSubtaskMeta(ctx, subtask)
	if err != nil {
		return err
	}
	nodeCPU := int64(max(1, int(e.GetResource().CPU.Capacity())))
	fileSize := max(int64(1), e.taskMeta.FileSize)
	weightCap := nodeCPU * fileSize
	countCap := countCapMultiplier * nodeCPU
	e.logger.Info("run export dump subtask",
		zap.Int64("subtask-id", subtask.ID),
		zap.Int("chunk-cnt", len(stMeta.Chunks)),
		zap.Int64("weight-cap", weightCap),
		zap.Int64("count-cap", countCap))

	// cePool reuses chunkExporters across chunks (they're expensive to build
	// but not safe for concurrent use) instead of one per worker, since there
	// are no fixed long-lived workers left to own one each.
	cePool := make(chan *chunkExporter, countCap)
	getCE := func() *chunkExporter {
		select {
		case ce := <-cePool:
			return ce
		default:
			return e.newChunkExporter()
		}
	}
	putCE := func(ce *chunkExporter) {
		select {
		case cePool <- ce:
		default:
		}
	}

	weightSem := semaphore.NewWeighted(weightCap)
	countSem := semaphore.NewWeighted(countCap)
	eg, egCtx := errgroup.WithContext(ctx)
	for _, c := range stMeta.Chunks {
		w := max(int64(1), min(c.Size, fileSize))
		if err := weightSem.Acquire(egCtx, w); err != nil {
			break
		}
		if err := countSem.Acquire(egCtx, 1); err != nil {
			weightSem.Release(w)
			break
		}
		eg.Go(func() error {
			defer weightSem.Release(w)
			defer countSem.Release(1)
			ce := getCE()
			defer putCE(ce)
			return e.exportChunk(egCtx, ce, c)
		})
	}
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
