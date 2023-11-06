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
	"path"
	"strconv"
	"time"

	"github.com/docker/go-units"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/disttask/operator"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/workerpool"
	"github.com/pingcap/tidb/pkg/resourcemanager/util"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/size"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	maxWaitDuration = 30 * time.Second

	// We limit the memory usage of KV deliver to 1GB per concurrency, and data
	// KV deliver has external.DefaultMemSizeLimit, the rest of memory is for
	// all index KV deliver.
	// Note: this size is the memory taken by KV, not the size of taken by golang,
	// each KV has additional 24*2 bytes overhead for golang slice.
	indexKVTotalBufSize = size.GB - external.DefaultMemSizeLimit
	// we use a larger block size for data KV group to support larger row.
	// TODO: make it configurable?
	dataKVGroupBlockSize = 32 * units.MiB
)

// encodeAndSortOperator is an operator that encodes and sorts data.
// this operator process data of a subtask, i.e. one engine, it contains a lot
// of data chunks, each chunk is a data file or part of it.
// we don't split into encode and sort operators of chunk level, we parallel
// them inside.
type encodeAndSortOperator struct {
	*operator.AsyncOperator[*importStepMinimalTask, workerpool.None]
	wg       tidbutil.WaitGroupWrapper
	firstErr atomic.Error

	ctx    context.Context
	cancel context.CancelFunc

	taskID, subtaskID int64
	tableImporter     *importer.TableImporter
	sharedVars        *SharedVars
	logger            *zap.Logger
	errCh             chan error
}

var _ operator.Operator = (*encodeAndSortOperator)(nil)
var _ operator.WithSource[*importStepMinimalTask] = (*encodeAndSortOperator)(nil)
var _ operator.WithSink[workerpool.None] = (*encodeAndSortOperator)(nil)

func newEncodeAndSortOperator(ctx context.Context, executor *importStepExecutor,
	sharedVars *SharedVars, subtaskID int64, indexMemorySizeLimit uint64) *encodeAndSortOperator {
	subCtx, cancel := context.WithCancel(ctx)
	op := &encodeAndSortOperator{
		ctx:           subCtx,
		cancel:        cancel,
		taskID:        executor.taskID,
		subtaskID:     subtaskID,
		tableImporter: executor.tableImporter,
		sharedVars:    sharedVars,
		logger:        executor.logger,
		errCh:         make(chan error),
	}
	pool := workerpool.NewWorkerPool(
		"encodeAndSortOperator",
		util.ImportInto,
		int(executor.taskMeta.Plan.ThreadCnt),
		func() workerpool.Worker[*importStepMinimalTask, workerpool.None] {
			return newChunkWorker(ctx, op, indexMemorySizeLimit)
		},
	)
	op.AsyncOperator = operator.NewAsyncOperator(subCtx, pool)
	return op
}

func (op *encodeAndSortOperator) Open() error {
	op.wg.Run(func() {
		for err := range op.errCh {
			if op.firstErr.CompareAndSwap(nil, err) {
				op.cancel()
			} else {
				if errors.Cause(err) != context.Canceled {
					op.logger.Error("error on encode and sort", zap.Error(err))
				}
			}
		}
	})
	return op.AsyncOperator.Open()
}

func (op *encodeAndSortOperator) Close() error {
	// TODO: handle close err after we separate wait part from close part.
	// right now AsyncOperator.Close always returns nil, ok to ignore it.
	// nolint:errcheck
	op.AsyncOperator.Close()
	op.cancel()
	close(op.errCh)
	op.wg.Wait()
	// see comments on interface definition, this Close is actually WaitAndClose.
	return op.firstErr.Load()
}

func (*encodeAndSortOperator) String() string {
	return "encodeAndSortOperator"
}

func (op *encodeAndSortOperator) hasError() bool {
	return op.firstErr.Load() != nil
}

func (op *encodeAndSortOperator) onError(err error) {
	op.errCh <- err
}

func (op *encodeAndSortOperator) Done() <-chan struct{} {
	return op.ctx.Done()
}

type chunkWorker struct {
	ctx context.Context
	op  *encodeAndSortOperator

	dataWriter  *external.EngineWriter
	indexWriter *importer.IndexRouteWriter
}

func newChunkWorker(ctx context.Context, op *encodeAndSortOperator, indexMemorySizeLimit uint64) *chunkWorker {
	w := &chunkWorker{
		ctx: ctx,
		op:  op,
	}
	if op.tableImporter.IsGlobalSort() {
		// in case on network partition, 2 nodes might run the same subtask.
		workerUUID := uuid.New().String()
		// sorted index kv storage path: /{taskID}/{subtaskID}/index/{indexID}/{workerID}
		indexWriterFn := func(indexID int64) *external.Writer {
			builder := external.NewWriterBuilder().
				SetOnCloseFunc(func(summary *external.WriterSummary) {
					op.sharedVars.mergeIndexSummary(indexID, summary)
				}).
				SetMemorySizeLimit(indexMemorySizeLimit).
				SetBlockSize(getKVGroupBlockSize(""))
			prefix := subtaskPrefix(op.taskID, op.subtaskID)
			// writer id for index: index/{indexID}/{workerID}
			writerID := path.Join("index", strconv.Itoa(int(indexID)), workerUUID)
			writer := builder.Build(op.tableImporter.GlobalSortStore, prefix, writerID)
			return writer
		}

		// sorted data kv storage path: /{taskID}/{subtaskID}/data/{workerID}
		builder := external.NewWriterBuilder().
			SetOnCloseFunc(op.sharedVars.mergeDataSummary).
			SetBlockSize(getKVGroupBlockSize(dataKVGroup))
		prefix := subtaskPrefix(op.taskID, op.subtaskID)
		// writer id for data: data/{workerID}
		writerID := path.Join("data", workerUUID)
		writer := builder.Build(op.tableImporter.GlobalSortStore, prefix, writerID)
		w.dataWriter = external.NewEngineWriter(writer)

		w.indexWriter = importer.NewIndexRouteWriter(op.logger, indexWriterFn)
	}
	return w
}

func (w *chunkWorker) HandleTask(task *importStepMinimalTask, _ func(workerpool.None)) {
	if w.op.hasError() {
		return
	}
	// we don't use the input send function, it makes workflow more complex
	// we send result to errCh and handle it here.
	executor := newImportMinimalTaskExecutor(task)
	if err := executor.Run(w.ctx, w.dataWriter, w.indexWriter); err != nil {
		w.op.onError(err)
	}
}

func (w *chunkWorker) Close() {
	closeCtx := w.ctx
	if closeCtx.Err() != nil {
		// in case of context canceled, we need to create a new context to close writers.
		newCtx, cancel := context.WithTimeout(context.Background(), maxWaitDuration)
		closeCtx = newCtx
		defer cancel()
	}
	if w.dataWriter != nil {
		// Note: we cannot ignore close error as we're writing to S3 or GCS.
		// ignore error might cause data loss. below too.
		if _, err := w.dataWriter.Close(closeCtx); err != nil {
			w.op.onError(errors.Trace(err))
		}
	}
	if w.indexWriter != nil {
		if _, err := w.indexWriter.Close(closeCtx); err != nil {
			w.op.onError(errors.Trace(err))
		}
	}
}

func subtaskPrefix(taskID, subtaskID int64) string {
	return path.Join(strconv.Itoa(int(taskID)), strconv.Itoa(int(subtaskID)))
}

func getWriterMemorySizeLimit(plan *importer.Plan) uint64 {
	// min(external.DefaultMemSizeLimit, indexKVTotalBufSize / num-of-index-that-gen-kv)
	cnt := getNumOfIndexGenKV(plan.DesiredTableInfo)
	limit := indexKVTotalBufSize
	if cnt > 0 {
		limit = limit / uint64(cnt)
	}
	if limit > external.DefaultMemSizeLimit {
		limit = external.DefaultMemSizeLimit
	}
	return limit
}

func getNumOfIndexGenKV(tblInfo *model.TableInfo) int {
	var count int
	var nonClusteredPK bool
	for _, idxInfo := range tblInfo.Indices {
		// all public non-primary index generates index KVs
		if idxInfo.State != model.StatePublic {
			continue
		}
		if idxInfo.Primary && !tblInfo.HasClusteredIndex() {
			nonClusteredPK = true
			continue
		}
		count++
	}
	if nonClusteredPK {
		count++
	}
	return count
}

func getKVGroupBlockSize(group string) int {
	if group == dataKVGroup {
		return dataKVGroupBlockSize
	}
	return external.DefaultBlockSize
}
