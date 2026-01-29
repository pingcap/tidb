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
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/operator"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/workerpool"
	"github.com/pingcap/tidb/pkg/resourcemanager/util"
	"go.uber.org/zap"
)

const (
	maxWaitDuration = 30 * time.Second

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

	taskID, subtaskID int64
	tableImporter     *importer.TableImporter
	sharedVars        *SharedVars
	logger            *zap.Logger
	errCh             chan error
	indicesGenKV      map[int64]genKVIndex
}

var _ operator.Operator = (*encodeAndSortOperator)(nil)
var _ operator.WithSource[*importStepMinimalTask] = (*encodeAndSortOperator)(nil)
var _ operator.WithSink[workerpool.None] = (*encodeAndSortOperator)(nil)

func newEncodeAndSortOperator(
	wctx *workerpool.Context,
	executor *importStepExecutor,
	sharedVars *SharedVars,
	subtaskID int64,
	concurrency int,
) *encodeAndSortOperator {
	op := &encodeAndSortOperator{
		taskID:        executor.taskID,
		subtaskID:     subtaskID,
		tableImporter: executor.tableImporter,
		sharedVars:    sharedVars,
		logger:        executor.logger,
		errCh:         make(chan error),
		indicesGenKV:  executor.indicesGenKV,
	}
	pool := workerpool.NewWorkerPool(
		"encodeAndSortOperator",
		util.ImportInto,
		concurrency,
		func() workerpool.Worker[*importStepMinimalTask, workerpool.None] {
			return newChunkWorker(wctx, op, executor.dataKVMemSizePerCon,
				executor.perIndexKVMemSizePerCon, executor.dataBlockSize, executor.indexBlockSize)
		},
	)
	op.AsyncOperator = operator.NewAsyncOperator(wctx, pool)
	return op
}

func (*encodeAndSortOperator) String() string {
	return "encodeAndSortOperator"
}

type chunkWorker struct {
	ctx context.Context
	op  *encodeAndSortOperator

	dataWriter  *external.EngineWriter
	indexWriter *importer.IndexRouteWriter
}

func newChunkWorker(ctx context.Context, op *encodeAndSortOperator, dataKVMemSizePerCon,
	perIndexKVMemSizePerCon uint64, dataBlockSize, indexBlockSize int) *chunkWorker {
	w := &chunkWorker{
		ctx: ctx,
		op:  op,
	}
	if op.tableImporter.IsGlobalSort() {
		// in case on network partition, 2 nodes might run the same subtask.
		workerUUID := uuid.New().String()
		// sorted index kv storage path: /{taskID}/{subtaskID}/index/{indexID}/{workerID}
		indexWriterFn := func(indexID int64) (*external.Writer, error) {
			idx, ok := op.indicesGenKV[indexID]
			if !ok {
				// shouldn't happen normally, unless we have bug at getIndicesGenKV
				return nil, errors.Errorf("unknown index with ID: %d", indexID)
			}
			onDup := common.OnDuplicateKeyRemove
			if idx.unique {
				onDup = common.OnDuplicateKeyRecord
			}
			builder := external.NewWriterBuilder().
				SetOnCloseFunc(func(summary *external.WriterSummary) {
					op.sharedVars.mergeIndexSummary(indexID, summary)
				}).
				SetMemorySizeLimit(perIndexKVMemSizePerCon).
				SetBlockSize(indexBlockSize).
				SetOnDup(onDup)
			prefix := subtaskPrefix(op.taskID, op.subtaskID)
			// writer id for index: index/{indexID}/{workerID}
			writerID := path.Join("index", IndexID2KVGroup(indexID), workerUUID)
			writer := builder.Build(op.tableImporter.GlobalSortStore, prefix, writerID)
			return writer, nil
		}

		// sorted data kv storage path: /{taskID}/{subtaskID}/data/{workerID}
		builder := external.NewWriterBuilder().
			SetOnCloseFunc(op.sharedVars.mergeDataSummary).
			SetMemorySizeLimit(dataKVMemSizePerCon).
			SetBlockSize(dataBlockSize).
			SetOnDup(common.OnDuplicateKeyRecord)
		prefix := subtaskPrefix(op.taskID, op.subtaskID)
		// writer id for data: data/{workerID}
		writerID := path.Join("data", workerUUID)
		writer := builder.Build(op.tableImporter.GlobalSortStore, prefix, writerID)
		w.dataWriter = external.NewEngineWriter(writer)

		w.indexWriter = importer.NewIndexRouteWriter(op.logger, indexWriterFn)
	}
	return w
}

func (w *chunkWorker) HandleTask(task *importStepMinimalTask, _ func(workerpool.None)) error {
	// we don't use the input send function, it makes workflow more complex
	// we send result to errCh and handle it here.
	executor := newImportMinimalTaskExecutor(task)
	return executor.Run(w.ctx, w.dataWriter, w.indexWriter)
}

func (w *chunkWorker) Close() error {
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
			return err
		}
	}
	if w.indexWriter != nil {
		if _, err := w.indexWriter.Close(closeCtx); err != nil {
			return err
		}
	}

	return nil
}

func subtaskPrefix(taskID, subtaskID int64) string {
	return path.Join(strconv.Itoa(int(taskID)), strconv.Itoa(int(subtaskID)))
}

func getWriterMemorySizeLimit(resource *proto.StepResource, plan *importer.Plan) (
	dataKVMemSizePerCon, perIndexKVMemSizePerCon uint64) {
	indexKVGroupCnt := getNumOfIndexGenKV(plan.DesiredTableInfo)
	memPerCon := resource.Mem.Capacity() / int64(plan.ThreadCnt)
	// we use half of the total available memory for data writer, and the other half
	// for encoding and other stuffs, it's an experience value, might not optimal.
	// Then we divide those memory into indexKVGroupCnt + 3 shares, data KV writer
	// takes 3 shares, and each index KV writer takes 1 share.
	// suppose we have memPerCon = 2G
	// 	| indexKVGroupCnt | data/per-index writer |
	// 	| :-------------- | :-------------------- |
	// 	| 0               | 1GiB/0                |
	// 	| 1               | 768/256 MiB           |
	// 	| 5               | 384/128 MiB           |
	// 	| 13              | 192/64 MiB            |
	memPerShare := float64(memPerCon) / 2 / float64(indexKVGroupCnt+3)
	return uint64(memPerShare * 3), uint64(memPerShare)
}

// getNumOfIndexGenKV returns the number of index generated KVs.
func getNumOfIndexGenKV(tblInfo *model.TableInfo) int {
	return len(getIndicesGenKV(tblInfo))
}

type genKVIndex struct {
	name   string
	unique bool
}

func getIndicesGenKV(tblInfo *model.TableInfo) map[int64]genKVIndex {
	res := make(map[int64]genKVIndex, len(tblInfo.Indices))
	for _, idxInfo := range tblInfo.Indices {
		// all public non-primary index generates index KVs
		if idxInfo.State != model.StatePublic {
			continue
		}
		if idxInfo.Primary && tblInfo.HasClusteredIndex() {
			continue
		}
		res[idxInfo.ID] = genKVIndex{
			name:   idxInfo.Name.L,
			unique: idxInfo.Unique,
		}
	}
	return res
}

func getKVGroupBlockSize(group string) int {
	if group == dataKVGroup {
		return dataKVGroupBlockSize
	}
	return external.DefaultBlockSize
}
