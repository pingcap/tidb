// Copyright 2025 PingCAP, Inc.
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

package globalindexcleanup

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	kvutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

// Executor implements taskexecutor.TaskExecutor for global index cleanup.
type Executor struct {
	*taskexecutor.BaseTaskExecutor
	store    kv.Storage
	task     *proto.Task
	taskMeta *CleanupTaskMeta
}

var _ taskexecutor.Extension = (*Executor)(nil)

// NewExecutor creates a new Executor.
func NewExecutor(ctx context.Context, store kv.Storage, task *proto.Task, param taskexecutor.Param) taskexecutor.TaskExecutor {
	e := &Executor{
		BaseTaskExecutor: taskexecutor.NewBaseTaskExecutor(ctx, task, param),
		store:            store,
		task:             task,
	}
	e.BaseTaskExecutor.Extension = e
	return e
}

// Init implements taskexecutor.Extension.
func (e *Executor) Init(ctx context.Context) error {
	if err := e.BaseTaskExecutor.Init(ctx); err != nil {
		return err
	}

	var taskMeta CleanupTaskMeta
	if err := taskMeta.Unmarshal(e.task.Meta); err != nil {
		return errors.Trace(err)
	}
	e.taskMeta = &taskMeta
	return nil
}

// GetStepExecutor implements taskexecutor.Extension.
func (e *Executor) GetStepExecutor(task *proto.Task) (execute.StepExecutor, error) {
	switch task.Step {
	case proto.GlobalIndexCleanupStepScanAndDelete:
		return newCleanupStepExecutor(e.store, e.taskMeta), nil
	default:
		return nil, errors.Errorf("unknown step %d for global index cleanup task %d", task.Step, task.ID)
	}
}

// IsIdempotent implements taskexecutor.Extension.
func (*Executor) IsIdempotent(*proto.Subtask) bool {
	return true
}

// IsRetryableError implements taskexecutor.Extension.
func (*Executor) IsRetryableError(err error) bool {
	return common.IsRetryableError(err)
}

// Close implements taskexecutor.Extension.
func (e *Executor) Close() {
	e.BaseTaskExecutor.Close()
}

// cleanupStepExecutor executes the scan-and-delete step.
type cleanupStepExecutor struct {
	taskexecutor.BaseStepExecutor
	store    kv.Storage
	taskMeta *CleanupTaskMeta
	summary  *execute.SubtaskSummary
}

func newCleanupStepExecutor(store kv.Storage, taskMeta *CleanupTaskMeta) *cleanupStepExecutor {
	return &cleanupStepExecutor{
		store:    store,
		taskMeta: taskMeta,
		summary:  &execute.SubtaskSummary{},
	}
}

// Init implements execute.StepExecutor.
func (*cleanupStepExecutor) Init(context.Context) error {
	return nil
}

// RunSubtask implements execute.StepExecutor.
func (e *cleanupStepExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) error {
	var subtaskMeta CleanupSubtaskMeta
	if err := subtaskMeta.Unmarshal(subtask.Meta); err != nil {
		return errors.Trace(err)
	}

	logger := logutil.DDLLogger().With(
		zap.Int64("subtask-id", subtask.ID),
		zap.Int64("partition-id", subtaskMeta.PhysicalTableID),
	)
	logger.Info("start running global index cleanup subtask")

	// Get global index infos.
	globalIndexInfos := make([]*model.IndexInfo, 0, len(e.taskMeta.GlobalIndexIDs))
	for _, idxInfo := range e.taskMeta.TableInfo.Indices {
		if idxInfo.Global {
			for _, gid := range e.taskMeta.GlobalIndexIDs {
				if idxInfo.ID == gid {
					globalIndexInfos = append(globalIndexInfos, idxInfo)
					break
				}
			}
		}
	}
	if len(globalIndexInfos) == 0 {
		logger.Info("no global indexes to clean up")
		return nil
	}

	deleteCount, err := e.cleanupGlobalIndexEntries(
		ctx,
		globalIndexInfos,
		subtaskMeta.RowStart,
		subtaskMeta.RowEnd,
		subtaskMeta.PhysicalTableID,
		logger,
	)
	if err != nil {
		return errors.Trace(err)
	}

	e.summary.RowCnt.Add(deleteCount)
	logger.Info("finished global index cleanup subtask", zap.Int64("deleted", deleteCount))
	return nil
}

func (e *cleanupStepExecutor) cleanupGlobalIndexEntries(
	ctx context.Context,
	globalIndexInfos []*model.IndexInfo,
	startKey, endKey []byte,
	partitionID int64,
	_ *zap.Logger,
) (int64, error) {
	var totalDeleted int64
	batchSize := e.taskMeta.Job.ReorgMeta.GetBatchSize()
	if batchSize <= 0 {
		batchSize = 1000
	}

	tblInfo := e.taskMeta.TableInfo
	currentKey := startKey
	kvCtx := kv.WithInternalSourceAndTaskType(ctx, kv.InternalTxnDDL, kvutil.ExplicitTypeDDL)

	for {
		var (
			deletedInBatch int64
			nextKey        []byte
			done           bool
		)

		err := kv.RunInNewTxn(kvCtx, e.store, true, func(_ context.Context, txn kv.Transaction) error {
			deletedInBatch = 0

			// Scan rows in the range.
			it, err := txn.Iter(currentKey, endKey)
			if err != nil {
				return errors.Trace(err)
			}
			defer it.Close()

			rowCount := 0
			for it.Valid() && rowCount < batchSize {
				if !tablecodec.IsRecordKey(it.Key()) {
					nextKey = it.Key().Next()
					if err := it.Next(); err != nil {
						return errors.Trace(err)
					}
					continue
				}

				handle, err := tablecodec.DecodeRowKey(it.Key())
				if err != nil {
					return errors.Trace(err)
				}

				// Decode row data to get column values.
				rowData, err := decodeRowData(it.Value(), tblInfo)
				if err != nil {
					// Skip rows that can't be decoded.
					nextKey = it.Key().Next()
					rowCount++
					if err := it.Next(); err != nil {
						return errors.Trace(err)
					}
					continue
				}

				// For each global index, delete the entry if it belongs to this partition.
				for _, idxInfo := range globalIndexInfos {
					// Get index column values.
					idxColVals := make([]types.Datum, 0, len(idxInfo.Columns))
					for _, col := range idxInfo.Columns {
						if val, ok := rowData[tblInfo.Columns[col.Offset].ID]; ok {
							idxColVals = append(idxColVals, val)
						} else {
							idxColVals = append(idxColVals, types.Datum{})
						}
					}

					// Generate the global index key.
					// For global index, the table ID in the key is the table ID, not partition ID.
					indexKey, _, err := tablecodec.GenIndexKey(
						time.UTC,
						tblInfo,
						idxInfo,
						tblInfo.ID, // Use table ID for global index
						idxColVals,
						handle,
						nil,
					)
					if err != nil {
						continue
					}

					// Check if the index entry exists.
					val, err := txn.Get(kvCtx, indexKey)
					if err != nil {
						if kv.IsErrNotFound(err) {
							continue
						}
						return errors.Trace(err)
					}

					// Decode the handle from index value to check partition ID.
					idxHandle, err := tablecodec.DecodeHandleInIndexValue(val.Value)
					if err != nil {
						continue
					}
					partHandle, ok := idxHandle.(kv.PartitionHandle)
					if !ok {
						continue
					}
					if partHandle.PartitionID != partitionID {
						continue
					}

					// Lock and delete.
					lockCtx := &kv.LockCtx{}
					if err := txn.LockKeys(kvCtx, lockCtx, indexKey); err != nil {
						return errors.Trace(err)
					}
					if err := txn.Delete(indexKey); err != nil {
						return errors.Trace(err)
					}
					deletedInBatch++
				}

				nextKey = it.Key().Next()
				rowCount++
				if err := it.Next(); err != nil {
					return errors.Trace(err)
				}
			}

			if rowCount == 0 {
				done = true
			}
			return nil
		})

		if err != nil {
			return totalDeleted, errors.Trace(err)
		}

		totalDeleted += deletedInBatch

		if done || nextKey == nil || kv.Key(nextKey).Cmp(endKey) >= 0 {
			break
		}
		currentKey = nextKey
	}

	return totalDeleted, nil
}

// decodeRowData decodes row value to column ID -> datum map.
func decodeRowData(rowVal []byte, tblInfo *model.TableInfo) (map[int64]types.Datum, error) {
	colFtMap := make(map[int64]*types.FieldType, len(tblInfo.Columns))
	for _, col := range tblInfo.Columns {
		colFtMap[col.ID] = &col.FieldType
	}
	return tablecodec.DecodeRowToDatumMap(rowVal, colFtMap, time.UTC)
}

// RealtimeSummary implements execute.StepExecutor.
func (e *cleanupStepExecutor) RealtimeSummary() *execute.SubtaskSummary {
	return e.summary
}

// ResetSummary implements execute.StepExecutor.
func (e *cleanupStepExecutor) ResetSummary() {
	e.summary.RowCnt.Store(0)
}

// Cleanup implements execute.StepExecutor.
func (*cleanupStepExecutor) Cleanup(context.Context) error {
	return nil
}

// TaskMetaModified implements execute.StepExecutor.
func (e *cleanupStepExecutor) TaskMetaModified(_ context.Context, newTaskMeta []byte) error {
	var taskMeta CleanupTaskMeta
	if err := taskMeta.Unmarshal(newTaskMeta); err != nil {
		return errors.Trace(err)
	}
	e.taskMeta = &taskMeta
	return nil
}

// ResourceModified implements execute.StepExecutor.
func (*cleanupStepExecutor) ResourceModified(_ context.Context, _ *proto.StepResource) error {
	return nil
}

// Ensure unused imports are used.
var (
	_ = errctx.StrictNoWarningContext
	_ = codec.EncodeKey
)
