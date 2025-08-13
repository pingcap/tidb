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

package ddl

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	ddllogutil "github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/prometheus/client_golang/prometheus"
	kvutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

type mergeTempIndexExecutor struct {
	taskexecutor.BaseStepExecutor
	job      *model.Job
	store    kv.Storage
	ptbl     table.PhysicalTable
	batchCnt int

	mergeCounter    prometheus.Counter
	conflictCounter prometheus.Counter
	idxInfo         *model.IndexInfo

	*execute.SubtaskSummary
	totalRows int64
	buffers   *tempIdxBuffers
}

func newMergeTempIndexExecutor(job *model.Job, store kv.Storage, ptbl table.PhysicalTable) (*mergeTempIndexExecutor, error) {
	batchCnt := job.ReorgMeta.GetBatchSize()
	return &mergeTempIndexExecutor{
		job:            job,
		store:          store,
		batchCnt:       batchCnt,
		ptbl:           ptbl,
		SubtaskSummary: &execute.SubtaskSummary{},
		buffers:        newTempIdxBuffers(batchCnt),
	}, nil
}

func (*mergeTempIndexExecutor) Init(ctx context.Context) error {
	logutil.Logger(ctx).Info("merge temp index executor init subtask exec env")
	return nil
}

func (e *mergeTempIndexExecutor) initializeByMeta(ctx context.Context, meta *BackfillSubTaskMeta) error {
	bfMs := []int{1, 50, 250, 500, 1000, 2000}
	ticker := time.NewTicker(time.Duration(bfMs[0]) * time.Millisecond)
	defer ticker.Stop()

	attempts := 0
	var readyToMerge bool
	for !readyToMerge {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			_, tbl, err := getTableByTxn(ctx, e.store, e.job.SchemaID, e.job.TableID)
			if err != nil {
				if kv.IsTxnRetryableError(err) {
					logutil.Logger(ctx).Warn("failed to get table, retrying", zap.Error(err))
					continue
				}
				return errors.Trace(err)
			}
			physicalID := tablecodec.DecodeTableID(meta.StartKey)
			idxInfo, err := findIndexInfoByDecodingKey(tbl.Indices(), meta.StartKey)
			if err != nil {
				return errors.Trace(err)
			}
			if idxInfo.BackfillState != model.BackfillStateMerging {
				ddllogutil.SampleLogger().Info("wait for temp index to be ready for merging",
					zap.Int64("jobID", e.job.ID),
					zap.String("indexName", idxInfo.Name.L),
					zap.Stringer("currentState", idxInfo.BackfillState),
				)
				attempts++
				bfMsIdx := min(attempts, len(bfMs)-1)
				ticker.Reset(time.Duration(bfMs[bfMsIdx]) * time.Millisecond)
				continue
			}
			e.ptbl = tbl.(table.PhysicalTable)
			if tbl.Meta().Partition != nil && !idxInfo.Global {
				e.ptbl = tbl.GetPartitionedTable().GetPartition(physicalID)
				if e.ptbl == nil {
					return errors.Errorf("partitioned table %d not found for index %s", physicalID, idxInfo.Name.L)
				}
			}
			e.idxInfo = idxInfo
			readyToMerge = true
		}
	}
	e.mergeCounter = metrics.GetBackfillTotalByLabel(
		metrics.LblMergeTmpIdxRate,
		e.job.SchemaName, e.ptbl.Meta().Name.String(), e.idxInfo.Name.L)
	e.conflictCounter = metrics.GetBackfillTotalByLabel(
		fmt.Sprintf("%s-conflict", metrics.LblMergeTmpIdxRate),
		e.job.SchemaName, e.ptbl.Meta().Name.String(), e.idxInfo.Name.L)
	return nil
}

func (e *mergeTempIndexExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) error {
	logutil.Logger(ctx).Info("merge temp index executor run subtask")

	var meta BackfillSubTaskMeta
	err := json.Unmarshal(subtask.Meta, &meta)
	if err != nil {
		return errors.Trace(err)
	}
	start := meta.StartKey
	err = e.initializeByMeta(ctx, &meta)
	if err != nil {
		return errors.Trace(err)
	}
	e.Reset()

	for {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		rs, err := e.handleOneRange(ctx, start, meta.EndKey)
		if err != nil {
			return err
		}
		e.mergeCounter.Add(float64(rs.addCount))
		e.RowCnt.Add(int64(rs.addCount))
		e.totalRows += int64(rs.scanCount)
		start = rs.nextKey
		if rs.done {
			break
		}
	}
	return nil
}

func (e *mergeTempIndexExecutor) handleOneRange(
	ctx context.Context,
	start, end kv.Key,
) (result tempIdxResult, err error) {
	var currentTxnStartTS uint64
	oprStartTime := time.Now()
	ctx = kv.WithInternalSourceAndTaskType(ctx, "ddl_merge_temp_index", kvutil.ExplicitTypeDDL)
	originBatchCnt := e.batchCnt
	defer func() {
		e.batchCnt = originBatchCnt
	}()
	jobCtx := NewReorgContext()
	jobCtx.tp = "ddl_merge_temp_index"
	jobCtx.getResourceGroupTaggerForTopSQL()
	jobCtx.resourceGroupName = e.job.ReorgMeta.ResourceGroupName

	attempts := 0
	for {
		attempts++
		err := kv.RunInNewTxn(ctx, e.store, false, func(_ context.Context, txn kv.Transaction) error {
			currentTxnStartTS = txn.StartTS()
			updateTxnEntrySizeLimitIfNeeded(txn)
			rs, err := fetchTempIndexVals(jobCtx, e.store, e.ptbl, e.idxInfo, txn, start, end, e.batchCnt, e.buffers)
			if err != nil {
				return errors.Trace(err)
			}
			result = rs
			err = batchCheckTemporaryUniqueKey(txn, e.ptbl, e.idxInfo, e.buffers.originIdxKeys, e.buffers.tmpIdxRecords)
			if err != nil {
				return errors.Trace(err)
			}

			for i, idxRecord := range e.buffers.tmpIdxRecords {
				// The index is already exists, we skip it, no needs to backfill it.
				// The following update, delete, insert on these rows, TiDB can handle it correctly.
				// If all batch are skipped, update first index key to make txn commit to release lock.
				if idxRecord.skip {
					continue
				}

				originIdxKey := e.buffers.originIdxKeys[i]
				if idxRecord.delete {
					err = txn.GetMemBuffer().Delete(originIdxKey)
				} else {
					err = txn.GetMemBuffer().Set(originIdxKey, idxRecord.vals)
				}
				if err != nil {
					return err
				}

				err = txn.GetMemBuffer().Delete(e.buffers.tmpIdxKeys[i])
				if err != nil {
					return err
				}

				failpoint.InjectCall("mockDMLExecutionMergingInTxn")

				result.addCount++
			}
			return nil
		})
		if err != nil {
			if kv.IsTxnRetryableError(err) {
				if e.batchCnt > 1 {
					e.batchCnt /= 2
				}
				e.conflictCounter.Add(1)
				backoff := kv.BackOff(uint(attempts))
				logutil.Logger(ctx).Warn("temp index merge worker retry",
					zap.Int64("jobID", e.job.ID),
					zap.Int("batchCnt", e.batchCnt),
					zap.Int("attempts", attempts),
					zap.Duration("backoff", time.Duration(backoff)),
					zap.Uint64("startTS", currentTxnStartTS),
					zap.Error(err))
				continue
			}
			return result, errors.Trace(err)
		}
		break
	}

	metrics.DDLSetTempIndexScanAndMerge(e.ptbl.GetPhysicalID(), uint64(result.scanCount), uint64(result.addCount))
	failpoint.Inject("mockDMLExecutionMerging", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) && MockDMLExecutionMerging != nil {
			MockDMLExecutionMerging()
		}
	})
	logSlowOperations(time.Since(oprStartTime), "mergeTempIndexExecutorHandleOneRange", 3000)
	return result, nil
}

func (e *mergeTempIndexExecutor) RealtimeSummary() *execute.SubtaskSummary {
	return e.SubtaskSummary
}

func (e *mergeTempIndexExecutor) Cleanup(ctx context.Context) error {
	logutil.Logger(ctx).Info("merge temp index executor clean up subtask env", zap.Int64("rows", e.totalRows))
	return nil
}

// TaskMetaModified changes the max write speed for ingest
func (*mergeTempIndexExecutor) TaskMetaModified(_ context.Context, _ []byte) error {
	// Will be added in the future PR
	return nil
}

// ResourceModified change the concurrency for ingest
func (*mergeTempIndexExecutor) ResourceModified(_ context.Context, _ *proto.StepResource) error {
	// Will be added in the future PR
	return nil
}
