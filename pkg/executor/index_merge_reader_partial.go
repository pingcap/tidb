// Copyright 2019 PingCAP, Inc.
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

package executor

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/distsql"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

type partialIndexWorker struct {
	stats              *IndexMergeRuntimeStat
	sc                 sessionctx.Context
	idxID              int
	batchSize          int
	maxBatchSize       int
	maxChunkSize       int
	memTracker         *memory.Tracker
	partitionTableMode bool
	prunedPartitions   []table.PhysicalTable
	byItems            []*plannerutil.ByItems
	scannedKeys        uint64
	pushedLimit        *physicalop.PushedDownLimit
	dagPB              *tipb.DAGRequest
	plan               []base.PhysicalPlan
}

func syncErr(ctx context.Context, finished <-chan struct{}, errCh chan<- *indexMergeTableTask, err error) {
	logutil.BgLogger().Error("IndexMergeReaderExecutor.syncErr", zap.Error(err))
	doneCh := make(chan error, 1)
	doneCh <- err
	task := &indexMergeTableTask{
		lookupTableTask: lookupTableTask{
			doneCh: doneCh,
		},
	}

	// ctx.Done and finished is to avoid write channel is stuck.
	select {
	case <-ctx.Done():
		return
	case <-finished:
		return
	case errCh <- task:
		return
	}
}

// needPartitionHandle indicates whether we need create a partitionHandle or not.
// If the schema from planner part contains ExtraPhysTblID,
// we need create a partitionHandle, otherwise create a normal handle.
// In TableRowIDScan, the partitionHandle will be used to create key ranges.
func (w *partialIndexWorker) needPartitionHandle() (bool, error) {
	cols := w.plan[0].Schema().Columns
	outputOffsets := w.dagPB.OutputOffsets
	col := cols[outputOffsets[len(outputOffsets)-1]]

	is := w.plan[0].(*physicalop.PhysicalIndexScan)
	needPartitionHandle := w.partitionTableMode && len(w.byItems) > 0 || is.Index.Global
	hasExtraCol := col.ID == model.ExtraPhysTblID

	// There will be two needPartitionHandle != hasExtraCol situations.
	// Only `needPartitionHandle` == true and `hasExtraCol` == false are not allowed.
	// `ExtraPhysTblID` will be used in `SelectLock` when `needPartitionHandle` == false and `hasExtraCol` == true.
	if needPartitionHandle && !hasExtraCol {
		return needPartitionHandle, errors.Errorf("Internal error, needPartitionHandle != ret")
	}
	return needPartitionHandle, nil
}

func (w *partialIndexWorker) fetchHandles(
	ctx context.Context,
	results []distsql.SelectResult,
	exitCh <-chan struct{},
	fetchCh chan<- *indexMergeTableTask,
	finished <-chan struct{},
	handleCols plannerutil.HandleCols,
	partialPlanIndex int) (count int64, err error) {
	tps := w.getRetTpsForIndexScan(handleCols)
	chk := chunk.NewChunkWithCapacity(tps, w.maxChunkSize)
	for i := 0; i < len(results); {
		start := time.Now()
		handles, retChunk, err := w.extractTaskHandles(ctx, chk, results[i], handleCols)
		if err != nil {
			syncErr(ctx, finished, fetchCh, err)
			return count, err
		}
		if len(handles) == 0 {
			i++
			continue
		}
		count += int64(len(handles))
		task := w.buildTableTask(handles, retChunk, i, partialPlanIndex)
		if w.stats != nil {
			atomic.AddInt64(&w.stats.FetchIdxTime, int64(time.Since(start)))
		}
		select {
		case <-ctx.Done():
			return count, ctx.Err()
		case <-exitCh:
			return count, nil
		case <-finished:
			return count, nil
		case fetchCh <- task:
		}
	}
	return count, nil
}

func (w *partialIndexWorker) getRetTpsForIndexScan(handleCols plannerutil.HandleCols) []*types.FieldType {
	var tps []*types.FieldType
	if len(w.byItems) != 0 {
		for _, item := range w.byItems {
			tps = append(tps, item.Expr.GetType(w.sc.GetExprCtx().GetEvalCtx()))
		}
	}
	tps = append(tps, handleCols.GetFieldsTypes()...)
	if ok, _ := w.needPartitionHandle(); ok {
		tps = append(tps, types.NewFieldType(mysql.TypeLonglong))
	}
	return tps
}

func (w *partialIndexWorker) extractTaskHandles(ctx context.Context, chk *chunk.Chunk, idxResult distsql.SelectResult, handleCols plannerutil.HandleCols) (
	handles []kv.Handle, retChk *chunk.Chunk, err error) {
	handles = make([]kv.Handle, 0, w.batchSize)
	if len(w.byItems) != 0 {
		retChk = chunk.NewChunkWithCapacity(w.getRetTpsForIndexScan(handleCols), w.batchSize)
	}
	var memUsage int64
	var chunkRowOffset int
	defer w.memTracker.Consume(-memUsage)
	for len(handles) < w.batchSize {
		requiredRows := w.batchSize - len(handles)
		if w.pushedLimit != nil {
			if w.pushedLimit.Offset+w.pushedLimit.Count <= w.scannedKeys {
				return handles, retChk, nil
			}
			requiredRows = min(int(w.pushedLimit.Offset+w.pushedLimit.Count-w.scannedKeys), requiredRows)
		}
		chk.SetRequiredRows(requiredRows, w.maxChunkSize)
		start := time.Now()
		err = errors.Trace(idxResult.Next(ctx, chk))
		if err != nil {
			return nil, nil, err
		}
		if w.stats != nil && w.idxID != 0 {
			w.sc.GetSessionVars().StmtCtx.RuntimeStatsColl.GetBasicRuntimeStats(w.idxID, false).Record(time.Since(start), chk.NumRows())
		}
		if chk.NumRows() == 0 {
			failpoint.Inject("testIndexMergeErrorPartialIndexWorker", func(v failpoint.Value) {
				failpoint.Return(handles, nil, errors.New(v.(string)))
			})
			return handles, retChk, nil
		}
		memDelta := chk.MemoryUsage()
		memUsage += memDelta
		w.memTracker.Consume(memDelta)
		// We want both to reset chunkRowOffset and keep it up-to-date after the loop
		// nolint:intrange
		for chunkRowOffset = 0; chunkRowOffset < chk.NumRows(); chunkRowOffset++ {
			if w.pushedLimit != nil {
				w.scannedKeys++
				if w.scannedKeys > (w.pushedLimit.Offset + w.pushedLimit.Count) {
					// Skip the handles after Offset+Count.
					break
				}
			}
			var handle kv.Handle
			ok, err1 := w.needPartitionHandle()
			if err1 != nil {
				return nil, nil, err1
			}
			if ok {
				handle, err = handleCols.BuildPartitionHandleFromIndexRow(w.sc.GetSessionVars().StmtCtx, chk.GetRow(chunkRowOffset))
			} else {
				handle, err = handleCols.BuildHandleFromIndexRow(w.sc.GetSessionVars().StmtCtx, chk.GetRow(chunkRowOffset))
			}
			if err != nil {
				return nil, nil, err
			}
			handles = append(handles, handle)
		}
		// used for order by
		if len(w.byItems) != 0 {
			retChk.Append(chk, 0, chunkRowOffset)
		}
	}
	w.batchSize *= 2
	if w.batchSize > w.maxBatchSize {
		w.batchSize = w.maxBatchSize
	}
	return handles, retChk, nil
}

func (w *partialIndexWorker) buildTableTask(handles []kv.Handle, retChk *chunk.Chunk, parTblIdx int, partialPlanID int) *indexMergeTableTask {
	task := &indexMergeTableTask{
		lookupTableTask: lookupTableTask{
			handles: handles,
			idxRows: retChk,
		},
		parTblIdx:     parTblIdx,
		partialPlanID: partialPlanID,
	}

	if w.prunedPartitions != nil {
		task.partitionTable = w.prunedPartitions[parTblIdx]
	}

	task.doneCh = make(chan error, 1)
	return task
}

type indexMergeTableScanWorker struct {
	stats          *IndexMergeRuntimeStat
	workCh         <-chan *indexMergeTableTask
	finished       <-chan struct{}
	indexMergeExec *IndexMergeReaderExecutor
	tblPlans       []base.PhysicalPlan

	// memTracker is used to track the memory usage of this executor.
	memTracker *memory.Tracker
}

func (w *indexMergeTableScanWorker) pickAndExecTask(ctx context.Context, task **indexMergeTableTask) {
	var ok bool
	for {
		waitStart := time.Now()
		select {
		case <-ctx.Done():
			return
		case <-w.finished:
			return
		case *task, ok = <-w.workCh:
			if !ok {
				return
			}
		}
		// Make sure panic failpoint is after fetch task from workCh.
		// Otherwise, cannot send error to task.doneCh.
		failpoint.Inject("testIndexMergePanicTableScanWorker", nil)
		execStart := time.Now()
		err := w.executeTask(ctx, *task)
		if w.stats != nil {
			atomic.AddInt64(&w.stats.WaitTime, int64(execStart.Sub(waitStart)))
			atomic.AddInt64(&w.stats.FetchRow, int64(time.Since(execStart)))
			atomic.AddInt64(&w.stats.TableTaskNum, 1)
		}
		failpoint.Inject("testIndexMergePickAndExecTaskPanic", nil)
		select {
		case <-ctx.Done():
			return
		case <-w.finished:
			return
		case (*task).doneCh <- err:
		}
	}
}

func (*indexMergeTableScanWorker) handleTableScanWorkerPanic(ctx context.Context, finished <-chan struct{}, task **indexMergeTableTask, worker string) func(r any) {
	return func(r any) {
		if r == nil {
			logutil.BgLogger().Debug("worker finish without panic", zap.String("worker", worker))
			return
		}

		err4Panic := errors.Errorf("%s: %v", worker, r)
		logutil.Logger(ctx).Warn(err4Panic.Error())
		if *task != nil {
			select {
			case <-ctx.Done():
				return
			case <-finished:
				return
			case (*task).doneCh <- err4Panic:
				return
			}
		}
	}
}

func (w *indexMergeTableScanWorker) executeTask(ctx context.Context, task *indexMergeTableTask) error {
	tbl := w.indexMergeExec.table
	if w.indexMergeExec.partitionTableMode && task.partitionTable != nil {
		tbl = task.partitionTable
	}
	tableReader, err := w.indexMergeExec.buildFinalTableReader(ctx, tbl, task.handles)
	if err != nil {
		logutil.Logger(ctx).Error("build table reader failed", zap.Error(err))
		return err
	}
	defer func() { terror.Log(exec.Close(tableReader)) }()
	task.memTracker = w.memTracker
	memUsage := int64(cap(task.handles) * 8)
	task.memUsage = memUsage
	task.memTracker.Consume(memUsage)
	handleCnt := len(task.handles)
	task.rows = make([]chunk.Row, 0, handleCnt)
	for {
		chk := exec.TryNewCacheChunk(tableReader)
		err = exec.Next(ctx, tableReader, chk)
		if err != nil {
			logutil.Logger(ctx).Warn("table reader fetch next chunk failed", zap.Error(err))
			return err
		}
		if chk.NumRows() == 0 {
			break
		}
		memUsage = chk.MemoryUsage()
		task.memUsage += memUsage
		task.memTracker.Consume(memUsage)
		iter := chunk.NewIterator4Chunk(chk)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			task.rows = append(task.rows, row)
		}
	}

	if w.indexMergeExec.keepOrder {
		// Because len(outputOffsets) == tableScan.Schema().Len(),
		// so we could use row.GetInt64(idx) to get partition ID here.
		// TODO: We could add plannercore.PartitionHandleCols to unify them.
		physicalTableIDIdx := -1
		for i, c := range w.indexMergeExec.Schema().Columns {
			if c.ID == model.ExtraPhysTblID {
				physicalTableIDIdx = i
				break
			}
		}
		task.rowIdx = make([]int, 0, len(task.rows))
		for _, row := range task.rows {
			handle, err := w.indexMergeExec.handleCols.BuildHandle(w.indexMergeExec.Ctx().GetSessionVars().StmtCtx, row)
			if err != nil {
				return err
			}
			if w.indexMergeExec.partitionTableMode && physicalTableIDIdx != -1 {
				handle = kv.NewPartitionHandle(row.GetInt64(physicalTableIDIdx), handle)
			}
			rowIdx, _ := task.indexOrder.Get(handle)
			task.rowIdx = append(task.rowIdx, rowIdx.(int))
		}
		sort.Sort(task)
	}

	memUsage = int64(cap(task.rows)) * int64(unsafe.Sizeof(chunk.Row{}))
	task.memUsage += memUsage
	task.memTracker.Consume(memUsage)
	if handleCnt != len(task.rows) && len(w.tblPlans) == 1 {
		return errors.Errorf("handle count %d isn't equal to value count %d", handleCnt, len(task.rows))
	}
	return nil
}

// IndexMergeRuntimeStat record the indexMerge runtime stat
type IndexMergeRuntimeStat struct {
	IndexMergeProcess time.Duration
	FetchIdxTime      int64
	WaitTime          int64
	FetchRow          int64
	TableTaskNum      int64
	Concurrency       int
}

func (e *IndexMergeRuntimeStat) String() string {
	var buf bytes.Buffer
	if e.FetchIdxTime != 0 {
		buf.WriteString(fmt.Sprintf("index_task:{fetch_handle:%s", time.Duration(e.FetchIdxTime)))
		if e.IndexMergeProcess != 0 {
			buf.WriteString(fmt.Sprintf(", merge:%s", e.IndexMergeProcess))
		}
		buf.WriteByte('}')
	}
	if e.FetchRow != 0 {
		if buf.Len() > 0 {
			buf.WriteByte(',')
		}
		fmt.Fprintf(&buf, " table_task:{num:%d, concurrency:%d, fetch_row:%s, wait_time:%s}", e.TableTaskNum, e.Concurrency, time.Duration(e.FetchRow), time.Duration(e.WaitTime))
	}
	return buf.String()
}

// Clone implements the RuntimeStats interface.
func (e *IndexMergeRuntimeStat) Clone() execdetails.RuntimeStats {
	newRs := *e
	return &newRs
}

// Merge implements the RuntimeStats interface.
func (e *IndexMergeRuntimeStat) Merge(other execdetails.RuntimeStats) {
	tmp, ok := other.(*IndexMergeRuntimeStat)
	if !ok {
		return
	}
	e.IndexMergeProcess += tmp.IndexMergeProcess
	e.FetchIdxTime += tmp.FetchIdxTime
	e.FetchRow += tmp.FetchRow
	e.WaitTime += e.WaitTime
	e.TableTaskNum += tmp.TableTaskNum
}

// Tp implements the RuntimeStats interface.
func (*IndexMergeRuntimeStat) Tp() int {
	return execdetails.TpIndexMergeRunTimeStats
}
