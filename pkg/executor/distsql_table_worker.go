// Copyright 2017 PingCAP, Inc.
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
	"runtime/trace"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/logutil/consistency"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/size"
	"go.uber.org/zap"
)

func execTableTask(e *IndexLookUpExecutor, task *lookupTableTask) {
	var (
		ctx    = e.workerCtx
		region *trace.Region
	)
	if trace.IsEnabled() {
		region = trace.StartRegion(ctx, "IndexLookUpTableTask"+strconv.Itoa(task.id))
	}
	defer func() {
		if r := recover(); r != nil {
			logutil.Logger(ctx).Warn("TableWorker in IndexLookUpExecutor panicked", zap.Any("recover", r), zap.Stack("stack"))
			err := util.GetRecoverError(r)
			task.doneCh <- err
		}
		if region != nil {
			region.End()
		}
	}()
	tracker := memory.NewTracker(task.id, -1)
	tracker.AttachTo(e.memTracker)
	w := &tableWorker{
		idxLookup:       e,
		finished:        e.finished,
		keepOrder:       e.keepOrder,
		handleIdx:       e.handleIdx,
		checkIndexValue: e.checkIndexValue,
		memTracker:      tracker,
	}
	startTime := time.Now()
	err := w.executeTask(ctx, task)
	if e.stats != nil {
		atomic.AddInt64(&e.stats.TableRowScan, int64(time.Since(startTime)))
		atomic.AddInt64(&e.stats.TableTaskNum, 1)
	}
	task.doneCh <- err
}

// tableWorker is used by IndexLookUpExecutor to maintain table lookup background goroutines.
type tableWorker struct {
	idxLookup *IndexLookUpExecutor
	finished  <-chan struct{}
	keepOrder bool
	handleIdx []int

	// memTracker is used to track the memory usage of this executor.
	memTracker *memory.Tracker

	// checkIndexValue is used to check the consistency of the index data.
	*checkIndexValue
}

func (e *IndexLookUpExecutor) getHandle(row chunk.Row, handleIdx []int,
	isCommonHandle bool, tp getHandleType) (handle kv.Handle, err error) {
	if isCommonHandle {
		var handleEncoded []byte
		var datums []types.Datum
		for i, idx := range handleIdx {
			// If the new collation is enabled and the handle contains non-binary string,
			// the handle in the index is encoded as "sortKey". So we cannot restore its
			// original value(the primary key) here.
			// We use a trick to avoid encoding the "sortKey" again by changing the charset
			// collation to `binary`.
			rtp := e.handleCols[i].RetType
			if collate.NewCollationEnabled() && e.table.Meta().CommonHandleVersion == 0 && rtp.EvalType() == types.ETString &&
				!mysql.HasBinaryFlag(rtp.GetFlag()) && tp == getHandleFromIndex {
				rtp = rtp.Clone()
				rtp.SetCollate(charset.CollationBin)
				datums = append(datums, row.GetDatum(idx, rtp))
				continue
			}
			datums = append(datums, row.GetDatum(idx, e.handleCols[i].RetType))
		}
		tablecodec.TruncateIndexValues(e.table.Meta(), e.primaryKeyIndex, datums)
		ectx := e.ectx.GetEvalCtx()
		handleEncoded, err = codec.EncodeKey(ectx.Location(), nil, datums...)
		errCtx := ectx.ErrCtx()
		err = errCtx.HandleError(err)
		if err != nil {
			return nil, err
		}
		handle, err = kv.NewCommonHandle(handleEncoded)
		if err != nil {
			return nil, err
		}
	} else {
		if len(handleIdx) == 0 {
			handle = kv.IntHandle(row.GetInt64(0))
		} else {
			handle = kv.IntHandle(row.GetInt64(handleIdx[0]))
		}
	}
	ok, err := e.needPartitionHandle(tp)
	if err != nil {
		return nil, err
	}
	if ok {
		pid := row.GetInt64(row.Len() - 1)
		handle = kv.NewPartitionHandle(pid, handle)
	}
	return
}

// IndexLookUpRunTimeStats record the indexlookup runtime stat
type IndexLookUpRunTimeStats struct {
	// indexScanBasicStats uses to record basic runtime stats for index scan.
	indexScanBasicStats *execdetails.BasicRuntimeStats
	FetchHandleTotal    int64
	FetchHandle         int64
	TaskWait            int64
	TableRowScan        int64
	TableTaskNum        int64
	Concurrency         int
	// Record the `Next` call affected wait duration details.
	NextWaitIndexScan        time.Duration
	NextWaitTableLookUpBuild time.Duration
	NextWaitTableLookUpResp  time.Duration
}

func (e *IndexLookUpRunTimeStats) String() string {
	var buf bytes.Buffer
	fetchHandle := atomic.LoadInt64(&e.FetchHandleTotal)
	indexScan := atomic.LoadInt64(&e.FetchHandle)
	taskWait := atomic.LoadInt64(&e.TaskWait)
	tableScan := atomic.LoadInt64(&e.TableRowScan)
	tableTaskNum := atomic.LoadInt64(&e.TableTaskNum)
	concurrency := e.Concurrency
	if indexScan != 0 {
		buf.WriteString(fmt.Sprintf("index_task: {total_time: %s, fetch_handle: %s, build: %s, wait: %s}",
			execdetails.FormatDuration(time.Duration(fetchHandle)),
			execdetails.FormatDuration(time.Duration(indexScan)),
			execdetails.FormatDuration(time.Duration(fetchHandle-indexScan-taskWait)),
			execdetails.FormatDuration(time.Duration(taskWait))))
	}
	if tableScan != 0 {
		if buf.Len() > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(fmt.Sprintf(" table_task: {total_time: %v, num: %d, concurrency: %d}", execdetails.FormatDuration(time.Duration(tableScan)), tableTaskNum, concurrency))
	}
	if e.NextWaitIndexScan > 0 || e.NextWaitTableLookUpBuild > 0 || e.NextWaitTableLookUpResp > 0 {
		if buf.Len() > 0 {
			buf.WriteByte(',')
			fmt.Fprintf(&buf, " next: {wait_index: %s, wait_table_lookup_build: %s, wait_table_lookup_resp: %s}",
				execdetails.FormatDuration(e.NextWaitIndexScan),
				execdetails.FormatDuration(e.NextWaitTableLookUpBuild),
				execdetails.FormatDuration(e.NextWaitTableLookUpResp))
		}
	}
	return buf.String()
}

// Clone implements the RuntimeStats interface.
func (e *IndexLookUpRunTimeStats) Clone() execdetails.RuntimeStats {
	newRs := *e
	return &newRs
}

// Merge implements the RuntimeStats interface.
func (e *IndexLookUpRunTimeStats) Merge(other execdetails.RuntimeStats) {
	tmp, ok := other.(*IndexLookUpRunTimeStats)
	if !ok {
		return
	}
	e.FetchHandleTotal += tmp.FetchHandleTotal
	e.FetchHandle += tmp.FetchHandle
	e.TaskWait += tmp.TaskWait
	e.TableRowScan += tmp.TableRowScan
	e.TableTaskNum += tmp.TableTaskNum
	e.NextWaitIndexScan += tmp.NextWaitIndexScan
	e.NextWaitTableLookUpBuild += tmp.NextWaitTableLookUpBuild
	e.NextWaitTableLookUpResp += tmp.NextWaitTableLookUpResp
}

// Tp implements the RuntimeStats interface.
func (*IndexLookUpRunTimeStats) Tp() int {
	return execdetails.TpIndexLookUpRunTimeStats
}

func (w *tableWorker) compareData(ctx context.Context, task *lookupTableTask, tableReader exec.Executor) error {
	chk := exec.TryNewCacheChunk(tableReader)
	tblInfo := w.idxLookup.table.Meta()
	vals := make([]types.Datum, 0, len(w.idxTblCols))

	// Prepare collator for compare.
	collators := make([]collate.Collator, 0, len(w.idxColTps))
	for _, tp := range w.idxColTps {
		collators = append(collators, collate.GetCollator(tp.GetCollate()))
	}

	ir := func() *consistency.Reporter {
		return &consistency.Reporter{
			HandleEncode: func(handle kv.Handle) kv.Key {
				return tablecodec.EncodeRecordKey(w.idxLookup.table.RecordPrefix(), handle)
			},
			IndexEncode: func(idxRow *consistency.RecordData) kv.Key {
				var idx table.Index
				for _, v := range w.idxLookup.table.Indices() {
					if strings.EqualFold(v.Meta().Name.String(), w.idxLookup.index.Name.O) {
						idx = v
						break
					}
				}
				if idx == nil {
					return nil
				}
				ectx := w.idxLookup.ectx.GetEvalCtx()
				k, _, err := idx.GenIndexKey(ectx.ErrCtx(), ectx.Location(), idxRow.Values[:len(idx.Meta().Columns)], idxRow.Handle, nil)
				if err != nil {
					return nil
				}
				return k
			},
			Tbl:             tblInfo,
			Idx:             w.idxLookup.index,
			EnableRedactLog: w.idxLookup.enableRedactLog,
			Storage:         w.idxLookup.storage,
		}
	}

	for {
		err := exec.Next(ctx, tableReader, chk)
		if err != nil {
			return errors.Trace(err)
		}

		// If ctx is cancelled, `Next` may return empty result when the actual data is not empty. To avoid producing
		// false-positive error logs that cause confusion, exit in this case.
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if chk.NumRows() == 0 {
			task.indexOrder.Range(func(h kv.Handle, val any) bool {
				idxRow := task.idxRows.GetRow(val.(int))
				err = ir().ReportAdminCheckInconsistent(ctx, h, &consistency.RecordData{Handle: h, Values: getDatumRow(&idxRow, w.idxColTps)}, nil)
				return false
			})
			if err != nil {
				return err
			}
			break
		}

		iter := chunk.NewIterator4Chunk(chk)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			handle, err := w.idxLookup.getHandle(row, w.handleIdx, w.idxLookup.isCommonHandle(), getHandleFromTable)
			if err != nil {
				return err
			}
			v, ok := task.indexOrder.Get(handle)
			if !ok {
				v, _ = task.duplicatedIndexOrder.Get(handle)
			}
			offset, _ := v.(int)
			task.indexOrder.Delete(handle)
			idxRow := task.idxRows.GetRow(offset)
			vals = vals[:0]
			for i, col := range w.idxTblCols {
				vals = append(vals, row.GetDatum(i, &col.FieldType))
			}
			tablecodec.TruncateIndexValues(tblInfo, w.idxLookup.index, vals)
			tc := w.idxLookup.ectx.GetEvalCtx().TypeCtx()
			for i := range vals {
				col := w.idxTblCols[i]
				idxVal := idxRow.GetDatum(i, w.idxColTps[i])
				tablecodec.TruncateIndexValue(&idxVal, w.idxLookup.index.Columns[i], col.ColumnInfo)
				cmpRes, err := tables.CompareIndexAndVal(tc, vals[i], idxVal, collators[i], col.FieldType.IsArray() && vals[i].Kind() == types.KindMysqlJSON)
				if err != nil {
					return ir().ReportAdminCheckInconsistentWithColInfo(ctx,
						handle,
						col.Name.O,
						idxVal,
						vals[i],
						err,
						&consistency.RecordData{Handle: handle, Values: getDatumRow(&idxRow, w.idxColTps)},
					)
				}
				if cmpRes != 0 {
					return ir().ReportAdminCheckInconsistentWithColInfo(ctx,
						handle,
						col.Name.O,
						idxRow.GetDatum(i, w.idxColTps[i]),
						vals[i],
						err,
						&consistency.RecordData{Handle: handle, Values: getDatumRow(&idxRow, w.idxColTps)},
					)
				}
			}
		}
	}
	return nil
}

func getDatumRow(r *chunk.Row, fields []*types.FieldType) []types.Datum {
	datumRow := make([]types.Datum, 0, r.Chunk().NumCols())
	for colIdx := range r.Chunk().NumCols() {
		if colIdx >= len(fields) {
			break
		}
		datum := r.GetDatum(colIdx, fields[colIdx])
		datumRow = append(datumRow, datum)
	}
	return datumRow
}

// executeTask executes the table look up tasks. We will construct a table reader and send request by handles.
// Then we hold the returning rows and finish this task.
func (w *tableWorker) executeTask(ctx context.Context, task *lookupTableTask) error {
	tableReader, err := w.idxLookup.buildTableReader(ctx, task)
	task.buildDoneTime = time.Now()
	if err != nil {
		if ctx.Err() != context.Canceled {
			logutil.Logger(ctx).Error("build table reader failed", zap.Error(err))
		}
		return err
	}
	defer func() { terror.Log(exec.Close(tableReader)) }()

	if w.checkIndexValue != nil {
		return w.compareData(ctx, task, tableReader)
	}

	{
		task.memTracker = w.memTracker
		memUsage := int64(cap(task.handles))*size.SizeOfInterface + tableReader.memUsage()
		for _, h := range task.handles {
			memUsage += int64(h.MemUsage())
		}
		if task.indexOrder != nil {
			memUsage += task.indexOrder.MemUsage()
		}
		if task.duplicatedIndexOrder != nil {
			memUsage += task.duplicatedIndexOrder.MemUsage()
		}
		memUsage += task.idxRows.MemoryUsage()
		task.memUsage = memUsage
		task.memTracker.Consume(memUsage)
	}
	handleCnt := len(task.handles)
	task.rows = make([]chunk.Row, 0, handleCnt)
	for {
		chk := exec.TryNewCacheChunk(tableReader)
		err = exec.Next(ctx, tableReader, chk)
		if err != nil {
			if ctx.Err() != context.Canceled {
				logutil.Logger(ctx).Warn("table reader fetch next chunk failed", zap.Error(err))
			}
			return err
		}
		if chk.NumRows() == 0 {
			break
		}
		{
			memUsage := chk.MemoryUsage()
			task.memUsage += memUsage
			task.memTracker.Consume(memUsage)
		}
		iter := chunk.NewIterator4Chunk(chk)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			task.rows = append(task.rows, row)
		}
	}

	defer trace.StartRegion(ctx, "IndexLookUpTableCompute").End()
	{
		memUsage := int64(cap(task.rows)) * int64(unsafe.Sizeof(chunk.Row{}))
		task.memUsage += memUsage
		task.memTracker.Consume(memUsage)
	}
	if w.keepOrder {
		task.rowIdx = make([]int, 0, len(task.rows))
		for i := range task.rows {
			handle, err := w.idxLookup.getHandle(task.rows[i], w.handleIdx, w.idxLookup.isCommonHandle(), getHandleFromTable)
			if err != nil {
				return err
			}
			rowIdx, _ := task.indexOrder.Get(handle)
			task.rowIdx = append(task.rowIdx, rowIdx.(int))
		}
		{
			memUsage := int64(cap(task.rowIdx) * int(size.SizeOfInt))
			task.memUsage += memUsage
			task.memTracker.Consume(memUsage)
		}
		sort.Sort(task)
	}

	if handleCnt != len(task.rows) && !util.HasCancelled(ctx) &&
		!w.idxLookup.weakConsistency {
		if len(w.idxLookup.tblPlans) == 1 {
			obtainedHandlesMap := kv.NewHandleMap()
			for _, row := range task.rows {
				handle, err := w.idxLookup.getHandle(row, w.handleIdx, w.idxLookup.isCommonHandle(), getHandleFromTable)
				if err != nil {
					return err
				}
				obtainedHandlesMap.Set(handle, true)
			}
			missHds := GetLackHandles(task.handles, obtainedHandlesMap)
			return (&consistency.Reporter{
				HandleEncode: func(hd kv.Handle) kv.Key {
					return tablecodec.EncodeRecordKey(w.idxLookup.table.RecordPrefix(), hd)
				},
				Tbl:             w.idxLookup.table.Meta(),
				Idx:             w.idxLookup.index,
				EnableRedactLog: w.idxLookup.enableRedactLog,
				Storage:         w.idxLookup.storage,
			}).ReportLookupInconsistent(ctx,
				handleCnt,
				len(task.rows),
				missHds,
				task.handles,
				nil,
				//missRecords,
			)
		}
	}

	return nil
}

// GetLackHandles gets the handles in expectedHandles but not in obtainedHandlesMap.
func GetLackHandles(expectedHandles []kv.Handle, obtainedHandlesMap *kv.HandleMap) []kv.Handle {
	diffCnt := len(expectedHandles) - obtainedHandlesMap.Len()
	diffHandles := make([]kv.Handle, 0, diffCnt)
	var cnt int
	for _, handle := range expectedHandles {
		isExist := false
		if _, ok := obtainedHandlesMap.Get(handle); ok {
			obtainedHandlesMap.Delete(handle)
			isExist = true
		}
		if !isExist {
			diffHandles = append(diffHandles, handle)
			cnt++
			if cnt == diffCnt {
				break
			}
		}
	}

	return diffHandles
}
