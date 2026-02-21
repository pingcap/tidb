// Copyright 2022 PingCAP, Inc.
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
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/ranger"
	handleutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

func (e *AnalyzeColumnsExecV2) subMergeWorker(resultCh chan<- *samplingMergeResult, taskCh <-chan []byte, l int, index int) {
	// Only close the resultCh in the first worker.
	closeTheResultCh := index == 0
	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Warn("analyze worker panicked", zap.Any("recover", r), zap.Stack("stack"))
			metrics.PanicCounter.WithLabelValues(metrics.LabelAnalyze).Inc()
			resultCh <- &samplingMergeResult{err: getAnalyzePanicErr(r)}
		}
		// Consume the remaining things.
		for {
			_, ok := <-taskCh
			if !ok {
				break
			}
		}
		e.samplingMergeWg.Done()
		if closeTheResultCh {
			e.samplingMergeWg.Wait()
			close(resultCh)
		}
	}()
	failpoint.Inject("mockAnalyzeSamplingMergeWorkerPanic", func() {
		panic("failpoint triggered")
	})
	failpoint.Inject("mockAnalyzeMergeWorkerSlowConsume", func(val failpoint.Value) {
		times := val.(int)
		for range times {
			e.memTracker.Consume(5 << 20)
			time.Sleep(100 * time.Millisecond)
		}
	})
	retCollector := statistics.NewRowSampleCollector(int(e.analyzePB.ColReq.SampleSize), e.analyzePB.ColReq.GetSampleRate(), l)
	for range l {
		retCollector.Base().FMSketches = append(retCollector.Base().FMSketches, statistics.NewFMSketch(statistics.MaxSketchSize))
	}
	statsHandle := domain.GetDomain(e.ctx).StatsHandle()
	for {
		data, ok := <-taskCh
		if !ok {
			break
		}

		// Unmarshal the data.
		dataSize := int64(cap(data))
		colResp := &tipb.AnalyzeColumnsResp{}
		err := colResp.Unmarshal(data)
		if err != nil {
			resultCh <- &samplingMergeResult{err: err}
			return
		}
		// Consume the memory of the data.
		colRespSize := int64(colResp.Size())
		e.memTracker.Consume(colRespSize)

		// Update processed rows.
		subCollector := statistics.NewRowSampleCollector(int(e.analyzePB.ColReq.SampleSize), e.analyzePB.ColReq.GetSampleRate(), l)
		subCollector.Base().FromProto(colResp.RowCollector, e.memTracker)
		statsHandle.UpdateAnalyzeJobProgress(e.job, subCollector.Base().Count)

		// Print collect log.
		oldRetCollectorSize := retCollector.Base().MemSize
		oldRetCollectorCount := retCollector.Base().Count
		retCollector.MergeCollector(subCollector)
		newRetCollectorCount := retCollector.Base().Count
		printAnalyzeMergeCollectorLog(oldRetCollectorCount, newRetCollectorCount, subCollector.Base().Count,
			e.tableID.TableID, e.tableID.PartitionID, e.TableID.IsPartitionTable(),
			"merge subCollector in concurrency in AnalyzeColumnsExecV2", index)

		// Consume the memory of the result.
		newRetCollectorSize := retCollector.Base().MemSize
		subCollectorSize := subCollector.Base().MemSize
		e.memTracker.Consume(newRetCollectorSize - oldRetCollectorSize - subCollectorSize)
		e.memTracker.Release(dataSize + colRespSize)
		subCollector.DestroyAndPutToPool()
	}

	resultCh <- &samplingMergeResult{collector: retCollector}
}

func (e *AnalyzeColumnsExecV2) subBuildWorker(resultCh chan error, taskCh chan *samplingBuildTask, hists []*statistics.Histogram, topns []*statistics.TopN, collectors []*statistics.SampleCollector, exitCh chan struct{}) {
	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Warn("analyze worker panicked", zap.Any("recover", r), zap.Stack("stack"))
			metrics.PanicCounter.WithLabelValues(metrics.LabelAnalyze).Inc()
			resultCh <- getAnalyzePanicErr(r)
		}
	}()
	failpoint.Inject("mockAnalyzeSamplingBuildWorkerPanic", func() {
		panic("failpoint triggered")
	})

	colLen := len(e.colsInfo)

workLoop:
	for {
		select {
		case task, ok := <-taskCh:
			if !ok {
				break workLoop
			}
			// Track per-task allocations: curBufferedMemSize is pending charges to the tracker,
			// totalBuffered accumulates bytes that become part of collector.MemSize.
			curBufferedMemSize := int64(0)
			totalBuffered := int64(0)

			consumeBuffered := func(bytes int64) {
				totalBuffered += bytes
				e.memTracker.BufferedConsume(&curBufferedMemSize, bytes)
			}
			flushBuffered := func(cum *int64) {
				if curBufferedMemSize != 0 {
					e.memTracker.Consume(curBufferedMemSize)
					curBufferedMemSize = 0
				}
				*cum += totalBuffered
				totalBuffered = 0
			}
			var collector *statistics.SampleCollector
			if task.isColumn {
				if e.colsInfo[task.slicePos].IsGenerated() && !e.colsInfo[task.slicePos].GeneratedStored {
					hists[task.slicePos] = nil
					topns[task.slicePos] = nil
					continue
				}
				sampleNum := task.rootRowCollector.Base().Samples.Len()
				sampleItems := make([]*statistics.SampleItem, 0, sampleNum)
				// consume mandatory memory at the beginning, including empty SampleItems of all sample rows, if exceeds, fast fail
				// 8 means the pointer size of sampleItems slice.
				// types.EmptyDatumSize means the empty datum size we shallow copied from row.Columns to SampleItem.Value.
				// The real underlying byte slice of Datum in row.Columns has already be accounted FromProto().
				collectorMemSize := int64(sampleNum) * (8 + statistics.EmptySampleItemSize + types.EmptyDatumSize)
				e.memTracker.Consume(collectorMemSize)
				var collator collate.Collator
				ft := e.colsInfo[task.slicePos].FieldType
				// When it's new collation data, we need to use its collate key instead of original value because only
				// the collate key can ensure the correct ordering.
				// This is also corresponding to similar operation in (*statistics.Column).GetColumnRowCount().
				if ft.EvalType() == types.ETString && ft.GetType() != mysql.TypeEnum && ft.GetType() != mysql.TypeSet {
					collator = collate.GetCollator(ft.GetCollate())
				}
				for j, row := range task.rootRowCollector.Base().Samples {
					if row.Columns[task.slicePos].IsNull() {
						continue
					}
					val := row.Columns[task.slicePos]
					// If this value is very big, we think that it is not a value that can occur many times. So we don't record it.
					if len(val.GetBytes()) > statistics.MaxSampleValueLength {
						continue
					}
					if collator != nil {
						val.SetBytes(collator.Key(val.GetString()))
						deltaSize := int64(cap(val.GetBytes()))
						consumeBuffered(deltaSize)
					}
					sampleItems = append(sampleItems, &statistics.SampleItem{
						Value:   &val,
						Ordinal: j,
					})
				}
				flushBuffered(&collectorMemSize)
				collector = &statistics.SampleCollector{
					Samples:   sampleItems,
					NullCount: task.rootRowCollector.Base().NullCount[task.slicePos],
					Count:     task.rootRowCollector.Base().Count - task.rootRowCollector.Base().NullCount[task.slicePos],
					FMSketch:  task.rootRowCollector.Base().FMSketches[task.slicePos],
					TotalSize: task.rootRowCollector.Base().TotalSizes[task.slicePos],
					MemSize:   collectorMemSize,
				}
			} else {
				var tmpDatum types.Datum
				var err error
				idx := e.indexes[task.slicePos-colLen]
				sampleNum := task.rootRowCollector.Base().Samples.Len()
				sampleItems := make([]*statistics.SampleItem, 0, sampleNum)
				// consume mandatory memory at the beginning, including all SampleItems, if exceeds, fast fail
				// 8 is size of reference, 8 is the size of "b := make([]byte, 0, 8)"
				// types.EmptyDatumSize: same meaning as above branch.
				collectorMemSize := int64(sampleNum) * (8 + statistics.EmptySampleItemSize + 8 + types.EmptyDatumSize)
				e.memTracker.Consume(collectorMemSize)
				errCtx := e.ctx.GetSessionVars().StmtCtx.ErrCtx()
			indexSampleCollectLoop:
				for _, row := range task.rootRowCollector.Base().Samples {
					if len(idx.Columns) == 1 && row.Columns[idx.Columns[0].Offset].IsNull() {
						continue
					}
					b := make([]byte, 0, 8)
					for _, col := range idx.Columns {
						// If the index value contains one value which is too long, we think that it's a value that doesn't occur many times.
						if len(row.Columns[col.Offset].GetBytes()) > statistics.MaxSampleValueLength {
							continue indexSampleCollectLoop
						}
						if col.Length != types.UnspecifiedLength {
							row.Columns[col.Offset].Copy(&tmpDatum)
							ranger.CutDatumByPrefixLen(&tmpDatum, col.Length, &e.colsInfo[col.Offset].FieldType)
							b, err = codec.EncodeKey(e.ctx.GetSessionVars().StmtCtx.TimeZone(), b, tmpDatum)
							err = errCtx.HandleError(err)
							if err != nil {
								resultCh <- err
								continue workLoop
							}
							continue
						}
						b, err = codec.EncodeKey(e.ctx.GetSessionVars().StmtCtx.TimeZone(), b, row.Columns[col.Offset])
						err = errCtx.HandleError(err)
						if err != nil {
							resultCh <- err
							continue workLoop
						}
					}
					if cap(b) > 8 {
						// We already accounted 8 bytes before the loop started,
						// here we need to account the remaining bytes.
						consumeBuffered(int64(cap(b) - 8))
					}
					tmp := types.NewBytesDatum(b)
					sampleItems = append(sampleItems, &statistics.SampleItem{
						Value: &tmp,
					})
				}
				flushBuffered(&collectorMemSize)
				collector = &statistics.SampleCollector{
					Samples:   sampleItems,
					NullCount: task.rootRowCollector.Base().NullCount[task.slicePos],
					Count:     task.rootRowCollector.Base().Count - task.rootRowCollector.Base().NullCount[task.slicePos],
					FMSketch:  task.rootRowCollector.Base().FMSketches[task.slicePos],
					TotalSize: task.rootRowCollector.Base().TotalSizes[task.slicePos],
					MemSize:   collectorMemSize,
				}
			}
			if task.isColumn {
				collectors[task.slicePos] = collector
			}
			releaseCollectorMemory := func() {
				if !task.isColumn {
					e.memTracker.Release(collector.MemSize)
				}
			}
			hist, topn, err := statistics.BuildHistAndTopN(e.ctx, int(e.opts[ast.AnalyzeOptNumBuckets]), int(e.opts[ast.AnalyzeOptNumTopN]), task.id, collector, task.tp, task.isColumn, e.memTracker, e.ctx.GetSessionVars().EnableExtendedStats)
			if err != nil {
				resultCh <- err
				releaseCollectorMemory()
				continue
			}
			finalMemSize := hist.MemoryUsage() + topn.MemoryUsage()
			e.memTracker.Consume(finalMemSize)
			hists[task.slicePos] = hist
			topns[task.slicePos] = topn
			resultCh <- nil
			releaseCollectorMemory()
		case <-exitCh:
			return
		}
	}
}

type analyzeIndexNDVTotalResult struct {
	results map[int64]*statistics.AnalyzeResults
	err     error
}

type samplingMergeResult struct {
	collector statistics.RowSampleCollector
	err       error
}

type samplingBuildTask struct {
	id               int64
	rootRowCollector statistics.RowSampleCollector
	tp               *types.FieldType
	isColumn         bool
	slicePos         int
}

func readDataAndSendTask(ctx sessionctx.Context, handler *tableResultHandler, mergeTaskCh chan []byte, memTracker *memory.Tracker) error {
	// After all tasks are sent, close the mergeTaskCh to notify the mergeWorker that all tasks have been sent.
	defer close(mergeTaskCh)
	for {
		failpoint.Inject("mockKillRunningV2AnalyzeJob", func() {
			dom := domain.GetDomain(ctx)
			for _, id := range handleutil.GlobalAutoAnalyzeProcessList.All() {
				dom.SysProcTracker().KillSysProcess(id)
			}
		})
		if err := ctx.GetSessionVars().SQLKiller.HandleSignal(); err != nil {
			return err
		}
		failpoint.Inject("mockSlowAnalyzeV2", func() {
			time.Sleep(1000 * time.Second)
		})

		data, err := handler.nextRaw(context.TODO())
		if err != nil {
			return errors.Trace(err)
		}
		if data == nil {
			break
		}

		memTracker.Consume(int64(cap(data)))
		mergeTaskCh <- data
	}

	return nil
}
