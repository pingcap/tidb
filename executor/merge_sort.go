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
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"github.com/pingcap/tidb/util"
	"sort"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/memory"
)

// MergeSortExec represents sorting executor.
type MergeSortExec struct {
	baseExecutor

	ByItems []*plannercore.ByItems
	fetched bool
	schema  *expression.Schema

	keyExprs []expression.Expression
	keyTypes []*types.FieldType
	// keyColumns is the column index of the by items.
	keyColumns []int
	// keyCmpFuncs is used to compare each ByItem.
	keyCmpFuncs []chunk.CompareFunc
	// keyChunks is used to store ByItems values when not all ByItems are column.
	keyChunks *chunk.List
	// rowChunks is the chunks to store row values.
	rowChunks *chunk.List
	// rowPointer store the chunk index and row index for each row.
	workerRowPtrs []*[]chunk.RowPtr

	workerRowLen []int
	workerRowIdx []int

	memTracker    *memory.Tracker
	concurrency   int
	allColumnExpr bool
}

type SortWorker struct {
	MergeSortExec
	chkIdx  int
	rowIdx  int
	len     int
	rowPtrs []chunk.RowPtr
}

func (sw *SortWorker) run() {
	//sw.memTracker.Consume(int64(8 * sw.rowChunks.Len()))
	for chkIdx := sw.chkIdx; chkIdx < sw.rowChunks.NumChunks() && len(sw.rowPtrs) < sw.len; chkIdx++ {
		rowChk := sw.rowChunks.GetChunk(chkIdx)
		rowIdx := 0
		if chkIdx == sw.chkIdx {
			rowIdx = sw.rowIdx
		}
		for ; rowIdx < rowChk.NumRows() && len(sw.rowPtrs) < sw.len; rowIdx++ {
			sw.rowPtrs = append(sw.rowPtrs, chunk.RowPtr{ChkIdx: uint32(chkIdx), RowIdx: uint32(rowIdx)})
		}
	}
	if sw.allColumnExpr {
		sort.Slice(sw.rowPtrs, sw.keyColumnsLess)
	} else {
		sort.Slice(sw.rowPtrs, sw.keyChunksLess)
	}
	return
}

// Close implements the Executor Close interface.
func (e *MergeSortExec) Close() error {
	e.memTracker.Detach()
	e.memTracker = nil
	return errors.Trace(e.children[0].Close())
}

// Open implements the Executor Open interface.
func (e *MergeSortExec) Open(ctx context.Context) error {
	e.fetched = false
	e.concurrency = e.ctx.GetSessionVars().MergeSortConcurrency

	e.workerRowIdx = make([]int, e.concurrency)
	e.workerRowLen = make([]int, e.concurrency)
	e.workerRowPtrs = make([]*[]chunk.RowPtr, e.concurrency)
	// To avoid duplicated initialization for TopNExec.
	if e.memTracker == nil {
		e.memTracker = memory.NewTracker(e.id, e.ctx.GetSessionVars().MemQuotaSort)
		e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)
	}
	return errors.Trace(e.children[0].Open(ctx))
}

func (e *MergeSortExec) newSortWorker(workerId, chk, row, len int) *SortWorker {
	sw := &SortWorker{
		MergeSortExec: *e,
		chkIdx:        chk,
		rowIdx:        row,
		len:           len,
		rowPtrs:       make([]chunk.RowPtr, 0, len),
	}
	e.workerRowLen[workerId] = len
	e.workerRowIdx[workerId] = 0
	e.workerRowPtrs[workerId] = &sw.rowPtrs
	return sw
}

func (e *MergeSortExec) wait4WorkerSort(wg *sync.WaitGroup, finishedCh chan struct{}) {
	wg.Wait()
	close(finishedCh)
}

//sortWorkerIndex calc the chunk index and row index with every worker start to sort, first column of swIdx is chunk idx, second columm of swIdx is row idx
func (e *MergeSortExec) sortWorkerIndex(workerRowsCount int) [][]int {
	chkIdx := 0
	rowIdx := 0
	swIdx := make([][]int, e.concurrency)
	swIdx[0] = []int{0, 0}
	for i := 1; i < e.concurrency; i++ {
		count := 0
		swIdx[i] = []int{0, 0}
		for j := chkIdx; j < e.rowChunks.NumChunks(); j++ {
			curChk := e.rowChunks.GetChunk(j)
			count += curChk.NumRows()
			if j == chkIdx {
				count -= rowIdx
			}
			if count > workerRowsCount {
				rowIdx = curChk.NumRows() - (count - workerRowsCount)
				chkIdx = j
				swIdx[i] = []int{chkIdx, rowIdx}
				break
			}
		}
	}
	return swIdx
}

// Next implements the Executor Next interface.
func (e *MergeSortExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("sort.Next", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}
	if e.runtimeStats != nil {
		start := time.Now()
		defer func() { e.runtimeStats.Record(time.Since(start), req.NumRows()) }()
	}
	req.Reset()
	if !e.fetched {
		err := e.fetchRowChunks(ctx)
		if err != nil {
			return errors.Trace(err)
		}

		e.initCompareFuncs()
		e.allColumnExpr = e.buildKeyColumns()
		if !e.allColumnExpr {
			e.buildKeyExprsAndTypes()
			err := e.buildKeyChunks()
			if err != nil {
				return err
			}
		}
		finishedCh := make(chan struct{})

		workerRowsCount := e.rowChunks.Len() / e.concurrency
		workerIdx := e.sortWorkerIndex(workerRowsCount)

		wg := &sync.WaitGroup{}
		wg.Add(int(e.concurrency))
		go e.wait4WorkerSort(wg, finishedCh)

		for i := 0; i < e.concurrency; i++ {
			workerId := i
			// last worker must complete the rest of rows
			if i == e.concurrency-1 {
				workerRowsCount += e.rowChunks.Len() % e.concurrency
			}
			sw := e.newSortWorker(workerId, workerIdx[i][0], workerIdx[i][1], workerRowsCount)
			go util.WithRecovery(func() {
				defer wg.Done()
				sw.run()
			}, nil)
		}

		<-finishedCh
		e.fetched = true
	}

	for req.NumRows() < e.maxChunkSize {
		j := 0
		for j < e.concurrency && e.workerRowIdx[j] >= e.workerRowLen[j] {
			j++
		}
		if j >= e.concurrency {
			break
		}
		minRowPtr := (*e.workerRowPtrs[j])[e.workerRowIdx[j]]
		for i := j + 1; i < e.concurrency; i++ {
			if e.workerRowIdx[i] < e.workerRowLen[i] {
				flag := false
				if e.allColumnExpr {
					keyRowI := e.rowChunks.GetRow(minRowPtr)
					keyRowJ := e.rowChunks.GetRow((*e.workerRowPtrs[i])[e.workerRowIdx[i]])
					flag = e.lessRow(keyRowI, keyRowJ)
				} else {
					keyRowI := e.keyChunks.GetRow(minRowPtr)
					keyRowJ := e.keyChunks.GetRow((*e.workerRowPtrs[i])[e.workerRowIdx[i]])
					flag = e.lessRow(keyRowI, keyRowJ)
				}
				if !flag {
					minRowPtr = (*e.workerRowPtrs[i])[e.workerRowIdx[i]]
					j = i
				}
			}
		}
		e.workerRowIdx[j]++
		req.AppendRow(e.rowChunks.GetRow(minRowPtr))
	}
	return nil
}

func (e *MergeSortExec) fetchRowChunks(ctx context.Context) error {
	fields := e.retTypes()
	e.rowChunks = chunk.NewList(fields, e.initCap, e.maxChunkSize)
	e.rowChunks.GetMemTracker().AttachTo(e.memTracker)
	e.rowChunks.GetMemTracker().SetLabel("rowChunks")
	for {
		chk := e.children[0].newFirstChunk()
		err := e.children[0].Next(ctx, chunk.NewRecordBatch(chk))
		if err != nil {
			return errors.Trace(err)
		}
		rowCount := chk.NumRows()
		if rowCount == 0 {
			break
		}
		e.rowChunks.Add(chk)
	}
	return nil
}

func (e *MergeSortExec) initCompareFuncs() {
	e.keyCmpFuncs = make([]chunk.CompareFunc, len(e.ByItems))
	for i := range e.ByItems {
		keyType := e.ByItems[i].Expr.GetType()
		e.keyCmpFuncs[i] = chunk.GetCompareFunc(keyType)
	}
}

func (e *MergeSortExec) buildKeyColumns() (allColumnExpr bool) {
	e.keyColumns = make([]int, 0, len(e.ByItems))
	for _, by := range e.ByItems {
		if col, ok := by.Expr.(*expression.Column); ok {
			e.keyColumns = append(e.keyColumns, col.Index)
		} else {
			e.keyColumns = e.keyColumns[:0]
			for i := range e.ByItems {
				e.keyColumns = append(e.keyColumns, i)
			}
			return false
		}
	}
	return true
}

func (e *MergeSortExec) buildKeyExprsAndTypes() {
	keyLen := len(e.ByItems)
	e.keyTypes = make([]*types.FieldType, keyLen)
	e.keyExprs = make([]expression.Expression, keyLen)
	for keyColIdx := range e.ByItems {
		e.keyExprs[keyColIdx] = e.ByItems[keyColIdx].Expr
		e.keyTypes[keyColIdx] = e.ByItems[keyColIdx].Expr.GetType()
	}
}

func (e *MergeSortExec) buildKeyChunks() error {
	e.keyChunks = chunk.NewList(e.keyTypes, e.initCap, e.maxChunkSize)
	e.keyChunks.GetMemTracker().SetLabel("keyChunks")
	e.keyChunks.GetMemTracker().AttachTo(e.memTracker)

	for chkIdx := 0; chkIdx < e.rowChunks.NumChunks(); chkIdx++ {
		keyChk := chunk.NewChunkWithCapacity(e.keyTypes, e.rowChunks.GetChunk(chkIdx).NumRows())
		childIter := chunk.NewIterator4Chunk(e.rowChunks.GetChunk(chkIdx))
		err := expression.VectorizedExecute(e.ctx, e.keyExprs, childIter, keyChk)
		if err != nil {
			return errors.Trace(err)
		}
		e.keyChunks.Add(keyChk)
	}
	return nil
}

func (e *MergeSortExec) lessRow(rowI, rowJ chunk.Row) bool {
	for i, colIdx := range e.keyColumns {
		cmpFunc := e.keyCmpFuncs[i]
		cmp := cmpFunc(rowI, colIdx, rowJ, colIdx)
		if e.ByItems[i].Desc {
			cmp = -cmp
		}
		if cmp < 0 {
			return true
		} else if cmp > 0 {
			return false
		}
	}
	return false
}

// keyColumnsLess is the less function for key columns.
func (e *SortWorker) keyColumnsLess(i, j int) bool {
	rowI := e.rowChunks.GetRow(e.rowPtrs[i])
	rowJ := e.rowChunks.GetRow(e.rowPtrs[j])
	return e.lessRow(rowI, rowJ)
}

// keyChunksLess is the less function for key chunk.
func (e *SortWorker) keyChunksLess(i, j int) bool {
	keyRowI := e.keyChunks.GetRow(e.rowPtrs[i])
	keyRowJ := e.keyChunks.GetRow(e.rowPtrs[j])
	return e.lessRow(keyRowI, keyRowJ)
}
