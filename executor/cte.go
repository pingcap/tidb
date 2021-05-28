// Copyright 2021 PingCAP, Inc.
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
	"hash"
	"hash/fnv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/cteutil"
	"github.com/pingcap/tidb/util/memory"
)

var _ Executor = &CTEExec{}

// CTEExec implements CTE.
// Following diagram describes how CTEExec works.
//
// `iterInTbl` is shared by `CTEExec` and `CTETableReaderExec`.
// `CTETableReaderExec` reads data from `iterInTbl`,
// and its output will be stored `iterOutTbl` by `CTEExec`.
//
// When an iteration ends, `CTEExec` will move all data from `iterOutTbl` into `iterInTbl`,
// which will be the input for new iteration.
// At the end of each iteration, data in `iterOutTbl` will also be added into `resTbl`.
// `resTbl` stores data of all iteration.
//                                   +----------+
//                     write         |iterOutTbl|
//       CTEExec ------------------->|          |
//          |                        +----+-----+
//    -------------                       | write
//    |           |                       v
// other op     other op             +----------+
// (seed)       (recursive)          |  resTbl  |
//                  ^                |          |
//                  |                +----------+
//            CTETableReaderExec
//                   ^
//                   |  read         +----------+
//                   +---------------+iterInTbl |
//                                   |          |
//                                   +----------+
type CTEExec struct {
	baseExecutor

	seedExec      Executor
	recursiveExec Executor

	// `resTbl` and `iterInTbl` are shared by all CTEExec which reference to same the CTE.
	// `iterInTbl` is also shared by CTETableReaderExec.
	resTbl     cteutil.Storage
	iterInTbl  cteutil.Storage
	iterOutTbl cteutil.Storage

	resHashTbl    baseHashTable
	iterInHashTbl baseHashTable

	// Index of chunk to read from `resTbl`.
	chkIdx int

	// UNION ALL or UNION DISTINCT.
	isDistinct bool
	curIter    int
}

// Open implements the Executor interface.
func (e *CTEExec) Open(ctx context.Context) (err error) {
	e.reset()
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}

	if e.seedExec == nil {
		return errors.New("seedExec for CTEExec is nil")
	}
	if err = e.seedExec.Open(ctx); err != nil {
		return err
	}

	if e.recursiveExec != nil {
		if err = e.recursiveExec.Open(ctx); err != nil {
			return err
		}
		recursiveTypes := e.recursiveExec.base().retFieldTypes
		e.iterOutTbl = cteutil.NewStorageRC(recursiveTypes, e.maxChunkSize)
		if err = e.iterOutTbl.OpenAndRef(); err != nil {
			return err
		}

		setupCTEStorageTracker(e.iterOutTbl, e.ctx)
	}

	if e.isDistinct {
		e.resHashTbl = newConcurrentMapHashTable()
		e.iterInHashTbl = newConcurrentMapHashTable()
	}
	return nil
}

// Next implements the Executor interface.
func (e *CTEExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	req.Reset()
	e.resTbl.Lock()
	defer func() {
		e.resTbl.Unlock()
	}()
	if !e.resTbl.Done() {
		setupCTEStorageTracker(e.resTbl, e.ctx)
		setupCTEStorageTracker(e.iterInTbl, e.ctx)

		if err = e.computeSeedPart(ctx); err != nil {
			// Don't put it in defer.
			// Because it should be called only when the filling process is not completed.
			if err1 := e.reopenTbls(); err1 != nil {
				return err1
			}
			return err
		}
		if err = e.computeRecursivePart(ctx); err != nil {
			if err1 := e.reopenTbls(); err1 != nil {
				return err1
			}
			return err
		}
	}
	e.resTbl.SetDone()

	if e.chkIdx < e.resTbl.NumChunks() {
		res, err := e.resTbl.GetChunk(e.chkIdx)
		if err != nil {
			return err
		}
		// Need to copy chunk to make sure upper operator will not change chunk in resTbl.
		req.SwapColumns(res.CopyConstruct())
		e.chkIdx++
	}
	return nil
}

// Close implements the Executor interface.
func (e *CTEExec) Close() (err error) {
	e.reset()
	if err = e.seedExec.Close(); err != nil {
		return err
	}
	if e.recursiveExec != nil {
		if err = e.recursiveExec.Close(); err != nil {
			return err
		}
	}

	// `iterInTbl` and `resTbl` are shared by multiple operators,
	// so will be closed when the SQL finishes.
	if err = e.iterOutTbl.DerefAndClose(); err != nil {
		return err
	}
	return e.baseExecutor.Close()
}

func (e *CTEExec) computeSeedPart(ctx context.Context) (err error) {
	e.curIter = 0
	e.iterInTbl.SetIter(e.curIter)
	for {
		chk := newFirstChunk(e.seedExec)
		if err = Next(ctx, e.seedExec, chk); err != nil {
			return err
		}
		if chk.NumRows() == 0 {
			break
		}
		if chk, err = e.tryFilterAndAdd(chk, e.iterInTbl, e.iterInHashTbl); err != nil {
			return err
		}
		if _, err = e.tryFilterAndAdd(chk, e.resTbl, e.resHashTbl); err != nil {
			return err
		}
	}
	e.curIter++
	e.iterInTbl.SetIter(e.curIter)

	// This means iterInTbl's can be read.
	close(e.iterInTbl.GetBegCh())
	return nil
}

func (e *CTEExec) computeRecursivePart(ctx context.Context) (err error) {
	if e.recursiveExec == nil || e.iterInTbl.NumChunks() == 0 {
		return nil
	}

	if e.curIter > e.ctx.GetSessionVars().CTEMaxRecursionDepth {
		return ErrCTEMaxRecursionDepth.GenWithStackByArgs(e.curIter)
	}

	for {
		chk := newFirstChunk(e.recursiveExec)
		if err = Next(ctx, e.recursiveExec, chk); err != nil {
			return err
		}
		if chk.NumRows() == 0 {
			if err = e.setupTblsForNewIteration(); err != nil {
				return err
			}
			if e.iterInTbl.NumChunks() == 0 {
				break
			}
			// Next iteration begins. Need use iterOutTbl as input of next iteration.
			e.curIter++
			e.iterInTbl.SetIter(e.curIter)
			if e.curIter > e.ctx.GetSessionVars().CTEMaxRecursionDepth {
				return ErrCTEMaxRecursionDepth.GenWithStackByArgs(e.curIter)
			}
			// Make sure iterInTbl is setup before Close/Open,
			// because some executors will read iterInTbl in Open() (like IndexLookupJoin).
			if err = e.recursiveExec.Close(); err != nil {
				return err
			}
			if err = e.recursiveExec.Open(ctx); err != nil {
				return err
			}
		} else {
			if err = e.iterOutTbl.Add(chk); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *CTEExec) setupTblsForNewIteration() (err error) {
	num := e.iterOutTbl.NumChunks()
	chks := make([]*chunk.Chunk, 0, num)
	// Setup resTbl's data.
	for i := 0; i < num; i++ {
		chk, err := e.iterOutTbl.GetChunk(i)
		if err != nil {
			return err
		}
		chk, err = e.tryFilterAndAdd(chk, e.resTbl, e.resHashTbl)
		if err != nil {
			return err
		}
		chks = append(chks, chk)
	}

	// Setup new iteration data in iterInTbl.
	if err = e.iterInTbl.Reopen(); err != nil {
		return err
	}
	if e.isDistinct {
		for _, chk := range chks {
			if _, err = e.tryFilterAndAdd(chk, e.iterInTbl, e.iterInHashTbl); err != nil {
				return err
			}
		}
	} else {
		if err = e.iterInTbl.SwapData(e.iterOutTbl); err != nil {
			return err
		}
	}
	close(e.iterInTbl.GetBegCh())

	// Clear data in iterOutTbl.
	return e.iterOutTbl.Reopen()
}

func (e *CTEExec) reset() {
	e.curIter = 0
	e.chkIdx = 0
	e.iterInHashTbl = nil
	e.resHashTbl = nil
}

func (e *CTEExec) reopenTbls() (err error) {
	if err := e.resTbl.Reopen(); err != nil {
		return err
	}
	return e.iterInTbl.Reopen()
}

func setupCTEStorageTracker(tbl cteutil.Storage, ctx sessionctx.Context) {
	memTracker := tbl.GetMemTracker()
	memTracker.SetLabel(memory.LabelForCTEStorage)
	memTracker.AttachTo(ctx.GetSessionVars().StmtCtx.MemTracker)

	diskTracker := tbl.GetDiskTracker()
	diskTracker.SetLabel(memory.LabelForCTEStorage)
	diskTracker.AttachTo(ctx.GetSessionVars().StmtCtx.DiskTracker)

	if config.GetGlobalConfig().OOMUseTmpStorage {
		actionSpill := tbl.ActionSpill()
		ctx.GetSessionVars().StmtCtx.MemTracker.FallbackOldAndSetNewAction(actionSpill)
	}
}

func (e *CTEExec) tryFilterAndAdd(chk *chunk.Chunk,
	storage cteutil.Storage,
	hashtbl baseHashTable) (res *chunk.Chunk, err error) {
	if e.isDistinct {
		if chk, err = e.filterAndAddHashTable(chk, storage, hashtbl); err != nil {
			return nil, err
		}
	}
	if chk.NumRows() == 0 {
		return chk, nil
	}
	return chk, storage.Add(chk)
}

func (e *CTEExec) filterAndAddHashTable(chk *chunk.Chunk,
	storage cteutil.Storage,
	hashtbl baseHashTable) (finalChkNoDup *chunk.Chunk, err error) {
	rows := chk.NumRows()
	if rows == 0 {
		return chk, nil
	}

	buf := make([]byte, 1)
	isNull := make([]bool, rows)
	hasher := make([]hash.Hash64, rows)
	for i := 0; i < rows; i++ {
		// New64() just returns a int64 constant,
		// but we can avoid calling it every time this func is called.
		hasher[i] = fnv.New64()
	}

	tps := e.base().retFieldTypes
	for i := 0; i < chk.NumCols(); i++ {
		err = codec.HashChunkColumns(e.ctx.GetSessionVars().StmtCtx, hasher, chk, tps[i], i, buf, isNull)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	tmpChkNoDup := chunk.NewChunkWithCapacity(tps, chk.Capacity())
	chkHashTbl := newConcurrentMapHashTable()
	idxForOriRows := make([]int, 0, chk.NumRows())

	// filter rows duplicated in cur chk
	for i := 0; i < rows; i++ {
		key := hasher[i].Sum64()
		row := chk.GetRow(i)

		hasDup, err := e.checkHasDup(key, row, chk, storage, chkHashTbl)
		if err != nil {
			return nil, err
		}
		if hasDup {
			continue
		}

		tmpChkNoDup.AppendRow(row)

		rowPtr := chunk.RowPtr{ChkIdx: uint32(0), RowIdx: uint32(i)}
		chkHashTbl.Put(key, rowPtr)
		idxForOriRows = append(idxForOriRows, i)
	}

	// filter rows duplicated in RowContainer
	chkIdx := storage.NumChunks()
	finalChkNoDup = chunk.NewChunkWithCapacity(tps, chk.Capacity())
	for i := 0; i < tmpChkNoDup.NumRows(); i++ {
		key := hasher[idxForOriRows[i]].Sum64()
		row := tmpChkNoDup.GetRow(i)

		hasDup, err := e.checkHasDup(key, row, nil, storage, hashtbl)
		if err != nil {
			return nil, err
		}
		if hasDup {
			continue
		}

		rowIdx := finalChkNoDup.NumRows()
		finalChkNoDup.AppendRow(row)

		rowPtr := chunk.RowPtr{ChkIdx: uint32(chkIdx), RowIdx: uint32(rowIdx)}
		hashtbl.Put(key, rowPtr)
	}
	return finalChkNoDup, nil
}

func (e *CTEExec) checkHasDup(probeKey uint64,
	row chunk.Row,
	curChk *chunk.Chunk,
	storage cteutil.Storage,
	hashtbl baseHashTable) (hasDup bool, err error) {
	ptrs := hashtbl.Get(probeKey)

	if len(ptrs) == 0 {
		return false, nil
	}

	colIdx := make([]int, row.Len())
	for i := range colIdx {
		colIdx[i] = i
	}

	tps := e.base().retFieldTypes
	for _, ptr := range ptrs {
		var matchedRow chunk.Row
		if curChk != nil {
			matchedRow = curChk.GetRow(int(ptr.RowIdx))
		} else {
			matchedRow, err = storage.GetRow(ptr)
		}
		if err != nil {
			return false, err
		}
		isEqual, err := codec.EqualChunkRow(e.ctx.GetSessionVars().StmtCtx, row, tps, colIdx, matchedRow, tps, colIdx)
		if err != nil {
			return false, err
		}
		if isEqual {
			return true, nil
		}
	}
	return false, nil
}
