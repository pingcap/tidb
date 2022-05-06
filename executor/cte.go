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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/cteutil"
	"github.com/pingcap/tidb/util/disk"
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

	hashTbl baseHashTable

	// Index of chunk to read from `resTbl`.
	chkIdx int

	// UNION ALL or UNION DISTINCT.
	isDistinct bool
	curIter    int
	hCtx       *hashContext
	sel        []int

	// Limit related info.
	hasLimit       bool
	limitBeg       uint64
	limitEnd       uint64
	cursor         uint64
	meetFirstBatch bool

	memTracker  *memory.Tracker
	diskTracker *disk.Tracker

	// isInApply indicates whether CTE is in inner side of Apply
	// and should resTbl/iterInTbl be reset for each outer row of Apply.
	// Because we reset them when SQL is finished instead of when CTEExec.Close() is called.
	isInApply bool
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

	e.memTracker = memory.NewTracker(e.id, -1)
	e.diskTracker = disk.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)
	e.diskTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.DiskTracker)

	if e.recursiveExec != nil {
		if err = e.recursiveExec.Open(ctx); err != nil {
			return err
		}
		// For non-recursive CTE, the result will be put into resTbl directly.
		// So no need to build iterOutTbl.
		// Construct iterOutTbl in Open() instead of buildCTE(), because its destruct is in Close().
		recursiveTypes := e.recursiveExec.base().retFieldTypes
		e.iterOutTbl = cteutil.NewStorageRowContainer(recursiveTypes, e.maxChunkSize)
		if err = e.iterOutTbl.OpenAndRef(); err != nil {
			return err
		}
	}

	if e.isDistinct {
		e.hashTbl = newConcurrentMapHashTable()
		e.hCtx = &hashContext{
			allTypes: e.base().retFieldTypes,
		}
		// We use all columns to compute hash.
		e.hCtx.keyColIdx = make([]int, len(e.hCtx.allTypes))
		for i := range e.hCtx.keyColIdx {
			e.hCtx.keyColIdx[i] = i
		}
	}
	return nil
}

// Next implements the Executor interface.
func (e *CTEExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	req.Reset()
	e.resTbl.Lock()
	defer e.resTbl.Unlock()
	if !e.resTbl.Done() {
		resAction := setupCTEStorageTracker(e.resTbl, e.ctx, e.memTracker, e.diskTracker)
		iterInAction := setupCTEStorageTracker(e.iterInTbl, e.ctx, e.memTracker, e.diskTracker)
		var iterOutAction *chunk.SpillDiskAction
		if e.iterOutTbl != nil {
			iterOutAction = setupCTEStorageTracker(e.iterOutTbl, e.ctx, e.memTracker, e.diskTracker)
		}

		failpoint.Inject("testCTEStorageSpill", func(val failpoint.Value) {
			if val.(bool) && config.GetGlobalConfig().OOMUseTmpStorage {
				defer resAction.WaitForTest()
				defer iterInAction.WaitForTest()
				if iterOutAction != nil {
					defer iterOutAction.WaitForTest()
				}
			}
		})

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
		e.resTbl.SetDone()
	}

	if e.hasLimit {
		return e.nextChunkLimit(req)
	}
	if e.chkIdx < e.resTbl.NumChunks() {
		res, err := e.resTbl.GetChunk(e.chkIdx)
		if err != nil {
			return err
		}
		// Need to copy chunk to make sure upper operator will not change chunk in resTbl.
		// Also we ignore copying rows not selected, because some operators like Projection
		// doesn't support swap column if chunk.sel is no nil.
		req.SwapColumns(res.CopyConstructSel())
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
		// `iterInTbl` and `resTbl` are shared by multiple operators,
		// so will be closed when the SQL finishes.
		if e.iterOutTbl != nil {
			if err = e.iterOutTbl.DerefAndClose(); err != nil {
				return err
			}
		}
	}
	if e.isInApply {
		if err = e.reopenTbls(); err != nil {
			return err
		}
	}

	return e.baseExecutor.Close()
}

func (e *CTEExec) computeSeedPart(ctx context.Context) (err error) {
	e.curIter = 0
	e.iterInTbl.SetIter(e.curIter)
	chks := make([]*chunk.Chunk, 0, 10)
	for {
		if e.limitDone(e.iterInTbl) {
			break
		}
		chk := newFirstChunk(e.seedExec)
		if err = Next(ctx, e.seedExec, chk); err != nil {
			return err
		}
		if chk.NumRows() == 0 {
			break
		}
		if chk, err = e.tryDedupAndAdd(chk, e.iterInTbl, e.hashTbl); err != nil {
			return err
		}
		chks = append(chks, chk)
	}
	// Initial resTbl is empty, so no need to deduplicate chk using resTbl.
	// Just adding is ok.
	for _, chk := range chks {
		if err = e.resTbl.Add(chk); err != nil {
			return err
		}
	}
	e.curIter++
	e.iterInTbl.SetIter(e.curIter)

	return nil
}

func (e *CTEExec) computeRecursivePart(ctx context.Context) (err error) {
	if e.recursiveExec == nil || e.iterInTbl.NumChunks() == 0 {
		return nil
	}

	if e.curIter > e.ctx.GetSessionVars().CTEMaxRecursionDepth {
		return ErrCTEMaxRecursionDepth.GenWithStackByArgs(e.curIter)
	}

	if e.limitDone(e.resTbl) {
		return nil
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
			if e.limitDone(e.resTbl) {
				break
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

// Get next chunk from resTbl for limit.
func (e *CTEExec) nextChunkLimit(req *chunk.Chunk) error {
	if !e.meetFirstBatch {
		for e.chkIdx < e.resTbl.NumChunks() {
			res, err := e.resTbl.GetChunk(e.chkIdx)
			if err != nil {
				return err
			}
			e.chkIdx++
			numRows := uint64(res.NumRows())
			if newCursor := e.cursor + numRows; newCursor >= e.limitBeg {
				e.meetFirstBatch = true
				begInChk, endInChk := e.limitBeg-e.cursor, numRows
				if newCursor > e.limitEnd {
					endInChk = e.limitEnd - e.cursor
				}
				e.cursor += endInChk
				if begInChk == endInChk {
					break
				}
				tmpChk := res.CopyConstructSel()
				req.Append(tmpChk, int(begInChk), int(endInChk))
				return nil
			}
			e.cursor += numRows
		}
	}
	if e.chkIdx < e.resTbl.NumChunks() && e.cursor < e.limitEnd {
		res, err := e.resTbl.GetChunk(e.chkIdx)
		if err != nil {
			return err
		}
		e.chkIdx++
		numRows := uint64(res.NumRows())
		if e.cursor+numRows > e.limitEnd {
			numRows = e.limitEnd - e.cursor
			req.Append(res.CopyConstructSel(), 0, int(numRows))
		} else {
			req.SwapColumns(res.CopyConstructSel())
		}
		e.cursor += numRows
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
		// Data should be copied in UNION DISTINCT.
		// Because deduplicate() will change data in iterOutTbl,
		// which will cause panic when spilling data into disk concurrently.
		if e.isDistinct {
			chk = chk.CopyConstruct()
		}
		chk, err = e.tryDedupAndAdd(chk, e.resTbl, e.hashTbl)
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
		// Already deduplicated by resTbl, adding directly is ok.
		for _, chk := range chks {
			if err = e.iterInTbl.Add(chk); err != nil {
				return err
			}
		}
	} else {
		if err = e.iterInTbl.SwapData(e.iterOutTbl); err != nil {
			return err
		}
	}

	// Clear data in iterOutTbl.
	return e.iterOutTbl.Reopen()
}

func (e *CTEExec) reset() {
	e.curIter = 0
	e.chkIdx = 0
	e.hashTbl = nil
	e.cursor = 0
	e.meetFirstBatch = false
}

func (e *CTEExec) reopenTbls() (err error) {
	if e.isDistinct {
		e.hashTbl = newConcurrentMapHashTable()
	}
	if err := e.resTbl.Reopen(); err != nil {
		return err
	}
	return e.iterInTbl.Reopen()
}

// Check if tbl meets the requirement of limit.
func (e *CTEExec) limitDone(tbl cteutil.Storage) bool {
	return e.hasLimit && uint64(tbl.NumRows()) >= e.limitEnd
}

func setupCTEStorageTracker(tbl cteutil.Storage, ctx sessionctx.Context, parentMemTracker *memory.Tracker,
	parentDiskTracker *disk.Tracker) (actionSpill *chunk.SpillDiskAction) {
	memTracker := tbl.GetMemTracker()
	memTracker.SetLabel(memory.LabelForCTEStorage)
	memTracker.AttachTo(parentMemTracker)

	diskTracker := tbl.GetDiskTracker()
	diskTracker.SetLabel(memory.LabelForCTEStorage)
	diskTracker.AttachTo(parentDiskTracker)

	if config.GetGlobalConfig().OOMUseTmpStorage {
		actionSpill = tbl.ActionSpill()
		failpoint.Inject("testCTEStorageSpill", func(val failpoint.Value) {
			if val.(bool) {
				actionSpill = tbl.(*cteutil.StorageRC).ActionSpillForTest()
			}
		})
		ctx.GetSessionVars().StmtCtx.MemTracker.FallbackOldAndSetNewAction(actionSpill)
	}
	return actionSpill
}

func (e *CTEExec) tryDedupAndAdd(chk *chunk.Chunk,
	storage cteutil.Storage,
	hashTbl baseHashTable) (res *chunk.Chunk, err error) {
	if e.isDistinct {
		if chk, err = e.deduplicate(chk, storage, hashTbl); err != nil {
			return nil, err
		}
	}
	return chk, storage.Add(chk)
}

// Compute hash values in chk and put it in hCtx.hashVals.
// Use the returned sel to choose the computed hash values.
func (e *CTEExec) computeChunkHash(chk *chunk.Chunk) (sel []int, err error) {
	numRows := chk.NumRows()
	e.hCtx.initHash(numRows)
	// Continue to reset to make sure all hasher is new.
	for i := numRows; i < len(e.hCtx.hashVals); i++ {
		e.hCtx.hashVals[i].Reset()
	}
	sel = chk.Sel()
	var hashBitMap []bool
	if sel != nil {
		hashBitMap = make([]bool, chk.Capacity())
		for _, val := range sel {
			hashBitMap[val] = true
		}
	} else {
		// All rows is selected, sel will be [0....numRows).
		// e.sel is setup when building executor.
		sel = e.sel
	}

	for i := 0; i < chk.NumCols(); i++ {
		if err = codec.HashChunkSelected(e.ctx.GetSessionVars().StmtCtx, e.hCtx.hashVals,
			chk, e.hCtx.allTypes[i], i, e.hCtx.buf, e.hCtx.hasNull,
			hashBitMap, false); err != nil {
			return nil, err
		}
	}
	return sel, nil
}

// Use hashTbl to deduplicate rows, and unique rows will be added to hashTbl.
// Duplicated rows are only marked to be removed by sel in Chunk, instead of really deleted.
func (e *CTEExec) deduplicate(chk *chunk.Chunk,
	storage cteutil.Storage,
	hashTbl baseHashTable) (chkNoDup *chunk.Chunk, err error) {
	numRows := chk.NumRows()
	if numRows == 0 {
		return chk, nil
	}

	// 1. Compute hash values for chunk.
	chkHashTbl := newConcurrentMapHashTable()
	selOri, err := e.computeChunkHash(chk)
	if err != nil {
		return nil, err
	}

	// 2. Filter rows duplicated in input chunk.
	// This sel is for filtering rows duplicated in cur chk.
	selChk := make([]int, 0, numRows)
	for i := 0; i < numRows; i++ {
		key := e.hCtx.hashVals[selOri[i]].Sum64()
		row := chk.GetRow(i)

		hasDup, err := e.checkHasDup(key, row, chk, storage, chkHashTbl)
		if err != nil {
			return nil, err
		}
		if hasDup {
			continue
		}

		selChk = append(selChk, selOri[i])

		rowPtr := chunk.RowPtr{ChkIdx: uint32(0), RowIdx: uint32(i)}
		chkHashTbl.Put(key, rowPtr)
	}
	chk.SetSel(selChk)
	chkIdx := storage.NumChunks()

	// 3. Filter rows duplicated in RowContainer.
	// This sel is for filtering rows duplicated in cteutil.Storage.
	selStorage := make([]int, 0, len(selChk))
	for i := 0; i < len(selChk); i++ {
		key := e.hCtx.hashVals[selChk[i]].Sum64()
		row := chk.GetRow(i)

		hasDup, err := e.checkHasDup(key, row, nil, storage, hashTbl)
		if err != nil {
			return nil, err
		}
		if hasDup {
			continue
		}

		rowIdx := len(selStorage)
		selStorage = append(selStorage, selChk[i])

		rowPtr := chunk.RowPtr{ChkIdx: uint32(chkIdx), RowIdx: uint32(rowIdx)}
		hashTbl.Put(key, rowPtr)
	}

	chk.SetSel(selStorage)
	return chk, nil
}

// Use the row's probe key to check if it already exists in chk or storage.
// We also need to compare the row's real encoding value to avoid hash collision.
func (e *CTEExec) checkHasDup(probeKey uint64,
	row chunk.Row,
	curChk *chunk.Chunk,
	storage cteutil.Storage,
	hashTbl baseHashTable) (hasDup bool, err error) {
	ptrs := hashTbl.Get(probeKey)

	if len(ptrs) == 0 {
		return false, nil
	}

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
		isEqual, err := codec.EqualChunkRow(e.ctx.GetSessionVars().StmtCtx,
			row, e.hCtx.allTypes, e.hCtx.keyColIdx,
			matchedRow, e.hCtx.allTypes, e.hCtx.keyColIdx)
		if err != nil {
			return false, err
		}
		if isEqual {
			return true, nil
		}
	}
	return false, nil
}
