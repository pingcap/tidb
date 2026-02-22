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
	"bytes"
	"context"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/join"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/cteutil"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func (p *cteProducer) computeRecursivePart(ctx context.Context) (err error) {
	defer func() {
		if r := recover(); r != nil && err == nil {
			err = util.GetRecoverError(r)
		}
	}()
	failpoint.Inject("testCTERecursivePanic", nil)
	if p.recursiveExec == nil || p.iterInTbl.NumChunks() == 0 {
		return
	}

	if p.curIter > p.ctx.GetSessionVars().CTEMaxRecursionDepth {
		return exeerrors.ErrCTEMaxRecursionDepth.GenWithStackByArgs(p.curIter)
	}

	if p.limitDone(p.resTbl) {
		return
	}

	var iterNum uint64
	for {
		chk := exec.TryNewCacheChunk(p.recursiveExec)
		if err = exec.Next(ctx, p.recursiveExec, chk); err != nil {
			return
		}
		if chk.NumRows() == 0 {
			if iterNum%1000 == 0 {
				// To avoid too many logs.
				p.logTbls(ctx, err, iterNum, zapcore.DebugLevel)
			}
			iterNum++
			failpoint.Inject("assertIterTableSpillToDisk", func(maxIter failpoint.Value) {
				if iterNum > 0 && iterNum < uint64(maxIter.(int)) && err == nil {
					if p.iterInTbl.GetDiskBytes() == 0 && p.iterOutTbl.GetDiskBytes() == 0 && p.resTbl.GetDiskBytes() == 0 {
						p.logTbls(ctx, err, iterNum, zapcore.InfoLevel)
						panic("assert row container spill disk failed")
					}
				}
			})

			if err = p.setupTblsForNewIteration(); err != nil {
				return
			}
			if p.limitDone(p.resTbl) {
				break
			}
			if p.iterInTbl.NumChunks() == 0 {
				break
			}
			// Next iteration begins. Need use iterOutTbl as input of next iteration.
			p.curIter++
			p.iterInTbl.SetIter(p.curIter)
			if p.curIter > p.ctx.GetSessionVars().CTEMaxRecursionDepth {
				return exeerrors.ErrCTEMaxRecursionDepth.GenWithStackByArgs(p.curIter)
			}
			// Make sure iterInTbl is setup before Close/Open,
			// because some executors will read iterInTbl in Open() (like IndexLookupJoin).
			if err = exec.Close(p.recursiveExec); err != nil {
				return
			}
			if err = exec.Open(ctx, p.recursiveExec); err != nil {
				return
			}
		} else {
			if err = p.iterOutTbl.Add(chk); err != nil {
				return
			}
		}
	}
	return
}

func (p *cteProducer) setupTblsForNewIteration() (err error) {
	num := p.iterOutTbl.NumChunks()
	chks := make([]*chunk.Chunk, 0, num)
	// Setup resTbl's data.
	for i := range num {
		chk, err := p.iterOutTbl.GetChunk(i)
		if err != nil {
			return err
		}
		// Data should be copied in UNION DISTINCT.
		// Because deduplicate() will change data in iterOutTbl,
		// which will cause panic when spilling data into disk concurrently.
		if p.isDistinct {
			chk = chk.CopyConstruct()
		}
		chk, err = p.tryDedupAndAdd(chk, p.resTbl, p.hashTbl)
		if err != nil {
			return err
		}
		chks = append(chks, chk)
	}

	// Setup new iteration data in iterInTbl.
	if err = p.iterInTbl.Reopen(); err != nil {
		return err
	}
	setupCTEStorageTracker(p.iterInTbl, p.ctx, p.memTracker, p.diskTracker)

	if p.isDistinct {
		// Already deduplicated by resTbl, adding directly is ok.
		for _, chk := range chks {
			if err = p.iterInTbl.Add(chk); err != nil {
				return err
			}
		}
	} else {
		if err = p.iterInTbl.SwapData(p.iterOutTbl); err != nil {
			return err
		}
	}

	// Clear data in iterOutTbl.
	if err = p.iterOutTbl.Reopen(); err != nil {
		return err
	}
	setupCTEStorageTracker(p.iterOutTbl, p.ctx, p.memTracker, p.diskTracker)
	return nil
}

func (p *cteProducer) reset() error {
	p.curIter = 0
	p.hashTbl = nil
	p.executorOpened = false
	p.openErr = nil

	// Normally we need to setup tracker after calling Reopen(),
	// But reopen resTbl means we need to call genCTEResult() again, it will setup tracker.
	if err := p.resTbl.Reopen(); err != nil {
		return err
	}
	return p.iterInTbl.Reopen()
}

func (p *cteProducer) resetTracker() {
	if p.memTracker != nil {
		p.memTracker.Reset()
		p.memTracker = nil
	}
	if p.diskTracker != nil {
		p.diskTracker.Reset()
		p.diskTracker = nil
	}
}

// Check if tbl meets the requirement of limit.
func (p *cteProducer) limitDone(tbl cteutil.Storage) bool {
	return p.hasLimit && uint64(tbl.NumRows()) >= p.limitEnd
}

func setupCTEStorageTracker(tbl cteutil.Storage, ctx sessionctx.Context, parentMemTracker *memory.Tracker,
	parentDiskTracker *disk.Tracker) (actionSpill *chunk.SpillDiskAction) {
	memTracker := tbl.GetMemTracker()
	memTracker.SetLabel(memory.LabelForCTEStorage)
	memTracker.AttachTo(parentMemTracker)

	diskTracker := tbl.GetDiskTracker()
	diskTracker.SetLabel(memory.LabelForCTEStorage)
	diskTracker.AttachTo(parentDiskTracker)

	if vardef.EnableTmpStorageOnOOM.Load() {
		actionSpill = tbl.ActionSpill()
		failpoint.Inject("testCTEStorageSpill", func(val failpoint.Value) {
			if val.(bool) {
				actionSpill = tbl.(*cteutil.StorageRC).ActionSpillForTest()
			}
		})
		ctx.GetSessionVars().MemTracker.FallbackOldAndSetNewAction(actionSpill)
	}
	return actionSpill
}

func (p *cteProducer) tryDedupAndAdd(chk *chunk.Chunk,
	storage cteutil.Storage,
	hashTbl join.BaseHashTable) (res *chunk.Chunk, err error) {
	if p.isDistinct {
		if chk, err = p.deduplicate(chk, storage, hashTbl); err != nil {
			return nil, err
		}
	}
	return chk, storage.Add(chk)
}

// Compute hash values in chk and put it in hCtx.hashVals.
// Use the returned sel to choose the computed hash values.
func (p *cteProducer) computeChunkHash(chk *chunk.Chunk) (sel []int, err error) {
	numRows := chk.NumRows()
	p.hCtx.InitHash(numRows)
	// Continue to reset to make sure all hasher is new.
	for i := numRows; i < len(p.hCtx.HashVals); i++ {
		p.hCtx.HashVals[i].Reset()
	}
	sel = chk.Sel()
	var hashBitMap []bool
	if sel != nil {
		hashBitMap = make([]bool, chk.Capacity())
		for _, val := range sel {
			hashBitMap[val] = true
		}
	} else {
		// Length of p.sel is init as MaxChunkSize, but the row num of chunk may still exceeds MaxChunkSize.
		// So needs to handle here to make sure len(p.sel) == chk.NumRows().
		if len(p.sel) < numRows {
			tmpSel := make([]int, numRows-len(p.sel))
			for i := range tmpSel {
				tmpSel[i] = i + len(p.sel)
			}
			p.sel = append(p.sel, tmpSel...)
		}

		// All rows is selected, sel will be [0....numRows).
		// e.sel is setup when building executor.
		sel = p.sel
	}

	for i := range chk.NumCols() {
		if err = codec.HashChunkSelected(p.ctx.GetSessionVars().StmtCtx.TypeCtx(), p.hCtx.HashVals,
			chk, p.hCtx.AllTypes[i], i, p.hCtx.Buf, p.hCtx.HasNull,
			hashBitMap, false); err != nil {
			return nil, err
		}
	}
	return sel, nil
}

// Use hashTbl to deduplicate rows, and unique rows will be added to hashTbl.
// Duplicated rows are only marked to be removed by sel in Chunk, instead of really deleted.
func (p *cteProducer) deduplicate(chk *chunk.Chunk,
	storage cteutil.Storage,
	hashTbl join.BaseHashTable) (chkNoDup *chunk.Chunk, err error) {
	numRows := chk.NumRows()
	if numRows == 0 {
		return chk, nil
	}

	// 1. Compute hash values for chunk.
	chkHashTbl := join.NewConcurrentMapHashTable()
	selOri, err := p.computeChunkHash(chk)
	if err != nil {
		return nil, err
	}

	// 2. Filter rows duplicated in input chunk.
	// This sel is for filtering rows duplicated in cur chk.
	selChk := make([]int, 0, numRows)
	for i := range numRows {
		key := p.hCtx.HashVals[selOri[i]].Sum64()
		row := chk.GetRow(i)

		hasDup, err := p.checkHasDup(key, row, chk, storage, chkHashTbl)
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
	for i := range selChk {
		key := p.hCtx.HashVals[selChk[i]].Sum64()
		row := chk.GetRow(i)

		hasDup, err := p.checkHasDup(key, row, nil, storage, hashTbl)
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
func (p *cteProducer) checkHasDup(probeKey uint64,
	row chunk.Row,
	curChk *chunk.Chunk,
	storage cteutil.Storage,
	hashTbl join.BaseHashTable) (hasDup bool, err error) {
	entry := hashTbl.Get(probeKey)

	for ; entry != nil; entry = entry.Next {
		ptr := entry.Ptr
		var matchedRow chunk.Row
		if curChk != nil {
			matchedRow = curChk.GetRow(int(ptr.RowIdx))
		} else {
			matchedRow, err = storage.GetRow(ptr)
		}
		if err != nil {
			return false, err
		}
		isEqual, err := codec.EqualChunkRow(p.ctx.GetSessionVars().StmtCtx.TypeCtx(),
			row, p.hCtx.AllTypes, p.hCtx.KeyColIdx,
			matchedRow, p.hCtx.AllTypes, p.hCtx.KeyColIdx)
		if err != nil {
			return false, err
		}
		if isEqual {
			return true, nil
		}
	}
	return false, nil
}

func getCorColHashCode(corCol *expression.CorrelatedColumn) (res []byte) {
	return codec.HashCode(res, *corCol.Data)
}

// Return true if cor col has changed.
func (p *cteProducer) checkAndUpdateCorColHashCode() bool {
	var changed bool
	for i, corCol := range p.corCols {
		newHashCode := getCorColHashCode(corCol)
		if !bytes.Equal(newHashCode, p.corColHashCodes[i]) {
			changed = true
			p.corColHashCodes[i] = newHashCode
		}
	}
	return changed
}

func (p *cteProducer) logTbls(ctx context.Context, err error, iterNum uint64, lvl zapcore.Level) {
	logutil.Logger(ctx).Log(lvl, "cte iteration info",
		zap.Int64("iterInTbl mem usage", p.iterInTbl.GetMemBytes()), zap.Int64("iterInTbl disk usage", p.iterInTbl.GetDiskBytes()),
		zap.Int64("iterOutTbl mem usage", p.iterOutTbl.GetMemBytes()), zap.Int64("iterOutTbl disk usage", p.iterOutTbl.GetDiskBytes()),
		zap.Int64("resTbl mem usage", p.resTbl.GetMemBytes()), zap.Int64("resTbl disk usage", p.resTbl.GetDiskBytes()),
		zap.Int("resTbl rows", p.resTbl.NumRows()), zap.Uint64("iteration num", iterNum), zap.Error(err))
}
