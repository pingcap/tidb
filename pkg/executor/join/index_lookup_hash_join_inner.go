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

package join

import (
	"context"
	"hash"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
)

func (iw *indexHashJoinInnerWorker) doJoinUnordered(ctx context.Context, task *indexHashJoinTask, joinResult *indexHashJoinResult, h hash.Hash64, resultCh chan *indexHashJoinResult) error {
	if iw.stats != nil {
		start := time.Now()
		defer func() {
			atomic.AddInt64(&iw.stats.join, int64(time.Since(start)))
		}()
	}
	var ok bool
	iter := chunk.NewIterator4List(task.innerResult)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		ok, joinResult = iw.joinMatchedInnerRow2Chunk(ctx, row, task, joinResult, h, iw.joinKeyBuf)
		if !ok {
			return joinResult.err
		}
	}
	if task.innerExec != nil {
		return nil
	}
	// Only when innerExec is finished, we can call OnMissMatch for the unmatched outer rows.
	for chkIdx, outerRowStatus := range task.outerRowStatus {
		chk := task.outerResult.GetChunk(chkIdx)
		for rowIdx, val := range outerRowStatus {
			if val == outerRowMatched {
				continue
			}
			iw.joiner.OnMissMatch(val == outerRowHasNull, chk.GetRow(rowIdx), joinResult.chk)
			if joinResult.chk.IsFull() {
				select {
				case resultCh <- joinResult:
				case <-ctx.Done():
					return ctx.Err()
				}
				joinResult, ok = iw.getNewJoinResult(ctx)
				if !ok {
					return errors.New("indexHashJoinInnerWorker.doJoinUnordered failed")
				}
			}
		}
	}
	return nil
}

func (iw *indexHashJoinInnerWorker) getMatchedOuterRows(innerRow chunk.Row, task *indexHashJoinTask, h hash.Hash64, buf []byte) (matchedRows []chunk.Row, matchedRowPtr []chunk.RowPtr, err error) {
	for i, colIdx := range iw.HashCols {
		if innerRow.IsNull(colIdx) && !iw.HashIsNullEQ[i] {
			return nil, nil, nil
		}
	}
	h.Reset()
	err = codec.HashChunkRow(iw.ctx.GetSessionVars().StmtCtx.TypeCtx(), h, innerRow, iw.HashTypes, iw.HashCols, buf)
	if err != nil {
		return nil, nil, err
	}
	matchedOuterEntry := task.lookupMap.Get(h.Sum64())
	if matchedOuterEntry == nil {
		return nil, nil, nil
	}
	joinType := JoinerType(iw.joiner)
	isSemiJoin := joinType.IsSemiJoin()
	for ; matchedOuterEntry != nil; matchedOuterEntry = matchedOuterEntry.Next {
		ptr := matchedOuterEntry.Ptr
		outerRow := task.outerResult.GetRow(ptr)
		ok, err := codec.EqualChunkRow(iw.ctx.GetSessionVars().StmtCtx.TypeCtx(), innerRow, iw.HashTypes, iw.HashCols, outerRow, iw.outerCtx.HashTypes, iw.outerCtx.HashCols)
		if err != nil {
			return nil, nil, err
		}
		if !ok || (task.outerRowStatus[ptr.ChkIdx][ptr.RowIdx] == outerRowMatched && isSemiJoin) {
			continue
		}
		matchedRows = append(matchedRows, outerRow)
		matchedRowPtr = append(matchedRowPtr, chunk.RowPtr{ChkIdx: ptr.ChkIdx, RowIdx: ptr.RowIdx})
	}
	return matchedRows, matchedRowPtr, nil
}

func (iw *indexHashJoinInnerWorker) joinMatchedInnerRow2Chunk(ctx context.Context, innerRow chunk.Row, task *indexHashJoinTask,
	joinResult *indexHashJoinResult, h hash.Hash64, buf []byte) (bool, *indexHashJoinResult) {
	matchedOuterRows, matchedOuterRowPtr, err := iw.getMatchedOuterRows(innerRow, task, h, buf)
	if err != nil {
		joinResult.err = err
		return false, joinResult
	}
	if len(matchedOuterRows) == 0 {
		return true, joinResult
	}
	var ok bool
	cursor := 0
	iw.rowIter.Reset(matchedOuterRows)
	iter := iw.rowIter
	for iw.rowIter.Begin(); iter.Current() != iter.End(); {
		iw.outerRowStatus, err = iw.joiner.TryToMatchOuters(iter, innerRow, joinResult.chk, iw.outerRowStatus)
		if err != nil {
			joinResult.err = err
			return false, joinResult
		}
		for _, status := range iw.outerRowStatus {
			chkIdx, rowIdx := matchedOuterRowPtr[cursor].ChkIdx, matchedOuterRowPtr[cursor].RowIdx
			if status == outerRowMatched || task.outerRowStatus[chkIdx][rowIdx] == outerRowUnmatched {
				task.outerRowStatus[chkIdx][rowIdx] = status
			}
			cursor++
		}
		if joinResult.chk.IsFull() {
			select {
			case iw.resultCh <- joinResult:
			case <-ctx.Done():
				joinResult.err = ctx.Err()
				return false, joinResult
			}
			failpoint.InjectCall("joinMatchedInnerRow2Chunk")
			joinResult, ok = iw.getNewJoinResult(ctx)
			if !ok {
				return false, joinResult
			}
		}
	}
	return true, joinResult
}

func (iw *indexHashJoinInnerWorker) collectMatchedInnerPtrs4OuterRows(innerRow chunk.Row, innerRowPtr chunk.RowPtr,
	task *indexHashJoinTask, h hash.Hash64, buf []byte) error {
	_, matchedOuterRowIdx, err := iw.getMatchedOuterRows(innerRow, task, h, buf)
	if err != nil {
		return err
	}
	for _, outerRowPtr := range matchedOuterRowIdx {
		chkIdx, rowIdx := outerRowPtr.ChkIdx, outerRowPtr.RowIdx
		task.matchedInnerRowPtrs[chkIdx][rowIdx] = append(task.matchedInnerRowPtrs[chkIdx][rowIdx], innerRowPtr)
	}
	return nil
}

// doJoinInOrder follows the following steps:
//  1. collect all the matched inner row ptrs for every outer row
//  2. do the join work
//     2.1 collect all the matched inner rows using the collected ptrs for every outer row
//     2.2 call TryToMatchInners for every outer row
//     2.3 call OnMissMatch when no inner rows are matched
func (iw *indexHashJoinInnerWorker) doJoinInOrder(ctx context.Context, task *indexHashJoinTask, joinResult *indexHashJoinResult, h hash.Hash64, resultCh chan *indexHashJoinResult) (err error) {
	defer func() {
		if err == nil && joinResult.chk != nil {
			if joinResult.chk.NumRows() > 0 {
				select {
				case resultCh <- joinResult:
				case <-ctx.Done():
					return
				}
			} else {
				joinResult.src <- joinResult.chk
			}
		}
	}()
	for i, numChunks := 0, task.innerResult.NumChunks(); i < numChunks; i++ {
		for j, chk := 0, task.innerResult.GetChunk(i); j < chk.NumRows(); j++ {
			row := chk.GetRow(j)
			ptr := chunk.RowPtr{ChkIdx: uint32(i), RowIdx: uint32(j)}
			err = iw.collectMatchedInnerPtrs4OuterRows(row, ptr, task, h, iw.joinKeyBuf)
			failpoint.Inject("TestIssue31129", func() {
				err = errors.New("TestIssue31129")
			})
			if err != nil {
				return err
			}
		}
	}
	// TODO: matchedInnerRowPtrs and matchedInnerRows can be moved to inner worker.
	matchedInnerRows := make([]chunk.Row, 0, len(task.matchedInnerRowPtrs))
	var hasMatched, hasNull, ok bool
	for chkIdx, innerRowPtrs4Chk := range task.matchedInnerRowPtrs {
		for outerRowIdx, innerRowPtrs := range innerRowPtrs4Chk {
			matchedInnerRows, hasMatched, hasNull = matchedInnerRows[:0], false, false
			outerRow := task.outerResult.GetChunk(chkIdx).GetRow(outerRowIdx)
			for _, ptr := range innerRowPtrs {
				matchedInnerRows = append(matchedInnerRows, task.innerResult.GetRow(ptr))
			}
			iw.rowIter.Reset(matchedInnerRows)
			iter := iw.rowIter
			for iter.Begin(); iter.Current() != iter.End(); {
				matched, isNull, err := iw.joiner.TryToMatchInners(outerRow, iter, joinResult.chk)
				if err != nil {
					return err
				}
				hasMatched, hasNull = matched || hasMatched, isNull || hasNull
				if joinResult.chk.IsFull() {
					select {
					case resultCh <- joinResult:
					case <-ctx.Done():
						return ctx.Err()
					}
					joinResult, ok = iw.getNewJoinResult(ctx)
					if !ok {
						return errors.New("indexHashJoinInnerWorker.doJoinInOrder failed")
					}
				}
			}
			if !hasMatched {
				iw.joiner.OnMissMatch(hasNull, outerRow, joinResult.chk)
			}
		}
	}
	return nil
}
