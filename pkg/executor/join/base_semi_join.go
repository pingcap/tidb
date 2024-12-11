// Copyright 2024 PingCAP, Inc.
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
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/queue"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
)

// The following described case has other condition.
// During the probe, when a probe matches one build row, we need to put the probe and build rows
// together and generate a new row. If one probe row could match n build row, then we will get
// n new rows. If n is very big, there will generate too much rows. In order to avoid this case
// we need to limit the max generated row number. This variable describe this max number.
// NOTE: Suppose probe chunk has n rows and n*maxMatchedRowNum << chunkRemainingCapacity.
// We will keep on join probe rows that have been matched before with build rows, though
// probe row with idx i may have produced `maxMatchedRowNum` number rows before. So that
// we can process as many rows as possible.
var maxMatchedRowNum = 4

type baseSemiJoin struct {
	baseJoinProbe
	isLeftSideBuild bool

	// isMatchedRows marks whether the left side row is matched
	// It's used only when right side is build side.
	isMatchedRows []bool

	isNulls []bool

	// used when left side is build side
	rowIter *rowIter

	// used in other condition to record which rows need to be processed
	unFinishedProbeRowIdxQueue *queue.Queue[int]

	// Used for right side build without other condition in semi and anti semi join
	offsets []int
}

func newBaseSemiJoin(base baseJoinProbe, isLeftSideBuild bool) *baseSemiJoin {
	ret := &baseSemiJoin{
		baseJoinProbe:   base,
		isLeftSideBuild: isLeftSideBuild,
		isNulls:         make([]bool, 0),
	}

	return ret
}

func (b *baseSemiJoin) resetProbeState() {
	if !b.isLeftSideBuild {
		b.isMatchedRows = b.isMatchedRows[:0]
		for i := 0; i < b.chunkRows; i++ {
			b.isMatchedRows = append(b.isMatchedRows, false)
		}
	}

	if b.ctx.hasOtherCondition() {
		if b.unFinishedProbeRowIdxQueue == nil {
			b.unFinishedProbeRowIdxQueue = queue.NewQueue[int](b.chunkRows)
		} else {
			b.unFinishedProbeRowIdxQueue.ClearAndExpandIfNeed(b.chunkRows)
		}

		for i := 0; i < b.chunkRows; i++ {
			if b.matchedRowsHeaders[i] != 0 {
				b.unFinishedProbeRowIdxQueue.Push(i)
			}
		}
	}
}

func (b *baseSemiJoin) matchMultiBuildRows(joinedChk *chunk.Chunk, joinedChkRemainCap *int, isRightSideBuild bool) {
	tagHelper := b.ctx.hashTableContext.tagHelper
	meta := b.ctx.hashTableMeta
	for b.matchedRowsHeaders[b.currentProbeRow] != 0 && *joinedChkRemainCap > 0 && b.matchedRowsForCurrentProbeRow < maxMatchedRowNum {
		candidateRow := tagHelper.toUnsafePointer(b.matchedRowsHeaders[b.currentProbeRow])
		if isRightSideBuild || !meta.isCurrentRowUsedWithAtomic(candidateRow) {
			if isKeyMatched(meta.keyMode, b.serializedKeys[b.currentProbeRow], candidateRow, meta) {
				b.appendBuildRowToCachedBuildRowsV1(b.currentProbeRow, candidateRow, joinedChk, 0, true)
				b.matchedRowsForCurrentProbeRow++
				*joinedChkRemainCap--
			} else {
				b.probeCollision++
			}
		}

		b.matchedRowsHeaders[b.currentProbeRow] = getNextRowAddress(candidateRow, tagHelper, b.matchedRowsHashValue[b.currentProbeRow])
	}

	b.finishLookupCurrentProbeRow()
}

func (b *baseSemiJoin) concatenateProbeAndBuildRows(joinedChk *chunk.Chunk, sqlKiller *sqlkiller.SQLKiller, isRightSideBuild bool) error {
	joinedChkRemainCap := joinedChk.Capacity()

	for joinedChkRemainCap > 0 && !b.unFinishedProbeRowIdxQueue.IsEmpty() {
		probeRowIdx := b.unFinishedProbeRowIdxQueue.Pop()
		if isRightSideBuild && b.isMatchedRows[probeRowIdx] {
			continue
		}

		b.currentProbeRow = probeRowIdx
		b.matchMultiBuildRows(joinedChk, &joinedChkRemainCap, isRightSideBuild)

		if b.matchedRowsHeaders[probeRowIdx] == 0 {
			continue
		}

		b.unFinishedProbeRowIdxQueue.Push(probeRowIdx)
	}

	err := checkSQLKiller(sqlKiller, "killedDuringProbe")
	if err != nil {
		return err
	}

	b.finishCurrentLookupLoop(joinedChk)
	return nil
}

// Only used for semi and anti semi join
func (b *baseSemiJoin) generateResultChkForRightBuildNoOtherCondition(resultChk *chunk.Chunk) {
	if len(b.offsets) == 0 {
		return
	}

	for index, colIndex := range b.lUsed {
		srcCol := b.currentChunk.Column(colIndex)
		dstCol := resultChk.Column(index)
		chunk.CopyRows(dstCol, srcCol, b.offsets)
	}

	if len(b.lUsed) == 0 {
		resultChk.SetNumVirtualRows(resultChk.NumRows() + len(b.offsets))
	} else {
		resultChk.SetNumVirtualRows(resultChk.NumRows())
	}
}

// Only used for semi and anti semi join
func (b *baseSemiJoin) generateResultChkForRightBuildWithOtherCondition(remainCap int, chk *chunk.Chunk, resultRows []bool, expectedResult bool) {
	for remainCap > 0 && (b.currentProbeRow < b.chunkRows) {
		rowNumToTryAppend := min(remainCap, b.chunkRows-b.currentProbeRow)
		start := b.currentProbeRow
		end := b.currentProbeRow + rowNumToTryAppend

		for index, usedColIdx := range b.lUsed {
			dstCol := chk.Column(index)
			srcCol := b.currentChunk.Column(usedColIdx)
			chunk.CopyExpectedRowsWithRowIDFunc(dstCol, srcCol, resultRows, expectedResult, start, end, func(i int) int {
				return b.usedRows[i]
			})
		}

		if len(b.lUsed) == 0 {
			// For calculating virtual row num
			virtualRowNum := chk.GetNumVirtualRows()
			for i := start; i < end; i++ {
				if resultRows[i] == expectedResult {
					virtualRowNum++
				}
			}

			// When `len(b.lUsed) == 0`, column number in chk is 0
			// We need to manually calculate virtual row number.
			chk.SetNumVirtualRows(virtualRowNum)
		} else {
			chk.SetNumVirtualRows(chk.NumRows())
		}

		b.currentProbeRow += rowNumToTryAppend
		remainCap = chk.RequiredRows() - chk.NumRows()
	}
}
