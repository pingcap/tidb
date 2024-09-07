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
	"hash/fnv"
	"unsafe"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
)

type rowTableBuilder struct {
	buildKeyIndex    []int
	buildKeyTypes    []*types.FieldType
	hasNullableKey   bool
	hasFilter        bool
	keepFilteredRows bool

	serializedKeyVectorBuffer [][]byte
	partIdxVector             []int
	selRows                   []int
	usedRows                  []int
	hashValue                 []uint64
	firstSegRowSizeHint       uint
	// filterVector and nullKeyVector is indexed by physical row index because the return vector of VectorizedFilter is based on physical row index
	filterVector  []bool // if there is filter before probe, filterVector saves the filter result
	nullKeyVector []bool // nullKeyVector[i] = true if any of the key is null

	rowNumberInCurrentRowTableSeg []int64
}

func createRowTableBuilder(buildKeyIndex []int, buildKeyTypes []*types.FieldType, partitionNumber uint, hasNullableKey bool, hasFilter bool, keepFilteredRows bool) *rowTableBuilder {
	builder := &rowTableBuilder{
		buildKeyIndex:                 buildKeyIndex,
		buildKeyTypes:                 buildKeyTypes,
		rowNumberInCurrentRowTableSeg: make([]int64, partitionNumber),
		hasNullableKey:                hasNullableKey,
		hasFilter:                     hasFilter,
		keepFilteredRows:              keepFilteredRows,
	}
	return builder
}

func (b *rowTableBuilder) initHashValueAndPartIndexForOneChunk(partitionMaskOffset int, partitionNumber uint) {
	h := fnv.New64()
	fakePartIndex := uint64(0)
	for logicalRowIndex, physicalRowIndex := range b.usedRows {
		if (b.filterVector != nil && !b.filterVector[physicalRowIndex]) || (b.nullKeyVector != nil && b.nullKeyVector[physicalRowIndex]) {
			b.hashValue[logicalRowIndex] = fakePartIndex
			b.partIdxVector[logicalRowIndex] = int(fakePartIndex)
			fakePartIndex = (fakePartIndex + 1) % uint64(partitionNumber)
			continue
		}
		h.Write(b.serializedKeyVectorBuffer[logicalRowIndex])
		hash := h.Sum64()
		b.hashValue[logicalRowIndex] = hash
		b.partIdxVector[logicalRowIndex] = int(hash >> partitionMaskOffset)
		h.Reset()
	}
}

func (b *rowTableBuilder) processOneChunk(chk *chunk.Chunk, typeCtx types.Context, hashJoinCtx *HashJoinCtxV2, workerID int) error {
	b.ResetBuffer(chk)
	b.firstSegRowSizeHint = max(uint(1), uint(float64(len(b.usedRows))/float64(hashJoinCtx.partitionNumber)*float64(1.2)))
	var err error
	if b.hasFilter {
		b.filterVector, err = expression.VectorizedFilter(hashJoinCtx.SessCtx.GetExprCtx().GetEvalCtx(), hashJoinCtx.SessCtx.GetSessionVars().EnableVectorizedExpression, hashJoinCtx.BuildFilter, chunk.NewIterator4Chunk(chk), b.filterVector)
		if err != nil {
			return err
		}
	}
	err = checkSQLKiller(&hashJoinCtx.SessCtx.GetSessionVars().SQLKiller, "killedDuringBuild")
	if err != nil {
		return err
	}
	// 1. split partition
	for index, colIdx := range b.buildKeyIndex {
		err := codec.SerializeKeys(typeCtx, chk, b.buildKeyTypes[index], colIdx, b.usedRows, b.filterVector, b.nullKeyVector, hashJoinCtx.hashTableMeta.serializeModes[index], b.serializedKeyVectorBuffer)
		if err != nil {
			return err
		}
	}
	err = checkSQLKiller(&hashJoinCtx.SessCtx.GetSessionVars().SQLKiller, "killedDuringBuild")
	if err != nil {
		return err
	}

	b.initHashValueAndPartIndexForOneChunk(hashJoinCtx.partitionMaskOffset, hashJoinCtx.partitionNumber)

	// 2. build rowtable
	return b.appendToRowTable(chk, hashJoinCtx, workerID)
}

func resizeSlice[T int | uint64 | bool](s []T, newSize int) []T {
	if cap(s) >= newSize {
		s = s[:newSize]
	} else {
		s = make([]T, newSize)
	}
	return s
}

func (b *rowTableBuilder) ResetBuffer(chk *chunk.Chunk) {
	b.usedRows = chk.Sel()
	logicalRows := chk.NumRows()
	physicalRows := chk.Column(0).Rows()

	if b.usedRows == nil {
		b.selRows = resizeSlice(b.selRows, logicalRows)
		for i := 0; i < logicalRows; i++ {
			b.selRows[i] = i
		}
		b.usedRows = b.selRows
	}
	b.partIdxVector = resizeSlice(b.partIdxVector, logicalRows)
	b.hashValue = resizeSlice(b.hashValue, logicalRows)
	if b.hasFilter {
		b.filterVector = resizeSlice(b.filterVector, physicalRows)
	}
	if b.hasNullableKey {
		b.nullKeyVector = resizeSlice(b.nullKeyVector, physicalRows)
		for i := 0; i < physicalRows; i++ {
			b.nullKeyVector[i] = false
		}
	}
	if cap(b.serializedKeyVectorBuffer) >= logicalRows {
		b.serializedKeyVectorBuffer = b.serializedKeyVectorBuffer[:logicalRows]
		for i := 0; i < logicalRows; i++ {
			b.serializedKeyVectorBuffer[i] = b.serializedKeyVectorBuffer[i][:0]
		}
	} else {
		b.serializedKeyVectorBuffer = make([][]byte, logicalRows)
	}
}

func (b *rowTableBuilder) appendRemainingRowLocations(workerID int, htCtx *hashTableContext) {
	for partID := 0; partID < int(htCtx.hashTable.partitionNumber); partID++ {
		if b.rowNumberInCurrentRowTableSeg[partID] > 0 {
			htCtx.finalizeCurrentSeg(workerID, partID, b)
		}
	}
}

func fillNullMap(rowTableMeta *joinTableMeta, row *chunk.Row, seg *rowTableSegment) int {
	if nullMapLength := rowTableMeta.nullMapLength; nullMapLength > 0 {
		bitmap := make([]byte, nullMapLength)
		for colIndexInRowTable, colIndexInRow := range rowTableMeta.rowColumnsOrder {
			colIndexInBitMap := colIndexInRowTable + rowTableMeta.colOffsetInNullMap
			if row.IsNull(colIndexInRow) {
				bitmap[colIndexInBitMap/8] |= 1 << (7 - colIndexInBitMap%8)
			}
		}
		seg.rawData = append(seg.rawData, bitmap...)
		return nullMapLength
	}
	return 0
}

func fillNextRowPtr(seg *rowTableSegment) int {
	seg.rawData = append(seg.rawData, fakeAddrPlaceHolder...)
	return sizeOfNextPtr
}

func (b *rowTableBuilder) fillSerializedKeyAndKeyLengthIfNeeded(rowTableMeta *joinTableMeta, hasValidKey bool, logicalRowIndex int, seg *rowTableSegment) int {
	appendRowLength := 0
	// 1. fill key length if needed
	if !rowTableMeta.isJoinKeysFixedLength {
		// if join_key is not fixed length: `key_length` need to be written in rawData
		// even the join keys is inlined, for example if join key is 2 binary string
		// then the inlined join key should be: col1_size + col1_data + col2_size + col2_data
		// and len(col1_size + col1_data + col2_size + col2_data) need to be written before the inlined join key
		length := uint64(0)
		if hasValidKey {
			length = uint64(len(b.serializedKeyVectorBuffer[logicalRowIndex]))
		} else {
			length = 0
		}
		seg.rawData = append(seg.rawData, unsafe.Slice((*byte)(unsafe.Pointer(&length)), sizeOfLengthField)...)
		appendRowLength += sizeOfLengthField
	}
	// 2. fill serialized key if needed
	if !rowTableMeta.isJoinKeysInlined {
		// if join_key is not inlined: `serialized_key` need to be written in rawData
		if hasValidKey {
			seg.rawData = append(seg.rawData, b.serializedKeyVectorBuffer[logicalRowIndex]...)
			appendRowLength += len(b.serializedKeyVectorBuffer[logicalRowIndex])
		} else {
			// if there is no valid key, and the key is fixed length, then write a fake key
			if rowTableMeta.isJoinKeysFixedLength {
				seg.rawData = append(seg.rawData, rowTableMeta.fakeKeyByte...)
				appendRowLength += rowTableMeta.joinKeysLength
			}
			// otherwise don't need to write since length is 0
		}
	}
	return appendRowLength
}

func fillRowData(rowTableMeta *joinTableMeta, row *chunk.Row, seg *rowTableSegment) int {
	appendRowLength := 0
	for index, colIdx := range rowTableMeta.rowColumnsOrder {
		if rowTableMeta.columnsSize[index] > 0 {
			// fixed size
			seg.rawData = append(seg.rawData, row.GetRaw(colIdx)...)
			appendRowLength += rowTableMeta.columnsSize[index]
		} else {
			// length, raw_data
			raw := row.GetRaw(colIdx)
			length := uint64(len(raw))
			seg.rawData = append(seg.rawData, unsafe.Slice((*byte)(unsafe.Pointer(&length)), sizeOfLengthField)...)
			appendRowLength += sizeOfLengthField
			seg.rawData = append(seg.rawData, raw...)
			appendRowLength += int(length)
		}
	}
	return appendRowLength
}

func (b *rowTableBuilder) appendToRowTable(chk *chunk.Chunk, hashJoinCtx *HashJoinCtxV2, workerID int) error {
	rowTableMeta := hashJoinCtx.hashTableMeta
	for logicalRowIndex, physicalRowIndex := range b.usedRows {
		if logicalRowIndex%10 == 0 || logicalRowIndex == len(b.usedRows)-1 {
			err := checkSQLKiller(&hashJoinCtx.SessCtx.GetSessionVars().SQLKiller, "killedDuringBuild")
			if err != nil {
				return err
			}
		}
		hasValidKey := (!b.hasFilter || b.filterVector[physicalRowIndex]) && (!b.hasNullableKey || !b.nullKeyVector[physicalRowIndex])
		if !hasValidKey && !b.keepFilteredRows {
			continue
		}
		// need append the row to rowTable
		var (
			row     = chk.GetRow(logicalRowIndex)
			partIdx = b.partIdxVector[logicalRowIndex]
			seg     *rowTableSegment
		)
		seg = hashJoinCtx.hashTableContext.getCurrentRowSegment(workerID, partIdx, hashJoinCtx.hashTableMeta, true, b.firstSegRowSizeHint)
		// first check if current seg is full
		if b.rowNumberInCurrentRowTableSeg[partIdx] >= maxRowTableSegmentSize || len(seg.rawData) >= maxRowTableSegmentByteSize {
			// finalize current seg and create a new seg
			hashJoinCtx.hashTableContext.finalizeCurrentSeg(workerID, partIdx, b)
			seg = hashJoinCtx.hashTableContext.getCurrentRowSegment(workerID, partIdx, hashJoinCtx.hashTableMeta, true, b.firstSegRowSizeHint)
		}
		if hasValidKey {
			seg.validJoinKeyPos = append(seg.validJoinKeyPos, len(seg.hashValues))
		}
		seg.hashValues = append(seg.hashValues, b.hashValue[logicalRowIndex])
		seg.rowStartOffset = append(seg.rowStartOffset, uint64(len(seg.rawData)))
		rowLength := 0
		// fill next_row_ptr field
		rowLength += fillNextRowPtr(seg)
		// fill null_map
		rowLength += fillNullMap(rowTableMeta, &row, seg)
		// fill serialized key and key length if needed
		rowLength += b.fillSerializedKeyAndKeyLengthIfNeeded(rowTableMeta, hasValidKey, logicalRowIndex, seg)
		// fill row data
		rowLength += fillRowData(rowTableMeta, &row, seg)
		// to make sure rowLength is 8 bit alignment
		if rowLength%8 != 0 {
			seg.rawData = append(seg.rawData, fakeAddrPlaceHolder[:8-rowLength%8]...)
		}
		b.rowNumberInCurrentRowTableSeg[partIdx]++
	}
	return nil
}
