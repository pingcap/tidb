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
	"errors"
	"hash"
	"hash/fnv"
	"math"
	"strconv"
	"unsafe"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/serialization"
)

type preAllocHelper struct {
	totalRowNum int64
	validRowNum int64
	rawDataLen  int64

	hashValuesBuf      []uint64
	validJoinKeyPosBuf []int
}

func (h *preAllocHelper) reset() {
	h.totalRowNum = 0
	h.validRowNum = 0
	h.rawDataLen = 0
}

func (h *preAllocHelper) initBuf() {
	if cap(h.hashValuesBuf) == 0 {
		h.hashValuesBuf = make([]uint64, 0, 1024)
		h.validJoinKeyPosBuf = make([]int, 0, 1024)
	}

	h.hashValuesBuf = h.hashValuesBuf[:0]
	h.validJoinKeyPosBuf = h.validJoinKeyPosBuf[:0]
}

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

	serializedKeyLens    []int
	serializedKeysBuffer []byte

	// When respilling a row, we need to recalculate the row's hash value.
	// These are auxiliary utility for rehash.
	hash      hash.Hash64
	rehashBuf []byte

	nullMap         []byte
	partitionNumber uint

	helpers          []preAllocHelper
	partIDForEachRow []int
}

func createRowTableBuilder(buildKeyIndex []int, buildKeyTypes []*types.FieldType, partitionNumber uint, hasNullableKey bool, hasFilter bool, keepFilteredRows bool, nullMapLength int) *rowTableBuilder {
	builder := &rowTableBuilder{
		buildKeyIndex:    buildKeyIndex,
		buildKeyTypes:    buildKeyTypes,
		hasNullableKey:   hasNullableKey,
		hasFilter:        hasFilter,
		keepFilteredRows: keepFilteredRows,
		partitionNumber:  partitionNumber,
		nullMap:          make([]byte, nullMapLength),
		helpers:          make([]preAllocHelper, partitionNumber),
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

func (b *rowTableBuilder) checkMaxElementSize(chk *chunk.Chunk, hashJoinCtx *HashJoinCtxV2) (bool, int) {
	// check both join keys and the columns needed to be converted to row format
	for _, colIdx := range b.buildKeyIndex {
		column := chk.Column(colIdx)
		if column.ContainsVeryLargeElement() {
			return true, colIdx
		}
	}
	for _, colIdx := range hashJoinCtx.hashTableMeta.rowColumnsOrder {
		column := chk.Column(colIdx)
		if column.ContainsVeryLargeElement() {
			return true, colIdx
		}
	}
	return false, 0
}

func (b *rowTableBuilder) processOneChunk(chk *chunk.Chunk, typeCtx types.Context, hashJoinCtx *HashJoinCtxV2, workerID int) error {
	elementSizeExceedLimit, colIdx := b.checkMaxElementSize(chk, hashJoinCtx)
	if elementSizeExceedLimit {
		// TiDB's max row size is 128MB, so element size should never exceed limit
		return errors.New("row table build failed: column contains element larger than 4GB, column index: " + strconv.Itoa(colIdx))
	}
	b.ResetBuffer(chk)

	if len(b.usedRows) == 0 {
		return nil
	}

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
	b.serializedKeysBuffer, err = codec.SerializeKeys(
		typeCtx,
		chk,
		b.buildKeyTypes,
		b.buildKeyIndex,
		b.usedRows,
		b.filterVector,
		b.nullKeyVector,
		hashJoinCtx.hashTableMeta.serializeModes,
		b.serializedKeyVectorBuffer,
		b.serializedKeyLens,
		b.serializedKeysBuffer)
	if err != nil {
		return err
	}

	for _, key := range b.serializedKeyVectorBuffer {
		if len(key) > math.MaxUint32 {
			// TiDB's max row size is 128MB, so key size should never exceed limit
			return errors.New("row table build failed: join key contains element larger than 4GB")
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
		if logicalRows <= fakeSelLength {
			b.selRows = fakeSel[:logicalRows]
		} else {
			b.selRows = make([]int, logicalRows)
			for i := range logicalRows {
				b.selRows[i] = i
			}
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
		for i := range physicalRows {
			b.nullKeyVector[i] = false
		}
	}
	if cap(b.serializedKeyVectorBuffer) >= logicalRows {
		clear(b.serializedKeyVectorBuffer)
		b.serializedKeyVectorBuffer = b.serializedKeyVectorBuffer[:logicalRows]
	} else {
		b.serializedKeyVectorBuffer = make([][]byte, logicalRows)
	}
	if cap(b.serializedKeyLens) < logicalRows {
		b.serializedKeyLens = make([]int, logicalRows)
	} else {
		clear(b.serializedKeyLens)
		b.serializedKeyLens = b.serializedKeyLens[:logicalRows]
	}
}

func (b *rowTableBuilder) initRehashUtil() {
	if b.rehashBuf == nil {
		b.hash = fnv.New64()
		b.rehashBuf = make([]byte, serialization.Uint64Len)
	}
}

func (b *rowTableBuilder) preAllocForSegmentsInSpill(segs []*rowTableSegment, chk *chunk.Chunk, hashJoinCtx *HashJoinCtxV2, partitionNumber int) error {
	for i := range b.helpers {
		b.helpers[i].reset()
		b.helpers[i].initBuf()
	}

	rowNum := chk.NumRows()
	if cap(b.partIDForEachRow) < rowNum {
		b.partIDForEachRow = make([]int, 0, rowNum)
	}
	b.partIDForEachRow = b.partIDForEachRow[:rowNum]

	fakePartIndex := uint64(0)
	var newHashValue uint64
	var partID int
	var err error

	for i := range rowNum {
		if i%200 == 0 {
			err := checkSQLKiller(&hashJoinCtx.SessCtx.GetSessionVars().SQLKiller, "killedDuringRestoreBuild")
			if err != nil {
				return err
			}
		}

		row := chk.GetRow(i)
		validJoinKey := row.GetBytes(1)
		oldHashValue := row.GetUint64(0)

		var hasValidJoinKey uint64
		if validJoinKey[0] != byte(0) {
			hasValidJoinKey = 1
		} else {
			hasValidJoinKey = 0
		}

		if hasValidJoinKey != 0 {
			newHashValue, partID, err = b.regenerateHashValueAndPartIndex(oldHashValue, hashJoinCtx.partitionMaskOffset)
			if err != nil {
				return err
			}
			b.helpers[partID].validJoinKeyPosBuf = append(b.helpers[partID].validJoinKeyPosBuf, len(b.helpers[partID].hashValuesBuf))
		} else {
			partID = int(fakePartIndex)
			newHashValue = fakePartIndex
			fakePartIndex = (fakePartIndex + 1) % uint64(partitionNumber)
		}

		b.partIDForEachRow[i] = partID
		b.helpers[partID].hashValuesBuf = append(b.helpers[partID].hashValuesBuf, newHashValue)
		b.helpers[partID].totalRowNum++
		b.helpers[partID].rawDataLen += int64(row.GetRawLen(2))
	}

	totalMemUsage := int64(0)
	for _, helper := range b.helpers {
		totalMemUsage += helper.rawDataLen + (helper.totalRowNum+int64(len(helper.hashValuesBuf)))*serialization.Uint64Len + int64(len(helper.validJoinKeyPosBuf))*serialization.IntLen
	}

	hashJoinCtx.hashTableContext.memoryTracker.Consume(totalMemUsage)

	for partID, helper := range b.helpers {
		segs[partID].rawData = make([]byte, 0, helper.rawDataLen)
		segs[partID].rowStartOffset = make([]uint64, 0, helper.totalRowNum)
		segs[partID].hashValues = make([]uint64, len(helper.hashValuesBuf))
		segs[partID].validJoinKeyPos = make([]int, len(helper.validJoinKeyPosBuf))
	}
	return nil
}

func (b *rowTableBuilder) processOneRestoredChunk(chk *chunk.Chunk, hashJoinCtx *HashJoinCtxV2, workerID int, partitionNumber int) (err error) {
	// It must be called before `preAllocForSegmentsInSpill`
	b.initRehashUtil()

	segs := make([]*rowTableSegment, b.partitionNumber)
	for partIdx := range b.partitionNumber {
		segs[partIdx] = newRowTableSegment()
	}

	defer func() {
		if err == nil {
			for partIdx, seg := range segs {
				hashJoinCtx.hashTableContext.appendRowSegment(workerID, partIdx, seg)
			}
		}
	}()

	err = b.preAllocForSegmentsInSpill(segs, chk, hashJoinCtx, partitionNumber)
	if err != nil {
		return err
	}

	rowNum := chk.NumRows()
	for i := range rowNum {
		if i%200 == 0 {
			err = checkSQLKiller(&hashJoinCtx.SessCtx.GetSessionVars().SQLKiller, "killedDuringRestoreBuild")
			if err != nil {
				return err
			}
		}

		partID := b.partIDForEachRow[i]
		row := chk.GetRow(i)
		rowData := row.GetBytes(2)

		segs[partID].rowStartOffset = append(segs[partID].rowStartOffset, uint64(len(segs[partID].rawData)))
		segs[partID].rawData = append(segs[partID].rawData, rowData...)
	}

	for partID, helper := range b.helpers {
		copy(segs[partID].hashValues, helper.hashValuesBuf)
		copy(segs[partID].validJoinKeyPos, helper.validJoinKeyPosBuf)
	}
	return nil
}

func (b *rowTableBuilder) regenerateHashValueAndPartIndex(hashValue uint64, partitionMaskOffset int) (uint64, int, error) {
	newHashVal := rehash(hashValue, b.rehashBuf, b.hash)
	return newHashVal, int(generatePartitionIndex(newHashVal, partitionMaskOffset)), nil
}

func fillNullMap(rowTableMeta *joinTableMeta, row *chunk.Row, seg *rowTableSegment, bitmap []byte) int {
	if nullMapLength := rowTableMeta.nullMapLength; nullMapLength > 0 {
		for i := range nullMapLength {
			bitmap[i] = 0
		}

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
	return fakeAddrPlaceHolderLen
}

func (b *rowTableBuilder) fillSerializedKeyAndKeyLengthIfNeeded(rowTableMeta *joinTableMeta, hasValidKey bool, logicalRowIndex int, seg *rowTableSegment) int64 {
	appendRowLength := int64(0)
	// 1. fill key length if needed
	if !rowTableMeta.isJoinKeysFixedLength {
		// if join_key is not fixed length: `key_length` need to be written in rawData
		// even the join keys is inlined, for example if join key is 2 binary string
		// then the inlined join key should be: col1_size + col1_data + col2_size + col2_data
		// and len(col1_size + col1_data + col2_size + col2_data) need to be written before the inlined join key
		length := uint32(0)
		if hasValidKey {
			length = uint32(len(b.serializedKeyVectorBuffer[logicalRowIndex]))
		} else {
			length = 0
		}
		seg.rawData = append(seg.rawData, unsafe.Slice((*byte)(unsafe.Pointer(&length)), sizeOfElementSize)...)
		appendRowLength += int64(sizeOfElementSize)
	}
	// 2. fill serialized key if needed
	if !rowTableMeta.isJoinKeysInlined {
		// if join_key is not inlined: `serialized_key` need to be written in rawData
		if hasValidKey {
			seg.rawData = append(seg.rawData, b.serializedKeyVectorBuffer[logicalRowIndex]...)
			appendRowLength += int64(len(b.serializedKeyVectorBuffer[logicalRowIndex]))
		} else {
			// if there is no valid key, and the key is fixed length, then write a fake key
			if rowTableMeta.isJoinKeysFixedLength {
				seg.rawData = append(seg.rawData, rowTableMeta.fakeKeyByte...)
				appendRowLength += int64(rowTableMeta.joinKeysLength)
			}
			// otherwise don't need to write since length is 0
		}
	}
	return appendRowLength
}

func fillRowData(rowTableMeta *joinTableMeta, row *chunk.Row, seg *rowTableSegment) int64 {
	appendRowLength := int64(0)
	for index, colIdx := range rowTableMeta.rowColumnsOrder {
		if rowTableMeta.columnsSize[index] > 0 {
			// fixed size
			seg.rawData = append(seg.rawData, row.GetRaw(colIdx)...)
			appendRowLength += int64(rowTableMeta.columnsSize[index])
		} else {
			// length, raw_data
			raw := row.GetRaw(colIdx)
			length := uint32(len(raw))
			seg.rawData = append(seg.rawData, unsafe.Slice((*byte)(unsafe.Pointer(&length)), sizeOfElementSize)...)
			seg.rawData = append(seg.rawData, raw...)
			appendRowLength += int64(length) + int64(sizeOfElementSize)
		}
	}
	return appendRowLength
}

func (b *rowTableBuilder) calculateSerializedKeyAndKeyLength(rowTableMeta *joinTableMeta, hasValidKey bool, logicalRowIndex int) int64 {
	appendRowLength := int64(0)
	if !rowTableMeta.isJoinKeysFixedLength {
		appendRowLength += int64(sizeOfElementSize)
	}
	if !rowTableMeta.isJoinKeysInlined {
		if hasValidKey {
			appendRowLength += int64(len(b.serializedKeyVectorBuffer[logicalRowIndex]))
		} else {
			if rowTableMeta.isJoinKeysFixedLength {
				appendRowLength += int64(rowTableMeta.joinKeysLength)
			}
		}
	}
	return appendRowLength
}

func calculateRowDataLength(rowTableMeta *joinTableMeta, row *chunk.Row) int64 {
	appendRowLength := int64(0)
	for index, colIdx := range rowTableMeta.rowColumnsOrder {
		if rowTableMeta.columnsSize[index] > 0 {
			appendRowLength += int64(rowTableMeta.columnsSize[index])
		} else {
			appendRowLength += int64(row.GetRawLen(colIdx)) + int64(sizeOfElementSize)
		}
	}
	return appendRowLength
}

func calculateFakeLength(rowLength int64) int64 {
	return (8 - rowLength%8) % 8
}

func (b *rowTableBuilder) preAllocForSegments(segs []*rowTableSegment, chk *chunk.Chunk, hashJoinCtx *HashJoinCtxV2) {
	for i := range b.helpers {
		b.helpers[i].reset()
	}

	rowTableMeta := hashJoinCtx.hashTableMeta
	for logicalRowIndex, physicalRowIndex := range b.usedRows {
		hasValidKey := (!b.hasFilter || b.filterVector[physicalRowIndex]) && (!b.hasNullableKey || !b.nullKeyVector[physicalRowIndex])
		if !hasValidKey && !b.keepFilteredRows {
			continue
		}

		var (
			row     = chk.GetRow(logicalRowIndex)
			partIdx = b.partIdxVector[logicalRowIndex]
		)

		b.helpers[partIdx].totalRowNum++

		if hasValidKey {
			b.helpers[partIdx].validRowNum++
		}

		rowLength := int64(fakeAddrPlaceHolderLen) + int64(rowTableMeta.nullMapLength)
		rowLength += b.calculateSerializedKeyAndKeyLength(rowTableMeta, hasValidKey, logicalRowIndex)
		rowLength += calculateRowDataLength(rowTableMeta, &row)
		rowLength += calculateFakeLength(rowLength)
		b.helpers[partIdx].rawDataLen += rowLength
	}

	totalMemUsage := int64(0)
	for i := range b.helpers {
		totalMemUsage += b.helpers[i].rawDataLen + (b.helpers[i].totalRowNum+b.helpers[i].totalRowNum)*serialization.Uint64Len + b.helpers[i].validRowNum*serialization.IntLen
	}

	hashJoinCtx.hashTableContext.memoryTracker.Consume(totalMemUsage)

	for partIdx, seg := range segs {
		seg.rawData = make([]byte, 0, b.helpers[partIdx].rawDataLen)
		seg.hashValues = make([]uint64, 0, b.helpers[partIdx].totalRowNum)
		seg.rowStartOffset = make([]uint64, 0, b.helpers[partIdx].totalRowNum)
		seg.validJoinKeyPos = make([]int, 0, b.helpers[partIdx].validRowNum)
	}
}

func (b *rowTableBuilder) appendToRowTable(chk *chunk.Chunk, hashJoinCtx *HashJoinCtxV2, workerID int) (err error) {
	segs := make([]*rowTableSegment, b.partitionNumber)
	for partIdx := range b.partitionNumber {
		segs[partIdx] = newRowTableSegment()
	}

	defer func() {
		if err == nil {
			for partIdx, seg := range segs {
				hashJoinCtx.hashTableContext.appendRowSegment(workerID, partIdx, seg)
			}
		}
	}()

	b.preAllocForSegments(segs, chk, hashJoinCtx)

	rowTableMeta := hashJoinCtx.hashTableMeta
	for logicalRowIndex, physicalRowIndex := range b.usedRows {
		if logicalRowIndex%10 == 0 || logicalRowIndex == len(b.usedRows)-1 {
			err = checkSQLKiller(&hashJoinCtx.SessCtx.GetSessionVars().SQLKiller, "killedDuringBuild")
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

		seg = segs[partIdx]

		if hasValidKey {
			seg.validJoinKeyPos = append(seg.validJoinKeyPos, len(seg.hashValues))
		}
		seg.hashValues = append(seg.hashValues, b.hashValue[logicalRowIndex])
		seg.rowStartOffset = append(seg.rowStartOffset, uint64(len(seg.rawData)))
		rowLength := int64(0)
		// fill next_row_ptr field
		rowLength += int64(fillNextRowPtr(seg))
		// fill null_map
		rowLength += int64(fillNullMap(rowTableMeta, &row, seg, b.nullMap))
		// fill serialized key and key length if needed
		rowLength += b.fillSerializedKeyAndKeyLengthIfNeeded(rowTableMeta, hasValidKey, logicalRowIndex, seg)
		// fill row data
		rowLength += fillRowData(rowTableMeta, &row, seg)
		// to make sure rowLength is 8 bit alignment
		if rowLength%8 != 0 {
			seg.rawData = append(seg.rawData, fakeAddrPlaceHolder[:8-rowLength%8]...)
		}
	}
	return nil
}
