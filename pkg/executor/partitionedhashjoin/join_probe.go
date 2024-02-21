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

package partitionedhashjoin

import (
	"bytes"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/hack"
	"hash/fnv"
	"unsafe"
)

type probeProcessInfo struct {
	chunk              *chunk.Chunk
	matchedRowsHeaders []unsafe.Pointer // the start address of each matched rows
	currentRowsPos     []unsafe.Pointer // the current address of each matched rows
	serializedKeys     [][]byte         // used for save serialized keys
	filterVector       []bool           // if there is filter before probe, filterVector saves the filter result
	nullKeyVector      []bool           // nullKeyVector[i] = true if any of the key is null
	currentProbeRow    int
	init               bool
}

type keyMode int

const (
	OneInt64 keyMode = iota
	FixedSerializedKey
	VariableSerializedKey
)

type probeCtx struct {
	sessCtx            sessionctx.Context
	keyIndex           []int
	columnTypes        []*types.FieldType
	filter             expression.CNFExprs
	postProbeCondition expression.CNFExprs
	tmpChk             *chunk.Chunk
	keyIsNullable      bool
	joinHashTable      *joinHashTable
	hashTableMeta      *tableMeta
	typeCtx            types.Context
	keyMode            keyMode
}

func (probeCtx *probeCtx) hasPostProbeCondition() bool {
	return probeCtx.postProbeCondition != nil
}

func (ppi *probeProcessInfo) initForCurrentChunk(ctx *probeCtx) error {
	if ppi.init {
		return nil
	}
	ppi.init = true
	rows := ppi.chunk.NumRows()
	ppi.currentProbeRow = 0
	if cap(ppi.matchedRowsHeaders) >= rows {
		ppi.matchedRowsHeaders = ppi.matchedRowsHeaders[:rows]
	} else {
		ppi.matchedRowsHeaders = make([]unsafe.Pointer, rows)
	}
	if ctx.filter != nil {
		if cap(ppi.filterVector) >= rows {
			ppi.filterVector = ppi.filterVector[:rows]
		} else {
			ppi.filterVector = make([]bool, rows)
		}
	}
	if ctx.keyIsNullable {
		if cap(ppi.nullKeyVector) >= rows {
			ppi.nullKeyVector = ppi.nullKeyVector[:rows]
		} else {
			ppi.nullKeyVector = make([]bool, rows)
		}
		for i := 0; i < rows; i++ {
			ppi.nullKeyVector = append(ppi.nullKeyVector, false)
		}
	}
	if cap(ppi.serializedKeys) >= rows {
		ppi.serializedKeys = ppi.serializedKeys[:rows]
	} else {
		ppi.serializedKeys = make([][]byte, rows)
	}
	if ctx.filter != nil {
		var err error
		ppi.filterVector, err = expression.VectorizedFilter(ctx.sessCtx, ctx.filter, chunk.NewIterator4Chunk(ppi.chunk), ppi.filterVector)
		if err != nil {
			return err
		}
	}

	// generate serialized key
	for index, keyIndex := range ctx.keyIndex {
		// todo set ignoreSign to false for unsigned key join signed key
		err := codec.SerializeKeys(ctx.typeCtx, ppi.chunk, ctx.columnTypes[keyIndex], keyIndex, ppi.filterVector, ppi.nullKeyVector, ctx.hashTableMeta.ignoreIntegerKeySignFlag[index], ppi.serializedKeys)
		if err != nil {
			return err
		}
	}
	// generate hash value
	hash := fnv.New64()
	for i := 0; i < rows; i++ {
		if (ppi.filterVector != nil && !ppi.filterVector[i]) || (ppi.nullKeyVector != nil && ppi.nullKeyVector[i]) {
			continue
		}
		hash.Reset()
		// As the golang doc described, `Hash.Write` never returns an error.
		// See https://golang.org/pkg/hash/#Hash
		_, _ = hash.Write(ppi.serializedKeys[i])
		ppi.matchedRowsHeaders[i] = ctx.joinHashTable.lookup(hash.Sum64())
	}
	return nil
}

type joinProbe interface {
	doProbe(chk *chunk.Chunk, info *probeProcessInfo) (err error)
}

type offsetAndLength struct {
	offset int
	length int
}
type baseJoinProbe struct {
	ctx              *probeCtx
	maxChunkSize     int
	rightAsBuildSide bool
	// lUsed/rUsed show which columns are used by father for left child and right child.
	// NOTE:
	// 1. lUsed/rUsed should never be nil.
	// 2. no columns are used if lUsed/rUsed is not nil but the size of lUsed/rUsed is 0.
	lUsed, rUsed         []int
	offsetAndLengthArray []offsetAndLength
}

func (baseJoinProbe *baseJoinProbe) getNext(chk *chunk.Chunk, info *probeProcessInfo) (err error) {
	if chk.NumRows() >= baseJoinProbe.maxChunkSize {
		return nil
	}
	if !info.init {
		err = info.initForCurrentChunk(baseJoinProbe.ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

type innerJoinProbe struct {
	baseJoinProbe
}

func (baseJoinProbe *baseJoinProbe) appendBuildRowToChunk(chk *chunk.Chunk, usedCols []int, rowStart unsafe.Pointer, rowData unsafe.Pointer, info *probeProcessInfo, colOffset int) (currentRowData unsafe.Pointer) {
	if len(usedCols) == 0 {
		return
	}
	if rowData == nil {
		rowData = baseJoinProbe.ctx.hashTableMeta.advanceToRowData(rowStart)
	}
	return baseJoinProbe.appendBuildRowToChunkInternal(chk, usedCols, rowStart, rowData, 0, -1, info, colOffset)
}

func (baseJoinProbe *baseJoinProbe) appendBuildRowToChunkInternal(chk *chunk.Chunk, usedCols []int, rowStart, rowData unsafe.Pointer, columnStartIndexInRowData int, columnsToAppend int, info *probeProcessInfo, colOffset int) (currentRowData unsafe.Pointer) {
	colIndexMap := make(map[int]int)
	for index, value := range usedCols {
		colIndexMap[value] = index + colOffset
	}
	meta := baseJoinProbe.ctx.hashTableMeta
	if columnsToAppend < 0 {
		columnsToAppend = len(meta.rowColumnsOrder)
	}
	for columnIndex := columnStartIndexInRowData; columnIndex < len(meta.rowColumnsOrder) && columnIndex < columnsToAppend; columnIndex++ {
		index, ok := colIndexMap[meta.rowColumnsOrder[columnIndex]]
		var currentColumn *chunk.Column
		if ok {
			currentColumn = chk.Column(index)
			currentColumn.AppendNullBitmap(!meta.isColumnNull(rowStart, columnIndex))
			rowData = chunk.AppendCellFromRawData(currentColumn, rowData)
		} else {
			// not used so don't need to insert into chk, but still need to advance rowData
			if meta.columnsSize[columnIndex] < 0 {
				size := *(*uint64)(rowData)
				rowData = unsafe.Add(rowData, SizeOfKeyLengthField+int(size))
			} else {
				rowData = unsafe.Add(rowData, meta.columnsSize[columnIndex])
			}
		}
	}
	return rowData
}

func (baseJoinProbe *baseJoinProbe) appendProbeRowToChunk(chk *chunk.Chunk, probeChk *chunk.Chunk, used []int, collOffset int) {
	if len(used) == 0 || len(baseJoinProbe.offsetAndLengthArray) == 0 {
		return
	}
	for index, colIndex := range used {
		srcCol := probeChk.Column(colIndex)
		dstCol := chk.Column(index + collOffset)
		for _, offsetAndLength := range baseJoinProbe.offsetAndLengthArray {
			dstCol.BatchAppend(srcCol, offsetAndLength.offset, offsetAndLength.length)
		}
	}
}

func isKeyMatched(keyMode keyMode, serializedKey []byte, rowStart unsafe.Pointer, meta *tableMeta) bool {
	switch keyMode {
	case OneInt64:
		return *(*int64)(unsafe.Pointer(&serializedKey[0])) == *(*int64)(unsafe.Add(rowStart, meta.nullMapLength+SizeOfNextPtr))
	case FixedSerializedKey:
		return bytes.Equal(serializedKey, hack.GetBytesFromPtr(unsafe.Add(rowStart, meta.nullMapLength+SizeOfNextPtr), meta.joinKeysLength))
	case VariableSerializedKey:
		return bytes.Equal(serializedKey, hack.GetBytesFromPtr(unsafe.Add(rowStart, meta.nullMapLength+SizeOfNextPtr+SizeOfKeyLengthField), int(meta.getSerializedKeyLength(rowStart))))
	default:
		panic("unknown key match type")
	}
}

func (innerJoinProbe *innerJoinProbe) doProbe(chk *chunk.Chunk, info *probeProcessInfo) (err error) {
	if innerJoinProbe.offsetAndLengthArray == nil {
		innerJoinProbe.offsetAndLengthArray = make([]offsetAndLength, 0, info.chunk.NumRows())
	} else {
		innerJoinProbe.offsetAndLengthArray = innerJoinProbe.offsetAndLengthArray[:0]
	}
	length := 0
	totalAddedRows := 0
	joinedChk := chk
	if innerJoinProbe.ctx.postProbeCondition != nil {
		panic("other condition is not supported")
		//if innerJoinProbe.ctx.tmpChk == nil {
		//	innerJoinProbe.ctx.tmpChk = chk.CloneEmpty(innerJoinProbe.maxChunkSize)
		//}
		//joinedChk = innerJoinProbe.ctx.tmpChk
	}
	maxAddedRows := chk.RequiredRows() - chk.NumRows()
	for totalAddedRows < maxAddedRows && info.currentProbeRow < info.chunk.NumRows() {
		if info.matchedRowsHeaders[info.currentProbeRow] != nil {
			if isKeyMatched(innerJoinProbe.ctx.keyMode, info.serializedKeys[info.currentProbeRow], info.matchedRowsHeaders[info.currentProbeRow], innerJoinProbe.ctx.hashTableMeta) {
				// key matched, convert row to column for build side
				if innerJoinProbe.rightAsBuildSide {
					innerJoinProbe.appendBuildRowToChunk(joinedChk, innerJoinProbe.rUsed, info.matchedRowsHeaders[info.currentProbeRow], nil, info, len(innerJoinProbe.lUsed))
				} else {
					innerJoinProbe.appendBuildRowToChunk(joinedChk, innerJoinProbe.lUsed, info.matchedRowsHeaders[info.currentProbeRow], nil, info, 0)
				}
				length++
				totalAddedRows++
			}
			info.matchedRowsHeaders[info.currentProbeRow] = getNextRowAddress(info.matchedRowsHeaders[info.currentProbeRow])
		} else {
			if length > 0 {
				innerJoinProbe.offsetAndLengthArray = append(innerJoinProbe.offsetAndLengthArray, offsetAndLength{offset: info.currentProbeRow, length: length})
				length = 0
			}
			info.currentProbeRow++
		}
	}
	if length > 0 {
		innerJoinProbe.offsetAndLengthArray = append(innerJoinProbe.offsetAndLengthArray, offsetAndLength{offset: info.currentProbeRow, length: length})
	}
	if innerJoinProbe.rightAsBuildSide {
		innerJoinProbe.appendProbeRowToChunk(joinedChk, info.chunk, innerJoinProbe.lUsed, 0)
	} else {
		innerJoinProbe.appendProbeRowToChunk(joinedChk, info.chunk, innerJoinProbe.rUsed, len(innerJoinProbe.lUsed))
	}
	if joinedChk.NumCols() == 0 {
		joinedChk.SetNumVirtualRows(chk.NumRows() + totalAddedRows)
	} else {
		joinedChk.SetNumVirtualRows(chk.NumRows())
	}
	return
}
