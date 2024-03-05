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
	"github.com/pingcap/tidb/pkg/executor/internal/util"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core"
	"hash/fnv"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/hack"
)

type keyMode int

const (
	OneInt64 keyMode = iota
	FixedSerializedKey
	VariableSerializedKey
)

func (hCtx *PartitionedHashJoinCtx) hasOtherCondition() bool {
	return hCtx.OtherCondition != nil
}

type JoinProbe interface {
	SetChunkForProbe(chunk *chunk.Chunk) error
	Probe(joinResult *util.HashjoinWorkerResult) (ok bool, result *util.HashjoinWorkerResult)
	IsCurrentChunkProbeDone() bool
	ScanHT(joinResult *util.HashjoinWorkerResult) (result *util.HashjoinWorkerResult)
	IsScanHTDone() bool
	NeedScanHT() bool
}

type offsetAndLength struct {
	offset int
	length int
}

type rowInfo struct {
	rowStart           unsafe.Pointer
	rowData            unsafe.Pointer
	currentColumnIndex int
}

type rowIndexInfo struct {
	probeRowIndex int
	buildRowStart unsafe.Pointer
	buildRowData  unsafe.Pointer
}

type baseJoinProbe struct {
	ctx *PartitionedHashJoinCtx

	currentChunk       *chunk.Chunk
	matchedRowsHeaders []unsafe.Pointer // the start address of each matched rows
	currentRowsPos     []unsafe.Pointer // the current address of each matched rows
	serializedKeys     [][]byte         // used for save serialized keys
	filterVector       []bool           // if there is filter before probe, filterVector saves the filter result
	nullKeyVector      []bool           // nullKeyVector[i] = true if any of the key is null
	currentProbeRow    int
	chunkRows          int

	keyIndex         []int
	columnTypes      []*types.FieldType
	hasNullableKey   bool
	maxChunkSize     int
	rightAsBuildSide bool
	// lUsed/rUsed show which columns are used by father for left child and right child.
	// NOTE:
	// 1. lUsed/rUsed should never be nil.
	// 2. no columns are used if lUsed/rUsed is not nil but the size of lUsed/rUsed is 0.
	lUsed, rUsed                                 []int
	lUsedInOtherCondition, rUsedInOtherCondition []int
	// used when construct column from probe side
	offsetAndLengthArray []offsetAndLength
	// these 3 variables are used for join that has other condition, should be inited when the join has other condition
	tmpChk        *chunk.Chunk
	rowIndexInfos []rowIndexInfo
	selected      []bool
}

func (j *baseJoinProbe) IsCurrentChunkProbeDone() bool {
	return j.currentChunk == nil || j.currentProbeRow >= j.chunkRows
}

func (j *baseJoinProbe) SetChunkForProbe(chk *chunk.Chunk) (err error) {
	if j.currentChunk != nil {
		if j.currentProbeRow < j.chunkRows {
			return errors.New("Previous chunk is not probed yet")
		}
	}
	j.currentChunk = chk
	j.currentProbeRow = 0
	rows := chk.NumRows()
	j.chunkRows = rows
	if cap(j.matchedRowsHeaders) >= rows {
		j.matchedRowsHeaders = j.matchedRowsHeaders[:rows]
	} else {
		j.matchedRowsHeaders = make([]unsafe.Pointer, rows)
	}
	if j.ctx.Filter != nil {
		if cap(j.filterVector) >= rows {
			j.filterVector = j.filterVector[:rows]
		} else {
			j.filterVector = make([]bool, rows)
		}
	}
	if j.hasNullableKey {
		if cap(j.nullKeyVector) >= rows {
			j.nullKeyVector = j.nullKeyVector[:rows]
		} else {
			j.nullKeyVector = make([]bool, rows)
		}
		for i := 0; i < rows; i++ {
			j.nullKeyVector = append(j.nullKeyVector, false)
		}
	}
	if cap(j.serializedKeys) >= rows {
		j.serializedKeys = j.serializedKeys[:rows]
	} else {
		j.serializedKeys = make([][]byte, rows)
	}
	if j.ctx.Filter != nil {
		j.filterVector, err = expression.VectorizedFilter(j.ctx.SessCtx.GetExprCtx(), j.ctx.Filter, chunk.NewIterator4Chunk(j.currentChunk), j.filterVector)
		if err != nil {
			return err
		}
	}

	// generate serialized key
	for index, keyIndex := range j.keyIndex {
		err = codec.SerializeKeys(j.ctx.SessCtx.GetSessionVars().StmtCtx.TypeCtx(), j.currentChunk, j.columnTypes[keyIndex], keyIndex, j.filterVector, j.nullKeyVector, j.ctx.hashTableMeta.ignoreIntegerKeySignFlag[index], j.serializedKeys)
		if err != nil {
			return err
		}
	}
	// generate hash value
	hash := fnv.New64()
	for i := 0; i < rows; i++ {
		if (j.filterVector != nil && !j.filterVector[i]) || (j.nullKeyVector != nil && j.nullKeyVector[i]) {
			continue
		}
		hash.Reset()
		// As the golang doc described, `Hash.Write` never returns an error.
		// See https://golang.org/pkg/hash/#Hash
		_, _ = hash.Write(j.serializedKeys[i])
		j.matchedRowsHeaders[i] = j.ctx.joinHashTable.lookup(hash.Sum64())
	}
	return
}

func (j *baseJoinProbe) appendOffsetAndLength(offset int, length int) {
	if length > 0 {
		j.offsetAndLengthArray = append(j.offsetAndLengthArray, offsetAndLength{offset: offset, length: length})
	}
}

func (j *baseJoinProbe) prepareForProbe(chk *chunk.Chunk) (joinedChk *chunk.Chunk, remainCap int, err error) {
	j.offsetAndLengthArray = j.offsetAndLengthArray[:0]
	joinedChk = chk
	if j.ctx.OtherCondition != nil {
		j.tmpChk.Reset()
		j.rowIndexInfos = j.rowIndexInfos[:0]
		j.selected = j.selected[:0]
		joinedChk = j.tmpChk
	}
	return joinedChk, chk.RequiredRows() - chk.NumRows(), nil
}

func (j *baseJoinProbe) appendBuildRowToChunk(chk *chunk.Chunk, rowInfo *rowInfo) (currentRowData unsafe.Pointer) {
	meta := j.ctx.hashTableMeta
	if j.rightAsBuildSide {
		if j.ctx.hasOtherCondition() {
			return j.appendBuildRowToChunkInternal(chk, j.rUsedInOtherCondition, rowInfo, meta.columnCountNeededForOtherCondition, j.currentChunk.NumCols())
		} else {
			return j.appendBuildRowToChunkInternal(chk, j.rUsed, rowInfo, -1, len(j.lUsed))
		}
	} else {
		if j.ctx.hasOtherCondition() {
			return j.appendBuildRowToChunkInternal(chk, j.lUsedInOtherCondition, rowInfo, meta.columnCountNeededForOtherCondition, 0)
		} else {
			return j.appendBuildRowToChunkInternal(chk, j.lUsed, rowInfo, -1, 0)
		}
	}
}

func (j *baseJoinProbe) appendBuildRowToChunkInternal(chk *chunk.Chunk, usedCols []int, rowInfo *rowInfo, columnsToAppend int, colOffset int) (currentRowData unsafe.Pointer) {
	if len(usedCols) == 0 {
		return
	}
	if rowInfo.rowData == nil {
		rowInfo.rowData = j.ctx.hashTableMeta.advanceToRowData(rowInfo.rowStart)
	}
	colIndexMap := make(map[int]int)
	for index, value := range usedCols {
		colIndexMap[value] = index + colOffset
	}
	meta := j.ctx.hashTableMeta
	if columnsToAppend < 0 {
		columnsToAppend = len(meta.rowColumnsOrder)
	}
	for columnIndex := rowInfo.currentColumnIndex; columnIndex < len(meta.rowColumnsOrder) && columnIndex < columnsToAppend; columnIndex++ {
		index, ok := colIndexMap[meta.rowColumnsOrder[columnIndex]]
		var currentColumn *chunk.Column
		if ok {
			currentColumn = chk.Column(index)
			currentColumn.AppendNullBitmap(!meta.isColumnNull(rowInfo.rowStart, columnIndex))
			rowInfo.rowData = chunk.AppendCellFromRawData(currentColumn, rowInfo.rowData)
		} else {
			// not used so don't need to insert into chk, but still need to advance rowData
			if meta.columnsSize[columnIndex] < 0 {
				size := *(*uint64)(rowInfo.rowData)
				rowInfo.rowData = unsafe.Add(rowInfo.rowData, SizeOfKeyLengthField+int(size))
			} else {
				rowInfo.rowData = unsafe.Add(rowInfo.rowData, meta.columnsSize[columnIndex])
			}
		}
	}
	return rowInfo.rowData
}

func (j *baseJoinProbe) appendProbeRowToChunk(chk *chunk.Chunk, probeChk *chunk.Chunk) {
	if j.rightAsBuildSide {
		if j.ctx.hasOtherCondition() {
			j.appendProbeRowToChunkInternal(chk, probeChk, j.lUsedInOtherCondition, 0)
		} else {
			j.appendProbeRowToChunkInternal(chk, probeChk, j.lUsed, 0)
		}
	} else {
		if j.ctx.hasOtherCondition() {
			j.appendProbeRowToChunkInternal(chk, probeChk, j.rUsedInOtherCondition, j.ctx.hashTableMeta.totalColumnNumber)
		} else {
			j.appendProbeRowToChunkInternal(chk, probeChk, j.rUsed, len(j.lUsed))
		}
	}
}

func (j *baseJoinProbe) appendProbeRowToChunkInternal(chk *chunk.Chunk, probeChk *chunk.Chunk, used []int, collOffset int) {
	if len(used) == 0 || len(j.offsetAndLengthArray) == 0 {
		return
	}
	for index, colIndex := range used {
		srcCol := probeChk.Column(colIndex)
		dstCol := chk.Column(index + collOffset)
		for _, offsetAndLength := range j.offsetAndLengthArray {
			dstCol.BatchAppend(srcCol, offsetAndLength.offset, offsetAndLength.length)
		}
	}
}

func isKeyMatched(keyMode keyMode, serializedKey []byte, rowStart unsafe.Pointer, meta *JoinTableMeta) bool {
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

func NewJoinProbe(ctx *PartitionedHashJoinCtx, joinType core.JoinType, keyIndex []int, joinedColumnTypes, probeColumnTypes []*types.FieldType) JoinProbe {
	base := baseJoinProbe{
		ctx:                   ctx,
		keyIndex:              keyIndex,
		columnTypes:           probeColumnTypes,
		maxChunkSize:          ctx.SessCtx.GetSessionVars().MaxChunkSize,
		lUsed:                 ctx.LUsed,
		rUsed:                 ctx.RUsed,
		lUsedInOtherCondition: ctx.LUsedInOtherCondition,
		rUsedInOtherCondition: ctx.RUsedInOtherCondition,
	}
	for i := range keyIndex {
		if !mysql.HasNotNullFlag(base.columnTypes[i].GetFlag()) {
			base.hasNullableKey = true
		}
	}
	base.matchedRowsHeaders = make([]unsafe.Pointer, 0, chunk.InitialCapacity)
	base.serializedKeys = make([][]byte, 0, chunk.InitialCapacity)
	if base.ctx.Filter != nil {
		base.filterVector = make([]bool, 0, chunk.InitialCapacity)
	}
	if base.hasNullableKey {
		base.nullKeyVector = make([]bool, 0, chunk.InitialCapacity)
	}
	if base.ctx.OtherCondition != nil {
		base.tmpChk = chunk.NewEmptyChunk(joinedColumnTypes)
		base.selected = make([]bool, 0, chunk.InitialCapacity)
		base.rowIndexInfos = make([]rowIndexInfo, 0, chunk.InitialCapacity)
	}
	switch joinType {
	case core.InnerJoin:
		return &innerJoinProbe{base}
	case core.LeftOuterJoin:
		return &leftOuterJoinProbe{innerJoinProbe: innerJoinProbe{base}}
	default:
		panic("unsupported join type")
	}
}
