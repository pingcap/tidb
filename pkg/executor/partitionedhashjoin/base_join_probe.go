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

const BATCH_BUILD_ROW_SIZE = 32

func (hCtx *PartitionedHashJoinCtx) hasOtherCondition() bool {
	return hCtx.OtherCondition != nil
}

type JoinProbe interface {
	SetChunkForProbe(chunk *chunk.Chunk) error
	Probe(joinResult *util.HashjoinWorkerResult) (ok bool, result *util.HashjoinWorkerResult)
	IsCurrentChunkProbeDone() bool
	ScanRowTable(joinResult *util.HashjoinWorkerResult) (result *util.HashjoinWorkerResult)
	IsScanRowTableDone() bool
	NeedScanRowTable() bool
	InitForScanRowTable()
}

type offsetAndLength struct {
	offset int
	length int
}

type rowIndexInfo struct {
	probeRowIndex int
	buildRowStart uintptr
	buildRowData  uintptr
}

type baseJoinProbe struct {
	ctx    *PartitionedHashJoinCtx
	workID uint

	currentChunk       *chunk.Chunk
	matchedRowsHeaders []uintptr // the start address of each matched rows
	currentRowsPos     []uintptr // the current address of each matched rows
	serializedKeys     [][]byte  // used for save serialized keys
	filterVector       []bool    // if there is filter before probe, filterVector saves the filter result
	nullKeyVector      []bool    // nullKeyVector[i] = true if any of the key is null
	currentProbeRow    int
	chunkRows          int
	cachedBuildRows    []rowIndexInfo

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
		j.matchedRowsHeaders = make([]uintptr, rows)
	}
	if j.ctx.ProbeFilter != nil {
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
	for i := 0; i < rows; i++ {
		j.serializedKeys[i] = j.serializedKeys[i][:0]
	}
	if j.ctx.ProbeFilter != nil {
		j.filterVector, err = expression.VectorizedFilter(j.ctx.SessCtx.GetExprCtx(), j.ctx.SessCtx.GetSessionVars().EnableVectorizedExpression, j.ctx.ProbeFilter, chunk.NewIterator4Chunk(j.currentChunk), j.filterVector)
		if err != nil {
			return err
		}
	}

	// generate serialized key
	for index, keyIndex := range j.keyIndex {
		err = codec.SerializeKeys(j.ctx.SessCtx.GetSessionVars().StmtCtx.TypeCtx(), j.currentChunk, j.columnTypes[keyIndex], keyIndex, j.filterVector, j.nullKeyVector, j.ctx.hashTableMeta.serializeModes[index], j.serializedKeys)
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

func (j *baseJoinProbe) appendBuildRowToCachedBuildRowsAndConstructBuildRowsIfNeeded(buildRow rowIndexInfo, chk *chunk.Chunk, currentColumnIndexInRow int) {
	j.cachedBuildRows = append(j.cachedBuildRows, buildRow)
	if len(j.cachedBuildRows) >= BATCH_BUILD_ROW_SIZE {
		j.batchConstructBuildRows(chk, currentColumnIndexInRow)
	}
}

func (j *baseJoinProbe) batchConstructBuildRows(chk *chunk.Chunk, currentColumnIndexInRow int) {
	j.appendBuildRowToChunk(chk, currentColumnIndexInRow)
	if j.ctx.hasOtherCondition() {
		j.rowIndexInfos = append(j.rowIndexInfos, j.cachedBuildRows...)
	}
	j.cachedBuildRows = j.cachedBuildRows[:0]
}

func (j *baseJoinProbe) prepareForProbe(chk *chunk.Chunk) (joinedChk *chunk.Chunk, remainCap int, err error) {
	j.offsetAndLengthArray = j.offsetAndLengthArray[:0]
	j.cachedBuildRows = j.cachedBuildRows[:0]
	joinedChk = chk
	if j.ctx.OtherCondition != nil {
		j.tmpChk.Reset()
		j.rowIndexInfos = j.rowIndexInfos[:0]
		j.selected = j.selected[:0]
		joinedChk = j.tmpChk
	}
	return joinedChk, chk.RequiredRows() - chk.NumRows(), nil
}

func (j *baseJoinProbe) appendBuildRowToChunk(chk *chunk.Chunk, currentColumnIndexInRow int) {
	if j.rightAsBuildSide {
		if j.ctx.hasOtherCondition() {
			j.appendBuildRowToChunkInternal(chk, j.rUsedInOtherCondition, true, j.currentChunk.NumCols(), currentColumnIndexInRow)
		} else {
			j.appendBuildRowToChunkInternal(chk, j.rUsed, false, len(j.lUsed), currentColumnIndexInRow)
		}
	} else {
		if j.ctx.hasOtherCondition() {
			j.appendBuildRowToChunkInternal(chk, j.lUsedInOtherCondition, true, 0, currentColumnIndexInRow)
		} else {
			j.appendBuildRowToChunkInternal(chk, j.lUsed, false, 0, currentColumnIndexInRow)
		}
	}
}

func (j *baseJoinProbe) appendBuildRowToChunkInternal(chk *chunk.Chunk, usedCols []int, forOtherCondition bool, colOffset int, currentColumnInRow int) {
	if len(usedCols) == 0 || len(j.cachedBuildRows) == 0 {
		return
	}
	for i := 0; i < len(j.cachedBuildRows); i++ {
		if j.cachedBuildRows[i].buildRowData == 0 {
			j.cachedBuildRows[i].buildRowData = j.ctx.hashTableMeta.advanceToRowData(j.cachedBuildRows[i].buildRowStart)
		}
	}
	colIndexMap := make(map[int]int)
	for index, value := range usedCols {
		if forOtherCondition {
			colIndexMap[value] = value + colOffset
		} else {
			colIndexMap[value] = index + colOffset
		}
	}
	meta := j.ctx.hashTableMeta
	columnsToAppend := len(meta.rowColumnsOrder)
	if forOtherCondition {
		columnsToAppend = meta.columnCountNeededForOtherCondition
		if j.ctx.RightAsBuildSide {
			for _, value := range j.rUsed {
				colIndexMap[value] = value + colOffset
			}
		} else {
			for _, value := range j.lUsed {
				colIndexMap[value] = value + colOffset
			}
		}
	}
	for columnIndex := currentColumnInRow; columnIndex < len(meta.rowColumnsOrder) && columnIndex < columnsToAppend; columnIndex++ {
		index, ok := colIndexMap[meta.rowColumnsOrder[columnIndex]]
		var currentColumn *chunk.Column
		if ok {
			currentColumn = chk.Column(index)
			for _, row := range j.cachedBuildRows {
				currentColumn.AppendNullBitmap(!meta.isColumnNull(row.buildRowStart, columnIndex))
				row.buildRowData = chunk.AppendCellFromRawData(currentColumn, row.buildRowData)
			}
		} else {
			// not used so don't need to insert into chk, but still need to advance rowData
			for _, row := range j.cachedBuildRows {
				if meta.columnsSize[columnIndex] < 0 {
					size := *(*uint64)(unsafe.Pointer(row.buildRowData))
					row.buildRowData = uintptr(unsafe.Add(unsafe.Pointer(row.buildRowData), SizeOfLengthField+int(size)))
				} else {
					row.buildRowData = uintptr(unsafe.Add(unsafe.Pointer(row.buildRowData), meta.columnsSize[columnIndex]))
				}
			}
		}
	}
}

func (j *baseJoinProbe) appendProbeRowToChunk(chk *chunk.Chunk, probeChk *chunk.Chunk) {
	if j.rightAsBuildSide {
		if j.ctx.hasOtherCondition() {
			j.appendProbeRowToChunkInternal(chk, probeChk, j.lUsedInOtherCondition, 0, true)
		} else {
			j.appendProbeRowToChunkInternal(chk, probeChk, j.lUsed, 0, false)
		}
	} else {
		if j.ctx.hasOtherCondition() {
			j.appendProbeRowToChunkInternal(chk, probeChk, j.rUsedInOtherCondition, j.ctx.hashTableMeta.totalColumnNumber, true)
		} else {
			j.appendProbeRowToChunkInternal(chk, probeChk, j.rUsed, len(j.lUsed), false)
		}
	}
}

func (j *baseJoinProbe) appendProbeRowToChunkInternal(chk *chunk.Chunk, probeChk *chunk.Chunk, used []int, collOffset int, forOtherCondition bool) {
	if len(used) == 0 || len(j.offsetAndLengthArray) == 0 {
		return
	}
	if forOtherCondition {
		usedColumnMap := make(map[int]struct{})
		for _, colIndex := range used {
			if _, ok := usedColumnMap[colIndex]; !ok {
				srcCol := probeChk.Column(colIndex)
				dstCol := chk.Column(colIndex + collOffset)
				for _, offsetAndLength := range j.offsetAndLengthArray {
					dstCol.BatchAppend(srcCol, offsetAndLength.offset, offsetAndLength.length)
				}
				usedColumnMap[colIndex] = struct{}{}
			}
		}
	} else {
		for index, colIndex := range used {
			srcCol := probeChk.Column(colIndex)
			dstCol := chk.Column(index + collOffset)
			for _, offsetAndLength := range j.offsetAndLengthArray {
				dstCol.BatchAppend(srcCol, offsetAndLength.offset, offsetAndLength.length)
			}
		}
	}
}

func isKeyMatched(keyMode keyMode, serializedKey []byte, rowStart uintptr, meta *JoinTableMeta) bool {
	switch keyMode {
	case OneInt64:
		return *(*int64)(unsafe.Pointer(&serializedKey[0])) == *(*int64)(unsafe.Add(unsafe.Pointer(rowStart), meta.nullMapLength+SizeOfNextPtr))
	case FixedSerializedKey:
		return bytes.Equal(serializedKey, hack.GetBytesFromPtr(unsafe.Add(unsafe.Pointer(rowStart), meta.nullMapLength+SizeOfNextPtr), meta.joinKeysLength))
	case VariableSerializedKey:
		return bytes.Equal(serializedKey, hack.GetBytesFromPtr(unsafe.Add(unsafe.Pointer(rowStart), meta.nullMapLength+SizeOfNextPtr+SizeOfLengthField), int(meta.getSerializedKeyLength(rowStart))))
	default:
		panic("unknown key match type")
	}
}

func NewJoinProbe(ctx *PartitionedHashJoinCtx, workID uint, joinType core.JoinType, keyIndex []int, joinedColumnTypes, probeColumnTypes []*types.FieldType, rightAsBuildSide bool) JoinProbe {
	base := baseJoinProbe{
		ctx:                   ctx,
		workID:                workID,
		keyIndex:              keyIndex,
		columnTypes:           probeColumnTypes,
		maxChunkSize:          ctx.SessCtx.GetSessionVars().MaxChunkSize,
		lUsed:                 ctx.LUsed,
		rUsed:                 ctx.RUsed,
		lUsedInOtherCondition: ctx.LUsedInOtherCondition,
		rUsedInOtherCondition: ctx.RUsedInOtherCondition,
		rightAsBuildSide:      rightAsBuildSide,
	}
	for i := range keyIndex {
		if !mysql.HasNotNullFlag(base.columnTypes[i].GetFlag()) {
			base.hasNullableKey = true
		}
	}
	base.cachedBuildRows = make([]rowIndexInfo, 0, BATCH_BUILD_ROW_SIZE)
	base.matchedRowsHeaders = make([]uintptr, 0, chunk.InitialCapacity)
	base.serializedKeys = make([][]byte, 0, chunk.InitialCapacity)
	if base.ctx.ProbeFilter != nil {
		base.filterVector = make([]bool, 0, chunk.InitialCapacity)
	}
	if base.hasNullableKey {
		base.nullKeyVector = make([]bool, 0, chunk.InitialCapacity)
	}
	if base.ctx.OtherCondition != nil {
		base.tmpChk = chunk.NewChunkWithCapacity(joinedColumnTypes, chunk.InitialCapacity)
		base.tmpChk.SetInCompleteChunk(true)
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
