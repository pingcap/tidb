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

type posAndHashValue struct {
	hashValue uint64
	pos       int
}

type baseJoinProbe struct {
	ctx    *PartitionedHashJoinCtx
	workID uint

	currentChunk *chunk.Chunk
	// if currentChunk.Sel() == nil, then construct a fake selRows
	selRows  []int
	usedRows []int
	// matchedRowsHeaders, currentRowsPos, serializedKeys is indexed by logical row index
	matchedRowsHeaders []uintptr // the start address of each matched rows
	currentRowsPos     []uintptr // the current address of each matched rows
	serializedKeys     [][]byte  // used for save serialized keys
	// filterVector and nullKeyVector is indexed by physical row index because the return vector of VectorizedFilter is based on physical row index
	filterVector    []bool              // if there is filter before probe, filterVector saves the filter result
	nullKeyVector   []bool              // nullKeyVector[i] = true if any of the key is null
	hashValues      [][]posAndHashValue // the start address of each matched rows
	currentProbeRow int
	chunkRows       int
	cachedBuildRows []rowIndexInfo

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

func (j *baseJoinProbe) advanceCurrentProbeRow() {
	j.currentProbeRow++
	//if j.currentProbeRow < j.chunkRows {
	//	j.matchedRowsHeaders[j.currentProbeRow] = j.ctx.joinHashTable.lookup(j.hashValues[j.currentProbeRow])
	//}
}

func (j *baseJoinProbe) SetChunkForProbe(chk *chunk.Chunk) (err error) {
	if j.currentChunk != nil {
		if j.currentProbeRow < j.chunkRows {
			return errors.New("Previous chunk is not probed yet")
		}
	}
	j.currentChunk = chk
	logicalRows := chk.NumRows()
	// if chk.sel != nil, then physicalRows is different from logicalRows
	physicalRows := chk.Column(0).Rows()
	j.usedRows = chk.Sel()
	if j.usedRows == nil {
		if cap(j.selRows) >= logicalRows {
			j.selRows = j.selRows[:0]
		} else {
			j.selRows = make([]int, 0, logicalRows)
		}
		for i := 0; i < logicalRows; i++ {
			j.selRows = append(j.selRows, i)
		}
		j.usedRows = j.selRows
	}
	j.chunkRows = logicalRows
	if cap(j.matchedRowsHeaders) >= logicalRows {
		j.matchedRowsHeaders = j.matchedRowsHeaders[:logicalRows]
	} else {
		j.matchedRowsHeaders = make([]uintptr, logicalRows)
	}
	for i := 0; i < j.ctx.PartitionNumber; i++ {
		j.hashValues[i] = j.hashValues[i][:0]
	}
	//if cap(j.hashValues) >= rows {
	//	j.hashValues = j.hashValues[:rows]
	//} else {
	//	j.hashValues = make([]uint64, rows)
	//}
	if j.ctx.ProbeFilter != nil {
		if cap(j.filterVector) >= physicalRows {
			j.filterVector = j.filterVector[:physicalRows]
		} else {
			j.filterVector = make([]bool, physicalRows)
		}
	}
	if j.hasNullableKey {
		if cap(j.nullKeyVector) >= physicalRows {
			j.nullKeyVector = j.nullKeyVector[:physicalRows]
		} else {
			j.nullKeyVector = make([]bool, physicalRows)
		}
		for i := 0; i < physicalRows; i++ {
			j.nullKeyVector = append(j.nullKeyVector, false)
		}
	}
	if cap(j.serializedKeys) >= logicalRows {
		j.serializedKeys = j.serializedKeys[:logicalRows]
	} else {
		j.serializedKeys = make([][]byte, logicalRows)
	}
	for i := 0; i < logicalRows; i++ {
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
		err = codec.SerializeKeys(j.ctx.SessCtx.GetSessionVars().StmtCtx.TypeCtx(), j.currentChunk, j.columnTypes[keyIndex], keyIndex, j.usedRows, j.filterVector, j.nullKeyVector, j.ctx.hashTableMeta.serializeModes[index], j.serializedKeys)
		if err != nil {
			return err
		}
	}
	// generate hash value
	hash := fnv.New64()
	for logicalRowIndex, physicalRowIndex := range j.usedRows {
		if (j.filterVector != nil && !j.filterVector[physicalRowIndex]) || (j.nullKeyVector != nil && j.nullKeyVector[physicalRowIndex]) {
			// explicit set the matchedRowsHeaders[logicalRowIndex] to nil to indicate there is no matched rows
			j.matchedRowsHeaders[logicalRowIndex] = 0
			continue
		}
		hash.Reset()
		// As the golang doc described, `Hash.Write` never returns an error.
		// See https://golang.org/pkg/hash/#Hash
		_, _ = hash.Write(j.serializedKeys[logicalRowIndex])
		hashValue := hash.Sum64()
		partIndex := hashValue % uint64(j.ctx.PartitionNumber)
		j.hashValues[partIndex] = append(j.hashValues[partIndex], posAndHashValue{hashValue: hashValue, pos: logicalRowIndex})
		//j.matchedRowsHeaders[i] = j.ctx.joinHashTable.lookup(j.hashValues[i])
	}
	j.currentProbeRow = 0
	for i := 0; i < j.ctx.PartitionNumber; i++ {
		for index := range j.hashValues[i] {
			j.matchedRowsHeaders[j.hashValues[i][index].pos] = j.ctx.joinHashTable.tables[i].lookup(j.hashValues[i][index].hashValue)
		}
	}
	//j.matchedRowsHeaders[j.currentProbeRow] = j.ctx.joinHashTable.lookup(j.hashValues[j.currentProbeRow])
	return
}

func (j *baseJoinProbe) appendOffsetAndLength(offset int, length int) {
	if length > 0 {
		j.offsetAndLengthArray = append(j.offsetAndLengthArray, offsetAndLength{offset: j.usedRows[offset], length: length})
	}
}

func (j *baseJoinProbe) appendBuildRowToCachedBuildRowsAndConstructBuildRowsIfNeeded(buildRow rowIndexInfo, chk *chunk.Chunk, currentColumnIndexInRow int, forOtherCondition bool) {
	j.cachedBuildRows = append(j.cachedBuildRows, buildRow)
	if len(j.cachedBuildRows) >= BATCH_BUILD_ROW_SIZE {
		j.batchConstructBuildRows(chk, currentColumnIndexInRow, forOtherCondition)
	}
}

func (j *baseJoinProbe) batchConstructBuildRows(chk *chunk.Chunk, currentColumnIndexInRow int, forOtherCondition bool) {
	j.appendBuildRowToChunk(chk, currentColumnIndexInRow, forOtherCondition)
	if forOtherCondition {
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

func (j *baseJoinProbe) appendBuildRowToChunk(chk *chunk.Chunk, currentColumnIndexInRow int, forOtherCondition bool) {
	if j.rightAsBuildSide {
		if forOtherCondition {
			j.appendBuildRowToChunkInternal(chk, j.rUsedInOtherCondition, true, j.currentChunk.NumCols(), currentColumnIndexInRow)
		} else {
			j.appendBuildRowToChunkInternal(chk, j.rUsed, false, len(j.lUsed), currentColumnIndexInRow)
		}
	} else {
		if forOtherCondition {
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
	chkRows := chk.NumRows()
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
			for index := range j.cachedBuildRows {
				currentColumn.AppendNullBitmap(!meta.isColumnNull(j.cachedBuildRows[index].buildRowStart, columnIndex))
				j.cachedBuildRows[index].buildRowData = chunk.AppendCellFromRawData(currentColumn, j.cachedBuildRows[index].buildRowData)
			}
		} else {
			// not used so don't need to insert into chk, but still need to advance rowData
			if meta.columnsSize[columnIndex] < 0 {
				for index := range j.cachedBuildRows {
					size := *(*uint64)(unsafe.Pointer(j.cachedBuildRows[index].buildRowData))
					j.cachedBuildRows[index].buildRowData = uintptr(unsafe.Add(unsafe.Pointer(j.cachedBuildRows[index].buildRowData), SizeOfLengthField+int(size)))
				}
			} else {
				for index := range j.cachedBuildRows {
					j.cachedBuildRows[index].buildRowData = uintptr(unsafe.Add(unsafe.Pointer(j.cachedBuildRows[index].buildRowData), meta.columnsSize[columnIndex]))
				}
			}
		}
	}
	chk.SetNumVirtualRows(chkRows + len(j.cachedBuildRows))
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
	base.selRows = make([]int, 0, chunk.InitialCapacity)
	base.hashValues = make([][]posAndHashValue, ctx.PartitionNumber)
	for i := 0; i < ctx.PartitionNumber; i++ {
		base.hashValues[i] = make([]posAndHashValue, 0, chunk.InitialCapacity)
	}
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
