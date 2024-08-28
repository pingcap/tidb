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
	"bytes"
	"hash/fnv"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
)

type keyMode int

const (
	// OneInt64 mean the key contains only one Int64
	OneInt64 keyMode = iota
	// FixedSerializedKey mean the key has fixed length
	FixedSerializedKey
	// VariableSerializedKey mean the key has variable length
	VariableSerializedKey
)

const batchBuildRowSize = 32

func (hCtx *HashJoinCtxV2) hasOtherCondition() bool {
	return hCtx.OtherCondition != nil
}

// ProbeV2 is the interface used to do probe in hash join v2
type ProbeV2 interface {
	// SetChunkForProbe will do some pre-work when start probing a chunk
	SetChunkForProbe(chunk *chunk.Chunk) error
	// Probe is to probe current chunk, the result chunk is set in result.chk, and Probe need to make sure result.chk.NumRows() <= result.chk.RequiredRows()
	Probe(joinResult *hashjoinWorkerResult, sqlKiller *sqlkiller.SQLKiller) (ok bool, result *hashjoinWorkerResult)
	// IsCurrentChunkProbeDone returns true if current probe chunk is all probed
	IsCurrentChunkProbeDone() bool
	// ScanRowTable is called after all the probe chunks are probed. It is used in some special joins, like left outer join with left side to build, after all
	// the probe side chunks are handled, it needs to scan the row table to return the un-matched rows
	ScanRowTable(joinResult *hashjoinWorkerResult, sqlKiller *sqlkiller.SQLKiller) (result *hashjoinWorkerResult)
	// IsScanRowTableDone returns true after scan row table is done
	IsScanRowTableDone() bool
	// NeedScanRowTable returns true if current join need to scan row table after all the probe side chunks are handled
	NeedScanRowTable() bool
	// InitForScanRowTable do some pre-work before ScanRowTable, it must be called before ScanRowTable
	InitForScanRowTable()
	// Return probe collsion
	GetProbeCollision() uint64
	// Reset probe collsion
	ResetProbeCollision()
}

type offsetAndLength struct {
	offset int
	length int
}

type matchedRowInfo struct {
	// probeRowIndex mean the probe side index of current matched row
	probeRowIndex int
	// buildRowStart mean the build row start of the current matched row
	buildRowStart uintptr
	// buildRowOffset mean the current offset of current BuildRow, used to construct column data from BuildRow
	buildRowOffset int
}

func createMatchRowInfo(probeRowIndex int, buildRowStart unsafe.Pointer) *matchedRowInfo {
	ret := &matchedRowInfo{probeRowIndex: probeRowIndex}
	*(*unsafe.Pointer)(unsafe.Pointer(&ret.buildRowStart)) = buildRowStart
	return ret
}

type posAndHashValue struct {
	hashValue uint64
	pos       int
}

type baseJoinProbe struct {
	ctx    *HashJoinCtxV2
	workID uint

	currentChunk *chunk.Chunk
	// if currentChunk.Sel() == nil, then construct a fake selRows
	selRows  []int
	usedRows []int
	// matchedRowsHeaders, serializedKeys is indexed by logical row index
	matchedRowsHeaders []uintptr // the start address of each matched rows
	serializedKeys     [][]byte  // used for save serialized keys
	// filterVector and nullKeyVector is indexed by physical row index because the return vector of VectorizedFilter is based on physical row index
	filterVector                  []bool              // if there is filter before probe, filterVector saves the filter result
	nullKeyVector                 []bool              // nullKeyVector[i] = true if any of the key is null
	hashValues                    [][]posAndHashValue // the start address of each matched rows
	currentProbeRow               int
	matchedRowsForCurrentProbeRow int
	chunkRows                     int
	cachedBuildRows               []*matchedRowInfo

	keyIndex         []int
	keyTypes         []*types.FieldType
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
	rowIndexInfos []*matchedRowInfo
	selected      []bool

	probeCollision uint64
}

func (j *baseJoinProbe) GetProbeCollision() uint64 {
	return j.probeCollision
}

func (j *baseJoinProbe) ResetProbeCollision() {
	j.probeCollision = 0
}

func (j *baseJoinProbe) IsCurrentChunkProbeDone() bool {
	return j.currentChunk == nil || j.currentProbeRow >= j.chunkRows
}

func (j *baseJoinProbe) finishCurrentLookupLoop(joinedChk *chunk.Chunk) {
	if len(j.cachedBuildRows) > 0 {
		j.batchConstructBuildRows(joinedChk, 0, j.ctx.hasOtherCondition())
	}
	j.finishLookupCurrentProbeRow()
	j.appendProbeRowToChunk(joinedChk, j.currentChunk)
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
			j.selRows = j.selRows[:logicalRows]
		} else {
			j.selRows = make([]int, 0, logicalRows)
			for i := 0; i < logicalRows; i++ {
				j.selRows = append(j.selRows, i)
			}
		}
		j.usedRows = j.selRows
	}
	j.chunkRows = logicalRows
	if cap(j.matchedRowsHeaders) >= logicalRows {
		j.matchedRowsHeaders = j.matchedRowsHeaders[:logicalRows]
	} else {
		j.matchedRowsHeaders = make([]uintptr, logicalRows)
	}
	for i := 0; i < int(j.ctx.partitionNumber); i++ {
		j.hashValues[i] = j.hashValues[i][:0]
	}
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
			j.nullKeyVector[i] = false
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
		j.filterVector, err = expression.VectorizedFilter(j.ctx.SessCtx.GetExprCtx().GetEvalCtx(), j.ctx.SessCtx.GetSessionVars().EnableVectorizedExpression, j.ctx.ProbeFilter, chunk.NewIterator4Chunk(j.currentChunk), j.filterVector)
		if err != nil {
			return err
		}
	}

	// generate serialized key
	for i, index := range j.keyIndex {
		err = codec.SerializeKeys(j.ctx.SessCtx.GetSessionVars().StmtCtx.TypeCtx(), j.currentChunk, j.keyTypes[i], index, j.usedRows, j.filterVector, j.nullKeyVector, j.ctx.hashTableMeta.serializeModes[i], j.serializedKeys)
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
		partIndex := hashValue >> j.ctx.partitionMaskOffset
		j.hashValues[partIndex] = append(j.hashValues[partIndex], posAndHashValue{hashValue: hashValue, pos: logicalRowIndex})
	}
	j.currentProbeRow = 0
	for i := 0; i < int(j.ctx.partitionNumber); i++ {
		for index := range j.hashValues[i] {
			j.matchedRowsHeaders[j.hashValues[i][index].pos] = j.ctx.hashTableContext.hashTable.tables[i].lookup(j.hashValues[i][index].hashValue)
		}
	}
	return
}

func (j *baseJoinProbe) finishLookupCurrentProbeRow() {
	if j.matchedRowsForCurrentProbeRow > 0 {
		j.offsetAndLengthArray = append(j.offsetAndLengthArray, offsetAndLength{offset: j.usedRows[j.currentProbeRow], length: j.matchedRowsForCurrentProbeRow})
	}
	j.matchedRowsForCurrentProbeRow = 0
}

func checkSQLKiller(killer *sqlkiller.SQLKiller, fpName string) error {
	err := killer.HandleSignal()
	failpoint.Inject(fpName, func(val failpoint.Value) {
		if val.(bool) {
			err = exeerrors.ErrQueryInterrupted
		}
	})
	return err
}

func (j *baseJoinProbe) appendBuildRowToCachedBuildRowsAndConstructBuildRowsIfNeeded(buildRow *matchedRowInfo, chk *chunk.Chunk, currentColumnIndexInRow int, forOtherCondition bool) {
	j.cachedBuildRows = append(j.cachedBuildRows, buildRow)
	if len(j.cachedBuildRows) >= batchBuildRowSize {
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
	j.matchedRowsForCurrentProbeRow = 0
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
	chkRows := chk.NumRows()
	needUpdateVirtualRow := currentColumnInRow == 0
	if len(usedCols) == 0 || len(j.cachedBuildRows) == 0 {
		if needUpdateVirtualRow {
			chk.SetNumVirtualRows(chkRows + len(j.cachedBuildRows))
		}
		return
	}
	for i := 0; i < len(j.cachedBuildRows); i++ {
		if j.cachedBuildRows[i].buildRowOffset == 0 {
			j.ctx.hashTableMeta.advanceToRowData(j.cachedBuildRows[i])
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
		indexInDstChk, ok := colIndexMap[meta.rowColumnsOrder[columnIndex]]
		var currentColumn *chunk.Column
		if ok {
			currentColumn = chk.Column(indexInDstChk)
			// Other goroutine will use `atomic.StoreUint32` to write to the first 32 bit in nullmap when it need to set usedFlag
			// so read from nullMap may meet concurrent write if meta.colOffsetInNullMap == 1 && (columnIndex + meta.colOffsetInNullMap <= 32)
			mayConcurrentWrite := meta.colOffsetInNullMap == 1 && columnIndex <= 31
			if !mayConcurrentWrite {
				for index := range j.cachedBuildRows {
					currentColumn.AppendNullBitmap(!meta.isColumnNull(*(*unsafe.Pointer)(unsafe.Pointer(&j.cachedBuildRows[index].buildRowStart)), columnIndex))
					j.cachedBuildRows[index].buildRowOffset = chunk.AppendCellFromRawData(currentColumn, *(*unsafe.Pointer)(unsafe.Pointer(&j.cachedBuildRows[index].buildRowStart)), j.cachedBuildRows[index].buildRowOffset)
				}
			} else {
				for index := range j.cachedBuildRows {
					currentColumn.AppendNullBitmap(!meta.isColumnNullThreadSafe(*(*unsafe.Pointer)(unsafe.Pointer(&j.cachedBuildRows[index].buildRowStart)), columnIndex))
					j.cachedBuildRows[index].buildRowOffset = chunk.AppendCellFromRawData(currentColumn, *(*unsafe.Pointer)(unsafe.Pointer(&j.cachedBuildRows[index].buildRowStart)), j.cachedBuildRows[index].buildRowOffset)
				}
			}
		} else {
			// not used so don't need to insert into chk, but still need to advance rowData
			if meta.columnsSize[columnIndex] < 0 {
				for index := range j.cachedBuildRows {
					size := *(*uint64)(unsafe.Add(*(*unsafe.Pointer)(unsafe.Pointer(&j.cachedBuildRows[index].buildRowStart)), j.cachedBuildRows[index].buildRowOffset))
					j.cachedBuildRows[index].buildRowOffset += sizeOfLengthField + int(size)
				}
			} else {
				for index := range j.cachedBuildRows {
					j.cachedBuildRows[index].buildRowOffset += meta.columnsSize[columnIndex]
				}
			}
		}
	}
	if needUpdateVirtualRow {
		chk.SetNumVirtualRows(chkRows + len(j.cachedBuildRows))
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
					dstCol.AppendCellNTimes(srcCol, offsetAndLength.offset, offsetAndLength.length)
				}
				usedColumnMap[colIndex] = struct{}{}
			}
		}
	} else {
		for index, colIndex := range used {
			srcCol := probeChk.Column(colIndex)
			dstCol := chk.Column(index + collOffset)
			for _, offsetAndLength := range j.offsetAndLengthArray {
				dstCol.AppendCellNTimes(srcCol, offsetAndLength.offset, offsetAndLength.length)
			}
		}
	}
}

func (j *baseJoinProbe) buildResultAfterOtherCondition(chk *chunk.Chunk, joinedChk *chunk.Chunk) (err error) {
	// construct the return chunk based on joinedChk and selected, there are 3 kinds of columns
	// 1. columns already in joinedChk
	// 2. columns from build side, but not in joinedChk
	// 3. columns from probe side, but not in joinedChk
	rowCount := chk.NumRows()
	probeUsedColumns, probeColOffset, probeColOffsetInJoinedChk := j.lUsed, 0, 0
	if !j.rightAsBuildSide {
		probeUsedColumns, probeColOffset, probeColOffsetInJoinedChk = j.rUsed, len(j.lUsed), j.ctx.hashTableMeta.totalColumnNumber
	}

	for index, colIndex := range probeUsedColumns {
		dstCol := chk.Column(index + probeColOffset)
		if joinedChk.Column(colIndex+probeColOffsetInJoinedChk).Rows() > 0 {
			// probe column that is already in joinedChk
			srcCol := joinedChk.Column(colIndex + probeColOffsetInJoinedChk)
			chunk.CopySelectedRows(dstCol, srcCol, j.selected)
		} else {
			// probe column that is not in joinedChk
			srcCol := j.currentChunk.Column(colIndex)
			chunk.CopySelectedRowsWithRowIDFunc(dstCol, srcCol, j.selected, 0, len(j.selected), func(i int) int {
				return j.usedRows[j.rowIndexInfos[i].probeRowIndex]
			})
		}
	}
	buildUsedColumns, buildColOffset, buildColOffsetInJoinedChk := j.lUsed, 0, 0
	if j.rightAsBuildSide {
		buildUsedColumns, buildColOffset, buildColOffsetInJoinedChk = j.rUsed, len(j.lUsed), j.currentChunk.NumCols()
	}
	hasRemainCols := false
	for index, colIndex := range buildUsedColumns {
		dstCol := chk.Column(index + buildColOffset)
		srcCol := joinedChk.Column(colIndex + buildColOffsetInJoinedChk)
		if srcCol.Rows() > 0 {
			// build column that is already in joinedChk
			chunk.CopySelectedRows(dstCol, srcCol, j.selected)
		} else {
			hasRemainCols = true
		}
	}
	if hasRemainCols {
		j.cachedBuildRows = j.cachedBuildRows[:0]
		// build column that is not in joinedChk
		for index, result := range j.selected {
			if result {
				j.appendBuildRowToCachedBuildRowsAndConstructBuildRowsIfNeeded(j.rowIndexInfos[index], chk, j.ctx.hashTableMeta.columnCountNeededForOtherCondition, false)
			}
		}
		if len(j.cachedBuildRows) > 0 {
			j.batchConstructBuildRows(chk, j.ctx.hashTableMeta.columnCountNeededForOtherCondition, false)
		}
	}
	rowsAdded := 0
	for _, result := range j.selected {
		if result {
			rowsAdded++
		}
	}
	chk.SetNumVirtualRows(rowCount + rowsAdded)
	return
}

func isKeyMatched(keyMode keyMode, serializedKey []byte, rowStart unsafe.Pointer, meta *TableMeta) bool {
	switch keyMode {
	case OneInt64:
		return *(*int64)(unsafe.Pointer(&serializedKey[0])) == *(*int64)(unsafe.Add(rowStart, meta.nullMapLength+sizeOfNextPtr))
	case FixedSerializedKey:
		return bytes.Equal(serializedKey, hack.GetBytesFromPtr(unsafe.Add(rowStart, meta.nullMapLength+sizeOfNextPtr), meta.joinKeysLength))
	case VariableSerializedKey:
		return bytes.Equal(serializedKey, hack.GetBytesFromPtr(unsafe.Add(rowStart, meta.nullMapLength+sizeOfNextPtr+sizeOfLengthField), int(meta.getSerializedKeyLength(rowStart))))
	default:
		panic("unknown key match type")
	}
}

// NewJoinProbe create a join probe used for hash join v2
func NewJoinProbe(ctx *HashJoinCtxV2, workID uint, joinType logicalop.JoinType, keyIndex []int, joinedColumnTypes, probeKeyTypes []*types.FieldType, rightAsBuildSide bool) ProbeV2 {
	base := baseJoinProbe{
		ctx:                   ctx,
		workID:                workID,
		keyIndex:              keyIndex,
		keyTypes:              probeKeyTypes,
		maxChunkSize:          ctx.SessCtx.GetSessionVars().MaxChunkSize,
		lUsed:                 ctx.LUsed,
		rUsed:                 ctx.RUsed,
		lUsedInOtherCondition: ctx.LUsedInOtherCondition,
		rUsedInOtherCondition: ctx.RUsedInOtherCondition,
		rightAsBuildSide:      rightAsBuildSide,
	}
	for i := range keyIndex {
		if !mysql.HasNotNullFlag(base.keyTypes[i].GetFlag()) {
			base.hasNullableKey = true
		}
	}
	base.cachedBuildRows = make([]*matchedRowInfo, 0, batchBuildRowSize)
	base.matchedRowsHeaders = make([]uintptr, 0, chunk.InitialCapacity)
	base.selRows = make([]int, 0, chunk.InitialCapacity)
	for i := 0; i < chunk.InitialCapacity; i++ {
		base.selRows = append(base.selRows, i)
	}
	base.hashValues = make([][]posAndHashValue, ctx.partitionNumber)
	for i := 0; i < int(ctx.partitionNumber); i++ {
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
		base.rowIndexInfos = make([]*matchedRowInfo, 0, chunk.InitialCapacity)
	}
	switch joinType {
	case logicalop.InnerJoin:
		return &innerJoinProbe{base}
	case logicalop.LeftOuterJoin:
		return newOuterJoinProbe(base, !rightAsBuildSide, rightAsBuildSide)
	case logicalop.RightOuterJoin:
		return newOuterJoinProbe(base, rightAsBuildSide, rightAsBuildSide)
	default:
		panic("unsupported join type")
	}
}

type mockJoinProbe struct {
	baseJoinProbe
}

func (*mockJoinProbe) SetChunkForProbe(*chunk.Chunk) error {
	return errors.New("not supported")
}

func (*mockJoinProbe) Probe(*hashjoinWorkerResult, *sqlkiller.SQLKiller) (ok bool, result *hashjoinWorkerResult) {
	panic("not supported")
}

func (*mockJoinProbe) ScanRowTable(*hashjoinWorkerResult, *sqlkiller.SQLKiller) (result *hashjoinWorkerResult) {
	panic("not supported")
}

func (*mockJoinProbe) IsScanRowTableDone() bool {
	panic("not supported")
}

func (*mockJoinProbe) NeedScanRowTable() bool {
	panic("not supported")
}

func (*mockJoinProbe) InitForScanRowTable() {
	panic("not supported")
}

// used for test
func newMockJoinProbe(ctx *HashJoinCtxV2) *mockJoinProbe {
	base := baseJoinProbe{
		ctx:                   ctx,
		lUsed:                 ctx.LUsed,
		rUsed:                 ctx.RUsed,
		lUsedInOtherCondition: ctx.LUsedInOtherCondition,
		rUsedInOtherCondition: ctx.RUsedInOtherCondition,
		rightAsBuildSide:      false,
	}
	return &mockJoinProbe{base}
}
