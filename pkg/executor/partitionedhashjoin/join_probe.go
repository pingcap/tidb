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
	sessCtx        sessionctx.Context
	keyIndex       []int
	columnTypes    []*types.FieldType
	filter         expression.CNFExprs
	otherCondition expression.CNFExprs
	keyIsNullable  bool
	joinHashTable  *joinHashTable
	hashTableMeta  *tableMeta
	typeCtx        types.Context
	keyMode        keyMode
}

func (pCtx *probeCtx) hasOtherCondition() bool {
	return pCtx.otherCondition != nil
}

func (ppi *probeProcessInfo) initForCurrentChunk(ctx *probeCtx) (err error) {
	if ppi.init {
		return
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
		ppi.filterVector, err = expression.VectorizedFilter(ctx.sessCtx, ctx.filter, chunk.NewIterator4Chunk(ppi.chunk), ppi.filterVector)
		if err != nil {
			return err
		}
	}

	// generate serialized key
	for index, keyIndex := range ctx.keyIndex {
		err = codec.SerializeKeys(ctx.typeCtx, ppi.chunk, ctx.columnTypes[keyIndex], keyIndex, ppi.filterVector, ppi.nullKeyVector, ctx.hashTableMeta.ignoreIntegerKeySignFlag[index], ppi.serializedKeys)
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
	return
}

type joinProbe interface {
	probe(chk *chunk.Chunk, info *probeProcessInfo) (err error)
	scanHT(chk *chunk.Chunk) (err error)
	needScanHT() bool
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
	ctx              *probeCtx
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

func (j *baseJoinProbe) prepareForProbe(chk *chunk.Chunk, info *probeProcessInfo) (joinedChk *chunk.Chunk, remainCap int, err error) {
	if !info.init {
		err = info.initForCurrentChunk(j.ctx)
		if err != nil {
			return nil, 0, err
		}
	}
	j.offsetAndLengthArray = j.offsetAndLengthArray[:0]
	if j.ctx.otherCondition != nil {
		j.tmpChk.Reset()
		j.rowIndexInfos = j.rowIndexInfos[:0]
		joinedChk = j.tmpChk
	}
	return joinedChk, chk.RequiredRows() - chk.NumRows(), nil
}

type innerJoinProbe struct {
	baseJoinProbe
}

func (j *baseJoinProbe) appendBuildRowToChunk(chk *chunk.Chunk, rowInfo *rowInfo, info *probeProcessInfo) (currentRowData unsafe.Pointer) {
	meta := j.ctx.hashTableMeta
	if j.rightAsBuildSide {
		if j.ctx.hasOtherCondition() {
			return j.appendBuildRowToChunkInternal(chk, j.rUsedInOtherCondition, rowInfo, meta.columnCountNeededForOtherCondition, info.chunk.NumCols())
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

func (j *innerJoinProbe) probe(chk *chunk.Chunk, info *probeProcessInfo) (err error) {
	if chk.IsFull() {
		return nil
	}
	joinedChk, remainCap, err1 := j.prepareForProbe(chk, info)
	if err1 != nil {
		return err1
	}
	length := 0
	meta := j.ctx.hashTableMeta

	for remainCap > 0 && info.currentProbeRow < info.chunk.NumRows() {
		if info.matchedRowsHeaders[info.currentProbeRow] != nil {
			candidateRow := info.matchedRowsHeaders[info.currentProbeRow]
			if isKeyMatched(j.ctx.keyMode, info.serializedKeys[info.currentProbeRow], candidateRow, meta) {
				// key matched, convert row to column for build side
				rowInfo := &rowInfo{rowStart: candidateRow, rowData: nil, currentColumnIndex: 0}
				currentRowData := j.appendBuildRowToChunk(joinedChk, rowInfo, info)
				if j.ctx.hasOtherCondition() {
					j.rowIndexInfos = append(j.rowIndexInfos, rowIndexInfo{probeRowIndex: info.currentProbeRow, buildRowStart: candidateRow, buildRowData: currentRowData})
				}
				length++
				remainCap--
				joinedChk.IncNumVirtualRows()
			}
			info.matchedRowsHeaders[info.currentProbeRow] = getNextRowAddress(candidateRow)
		} else {
			if length > 0 {
				j.offsetAndLengthArray = append(j.offsetAndLengthArray, offsetAndLength{offset: info.currentProbeRow, length: length})
				length = 0
			}
			info.currentProbeRow++
		}
	}

	if length > 0 {
		j.offsetAndLengthArray = append(j.offsetAndLengthArray, offsetAndLength{offset: info.currentProbeRow, length: length})
	}

	j.appendProbeRowToChunk(joinedChk, info.chunk)

	if j.ctx.hasOtherCondition() && joinedChk.NumRows() > 0 {
		// eval other condition, and construct final chunk
		j.selected = j.selected[:0]
		j.selected, err = expression.VectorizedFilter(j.ctx.sessCtx, j.ctx.otherCondition, chunk.NewIterator4Chunk(joinedChk), j.selected)
		if err != nil {
			return err
		}
		err = j.buildResultAfterOtherCondition(chk, joinedChk, info)
	}
	// if there is no other condition, the joinedChk is the final result
	return err
}

func (j *innerJoinProbe) needScanHT() bool {
	return false
}

func (j *innerJoinProbe) scanHT(*chunk.Chunk) (err error) {
	panic("should not reach here")
}

func (j *innerJoinProbe) buildResultAfterOtherCondition(chk *chunk.Chunk, joinedChk *chunk.Chunk, info *probeProcessInfo) (err error) {
	// construct the return chunk based on joinedChk and selected, there are 3 kinds of columns
	// 1. columns already in joinedChk
	// 2. columns from build side, but not in joinedChk
	// 3. columns from probe side, but not in joinedChk
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
			srcCol := info.chunk.Column(colIndex)
			chunk.CopySelectedRowsWithRowIdFunc(dstCol, srcCol, j.selected, 0, len(j.selected), func(i int) int {
				return j.rowIndexInfos[i].probeRowIndex
			})
		}
	}
	buildUsedColumns, buildColOffset, buildColOffsetInJoinedChk := j.rUsed, len(j.lUsed), info.chunk.NumCols()
	if !j.rightAsBuildSide {
		buildUsedColumns, buildColOffset, buildColOffsetInJoinedChk = j.lUsed, 0, 0
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
		// build column that is not in joinedChk
		for index, result := range j.selected {
			if result {
				j.appendBuildRowToChunk(chk, &rowInfo{
					rowStart:           j.rowIndexInfos[index].buildRowStart,
					rowData:            j.rowIndexInfos[index].buildRowData,
					currentColumnIndex: j.ctx.hashTableMeta.columnCountNeededForOtherCondition,
				}, info)
			}
		}
	}
	return
}

type leftOuterJoinProbe struct {
	baseJoinProbe
	isNotMatchedRows []bool
}

func (j *leftOuterJoinProbe) prepareForProbe(chk *chunk.Chunk, info *probeProcessInfo) (*chunk.Chunk, int, error) {
	if !info.init && j.rightAsBuildSide {
		j.isNotMatchedRows = j.isNotMatchedRows[:0]
		for i := 0; i < info.chunk.NumRows(); i++ {
			j.isNotMatchedRows = append(j.isNotMatchedRows, false)
		}
	}
	joinedChk, remainCap, err := j.baseJoinProbe.prepareForProbe(chk, info)
	if err != nil {
		return nil, 0, err
	}
	return joinedChk, remainCap, nil
}

func (j *leftOuterJoinProbe) needScanHT() bool {
	return !j.rightAsBuildSide
}

func (j *leftOuterJoinProbe) scanHT(chunk2 *chunk.Chunk) (err error) {
	panic("not supported yet")
}

func (j *leftOuterJoinProbe) buildResultForMatchedRowsAfterOtherCondition(chk, joinedChk *chunk.Chunk, info *probeProcessInfo) {
	markedJoined := false
	for index, colIndex := range j.lUsed {
		dstCol := chk.Column(index)
		if joinedChk.Column(colIndex).Rows() > 0 {
			// probe column that is already in joinedChk
			srcCol := joinedChk.Column(colIndex)
			chunk.CopySelectedRows(dstCol, srcCol, j.selected)
		} else {
			markedJoined = true
			srcCol := info.chunk.Column(colIndex)
			chunk.CopySelectedRowsWithRowIdFunc(dstCol, srcCol, j.selected, 0, len(j.selected), func(i int) int {
				ret := j.rowIndexInfos[i].probeRowIndex
				j.isNotMatchedRows[ret] = false
				return ret
			})
		}
	}
	hasRemainCols := false
	for index, colIndex := range j.rUsed {
		dstCol := chk.Column(index + len(j.lUsed))
		srcCol := joinedChk.Column(colIndex + info.chunk.NumCols())
		if srcCol.Rows() > 0 {
			// build column that is already in joinedChk
			chunk.CopySelectedRows(dstCol, srcCol, j.selected)
		} else {
			hasRemainCols = true
		}
	}
	if hasRemainCols {
		markedJoined = true
		for index, result := range j.selected {
			if result {
				rowIndexInfo := j.rowIndexInfos[index]
				j.isNotMatchedRows[rowIndexInfo.probeRowIndex] = true
				j.appendBuildRowToChunk(chk, &rowInfo{
					rowStart:           rowIndexInfo.buildRowStart,
					rowData:            rowIndexInfo.buildRowData,
					currentColumnIndex: j.ctx.hashTableMeta.columnCountNeededForOtherCondition,
				}, info)
			}
		}
	}
	if !markedJoined {
		for index, result := range j.selected {
			if result {
				j.isNotMatchedRows[j.rowIndexInfos[index].probeRowIndex] = true
			}
		}
	}
}

func (j *leftOuterJoinProbe) buildResultForNotMatchedRows(chk *chunk.Chunk, startProbeRow int, info *probeProcessInfo) {
	// append not matched rows
	// for not matched rows, probe col is appended using original cols, and build column is appended using nulls
	prevRows, afterRows := 0, 0
	for index, colIndex := range j.lUsed {
		dstCol := chk.Column(index)
		srcCol := info.chunk.Column(colIndex)
		prevRows = dstCol.Rows()
		chunk.CopyRangeSelectedRows(dstCol, srcCol, startProbeRow, info.currentProbeRow, j.isNotMatchedRows)
		afterRows = dstCol.Rows()
	}
	nullRows := afterRows - prevRows
	if len(j.lUsed) == 0 {
		for i := startProbeRow; i < info.currentProbeRow; i++ {
			if j.isNotMatchedRows[i] == true {
				nullRows++
			}
		}
	}
	if nullRows > 0 {
		colOffset := len(j.lUsed)
		for index := range j.rUsed {
			dstCol := chk.Column(colOffset + index)
			dstCol.AppendNNulls(nullRows)
		}
	}
}

func (j *leftOuterJoinProbe) probeForRightBuild(chk, joinedChk *chunk.Chunk, remainCap int, info *probeProcessInfo) (err error) {
	meta := j.ctx.hashTableMeta
	length := 0
	startProbeRow := info.currentProbeRow

	for remainCap > 0 && info.currentProbeRow < info.chunk.NumRows() {
		if info.matchedRowsHeaders[info.currentProbeRow] != nil {
			// hash value match
			candidateRow := info.matchedRowsHeaders[info.currentProbeRow]
			if isKeyMatched(j.ctx.keyMode, info.serializedKeys[info.currentProbeRow], candidateRow, meta) {
				// join key match
				rowInfo := &rowInfo{rowStart: candidateRow, rowData: nil, currentColumnIndex: 0}
				currentRowData := j.appendBuildRowToChunk(joinedChk, rowInfo, info)
				if j.ctx.hasOtherCondition() {
					j.rowIndexInfos = append(j.rowIndexInfos, rowIndexInfo{probeRowIndex: info.currentProbeRow, buildRowStart: candidateRow, buildRowData: currentRowData})
				} else {
					// has no other condition, key match mean join match
					j.isNotMatchedRows[info.currentProbeRow] = false
				}
				length++
				joinedChk.IncNumVirtualRows()
			}
		} else {
			if length > 0 {
				// length > 0 mean current row has at least one key matched build rows
				j.offsetAndLengthArray = append(j.offsetAndLengthArray, offsetAndLength{offset: info.currentProbeRow, length: length})
				length = 0
			}
			info.currentProbeRow++
		}
		remainCap--
	}
	if length > 0 {
		j.offsetAndLengthArray = append(j.offsetAndLengthArray, offsetAndLength{offset: info.currentProbeRow, length: length})
	}
	j.appendProbeRowToChunk(joinedChk, info.chunk)

	if j.ctx.hasOtherCondition() {
		if joinedChk.NumRows() > 0 {
			j.selected = j.selected[:0]
			j.selected, err = expression.VectorizedFilter(j.ctx.sessCtx, j.ctx.otherCondition, chunk.NewIterator4Chunk(joinedChk), j.selected)
			if err != nil {
				return err
			}
			j.buildResultForMatchedRowsAfterOtherCondition(chk, joinedChk, info)
		}
		// append the not matched rows
		j.buildResultForNotMatchedRows(chk, startProbeRow, info)
	} else {
		// if no the condition, chk == joinedChk, and the matched rows are already in joinedChk
		j.buildResultForNotMatchedRows(joinedChk, startProbeRow, info)
	}
	return
}

func (j *leftOuterJoinProbe) probeForLeftBuild(chk, joinedChk *chunk.Chunk, remainCap int, info *probeProcessInfo) (err error) {
	return
}

func (j *leftOuterJoinProbe) probe(chk *chunk.Chunk, info *probeProcessInfo) (err error) {
	if chk.IsFull() {
		return nil
	}
	joinedChk, remainCap, err1 := j.prepareForProbe(chk, info)
	if err1 != nil {
		return err1
	}
	if j.rightAsBuildSide {
		return j.probeForRightBuild(chk, joinedChk, remainCap, info)
	}
	return j.probeForLeftBuild(chk, joinedChk, remainCap, info)
}
