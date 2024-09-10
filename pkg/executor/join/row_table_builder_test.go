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
	"strconv"
	"testing"
	"unsafe"

	"github.com/pingcap/tidb/pkg/executor/internal/testutil"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func createSimpleFilter(t *testing.T) expression.CNFExprs {
	tinyTp := types.NewFieldType(mysql.TypeTiny)
	intTp := types.NewFieldType(mysql.TypeLonglong)
	a := &expression.Column{Index: 0, RetType: intTp}
	intConstant := int64(10000)
	var d types.Datum
	d.SetMinNotNull()
	d.SetValueWithDefaultCollation(intConstant)
	b := &expression.Constant{RetType: intTp, Value: d}
	sf, err := expression.NewFunction(mock.NewContext(), ast.GT, tinyTp, a, b)
	require.NoError(t, err, "error when create simple filter")
	filter := make(expression.CNFExprs, 0)
	filter = append(filter, sf)
	return filter
}

func createImpossibleFilter(t *testing.T) expression.CNFExprs {
	tinyTp := types.NewFieldType(mysql.TypeTiny)
	intTp := types.NewFieldType(mysql.TypeLonglong)
	a := &expression.Column{Index: 0, RetType: intTp}
	intConstant := int64(10000)
	var d types.Datum
	d.SetMinNotNull()
	d.SetValueWithDefaultCollation(intConstant)
	b := &expression.Constant{RetType: intTp, Value: d}
	sf, err := expression.NewFunction(mock.NewContext(), ast.GT, tinyTp, a, b)
	require.NoError(t, err)
	buildFilter := make(expression.CNFExprs, 0)
	buildFilter = append(buildFilter, sf)
	intConstant = int64(5000)
	d.SetMinNotNull()
	d.SetValueWithDefaultCollation(intConstant)
	b = &expression.Constant{RetType: intTp, Value: d}
	sf, err = expression.NewFunction(mock.NewContext(), ast.LT, tinyTp, a, b)
	require.NoError(t, err)
	buildFilter = append(buildFilter, sf)
	return buildFilter
}

func checkRowLocationAlignment(t *testing.T, rowTables []*rowTable) {
	for _, rt := range rowTables {
		if rt == nil {
			continue
		}
		for _, seg := range rt.segments {
			for index := range seg.rowStartOffset {
				rowLocation := seg.getRowPointer(index)
				require.Equal(t, uint64(0), uint64(uintptr(rowLocation))%8, "row location must be 8 byte aligned")
			}
		}
	}
}

func checkKeys(t *testing.T, withSelCol bool, buildFilter expression.CNFExprs, buildKeyIndex []int, buildTypes []*types.FieldType, buildKeyTypes []*types.FieldType, probeKeyTypes []*types.FieldType, keepFilteredRows bool) {
	meta := newTableMeta(buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, nil, []int{1}, false)
	buildSchema := &expression.Schema{}
	for _, tp := range buildTypes {
		buildSchema.Append(&expression.Column{
			RetType: tp,
		})
	}
	hasNullableKey := false
	for _, buildKeyType := range buildKeyTypes {
		if !mysql.HasNotNullFlag(buildKeyType.GetFlag()) {
			hasNullableKey = true
			break
		}
	}
	chk := testutil.GenRandomChunks(buildTypes, 2049)
	if withSelCol {
		sel := make([]int, 0, 2049)
		for i := 0; i < chk.NumRows(); i++ {
			if i%3 == 0 {
				continue
			}
			sel = append(sel, i)
		}
		chk.SetSel(sel)
	}
	hashJoinCtx := &HashJoinCtxV2{
		hashTableMeta: meta,
		BuildFilter:   buildFilter,
	}
	hashJoinCtx.Concurrency = 1
	hashJoinCtx.SetupPartitionInfo()
	hashJoinCtx.initHashTableContext()
	hashJoinCtx.SessCtx = mock.NewContext()
	builder := createRowTableBuilder(buildKeyIndex, buildKeyTypes, hashJoinCtx.partitionNumber, hasNullableKey, buildFilter != nil, keepFilteredRows)
	err := builder.processOneChunk(chk, hashJoinCtx.SessCtx.GetSessionVars().StmtCtx.TypeCtx(), hashJoinCtx, 0)
	require.NoError(t, err, "processOneChunk returns error")
	builder.appendRemainingRowLocations(0, hashJoinCtx.hashTableContext)
	require.Equal(t, chk.NumRows(), len(builder.usedRows))
	rowTables := hashJoinCtx.hashTableContext.rowTables[0]
	checkRowLocationAlignment(t, rowTables)
	// all the selected rows should be converted to row format, even for the rows that contains null key
	if keepFilteredRows {
		require.Equal(t, uint64(len(builder.usedRows)), rowTables[0].rowCount())
		// check both validJoinKeyRow index and join key
		validJoinKeyRowIndex := 0
		for logicalIndex, physicalIndex := range builder.usedRows {
			if (builder.filterVector != nil && !builder.filterVector[physicalIndex]) || (builder.nullKeyVector != nil && builder.nullKeyVector[physicalIndex]) {
				continue
			}
			validKeyPos := rowTables[0].getValidJoinKeyPos(validJoinKeyRowIndex)
			require.Equal(t, logicalIndex, validKeyPos, "valid key pos not match, logical index = "+strconv.Itoa(logicalIndex)+", physical index = "+strconv.Itoa(physicalIndex))
			rowStart := rowTables[0].getRowPointer(validKeyPos)
			require.NotEqual(t, unsafe.Pointer(nil), rowStart, "row start must not be nil, logical index = "+strconv.Itoa(logicalIndex)+", physical index = "+strconv.Itoa(physicalIndex))
			require.Equal(t, builder.serializedKeyVectorBuffer[logicalIndex], meta.getKeyBytes(rowStart), "key not match, logical index = "+strconv.Itoa(logicalIndex)+", physical index = "+strconv.Itoa(physicalIndex))
			validJoinKeyRowIndex++
		}
		validKeyPos := rowTables[0].getValidJoinKeyPos(validJoinKeyRowIndex)
		require.Equal(t, -1, validKeyPos, "validKeyPos must be -1 at the end of test")
	} else {
		// check join key
		rowIndex := 0
		for logicalIndex, physicalIndex := range builder.usedRows {
			if (builder.filterVector != nil && !builder.filterVector[physicalIndex]) || (builder.nullKeyVector != nil && builder.nullKeyVector[physicalIndex]) {
				// filtered row, skip
				continue
			}
			rowStart := rowTables[0].getRowPointer(rowIndex)
			require.NotEqual(t, unsafe.Pointer(nil), rowStart, "row start must not be nil, logical index = "+strconv.Itoa(logicalIndex)+", physical index = "+strconv.Itoa(physicalIndex))
			require.Equal(t, builder.serializedKeyVectorBuffer[logicalIndex], meta.getKeyBytes(rowStart), "key not match, logical index = "+strconv.Itoa(logicalIndex)+", physical index = "+strconv.Itoa(physicalIndex))
			rowIndex++
		}
		rowStart := rowTables[0].getRowPointer(rowIndex)
		require.Equal(t, unsafe.Pointer(nil), rowStart, "row start must be nil at the end of the test")
	}
}

func TestLargeColumn(t *testing.T) {
	intTp := types.NewFieldType(mysql.TypeLonglong)
	stringTp := types.NewFieldType(mysql.TypeVarString)
	buildKeyIndex := []int{0, 1}
	buildKeyTypes := []*types.FieldType{intTp, stringTp}
	buildTypes := []*types.FieldType{intTp, stringTp}
	probeKeyTypes := []*types.FieldType{intTp, stringTp}

	meta := newTableMeta(buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, nil, []int{1}, false)
	buildSchema := &expression.Schema{}
	for _, tp := range buildTypes {
		buildSchema.Append(&expression.Column{
			RetType: tp,
		})
	}
	builder := createRowTableBuilder(buildKeyIndex, buildKeyTypes, 1, true, false, false)
	rows := 2048
	chk := chunk.NewEmptyChunk(buildTypes)
	// each string value is 256k
	stringValue := make([]byte, 1024*256)
	for i := 0; i < rows; i++ {
		// first column is int
		chk.AppendInt64(0, int64(i))
		chk.AppendBytes(1, stringValue)
	}

	hashJoinCtx := &HashJoinCtxV2{
		hashTableMeta: meta,
	}
	hashJoinCtx.Concurrency = 1
	hashJoinCtx.SetupPartitionInfo()
	hashJoinCtx.initHashTableContext()
	hashJoinCtx.SessCtx = mock.NewContext()
	err := builder.processOneChunk(chk, hashJoinCtx.SessCtx.GetSessionVars().StmtCtx.TypeCtx(), hashJoinCtx, 0)
	require.NoError(t, err, "processOneChunk returns error")
	builder.appendRemainingRowLocations(0, hashJoinCtx.hashTableContext)
	require.Equal(t, chk.NumRows(), len(builder.usedRows))
	rowTables := hashJoinCtx.hashTableContext.rowTables[0]
	checkRowLocationAlignment(t, rowTables)
	for _, rowTable := range rowTables {
		for _, seg := range rowTable.segments {
			require.True(t, len(seg.rawData) < maxRowTableSegmentByteSize*2)
			require.True(t, len(seg.hashValues) < maxRowTableSegmentSize)
		}
	}
}

func TestKey(t *testing.T) {
	intTp := types.NewFieldType(mysql.TypeLonglong)
	uintTp := types.NewFieldType(mysql.TypeLonglong)
	uintTp.AddFlag(mysql.UnsignedFlag)
	stringTp := types.NewFieldType(mysql.TypeVarString)
	binaryStringTp := types.NewFieldType(mysql.TypeBlob)
	notNullIntTp := types.NewFieldType(mysql.TypeLonglong)
	notNullIntTp.SetFlag(mysql.NotNullFlag)

	// inlined fixed length keys
	buildKeyIndex := []int{0}
	buildTypes := []*types.FieldType{intTp, uintTp, uintTp}
	buildKeyTypes := []*types.FieldType{intTp}
	probeKeyTypes := []*types.FieldType{intTp}
	checkKeys(t, false, nil, buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, false)
	checkKeys(t, false, nil, buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, true)
	buildKeyIndex = []int{0}
	buildTypes = []*types.FieldType{notNullIntTp, uintTp, uintTp}
	buildKeyTypes = []*types.FieldType{notNullIntTp}
	probeKeyTypes = []*types.FieldType{notNullIntTp}
	checkKeys(t, false, nil, buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, false)
	checkKeys(t, false, nil, buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, true)
	// inlined variable length keys
	buildKeyIndex = []int{0, 1}
	buildTypes = []*types.FieldType{binaryStringTp, intTp, uintTp}
	buildKeyTypes = []*types.FieldType{binaryStringTp, intTp}
	probeKeyTypes = []*types.FieldType{binaryStringTp, intTp}
	checkKeys(t, false, nil, buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, false)
	checkKeys(t, false, nil, buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, true)

	// not inlined fixed length keys
	buildKeyIndex = []int{0}
	buildTypes = []*types.FieldType{intTp, uintTp, uintTp}
	buildKeyTypes = []*types.FieldType{intTp}
	probeKeyTypes = []*types.FieldType{uintTp}
	checkKeys(t, false, nil, buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, false)
	checkKeys(t, false, nil, buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, true)

	// not inlined variable length keys
	buildKeyIndex = []int{0, 1}
	buildTypes = []*types.FieldType{stringTp, intTp, uintTp}
	buildKeyTypes = []*types.FieldType{stringTp, intTp}
	probeKeyTypes = []*types.FieldType{stringTp, intTp}
	checkKeys(t, false, nil, buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, false)
	checkKeys(t, false, nil, buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, true)
	buildKeyIndex = []int{2, 0, 1}
	buildTypes = []*types.FieldType{stringTp, intTp, binaryStringTp, uintTp}
	buildKeyTypes = []*types.FieldType{binaryStringTp, stringTp, intTp}
	probeKeyTypes = []*types.FieldType{binaryStringTp, stringTp, intTp}
	checkKeys(t, false, nil, buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, false)
	checkKeys(t, false, nil, buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, true)

	// keepRows = true
	buildKeyIndex = []int{0}
	buildTypes = []*types.FieldType{intTp, uintTp, uintTp}
	buildKeyTypes = []*types.FieldType{intTp}
	probeKeyTypes = []*types.FieldType{intTp}
	checkKeys(t, false, nil, buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, true)
	// with sel = true
	checkKeys(t, true, nil, buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, true)
	checkKeys(t, true, nil, buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, false)
	// buildFilter != nil
	buildFilter := createSimpleFilter(t)
	checkKeys(t, true, buildFilter, buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, true)
	checkKeys(t, true, buildFilter, buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, false)
	buildKeyIndex = []int{0}
	buildTypes = []*types.FieldType{notNullIntTp, uintTp, uintTp}
	buildKeyTypes = []*types.FieldType{notNullIntTp}
	probeKeyTypes = []*types.FieldType{notNullIntTp}
	checkKeys(t, true, buildFilter, buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, true)
	checkKeys(t, true, buildFilter, buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, false)
}

func checkColumnResult(t *testing.T, builder *rowTableBuilder, keepFilteredRows bool, result *chunk.Chunk, expected *chunk.Chunk, ctx *HashJoinCtxV2, forOtherCondition bool) {
	if keepFilteredRows {
		require.Equal(t, expected.NumRows(), result.NumRows())
	}
	meta := ctx.hashTableMeta
	if forOtherCondition {
		for i := 0; i < meta.columnCountNeededForOtherCondition; i++ {
			colIndex := meta.rowColumnsOrder[i]
			resultCol := result.Column(colIndex)
			require.Equal(t, result.NumRows(), resultCol.Rows())
			resultIndex := 0
			for logicalIndex, physicalIndex := range builder.usedRows {
				if !keepFilteredRows {
					if (builder.filterVector != nil && !builder.filterVector[physicalIndex]) || (builder.nullKeyVector != nil && builder.nullKeyVector[physicalIndex]) {
						// filtered row, skip
						continue
					}
				}
				isNull := expected.GetRow(logicalIndex).IsNull(colIndex)
				require.Equal(t, isNull, resultCol.IsNull(resultIndex), "data not match, col index = "+strconv.Itoa(colIndex)+" row index = "+strconv.Itoa(logicalIndex))
				if !isNull {
					require.Equal(t, expected.GetRow(logicalIndex).GetRaw(colIndex), resultCol.GetRaw(resultIndex), "data not match, col index = "+strconv.Itoa(colIndex)+" row index = "+strconv.Itoa(logicalIndex))
				}
				resultIndex++
			}
			require.Equal(t, resultIndex, result.NumRows())
		}
	} else {
		for index, orgIndex := range ctx.LUsed {
			resultCol := result.Column(index)
			require.Equal(t, result.NumRows(), resultCol.Rows())
			resultIndex := 0
			for logicalIndex, physicalIndex := range builder.usedRows {
				if !keepFilteredRows {
					if (builder.filterVector != nil && !builder.filterVector[physicalIndex]) || (builder.nullKeyVector != nil && builder.nullKeyVector[physicalIndex]) {
						// filtered row, skip
						continue
					}
				}
				isNull := expected.GetRow(logicalIndex).IsNull(orgIndex)
				require.Equal(t, isNull, resultCol.IsNull(resultIndex), "data not match, col index = "+strconv.Itoa(orgIndex)+" row index = "+strconv.Itoa(logicalIndex))
				if !isNull {
					require.Equal(t, expected.GetRow(logicalIndex).GetRaw(orgIndex), resultCol.GetRaw(resultIndex), "data not match, col index = "+strconv.Itoa(orgIndex)+" row index = "+strconv.Itoa(logicalIndex))
				}
				resultIndex++
			}
			require.Equal(t, resultIndex, result.NumRows())
		}
	}
}

func checkColumns(t *testing.T, withSelCol bool, buildFilter expression.CNFExprs, buildKeyIndex []int, buildTypes []*types.FieldType,
	buildKeyTypes []*types.FieldType, probeKeyTypes []*types.FieldType, keepFilteredRows bool, columnsUsedByOtherCondition []int,
	outputColumns []int, needUsedFlag bool) {
	meta := newTableMeta(buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, columnsUsedByOtherCondition, outputColumns, needUsedFlag)
	resultTypes := make([]*types.FieldType, 0, len(outputColumns))
	for _, index := range outputColumns {
		resultTypes = append(resultTypes, buildTypes[index])
	}
	buildSchema := &expression.Schema{}
	for _, tp := range buildTypes {
		buildSchema.Append(&expression.Column{
			RetType: tp,
		})
	}
	hasNullableKey := false
	for _, buildKeyType := range buildKeyTypes {
		if !mysql.HasNotNullFlag(buildKeyType.GetFlag()) {
			hasNullableKey = true
			break
		}
	}
	builder := createRowTableBuilder(buildKeyIndex, buildKeyTypes, 1, hasNullableKey, buildFilter != nil, keepFilteredRows)
	chk := testutil.GenRandomChunks(buildTypes, 2049)
	if withSelCol {
		sel := make([]int, 0, 2049)
		for i := 0; i < chk.NumRows(); i++ {
			if i%3 == 0 {
				continue
			}
			sel = append(sel, i)
		}
		chk.SetSel(sel)
	}
	hashJoinCtx := &HashJoinCtxV2{
		hashTableMeta:         meta,
		BuildFilter:           buildFilter,
		LUsedInOtherCondition: columnsUsedByOtherCondition,
		LUsed:                 outputColumns,
	}
	hashJoinCtx.Concurrency = 1
	hashJoinCtx.SetupPartitionInfo()
	hashJoinCtx.initHashTableContext()
	hashJoinCtx.SessCtx = mock.NewContext()
	err := builder.processOneChunk(chk, hashJoinCtx.SessCtx.GetSessionVars().StmtCtx.TypeCtx(), hashJoinCtx, 0)
	builder.appendRemainingRowLocations(0, hashJoinCtx.hashTableContext)
	require.NoError(t, err, "processOneChunk returns error")
	require.Equal(t, chk.NumRows(), len(builder.usedRows))
	rowTables := hashJoinCtx.hashTableContext.rowTables[0]
	checkRowLocationAlignment(t, rowTables)
	mockJoinProber := newMockJoinProbe(hashJoinCtx)
	resultChunk := chunk.NewEmptyChunk(resultTypes)
	resultChunk.SetInCompleteChunk(true)
	tmpChunk := chunk.NewEmptyChunk(buildTypes)
	tmpChunk.SetInCompleteChunk(true)
	hasOtherConditionColumns := len(columnsUsedByOtherCondition) > 0
	// all the selected rows should be converted to row format, even for the rows that contains null key
	if keepFilteredRows {
		require.Equal(t, uint64(len(builder.usedRows)), rowTables[0].rowCount())
		for logicalIndex, physicalIndex := range builder.usedRows {
			rowStart := rowTables[0].getRowPointer(logicalIndex)
			require.NotEqual(t, unsafe.Pointer(nil), rowStart, "row start must not be nil, logical index = "+strconv.Itoa(logicalIndex)+", physical index = "+strconv.Itoa(physicalIndex))
			if hasOtherConditionColumns {
				mockJoinProber.appendBuildRowToCachedBuildRowsV1(0, rowStart, tmpChunk, 0, hasOtherConditionColumns)
			} else {
				mockJoinProber.appendBuildRowToCachedBuildRowsV1(0, rowStart, resultChunk, 0, hasOtherConditionColumns)
			}
		}
		if mockJoinProber.nextCachedBuildRowIndex > 0 {
			if hasOtherConditionColumns {
				mockJoinProber.batchConstructBuildRows(tmpChunk, 0, hasOtherConditionColumns)
			} else {
				mockJoinProber.batchConstructBuildRows(resultChunk, 0, hasOtherConditionColumns)
			}
		}
		if hasOtherConditionColumns {
			checkColumnResult(t, builder, keepFilteredRows, tmpChunk, chk, hashJoinCtx, hasOtherConditionColumns)
			// assume all the column is selected
			mockJoinProber.selected = make([]bool, 0, tmpChunk.NumRows())
			for i := 0; i < tmpChunk.NumRows(); i++ {
				mockJoinProber.selected = append(mockJoinProber.selected, true)
			}
			// need to append the rest columns
			err1 := mockJoinProber.buildResultAfterOtherCondition(resultChunk, tmpChunk)
			require.NoError(t, err1)
			require.Equal(t, rowTables[0].rowCount(), uint64(resultChunk.NumRows()))
			checkColumnResult(t, builder, keepFilteredRows, resultChunk, chk, hashJoinCtx, false)
		} else {
			checkColumnResult(t, builder, keepFilteredRows, resultChunk, chk, hashJoinCtx, hasOtherConditionColumns)
		}
	} else {
		// check join key
		rowIndex := 0
		for logicalIndex, physicalIndex := range builder.usedRows {
			if (builder.filterVector != nil && !builder.filterVector[physicalIndex]) || (builder.nullKeyVector != nil && builder.nullKeyVector[physicalIndex]) {
				// filtered row, skip
				continue
			}
			rowStart := rowTables[0].getRowPointer(rowIndex)
			require.NotEqual(t, unsafe.Pointer(nil), rowStart, "row start must not be nil, logical index = "+strconv.Itoa(logicalIndex)+", physical index = "+strconv.Itoa(physicalIndex))
			if hasOtherConditionColumns {
				mockJoinProber.appendBuildRowToCachedBuildRowsV1(0, rowStart, tmpChunk, 0, hasOtherConditionColumns)
			} else {
				mockJoinProber.appendBuildRowToCachedBuildRowsV1(0, rowStart, resultChunk, 0, hasOtherConditionColumns)
			}
			rowIndex++
		}
		if mockJoinProber.nextCachedBuildRowIndex > 0 {
			if hasOtherConditionColumns {
				mockJoinProber.batchConstructBuildRows(tmpChunk, 0, hasOtherConditionColumns)
			} else {
				mockJoinProber.batchConstructBuildRows(resultChunk, 0, hasOtherConditionColumns)
			}
		}
		rowStart := rowTables[0].getRowPointer(rowIndex)
		require.Equal(t, unsafe.Pointer(nil), rowStart, "row start must be nil at the end of the test")
		if hasOtherConditionColumns {
			checkColumnResult(t, builder, keepFilteredRows, tmpChunk, chk, hashJoinCtx, hasOtherConditionColumns)
			mockJoinProber.selected = make([]bool, 0, tmpChunk.NumRows())
			for i := 0; i < tmpChunk.NumRows(); i++ {
				mockJoinProber.selected = append(mockJoinProber.selected, true)
			}
			err1 := mockJoinProber.buildResultAfterOtherCondition(resultChunk, tmpChunk)
			require.NoError(t, err1)
			require.Equal(t, rowTables[0].rowCount(), uint64(resultChunk.NumRows()))
			checkColumnResult(t, builder, keepFilteredRows, resultChunk, chk, hashJoinCtx, false)
		} else {
			checkColumnResult(t, builder, keepFilteredRows, resultChunk, chk, hashJoinCtx, hasOtherConditionColumns)
		}
	}
}

func TestColumnsBasic(t *testing.T) {
	notNullIntTp := types.NewFieldType(mysql.TypeLonglong)
	notNullIntTp.AddFlag(mysql.NotNullFlag)
	notNullUintTp := types.NewFieldType(mysql.TypeLonglong)
	notNullUintTp.AddFlag(mysql.UnsignedFlag)
	notNullUintTp.AddFlag(mysql.NotNullFlag)
	notNullString := types.NewFieldType(mysql.TypeVarString)
	notNullString.AddFlag(mysql.NotNullFlag)
	notNullBinaryStringTp := types.NewFieldType(mysql.TypeBlob)
	notNullBinaryStringTp.AddFlag(mysql.NotNullFlag)
	intTp := types.NewFieldType(mysql.TypeLonglong)
	uintTp := types.NewFieldType(mysql.TypeLonglong)
	uintTp.AddFlag(mysql.UnsignedFlag)
	stringTp := types.NewFieldType(mysql.TypeVarString)
	binaryStringTp := types.NewFieldType(mysql.TypeBlob)
	buildKeyIndex := []int{0}
	buildTypes := []*types.FieldType{notNullIntTp, notNullIntTp, notNullString, notNullUintTp, notNullBinaryStringTp, notNullIntTp}
	buildKeyTypes := []*types.FieldType{notNullIntTp}
	probeKeyTypes := []*types.FieldType{notNullIntTp}
	buildTypesNullable := []*types.FieldType{intTp, intTp, stringTp, uintTp, binaryStringTp, intTp}
	buildKeyTypesNullable := []*types.FieldType{intTp}
	probeKeyTypesNullable := []*types.FieldType{intTp}
	columnUsedByOtherConditions := [][]int{{2, 3}, {0, 2}, nil}
	outputColumns := [][]int{{0, 1, 2, 3, 4, 5}, {2, 3, 4, 5, 1, 0}}
	keepFilteredRows := []bool{true, false}
	needUsedFlag := []bool{true, false}
	filter := createSimpleFilter(t)
	filters := []expression.CNFExprs{filter, nil}
	withSelCol := []bool{true, false}
	for _, otherCondition := range columnUsedByOtherConditions {
		for _, allColumns := range outputColumns {
			for _, keep := range keepFilteredRows {
				for _, usedFlag := range needUsedFlag {
					for _, buildFilter := range filters {
						for _, withSel := range withSelCol {
							checkColumns(t, withSel, buildFilter, buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, keep, otherCondition, allColumns, usedFlag)
							checkColumns(t, withSel, buildFilter, buildKeyIndex, buildTypesNullable, buildKeyTypesNullable, probeKeyTypesNullable, keep, otherCondition, allColumns, usedFlag)
						}
					}
				}
			}
		}
	}
}

func TestColumnsAllDataTypes(t *testing.T) {
	tinyTp := types.NewFieldType(mysql.TypeTiny)
	tinyTp.AddFlag(mysql.NotNullFlag)
	intTp := types.NewFieldType(mysql.TypeLonglong)
	intTp.AddFlag(mysql.NotNullFlag)
	uintTp := types.NewFieldType(mysql.TypeLonglong)
	uintTp.AddFlag(mysql.UnsignedFlag)
	uintTp.AddFlag(mysql.NotNullFlag)
	yearTp := types.NewFieldType(mysql.TypeYear)
	yearTp.AddFlag(mysql.NotNullFlag)
	durationTp := types.NewFieldType(mysql.TypeDuration)
	durationTp.AddFlag(mysql.NotNullFlag)
	enumTp := types.NewFieldType(mysql.TypeEnum)
	enumTp.AddFlag(mysql.NotNullFlag)
	enumWithIntFlag := types.NewFieldType(mysql.TypeEnum)
	enumWithIntFlag.AddFlag(mysql.EnumSetAsIntFlag)
	enumWithIntFlag.AddFlag(mysql.NotNullFlag)
	setTp := types.NewFieldType(mysql.TypeSet)
	setTp.AddFlag(mysql.NotNullFlag)
	bitTp := types.NewFieldType(mysql.TypeBit)
	bitTp.AddFlag(mysql.NotNullFlag)
	jsonTp := types.NewFieldType(mysql.TypeJSON)
	jsonTp.AddFlag(mysql.NotNullFlag)
	floatTp := types.NewFieldType(mysql.TypeFloat)
	floatTp.AddFlag(mysql.NotNullFlag)
	doubleTp := types.NewFieldType(mysql.TypeDouble)
	doubleTp.AddFlag(mysql.NotNullFlag)
	stringTp := types.NewFieldType(mysql.TypeVarString)
	stringTp.AddFlag(mysql.NotNullFlag)
	datetimeTp := types.NewFieldType(mysql.TypeDatetime)
	datetimeTp.AddFlag(mysql.NotNullFlag)
	decimalTp := types.NewFieldType(mysql.TypeNewDecimal)
	decimalTp.AddFlag(mysql.NotNullFlag)
	timestampTp := types.NewFieldType(mysql.TypeTimestamp)
	timestampTp.AddFlag(mysql.NotNullFlag)
	dateTp := types.NewFieldType(mysql.TypeDate)
	dateTp.AddFlag(mysql.NotNullFlag)
	buildKeyIndex := []int{0}
	buildTypes := []*types.FieldType{tinyTp, intTp, uintTp, yearTp, durationTp, enumTp, enumWithIntFlag, setTp, bitTp, jsonTp, floatTp, doubleTp, stringTp, datetimeTp, decimalTp, timestampTp, dateTp}
	buildKeyTypes := []*types.FieldType{tinyTp}
	probeKeyTypes := []*types.FieldType{tinyTp}
	nullableBuildTypes := toNullableTypes(buildTypes)
	nullableBuildKeyTypes := toNullableTypes(buildKeyTypes)
	nullableProbeKeyTypes := toNullableTypes(probeKeyTypes)
	outputColumns := make([]int, 0, len(buildTypes))
	for index := range buildTypes {
		outputColumns = append(outputColumns, index)
	}
	keepFilteredRows := []bool{true, false}
	needUsedFlag := []bool{true, false}
	for _, keep := range keepFilteredRows {
		for _, usedFlag := range needUsedFlag {
			checkColumns(t, false, nil, buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, keep, nil, outputColumns, usedFlag)
			checkColumns(t, false, nil, buildKeyIndex, nullableBuildTypes, nullableBuildKeyTypes, nullableProbeKeyTypes, keep, nil, outputColumns, usedFlag)
		}
	}
}

func TestBalanceOfFilteredRows(t *testing.T) {
	intTp := types.NewFieldType(mysql.TypeLonglong)
	stringTp := types.NewFieldType(mysql.TypeVarString)
	binaryStringTp := types.NewFieldType(mysql.TypeBlob)
	buildKeyIndex := []int{0}
	buildTypes := []*types.FieldType{intTp, stringTp, binaryStringTp}
	buildKeyTypes := []*types.FieldType{intTp}
	probeKeyTypes := []*types.FieldType{intTp}
	buildSchema := &expression.Schema{}
	for _, tp := range buildTypes {
		buildSchema.Append(&expression.Column{
			RetType: tp,
		})
	}

	meta := newTableMeta(buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, nil, []int{}, false)
	hasNullableKey := false
	for _, buildKeyType := range buildKeyTypes {
		if !mysql.HasNotNullFlag(buildKeyType.GetFlag()) {
			hasNullableKey = true
			break
		}
	}

	buildFilter := createImpossibleFilter(t)
	chk := testutil.GenRandomChunks(buildTypes, 3000)
	hashJoinCtx := &HashJoinCtxV2{
		hashTableMeta: meta,
		BuildFilter:   buildFilter,
	}
	hashJoinCtx.Concurrency = 4
	hashJoinCtx.SetupPartitionInfo()
	hashJoinCtx.initHashTableContext()
	hashJoinCtx.SessCtx = mock.NewContext()
	builder := createRowTableBuilder(buildKeyIndex, buildKeyTypes, hashJoinCtx.partitionNumber, hasNullableKey, true, true)
	err := builder.processOneChunk(chk, hashJoinCtx.SessCtx.GetSessionVars().StmtCtx.TypeCtx(), hashJoinCtx, 0)
	require.NoError(t, err)
	builder.appendRemainingRowLocations(0, hashJoinCtx.hashTableContext)
	rowTables := hashJoinCtx.hashTableContext.rowTables[0]
	for i := 0; i < int(hashJoinCtx.partitionNumber); i++ {
		require.Equal(t, int(3000/hashJoinCtx.partitionNumber), int(rowTables[i].rowCount()))
	}
}

func TestSetupPartitionInfo(t *testing.T) {
	type testCase struct {
		concurrency         uint
		partitionNumber     uint
		partitionMaskOffset int
	}

	testCases := []testCase{
		{1, 1, 64},
		{2, 2, 63},
		{3, 4, 62},
		{4, 4, 62},
		{5, 8, 61},
		{6, 8, 61},
		{7, 8, 61},
		{8, 8, 61},
		{9, 16, 60},
		{10, 16, 60},
		{11, 16, 60},
		{12, 16, 60},
		{13, 16, 60},
		{14, 16, 60},
		{15, 16, 60},
		{16, 16, 60},
		{17, 16, 60},
		{18, 16, 60},
		{100, 16, 60},
	}

	for _, test := range testCases {
		hashJoinCtx := &HashJoinCtxV2{}
		hashJoinCtx.Concurrency = test.concurrency
		hashJoinCtx.SetupPartitionInfo()
		require.Equal(t, test.partitionNumber, hashJoinCtx.partitionNumber)
		require.Equal(t, test.partitionMaskOffset, hashJoinCtx.partitionMaskOffset)
	}
}
