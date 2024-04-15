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
	"strconv"
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func createSimpleFilter() (expression.CNFExprs, error) {
	tinyTp := types.NewFieldType(mysql.TypeTiny)
	intTp := types.NewFieldType(mysql.TypeLonglong)
	a := &expression.Column{Index: 0, RetType: intTp}
	intConstant := int64(10000)
	var d types.Datum
	d.SetMinNotNull()
	d.SetValueWithDefaultCollation(intConstant)
	b := &expression.Constant{RetType: intTp, Value: d}
	sf, err := expression.NewFunction(mock.NewContext(), ast.GT, tinyTp, a, b)
	if err != nil {
		return nil, err
	}
	buildFilter := make(expression.CNFExprs, 0)
	buildFilter = append(buildFilter, sf)
	return buildFilter, nil
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
	builder := createRowTableBuilder(buildKeyIndex, buildSchema, meta, 1, hasNullableKey, buildFilter != nil, keepFilteredRows)
	chk := chunk.GenRandomChunks(buildTypes, 2049)
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
	hashJoinCtx := &PartitionedHashJoinCtx{
		SessCtx:         mock.NewContext(),
		PartitionNumber: 1,
		hashTableMeta:   meta,
		BuildFilter:     buildFilter,
	}
	rowTables := make([]*rowTable, hashJoinCtx.PartitionNumber)
	err := builder.processOneChunk(chk, hashJoinCtx.SessCtx.GetSessionVars().StmtCtx.TypeCtx(), hashJoinCtx, rowTables)
	builder.appendRemainingRowLocations(rowTables)
	require.NoError(t, err, "processOneChunk returns error")
	require.Equal(t, chk.NumRows(), len(builder.usedRows))
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
			rowStart := rowTables[0].getRowStart(validKeyPos)
			require.NotEqual(t, uintptr(0), rowStart, "row start must not be nil, logical index = "+strconv.Itoa(logicalIndex)+", physical index = "+strconv.Itoa(physicalIndex))
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
			rowStart := rowTables[0].getRowStart(rowIndex)
			require.NotEqual(t, uintptr(0), rowStart, "row start must not be nil, logical index = "+strconv.Itoa(logicalIndex)+", physical index = "+strconv.Itoa(physicalIndex))
			require.Equal(t, builder.serializedKeyVectorBuffer[logicalIndex], meta.getKeyBytes(rowStart), "key not match, logical index = "+strconv.Itoa(logicalIndex)+", physical index = "+strconv.Itoa(physicalIndex))
			rowIndex++
		}
		rowStart := rowTables[0].getRowStart(rowIndex)
		require.Equal(t, uintptr(0), rowStart, "row start must be nil at the end of the test")
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
	buildKeyTypes = []*types.FieldType{stringTp, intTp, binaryStringTp}
	probeKeyTypes = []*types.FieldType{stringTp, intTp, binaryStringTp}
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
	buildFilter, err := createSimpleFilter()
	require.NoError(t, err)
	checkKeys(t, true, buildFilter, buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, true)
	checkKeys(t, true, buildFilter, buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, false)
	buildKeyIndex = []int{0}
	buildTypes = []*types.FieldType{notNullIntTp, uintTp, uintTp}
	buildKeyTypes = []*types.FieldType{notNullIntTp}
	probeKeyTypes = []*types.FieldType{notNullIntTp}
	checkKeys(t, true, buildFilter, buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, true)
	checkKeys(t, true, buildFilter, buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, false)
}

func checkColumnResult(t *testing.T, result *chunk.Chunk, expected *chunk.Chunk, ctx *PartitionedHashJoinCtx, forOtherCondition bool) {
	require.Equal(t, expected.NumRows(), result.NumRows())
	meta := ctx.hashTableMeta
	if forOtherCondition {
		for i := 0; i < meta.columnCountNeededForOtherCondition; i++ {
			colIndex := meta.rowColumnsOrder[i]
			resultCol := result.Column(colIndex)
			require.Equal(t, result.NumRows(), resultCol.Rows())
			for j := 0; j < resultCol.Rows(); j++ {
				isNull := expected.GetRow(j).IsNull(colIndex)
				require.Equal(t, isNull, resultCol.IsNull(j), "data not match, col index = "+strconv.Itoa(colIndex)+" row index = "+strconv.Itoa(j))
				if !isNull {
					require.Equal(t, expected.GetRow(j).GetRaw(colIndex), resultCol.GetRaw(j), "data not match, col index = "+strconv.Itoa(colIndex)+" row index = "+strconv.Itoa(j))
				}
			}
		}
	} else {
		for index, orgIndex := range ctx.LUsed {
			resultCol := result.Column(index)
			require.Equal(t, result.NumRows(), resultCol.Rows())
			for j := 0; j < resultCol.Rows(); j++ {
				isNull := expected.GetRow(j).IsNull(orgIndex)
				require.Equal(t, isNull, resultCol.IsNull(j), "data not match, col index = "+strconv.Itoa(orgIndex)+" row index = "+strconv.Itoa(j))
				if !isNull {
					require.Equal(t, expected.GetRow(j).GetRaw(orgIndex), resultCol.GetRaw(j), "data not match, col index = "+strconv.Itoa(orgIndex)+" row index = "+strconv.Itoa(j))
				}
			}
		}
	}
}

func checkColumns(t *testing.T, withSelCol bool, buildFilter expression.CNFExprs, buildKeyIndex []int, buildTypes []*types.FieldType,
	buildKeyTypes []*types.FieldType, probeKeyTypes []*types.FieldType, keepFilteredRows bool, columnsUsedByOtherCondition []int,
	outputColumns []int, needUsedFlag bool) {
	meta := newTableMeta(buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, columnsUsedByOtherCondition, outputColumns, needUsedFlag)
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
	builder := createRowTableBuilder(buildKeyIndex, buildSchema, meta, 1, hasNullableKey, buildFilter != nil, keepFilteredRows)
	chk := chunk.GenRandomChunks(buildTypes, 2049)
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
	hashJoinCtx := &PartitionedHashJoinCtx{
		SessCtx:               mock.NewContext(),
		PartitionNumber:       1,
		hashTableMeta:         meta,
		BuildFilter:           buildFilter,
		LUsedInOtherCondition: columnsUsedByOtherCondition,
		LUsed:                 outputColumns,
	}
	rowTables := make([]*rowTable, hashJoinCtx.PartitionNumber)
	err := builder.processOneChunk(chk, hashJoinCtx.SessCtx.GetSessionVars().StmtCtx.TypeCtx(), hashJoinCtx, rowTables)
	builder.appendRemainingRowLocations(rowTables)
	require.NoError(t, err, "processOneChunk returns error")
	require.Equal(t, chk.NumRows(), len(builder.usedRows))
	mockJoinProber := newMockJoinProbe(hashJoinCtx)
	resultChunk := chunk.NewEmptyChunk(buildTypes)
	resultChunk.SetInCompleteChunk(true)
	// all the selected rows should be converted to row format, even for the rows that contains null key
	if keepFilteredRows {
		require.Equal(t, uint64(len(builder.usedRows)), rowTables[0].rowCount())
		hasOtherConditionColumns := len(columnsUsedByOtherCondition) > 0
		for logicalIndex, physicalIndex := range builder.usedRows {
			rowStart := rowTables[0].getRowStart(logicalIndex)
			require.NotEqual(t, uintptr(0), rowStart, "row start must not be nil, logical index = "+strconv.Itoa(logicalIndex)+", physical index = "+strconv.Itoa(physicalIndex))
			mockJoinProber.appendBuildRowToCachedBuildRowsAndConstructBuildRowsIfNeeded(rowIndexInfo{probeRowIndex: 0, buildRowStart: rowStart}, resultChunk, 0, hasOtherConditionColumns)
		}
		if len(mockJoinProber.cachedBuildRows) > 0 {
			mockJoinProber.batchConstructBuildRows(resultChunk, 0, hasOtherConditionColumns)
		}
		checkColumnResult(t, resultChunk, chk, hashJoinCtx, hasOtherConditionColumns)
		if hasOtherConditionColumns {
			// assume all the column is selected
			mockJoinProber.selected = make([]bool, 0, resultChunk.NumRows())
			for i := 0; i < resultChunk.NumRows(); i++ {
				mockJoinProber.selected = append(mockJoinProber.selected, true)
			}
			// need to append the rest columns
			newResultChk := chunk.NewEmptyChunk(buildTypes)
			newResultChk.SetInCompleteChunk(true)
			err1 := mockJoinProber.buildResultAfterOtherCondition(newResultChk, resultChunk)
			require.NoError(t, err1)
			require.Equal(t, rowTables[0].rowCount(), uint64(newResultChk.NumRows()))
			checkColumnResult(t, newResultChk, chk, hashJoinCtx, false)
		}
	} else {
		// check join key
		rowIndex := 0
		for logicalIndex, physicalIndex := range builder.usedRows {
			if (builder.filterVector != nil && !builder.filterVector[physicalIndex]) || (builder.nullKeyVector != nil && builder.nullKeyVector[physicalIndex]) {
				// filtered row, skip
				continue
			}
			rowStart := rowTables[0].getRowStart(rowIndex)
			require.NotEqual(t, uintptr(0), rowStart, "row start must not be nil, logical index = "+strconv.Itoa(logicalIndex)+", physical index = "+strconv.Itoa(physicalIndex))
			require.Equal(t, builder.serializedKeyVectorBuffer[logicalIndex], meta.getKeyBytes(rowStart), "key not match, logical index = "+strconv.Itoa(logicalIndex)+", physical index = "+strconv.Itoa(physicalIndex))
			rowIndex++
		}
		rowStart := rowTables[0].getRowStart(rowIndex)
		require.Equal(t, uintptr(0), rowStart, "row start must be nil at the end of the test")
	}
}

func TestColumns(t *testing.T) {
	// todo enable nullable type if builder support nullable
	//intTp := types.NewFieldType(mysql.TypeLonglong)
	//uintTp := types.NewFieldType(mysql.TypeLonglong)
	//uintTp.AddFlag(mysql.UnsignedFlag)
	//stringTp := types.NewFieldType(mysql.TypeVarString)
	notNullIntTp := types.NewFieldType(mysql.TypeLonglong)
	notNullIntTp.AddFlag(mysql.NotNullFlag)
	notNullUintTp := types.NewFieldType(mysql.TypeLonglong)
	notNullUintTp.AddFlag(mysql.UnsignedFlag)
	notNullUintTp.AddFlag(mysql.NotNullFlag)
	notNullString := types.NewFieldType(mysql.TypeVarString)
	notNullString.AddFlag(mysql.NotNullFlag)
	notNullBinaryStringTp := types.NewFieldType(mysql.TypeBlob)
	notNullBinaryStringTp.AddFlag(mysql.NotNullFlag)
	buildKeyIndex := []int{0}
	buildTypes := []*types.FieldType{notNullIntTp, notNullIntTp, notNullString, notNullUintTp, notNullBinaryStringTp, notNullIntTp}
	buildKeyTypes := []*types.FieldType{notNullIntTp}
	probeKeyTypes := []*types.FieldType{notNullIntTp}
	columnUsedByOtherCondition := []int{2, 3}
	outputColumns := []int{0, 1, 2, 3, 4, 5}
	checkColumns(t, false, nil, buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, true, columnUsedByOtherCondition, outputColumns, false)
}
