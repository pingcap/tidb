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
	"sort"
	"strconv"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/internal/testutil"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/stretchr/testify/require"
)

func toNullableTypes(tps []*types.FieldType) []*types.FieldType {
	ret := make([]*types.FieldType, 0, len(tps))
	for _, tp := range tps {
		nullableTp := tp.Clone()
		nullableTp.DelFlag(mysql.NotNullFlag)
		ret = append(ret, nullableTp)
	}
	return ret
}

func evalOtherCondition(sessCtx sessionctx.Context, leftRow chunk.Row, rightRow chunk.Row, shallowRow chunk.MutRow, otherCondition expression.CNFExprs) (bool, error) {
	shallowRow.ShallowCopyPartialRow(0, leftRow)
	shallowRow.ShallowCopyPartialRow(leftRow.Len(), rightRow)
	valid, _, err := expression.EvalBool(sessCtx.GetExprCtx().GetEvalCtx(), otherCondition, shallowRow.ToRow())
	return valid, err
}

func appendToResultChk(leftRow chunk.Row, rightRow chunk.Row, leftUsedColumns []int, rightUsedColumns []int, resultChunk *chunk.Chunk) {
	lWide := 0
	if leftRow.IsEmpty() {
		for index := range leftUsedColumns {
			resultChunk.Column(index).AppendNull()
		}
		resultChunk.SetNumVirtualRows(resultChunk.NumRows() + 1)
		lWide = len(leftUsedColumns)
	} else {
		lWide = resultChunk.AppendRowByColIdxs(leftRow, leftUsedColumns)
	}
	if rightRow.IsEmpty() {
		for index := range rightUsedColumns {
			resultChunk.Column(index + lWide).AppendNull()
		}
	} else {
		resultChunk.AppendPartialRowByColIdxs(lWide, rightRow, rightUsedColumns)
	}
}

func containsNullKey(row chunk.Row, keyIndex []int) bool {
	for _, index := range keyIndex {
		if row.IsNull(index) {
			return true
		}
	}
	return false
}

// generate inner join result using nested loop
func genInnerJoinResult(t *testing.T, sessCtx sessionctx.Context, leftChunks []*chunk.Chunk, rightChunks []*chunk.Chunk, leftKeyIndex []int, rightKeyIndex []int,
	leftTypes []*types.FieldType, rightTypes []*types.FieldType, leftKeyTypes []*types.FieldType, rightKeyTypes []*types.FieldType, leftUsedColumns []int,
	rightUsedColumns []int, otherConditions expression.CNFExprs, resultTypes []*types.FieldType) []*chunk.Chunk {
	returnChks := make([]*chunk.Chunk, 0, 1)
	resultChk := chunk.New(resultTypes, sessCtx.GetSessionVars().MaxChunkSize, sessCtx.GetSessionVars().MaxChunkSize)
	shallowRowTypes := make([]*types.FieldType, 0, len(leftTypes)+len(rightTypes))
	shallowRowTypes = append(shallowRowTypes, leftTypes...)
	shallowRowTypes = append(shallowRowTypes, rightTypes...)
	shallowRow := chunk.MutRowFromTypes(shallowRowTypes)
	// for right out join, use left as build, for other join, always use right as build
	for _, leftChunk := range leftChunks {
		for leftIndex := 0; leftIndex < leftChunk.NumRows(); leftIndex++ {
			leftRow := leftChunk.GetRow(leftIndex)
			for _, rightChunk := range rightChunks {
				for rightIndex := 0; rightIndex < rightChunk.NumRows(); rightIndex++ {
					if resultChk.IsFull() {
						returnChks = append(returnChks, resultChk)
						resultChk = chunk.New(resultTypes, sessCtx.GetSessionVars().MaxChunkSize, sessCtx.GetSessionVars().MaxChunkSize)
					}
					rightRow := rightChunk.GetRow(rightIndex)
					valid := !containsNullKey(leftRow, leftKeyIndex) && !containsNullKey(rightRow, rightKeyIndex)
					if valid {
						ok, err := codec.EqualChunkRow(sessCtx.GetSessionVars().StmtCtx.TypeCtx(), leftRow, leftKeyTypes, leftKeyIndex,
							rightRow, rightKeyTypes, rightKeyIndex)
						require.NoError(t, err)
						valid = ok
					}
					if valid && otherConditions != nil {
						// key is match, check other condition
						ok, err := evalOtherCondition(sessCtx, leftRow, rightRow, shallowRow, otherConditions)
						require.NoError(t, err)
						valid = ok
					}
					if valid {
						// construct result chunk
						appendToResultChk(leftRow, rightRow, leftUsedColumns, rightUsedColumns, resultChk)
					}
				}
			}
		}
	}
	if resultChk.NumRows() > 0 {
		returnChks = append(returnChks, resultChk)
	}
	return returnChks
}

func checkVirtualRows(t *testing.T, resultChunks []*chunk.Chunk) {
	for _, chk := range resultChunks {
		require.Equal(t, false, chk.IsInCompleteChunk())
		numRows := chk.GetNumVirtualRows()
		for i := 0; i < chk.NumCols(); i++ {
			require.Equal(t, numRows, chk.Column(i).Rows())
		}
	}
}

func checkChunksEqual(t *testing.T, expectedChunks []*chunk.Chunk, resultChunks []*chunk.Chunk, schema []*types.FieldType) {
	expectedNum := 0
	resultNum := 0
	for _, chk := range expectedChunks {
		expectedNum += chk.NumRows()
	}
	for _, chk := range resultChunks {
		resultNum += chk.NumRows()
	}
	require.Equal(t, expectedNum, resultNum)
	if expectedNum == 0 || len(schema) == 0 {
		return
	}
	cmpFuncs := make([]chunk.CompareFunc, 0, len(schema))
	for _, colType := range schema {
		cmpFuncs = append(cmpFuncs, chunk.GetCompareFunc(colType))
	}

	resultRows := make([]chunk.Row, 0, expectedNum)
	expectedRows := make([]chunk.Row, 0, expectedNum)
	for _, chk := range expectedChunks {
		iter := chunk.NewIterator4Chunk(chk)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			expectedRows = append(expectedRows, row)
		}
	}
	for _, chk := range resultChunks {
		iter := chunk.NewIterator4Chunk(chk)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			resultRows = append(resultRows, row)
		}
	}
	cmp := func(rowI, rowJ chunk.Row) int {
		for i, cmpFunc := range cmpFuncs {
			cmp := cmpFunc(rowI, i, rowJ, i)
			if cmp != 0 {
				return cmp
			}
		}
		return 0
	}
	sort.Slice(expectedRows, func(i, j int) bool {
		return cmp(expectedRows[i], expectedRows[j]) < 0
	})
	sort.Slice(resultRows, func(i, j int) bool {
		return cmp(resultRows[i], resultRows[j]) < 0
	})

	for i := 0; i < len(expectedRows); i++ {
		x := cmp(expectedRows[i], resultRows[i])
		if x != 0 {
			// used for debug
			x = cmp(expectedRows[i], resultRows[i])
		}
		require.Equal(t, 0, x, "result index = "+strconv.Itoa(i))
	}
}

func testJoinProbe(t *testing.T, withSel bool, leftKeyIndex []int, rightKeyIndex []int, leftKeyTypes []*types.FieldType, rightKeyTypes []*types.FieldType,
	leftTypes []*types.FieldType, rightTypes []*types.FieldType, rightAsBuildSide bool, leftUsed []int, rightUsed []int,
	leftUsedByOtherCondition []int, rightUsedByOtherCondition []int, leftFilter expression.CNFExprs, rightFilter expression.CNFExprs,
	otherCondition expression.CNFExprs, partitionNumber int, joinType logicalop.JoinType, inputRowNumber int) {
	buildKeyIndex, probeKeyIndex := leftKeyIndex, rightKeyIndex
	buildKeyTypes, probeKeyTypes := leftKeyTypes, rightKeyTypes
	buildTypes, probeTypes := leftTypes, rightTypes
	buildUsed := leftUsed
	buildUsedByOtherCondition := leftUsedByOtherCondition
	buildFilter, probeFilter := leftFilter, rightFilter
	needUsedFlag := false
	if rightAsBuildSide {
		probeKeyIndex, buildKeyIndex = leftKeyIndex, rightKeyIndex
		probeKeyTypes, buildKeyTypes = leftKeyTypes, rightKeyTypes
		probeTypes, buildTypes = leftTypes, rightTypes
		buildUsed = rightUsed
		buildUsedByOtherCondition = rightUsedByOtherCondition
		buildFilter, probeFilter = rightFilter, leftFilter
		if joinType == logicalop.RightOuterJoin {
			needUsedFlag = true
		}
	} else {
		switch joinType {
		case logicalop.LeftOuterJoin, logicalop.SemiJoin, logicalop.AntiSemiJoin:
			needUsedFlag = true
		case logicalop.LeftOuterSemiJoin, logicalop.AntiLeftOuterSemiJoin:
			require.NoError(t, errors.New("left semi/anti join does not support use left as build side"))
		}
	}
	switch joinType {
	case logicalop.InnerJoin:
		require.Equal(t, 0, len(leftFilter), "inner join does not support left filter")
		require.Equal(t, 0, len(rightFilter), "inner join does not support right filter")
	case logicalop.LeftOuterJoin:
		require.Equal(t, 0, len(rightFilter), "left outer join does not support right filter")
	case logicalop.RightOuterJoin:
		require.Equal(t, 0, len(leftFilter), "right outer join does not support left filter")
	case logicalop.SemiJoin, logicalop.AntiSemiJoin:
		require.Equal(t, 0, len(leftFilter), "semi/anti join does not support left filter")
		require.Equal(t, 0, len(rightFilter), "semi/anti join does not support right filter")
	case logicalop.LeftOuterSemiJoin, logicalop.AntiLeftOuterSemiJoin:
		require.Equal(t, 0, len(rightFilter), "left outer semi/anti join does not support right filter")
	}
	joinedTypes := make([]*types.FieldType, 0, len(leftTypes)+len(rightTypes))
	joinedTypes = append(joinedTypes, leftTypes...)
	joinedTypes = append(joinedTypes, rightTypes...)
	resultTypes := make([]*types.FieldType, 0, len(leftUsed)+len(rightUsed))
	for _, colIndex := range leftUsed {
		resultTypes = append(resultTypes, leftTypes[colIndex].Clone())
		if joinType == logicalop.RightOuterJoin {
			resultTypes[len(resultTypes)-1].DelFlag(mysql.NotNullFlag)
		}
	}
	for _, colIndex := range rightUsed {
		resultTypes = append(resultTypes, rightTypes[colIndex].Clone())
		if joinType == logicalop.LeftOuterJoin {
			resultTypes[len(resultTypes)-1].DelFlag(mysql.NotNullFlag)
		}
	}

	meta := newTableMeta(buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, buildUsedByOtherCondition, buildUsed, needUsedFlag)
	hashJoinCtx := &HashJoinCtxV2{
		hashTableMeta:         meta,
		BuildFilter:           buildFilter,
		ProbeFilter:           probeFilter,
		OtherCondition:        otherCondition,
		BuildKeyTypes:         buildKeyTypes,
		ProbeKeyTypes:         probeKeyTypes,
		RightAsBuildSide:      rightAsBuildSide,
		LUsed:                 leftUsed,
		RUsed:                 rightUsed,
		LUsedInOtherCondition: leftUsedByOtherCondition,
		RUsedInOtherCondition: rightUsedByOtherCondition,
	}
	hashJoinCtx.SessCtx = mock.NewContext()
	hashJoinCtx.JoinType = joinType
	hashJoinCtx.Concurrency = uint(partitionNumber)
	hashJoinCtx.SetupPartitionInfo()
	// update the partition number
	partitionNumber = int(hashJoinCtx.partitionNumber)
	hashJoinCtx.spillHelper = newHashJoinSpillHelper(nil, partitionNumber, nil)
	hashJoinCtx.initHashTableContext()
	joinProbe := NewJoinProbe(hashJoinCtx, 0, joinType, probeKeyIndex, joinedTypes, probeKeyTypes, rightAsBuildSide)
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
	builder := createRowTableBuilder(buildKeyIndex, buildKeyTypes, hashJoinCtx.partitionNumber, hasNullableKey, buildFilter != nil, joinProbe.NeedScanRowTable())
	chunkNumber := 3
	buildChunks := make([]*chunk.Chunk, 0, chunkNumber)
	probeChunks := make([]*chunk.Chunk, 0, chunkNumber)
	selected := make([]bool, 0, inputRowNumber)
	for i := 0; i < inputRowNumber; i++ {
		if i%3 == 0 {
			selected = append(selected, true)
		} else {
			selected = append(selected, false)
		}
	}
	// check if build column can be inserted to probe column directly
	for i := 0; i < len(buildTypes); i++ {
		buildLength := chunk.GetFixedLen(buildTypes[i])
		probeLength := chunk.GetFixedLen(probeTypes[i])
		require.Equal(t, buildLength, probeLength, "build type and probe type is not compatible")
	}
	for i := 0; i < chunkNumber; i++ {
		buildChunks = append(buildChunks, testutil.GenRandomChunks(buildTypes, inputRowNumber))
		probeChunk := testutil.GenRandomChunks(probeTypes, inputRowNumber*2/3)
		// copy some build data to probe side, to make sure there is some matched rows
		_, err := chunk.CopySelectedJoinRowsDirect(buildChunks[i], selected, probeChunk)
		probeChunks = append(probeChunks, probeChunk)
		require.NoError(t, err)
	}

	if withSel {
		sel := make([]int, 0, inputRowNumber)
		for i := 0; i < inputRowNumber; i++ {
			if i%9 == 0 {
				continue
			}
			sel = append(sel, i)
		}
		for _, chk := range buildChunks {
			chk.SetSel(sel)
		}
		for _, chk := range probeChunks {
			chk.SetSel(sel)
		}
	}

	leftChunks, rightChunks := probeChunks, buildChunks
	if !rightAsBuildSide {
		leftChunks, rightChunks = buildChunks, probeChunks
	}
	for i := 0; i < chunkNumber; i++ {
		err := builder.processOneChunk(buildChunks[i], hashJoinCtx.SessCtx.GetSessionVars().StmtCtx.TypeCtx(), hashJoinCtx, 0)
		require.NoError(t, err)
	}
	builder.appendRemainingRowLocations(0, hashJoinCtx.hashTableContext)
	checkRowLocationAlignment(t, hashJoinCtx.hashTableContext.rowTables[0])
	hashJoinCtx.hashTableContext.mergeRowTablesToHashTable(hashJoinCtx.partitionNumber, nil)
	// build hash table
	for i := 0; i < partitionNumber; i++ {
		hashJoinCtx.hashTableContext.build(&buildTask{partitionIdx: i, segStartIdx: 0, segEndIdx: len(hashJoinCtx.hashTableContext.hashTable.tables[i].rowData.segments)})
	}
	// probe
	resultChunks := make([]*chunk.Chunk, 0)
	joinResult := &hashjoinWorkerResult{
		chk: chunk.New(resultTypes, hashJoinCtx.SessCtx.GetSessionVars().MaxChunkSize, hashJoinCtx.SessCtx.GetSessionVars().MaxChunkSize),
	}
	for _, probeChunk := range probeChunks {
		err := joinProbe.SetChunkForProbe(probeChunk)
		require.NoError(t, err, "unexpected error during SetChunkForProbe")
		for !joinProbe.IsCurrentChunkProbeDone() {
			_, joinResult = joinProbe.Probe(joinResult, &sqlkiller.SQLKiller{})
			require.NoError(t, joinResult.err, "unexpected error during join probe")
			if joinResult.chk.IsFull() {
				resultChunks = append(resultChunks, joinResult.chk)
				joinResult.chk = chunk.New(resultTypes, hashJoinCtx.SessCtx.GetSessionVars().MaxChunkSize, hashJoinCtx.SessCtx.GetSessionVars().MaxChunkSize)
			}
		}
	}

	if joinProbe.NeedScanRowTable() {
		joinProbes := make([]ProbeV2, 0, hashJoinCtx.Concurrency)
		for i := uint(0); i < hashJoinCtx.Concurrency; i++ {
			joinProbes = append(joinProbes, NewJoinProbe(hashJoinCtx, i, joinType, probeKeyIndex, joinedTypes, probeKeyTypes, rightAsBuildSide))
		}
		for _, prober := range joinProbes {
			prober.InitForScanRowTable()
			for !prober.IsScanRowTableDone() {
				joinResult = prober.ScanRowTable(joinResult, &sqlkiller.SQLKiller{})
				require.NoError(t, joinResult.err, "unexpected error during scan row table")
				if joinResult.chk.IsFull() {
					resultChunks = append(resultChunks, joinResult.chk)
					joinResult.chk = chunk.New(resultTypes, hashJoinCtx.SessCtx.GetSessionVars().MaxChunkSize, hashJoinCtx.SessCtx.GetSessionVars().MaxChunkSize)
				}
			}
		}
	}
	if joinResult.chk.NumRows() > 0 {
		resultChunks = append(resultChunks, joinResult.chk)
	}
	checkVirtualRows(t, resultChunks)

	switch joinType {
	case logicalop.InnerJoin:
		expectedChunks := genInnerJoinResult(t, hashJoinCtx.SessCtx, leftChunks, rightChunks, leftKeyIndex, rightKeyIndex, leftTypes, rightTypes,
			leftKeyTypes, rightKeyTypes, leftUsed, rightUsed, otherCondition, resultTypes)
		checkChunksEqual(t, expectedChunks, resultChunks, resultTypes)
	case logicalop.LeftOuterJoin:
		expectedChunks := genLeftOuterJoinResult(t, hashJoinCtx.SessCtx, leftFilter, leftChunks, rightChunks, leftKeyIndex, rightKeyIndex, leftTypes,
			rightTypes, leftKeyTypes, rightKeyTypes, leftUsed, rightUsed, otherCondition, resultTypes)
		checkChunksEqual(t, expectedChunks, resultChunks, resultTypes)
	case logicalop.RightOuterJoin:
		expectedChunks := genRightOuterJoinResult(t, hashJoinCtx.SessCtx, rightFilter, leftChunks, rightChunks, leftKeyIndex, rightKeyIndex, leftTypes,
			rightTypes, leftKeyTypes, rightKeyTypes, leftUsed, rightUsed, otherCondition, resultTypes)
		checkChunksEqual(t, expectedChunks, resultChunks, resultTypes)
	default:
		require.NoError(t, errors.New("not supported join type"))
	}
}

type testCase struct {
	leftKeyIndex              []int
	rightKeyIndex             []int
	leftKeyTypes              []*types.FieldType
	rightKeyTypes             []*types.FieldType
	leftTypes                 []*types.FieldType
	rightTypes                []*types.FieldType
	leftUsed                  []int
	rightUsed                 []int
	otherCondition            expression.CNFExprs
	leftUsedByOtherCondition  []int
	rightUsedByOtherCondition []int
}

func TestInnerJoinProbeBasic(t *testing.T) {
	// todo test nullable type after builder support nullable type
	tinyTp := types.NewFieldType(mysql.TypeTiny)
	tinyTp.AddFlag(mysql.NotNullFlag)
	intTp := types.NewFieldType(mysql.TypeLonglong)
	intTp.AddFlag(mysql.NotNullFlag)
	uintTp := types.NewFieldType(mysql.TypeLonglong)
	uintTp.AddFlag(mysql.NotNullFlag)
	uintTp.AddFlag(mysql.UnsignedFlag)
	stringTp := types.NewFieldType(mysql.TypeVarString)
	stringTp.AddFlag(mysql.NotNullFlag)

	lTypes := []*types.FieldType{intTp, stringTp, uintTp, stringTp, tinyTp}
	rTypes := []*types.FieldType{intTp, stringTp, uintTp, stringTp, tinyTp}
	rTypes1 := []*types.FieldType{uintTp, stringTp, intTp, stringTp, tinyTp}

	rightAsBuildSide := []bool{true, false}
	partitionNumber := 4

	testCases := []testCase{
		// normal case
		{[]int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, lTypes, rTypes, []int{0, 1, 2, 3}, []int{0, 1, 2, 3}, nil, nil, nil},
		// rightUsed is empty
		{[]int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, lTypes, rTypes, []int{0, 1, 2, 3}, []int{}, nil, nil, nil},
		// leftUsed is empty
		{[]int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, lTypes, rTypes, []int{}, []int{0, 1, 2, 3}, nil, nil, nil},
		// both left/right Used are empty
		{[]int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, lTypes, rTypes, []int{}, []int{}, nil, nil, nil},
		// both left/right used is part of all columns
		{[]int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, lTypes, rTypes, []int{0, 2}, []int{1, 3}, nil, nil, nil},
		// int join uint
		{[]int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{uintTp}, lTypes, rTypes1, []int{0, 1, 2, 3}, []int{0, 1, 2, 3}, nil, nil, nil},
		// multiple join keys
		{[]int{0, 1}, []int{0, 1}, []*types.FieldType{intTp, stringTp}, []*types.FieldType{intTp, stringTp}, lTypes, rTypes, []int{0, 1, 2, 3}, []int{0, 1, 2, 3}, nil, nil, nil},
	}

	for _, tc := range testCases {
		// inner join does not have left/right Filter
		for _, rightAsBuild := range rightAsBuildSide {
			testJoinProbe(t, false, tc.leftKeyIndex, tc.rightKeyIndex, tc.leftKeyTypes, tc.rightKeyTypes, tc.leftTypes, tc.rightTypes, rightAsBuild, tc.leftUsed,
				tc.rightUsed, tc.leftUsedByOtherCondition, tc.rightUsedByOtherCondition, nil, nil, tc.otherCondition, partitionNumber, logicalop.InnerJoin, 200)
			testJoinProbe(t, false, tc.leftKeyIndex, tc.rightKeyIndex, toNullableTypes(tc.leftKeyTypes), toNullableTypes(tc.rightKeyTypes),
				toNullableTypes(tc.leftTypes), toNullableTypes(tc.rightTypes), rightAsBuild, tc.leftUsed, tc.rightUsed, tc.leftUsedByOtherCondition, tc.rightUsedByOtherCondition, nil, nil, tc.otherCondition, partitionNumber, logicalop.InnerJoin, 200)
		}
	}
}

func TestInnerJoinProbeAllJoinKeys(t *testing.T) {
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
	binaryStringTp := types.NewFieldType(mysql.TypeBlob)
	binaryStringTp.AddFlag(mysql.NotNullFlag)

	lTypes := []*types.FieldType{tinyTp, intTp, uintTp, yearTp, durationTp, enumTp, enumWithIntFlag, setTp, bitTp, jsonTp, floatTp, doubleTp, stringTp, datetimeTp, decimalTp, timestampTp, dateTp, binaryStringTp}
	rTypes := lTypes
	nullableLTypes := toNullableTypes(lTypes)
	nullableRTypes := toNullableTypes(rTypes)
	lUsed := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}
	rUsed := lUsed
	rightAsBuildSide := []bool{true, false}
	partitionNumber := 4

	// single key
	for i := 0; i < len(lTypes); i++ {
		for _, rightAsBuild := range rightAsBuildSide {
			lKeyTypes := []*types.FieldType{lTypes[i]}
			rKeyTypes := []*types.FieldType{rTypes[i]}
			testJoinProbe(t, false, []int{i}, []int{i}, lKeyTypes, rKeyTypes, lTypes, rTypes, rightAsBuild, lUsed, rUsed, nil, nil, nil, nil, nil, partitionNumber, logicalop.InnerJoin, 100)
			testJoinProbe(t, false, []int{i}, []int{i}, toNullableTypes(lKeyTypes), toNullableTypes(rKeyTypes), nullableLTypes, nullableRTypes, rightAsBuild, lUsed, rUsed, nil, nil, nil, nil, nil, partitionNumber, logicalop.InnerJoin, 100)
		}
	}
	// composed key
	// fixed size, inlined
	for _, rightAsBuild := range rightAsBuildSide {
		lKeyTypes := []*types.FieldType{intTp, uintTp}
		rKeyTypes := []*types.FieldType{intTp, uintTp}
		testJoinProbe(t, false, []int{1, 2}, []int{1, 2}, lKeyTypes, rKeyTypes, lTypes, rTypes, rightAsBuild, lUsed, rUsed, nil, nil, nil, nil, nil, partitionNumber, logicalop.InnerJoin, 100)
		testJoinProbe(t, false, []int{1, 2}, []int{1, 2}, toNullableTypes(lKeyTypes), toNullableTypes(rKeyTypes), nullableLTypes, nullableRTypes, rightAsBuild, lUsed, rUsed, nil, nil, nil, nil, nil, partitionNumber, logicalop.InnerJoin, 100)
	}
	// variable size, inlined
	for _, rightAsBuild := range rightAsBuildSide {
		lKeyTypes := []*types.FieldType{intTp, binaryStringTp}
		rKeyTypes := []*types.FieldType{intTp, binaryStringTp}
		testJoinProbe(t, false, []int{1, 17}, []int{1, 17}, lKeyTypes, rKeyTypes, lTypes, rTypes, rightAsBuild, lUsed, rUsed, nil, nil, nil, nil, nil, partitionNumber, logicalop.InnerJoin, 100)
		testJoinProbe(t, false, []int{1, 17}, []int{1, 17}, toNullableTypes(lKeyTypes), toNullableTypes(rKeyTypes), nullableLTypes, nullableRTypes, rightAsBuild, lUsed, rUsed, nil, nil, nil, nil, nil, partitionNumber, logicalop.InnerJoin, 100)
	}
	// fixed size, not inlined
	for _, rightAsBuild := range rightAsBuildSide {
		lKeyTypes := []*types.FieldType{intTp, datetimeTp}
		rKeyTypes := []*types.FieldType{intTp, datetimeTp}
		testJoinProbe(t, false, []int{1, 13}, []int{1, 13}, lKeyTypes, rKeyTypes, lTypes, rTypes, rightAsBuild, lUsed, rUsed, nil, nil, nil, nil, nil, partitionNumber, logicalop.InnerJoin, 100)
		testJoinProbe(t, false, []int{1, 13}, []int{1, 13}, toNullableTypes(lKeyTypes), toNullableTypes(rKeyTypes), nullableLTypes, nullableRTypes, rightAsBuild, lUsed, rUsed, nil, nil, nil, nil, nil, partitionNumber, logicalop.InnerJoin, 100)
	}
	// variable size, not inlined
	for _, rightAsBuild := range rightAsBuildSide {
		lKeyTypes := []*types.FieldType{intTp, decimalTp}
		rKeyTypes := []*types.FieldType{intTp, decimalTp}
		testJoinProbe(t, false, []int{1, 14}, []int{1, 14}, lKeyTypes, rKeyTypes, lTypes, rTypes, rightAsBuild, lUsed, rUsed, nil, nil, nil, nil, nil, partitionNumber, logicalop.InnerJoin, 100)
		testJoinProbe(t, false, []int{1, 14}, []int{1, 14}, toNullableTypes(lKeyTypes), toNullableTypes(rKeyTypes), nullableLTypes, nullableRTypes, rightAsBuild, lUsed, rUsed, nil, nil, nil, nil, nil, partitionNumber, logicalop.InnerJoin, 100)
	}
}

func TestInnerJoinProbeOtherCondition(t *testing.T) {
	intTp := types.NewFieldType(mysql.TypeLonglong)
	intTp.AddFlag(mysql.NotNullFlag)
	nullableIntTp := types.NewFieldType(mysql.TypeLonglong)
	uintTp := types.NewFieldType(mysql.TypeLonglong)
	uintTp.AddFlag(mysql.NotNullFlag)
	uintTp.AddFlag(mysql.UnsignedFlag)
	stringTp := types.NewFieldType(mysql.TypeVarString)
	stringTp.AddFlag(mysql.NotNullFlag)

	lTypes := []*types.FieldType{intTp, intTp, stringTp, uintTp, stringTp}
	rTypes := []*types.FieldType{intTp, intTp, stringTp, uintTp, stringTp}

	tinyTp := types.NewFieldType(mysql.TypeTiny)
	a := &expression.Column{Index: 1, RetType: nullableIntTp}
	b := &expression.Column{Index: 8, RetType: nullableIntTp}
	sf, err := expression.NewFunction(mock.NewContext(), ast.GT, tinyTp, a, b)
	require.NoError(t, err, "error when create other condition")
	otherCondition := make(expression.CNFExprs, 0)
	otherCondition = append(otherCondition, sf)
	rightAsBuildSide := []bool{true, false}
	partitionNumber := 4

	for _, rightAsBuild := range rightAsBuildSide {
		testJoinProbe(t, false, []int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, lTypes, rTypes, rightAsBuild, []int{1, 2, 4}, []int{0}, []int{1}, []int{3}, nil, nil, otherCondition, partitionNumber, logicalop.InnerJoin, 200)
		testJoinProbe(t, false, []int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, lTypes, rTypes, rightAsBuild, []int{}, []int{}, []int{1}, []int{3}, nil, nil, otherCondition, partitionNumber, logicalop.InnerJoin, 200)
		testJoinProbe(t, false, []int{0}, []int{0}, []*types.FieldType{nullableIntTp}, []*types.FieldType{nullableIntTp}, toNullableTypes(lTypes), toNullableTypes(rTypes), rightAsBuild, []int{1, 2, 4}, []int{0}, []int{1}, []int{3}, nil, nil, otherCondition, partitionNumber, logicalop.InnerJoin, 200)
	}
}

func TestInnerJoinProbeWithSel(t *testing.T) {
	intTp := types.NewFieldType(mysql.TypeLonglong)
	intTp.AddFlag(mysql.NotNullFlag)
	nullableIntTp := types.NewFieldType(mysql.TypeLonglong)
	uintTp := types.NewFieldType(mysql.TypeLonglong)
	uintTp.AddFlag(mysql.NotNullFlag)
	uintTp.AddFlag(mysql.UnsignedFlag)
	nullableUIntTp := types.NewFieldType(mysql.TypeLonglong)
	nullableIntTp.AddFlag(mysql.UnsignedFlag)
	stringTp := types.NewFieldType(mysql.TypeVarString)
	stringTp.AddFlag(mysql.NotNullFlag)

	lTypes := []*types.FieldType{intTp, intTp, stringTp, uintTp, stringTp}
	rTypes := []*types.FieldType{intTp, intTp, stringTp, uintTp, stringTp}

	tinyTp := types.NewFieldType(mysql.TypeTiny)
	a := &expression.Column{Index: 1, RetType: nullableIntTp}
	b := &expression.Column{Index: 8, RetType: nullableUIntTp}
	sf, err := expression.NewFunction(mock.NewContext(), ast.GT, tinyTp, a, b)
	require.NoError(t, err, "error when create other condition")
	otherCondition := make(expression.CNFExprs, 0)
	otherCondition = append(otherCondition, sf)
	otherConditions := []expression.CNFExprs{otherCondition, nil}
	partitionNumber := 4

	rightAsBuildSide := []bool{true, false}

	for _, rightAsBuild := range rightAsBuildSide {
		for _, oc := range otherConditions {
			testJoinProbe(t, true, []int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, lTypes, rTypes, rightAsBuild, []int{1, 2, 4}, []int{0}, []int{1}, []int{3}, nil, nil, oc, partitionNumber, logicalop.InnerJoin, 500)
			testJoinProbe(t, true, []int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, lTypes, rTypes, rightAsBuild, []int{}, []int{}, []int{1}, []int{3}, nil, nil, oc, partitionNumber, logicalop.InnerJoin, 500)
			testJoinProbe(t, true, []int{0}, []int{0}, []*types.FieldType{nullableIntTp}, []*types.FieldType{nullableIntTp}, toNullableTypes(lTypes), toNullableTypes(rTypes), rightAsBuild, []int{1, 2, 4}, []int{0}, []int{1}, []int{3}, nil, nil, oc, partitionNumber, logicalop.InnerJoin, 500)
		}
	}
}
