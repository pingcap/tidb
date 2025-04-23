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
	"math/rand"
	"testing"

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
	"github.com/stretchr/testify/require"
)

// generate anti semi join result using nested loop
func genAntiSemiJoinResult(t *testing.T, sessCtx sessionctx.Context, leftChunks []*chunk.Chunk, rightChunks []*chunk.Chunk, leftKeyIndex []int, rightKeyIndex []int,
	leftTypes []*types.FieldType, rightTypes []*types.FieldType, leftKeyTypes []*types.FieldType, rightKeyTypes []*types.FieldType, leftUsedColumns []int,
	otherConditions expression.CNFExprs, resultTypes []*types.FieldType) []*chunk.Chunk {
	returnChks := make([]*chunk.Chunk, 0, 1)
	resultChk := chunk.New(resultTypes, sessCtx.GetSessionVars().MaxChunkSize, sessCtx.GetSessionVars().MaxChunkSize)
	shallowRowTypes := make([]*types.FieldType, 0, len(leftTypes)+len(rightTypes))
	shallowRowTypes = append(shallowRowTypes, leftTypes...)
	shallowRowTypes = append(shallowRowTypes, rightTypes...)
	shallowRow := chunk.MutRowFromTypes(shallowRowTypes)

	for _, leftChunk := range leftChunks {
		isResult := true
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
					if !valid {
						continue
					}

					ok, err := codec.EqualChunkRow(sessCtx.GetSessionVars().StmtCtx.TypeCtx(), leftRow, leftKeyTypes, leftKeyIndex,
						rightRow, rightKeyTypes, rightKeyIndex)
					require.NoError(t, err)
					valid = ok
					if valid && otherConditions == nil {
						isResult = false
						break
					}

					if valid && otherConditions != nil {
						// key is match, check other condition
						ok, err := evalOtherCondition(sessCtx, leftRow, rightRow, shallowRow, otherConditions)
						require.NoError(t, err)
						valid = ok
						if valid {
							isResult = false
							break
						}
					}
				}

				if !isResult {
					break
				}
			}

			if isResult {
				appendToResultChk(leftRow, chunk.Row{}, leftUsedColumns, nil, resultChk)
			}
			isResult = true
		}
	}

	if resultChk.NumRows() > 0 {
		returnChks = append(returnChks, resultChk)
	}
	return returnChks
}

func constructInput(col0 *[]any, col1 *[]any, col0Nulls *[]bool, col1Nulls *[]bool, col0V int64, col1V int64, col0Null bool, col1Null bool) {
	*col0 = append(*col0, col0V)
	*col1 = append(*col1, col1V)
	*col0Nulls = append(*col0Nulls, col0Null)
	*col1Nulls = append(*col1Nulls, col1Null)
}

func buildNotInAntiSemiDataSourceAndExpectResult(ctx sessionctx.Context, leftCols []*expression.Column, rightCols []*expression.Column) (*testutil.MockDataSource, *testutil.MockDataSource, []chunk.Row) {
	leftSchema := expression.NewSchema(leftCols...)
	rightSchema := expression.NewSchema(rightCols...)

	rowNum := int64(10000)
	leftCol0Datums := make([]any, 0, rowNum)
	leftCol1Datums := make([]any, 0, rowNum)
	rightCol0Datums := make([]any, 0, rowNum)
	rightCol1Datums := make([]any, 0, rowNum)
	leftCol0Nulls := make([]bool, 0, rowNum)
	leftCol1Nulls := make([]bool, 0, rowNum)
	rightCol0Nulls := make([]bool, 0, rowNum)
	rightCol1Nulls := make([]bool, 0, rowNum)

	intTp := types.NewFieldType(mysql.TypeLonglong)
	expectResultChunk := chunk.NewChunkWithCapacity([]*types.FieldType{intTp, intTp}, 10000)

	// Fill null keys
	leftCol0Datums = append(leftCol0Datums, int64(0))
	leftCol1Datums = append(leftCol1Datums, int64(0))
	leftCol0Nulls = append(leftCol0Nulls, true)
	leftCol1Nulls = append(leftCol1Nulls, true)
	expectResultChunk.AppendNull(0)
	expectResultChunk.AppendNull(1)

	rightCol0Datums = append(rightCol0Datums, int64(0))
	rightCol1Datums = append(rightCol1Datums, int64(0))
	rightCol0Nulls = append(rightCol0Nulls, true)
	rightCol1Nulls = append(rightCol1Nulls, true)

	for i := 1; i < 5; i++ {
		constructInput(&leftCol0Datums, &leftCol1Datums, &leftCol0Nulls, &leftCol1Nulls, 0, int64(i), true, false)
		expectResultChunk.AppendNull(0)
		expectResultChunk.AppendInt64(1, int64(i))

		constructInput(&rightCol0Datums, &rightCol1Datums, &rightCol0Nulls, &rightCol1Nulls, 0, int64(i), true, false)
	}

	distinctKeyNum := 100
	for i := range distinctKeyNum {
		leftSingleKeyNum := rand.Intn(maxChunkSizeInTest) + 1

		if i%2 == 0 {
			for range leftSingleKeyNum {
				col1V := rand.Int63n(100)
				isCol1Null := rand.Intn(10) < 3
				constructInput(&leftCol0Datums, &leftCol1Datums, &leftCol0Nulls, &leftCol1Nulls, int64(i), col1V, false, isCol1Null)
				expectResultChunk.AppendInt64(0, int64(i))
				if isCol1Null {
					expectResultChunk.AppendNull(1)
				} else {
					expectResultChunk.AppendInt64(1, col1V)
				}
			}
		} else {
			rightCol1HasNull := false
			rightSingleKeyNum := rand.Intn(maxChunkSizeInTest) + 1

			for j := range rightSingleKeyNum {
				isNull := rand.Intn(100) < 0
				if isNull {
					rightCol1HasNull = true
				}

				constructInput(&rightCol0Datums, &rightCol1Datums, &rightCol0Nulls, &rightCol1Nulls, int64(i), int64(j), false, isNull)
			}

			for j := range leftSingleKeyNum {
				if rightCol1HasNull {
					isCol1Null := rand.Intn(10) < 2
					constructInput(&leftCol0Datums, &leftCol1Datums, &leftCol0Nulls, &leftCol1Nulls, int64(i), int64(j), false, isCol1Null)
				} else {
					isCol1Null := rand.Intn(10) < 0
					col1V := rand.Int63n(int64(rightSingleKeyNum) * 2)
					constructInput(&leftCol0Datums, &leftCol1Datums, &leftCol0Nulls, &leftCol1Nulls, int64(i), col1V, false, isCol1Null)
					if !isCol1Null && col1V >= int64(rightSingleKeyNum) {
						expectResultChunk.AppendInt64(0, int64(i))
						expectResultChunk.AppendInt64(1, col1V)
					}
				}
			}
		}
	}

	leftLen := len(leftCol0Datums)
	rightLen := len(rightCol0Datums)

	// Shuffle
	for i := int64(0); i < int64(leftLen); i++ {
		j := rand.Int63n(i + 1)
		leftCol0Datums[i], leftCol0Datums[j] = leftCol0Datums[j], leftCol0Datums[i]
		leftCol1Datums[i], leftCol1Datums[j] = leftCol1Datums[j], leftCol1Datums[i]
		leftCol0Nulls[i], leftCol0Nulls[j] = leftCol0Nulls[j], leftCol0Nulls[i]
		leftCol1Nulls[i], leftCol1Nulls[j] = leftCol1Nulls[j], leftCol1Nulls[i]
	}

	for i := int64(0); i < int64(rightLen); i++ {
		j := rand.Int63n(i + 1)
		rightCol0Datums[i], rightCol0Datums[j] = rightCol0Datums[j], rightCol0Datums[i]
		rightCol1Datums[i], rightCol1Datums[j] = rightCol1Datums[j], rightCol1Datums[i]
		rightCol0Nulls[i], rightCol0Nulls[j] = rightCol0Nulls[j], rightCol0Nulls[i]
		rightCol1Nulls[i], rightCol1Nulls[j] = rightCol1Nulls[j], rightCol1Nulls[i]
	}

	expectResult := sortRows([]*chunk.Chunk{expectResultChunk}, semiJoinRetTypes)
	leftMockSrcParm := testutil.MockDataSourceParameters{DataSchema: leftSchema, Ctx: ctx, Rows: leftLen, Ndvs: []int{-2, -2}, Datums: [][]any{leftCol0Datums, leftCol1Datums}, Nulls: [][]bool{leftCol0Nulls, leftCol1Nulls}, HasSel: false}
	rightMockSrcParm := testutil.MockDataSourceParameters{DataSchema: rightSchema, Ctx: ctx, Rows: rightLen, Ndvs: []int{-2, -2}, Datums: [][]any{rightCol0Datums, rightCol1Datums}, Nulls: [][]bool{rightCol0Nulls, rightCol1Nulls}, HasSel: false}
	return testutil.BuildMockDataSource(leftMockSrcParm), testutil.BuildMockDataSource(rightMockSrcParm), expectResult
}

func testAntiSemiJoin(t *testing.T, rightAsBuildSide bool, hasOtherCondition bool, hasDuplicateKey bool) {
	testSemiOrAntiSemiJoin(t, rightAsBuildSide, hasOtherCondition, hasDuplicateKey, true)
}

func testNotInAntiSemi(t *testing.T, rightAsBuildSide bool) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = maxChunkSizeInTest
	ctx.GetSessionVars().MaxChunkSize = maxChunkSizeInTest
	leftDataSource, rightDataSource, expectedResult := buildNotInAntiSemiDataSourceAndExpectResult(ctx, semiJoinleftCols, semiJoinrightCols)

	maxRowTableSegmentSize = 100

	intTp := types.NewFieldType(mysql.TypeLonglong)

	leftKeys := []*expression.Column{
		{Index: 0, RetType: intTp},
	}

	rightKeys := []*expression.Column{
		{Index: 0, RetType: intTp},
	}

	var buildKeys []*expression.Column
	var probeKeys []*expression.Column
	if rightAsBuildSide {
		buildKeys = rightKeys
		probeKeys = leftKeys
	} else {
		buildKeys = leftKeys
		probeKeys = rightKeys
	}

	var otherCondition expression.CNFExprs
	lUsedInOtherCondition := []int{}
	rUsedInOtherCondition := []int{}
	lUsedInOtherCondition = append(lUsedInOtherCondition, 1)
	rUsedInOtherCondition = append(rUsedInOtherCondition, 1)

	tinyTp := types.NewFieldType(mysql.TypeTiny)
	a := &expression.Column{Index: 1, RetType: intTp, InOperand: true}
	b := &expression.Column{Index: 3, RetType: intTp, InOperand: true}
	sf, err := expression.NewFunction(mock.NewContext(), ast.EQ, tinyTp, a, b)
	require.NoError(t, err, "error when create other condition")
	otherCondition = append(otherCondition, sf)

	info := &hashJoinInfo{
		ctx:                   ctx,
		schema:                buildSchema(semiJoinRetTypes),
		leftExec:              leftDataSource,
		rightExec:             rightDataSource,
		joinType:              logicalop.AntiSemiJoin,
		rightAsBuildSide:      rightAsBuildSide,
		buildKeys:             buildKeys,
		probeKeys:             probeKeys,
		lUsed:                 []int{0, 1},
		rUsed:                 []int{},
		otherCondition:        otherCondition,
		lUsedInOtherCondition: lUsedInOtherCondition,
		rUsedInOtherCondition: rUsedInOtherCondition,
	}

	leftDataSource.PrepareChunks()
	rightDataSource.PrepareChunks()

	hashJoinExec := buildHashJoinV2Exec(info)
	result := getSortedResults(t, hashJoinExec, semiJoinRetTypes)
	checkResults(t, semiJoinRetTypes, result, expectedResult)
}

func TestAntiSemiJoinBasic(t *testing.T) {
	testAntiSemiJoin(t, false, false, false) // Left side build without other condition
	testAntiSemiJoin(t, false, true, false)  // Left side build with other condition
	testAntiSemiJoin(t, true, false, false)  // Right side build without other condition
	testAntiSemiJoin(t, true, true, false)   // Right side build with other condition
}

func TestAntiSemiJoinDuplicateKeys(t *testing.T) {
	testAntiSemiJoin(t, false, false, true) // Left side build without other condition
	testAntiSemiJoin(t, false, true, true)  // Left side build with other condition
	testAntiSemiJoin(t, true, false, true)  // Right side build without other condition
	testAntiSemiJoin(t, true, true, true)   // Right side build with other condition
}

// sql: select * from t1 where col1 not in (select col1 from t2 where t1.col0 = t2.col0);
func TestNotInWithAntiSemi(t *testing.T) {
	testNotInAntiSemi(t, true)
	testNotInAntiSemi(t, false)
}

func TestAntiSemiJoinProbeBasic(t *testing.T) {
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
	rTypes = append(rTypes, retTypes...)
	rTypes1 := []*types.FieldType{uintTp, stringTp, intTp, stringTp, tinyTp}
	rTypes1 = append(rTypes1, rTypes1...)

	rightAsBuildSide := []bool{true, false}

	partitionNumber := 4

	joinType := logicalop.AntiSemiJoin

	testCases := []testCase{
		// normal case
		{[]int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, lTypes, rTypes, nil, []int{}, nil, nil, nil},
		// rightUsed is empty
		{[]int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, lTypes, rTypes, []int{0, 1, 2, 3}, []int{}, nil, nil, nil},
		// leftUsed is empty
		{[]int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, lTypes, rTypes, []int{}, []int{}, nil, nil, nil},
		// both left/right Used are empty
		{[]int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, lTypes, rTypes, []int{}, []int{}, nil, nil, nil},
		// both left/right used is part of all columns
		{[]int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, lTypes, rTypes, []int{0, 2}, []int{}, nil, nil, nil},
		// int join uint
		{[]int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{uintTp}, lTypes, rTypes1, []int{0, 1, 2, 3}, []int{}, nil, nil, nil},
		// multiple join keys
		{[]int{0, 1}, []int{0, 1}, []*types.FieldType{intTp, stringTp}, []*types.FieldType{intTp, stringTp}, lTypes, rTypes, []int{0, 1, 2, 3}, []int{}, nil, nil, nil},
	}

	for _, tc := range testCases {
		for _, value := range rightAsBuildSide {
			testJoinProbe(t, false, tc.leftKeyIndex, tc.rightKeyIndex, tc.leftKeyTypes, tc.rightKeyTypes, tc.leftTypes, tc.rightTypes, value, tc.leftUsed,
				tc.rightUsed, tc.leftUsedByOtherCondition, tc.rightUsedByOtherCondition, nil, nil, tc.otherCondition, partitionNumber, joinType, 200)
			testJoinProbe(t, false, tc.leftKeyIndex, tc.rightKeyIndex, toNullableTypes(tc.leftKeyTypes), toNullableTypes(tc.rightKeyTypes),
				toNullableTypes(tc.leftTypes), toNullableTypes(tc.rightTypes), value, tc.leftUsed, tc.rightUsed, tc.leftUsedByOtherCondition, tc.rightUsedByOtherCondition,
				nil, nil, tc.otherCondition, partitionNumber, joinType, 200)
		}
	}
}

func TestAntiSemiJoinProbeAllJoinKeys(t *testing.T) {
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
	lUsed := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}
	rUsed := []int{}
	joinType := logicalop.AntiSemiJoin
	partitionNumber := 4

	rightAsBuildSide := []bool{true, false}

	// single key
	for i := 0; i < len(lTypes); i++ {
		lKeyTypes := []*types.FieldType{lTypes[i]}
		rKeyTypes := []*types.FieldType{rTypes[i]}
		for _, rightAsBuild := range rightAsBuildSide {
			testJoinProbe(t, false, []int{i}, []int{i}, lKeyTypes, rKeyTypes, lTypes, rTypes, rightAsBuild, lUsed, rUsed, nil, nil, nil, nil, nil, partitionNumber, joinType, 100)
			testJoinProbe(t, false, []int{i}, []int{i}, toNullableTypes(lKeyTypes), toNullableTypes(rKeyTypes), toNullableTypes(lTypes), toNullableTypes(rTypes), rightAsBuild, lUsed, rUsed, nil, nil, nil, nil, nil, partitionNumber, joinType, 100)
		}
	}
	// composed key
	// fixed size, inlined
	for _, rightAsBuild := range rightAsBuildSide {
		lKeyTypes := []*types.FieldType{intTp, uintTp}
		rKeyTypes := []*types.FieldType{intTp, uintTp}
		testJoinProbe(t, false, []int{1, 2}, []int{1, 2}, lKeyTypes, rKeyTypes, lTypes, rTypes, rightAsBuild, lUsed, rUsed, nil, nil, nil, nil, nil, partitionNumber, joinType, 100)
		testJoinProbe(t, false, []int{1, 2}, []int{1, 2}, toNullableTypes(lKeyTypes), toNullableTypes(rKeyTypes), toNullableTypes(lTypes), toNullableTypes(rTypes), rightAsBuild, lUsed, rUsed, nil, nil, nil, nil, nil, partitionNumber, joinType, 100)
	}
	// variable size, inlined
	for _, rightAsBuild := range rightAsBuildSide {
		lKeyTypes := []*types.FieldType{intTp, binaryStringTp}
		rKeyTypes := []*types.FieldType{intTp, binaryStringTp}
		testJoinProbe(t, false, []int{1, 17}, []int{1, 17}, lKeyTypes, rKeyTypes, lTypes, rTypes, rightAsBuild, lUsed, rUsed, nil, nil, nil, nil, nil, partitionNumber, joinType, 100)
		testJoinProbe(t, false, []int{1, 17}, []int{1, 17}, toNullableTypes(lKeyTypes), toNullableTypes(rKeyTypes), toNullableTypes(lTypes), toNullableTypes(rTypes), rightAsBuild, lUsed, rUsed, nil, nil, nil, nil, nil, partitionNumber, joinType, 100)
	}
	// fixed size, not inlined
	for _, rightAsBuild := range rightAsBuildSide {
		lKeyTypes := []*types.FieldType{intTp, datetimeTp}
		rKeyTypes := []*types.FieldType{intTp, datetimeTp}
		testJoinProbe(t, false, []int{1, 13}, []int{1, 13}, lKeyTypes, rKeyTypes, lTypes, rTypes, rightAsBuild, lUsed, rUsed, nil, nil, nil, nil, nil, partitionNumber, joinType, 100)
		testJoinProbe(t, false, []int{1, 13}, []int{1, 13}, toNullableTypes(lKeyTypes), toNullableTypes(rKeyTypes), toNullableTypes(lTypes), toNullableTypes(rTypes), rightAsBuild, lUsed, rUsed, nil, nil, nil, nil, nil, partitionNumber, joinType, 100)
	}
	// variable size, not inlined
	for _, rightAsBuild := range rightAsBuildSide {
		lKeyTypes := []*types.FieldType{intTp, decimalTp}
		rKeyTypes := []*types.FieldType{intTp, decimalTp}
		testJoinProbe(t, false, []int{1, 14}, []int{1, 14}, lKeyTypes, rKeyTypes, lTypes, rTypes, rightAsBuild, lUsed, rUsed, nil, nil, nil, nil, nil, partitionNumber, joinType, 100)
		testJoinProbe(t, false, []int{1, 14}, []int{1, 14}, toNullableTypes(lKeyTypes), toNullableTypes(rKeyTypes), toNullableTypes(lTypes), toNullableTypes(rTypes), rightAsBuild, lUsed, rUsed, nil, nil, nil, nil, nil, partitionNumber, joinType, 100)
	}
}

func TestAntiSemiJoinJoinProbeWithSel(t *testing.T) {
	intTp := types.NewFieldType(mysql.TypeLonglong)
	intTp.AddFlag(mysql.NotNullFlag)
	nullableIntTp := types.NewFieldType(mysql.TypeLonglong)
	uintTp := types.NewFieldType(mysql.TypeLonglong)
	uintTp.AddFlag(mysql.NotNullFlag)
	uintTp.AddFlag(mysql.UnsignedFlag)
	nullableUIntTp := types.NewFieldType(mysql.TypeLonglong)
	nullableUIntTp.AddFlag(mysql.UnsignedFlag)
	stringTp := types.NewFieldType(mysql.TypeVarString)
	stringTp.AddFlag(mysql.NotNullFlag)

	lTypes := []*types.FieldType{intTp, intTp, stringTp, uintTp, stringTp}
	rTypes := []*types.FieldType{intTp, intTp, stringTp, uintTp, stringTp}
	rTypes = append(rTypes, rTypes...)

	tinyTp := types.NewFieldType(mysql.TypeTiny)
	a := &expression.Column{Index: 1, RetType: nullableIntTp}
	b := &expression.Column{Index: 8, RetType: nullableUIntTp}
	sf, err := expression.NewFunction(mock.NewContext(), ast.GT, tinyTp, a, b)
	require.NoError(t, err, "error when create other condition")
	otherCondition := make(expression.CNFExprs, 0)
	otherCondition = append(otherCondition, sf)

	joinType := logicalop.AntiSemiJoin

	rightAsBuildSide := []bool{true, false}

	partitionNumber := 4
	rightUsed := []int{}

	for _, rightBuild := range rightAsBuildSide {
		testJoinProbe(t, true, []int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, lTypes, rTypes, rightBuild, []int{1, 2, 4}, rightUsed, []int{1}, []int{3}, nil, nil, otherCondition, partitionNumber, joinType, 500)
		testJoinProbe(t, true, []int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, lTypes, rTypes, rightBuild, []int{}, rightUsed, []int{1}, []int{3}, nil, nil, otherCondition, partitionNumber, joinType, 500)
		testJoinProbe(t, true, []int{0}, []int{0}, []*types.FieldType{nullableIntTp}, []*types.FieldType{nullableIntTp}, toNullableTypes(lTypes), toNullableTypes(rTypes), rightBuild, []int{1, 2, 4}, rightUsed, []int{1}, []int{3}, nil, nil, otherCondition, partitionNumber, joinType, 500)
		testJoinProbe(t, true, []int{0}, []int{0}, []*types.FieldType{nullableIntTp}, []*types.FieldType{nullableIntTp}, toNullableTypes(lTypes), toNullableTypes(rTypes), rightBuild, nil, rightUsed, []int{1}, []int{3}, nil, nil, otherCondition, partitionNumber, joinType, 500)
	}
}
