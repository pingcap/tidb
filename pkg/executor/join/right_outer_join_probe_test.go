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
	"testing"

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

// generate right join result using nested loop
func genRightOuterJoinResult(t *testing.T, sessCtx sessionctx.Context, rightFilter expression.CNFExprs, leftChunks []*chunk.Chunk, rightChunks []*chunk.Chunk, leftKeyIndex []int, rightKeyIndex []int,
	leftTypes []*types.FieldType, rightTypes []*types.FieldType, leftKeyTypes []*types.FieldType, rightKeyTypes []*types.FieldType, leftUsedColumns []int, rightUsedColumns []int,
	otherConditions expression.CNFExprs, resultTypes []*types.FieldType) []*chunk.Chunk {
	filterVector := make([]bool, 0)
	var err error
	returnChks := make([]*chunk.Chunk, 0, 1)
	resultChk := chunk.New(resultTypes, sessCtx.GetSessionVars().MaxChunkSize, sessCtx.GetSessionVars().MaxChunkSize)
	shallowRowTypes := make([]*types.FieldType, 0, len(leftTypes)+len(rightTypes))
	shallowRowTypes = append(shallowRowTypes, leftTypes...)
	shallowRowTypes = append(shallowRowTypes, rightTypes...)
	shallowRow := chunk.MutRowFromTypes(shallowRowTypes)
	emptyRow := chunk.Row{}
	// for right out join, use left as build, for other join, always use right as build
	for _, rightChunk := range rightChunks {
		if rightFilter != nil {
			filterVector, err = expression.VectorizedFilter(sessCtx.GetExprCtx().GetEvalCtx(), sessCtx.GetSessionVars().EnableVectorizedExpression, rightFilter, chunk.NewIterator4Chunk(rightChunk), filterVector)
			require.NoError(t, err)
		}
		for rightIndex := 0; rightIndex < rightChunk.NumRows(); rightIndex++ {
			filterIndex := rightIndex
			if rightChunk.Sel() != nil {
				filterIndex = rightChunk.Sel()[rightIndex]
			}
			if rightFilter != nil && !filterVector[filterIndex] {
				// filtered by left filter, just construct the result row without probe
				appendToResultChk(emptyRow, rightChunk.GetRow(rightIndex), leftUsedColumns, rightUsedColumns, resultChk)
				if resultChk.IsFull() {
					returnChks = append(returnChks, resultChk)
					resultChk = chunk.New(resultTypes, sessCtx.GetSessionVars().MaxChunkSize, sessCtx.GetSessionVars().MaxChunkSize)
				}
				continue
			}
			rightRow := rightChunk.GetRow(rightIndex)
			hasAtLeastOneMatch := false
			for _, leftChunk := range leftChunks {
				for leftIndex := 0; leftIndex < leftChunk.NumRows(); leftIndex++ {
					if resultChk.IsFull() {
						returnChks = append(returnChks, resultChk)
						resultChk = chunk.New(resultTypes, sessCtx.GetSessionVars().MaxChunkSize, sessCtx.GetSessionVars().MaxChunkSize)
					}
					leftRow := leftChunk.GetRow(leftIndex)
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
						hasAtLeastOneMatch = true
						appendToResultChk(leftRow, rightRow, leftUsedColumns, rightUsedColumns, resultChk)
					}
				}
			}
			if !hasAtLeastOneMatch {
				appendToResultChk(emptyRow, rightRow, leftUsedColumns, rightUsedColumns, resultChk)
				if resultChk.IsFull() {
					returnChks = append(returnChks, resultChk)
					resultChk = chunk.New(resultTypes, sessCtx.GetSessionVars().MaxChunkSize, sessCtx.GetSessionVars().MaxChunkSize)
				}
			}
		}
	}
	if resultChk.NumRows() > 0 {
		returnChks = append(returnChks, resultChk)
	}
	return returnChks
}

func TestRightOuterJoinProbeBasic(t *testing.T) {
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
	partitionNumber := 3
	simpleFilter := createSimpleFilter(t)
	hasFilter := []bool{false, true}

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
		for _, value := range rightAsBuildSide {
			for _, testFilter := range hasFilter {
				rightFilter := simpleFilter
				if !testFilter {
					rightFilter = nil
				}
				testJoinProbe(t, false, tc.leftKeyIndex, tc.rightKeyIndex, tc.leftKeyTypes, tc.rightKeyTypes, tc.leftTypes, tc.rightTypes, value, tc.leftUsed,
					tc.rightUsed, tc.leftUsedByOtherCondition, tc.rightUsedByOtherCondition, nil, rightFilter, tc.otherCondition, partitionNumber, logicalop.RightOuterJoin, 200)
				testJoinProbe(t, false, tc.leftKeyIndex, tc.rightKeyIndex, toNullableTypes(tc.leftKeyTypes), toNullableTypes(tc.rightKeyTypes),
					toNullableTypes(tc.leftTypes), toNullableTypes(tc.rightTypes), value, tc.leftUsed, tc.rightUsed, tc.leftUsedByOtherCondition, tc.rightUsedByOtherCondition,
					nil, rightFilter, tc.otherCondition, partitionNumber, logicalop.RightOuterJoin, 200)
			}
		}
	}
}

func TestRightOuterJoinProbeAllJoinKeys(t *testing.T) {
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
	rUsed := lUsed
	joinType := logicalop.RightOuterJoin

	rightAsBuildSide := []bool{true, false}

	// single key
	for i := 0; i < len(lTypes); i++ {
		lKeyTypes := []*types.FieldType{lTypes[i]}
		rKeyTypes := []*types.FieldType{rTypes[i]}
		for _, rightAsBuild := range rightAsBuildSide {
			testJoinProbe(t, false, []int{i}, []int{i}, lKeyTypes, rKeyTypes, lTypes, rTypes, rightAsBuild, lUsed, rUsed, nil, nil, nil, nil, nil, 3, joinType, 100)
			testJoinProbe(t, false, []int{i}, []int{i}, toNullableTypes(lKeyTypes), toNullableTypes(rKeyTypes), toNullableTypes(lTypes), toNullableTypes(rTypes), rightAsBuild, lUsed, rUsed, nil, nil, nil, nil, nil, 3, joinType, 100)
		}
	}
	// composed key
	// fixed size, inlined
	for _, rightAsBuild := range rightAsBuildSide {
		lKeyTypes := []*types.FieldType{intTp, uintTp}
		rKeyTypes := []*types.FieldType{intTp, uintTp}
		testJoinProbe(t, false, []int{1, 2}, []int{1, 2}, lKeyTypes, rKeyTypes, lTypes, rTypes, rightAsBuild, lUsed, rUsed, nil, nil, nil, nil, nil, 3, joinType, 100)
		testJoinProbe(t, false, []int{1, 2}, []int{1, 2}, toNullableTypes(lKeyTypes), toNullableTypes(rKeyTypes), toNullableTypes(lTypes), toNullableTypes(rTypes), rightAsBuild, lUsed, rUsed, nil, nil, nil, nil, nil, 3, joinType, 100)
	}
	// variable size, inlined
	for _, rightAsBuild := range rightAsBuildSide {
		lKeyTypes := []*types.FieldType{intTp, binaryStringTp}
		rKeyTypes := []*types.FieldType{intTp, binaryStringTp}
		testJoinProbe(t, false, []int{1, 17}, []int{1, 17}, lKeyTypes, rKeyTypes, lTypes, rTypes, rightAsBuild, lUsed, rUsed, nil, nil, nil, nil, nil, 3, joinType, 100)
		testJoinProbe(t, false, []int{1, 17}, []int{1, 17}, toNullableTypes(lKeyTypes), toNullableTypes(rKeyTypes), toNullableTypes(lTypes), toNullableTypes(rTypes), rightAsBuild, lUsed, rUsed, nil, nil, nil, nil, nil, 3, joinType, 100)
	}
	// fixed size, not inlined
	for _, rightAsBuild := range rightAsBuildSide {
		lKeyTypes := []*types.FieldType{intTp, datetimeTp}
		rKeyTypes := []*types.FieldType{intTp, datetimeTp}
		testJoinProbe(t, false, []int{1, 13}, []int{1, 13}, lKeyTypes, rKeyTypes, lTypes, rTypes, rightAsBuild, lUsed, rUsed, nil, nil, nil, nil, nil, 3, joinType, 100)
		testJoinProbe(t, false, []int{1, 13}, []int{1, 13}, toNullableTypes(lKeyTypes), toNullableTypes(rKeyTypes), toNullableTypes(lTypes), toNullableTypes(rTypes), rightAsBuild, lUsed, rUsed, nil, nil, nil, nil, nil, 3, joinType, 100)
	}
	// variable size, not inlined
	for _, rightAsBuild := range rightAsBuildSide {
		lKeyTypes := []*types.FieldType{intTp, decimalTp}
		rKeyTypes := []*types.FieldType{intTp, decimalTp}
		testJoinProbe(t, false, []int{1, 14}, []int{1, 14}, lKeyTypes, rKeyTypes, lTypes, rTypes, rightAsBuild, lUsed, rUsed, nil, nil, nil, nil, nil, 3, joinType, 100)
		testJoinProbe(t, false, []int{1, 14}, []int{1, 14}, toNullableTypes(lKeyTypes), toNullableTypes(rKeyTypes), toNullableTypes(lTypes), toNullableTypes(rTypes), rightAsBuild, lUsed, rUsed, nil, nil, nil, nil, nil, 3, joinType, 100)
	}
}

func TestRightOuterJoinProbeOtherCondition(t *testing.T) {
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
	joinType := logicalop.RightOuterJoin
	simpleFilter := createSimpleFilter(t)
	hasFilter := []bool{false, true}
	rightAsBuildSide := []bool{false, true}

	for _, rightBuild := range rightAsBuildSide {
		for _, testFilter := range hasFilter {
			rightFilter := simpleFilter
			if !testFilter {
				rightFilter = nil
			}
			testJoinProbe(t, false, []int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, lTypes, rTypes, rightBuild, []int{1, 2, 4}, []int{0}, []int{1}, []int{3}, nil, rightFilter, otherCondition, 3, joinType, 200)
			testJoinProbe(t, false, []int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, lTypes, rTypes, rightBuild, []int{}, []int{}, []int{1}, []int{3}, nil, rightFilter, otherCondition, 3, joinType, 200)
			testJoinProbe(t, false, []int{0}, []int{0}, []*types.FieldType{nullableIntTp}, []*types.FieldType{nullableIntTp}, toNullableTypes(lTypes), toNullableTypes(rTypes), rightBuild, []int{1, 2, 4}, []int{0}, []int{1}, []int{3}, nil, rightFilter, otherCondition, 3, joinType, 200)
		}
	}
}

func TestRightOuterJoinProbeWithSel(t *testing.T) {
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

	tinyTp := types.NewFieldType(mysql.TypeTiny)
	a := &expression.Column{Index: 1, RetType: nullableIntTp}
	b := &expression.Column{Index: 8, RetType: nullableUIntTp}
	sf, err := expression.NewFunction(mock.NewContext(), ast.GT, tinyTp, a, b)
	require.NoError(t, err, "error when create other condition")
	otherCondition := make(expression.CNFExprs, 0)
	otherCondition = append(otherCondition, sf)
	joinType := logicalop.RightOuterJoin
	rightAsBuildSide := []bool{false, true}
	simpleFilter := createSimpleFilter(t)
	hasFilter := []bool{false, true}

	for _, rightBuild := range rightAsBuildSide {
		for _, useFilter := range hasFilter {
			rightFilter := simpleFilter
			if !useFilter {
				rightFilter = nil
			}
			testJoinProbe(t, true, []int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, lTypes, rTypes, rightBuild, []int{1, 2, 4}, []int{0}, []int{1}, []int{3}, nil, rightFilter, otherCondition, 3, joinType, 500)
			testJoinProbe(t, true, []int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, lTypes, rTypes, rightBuild, []int{}, []int{}, []int{1}, []int{3}, nil, rightFilter, otherCondition, 3, joinType, 500)
			testJoinProbe(t, true, []int{0}, []int{0}, []*types.FieldType{nullableIntTp}, []*types.FieldType{nullableIntTp}, toNullableTypes(lTypes), toNullableTypes(rTypes), rightBuild, []int{1, 2, 4}, []int{0}, []int{1}, []int{3}, nil, rightFilter, otherCondition, 3, joinType, 500)
		}
	}
}
