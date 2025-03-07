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

func genLeftOuterSemiJoinResult(t *testing.T, sessCtx sessionctx.Context, leftFilter expression.CNFExprs, leftChunks []*chunk.Chunk, rightChunks []*chunk.Chunk, leftKeyIndex []int, rightKeyIndex []int,
	leftTypes []*types.FieldType, rightTypes []*types.FieldType, leftKeyTypes []*types.FieldType, rightKeyTypes []*types.FieldType, leftUsedColumns []int, otherConditions expression.CNFExprs,
	resultTypes []*types.FieldType) []*chunk.Chunk {
	return genLeftOuterSemiOrSemiJoinOrLeftOuterAntiSemiResultImpl(t, sessCtx, leftFilter, leftChunks, rightChunks, leftKeyIndex, rightKeyIndex, leftTypes, rightTypes, leftKeyTypes, rightKeyTypes, leftUsedColumns, otherConditions, resultTypes, true, false)
}

func genSemiJoinResult(t *testing.T, sessCtx sessionctx.Context, leftFilter expression.CNFExprs, leftChunks []*chunk.Chunk, rightChunks []*chunk.Chunk, leftKeyIndex []int, rightKeyIndex []int,
	leftTypes []*types.FieldType, rightTypes []*types.FieldType, leftKeyTypes []*types.FieldType, rightKeyTypes []*types.FieldType, leftUsedColumns []int, otherConditions expression.CNFExprs,
	resultTypes []*types.FieldType) []*chunk.Chunk {
	return genLeftOuterSemiOrSemiJoinOrLeftOuterAntiSemiResultImpl(t, sessCtx, leftFilter, leftChunks, rightChunks, leftKeyIndex, rightKeyIndex, leftTypes, rightTypes, leftKeyTypes, rightKeyTypes, leftUsedColumns, otherConditions, resultTypes, false, false)
}

// generate left outer semi join result using nested loop
func genLeftOuterSemiOrSemiJoinOrLeftOuterAntiSemiResultImpl(t *testing.T, sessCtx sessionctx.Context, leftFilter expression.CNFExprs, leftChunks []*chunk.Chunk, rightChunks []*chunk.Chunk, leftKeyIndex []int, rightKeyIndex []int,
	leftTypes []*types.FieldType, rightTypes []*types.FieldType, leftKeyTypes []*types.FieldType, rightKeyTypes []*types.FieldType, leftUsedColumns []int, otherConditions expression.CNFExprs,
	resultTypes []*types.FieldType, isLeftOuter bool, isAnti bool) []*chunk.Chunk {
	filterVector := make([]bool, 0)
	var err error
	returnChks := make([]*chunk.Chunk, 0, 1)
	resultChk := chunk.New(resultTypes, sessCtx.GetSessionVars().MaxChunkSize, sessCtx.GetSessionVars().MaxChunkSize)
	shallowRowTypes := make([]*types.FieldType, 0, len(leftTypes)+len(rightTypes))
	shallowRowTypes = append(shallowRowTypes, leftTypes...)
	shallowRowTypes = append(shallowRowTypes, rightTypes...)
	shallowRow := chunk.MutRowFromTypes(shallowRowTypes)

	// For each row in left chunks
	for _, leftChunk := range leftChunks {
		if leftFilter != nil {
			filterVector, err = expression.VectorizedFilter(sessCtx.GetExprCtx().GetEvalCtx(), sessCtx.GetSessionVars().EnableVectorizedExpression, leftFilter, chunk.NewIterator4Chunk(leftChunk), filterVector)
			require.NoError(t, err)
		}
		for leftIndex := 0; leftIndex < leftChunk.NumRows(); leftIndex++ {
			filterIndex := leftIndex
			if leftChunk.Sel() != nil {
				filterIndex = leftChunk.Sel()[leftIndex]
			}
			if leftFilter != nil && !filterVector[filterIndex] {
				if isLeftOuter {
					// Filtered by left filter
					// Left Outer Semi Join: append 0 for matched flag
					// Left Outer Anti Semi Join: append 1 for matched flag
					appendToResultChk(leftChunk.GetRow(leftIndex), chunk.Row{}, leftUsedColumns, nil, resultChk)
					if isAnti {
						resultChk.AppendInt64(len(leftUsedColumns), 1)
					} else {
						resultChk.AppendInt64(len(leftUsedColumns), 0)
					}
				}

				if resultChk.IsFull() {
					returnChks = append(returnChks, resultChk)
					resultChk = chunk.New(resultTypes, sessCtx.GetSessionVars().MaxChunkSize, sessCtx.GetSessionVars().MaxChunkSize)
				}
				continue
			}

			leftRow := leftChunk.GetRow(leftIndex)
			hasMatch := false
			hasNull := false

			// For each row in right chunks
			for _, rightChunk := range rightChunks {
				for rightIndex := 0; rightIndex < rightChunk.NumRows(); rightIndex++ {
					rightRow := rightChunk.GetRow(rightIndex)
					valid := !containsNullKey(leftRow, leftKeyIndex) && !containsNullKey(rightRow, rightKeyIndex)
					if valid {
						ok, err := codec.EqualChunkRow(sessCtx.GetSessionVars().StmtCtx.TypeCtx(), leftRow, leftKeyTypes, leftKeyIndex,
							rightRow, rightKeyTypes, rightKeyIndex)
						require.NoError(t, err)
						valid = ok
					}

					if valid && otherConditions != nil {
						shallowRow.ShallowCopyPartialRow(0, leftRow)
						shallowRow.ShallowCopyPartialRow(len(leftTypes), rightRow)
						matched, null, err := expression.EvalBool(sessCtx.GetExprCtx().GetEvalCtx(), otherConditions, shallowRow.ToRow())
						require.NoError(t, err)
						valid = matched
						hasNull = hasNull || null
					}

					if valid {
						hasMatch = true
						break
					}
				}
				if hasMatch {
					break
				}
			}

			if isLeftOuter {
				// Append result with matched flag
				appendToResultChk(leftRow, chunk.Row{}, leftUsedColumns, nil, resultChk)
				if isAnti {
					if hasMatch {
						resultChk.AppendInt64(len(leftUsedColumns), 0)
					} else {
						if hasNull {
							resultChk.AppendNull(len(leftUsedColumns))
						} else {
							resultChk.AppendInt64(len(leftUsedColumns), 1)
						}
					}
				} else {
					if hasMatch {
						resultChk.AppendInt64(len(leftUsedColumns), 1)
					} else {
						if hasNull {
							resultChk.AppendNull(len(leftUsedColumns))
						} else {
							resultChk.AppendInt64(len(leftUsedColumns), 0)
						}
					}
				}
			} else {
				if hasMatch {
					appendToResultChk(leftRow, chunk.Row{}, leftUsedColumns, nil, resultChk)
				}
			}

			if resultChk.IsFull() {
				returnChks = append(returnChks, resultChk)
				resultChk = chunk.New(resultTypes, sessCtx.GetSessionVars().MaxChunkSize, sessCtx.GetSessionVars().MaxChunkSize)
			}
		}
	}
	if resultChk.NumRows() > 0 {
		returnChks = append(returnChks, resultChk)
	}
	return returnChks
}

func testLeftOuterSemiOrSemiJoinProbeBasic(t *testing.T, isLeftOuter bool, isAnti bool) {
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

	rightAsBuildSide := []bool{true}
	if !isLeftOuter {
		rightAsBuildSide = append(rightAsBuildSide, false)
	}

	partitionNumber := 4
	simpleFilter := createSimpleFilter(t)
	hasFilter := []bool{false}
	if isLeftOuter {
		hasFilter = append(hasFilter, true)
	}

	var joinType logicalop.JoinType
	if isLeftOuter {
		joinType = logicalop.LeftOuterSemiJoin
		if isAnti {
			joinType = logicalop.AntiLeftOuterSemiJoin
		}
	} else {
		joinType = logicalop.SemiJoin
	}

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
			for _, testFilter := range hasFilter {
				leftFilter := simpleFilter
				if !testFilter {
					leftFilter = nil
				}
				testJoinProbe(t, false, tc.leftKeyIndex, tc.rightKeyIndex, tc.leftKeyTypes, tc.rightKeyTypes, tc.leftTypes, tc.rightTypes, value, tc.leftUsed,
					tc.rightUsed, tc.leftUsedByOtherCondition, tc.rightUsedByOtherCondition, leftFilter, nil, tc.otherCondition, partitionNumber, joinType, 200)
				testJoinProbe(t, false, tc.leftKeyIndex, tc.rightKeyIndex, toNullableTypes(tc.leftKeyTypes), toNullableTypes(tc.rightKeyTypes),
					toNullableTypes(tc.leftTypes), toNullableTypes(tc.rightTypes), value, tc.leftUsed, tc.rightUsed, tc.leftUsedByOtherCondition, tc.rightUsedByOtherCondition,
					leftFilter, nil, tc.otherCondition, partitionNumber, joinType, 200)
			}
		}
	}
}

func testLeftOuterSemiJoinProbeAllJoinKeys(t *testing.T, isLeftOuter bool, isAnti bool) {
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
	var joinType logicalop.JoinType
	if isLeftOuter {
		joinType = logicalop.LeftOuterSemiJoin
		if isAnti {
			joinType = logicalop.AntiLeftOuterSemiJoin
		}
	} else {
		joinType = logicalop.SemiJoin
	}
	partitionNumber := 4

	rightAsBuildSide := []bool{true}
	if !isLeftOuter {
		rightAsBuildSide = append(rightAsBuildSide, false)
	}

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

func testLeftOuterSemiJoinProbeOtherCondition(t *testing.T, isLeftOuter bool, isAnti bool) {
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
	rTypes = append(rTypes, rTypes...)

	tinyTp := types.NewFieldType(mysql.TypeTiny)
	a := &expression.Column{Index: 1, RetType: nullableIntTp}
	b := &expression.Column{Index: 8, RetType: nullableIntTp}
	sf, err := expression.NewFunction(mock.NewContext(), ast.GT, tinyTp, a, b)
	require.NoError(t, err, "error when create other condition")
	// test condition `a = b` from `a in (select b from t2)`
	a2 := &expression.Column{Index: 1, RetType: nullableIntTp, InOperand: true}
	b2 := &expression.Column{Index: 8, RetType: nullableIntTp, InOperand: true}
	sf2, err := expression.NewFunction(mock.NewContext(), ast.EQ, tinyTp, a2, b2)
	require.NoError(t, err, "error when create other condition")
	otherCondition := make(expression.CNFExprs, 0)
	otherCondition = append(otherCondition, sf)
	otherCondition2 := make(expression.CNFExprs, 0)
	otherCondition2 = append(otherCondition2, sf2)

	var joinType logicalop.JoinType
	if isLeftOuter {
		joinType = logicalop.LeftOuterSemiJoin
		if isAnti {
			joinType = logicalop.AntiLeftOuterSemiJoin
		}
	} else {
		joinType = logicalop.SemiJoin
	}

	simpleFilter := createSimpleFilter(t)

	hasFilter := []bool{false}
	if isLeftOuter {
		hasFilter = append(hasFilter, true)
	}

	rightAsBuildSide := []bool{true}
	if !isLeftOuter {
		rightAsBuildSide = append(rightAsBuildSide, false)
	}

	partitionNumber := 4
	rightUsed := []int{}

	for _, rightBuild := range rightAsBuildSide {
		for _, testFilter := range hasFilter {
			leftFilter := simpleFilter
			if !testFilter {
				leftFilter = nil
			}
			testJoinProbe(t, false, []int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, lTypes, rTypes, rightBuild, []int{1, 2, 4}, rightUsed, []int{1}, []int{3}, leftFilter, nil, otherCondition, partitionNumber, joinType, 200)
			testJoinProbe(t, false, []int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, lTypes, rTypes, rightBuild, []int{}, rightUsed, []int{1}, []int{3}, leftFilter, nil, otherCondition, partitionNumber, joinType, 200)
			testJoinProbe(t, false, []int{0}, []int{0}, []*types.FieldType{nullableIntTp}, []*types.FieldType{nullableIntTp}, toNullableTypes(lTypes), toNullableTypes(rTypes), rightBuild, []int{1, 2, 4}, rightUsed, []int{1}, []int{3}, leftFilter, nil, otherCondition, partitionNumber, joinType, 200)
			testJoinProbe(t, false, []int{0}, []int{0}, []*types.FieldType{nullableIntTp}, []*types.FieldType{nullableIntTp}, toNullableTypes(lTypes), toNullableTypes(rTypes), rightBuild, nil, rightUsed, []int{1}, []int{3}, leftFilter, nil, otherCondition, partitionNumber, joinType, 200)

			testJoinProbe(t, false, []int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, lTypes, rTypes, rightBuild, []int{1, 2, 4}, rightUsed, []int{1}, []int{3}, leftFilter, nil, otherCondition2, partitionNumber, joinType, 200)
			testJoinProbe(t, false, []int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, lTypes, rTypes, rightBuild, []int{}, rightUsed, []int{1}, []int{3}, leftFilter, nil, otherCondition2, partitionNumber, joinType, 200)
			testJoinProbe(t, false, []int{0}, []int{0}, []*types.FieldType{nullableIntTp}, []*types.FieldType{nullableIntTp}, toNullableTypes(lTypes), toNullableTypes(rTypes), rightBuild, []int{1, 2, 4}, rightUsed, []int{1}, []int{3}, leftFilter, nil, otherCondition2, partitionNumber, joinType, 200)
			testJoinProbe(t, false, []int{0}, []int{0}, []*types.FieldType{nullableIntTp}, []*types.FieldType{nullableIntTp}, toNullableTypes(lTypes), toNullableTypes(rTypes), rightBuild, nil, rightUsed, []int{1}, []int{3}, leftFilter, nil, otherCondition2, partitionNumber, joinType, 200)
		}
	}
}

func testLeftOuterSemiJoinProbeWithSel(t *testing.T, isLeftOuter bool, isAnti bool) {
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

	var joinType logicalop.JoinType
	if isLeftOuter {
		joinType = logicalop.LeftOuterSemiJoin
		if isAnti {
			joinType = logicalop.AntiLeftOuterSemiJoin
		}
	} else {
		joinType = logicalop.SemiJoin
	}

	rightAsBuildSide := []bool{true}
	if !isLeftOuter {
		rightAsBuildSide = append(rightAsBuildSide, false)
	}

	simpleFilter := createSimpleFilter(t)

	hasFilter := []bool{false}
	if isLeftOuter {
		hasFilter = append(hasFilter, true)
	}

	partitionNumber := 4
	rightUsed := []int{}

	for _, rightBuild := range rightAsBuildSide {
		for _, useFilter := range hasFilter {
			leftFilter := simpleFilter
			if !useFilter {
				leftFilter = nil
			}
			testJoinProbe(t, true, []int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, lTypes, rTypes, rightBuild, []int{1, 2, 4}, rightUsed, []int{1}, []int{3}, leftFilter, nil, otherCondition, partitionNumber, joinType, 500)
			testJoinProbe(t, true, []int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, lTypes, rTypes, rightBuild, []int{}, rightUsed, []int{1}, []int{3}, leftFilter, nil, otherCondition, partitionNumber, joinType, 500)
			testJoinProbe(t, true, []int{0}, []int{0}, []*types.FieldType{nullableIntTp}, []*types.FieldType{nullableIntTp}, toNullableTypes(lTypes), toNullableTypes(rTypes), rightBuild, []int{1, 2, 4}, rightUsed, []int{1}, []int{3}, leftFilter, nil, otherCondition, partitionNumber, joinType, 500)
			testJoinProbe(t, true, []int{0}, []int{0}, []*types.FieldType{nullableIntTp}, []*types.FieldType{nullableIntTp}, toNullableTypes(lTypes), toNullableTypes(rTypes), rightBuild, nil, rightUsed, []int{1}, []int{3}, leftFilter, nil, otherCondition, partitionNumber, joinType, 500)
		}
	}
}

func TestLeftOuterSemiJoinProbeBasic(t *testing.T) {
	testLeftOuterSemiOrSemiJoinProbeBasic(t, true, false)
}

func TestLeftOuterSemiJoinProbeAllJoinKeys(t *testing.T) {
	testLeftOuterSemiJoinProbeAllJoinKeys(t, true, false)
}

func TestLeftOuterSemiJoinProbeOtherCondition(t *testing.T) {
	testLeftOuterSemiJoinProbeOtherCondition(t, true, false)
}

func TestLeftOuterSemiJoinProbeWithSel(t *testing.T) {
	testLeftOuterSemiJoinProbeWithSel(t, true, false)
}

func TestLeftOuterSemiJoinBuildResultFastPath(t *testing.T) {
	testLeftOuterSemiJoinOrLeftOuterAntiSemiJoinBuildResultFastPath(t, false)
}

func testLeftOuterSemiJoinOrLeftOuterAntiSemiJoinBuildResultFastPath(t *testing.T, isAnti bool) {
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
	rTypes = append(rTypes, rTypes...)

	tinyTp := types.NewFieldType(mysql.TypeTiny)
	a := &expression.Column{Index: 1, RetType: nullableIntTp}
	b := &expression.Column{Index: 8, RetType: nullableIntTp}
	sf, err := expression.NewFunction(mock.NewContext(), ast.GT, tinyTp, a, b)
	require.NoError(t, err, "error when create other condition")
	// test condition `a = b` from `a in (select b from t2)`
	a2 := &expression.Column{Index: 1, RetType: nullableIntTp, InOperand: true}
	b2 := &expression.Column{Index: 8, RetType: nullableIntTp, InOperand: true}
	sf2, err := expression.NewFunction(mock.NewContext(), ast.EQ, tinyTp, a2, b2)
	require.NoError(t, err, "error when create other condition")
	otherCondition := make(expression.CNFExprs, 0)
	otherCondition = append(otherCondition, sf)
	otherCondition2 := make(expression.CNFExprs, 0)
	otherCondition2 = append(otherCondition2, sf2)
	joinType := logicalop.LeftOuterSemiJoin
	if isAnti {
		joinType = logicalop.AntiLeftOuterSemiJoin
	}
	simpleFilter := createSimpleFilter(t)
	hasFilter := []bool{false, true}
	rightAsBuildSide := []bool{true}
	partitionNumber := 4
	rightUsed := []int{}

	for _, rightBuild := range rightAsBuildSide {
		for _, testFilter := range hasFilter {
			leftFilter := simpleFilter
			if !testFilter {
				leftFilter = nil
			}
			// MockContext set MaxChunkSize to 32, input chunk size should be less than 32 to test fast path
			testJoinProbe(t, false, []int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, lTypes, rTypes, rightBuild, []int{1, 2, 4}, rightUsed, []int{1}, []int{3}, leftFilter, nil, otherCondition, partitionNumber, joinType, 30)
			testJoinProbe(t, false, []int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, lTypes, rTypes, rightBuild, []int{}, rightUsed, []int{1}, []int{3}, leftFilter, nil, otherCondition, partitionNumber, joinType, 30)
			testJoinProbe(t, true, []int{0}, []int{0}, []*types.FieldType{nullableIntTp}, []*types.FieldType{nullableIntTp}, toNullableTypes(lTypes), toNullableTypes(rTypes), rightBuild, []int{1, 2, 4}, rightUsed, []int{1}, []int{3}, leftFilter, nil, otherCondition, partitionNumber, joinType, 30)
			testJoinProbe(t, true, []int{0}, []int{0}, []*types.FieldType{nullableIntTp}, []*types.FieldType{nullableIntTp}, toNullableTypes(lTypes), toNullableTypes(rTypes), rightBuild, nil, rightUsed, []int{1}, []int{3}, leftFilter, nil, otherCondition, partitionNumber, joinType, 30)

			testJoinProbe(t, false, []int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, lTypes, rTypes, rightBuild, []int{1, 2, 4}, rightUsed, []int{1}, []int{3}, leftFilter, nil, otherCondition2, partitionNumber, joinType, 30)
			testJoinProbe(t, false, []int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, lTypes, rTypes, rightBuild, []int{}, rightUsed, []int{1}, []int{3}, leftFilter, nil, otherCondition2, partitionNumber, joinType, 30)
			testJoinProbe(t, true, []int{0}, []int{0}, []*types.FieldType{nullableIntTp}, []*types.FieldType{nullableIntTp}, toNullableTypes(lTypes), toNullableTypes(rTypes), rightBuild, []int{1, 2, 4}, rightUsed, []int{1}, []int{3}, leftFilter, nil, otherCondition2, partitionNumber, joinType, 30)
			testJoinProbe(t, true, []int{0}, []int{0}, []*types.FieldType{nullableIntTp}, []*types.FieldType{nullableIntTp}, toNullableTypes(lTypes), toNullableTypes(rTypes), rightBuild, nil, rightUsed, []int{1}, []int{3}, leftFilter, nil, otherCondition2, partitionNumber, joinType, 30)

			testJoinProbe(t, false, []int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, lTypes, rTypes, rightBuild, []int{1, 2, 4}, rightUsed, []int{1}, []int{3}, leftFilter, nil, nil, partitionNumber, joinType, 30)
			testJoinProbe(t, false, []int{0}, []int{0}, []*types.FieldType{intTp}, []*types.FieldType{intTp}, lTypes, rTypes, rightBuild, []int{}, rightUsed, []int{1}, []int{3}, leftFilter, nil, nil, partitionNumber, joinType, 30)
			testJoinProbe(t, true, []int{0}, []int{0}, []*types.FieldType{nullableIntTp}, []*types.FieldType{nullableIntTp}, toNullableTypes(lTypes), toNullableTypes(rTypes), rightBuild, []int{1, 2, 4}, rightUsed, []int{1}, []int{3}, leftFilter, nil, nil, partitionNumber, joinType, 30)
			testJoinProbe(t, true, []int{0}, []int{0}, []*types.FieldType{nullableIntTp}, []*types.FieldType{nullableIntTp}, toNullableTypes(lTypes), toNullableTypes(rTypes), rightBuild, nil, rightUsed, []int{1}, []int{3}, leftFilter, nil, nil, partitionNumber, joinType, 30)
		}
	}
}

func TestLeftOuterSemiJoinSpill(t *testing.T) {
	testLeftOuterSemiJoinOrLeftOuterAntiSemiJoinSpill(t, false)
}

func testLeftOuterSemiJoinOrLeftOuterAntiSemiJoinSpill(t *testing.T, isAnti bool) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 32
	leftDataSource, rightDataSource := buildLeftAndRightDataSource(ctx, leftCols, rightCols, false)
	leftDataSourceWithSel, rightDataSourceWithSel := buildLeftAndRightDataSource(ctx, leftCols, rightCols, true)

	intTp := types.NewFieldType(mysql.TypeLonglong)
	intTp.AddFlag(mysql.NotNullFlag)
	stringTp := types.NewFieldType(mysql.TypeVarString)
	stringTp.AddFlag(mysql.NotNullFlag)

	leftTypes := []*types.FieldType{intTp, intTp, intTp, stringTp, intTp}
	rightTypes := []*types.FieldType{intTp, intTp, stringTp, intTp, intTp}

	leftKeys := []*expression.Column{
		{Index: 1, RetType: intTp},
		{Index: 3, RetType: stringTp},
	}
	rightKeys := []*expression.Column{
		{Index: 0, RetType: intTp},
		{Index: 2, RetType: stringTp},
	}

	tinyTp := types.NewFieldType(mysql.TypeTiny)
	a := &expression.Column{Index: 1, RetType: intTp}
	b := &expression.Column{Index: 8, RetType: intTp}
	sf, err := expression.NewFunction(mock.NewContext(), ast.GT, tinyTp, a, b)
	require.NoError(t, err, "error when create other condition")
	otherCondition := make(expression.CNFExprs, 0)
	otherCondition = append(otherCondition, sf)

	maxRowTableSegmentSize = 100
	spillChunkSize = 100

	joinType := logicalop.LeftOuterSemiJoin
	if isAnti {
		joinType = logicalop.AntiLeftOuterSemiJoin
	}
	params := []spillTestParam{
		// basic case
		{true, leftKeys, rightKeys, leftTypes, rightTypes, []int{0, 1, 3, 4}, []int{}, nil, nil, nil, []int64{3000000, 1700000, 3500000, 100000, 10000}},
		// with other condition
		{true, leftKeys, rightKeys, leftTypes, rightTypes, []int{0, 1, 3, 4}, []int{}, otherCondition, []int{1}, []int{3}, []int64{3000000, 1700000, 3500000, 100000, 10000}},
	}

	for _, param := range params {
		testSpill(t, ctx, joinType, leftDataSource, rightDataSource, param)
	}

	params2 := []spillTestParam{
		// basic case with sel
		{true, leftKeys, rightKeys, leftTypes, rightTypes, []int{0, 1, 3, 4}, []int{}, nil, nil, nil, []int64{1000000, 900000, 1700000, 100000, 10000}},
		// with other condition with sel
		{true, leftKeys, rightKeys, leftTypes, rightTypes, []int{0, 1, 3, 4}, []int{}, otherCondition, []int{1}, []int{3}, []int64{1000000, 900000, 1600000, 100000, 10000}},
	}

	for _, param := range params2 {
		testSpill(t, ctx, joinType, leftDataSourceWithSel, rightDataSourceWithSel, param)
	}
}
