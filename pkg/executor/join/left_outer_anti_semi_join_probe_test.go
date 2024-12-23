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
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
)

func genLeftOuterAntiSemiJoinResult(t *testing.T, sessCtx sessionctx.Context, leftFilter expression.CNFExprs, leftChunks []*chunk.Chunk, rightChunks []*chunk.Chunk, leftKeyIndex []int, rightKeyIndex []int,
	leftTypes []*types.FieldType, rightTypes []*types.FieldType, leftKeyTypes []*types.FieldType, rightKeyTypes []*types.FieldType, leftUsedColumns []int, otherConditions expression.CNFExprs,
	resultTypes []*types.FieldType) []*chunk.Chunk {
	return genLeftOuterAntiSemiResultImpl(t, sessCtx, leftFilter, leftChunks, rightChunks, leftKeyIndex, rightKeyIndex, leftTypes, rightTypes, leftKeyTypes, rightKeyTypes, leftUsedColumns, otherConditions, resultTypes, true)
}

// generate left outer semi join result using nested loop
func genLeftOuterAntiSemiResultImpl(t *testing.T, sessCtx sessionctx.Context, leftFilter expression.CNFExprs, leftChunks []*chunk.Chunk, rightChunks []*chunk.Chunk, leftKeyIndex []int, rightKeyIndex []int,
	leftTypes []*types.FieldType, rightTypes []*types.FieldType, leftKeyTypes []*types.FieldType, rightKeyTypes []*types.FieldType, leftUsedColumns []int, otherConditions expression.CNFExprs,
	resultTypes []*types.FieldType, isLeftOuter bool) []*chunk.Chunk {
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
					// Filtered by left filter, append 1 for matched flag
					appendToResultChk(leftChunk.GetRow(leftIndex), chunk.Row{}, leftUsedColumns, nil, resultChk)
					resultChk.AppendInt64(len(leftUsedColumns), 1)
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
				if !hasMatch && !hasNull {
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

func TestLeftOuterAntiSemiJoinProbeBasic(t *testing.T) {
	testLeftOuterSemiOrSemiJoinProbeBasic(t, true, true)
}

func TestLeftOuterAntiSemiJoinProbeAllJoinKeys(t *testing.T) {
	testLeftOuterSemiJoinProbeAllJoinKeys(t, true, true)
}

func TestLeftOuterAntiSemiJoinProbeOtherCondition(t *testing.T) {
	testLeftOuterSemiJoinProbeOtherCondition(t, true, true)
}

func TestLeftOuterAntiSemiJoinProbeWithSel(t *testing.T) {
	testLeftOuterSemiJoinProbeWithSel(t, true, true)
}

func TestLeftOuterAntiSemiJoinBuildResultFastPath(t *testing.T) {
	testLeftOuterSemiJoinOrLeftOuterAntiSemiJoinBuildResultFastPath(t, true)
}

func TestLeftOuterAntiSemiJoinSpill(t *testing.T) {
	testLeftOuterSemiJoinOrLeftOuterAntiSemiJoinSpill(t, true)
}
