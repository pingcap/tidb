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
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

const maxChunkSizeInTest = 32

var semiJoinleftCols = []*expression.Column{
	{Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)},
	{Index: 1, RetType: types.NewFieldType(mysql.TypeLonglong)},
}

var semiJoinrightCols = []*expression.Column{
	{Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)},
	{Index: 1, RetType: types.NewFieldType(mysql.TypeLonglong)},
}

var semiJoinRetTypes = []*types.FieldType{
	types.NewFieldType(mysql.TypeLonglong),
	types.NewFieldType(mysql.TypeLonglong),
}

func buildSemiDataSourceAndExpectResult(ctx sessionctx.Context, leftCols []*expression.Column, rightCols []*expression.Column, rightAsBuildSide bool, hasOtherCondition bool, hasDuplicateKey bool) (*testutil.MockDataSource, *testutil.MockDataSource, []chunk.Row) {
	leftSchema := expression.NewSchema(leftCols...)
	rightSchema := expression.NewSchema(rightCols...)

	rowNum := int64(50000)
	leftCol0Datums := make([]any, 0, rowNum)
	leftCol1Datums := make([]any, 0, rowNum)
	rightCol0Datums := make([]any, 0, rowNum)
	rightCol1Datums := make([]any, 0, rowNum)

	leftCol0StartNum := int64(30000)

	intTp := types.NewFieldType(mysql.TypeLonglong)
	expectResultChunk := chunk.NewChunkWithCapacity([]*types.FieldType{intTp, intTp}, 10000)
	expectResult := make([]chunk.Row, 0, 10000)

	if hasDuplicateKey {
		if rightAsBuildSide {
			differentKeyNum := int64(10000)
			for i := int64(0); i < differentKeyNum; i++ {
				leftCol0Datums = append(leftCol0Datums, i)
				leftCol1Datums = append(leftCol1Datums, int64(1))

				singleKeyNum := rand.Int31n(2 * maxChunkSizeInTest)
				if singleKeyNum == 0 {
					continue
				}

				canOtherConditionSuccess := rand.Int31n(10) < 5
				if canOtherConditionSuccess {
					expectResultChunk.AppendInt64(0, i)
					expectResultChunk.AppendInt64(1, 1)

					otherConditionSuccessNum := rand.Int31n(singleKeyNum) + 1
					for j := 0; j < int(singleKeyNum); j++ {
						rightCol0Datums = append(rightCol0Datums, i)
						if j < int(otherConditionSuccessNum) {
							rightCol1Datums = append(rightCol1Datums, int64(0))
						} else {
							rightCol1Datums = append(rightCol1Datums, int64(1))
						}
					}
				} else {
					for j := 0; j < int(singleKeyNum); j++ {
						rightCol0Datums = append(rightCol0Datums, i)
						rightCol1Datums = append(rightCol1Datums, int64(1))
					}
				}
			}
		} else {
			differentKeyNum := int64(10000)
			for i := int64(0); i < differentKeyNum; i++ {
				rightCol0Datums = append(rightCol0Datums, i)
				rightCol1Datums = append(rightCol1Datums, int64(0))

				singleKeyNum := rand.Int31n(2 * maxChunkSizeInTest)
				if singleKeyNum == 0 {
					continue
				}

				canOtherConditionSuccess := rand.Int31n(10) < 5
				if canOtherConditionSuccess {
					otherConditionSuccessNum := rand.Int31n(singleKeyNum) + 1
					for j := 0; j < int(singleKeyNum); j++ {
						leftCol0Datums = append(leftCol0Datums, i)
						if j < int(otherConditionSuccessNum) {
							leftCol1Datums = append(leftCol1Datums, int64(1))
							expectResultChunk.AppendInt64(0, i)
							expectResultChunk.AppendInt64(1, 1)
						} else {
							leftCol1Datums = append(leftCol1Datums, int64(0))
						}
					}
				} else {
					for j := 0; j < int(singleKeyNum); j++ {
						leftCol0Datums = append(leftCol0Datums, i)
						leftCol1Datums = append(leftCol1Datums, int64(0))
					}
				}
			}
		}
	} else {
		for i := int64(0); i < rowNum; i++ {
			leftCol0AppendedData := leftCol0StartNum + i
			leftCol0Datums = append(leftCol0Datums, leftCol0AppendedData)

			if hasOtherCondition {
				if leftCol0AppendedData%2 == 0 {
					leftCol1Datums = append(leftCol1Datums, int64(1))
					if leftCol0AppendedData < rowNum {
						expectResultChunk.AppendInt64(0, int64(leftCol0AppendedData))
						expectResultChunk.AppendInt64(1, 1)
					}
				} else {
					leftCol1Datums = append(leftCol1Datums, int64(0))
				}
			} else {
				leftCol1Datums = append(leftCol1Datums, int64(1))
				if leftCol0AppendedData < rowNum {
					expectResultChunk.AppendInt64(0, int64(leftCol0AppendedData))
					expectResultChunk.AppendInt64(1, 1)
				}
			}

			rightCol0Datums = append(rightCol0Datums, i)
			rightCol1Datums = append(rightCol1Datums, int64(0))
		}
	}

	leftLen := len(leftCol0Datums)
	rightLen := len(rightCol0Datums)

	// Shuffle
	for i := int64(0); i < int64(leftLen); i++ {
		j := rand.Int31n(int32(i + 1))
		leftCol0Datums[i], leftCol0Datums[j] = leftCol0Datums[j], leftCol0Datums[i]
		leftCol1Datums[i], leftCol1Datums[j] = leftCol1Datums[j], leftCol1Datums[i]
	}

	for i := int64(0); i < int64(rightLen); i++ {
		j := rand.Int31n(int32(i + 1))
		rightCol0Datums[i], rightCol0Datums[j] = rightCol0Datums[j], rightCol0Datums[i]
		rightCol1Datums[i], rightCol1Datums[j] = rightCol1Datums[j], rightCol1Datums[i]
	}

	resultRowNum := expectResultChunk.NumRows()
	for i := 0; i < resultRowNum; i++ {
		expectResult = append(expectResult, expectResultChunk.GetRow(i))
	}

	leftMockSrcParm := testutil.MockDataSourceParameters{DataSchema: leftSchema, Ctx: ctx, Rows: int(leftLen), Ndvs: []int{-1, -1}, Datums: [][]any{leftCol0Datums, leftCol1Datums}, HasSel: false}
	rightMockSrcParm := testutil.MockDataSourceParameters{DataSchema: rightSchema, Ctx: ctx, Rows: int(rightLen), Ndvs: []int{-1, -1}, Datums: [][]any{rightCol0Datums, rightCol1Datums}, HasSel: false}
	return testutil.BuildMockDataSource(leftMockSrcParm), testutil.BuildMockDataSource(rightMockSrcParm), expectResult
}

func testSemiJoin(t *testing.T, rightAsBuildSide bool, hasOtherCondition bool, hasDuplicateKey bool) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = maxChunkSizeInTest
	ctx.GetSessionVars().MaxChunkSize = maxChunkSizeInTest
	leftDataSource, rightDataSource, expectedResult := buildSemiDataSourceAndExpectResult(ctx, semiJoinleftCols, semiJoinrightCols, rightAsBuildSide, hasOtherCondition, hasDuplicateKey)

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
	if hasOtherCondition {
		lUsedInOtherCondition = append(lUsedInOtherCondition, 1)
		rUsedInOtherCondition = append(rUsedInOtherCondition, 1)

		tinyTp := types.NewFieldType(mysql.TypeTiny)
		a := &expression.Column{Index: 1, RetType: intTp}
		b := &expression.Column{Index: 3, RetType: intTp}
		sf, err := expression.NewFunction(mock.NewContext(), ast.GT, tinyTp, a, b)
		require.NoError(t, err, "error when create other condition")
		otherCondition = append(otherCondition, sf)
	}

	info := &hashJoinInfo{
		ctx:                   ctx,
		schema:                buildSchema(semiJoinRetTypes),
		leftExec:              leftDataSource,
		rightExec:             rightDataSource,
		joinType:              logicalop.SemiJoin,
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
	// printResult(result)
	checkResults(t, semiJoinRetTypes, result, expectedResult)
}

func TestSemiJoinBasic(t *testing.T) {
	testSemiJoin(t, false, false, false) // Left side build without other condition
	testSemiJoin(t, false, true, false)  // Left side build with other condition
	testSemiJoin(t, true, false, false)  // Right side build without other condition
	testSemiJoin(t, true, true, false)   // Right side build with other condition
}

func TestSemiJoinDuplicateKeys(t *testing.T) {
	testSemiJoin(t, true, true, true)  // Right side build with other condition
	testSemiJoin(t, false, true, true) // Left side build with other condition
}

// TODO add test for `truncateSelect` function

// TODO delete
// func printResult(result []chunk.Row) {
// 	for _, res := range result {
// 		log.Info(res.ToString(semiJoinRetTypes))
// 	}
// }
