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
