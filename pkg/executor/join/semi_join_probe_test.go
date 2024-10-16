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

func buildSemiDataSourceAndExpectResult(ctx sessionctx.Context, leftCols []*expression.Column, rightCols []*expression.Column, hasOtherCondition bool) (*testutil.MockDataSource, *testutil.MockDataSource, []chunk.Row) {
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

	// Shuffle
	for i := int64(0); i < rowNum; i++ {
		j := rand.Int31n(int32(i + 1))
		leftCol0Datums[i], leftCol0Datums[j] = leftCol0Datums[j], leftCol0Datums[i]
		leftCol1Datums[i], leftCol1Datums[j] = leftCol1Datums[j], leftCol1Datums[i]
	}

	resultRowNum := expectResultChunk.NumRows()
	for i := 0; i < resultRowNum; i++ {
		expectResult = append(expectResult, expectResultChunk.GetRow(i))
	}

	leftMockSrcParm := testutil.MockDataSourceParameters{DataSchema: leftSchema, Ctx: ctx, Rows: int(rowNum), Ndvs: []int{-1, -1}, Datums: [][]any{leftCol0Datums, leftCol1Datums}, HasSel: false}
	rightMockSrcParm := testutil.MockDataSourceParameters{DataSchema: rightSchema, Ctx: ctx, Rows: int(rowNum), Ndvs: []int{-1, -1}, Datums: [][]any{rightCol0Datums, rightCol1Datums}, HasSel: false}
	return testutil.BuildMockDataSource(leftMockSrcParm), testutil.BuildMockDataSource(rightMockSrcParm), expectResult
}

// TODO extract common codes in tests

func TestLeftSideBuildNoOtherCondition(t *testing.T) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 32
	leftDataSource, rightDataSource, expectedResult := buildSemiDataSourceAndExpectResult(ctx, semiJoinleftCols, semiJoinrightCols, false)

	maxRowTableSegmentSize = 100

	intTp := types.NewFieldType(mysql.TypeLonglong)

	leftKeys := []*expression.Column{
		{Index: 0, RetType: intTp},
	}

	rightKeys := []*expression.Column{
		{Index: 0, RetType: intTp},
	}

	rightAsBuildSide := false
	var buildKeys []*expression.Column
	var probeKeys []*expression.Column
	if rightAsBuildSide {
		buildKeys = rightKeys
		probeKeys = leftKeys
	} else {
		buildKeys = leftKeys
		probeKeys = rightKeys
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
		otherCondition:        nil,
		lUsedInOtherCondition: []int{},
		rUsedInOtherCondition: []int{},
	}

	leftDataSource.PrepareChunks()
	rightDataSource.PrepareChunks()

	// TODO delete
	// results := make([]*chunk.Chunk, 0)
	// totalRowNum := 0
	// for {
	// 	chk := chunk.NewChunkWithCapacity(semiJoinRetTypes, 32)
	// 	leftDataSource.Next(nil, chk)
	// 	rowNum := chk.NumRows()
	// 	if rowNum == 0 {
	// 		break
	// 	}
	// 	totalRowNum += rowNum
	// 	results = append(results, chk)
	// }
	// printResult(sortRows(results, semiJoinRetTypes))
	// log.Info(fmt.Sprintf("TotalRowNum: %d", totalRowNum))
	// leftDataSource.PrepareChunks()

	hashJoinExec := buildHashJoinV2Exec(info)
	result := getSortedResults(t, hashJoinExec, semiJoinRetTypes)
	// printResult(result) // TODO delete
	checkResults(t, semiJoinRetTypes, result, expectedResult)
}

// TODO delete
// func printResult(result []chunk.Row) {
// 	for _, res := range result {
// 		log.Info(res.ToString(semiJoinRetTypes))
// 	}
// }

func TestLeftSideBuildHasOtherCondition(t *testing.T) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 32
	leftDataSource, rightDataSource, expectedResult := buildSemiDataSourceAndExpectResult(ctx, semiJoinleftCols, semiJoinrightCols, true)

	maxRowTableSegmentSize = 100

	intTp := types.NewFieldType(mysql.TypeLonglong)

	leftKeys := []*expression.Column{
		{Index: 0, RetType: intTp},
	}

	rightKeys := []*expression.Column{
		{Index: 0, RetType: intTp},
	}

	tinyTp := types.NewFieldType(mysql.TypeTiny)
	a := &expression.Column{Index: 1, RetType: intTp}
	b := &expression.Column{Index: 3, RetType: intTp}
	sf, err := expression.NewFunction(mock.NewContext(), ast.GT, tinyTp, a, b)
	require.NoError(t, err, "error when create other condition")
	otherCondition := make(expression.CNFExprs, 0)
	otherCondition = append(otherCondition, sf)

	rightAsBuildSide := false
	var buildKeys []*expression.Column
	var probeKeys []*expression.Column
	if rightAsBuildSide {
		buildKeys = rightKeys
		probeKeys = leftKeys
	} else {
		buildKeys = leftKeys
		probeKeys = rightKeys
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
		lUsedInOtherCondition: []int{1},
		rUsedInOtherCondition: []int{1},
	}

	leftDataSource.PrepareChunks()
	rightDataSource.PrepareChunks()
	hashJoinExec := buildHashJoinV2Exec(info)
	result := getSortedResults(t, hashJoinExec, semiJoinRetTypes)
	checkResults(t, semiJoinRetTypes, result, expectedResult)
}

func TestRightSideBuildNoOtherCondition(t *testing.T) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 32
	leftDataSource, rightDataSource, expectedResult := buildSemiDataSourceAndExpectResult(ctx, semiJoinleftCols, semiJoinrightCols, false)

	maxRowTableSegmentSize = 100

	intTp := types.NewFieldType(mysql.TypeLonglong)

	leftKeys := []*expression.Column{
		{Index: 0, RetType: intTp},
	}

	rightKeys := []*expression.Column{
		{Index: 0, RetType: intTp},
	}

	rightAsBuildSide := true
	var buildKeys []*expression.Column
	var probeKeys []*expression.Column
	if rightAsBuildSide {
		buildKeys = rightKeys
		probeKeys = leftKeys
	} else {
		buildKeys = leftKeys
		probeKeys = rightKeys
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
		otherCondition:        nil,
		lUsedInOtherCondition: []int{},
		rUsedInOtherCondition: []int{},
	}

	leftDataSource.PrepareChunks()
	rightDataSource.PrepareChunks()
	hashJoinExec := buildHashJoinV2Exec(info)
	result := getSortedResults(t, hashJoinExec, semiJoinRetTypes)
	checkResults(t, semiJoinRetTypes, result, expectedResult)
}

func TestRightSideBuildHasOtherCondition(t *testing.T) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 32
	leftDataSource, rightDataSource, expectedResult := buildSemiDataSourceAndExpectResult(ctx, semiJoinleftCols, semiJoinrightCols, true)

	maxRowTableSegmentSize = 100

	intTp := types.NewFieldType(mysql.TypeLonglong)

	leftKeys := []*expression.Column{
		{Index: 0, RetType: intTp},
	}

	rightKeys := []*expression.Column{
		{Index: 0, RetType: intTp},
	}

	tinyTp := types.NewFieldType(mysql.TypeTiny)
	a := &expression.Column{Index: 1, RetType: intTp}
	b := &expression.Column{Index: 3, RetType: intTp}
	sf, err := expression.NewFunction(mock.NewContext(), ast.GT, tinyTp, a, b)
	require.NoError(t, err, "error when create other condition")
	otherCondition := make(expression.CNFExprs, 0)
	otherCondition = append(otherCondition, sf)

	rightAsBuildSide := true
	var buildKeys []*expression.Column
	var probeKeys []*expression.Column
	if rightAsBuildSide {
		buildKeys = rightKeys
		probeKeys = leftKeys
	} else {
		buildKeys = leftKeys
		probeKeys = rightKeys
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
		lUsedInOtherCondition: []int{1},
		rUsedInOtherCondition: []int{1},
	}

	leftDataSource.PrepareChunks()
	rightDataSource.PrepareChunks()
	hashJoinExec := buildHashJoinV2Exec(info)
	result := getSortedResults(t, hashJoinExec, semiJoinRetTypes)
	checkResults(t, semiJoinRetTypes, result, expectedResult)
}

// TODO add test for `truncateSelect` function
