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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/internal/testutil"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

var leftCols = []*expression.Column{
	{Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)},
	{Index: 1, RetType: types.NewFieldType(mysql.TypeLonglong)},
	{Index: 2, RetType: types.NewFieldType(mysql.TypeLonglong)},
	{Index: 3, RetType: types.NewFieldType(mysql.TypeVarString)},
	{Index: 4, RetType: types.NewFieldType(mysql.TypeLonglong)},
}

var rightCols = []*expression.Column{
	{Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)},
	{Index: 1, RetType: types.NewFieldType(mysql.TypeLonglong)},
	{Index: 2, RetType: types.NewFieldType(mysql.TypeVarString)},
	{Index: 3, RetType: types.NewFieldType(mysql.TypeLonglong)},
	{Index: 4, RetType: types.NewFieldType(mysql.TypeLonglong)},
}

var retTypes = []*types.FieldType{
	types.NewFieldType(mysql.TypeLonglong),
	types.NewFieldType(mysql.TypeLonglong),
	types.NewFieldType(mysql.TypeVarString),
	types.NewFieldType(mysql.TypeLonglong),
	types.NewFieldType(mysql.TypeLonglong),
	types.NewFieldType(mysql.TypeVarString),
	types.NewFieldType(mysql.TypeLonglong),
	types.NewFieldType(mysql.TypeLonglong),
}

type spillTestParam struct {
	rightAsBuildSide          bool
	leftKeys                  []*expression.Column
	rightKeys                 []*expression.Column
	leftTypes                 []*types.FieldType
	rightTypes                []*types.FieldType
	leftUsed                  []int
	rightUsed                 []int
	otherCondition            expression.CNFExprs
	leftUsedByOtherCondition  []int
	rightUsedByOtherCondition []int
	memoryLimits              []int64
}

func getExpectedResults(t *testing.T, ctx *mock.Context, info *hashJoinInfo, resultTypes []*types.FieldType, leftDataSource *testutil.MockDataSource, rightDataSource *testutil.MockDataSource) []chunk.Row {
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)

	leftDataSource.PrepareChunks()
	rightDataSource.PrepareChunks()

	// Execute no spill hash join to get expected result
	hashJoinExec := buildHashJoinV2Exec(info)
	results := executeHashJoinExec(t, hashJoinExec)
	require.False(t, hashJoinExec.spillHelper.isSpillTriggeredForTest())
	return sortRows(results, resultTypes)
}

func testInnerJoinSpillCase1(t *testing.T, ctx *mock.Context, expectedResult []chunk.Row, info *hashJoinInfo, retTypes []*types.FieldType, leftDataSource *testutil.MockDataSource, rightDataSource *testutil.MockDataSource, memoryLimit int64) {
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, memoryLimit)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)

	leftDataSource.PrepareChunks()
	rightDataSource.PrepareChunks()
	hashJoinExec := buildHashJoinV2Exec(info)
	result := getSortedResults(t, hashJoinExec, retTypes)
	require.True(t, hashJoinExec.isAllMemoryClearedForTest())
	require.True(t, hashJoinExec.spillHelper.isSpillTriggedInBuildingStageForTest())
	require.False(t, hashJoinExec.spillHelper.areAllPartitionsSpilledForTest())
	require.False(t, hashJoinExec.spillHelper.isRespillTriggeredForTest())
	checkResults(t, retTypes, result, expectedResult)
}

func testInnerJoinSpillCase2(t *testing.T, ctx *mock.Context, expectedResult []chunk.Row, info *hashJoinInfo, retTypes []*types.FieldType, leftDataSource *testutil.MockDataSource, rightDataSource *testutil.MockDataSource, memoryLimit int64) {
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, memoryLimit)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)

	leftDataSource.PrepareChunks()
	rightDataSource.PrepareChunks()
	hashJoinExec := buildHashJoinV2Exec(info)
	result := getSortedResults(t, hashJoinExec, retTypes)
	require.True(t, hashJoinExec.isAllMemoryClearedForTest())
	require.True(t, hashJoinExec.spillHelper.isSpillTriggedInBuildingStageForTest())
	require.False(t, hashJoinExec.spillHelper.isRespillTriggeredForTest())
	checkResults(t, retTypes, result, expectedResult)
}

func testInnerJoinSpillCase3(t *testing.T, ctx *mock.Context, expectedResult []chunk.Row, info *hashJoinInfo, retTypes []*types.FieldType, leftDataSource *testutil.MockDataSource, rightDataSource *testutil.MockDataSource, memoryLimit int64) {
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, memoryLimit)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)

	leftDataSource.PrepareChunks()
	rightDataSource.PrepareChunks()
	hashJoinExec := buildHashJoinV2Exec(info)
	result := getSortedResults(t, hashJoinExec, retTypes)
	require.True(t, hashJoinExec.isAllMemoryClearedForTest())
	require.False(t, hashJoinExec.spillHelper.isSpillTriggedInBuildingStageForTest())
	require.False(t, hashJoinExec.spillHelper.areAllPartitionsSpilledForTest())
	require.False(t, hashJoinExec.spillHelper.isRespillTriggeredForTest())
	require.True(t, hashJoinExec.spillHelper.isSpillTriggeredBeforeBuildingHashTableForTest())
	checkResults(t, retTypes, result, expectedResult)
}

func testInnerJoinSpillCase4(t *testing.T, ctx *mock.Context, expectedResult []chunk.Row, info *hashJoinInfo, retTypes []*types.FieldType, leftDataSource *testutil.MockDataSource, rightDataSource *testutil.MockDataSource, memoryLimit int64) {
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, memoryLimit)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)

	leftDataSource.PrepareChunks()
	rightDataSource.PrepareChunks()
	hashJoinExec := buildHashJoinV2Exec(info)
	result := getSortedResults(t, hashJoinExec, retTypes)
	require.True(t, hashJoinExec.isAllMemoryClearedForTest())
	require.True(t, hashJoinExec.spillHelper.isSpillTriggedInBuildingStageForTest())
	require.True(t, hashJoinExec.spillHelper.areAllPartitionsSpilledForTest())
	require.True(t, hashJoinExec.spillHelper.isRespillTriggeredForTest())
	require.True(t, hashJoinExec.spillHelper.isProbeSkippedInRestoreForTest())
	checkResults(t, retTypes, result, expectedResult)
}

func testInnerJoinSpillCase5(t *testing.T, ctx *mock.Context, info *hashJoinInfo, leftDataSource *testutil.MockDataSource, rightDataSource *testutil.MockDataSource, memoryLimit int64) {
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, memoryLimit)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)

	leftDataSource.PrepareChunks()
	rightDataSource.PrepareChunks()
	hashJoinExec := buildHashJoinV2Exec(info)
	err := executeHashJoinExecAndGetError(t, hashJoinExec)
	require.Equal(t, exceedMaxSpillRoundErrInfo, err.Error())
}

func testUnderApplyExec(t *testing.T, ctx *mock.Context, expectedResult []chunk.Row, info *hashJoinInfo, retTypes []*types.FieldType, leftDataSource *testutil.MockDataSource, rightDataSource *testutil.MockDataSource) {
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, 5000000)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)

	hashJoinExec := buildHashJoinV2Exec(info)
	for i := 0; i < 10; i++ {
		leftDataSource.PrepareChunks()
		rightDataSource.PrepareChunks()
		result := getSortedResults(t, hashJoinExec, retTypes)
		require.True(t, hashJoinExec.isAllMemoryClearedForTest())
		require.True(t, hashJoinExec.spillHelper.isSpillTriggedInBuildingStageForTest())
		require.False(t, hashJoinExec.spillHelper.areAllPartitionsSpilledForTest())
		require.False(t, hashJoinExec.spillHelper.isRespillTriggeredForTest())
		checkResults(t, retTypes, result, expectedResult)
	}
}

func getReturnTypes(joinType logicalop.JoinType, param spillTestParam) []*types.FieldType {
	resultTypes := make([]*types.FieldType, 0, len(param.leftUsed)+len(param.rightUsed))
	for _, colIndex := range param.leftUsed {
		resultTypes = append(resultTypes, param.leftTypes[colIndex].Clone())
		if joinType == logicalop.RightOuterJoin {
			resultTypes[len(resultTypes)-1].DelFlag(mysql.NotNullFlag)
		}
	}
	for _, colIndex := range param.rightUsed {
		resultTypes = append(resultTypes, param.rightTypes[colIndex].Clone())
		if joinType == logicalop.LeftOuterJoin {
			resultTypes[len(resultTypes)-1].DelFlag(mysql.NotNullFlag)
		}
	}
	if joinType == logicalop.LeftOuterSemiJoin || joinType == logicalop.AntiLeftOuterSemiJoin {
		resultTypes = append(resultTypes, types.NewFieldType(mysql.TypeTiny))
	}
	return resultTypes
}

// Case 1: Trigger spill during the building of row table and spill partial partitions
// Case 2: Trigger spill during the building of row table and spill all partitions
// Case 3: Trigger spill between the creation of hash table and the completion of building row table, then spill partial partitions
// Case 4: Trigger re-spill
// Case 5: Trigger re-spill and exceed max spill round
func testSpill(t *testing.T, ctx *mock.Context, joinType logicalop.JoinType, leftDataSource *testutil.MockDataSource, rightDataSource *testutil.MockDataSource, param spillTestParam) {
	returnTypes := getReturnTypes(joinType, param)

	var buildKeys []*expression.Column
	var probeKeys []*expression.Column
	if param.rightAsBuildSide {
		buildKeys = param.rightKeys
		probeKeys = param.leftKeys
	} else {
		buildKeys = param.leftKeys
		probeKeys = param.rightKeys
	}

	info := &hashJoinInfo{
		ctx:                   ctx,
		schema:                buildSchema(returnTypes),
		leftExec:              leftDataSource,
		rightExec:             rightDataSource,
		joinType:              joinType,
		rightAsBuildSide:      param.rightAsBuildSide,
		buildKeys:             buildKeys,
		probeKeys:             probeKeys,
		lUsed:                 param.leftUsed,
		rUsed:                 param.rightUsed,
		otherCondition:        param.otherCondition,
		lUsedInOtherCondition: param.leftUsedByOtherCondition,
		rUsedInOtherCondition: param.rightUsedByOtherCondition,
	}

	expectedResult := getExpectedResults(t, ctx, info, returnTypes, leftDataSource, rightDataSource)
	testInnerJoinSpillCase1(t, ctx, expectedResult, info, returnTypes, leftDataSource, rightDataSource, param.memoryLimits[0])
	testInnerJoinSpillCase2(t, ctx, expectedResult, info, returnTypes, leftDataSource, rightDataSource, param.memoryLimits[1])
	testInnerJoinSpillCase3(t, ctx, expectedResult, info, returnTypes, leftDataSource, rightDataSource, param.memoryLimits[2])
	testInnerJoinSpillCase4(t, ctx, expectedResult, info, returnTypes, leftDataSource, rightDataSource, param.memoryLimits[3])
	testInnerJoinSpillCase5(t, ctx, info, leftDataSource, rightDataSource, param.memoryLimits[4])
}

func TestInnerJoinSpillBasic(t *testing.T) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 32
	leftDataSource, rightDataSource := buildLeftAndRightDataSource(ctx, leftCols, rightCols, false)

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

	params := []spillTestParam{
		// Normal case
		{true, leftKeys, rightKeys, leftTypes, rightTypes, []int{0, 1, 3, 4}, []int{0, 2, 3, 4}, nil, nil, nil, []int64{5000000, 1700000, 6000000, 500000, 10000}},
		{false, leftKeys, rightKeys, leftTypes, rightTypes, []int{0, 1, 3, 4}, []int{0, 2, 3, 4}, nil, nil, nil, []int64{5000000, 1700000, 6000000, 500000, 10000}},
		// rightUsed is empty
		{true, leftKeys, rightKeys, leftTypes, rightTypes, []int{0, 1, 3, 4}, []int{}, nil, nil, nil, []int64{3000000, 1700000, 3500000, 250000, 10000}},
		{false, leftKeys, rightKeys, leftTypes, rightTypes, []int{0, 1, 3, 4}, []int{}, nil, nil, nil, []int64{5000000, 1700000, 6000000, 500000, 10000}},
		// leftUsed is empty
		{true, leftKeys, rightKeys, leftTypes, rightTypes, []int{}, []int{0, 2, 3, 4}, nil, nil, nil, []int64{5000000, 1700000, 6000000, 500000, 10000}},
		{false, leftKeys, rightKeys, leftTypes, rightTypes, []int{}, []int{0, 2, 3, 4}, nil, nil, nil, []int64{3000000, 1700000, 3500000, 250000, 10000}},
	}

	err := failpoint.Enable("github.com/pingcap/tidb/pkg/executor/join/slowWorkers", `return(true)`)
	require.NoError(t, err)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/executor/join/slowWorkers")

	maxRowTableSegmentSize = 100
	spillChunkSize = 100

	for _, param := range params {
		testSpill(t, ctx, logicalop.InnerJoin, leftDataSource, rightDataSource, param)
	}
}

func TestInnerJoinSpillWithOtherCondition(t *testing.T) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 32
	leftDataSource, rightDataSource := buildLeftAndRightDataSource(ctx, leftCols, rightCols, false)

	nullableIntTp := types.NewFieldType(mysql.TypeLonglong)
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
	a := &expression.Column{Index: 0, RetType: nullableIntTp}
	b := &expression.Column{Index: 9, RetType: nullableIntTp}
	sf, err := expression.NewFunction(mock.NewContext(), ast.GT, tinyTp, a, b)
	require.NoError(t, err, "error when create other condition")
	otherCondition := make(expression.CNFExprs, 0)
	otherCondition = append(otherCondition, sf)

	params := []spillTestParam{
		{true, leftKeys, rightKeys, leftTypes, rightTypes, []int{0, 1, 3, 4}, []int{0, 2, 3, 4}, otherCondition, []int{0}, []int{4}, []int64{5000000, 1700000, 6000000, 500000, 10000}},
		{false, leftKeys, rightKeys, leftTypes, rightTypes, []int{0, 1, 3, 4}, []int{0, 2, 3, 4}, otherCondition, []int{0}, []int{4}, []int64{5000000, 1700000, 6000000, 500000, 10000}},
	}

	err = failpoint.Enable("github.com/pingcap/tidb/pkg/executor/join/slowWorkers", `return(true)`)
	require.NoError(t, err)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/executor/join/slowWorkers")

	maxRowTableSegmentSize = 100
	spillChunkSize = 100

	for _, param := range params {
		testSpill(t, ctx, logicalop.InnerJoin, leftDataSource, rightDataSource, param)
	}
}

// Hash join executor may be repeatedly closed and opened
func TestInnerJoinUnderApplyExec(t *testing.T) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 32
	leftDataSource, rightDataSource := buildLeftAndRightDataSource(ctx, leftCols, rightCols, false)

	info := &hashJoinInfo{
		ctx:              ctx,
		schema:           buildSchema(retTypes),
		leftExec:         leftDataSource,
		rightExec:        rightDataSource,
		joinType:         logicalop.InnerJoin,
		rightAsBuildSide: true,
		buildKeys: []*expression.Column{
			{Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)},
			{Index: 2, RetType: types.NewFieldType(mysql.TypeVarString)},
		},
		probeKeys: []*expression.Column{
			{Index: 1, RetType: types.NewFieldType(mysql.TypeLonglong)},
			{Index: 3, RetType: types.NewFieldType(mysql.TypeVarString)},
		},
		lUsed:                 []int{0, 1, 3, 4},
		rUsed:                 []int{0, 2, 3, 4},
		otherCondition:        expression.CNFExprs{},
		lUsedInOtherCondition: []int{0},
		rUsedInOtherCondition: []int{4},
	}

	maxRowTableSegmentSize = 100
	spillChunkSize = 100

	expectedResult := getExpectedResults(t, ctx, info, retTypes, leftDataSource, rightDataSource)
	testUnderApplyExec(t, ctx, expectedResult, info, retTypes, leftDataSource, rightDataSource)
}
