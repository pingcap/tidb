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
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/executor/internal/testutil"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

// TODO delete it
// left table:
//   col0: int (other cond)
//   col1: int (join key)
//   col2: int (dropped)
//   col3: varchar (join key)
//   col4: int

// right table:
//   col0: int (join key)
//   col1: int (dropped)
//   col2: varchar (join key)
//   col3: int (right cond)
//   col4: int (other cond)

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

func getExpectedResults(t *testing.T, info *hashJoinInfo, resultTypes []*types.FieldType, leftDataSource *testutil.MockDataSource, rightDataSource *testutil.MockDataSource) []chunk.Row {
	leftDataSource.PrepareChunks()
	rightDataSource.PrepareChunks()

	// Execute no spill hash join to get expected result
	hashJoinExec := buildHashJoinV2Exec(info)
	results := executeHashJoinExec(t, hashJoinExec)
	require.False(t, hashJoinExec.spillHelper.isSpillTriggeredForTest())
	return sortRows(results, resultTypes)
}

func testInnerJoinSpillCase1(t *testing.T, ctx *mock.Context, expectedResult []chunk.Row, info *hashJoinInfo, retTypes []*types.FieldType, leftDataSource *testutil.MockDataSource, rightDataSource *testutil.MockDataSource) {
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, 4000000)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)

	leftDataSource.PrepareChunks()
	rightDataSource.PrepareChunks()
	hashJoinExec := buildHashJoinV2Exec(info)
	result := getSortedResults(t, hashJoinExec, retTypes)
	require.True(t, hashJoinExec.spillHelper.isSpillTriggedInBuildingStageForTest())
	require.False(t, hashJoinExec.spillHelper.areAllPartitionsSpilledForTest())
	require.False(t, hashJoinExec.spillHelper.isRespillTriggeredForTest())
	checkResults(t, retTypes, result, expectedResult)
}

func testInnerJoinSpillCase2(t *testing.T, ctx *mock.Context, expectedResult []chunk.Row, info *hashJoinInfo, retTypes []*types.FieldType, leftDataSource *testutil.MockDataSource, rightDataSource *testutil.MockDataSource) {
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, 1700000)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)

	leftDataSource.PrepareChunks()
	rightDataSource.PrepareChunks()
	hashJoinExec := buildHashJoinV2Exec(info)
	result := getSortedResults(t, hashJoinExec, retTypes)
	require.True(t, hashJoinExec.spillHelper.isSpillTriggedInBuildingStageForTest())
	require.False(t, hashJoinExec.spillHelper.isRespillTriggeredForTest())
	checkResults(t, retTypes, result, expectedResult)
}

func testInnerJoinSpillCase3(t *testing.T, ctx *mock.Context, expectedResult []chunk.Row, info *hashJoinInfo, retTypes []*types.FieldType, leftDataSource *testutil.MockDataSource, rightDataSource *testutil.MockDataSource) {
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, 6400000)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)

	leftDataSource.PrepareChunks()
	rightDataSource.PrepareChunks()
	hashJoinExec := buildHashJoinV2Exec(info)
	result := getSortedResults(t, hashJoinExec, retTypes)
	require.False(t, hashJoinExec.spillHelper.isSpillTriggedInBuildingStageForTest())
	require.False(t, hashJoinExec.spillHelper.areAllPartitionsSpilledForTest())
	require.False(t, hashJoinExec.spillHelper.isRespillTriggeredForTest())
	require.True(t, hashJoinExec.spillHelper.isSpillTriggeredBeforeBuildingHashTableForTest())
	checkResults(t, retTypes, result, expectedResult)
}

func testInnerJoinSpillCase4(t *testing.T, ctx *mock.Context, expectedResult []chunk.Row, info *hashJoinInfo, retTypes []*types.FieldType, leftDataSource *testutil.MockDataSource, rightDataSource *testutil.MockDataSource) {
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, 1500000)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)

	leftDataSource.PrepareChunks()
	rightDataSource.PrepareChunks()
	hashJoinExec := buildHashJoinV2Exec(info)
	result := getSortedResults(t, hashJoinExec, retTypes)
	require.True(t, hashJoinExec.spillHelper.isSpillTriggedInBuildingStageForTest())
	require.True(t, hashJoinExec.spillHelper.areAllPartitionsSpilledForTest())
	require.True(t, hashJoinExec.spillHelper.isRespillTriggeredForTest())
	require.True(t, hashJoinExec.spillHelper.isSpillTriggeredBeforeBuildingHashTableForTest())
	checkResults(t, retTypes, result, expectedResult)
}

func testInnerJoinSpillCase5(t *testing.T, ctx *mock.Context, info *hashJoinInfo, leftDataSource *testutil.MockDataSource, rightDataSource *testutil.MockDataSource) {
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, 10000)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)

	leftDataSource.PrepareChunks()
	rightDataSource.PrepareChunks()
	hashJoinExec := buildHashJoinV2Exec(info)
	err := executeHashJoinExecAndGetError(t, hashJoinExec)
	require.Equal(t, exceedMaxSpillRoundErrInfo, err.Error())
}

func testUnderApplyExec(t *testing.T, ctx *mock.Context, expectedResult []chunk.Row, info *hashJoinInfo, retTypes []*types.FieldType, leftDataSource *testutil.MockDataSource, rightDataSource *testutil.MockDataSource) {
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, 4000000)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)

	hashJoinExec := buildHashJoinV2Exec(info)
	for i := 0; i < 5; i++ {
		leftDataSource.PrepareChunks()
		rightDataSource.PrepareChunks()
		result := getSortedResults(t, hashJoinExec, retTypes)
		require.True(t, hashJoinExec.spillHelper.isSpillTriggedInBuildingStageForTest())
		require.False(t, hashJoinExec.spillHelper.areAllPartitionsSpilledForTest())
		require.False(t, hashJoinExec.spillHelper.isRespillTriggeredForTest())
		checkResults(t, retTypes, result, expectedResult)
	}
}

// Case 1: Trigger spill during the building of row table and spill partial partitions
// Case 2: Trigger spill during the building of row table and spill all partitions
// Case 3: Trigger spill before creating hash table when row table has been built and spill partial partitions
// Case 4: Trigger re-spill
// Case 5: Trigger re-spill and exceed max spill round
func TestInnerJoinSpillCorrectness(t *testing.T) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = 32 
	ctx.GetSessionVars().MaxChunkSize = 32
	leftDataSource, rightDataSource := buildLeftAndRightDataSource(ctx, leftCols, rightCols)

	retTypes := []*types.FieldType{
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeVarString),
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeVarString),
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeLonglong),
	}

	info := &hashJoinInfo{
		ctx:              ctx,
		schema:           buildSchema(retTypes),
		leftExec:         leftDataSource,
		rightExec:        rightDataSource,
		joinType:         plannercore.InnerJoin,
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

	expectedResult := getExpectedResults(t, info, retTypes, leftDataSource, rightDataSource)

	log.Info("xzxdebug ^^^^^^^^^^") // TODO remove it

	maxRowTableSegmentSize = 100
	spillChunkSize = 100

	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/join/slowWorkers", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/executor/join/slowWorkers")

	for i := 0; i < 3; i++ {
		testInnerJoinSpillCase1(t, ctx, expectedResult, info, retTypes, leftDataSource, rightDataSource)
		testInnerJoinSpillCase2(t, ctx, expectedResult, info, retTypes, leftDataSource, rightDataSource)
		testInnerJoinSpillCase3(t, ctx, expectedResult, info, retTypes, leftDataSource, rightDataSource)
		testInnerJoinSpillCase4(t, ctx, expectedResult, info, retTypes, leftDataSource, rightDataSource)
		testInnerJoinSpillCase5(t, ctx, info, leftDataSource, rightDataSource)
	}

	testUnderApplyExec(t, ctx, expectedResult, info, retTypes, leftDataSource, rightDataSource)
}

func TestLeftOuterJoinSpillCorrectness(t *testing.T) {
	// TODO trigger spill in different stages
}
