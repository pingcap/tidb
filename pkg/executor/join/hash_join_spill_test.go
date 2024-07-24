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

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/executor/internal/testutil"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
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
	return sortRows(results, resultTypes)
}

func spillOnlyTriggeredInBuildStageCase(t *testing.T, ctx *mock.Context, expectedResult []chunk.Row, info *hashJoinInfo, retTypes []*types.FieldType, leftDataSource *testutil.MockDataSource, rightDataSource *testutil.MockDataSource) {
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, 60000)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)

	leftDataSource.PrepareChunks()
	rightDataSource.PrepareChunks()
	hashJoinExec := buildHashJoinV2Exec(info)
	result := getSortedResults(t, hashJoinExec, retTypes)
	checkResults(t, retTypes, result, expectedResult)
}

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

	log.Info("xzxdebug -----------------------") // TODO remove it

	maxRowTableSegmentSize = 100
	spillChunkSize = 100

	spillOnlyTriggeredInBuildStageCase(t, ctx, expectedResult, info, retTypes, leftDataSource, rightDataSource)

	// TODO trigger spill only in build stage
	// TODO trigger spill in build stage and retrigger in restore stage
}

func TestLeftOuterJoinSpillCorrectness(t *testing.T) {
	// TODO trigger spill in different stages
}
