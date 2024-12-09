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
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func testRandomFail(t *testing.T, ctx *mock.Context, joinType logicalop.JoinType, param spillTestParam, leftDataSource *testutil.MockDataSource, rightDataSource *testutil.MockDataSource) {
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, 1500000)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)

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

	leftDataSource.PrepareChunks()
	rightDataSource.PrepareChunks()
	hashJoinExec := buildHashJoinV2Exec(info)
	executeHashJoinExecForRandomFailTest(t, hashJoinExec)
}

func TestOuterJoinSpillBasic(t *testing.T) {
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

	joinTypes := make([]logicalop.JoinType, 0)
	joinTypes = append(joinTypes, logicalop.LeftOuterJoin)
	joinTypes = append(joinTypes, logicalop.RightOuterJoin)

	for _, joinType := range joinTypes {
		for _, param := range params {
			testSpill(t, ctx, joinType, leftDataSource, rightDataSource, param)
		}
	}
}

func TestOuterJoinSpillWithSel(t *testing.T) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 32
	leftDataSource, rightDataSource := buildLeftAndRightDataSource(ctx, leftCols, rightCols, true)

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
		{true, leftKeys, rightKeys, leftTypes, rightTypes, []int{0, 1, 3, 4}, []int{0, 2, 3, 4}, nil, nil, nil, []int64{2000000, 1000000, 3000000, 200000, 10000}},
		{false, leftKeys, rightKeys, leftTypes, rightTypes, []int{0, 1, 3, 4}, []int{0, 2, 3, 4}, nil, nil, nil, []int64{2000000, 1000000, 3000000, 200000, 10000}},
	}

	err := failpoint.Enable("github.com/pingcap/tidb/pkg/executor/join/slowWorkers", `return(true)`)
	require.NoError(t, err)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/executor/join/slowWorkers")

	maxRowTableSegmentSize = 100
	spillChunkSize = 100

	joinTypes := make([]logicalop.JoinType, 0)
	joinTypes = append(joinTypes, logicalop.LeftOuterJoin)
	joinTypes = append(joinTypes, logicalop.RightOuterJoin)

	for _, joinType := range joinTypes {
		for _, param := range params {
			testSpill(t, ctx, joinType, leftDataSource, rightDataSource, param)
		}
	}
}

func TestOuterJoinSpillWithOtherCondition(t *testing.T) {
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

	joinTypes := make([]logicalop.JoinType, 0)
	joinTypes = append(joinTypes, logicalop.LeftOuterJoin)
	joinTypes = append(joinTypes, logicalop.RightOuterJoin)

	for _, joinType := range joinTypes {
		for _, param := range params {
			testSpill(t, ctx, joinType, leftDataSource, rightDataSource, param)
		}
	}
}

// Hash join executor may be repeatedly closed and opened
func TestOuterJoinUnderApplyExec(t *testing.T) {
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

	joinTypes := make([]logicalop.JoinType, 0)
	joinTypes = append(joinTypes, logicalop.LeftOuterJoin)
	joinTypes = append(joinTypes, logicalop.RightOuterJoin)

	for _, joinType := range joinTypes {
		info.joinType = joinType
		expectedResult := getExpectedResults(t, ctx, info, retTypes, leftDataSource, rightDataSource)
		testUnderApplyExec(t, ctx, expectedResult, info, retTypes, leftDataSource, rightDataSource)
	}
}

func TestFallBackAction(t *testing.T) {
	hardLimitBytesNum := int64(5000000)
	newRootExceedAction := new(testutil.MockActionOnExceed)

	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 32
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSession, hardLimitBytesNum)
	ctx.GetSessionVars().MemTracker.SetActionOnExceed(newRootExceedAction)
	// Consume lots of memory in advance to help to trigger fallback action.
	ctx.GetSessionVars().MemTracker.Consume(int64(float64(hardLimitBytesNum) * 0.99999))
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)

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

	param := spillTestParam{true, leftKeys, rightKeys, leftTypes, rightTypes, []int{0, 1, 3, 4}, []int{0, 2, 3, 4}, nil, nil, nil, []int64{5000000, 1700000, 6000000, 1500000, 10000}}

	maxRowTableSegmentSize = 100
	spillChunkSize = 100
	joinType := logicalop.InnerJoin

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

	leftDataSource.PrepareChunks()
	rightDataSource.PrepareChunks()
	hashJoinExec := buildHashJoinV2Exec(info)
	_ = executeHashJoinExec(t, hashJoinExec)
	require.Less(t, 0, newRootExceedAction.GetTriggeredNum())
}

func TestHashJoinRandomFail(t *testing.T) {
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
		{true, leftKeys, rightKeys, leftTypes, rightTypes, []int{0, 1, 3, 4}, []int{0, 2, 3, 4}, nil, nil, nil, nil},
		{false, leftKeys, rightKeys, leftTypes, rightTypes, []int{0, 1, 3, 4}, []int{0, 2, 3, 4}, nil, nil, nil, nil},
	}

	err := failpoint.Enable("github.com/pingcap/tidb/pkg/executor/join/slowWorkers", `return(true)`)
	require.NoError(t, err)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/executor/join/slowWorkers")

	err = failpoint.Enable("github.com/pingcap/tidb/pkg/executor/join/panicOrError", `return(true)`)
	require.NoError(t, err)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/executor/join/panicOrError")

	maxRowTableSegmentSize = 100
	spillChunkSize = 100

	joinTypes := make([]logicalop.JoinType, 0)
	joinTypes = append(joinTypes, logicalop.InnerJoin)
	joinTypes = append(joinTypes, logicalop.LeftOuterJoin)
	joinTypes = append(joinTypes, logicalop.RightOuterJoin)

	for i := 0; i < 30; i++ {
		for _, joinType := range joinTypes {
			for _, param := range params {
				testRandomFail(t, ctx, joinType, param, leftDataSource, rightDataSource)
			}
		}
	}
}
