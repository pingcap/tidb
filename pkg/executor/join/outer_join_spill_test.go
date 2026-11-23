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
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/testutil"
	"github.com/pingcap/tidb/pkg/executor/internal/util"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func prepareSimpleHashJoinEnv(fileNamePrefixForTest string) (*testutil.MockDataSource, *testutil.MockDataSource, *hashJoinInfo, *testutil.MockActionOnExceed) {
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

	param := spillTestParam{true, leftKeys, rightKeys, leftTypes, rightTypes, []int{0, 1, 3, 4}, []int{0, 2, 3, 4}, nil, nil, nil, []int64{5000000, 1700000, 6000000, 1500000, 10000}, fileNamePrefixForTest}

	spillChunkSize = 100
	joinType := base.InnerJoin

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
		fileNamePrefixForTest: param.fileNamePrefixForTest,
	}

	return leftDataSource, rightDataSource, info, newRootExceedAction
}

func testRandomFail(t *testing.T, ctx *mock.Context, joinType base.JoinType, param spillTestParam, leftDataSource *testutil.MockDataSource, rightDataSource *testutil.MockDataSource) {
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
		fileNamePrefixForTest: param.fileNamePrefixForTest,
	}

	leftDataSource.PrepareChunks()
	rightDataSource.PrepareChunks()
	hashJoinExec := buildHashJoinV2Exec(info)
	executeHashJoinExecForRandomFailTest(t, hashJoinExec)
}

func TestOuterJoinSpillBasic1(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempStoragePath = t.TempDir()
	})
	testFuncName := util.GetFunctionName()

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
		{true, leftKeys, rightKeys, leftTypes, rightTypes, []int{0, 1, 3, 4}, []int{0, 2, 3, 4}, nil, nil, nil, []int64{3000000, 2000000, 5000000, 400000, 10000}, testFuncName},
		{false, leftKeys, rightKeys, leftTypes, rightTypes, []int{0, 1, 3, 4}, []int{0, 2, 3, 4}, nil, nil, nil, []int64{3000000, 2000000, 5000000, 400000, 10000}, testFuncName},
		// rightUsed is empty
		{true, leftKeys, rightKeys, leftTypes, rightTypes, []int{0, 1, 3, 4}, []int{}, nil, nil, nil, []int64{2000000, 2000000, 3300000, 200000, 10000}, testFuncName},
		{false, leftKeys, rightKeys, leftTypes, rightTypes, []int{0, 1, 3, 4}, []int{}, nil, nil, nil, []int64{3000000, 2000000, 5300000, 400000, 10000}, testFuncName},
		// leftUsed is empty
		{true, leftKeys, rightKeys, leftTypes, rightTypes, []int{}, []int{0, 2, 3, 4}, nil, nil, nil, []int64{3000000, 2000000, 5000000, 400000, 10000}, testFuncName},
		{false, leftKeys, rightKeys, leftTypes, rightTypes, []int{}, []int{0, 2, 3, 4}, nil, nil, nil, []int64{2000000, 2000000, 3300000, 200000, 10000}, testFuncName},
	}

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/join/slowWorkers", "return(true)")

	spillChunkSize = 100

	for _, param := range params {
		testSpill(t, ctx, base.LeftOuterJoin, leftDataSource, rightDataSource, param)
	}

	util.CheckNoLeakFiles(t, testFuncName)
}

func TestOuterJoinSpillBasic2(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempStoragePath = t.TempDir()
	})
	testFuncName := util.GetFunctionName()

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
		{true, leftKeys, rightKeys, leftTypes, rightTypes, []int{0, 1, 3, 4}, []int{0, 2, 3, 4}, nil, nil, nil, []int64{3000000, 2000000, 5000000, 400000, 10000}, testFuncName},
		{false, leftKeys, rightKeys, leftTypes, rightTypes, []int{0, 1, 3, 4}, []int{0, 2, 3, 4}, nil, nil, nil, []int64{3000000, 2000000, 5000000, 400000, 10000}, testFuncName},
		// rightUsed is empty
		{true, leftKeys, rightKeys, leftTypes, rightTypes, []int{0, 1, 3, 4}, []int{}, nil, nil, nil, []int64{2000000, 2000000, 3300000, 200000, 10000}, testFuncName},
		{false, leftKeys, rightKeys, leftTypes, rightTypes, []int{0, 1, 3, 4}, []int{}, nil, nil, nil, []int64{3000000, 2000000, 5300000, 400000, 10000}, testFuncName},
		// leftUsed is empty
		{true, leftKeys, rightKeys, leftTypes, rightTypes, []int{}, []int{0, 2, 3, 4}, nil, nil, nil, []int64{3000000, 2000000, 5000000, 400000, 10000}, testFuncName},
		{false, leftKeys, rightKeys, leftTypes, rightTypes, []int{}, []int{0, 2, 3, 4}, nil, nil, nil, []int64{2000000, 2000000, 3300000, 200000, 10000}, testFuncName},
	}

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/join/slowWorkers", "return(true)")

	spillChunkSize = 100

	for _, param := range params {
		testSpill(t, ctx, base.RightOuterJoin, leftDataSource, rightDataSource, param)
	}

	util.CheckNoLeakFiles(t, testFuncName)
}

func TestOuterJoinSpillWithSel(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempStoragePath = t.TempDir()
	})
	testFuncName := util.GetFunctionName()

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
		{true, leftKeys, rightKeys, leftTypes, rightTypes, []int{0, 1, 3, 4}, []int{0, 2, 3, 4}, nil, nil, nil, []int64{2000000, 1000000, 2500000, 200000, 10000}, testFuncName},
		{false, leftKeys, rightKeys, leftTypes, rightTypes, []int{0, 1, 3, 4}, []int{0, 2, 3, 4}, nil, nil, nil, []int64{2000000, 1000000, 2500000, 200000, 10000}, testFuncName},
	}

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/join/slowWorkers", "return(true)")

	spillChunkSize = 100

	joinTypes := make([]base.JoinType, 0)
	joinTypes = append(joinTypes, base.LeftOuterJoin)
	joinTypes = append(joinTypes, base.RightOuterJoin)

	for _, joinType := range joinTypes {
		for _, param := range params {
			testSpill(t, ctx, joinType, leftDataSource, rightDataSource, param)
		}
	}
	util.CheckNoLeakFiles(t, testFuncName)
}

func TestOuterJoinSpillWithOtherCondition(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempStoragePath = t.TempDir()
	})
	testFuncName := util.GetFunctionName()

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
		{true, leftKeys, rightKeys, leftTypes, rightTypes, []int{0, 1, 3, 4}, []int{0, 2, 3, 4}, otherCondition, []int{0}, []int{4}, []int64{3000000, 2000000, 5000000, 400000, 10000}, testFuncName},
		{false, leftKeys, rightKeys, leftTypes, rightTypes, []int{0, 1, 3, 4}, []int{0, 2, 3, 4}, otherCondition, []int{0}, []int{4}, []int64{3000000, 2000000, 5000000, 400000, 10000}, testFuncName},
	}

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/join/slowWorkers", "return(true)")

	spillChunkSize = 100

	joinTypes := make([]base.JoinType, 0)
	joinTypes = append(joinTypes, base.LeftOuterJoin)
	joinTypes = append(joinTypes, base.RightOuterJoin)

	for _, joinType := range joinTypes {
		for _, param := range params {
			testSpill(t, ctx, joinType, leftDataSource, rightDataSource, param)
		}
	}
	util.CheckNoLeakFiles(t, testFuncName)
}

// Hash join executor may be repeatedly closed and opened
func TestOuterJoinUnderApplyExec(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempStoragePath = t.TempDir()
	})
	testFuncName := util.GetFunctionName()

	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 32
	ctx.GetSessionVars().StmtCtx.RuntimeStatsColl = execdetails.NewRuntimeStatsColl(nil)
	leftDataSource, rightDataSource := buildLeftAndRightDataSource(ctx, leftCols, rightCols, false)

	info := &hashJoinInfo{
		ctx:              ctx,
		schema:           buildSchema(retTypes),
		planID:           1,
		leftExec:         leftDataSource,
		rightExec:        rightDataSource,
		joinType:         base.InnerJoin,
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
		fileNamePrefixForTest: testFuncName,
	}

	spillChunkSize = 100

	joinTypes := make([]base.JoinType, 0)
	joinTypes = append(joinTypes, base.LeftOuterJoin)
	joinTypes = append(joinTypes, base.RightOuterJoin)

	for _, joinType := range joinTypes {
		info.joinType = joinType
		expectedResult := getExpectedResults(t, ctx, info, retTypes, leftDataSource, rightDataSource)
		testUnderApplyExec(t, ctx, expectedResult, info, retTypes, leftDataSource, rightDataSource)
	}
	util.CheckNoLeakFiles(t, testFuncName)
}

func TestFallBackAction(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempStoragePath = t.TempDir()
	})
	testFuncName := util.GetFunctionName()

	leftDataSource, rightDataSource, info, newRootExceedAction := prepareSimpleHashJoinEnv(testFuncName)

	leftDataSource.PrepareChunks()
	rightDataSource.PrepareChunks()
	hashJoinExec := buildHashJoinV2Exec(info)
	_ = executeHashJoinExec(t, hashJoinExec)
	require.Less(t, 0, newRootExceedAction.GetTriggeredNum())
	util.CheckNoLeakFiles(t, testFuncName)
}

func TestIssue59377(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempStoragePath = t.TempDir()
	})
	testFuncName := util.GetFunctionName()

	leftDataSource, rightDataSource, info, _ := prepareSimpleHashJoinEnv(testFuncName)
	leftDataSource.PrepareChunks()
	rightDataSource.PrepareChunks()
	hashJoinExec := buildHashJoinV2Exec(info)

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/join/Issue59377", "return")

	tmpCtx := context.Background()
	hashJoinExec.isMemoryClearedForTest = true
	err := hashJoinExec.Open(tmpCtx)
	require.NoError(t, err)
	chk := exec.NewFirstChunk(hashJoinExec)
	err = hashJoinExec.Next(tmpCtx, chk)
	require.True(t, err != nil)
	_ = hashJoinExec.Close()
	util.CheckNoLeakFiles(t, testFuncName)
}

func TestHashJoinRandomFail(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempStoragePath = t.TempDir()
	})
	testFuncName := util.GetFunctionName()

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
		{true, leftKeys, rightKeys, leftTypes, rightTypes, []int{0, 1, 3, 4}, []int{0, 2, 3, 4}, nil, nil, nil, nil, testFuncName},
		{false, leftKeys, rightKeys, leftTypes, rightTypes, []int{0, 1, 3, 4}, []int{0, 2, 3, 4}, nil, nil, nil, nil, testFuncName},
	}

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/join/slowWorkers", "return(true)")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/join/panicOrError", "return(true)")

	spillChunkSize = 100

	joinTypes := make([]base.JoinType, 0)
	joinTypes = append(joinTypes, base.InnerJoin)
	joinTypes = append(joinTypes, base.LeftOuterJoin)
	joinTypes = append(joinTypes, base.RightOuterJoin)

	for range 15 {
		for _, joinType := range joinTypes {
			for _, param := range params {
				testRandomFail(t, ctx, joinType, param, leftDataSource, rightDataSource)
			}
		}
	}
	util.CheckNoLeakFiles(t, testFuncName)
}

func buildNullableSpillDataSources(ctx *mock.Context, rows int, hasSel bool) (*testutil.MockDataSource, *testutil.MockDataSource, []*types.FieldType, []*types.FieldType) {
	nullableIntTp := types.NewFieldType(mysql.TypeLonglong)
	nullableStringTp := types.NewFieldType(mysql.TypeVarString)

	leftTypes := []*types.FieldType{nullableIntTp, nullableIntTp, nullableIntTp, nullableStringTp, nullableIntTp}
	rightTypes := []*types.FieldType{nullableIntTp, nullableIntTp, nullableStringTp, nullableIntTp, nullableIntTp}

	nullableLeftCols := []*expression.Column{
		{Index: 0, RetType: nullableIntTp},
		{Index: 1, RetType: nullableIntTp},
		{Index: 2, RetType: nullableIntTp},
		{Index: 3, RetType: nullableStringTp},
		{Index: 4, RetType: nullableIntTp},
	}
	nullableRightCols := []*expression.Column{
		{Index: 0, RetType: nullableIntTp},
		{Index: 1, RetType: nullableIntTp},
		{Index: 2, RetType: nullableStringTp},
		{Index: 3, RetType: nullableIntTp},
		{Index: 4, RetType: nullableIntTp},
	}

	leftNulls := make([][]bool, 5)
	rightNulls := make([][]bool, 5)
	for col := range 5 {
		leftNulls[col] = make([]bool, rows)
		rightNulls[col] = make([]bool, rows)
	}
	for i := range rows {
		leftNulls[0][i] = i%17 == 0
		leftNulls[1][i] = i%11 == 0
		leftNulls[3][i] = i%13 == 0
		rightNulls[0][i] = i%11 == 0
		rightNulls[2][i] = i%13 == 0
		rightNulls[4][i] = i%19 == 0
	}

	joinKeyIntDatums := buildJoinKeyIntDatums(5000)
	joinKeyStringDatums := buildJoinKeyStringDatums(4)
	leftMockSrcParm := testutil.MockDataSourceParameters{
		DataSchema: expression.NewSchema(nullableLeftCols...),
		Ctx:        ctx,
		Rows:       rows,
		Ndvs:       []int{0, -1, 0, -1, 0},
		Datums:     [][]any{nil, joinKeyIntDatums, nil, joinKeyStringDatums, nil},
		Nulls:      leftNulls,
		HasSel:     hasSel,
	}
	rightMockSrcParm := testutil.MockDataSourceParameters{
		DataSchema: expression.NewSchema(nullableRightCols...),
		Ctx:        ctx,
		Rows:       rows,
		Ndvs:       []int{-1, 0, -1, 0, 0},
		Datums:     [][]any{joinKeyIntDatums, nil, joinKeyStringDatums, nil, nil},
		Nulls:      rightNulls,
		HasSel:     hasSel,
	}
	return testutil.BuildMockDataSource(leftMockSrcParm), testutil.BuildMockDataSource(rightMockSrcParm), leftTypes, rightTypes
}

func TestAllPartitionsSpilledUnmatchedProbeRows(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempStoragePath = t.TempDir()
	})
	testFuncName := util.GetFunctionName()

	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 32

	nullableIntTp := types.NewFieldType(mysql.TypeLonglong)
	nullableStringTp := types.NewFieldType(mysql.TypeVarString)
	tinyTp := types.NewFieldType(mysql.TypeTiny)

	leftKeys := []*expression.Column{
		{Index: 1, RetType: nullableIntTp},
		{Index: 3, RetType: nullableStringTp},
	}
	rightKeys := []*expression.Column{
		{Index: 0, RetType: nullableIntTp},
		{Index: 2, RetType: nullableStringTp},
	}

	// Probe-side filter rejecting rows where probe col > 500000 is false/NULL.
	thresholdConst := &expression.Constant{Value: types.NewIntDatum(500000), RetType: nullableIntTp}
	leftProbeFilterFn, err := expression.NewFunction(mock.NewContext(), ast.GT, tinyTp, &expression.Column{Index: 0, RetType: nullableIntTp}, thresholdConst)
	require.NoError(t, err)
	leftProbeFilter := expression.CNFExprs{leftProbeFilterFn}

	rightProbeFilterFn, err := expression.NewFunction(mock.NewContext(), ast.GT, tinyTp, &expression.Column{Index: 4, RetType: nullableIntTp}, thresholdConst)
	require.NoError(t, err)
	rightProbeFilter := expression.CNFExprs{rightProbeFilterFn}

	otherCondFn, err := expression.NewFunction(mock.NewContext(), ast.GT, tinyTp, &expression.Column{Index: 0, RetType: nullableIntTp}, &expression.Column{Index: 9, RetType: nullableIntTp})
	require.NoError(t, err)
	otherCond := expression.CNFExprs{otherCondFn}

	semiOtherCondFn, err := expression.NewFunction(mock.NewContext(), ast.GT, tinyTp, &expression.Column{Index: 0, RetType: nullableIntTp}, &expression.Column{Index: 8, RetType: nullableIntTp})
	require.NoError(t, err)
	semiOtherCond := expression.CNFExprs{semiOtherCondFn}

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/join/slowWorkers", "return(true)")
	spillChunkSize = 64

	type subCase struct {
		name                  string
		joinType              base.JoinType
		rightAsBuildSide      bool
		leftUsed              []int
		rightUsed             []int
		probeFilter           expression.CNFExprs
		otherCondition        expression.CNFExprs
		lUsedInOtherCondition []int
		rUsedInOtherCondition []int
		hasSel                bool
	}

	cases := []subCase{
		// 1. LeftOuterJoin with right as build side (probe is outer side)
		{name: "left_outer_null_keys", joinType: base.LeftOuterJoin, rightAsBuildSide: true, leftUsed: []int{0, 1, 3, 4}, rightUsed: []int{0, 2, 3, 4}},
		{name: "left_outer_probe_filter", joinType: base.LeftOuterJoin, rightAsBuildSide: true, leftUsed: []int{0, 1, 3, 4}, rightUsed: []int{0, 2, 3, 4}, probeFilter: leftProbeFilter},
		{name: "left_outer_other_cond_and_sel", joinType: base.LeftOuterJoin, rightAsBuildSide: true, leftUsed: []int{0, 1, 3, 4}, rightUsed: []int{0, 2, 3, 4}, probeFilter: leftProbeFilter, otherCondition: otherCond, lUsedInOtherCondition: []int{0}, rUsedInOtherCondition: []int{4}, hasSel: true},
		{name: "left_outer_empty_used", joinType: base.LeftOuterJoin, rightAsBuildSide: true, leftUsed: []int{}, rightUsed: []int{}, probeFilter: leftProbeFilter},
		// 2. RightOuterJoin with left as build side (probe is outer side)
		{name: "right_outer_null_and_filter", joinType: base.RightOuterJoin, rightAsBuildSide: false, leftUsed: []int{0, 1, 3, 4}, rightUsed: []int{0, 2, 3, 4}, probeFilter: rightProbeFilter},
		{name: "right_outer_other_cond_and_sel", joinType: base.RightOuterJoin, rightAsBuildSide: false, leftUsed: []int{0, 1, 3, 4}, rightUsed: []int{0, 2, 3, 4}, probeFilter: rightProbeFilter, otherCondition: otherCond, lUsedInOtherCondition: []int{0}, rUsedInOtherCondition: []int{4}, hasSel: true},
		// 3. AntiSemiJoin with right as build side (probe is left side)
		{name: "anti_semi_null_and_filter", joinType: base.AntiSemiJoin, rightAsBuildSide: true, leftUsed: []int{0, 1, 3, 4}, rightUsed: []int{}, probeFilter: leftProbeFilter},
		{name: "anti_semi_other_cond_and_sel", joinType: base.AntiSemiJoin, rightAsBuildSide: true, leftUsed: []int{0, 1, 3, 4}, rightUsed: []int{}, probeFilter: leftProbeFilter, otherCondition: semiOtherCond, lUsedInOtherCondition: []int{0}, rUsedInOtherCondition: []int{3}, hasSel: true},
		{name: "anti_semi_empty_used", joinType: base.AntiSemiJoin, rightAsBuildSide: true, leftUsed: []int{}, rightUsed: []int{}, probeFilter: leftProbeFilter},
		// 4. LeftOuterSemiJoin (right is always build side)
		{name: "left_outer_semi_null_and_filter", joinType: base.LeftOuterSemiJoin, rightAsBuildSide: true, leftUsed: []int{0, 1, 3, 4}, rightUsed: []int{}, probeFilter: leftProbeFilter},
		{name: "left_outer_semi_other_cond_and_sel", joinType: base.LeftOuterSemiJoin, rightAsBuildSide: true, leftUsed: []int{0, 1, 3, 4}, rightUsed: []int{}, probeFilter: leftProbeFilter, otherCondition: semiOtherCond, lUsedInOtherCondition: []int{0}, rUsedInOtherCondition: []int{3}, hasSel: true},
		{name: "left_outer_semi_empty_used", joinType: base.LeftOuterSemiJoin, rightAsBuildSide: true, leftUsed: []int{}, rightUsed: []int{}, probeFilter: leftProbeFilter},
		// 5. AntiLeftOuterSemiJoin (right is always build side)
		{name: "anti_left_outer_semi_null_and_filter", joinType: base.AntiLeftOuterSemiJoin, rightAsBuildSide: true, leftUsed: []int{0, 1, 3, 4}, rightUsed: []int{}, probeFilter: leftProbeFilter},
		{name: "anti_left_outer_semi_other_cond_and_sel", joinType: base.AntiLeftOuterSemiJoin, rightAsBuildSide: true, leftUsed: []int{0, 1, 3, 4}, rightUsed: []int{}, probeFilter: leftProbeFilter, otherCondition: semiOtherCond, lUsedInOtherCondition: []int{0}, rUsedInOtherCondition: []int{3}, hasSel: true},
		{name: "anti_left_outer_semi_empty_used", joinType: base.AntiLeftOuterSemiJoin, rightAsBuildSide: true, leftUsed: []int{}, rightUsed: []int{}, probeFilter: leftProbeFilter},
	}

	leftDSNoSel, rightDSNoSel, leftTypes, rightTypes := buildNullableSpillDataSources(ctx, 12000, false)
	leftDSWithSel, rightDSWithSel, _, _ := buildNullableSpillDataSources(ctx, 12000, true)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			leftDS, rightDS := leftDSNoSel, rightDSNoSel
			if tc.hasSel {
				leftDS, rightDS = leftDSWithSel, rightDSWithSel
			}
			param := spillTestParam{
				rightAsBuildSide:          tc.rightAsBuildSide,
				leftKeys:                  leftKeys,
				rightKeys:                 rightKeys,
				leftTypes:                 leftTypes,
				rightTypes:                rightTypes,
				leftUsed:                  tc.leftUsed,
				rightUsed:                 tc.rightUsed,
				otherCondition:            tc.otherCondition,
				leftUsedByOtherCondition:  tc.lUsedInOtherCondition,
				rightUsedByOtherCondition: tc.rUsedInOtherCondition,
				fileNamePrefixForTest:     testFuncName,
			}
			returnTypes := getReturnTypes(tc.joinType, param)
			buildKeys, probeKeys := param.leftKeys, param.rightKeys
			if param.rightAsBuildSide {
				buildKeys, probeKeys = param.rightKeys, param.leftKeys
			}
			info := &hashJoinInfo{
				ctx:                   ctx,
				schema:                buildSchema(returnTypes),
				leftExec:              leftDS,
				rightExec:             rightDS,
				joinType:              tc.joinType,
				rightAsBuildSide:      param.rightAsBuildSide,
				buildKeys:             buildKeys,
				probeKeys:             probeKeys,
				probeFilter:           tc.probeFilter,
				lUsed:                 param.leftUsed,
				rUsed:                 param.rightUsed,
				otherCondition:        param.otherCondition,
				lUsedInOtherCondition: param.leftUsedByOtherCondition,
				rUsedInOtherCondition: param.rightUsedByOtherCondition,
				fileNamePrefixForTest: param.fileNamePrefixForTest,
			}

			expectedResult := getExpectedResults(t, ctx, info, returnTypes, leftDS, rightDS)

			// Tight memory limit forces all 4 partitions to spill in Round 0.
			memLimit := int64(80000)
			if tc.hasSel || len(tc.rightUsed) == 0 {
				memLimit = 35000
			}
			ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, memLimit)
			ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
			ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)

			leftDS.PrepareChunks()
			rightDS.PrepareChunks()
			hashJoinExec := buildHashJoinV2Exec(info)
			actualResult := getSortedResults(t, hashJoinExec, returnTypes)

			require.True(t, hashJoinExec.isAllMemoryClearedForTest())
			require.True(t, hashJoinExec.spillHelper.areAllPartitionsSpilledForTest())
			checkResults(t, returnTypes, actualResult, expectedResult)
			if !tc.hasSel && len(tc.rightUsed) > 0 {
				require.True(t, hashJoinExec.spillHelper.isRespillTriggeredForTest())
				require.True(t, hashJoinExec.spillHelper.isProbeSkippedInRestoreForTest())
			}
		})
	}
	util.CheckNoLeakFiles(t, testFuncName)
}
