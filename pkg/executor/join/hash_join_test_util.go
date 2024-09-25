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
	"math/rand"
	"sort"
	"strconv"
	"testing"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/testutil"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

type hashJoinInfo struct {
	ctx                   sessionctx.Context
	schema                *expression.Schema
	leftExec, rightExec   exec.Executor
	joinType              logicalop.JoinType
	rightAsBuildSide      bool
	buildKeys             []*expression.Column
	probeKeys             []*expression.Column
	conditions            expression.CNFExprs
	lUsed                 []int
	rUsed                 []int
	otherCondition        expression.CNFExprs
	lUsedInOtherCondition []int
	rUsedInOtherCondition []int
	equalConditions       []*expression.ScalarFunction
}

func buildHashJoinV2Exec(info *hashJoinInfo) *HashJoinV2Exec {
	concurrency := 3
	e := &HashJoinV2Exec{
		BaseExecutor:          exec.NewBaseExecutor(info.ctx, info.schema, 0, info.leftExec, info.rightExec),
		ProbeSideTupleFetcher: &ProbeSideTupleFetcherV2{},
		ProbeWorkers:          make([]*ProbeWorkerV2, concurrency),
		BuildWorkers:          make([]*BuildWorkerV2, concurrency),
		HashJoinCtxV2: &HashJoinCtxV2{
			OtherCondition:  info.otherCondition,
			partitionNumber: 4,
		},
	}
	e.HashJoinCtxV2.SessCtx = info.ctx
	e.HashJoinCtxV2.JoinType = info.joinType
	e.HashJoinCtxV2.Concurrency = uint(concurrency)
	e.HashJoinCtxV2.BuildFilter = info.conditions
	e.HashJoinCtxV2.SetupPartitionInfo()
	e.ChunkAllocPool = e.AllocPool
	e.RightAsBuildSide = info.rightAsBuildSide

	lhsTypes, rhsTypes := exec.RetTypes(info.leftExec), exec.RetTypes(info.rightExec)
	joinedTypes := make([]*types.FieldType, 0, len(lhsTypes)+len(rhsTypes))
	joinedTypes = append(joinedTypes, lhsTypes...)
	joinedTypes = append(joinedTypes, rhsTypes...)

	var buildSideExec exec.Executor
	if e.RightAsBuildSide {
		buildSideExec = info.rightExec
		e.ProbeSideTupleFetcher.ProbeSideExec = info.leftExec
	} else {
		buildSideExec = info.leftExec
		e.ProbeSideTupleFetcher.ProbeSideExec = info.rightExec
	}

	probeKeyColIdx := make([]int, len(info.probeKeys))
	buildKeyColIdx := make([]int, len(info.buildKeys))
	for i := range info.buildKeys {
		buildKeyColIdx[i] = info.buildKeys[i].Index
	}
	for i := range info.probeKeys {
		probeKeyColIdx[i] = info.probeKeys[i].Index
	}

	e.LUsed = info.lUsed
	e.RUsed = info.rUsed
	e.LUsedInOtherCondition = info.lUsedInOtherCondition
	e.RUsedInOtherCondition = info.rUsedInOtherCondition

	var leftJoinKeys, rightJoinKeys []*expression.Column
	if e.RightAsBuildSide {
		rightJoinKeys = info.buildKeys
		leftJoinKeys = info.probeKeys
	} else {
		rightJoinKeys = info.probeKeys
		leftJoinKeys = info.buildKeys
	}
	leftExecTypes, rightExecTypes := exec.RetTypes(info.leftExec), exec.RetTypes(info.rightExec)
	leftTypes, rightTypes := make([]*types.FieldType, 0, len(leftJoinKeys)), make([]*types.FieldType, 0, len(rightJoinKeys))
	for i, col := range leftJoinKeys {
		leftTypes = append(leftTypes, leftExecTypes[col.Index].Clone())
		leftTypes[i].SetFlag(col.RetType.GetFlag())
	}

	for i, col := range rightJoinKeys {
		rightTypes = append(rightTypes, rightExecTypes[col.Index].Clone())
		rightTypes[i].SetFlag(col.RetType.GetFlag())
	}

	for i := range info.equalConditions {
		chs, coll := info.equalConditions[i].CharsetAndCollation()
		leftTypes[i].SetCharset(chs)
		leftTypes[i].SetCollate(coll)
		rightTypes[i].SetCharset(chs)
		rightTypes[i].SetCollate(coll)
	}

	if e.RightAsBuildSide {
		e.BuildKeyTypes, e.ProbeKeyTypes = rightTypes, leftTypes
	} else {
		e.BuildKeyTypes, e.ProbeKeyTypes = leftTypes, rightTypes
	}

	for i := uint(0); i < uint(concurrency); i++ {
		e.ProbeWorkers[i] = &ProbeWorkerV2{
			HashJoinCtx: e.HashJoinCtxV2,
			JoinProbe:   NewJoinProbe(e.HashJoinCtxV2, i, info.joinType, probeKeyColIdx, joinedTypes, e.ProbeKeyTypes, e.RightAsBuildSide),
		}
		e.ProbeWorkers[i].WorkerID = i
		e.BuildWorkers[i] = NewJoinBuildWorkerV2(e.HashJoinCtxV2, i, buildSideExec, buildKeyColIdx, exec.RetTypes(buildSideExec))
	}

	return e
}

func buildDataSource(sortCase *testutil.SortCase, schema *expression.Schema) *testutil.MockDataSource {
	opt := testutil.MockDataSourceParameters{
		DataSchema: schema,
		Rows:       sortCase.Rows,
		Ctx:        sortCase.Ctx,
		Ndvs:       sortCase.Ndvs,
	}
	return testutil.BuildMockDataSource(opt)
}

func generateCMPFunc(fieldTypes []*types.FieldType) func(chunk.Row, chunk.Row) int {
	cmpFuncs := make([]chunk.CompareFunc, 0, len(fieldTypes))
	for _, colType := range fieldTypes {
		cmpFuncs = append(cmpFuncs, chunk.GetCompareFunc(colType))
	}

	cmp := func(rowI, rowJ chunk.Row) int {
		for i, cmpFunc := range cmpFuncs {
			cmp := cmpFunc(rowI, i, rowJ, i)
			if cmp != 0 {
				return cmp
			}
		}
		return 0
	}

	return cmp
}

func sortRows(chunks []*chunk.Chunk, fieldTypes []*types.FieldType) []chunk.Row {
	cmp := generateCMPFunc(fieldTypes)

	rowNum := 0
	for _, chk := range chunks {
		rowNum += chk.NumRows()
	}

	rows := make([]chunk.Row, 0, rowNum)
	for _, chk := range chunks {
		iter := chunk.NewIterator4Chunk(chk)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			rows = append(rows, row)
		}
	}

	sort.Slice(rows, func(i, j int) bool {
		return cmp(rows[i], rows[j]) < 0
	})

	return rows
}

func buildJoinKeyIntDatums(num int) []any {
	datumSet := make(map[int64]bool, num)
	datums := make([]any, 0, num)
	for len(datums) < num {
		val := rand.Int63n(100000)
		if datumSet[val] {
			continue
		}
		datumSet[val] = true
		datums = append(datums, val)
	}
	return datums
}

func getRandString() string {
	b := make([]byte, 10)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func buildJoinKeyStringDatums(num int) []any {
	datumSet := make(map[string]bool, num)
	datums := make([]any, 0, num)
	for len(datums) < num {
		val := getRandString()
		if datumSet[val] {
			continue
		}
		datumSet[val] = true
		datums = append(datums, val)
	}
	return datums
}

func buildLeftAndRightDataSource(ctx sessionctx.Context, leftCols []*expression.Column, rightCols []*expression.Column, hasSel bool) (*testutil.MockDataSource, *testutil.MockDataSource) {
	leftSchema := expression.NewSchema(leftCols...)
	rightSchema := expression.NewSchema(rightCols...)

	joinKeyIntDatums := buildJoinKeyIntDatums(20000)
	joinKeyStringDatums := buildJoinKeyStringDatums(2)
	leftMockSrcParm := testutil.MockDataSourceParameters{DataSchema: leftSchema, Ctx: ctx, Rows: 50000, Ndvs: []int{0, -1, 0, -1, 0}, Datums: [][]any{nil, joinKeyIntDatums, nil, joinKeyStringDatums, nil}, HasSel: hasSel}
	rightMockSrcParm := testutil.MockDataSourceParameters{DataSchema: rightSchema, Ctx: ctx, Rows: 50000, Ndvs: []int{-1, 0, -1, 0, 0}, Datums: [][]any{joinKeyIntDatums, nil, joinKeyStringDatums, nil, nil}, HasSel: hasSel}
	return testutil.BuildMockDataSource(leftMockSrcParm), testutil.BuildMockDataSource(rightMockSrcParm)
}

func buildSchema(schemaTypes []*types.FieldType) *expression.Schema {
	schema := &expression.Schema{}
	for _, tp := range schemaTypes {
		schema.Append(&expression.Column{
			RetType: tp,
		})
	}
	return schema
}

func executeHashJoinExec(t *testing.T, hashJoinExec *HashJoinV2Exec) []*chunk.Chunk {
	tmpCtx := context.Background()
	hashJoinExec.isMemoryClearedForTest = true
	err := hashJoinExec.Open(tmpCtx)
	require.NoError(t, err)
	results := make([]*chunk.Chunk, 0)
	chk := exec.NewFirstChunk(hashJoinExec)
	for {
		err = hashJoinExec.Next(tmpCtx, chk)
		require.NoError(t, err)
		if chk.NumRows() == 0 {
			break
		}
		results = append(results, chk)
		chk = exec.NewFirstChunk(hashJoinExec)
	}
	err = hashJoinExec.Close()
	require.NoError(t, err)
	return results
}

func executeHashJoinExecAndGetError(t *testing.T, hashJoinExec *HashJoinV2Exec) error {
	tmpCtx := context.Background()
	err := hashJoinExec.Open(tmpCtx)
	require.NoError(t, err)
	chk := exec.NewFirstChunk(hashJoinExec)
	for {
		err = hashJoinExec.Next(tmpCtx, chk)
		if err != nil {
			break
		}

		if chk.NumRows() == 0 {
			break
		}
		chk.Reset()
	}
	require.NoError(t, hashJoinExec.Close())
	return err
}

func executeHashJoinExecForRandomFailTest(t *testing.T, hashJoinExec *HashJoinV2Exec) {
	tmpCtx := context.Background()
	err := hashJoinExec.Open(tmpCtx)
	require.NoError(t, err)
	chk := exec.NewFirstChunk(hashJoinExec)
	for {
		err = hashJoinExec.Next(tmpCtx, chk)
		if err != nil {
			break
		}

		if chk.NumRows() == 0 {
			break
		}
		chk.Reset()
	}
	_ = hashJoinExec.Close()
}

func getSortedResults(t *testing.T, hashJoinExec *HashJoinV2Exec, resultTypes []*types.FieldType) []chunk.Row {
	results := executeHashJoinExec(t, hashJoinExec)
	return sortRows(results, resultTypes)
}

func checkResults(t *testing.T, fieldTypes []*types.FieldType, actualResult []chunk.Row, expectedResult []chunk.Row) {
	require.Equal(t, len(expectedResult), len(actualResult))
	cmp := generateCMPFunc(fieldTypes)

	for i := 0; i < len(actualResult); i++ {
		x := cmp(actualResult[i], expectedResult[i])
		require.Equal(t, 0, x, "result index = "+strconv.Itoa(i))
	}
}
