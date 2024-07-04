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
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/testutil"
	"github.com/pingcap/tidb/pkg/expression"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
)

type hashJoinInfo struct {
	ctx                   sessionctx.Context
	schema                *expression.Schema
	leftExec, rightExec   exec.Executor
	joinType              plannercore.JoinType
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
			PartitionNumber: concurrency,
		},
	}
	e.HashJoinCtxV2.SessCtx = info.ctx
	e.HashJoinCtxV2.JoinType = info.joinType
	e.HashJoinCtxV2.Concurrency = uint(concurrency)
	e.HashJoinCtxV2.BuildFilter = info.conditions
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
			JoinProbe:   NewJoinProbe(e.HashJoinCtxV2, i, info.joinType, probeKeyColIdx, joinedTypes, e.ProbeKeyTypes, e.RightAsBuildSide, e.ProbeSideTupleFetcher.ProbeSideExec.RetFieldTypes()),
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
