// Copyright 2015 PingCAP, Inc.
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

package executor

import (
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/join"
	executor_metrics "github.com/pingcap/tidb/pkg/executor/metrics"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/joinversion"
)

func (b *executorBuilder) buildMergeJoin(v *physicalop.PhysicalMergeJoin) exec.Executor {
	leftExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}

	rightExec := b.build(v.Children()[1])
	if b.err != nil {
		return nil
	}

	defaultValues := v.DefaultValues
	if defaultValues == nil {
		if v.JoinType == base.RightOuterJoin {
			defaultValues = make([]types.Datum, leftExec.Schema().Len())
		} else {
			defaultValues = make([]types.Datum, rightExec.Schema().Len())
		}
	}

	colsFromChildren := v.Schema().Columns
	if v.JoinType == base.LeftOuterSemiJoin || v.JoinType == base.AntiLeftOuterSemiJoin {
		colsFromChildren = colsFromChildren[:len(colsFromChildren)-1]
	}

	e := &join.MergeJoinExec{
		StmtCtx:      b.ctx.GetSessionVars().StmtCtx,
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), leftExec, rightExec),
		CompareFuncs: v.CompareFuncs,
		Joiner: join.NewJoiner(
			b.ctx,
			v.JoinType,
			v.JoinType == base.RightOuterJoin,
			defaultValues,
			v.OtherConditions,
			exec.RetTypes(leftExec),
			exec.RetTypes(rightExec),
			markChildrenUsedCols(colsFromChildren, v.Children()[0].Schema(), v.Children()[1].Schema()),
			false,
		),
		IsOuterJoin: v.JoinType.IsOuterJoin(),
		Desc:        v.Desc,
	}

	leftTable := &join.MergeJoinTable{
		ChildIndex: 0,
		JoinKeys:   v.LeftJoinKeys,
		Filters:    v.LeftConditions,
	}
	rightTable := &join.MergeJoinTable{
		ChildIndex: 1,
		JoinKeys:   v.RightJoinKeys,
		Filters:    v.RightConditions,
	}

	if v.JoinType == base.RightOuterJoin {
		e.InnerTable = leftTable
		e.OuterTable = rightTable
	} else {
		e.InnerTable = rightTable
		e.OuterTable = leftTable
	}
	e.InnerTable.IsInner = true

	// optimizer should guarantee that filters on inner table are pushed down
	// to tikv or extracted to a Selection.
	if len(e.InnerTable.Filters) != 0 {
		b.err = errors.Annotate(exeerrors.ErrBuildExecutor, "merge join's inner filter should be empty.")
		return nil
	}

	executor_metrics.ExecutorCounterMergeJoinExec.Inc()
	return e
}

func collectColumnIndexFromExpr(expr expression.Expression, leftColumnSize int, leftColumnIndex []int, rightColumnIndex []int) (_, _ []int) {
	switch x := expr.(type) {
	case *expression.Column:
		colIndex := x.Index
		if colIndex >= leftColumnSize {
			rightColumnIndex = append(rightColumnIndex, colIndex-leftColumnSize)
		} else {
			leftColumnIndex = append(leftColumnIndex, colIndex)
		}
		return leftColumnIndex, rightColumnIndex
	case *expression.Constant, *expression.CorrelatedColumn:
		// correlatedColumn can be treated as constant during runtime
		return leftColumnIndex, rightColumnIndex
	case *expression.ScalarFunction:
		for _, arg := range x.GetArgs() {
			leftColumnIndex, rightColumnIndex = collectColumnIndexFromExpr(arg, leftColumnSize, leftColumnIndex, rightColumnIndex)
		}
		return leftColumnIndex, rightColumnIndex
	default:
		// Gracefully handle unknown expression types by returning current indices unchanged.
		return leftColumnIndex, rightColumnIndex
	}
}

func extractUsedColumnsInJoinOtherCondition(expr expression.CNFExprs, leftColumnSize int) (leftColumnIndex, rightColumnIndex []int) {
	leftColumnIndex = make([]int, 0, 1)
	rightColumnIndex = make([]int, 0, 1)
	for _, subExpr := range expr {
		leftColumnIndex, rightColumnIndex = collectColumnIndexFromExpr(subExpr, leftColumnSize, leftColumnIndex, rightColumnIndex)
	}
	return leftColumnIndex, rightColumnIndex
}

func (b *executorBuilder) buildHashJoinV2(v *physicalop.PhysicalHashJoin) exec.Executor {
	leftExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}

	rightExec := b.build(v.Children()[1])
	if b.err != nil {
		return nil
	}
	return b.buildHashJoinV2FromChildExecs(leftExec, rightExec, v)
}

func (b *executorBuilder) buildHashJoinV2FromChildExecs(leftExec, rightExec exec.Executor, v *physicalop.PhysicalHashJoin) *join.HashJoinV2Exec {
	joinOtherCondition := v.OtherConditions
	joinLeftCondition := v.LeftConditions
	joinRightCondition := v.RightConditions
	if len(joinOtherCondition) == 0 {
		// sometimes the OtherCondtition could be a not nil slice with length = 0
		// in HashJoinV2, it is assumed that if there is no other condition, e.HashJoinCtxV2.OtherCondition should be nil
		joinOtherCondition = nil
	}
	if len(joinLeftCondition) == 0 {
		joinLeftCondition = nil
	}
	if len(joinRightCondition) == 0 {
		joinRightCondition = nil
	}

	e := &join.HashJoinV2Exec{
		BaseExecutor:          exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), leftExec, rightExec),
		ProbeSideTupleFetcher: &join.ProbeSideTupleFetcherV2{},
		ProbeWorkers:          make([]*join.ProbeWorkerV2, v.Concurrency),
		BuildWorkers:          make([]*join.BuildWorkerV2, v.Concurrency),
		HashJoinCtxV2: &join.HashJoinCtxV2{
			OtherCondition: joinOtherCondition,
		},
		IsGA: physicalop.IsGAForHashJoinV2(v.JoinType, v.LeftJoinKeys, v.IsNullEQ, v.LeftNAJoinKeys),
	}
	e.HashJoinCtxV2.SessCtx = b.ctx
	e.HashJoinCtxV2.JoinType = v.JoinType
	e.HashJoinCtxV2.Concurrency = v.Concurrency
	e.HashJoinCtxV2.SetupPartitionInfo()
	e.ChunkAllocPool = e.AllocPool
	e.HashJoinCtxV2.RightAsBuildSide = true
	if v.InnerChildIdx == 1 && v.UseOuterToBuild {
		e.HashJoinCtxV2.RightAsBuildSide = false
	} else if v.InnerChildIdx == 0 && !v.UseOuterToBuild {
		e.HashJoinCtxV2.RightAsBuildSide = false
	}

	lhsTypes, rhsTypes := exec.RetTypes(leftExec), exec.RetTypes(rightExec)
	joinedTypes := make([]*types.FieldType, 0, len(lhsTypes)+len(rhsTypes))
	joinedTypes = append(joinedTypes, lhsTypes...)
	joinedTypes = append(joinedTypes, rhsTypes...)

	if v.InnerChildIdx == 1 {
		if joinRightCondition != nil {
			b.err = errors.Annotate(exeerrors.ErrBuildExecutor, "join's inner condition should be empty")
			return nil
		}
	} else {
		if joinLeftCondition != nil {
			b.err = errors.Annotate(exeerrors.ErrBuildExecutor, "join's inner condition should be empty")
			return nil
		}
	}

	var probeKeys, buildKeys []*expression.Column
	var buildSideExec exec.Executor
	if v.UseOuterToBuild {
		if v.InnerChildIdx == 1 {
			buildSideExec, buildKeys = leftExec, v.LeftJoinKeys
			e.ProbeSideTupleFetcher.ProbeSideExec, probeKeys = rightExec, v.RightJoinKeys
			e.HashJoinCtxV2.BuildFilter = joinLeftCondition
		} else {
			buildSideExec, buildKeys = rightExec, v.RightJoinKeys
			e.ProbeSideTupleFetcher.ProbeSideExec, probeKeys = leftExec, v.LeftJoinKeys
			e.HashJoinCtxV2.BuildFilter = joinRightCondition
		}
	} else {
		if v.InnerChildIdx == 0 {
			buildSideExec, buildKeys = leftExec, v.LeftJoinKeys
			e.ProbeSideTupleFetcher.ProbeSideExec, probeKeys = rightExec, v.RightJoinKeys
			e.HashJoinCtxV2.ProbeFilter = joinRightCondition
		} else {
			buildSideExec, buildKeys = rightExec, v.RightJoinKeys
			e.ProbeSideTupleFetcher.ProbeSideExec, probeKeys = leftExec, v.LeftJoinKeys
			e.HashJoinCtxV2.ProbeFilter = joinLeftCondition
		}
	}
	probeKeyColIdx := make([]int, len(probeKeys))
	buildKeyColIdx := make([]int, len(buildKeys))
	for i := range buildKeys {
		buildKeyColIdx[i] = buildKeys[i].Index
	}
	for i := range probeKeys {
		probeKeyColIdx[i] = probeKeys[i].Index
	}

	colsFromChildren := v.Schema().Columns
	if v.JoinType == base.LeftOuterSemiJoin || v.JoinType == base.AntiLeftOuterSemiJoin {
		// the matched column is added inside join
		colsFromChildren = colsFromChildren[:len(colsFromChildren)-1]
	}
	childrenUsedSchema := markChildrenUsedCols(colsFromChildren, v.Children()[0].Schema(), v.Children()[1].Schema())
	if childrenUsedSchema == nil {
		b.err = errors.New("children used should never be nil")
		return nil
	}
	e.LUsed = make([]int, 0, len(childrenUsedSchema[0]))
	e.LUsed = append(e.LUsed, childrenUsedSchema[0]...)
	e.RUsed = make([]int, 0, len(childrenUsedSchema[1]))
	e.RUsed = append(e.RUsed, childrenUsedSchema[1]...)
	if joinOtherCondition != nil {
		leftColumnSize := v.Children()[0].Schema().Len()
		e.LUsedInOtherCondition, e.RUsedInOtherCondition = extractUsedColumnsInJoinOtherCondition(joinOtherCondition, leftColumnSize)
	}
	// todo add partition hash join exec
	executor_metrics.ExecutorCountHashJoinExec.Inc()

	leftExecTypes, rightExecTypes := exec.RetTypes(leftExec), exec.RetTypes(rightExec)
	leftTypes, rightTypes := make([]*types.FieldType, 0, len(v.LeftJoinKeys)+len(v.LeftNAJoinKeys)), make([]*types.FieldType, 0, len(v.RightJoinKeys)+len(v.RightNAJoinKeys))
	for i, col := range v.LeftJoinKeys {
		leftTypes = append(leftTypes, leftExecTypes[col.Index].Clone())
		leftTypes[i].SetFlag(col.RetType.GetFlag())
	}
	offset := len(v.LeftJoinKeys)
	for i, col := range v.LeftNAJoinKeys {
		leftTypes = append(leftTypes, leftExecTypes[col.Index].Clone())
		leftTypes[i+offset].SetFlag(col.RetType.GetFlag())
	}
	for i, col := range v.RightJoinKeys {
		rightTypes = append(rightTypes, rightExecTypes[col.Index].Clone())
		rightTypes[i].SetFlag(col.RetType.GetFlag())
	}
	offset = len(v.RightJoinKeys)
	for i, col := range v.RightNAJoinKeys {
		rightTypes = append(rightTypes, rightExecTypes[col.Index].Clone())
		rightTypes[i+offset].SetFlag(col.RetType.GetFlag())
	}

	// consider collations
	for i := range v.EqualConditions {
		chs, coll := v.EqualConditions[i].CharsetAndCollation()
		leftTypes[i].SetCharset(chs)
		leftTypes[i].SetCollate(coll)
		rightTypes[i].SetCharset(chs)
		rightTypes[i].SetCollate(coll)
	}
	offset = len(v.EqualConditions)
	for i := range v.NAEqualConditions {
		chs, coll := v.NAEqualConditions[i].CharsetAndCollation()
		leftTypes[i+offset].SetCharset(chs)
		leftTypes[i+offset].SetCollate(coll)
		rightTypes[i+offset].SetCharset(chs)
		rightTypes[i+offset].SetCollate(coll)
	}
	if e.RightAsBuildSide {
		e.BuildKeyTypes, e.ProbeKeyTypes = rightTypes, leftTypes
	} else {
		e.BuildKeyTypes, e.ProbeKeyTypes = leftTypes, rightTypes
	}
	for i := range e.Concurrency {
		e.ProbeWorkers[i] = &join.ProbeWorkerV2{
			HashJoinCtx: e.HashJoinCtxV2,
			JoinProbe:   join.NewJoinProbe(e.HashJoinCtxV2, i, v.JoinType, probeKeyColIdx, joinedTypes, e.ProbeKeyTypes, e.RightAsBuildSide),
		}
		e.ProbeWorkers[i].WorkerID = i

		e.BuildWorkers[i] = join.NewJoinBuildWorkerV2(e.HashJoinCtxV2, i, buildSideExec, buildKeyColIdx, exec.RetTypes(buildSideExec))
	}
	return e
}

func (b *executorBuilder) buildHashJoin(v *physicalop.PhysicalHashJoin) exec.Executor {
	if b.ctx.GetSessionVars().UseHashJoinV2 && joinversion.IsHashJoinV2Supported() && v.CanUseHashJoinV2() {
		return b.buildHashJoinV2(v)
	}
	leftExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}

	rightExec := b.build(v.Children()[1])
	if b.err != nil {
		return nil
	}
	return b.buildHashJoinFromChildExecs(leftExec, rightExec, v)
}

func (b *executorBuilder) buildHashJoinFromChildExecs(leftExec, rightExec exec.Executor, v *physicalop.PhysicalHashJoin) *join.HashJoinV1Exec {
	e := &join.HashJoinV1Exec{
		BaseExecutor:          exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), leftExec, rightExec),
		ProbeSideTupleFetcher: &join.ProbeSideTupleFetcherV1{},
		ProbeWorkers:          make([]*join.ProbeWorkerV1, v.Concurrency),
		BuildWorker:           &join.BuildWorkerV1{},
		HashJoinCtxV1: &join.HashJoinCtxV1{
			IsOuterJoin:     v.JoinType.IsOuterJoin(),
			UseOuterToBuild: v.UseOuterToBuild,
		},
	}
	e.HashJoinCtxV1.SessCtx = b.ctx
	e.HashJoinCtxV1.JoinType = v.JoinType
	e.HashJoinCtxV1.Concurrency = v.Concurrency
	e.HashJoinCtxV1.ChunkAllocPool = e.AllocPool
	defaultValues := v.DefaultValues
	lhsTypes, rhsTypes := exec.RetTypes(leftExec), exec.RetTypes(rightExec)
	if v.InnerChildIdx == 1 {
		if len(v.RightConditions) > 0 {
			b.err = errors.Annotate(exeerrors.ErrBuildExecutor, "join's inner condition should be empty")
			return nil
		}
	} else {
		if len(v.LeftConditions) > 0 {
			b.err = errors.Annotate(exeerrors.ErrBuildExecutor, "join's inner condition should be empty")
			return nil
		}
	}

	leftIsBuildSide := true

	e.IsNullEQ = v.IsNullEQ
	var probeKeys, probeNAKeys, buildKeys, buildNAKeys []*expression.Column
	var buildSideExec exec.Executor
	if v.UseOuterToBuild {
		// update the buildSideEstCount due to changing the build side
		if v.InnerChildIdx == 1 {
			buildSideExec, buildKeys, buildNAKeys = leftExec, v.LeftJoinKeys, v.LeftNAJoinKeys
			e.ProbeSideTupleFetcher.ProbeSideExec, probeKeys, probeNAKeys = rightExec, v.RightJoinKeys, v.RightNAJoinKeys
			e.OuterFilter = v.LeftConditions
		} else {
			buildSideExec, buildKeys, buildNAKeys = rightExec, v.RightJoinKeys, v.RightNAJoinKeys
			e.ProbeSideTupleFetcher.ProbeSideExec, probeKeys, probeNAKeys = leftExec, v.LeftJoinKeys, v.LeftNAJoinKeys
			e.OuterFilter = v.RightConditions
			leftIsBuildSide = false
		}
		if defaultValues == nil {
			defaultValues = make([]types.Datum, e.ProbeSideTupleFetcher.ProbeSideExec.Schema().Len())
		}
	} else {
		if v.InnerChildIdx == 0 {
			buildSideExec, buildKeys, buildNAKeys = leftExec, v.LeftJoinKeys, v.LeftNAJoinKeys
			e.ProbeSideTupleFetcher.ProbeSideExec, probeKeys, probeNAKeys = rightExec, v.RightJoinKeys, v.RightNAJoinKeys
			e.OuterFilter = v.RightConditions
		} else {
			buildSideExec, buildKeys, buildNAKeys = rightExec, v.RightJoinKeys, v.RightNAJoinKeys
			e.ProbeSideTupleFetcher.ProbeSideExec, probeKeys, probeNAKeys = leftExec, v.LeftJoinKeys, v.LeftNAJoinKeys
			e.OuterFilter = v.LeftConditions
			leftIsBuildSide = false
		}
		if defaultValues == nil {
			defaultValues = make([]types.Datum, buildSideExec.Schema().Len())
		}
	}
	probeKeyColIdx := make([]int, len(probeKeys))
	probeNAKeColIdx := make([]int, len(probeNAKeys))
	buildKeyColIdx := make([]int, len(buildKeys))
	buildNAKeyColIdx := make([]int, len(buildNAKeys))
	for i := range buildKeys {
		buildKeyColIdx[i] = buildKeys[i].Index
	}
	for i := range buildNAKeys {
		buildNAKeyColIdx[i] = buildNAKeys[i].Index
	}
	for i := range probeKeys {
		probeKeyColIdx[i] = probeKeys[i].Index
	}
	for i := range probeNAKeys {
		probeNAKeColIdx[i] = probeNAKeys[i].Index
	}
	isNAJoin := len(v.LeftNAJoinKeys) > 0
	colsFromChildren := v.Schema().Columns
	if v.JoinType == base.LeftOuterSemiJoin || v.JoinType == base.AntiLeftOuterSemiJoin {
		colsFromChildren = colsFromChildren[:len(colsFromChildren)-1]
	}
	childrenUsedSchema := markChildrenUsedCols(colsFromChildren, v.Children()[0].Schema(), v.Children()[1].Schema())
	for i := range e.Concurrency {
		e.ProbeWorkers[i] = &join.ProbeWorkerV1{
			HashJoinCtx:      e.HashJoinCtxV1,
			Joiner:           join.NewJoiner(b.ctx, v.JoinType, v.InnerChildIdx == 0, defaultValues, v.OtherConditions, lhsTypes, rhsTypes, childrenUsedSchema, isNAJoin),
			ProbeKeyColIdx:   probeKeyColIdx,
			ProbeNAKeyColIdx: probeNAKeColIdx,
		}
		e.ProbeWorkers[i].WorkerID = i
	}
	e.BuildWorker.BuildKeyColIdx, e.BuildWorker.BuildNAKeyColIdx, e.BuildWorker.BuildSideExec, e.BuildWorker.HashJoinCtx = buildKeyColIdx, buildNAKeyColIdx, buildSideExec, e.HashJoinCtxV1
	e.HashJoinCtxV1.IsNullAware = isNAJoin
	executor_metrics.ExecutorCountHashJoinExec.Inc()

	// We should use JoinKey to construct the type information using by hashing, instead of using the child's schema directly.
	// When a hybrid type column is hashed multiple times, we need to distinguish what field types are used.
	// For example, the condition `enum = int and enum = string`, we should use ETInt to hash the first column,
	// and use ETString to hash the second column, although they may be the same column.
	leftExecTypes, rightExecTypes := exec.RetTypes(leftExec), exec.RetTypes(rightExec)
	leftTypes, rightTypes := make([]*types.FieldType, 0, len(v.LeftJoinKeys)+len(v.LeftNAJoinKeys)), make([]*types.FieldType, 0, len(v.RightJoinKeys)+len(v.RightNAJoinKeys))
	// set left types and right types for joiner.
	for i, col := range v.LeftJoinKeys {
		leftTypes = append(leftTypes, leftExecTypes[col.Index].Clone())
		leftTypes[i].SetFlag(col.RetType.GetFlag())
	}
	offset := len(v.LeftJoinKeys)
	for i, col := range v.LeftNAJoinKeys {
		leftTypes = append(leftTypes, leftExecTypes[col.Index].Clone())
		leftTypes[i+offset].SetFlag(col.RetType.GetFlag())
	}
	for i, col := range v.RightJoinKeys {
		rightTypes = append(rightTypes, rightExecTypes[col.Index].Clone())
		rightTypes[i].SetFlag(col.RetType.GetFlag())
	}
	offset = len(v.RightJoinKeys)
	for i, col := range v.RightNAJoinKeys {
		rightTypes = append(rightTypes, rightExecTypes[col.Index].Clone())
		rightTypes[i+offset].SetFlag(col.RetType.GetFlag())
	}

	// consider collations
	for i := range v.EqualConditions {
		chs, coll := v.EqualConditions[i].CharsetAndCollation()
		leftTypes[i].SetCharset(chs)
		leftTypes[i].SetCollate(coll)
		rightTypes[i].SetCharset(chs)
		rightTypes[i].SetCollate(coll)
	}
	offset = len(v.EqualConditions)
	for i := range v.NAEqualConditions {
		chs, coll := v.NAEqualConditions[i].CharsetAndCollation()
		leftTypes[i+offset].SetCharset(chs)
		leftTypes[i+offset].SetCollate(coll)
		rightTypes[i+offset].SetCharset(chs)
		rightTypes[i+offset].SetCollate(coll)
	}
	if leftIsBuildSide {
		e.BuildTypes, e.ProbeTypes = leftTypes, rightTypes
	} else {
		e.BuildTypes, e.ProbeTypes = rightTypes, leftTypes
	}
	return e
}

func (b *executorBuilder) buildIndexLookUpJoin(v *physicalop.PhysicalIndexJoin) exec.Executor {
	outerExec := b.build(v.Children()[1-v.InnerChildIdx])
	if b.err != nil {
		return nil
	}
	outerTypes := exec.RetTypes(outerExec)
	innerPlan := v.Children()[v.InnerChildIdx]
	innerTypes := make([]*types.FieldType, innerPlan.Schema().Len())
	for i, col := range innerPlan.Schema().Columns {
		innerTypes[i] = col.RetType.Clone()
		// The `innerTypes` would be called for `Datum.ConvertTo` when converting the columns from outer table
		// to build hash map or construct lookup keys. So we need to modify its flen otherwise there would be
		// truncate error. See issue https://github.com/pingcap/tidb/issues/21232 for example.
		if innerTypes[i].EvalType() == types.ETString {
			innerTypes[i].SetFlen(types.UnspecifiedLength)
		}
	}

	// Use the probe table's collation.
	for i, col := range v.OuterHashKeys {
		outerTypes[col.Index] = outerTypes[col.Index].Clone()
		outerTypes[col.Index].SetCollate(innerTypes[v.InnerHashKeys[i].Index].GetCollate())
		outerTypes[col.Index].SetFlag(col.RetType.GetFlag())
	}

	// We should use JoinKey to construct the type information using by hashing, instead of using the child's schema directly.
	// When a hybrid type column is hashed multiple times, we need to distinguish what field types are used.
	// For example, the condition `enum = int and enum = string`, we should use ETInt to hash the first column,
	// and use ETString to hash the second column, although they may be the same column.
	innerHashTypes := make([]*types.FieldType, len(v.InnerHashKeys))
	outerHashTypes := make([]*types.FieldType, len(v.OuterHashKeys))
	for i, col := range v.InnerHashKeys {
		innerHashTypes[i] = innerTypes[col.Index].Clone()
		innerHashTypes[i].SetFlag(col.RetType.GetFlag())
	}
	for i, col := range v.OuterHashKeys {
		outerHashTypes[i] = outerTypes[col.Index].Clone()
		outerHashTypes[i].SetFlag(col.RetType.GetFlag())
	}
	hashIsNullEQ := make([]bool, len(v.InnerHashKeys))
	copy(hashIsNullEQ, v.IsNullEQ)

	var (
		outerFilter           []expression.Expression
		leftTypes, rightTypes []*types.FieldType
	)

	if v.InnerChildIdx == 0 {
		leftTypes, rightTypes = innerTypes, outerTypes
		outerFilter = v.RightConditions
		if len(v.LeftConditions) > 0 {
			b.err = errors.Annotate(exeerrors.ErrBuildExecutor, "join's inner condition should be empty")
			return nil
		}
	} else {
		leftTypes, rightTypes = outerTypes, innerTypes
		outerFilter = v.LeftConditions
		if len(v.RightConditions) > 0 {
			b.err = errors.Annotate(exeerrors.ErrBuildExecutor, "join's inner condition should be empty")
			return nil
		}
	}
	defaultValues := v.DefaultValues
	if defaultValues == nil {
		defaultValues = make([]types.Datum, len(innerTypes))
	}
	hasPrefixCol := false
	for _, l := range v.IdxColLens {
		if l != types.UnspecifiedLength {
			hasPrefixCol = true
			break
		}
	}

	readerBuilder, err := b.newDataReaderBuilder(innerPlan)
	if err != nil {
		b.err = err
		return nil
	}

	e := &join.IndexLookUpJoin{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), outerExec),
		OuterCtx: join.OuterCtx{
			RowTypes:  outerTypes,
			HashTypes: outerHashTypes,
			Filter:    outerFilter,
		},
		InnerCtx: join.InnerCtx{
			ReaderBuilder: readerBuilder,
			RowTypes:      innerTypes,
			HashTypes:     innerHashTypes,
			HashIsNullEQ:  hashIsNullEQ,
			ColLens:       v.IdxColLens,
			HasPrefixCol:  hasPrefixCol,
		},
		WorkerWg:      new(sync.WaitGroup),
		IsOuterJoin:   v.JoinType.IsOuterJoin(),
		IndexRanges:   v.Ranges,
		KeyOff2IdxOff: v.KeyOff2IdxOff,
		LastColHelper: v.CompareFilters,
		Finished:      &atomic.Value{},
	}
	colsFromChildren := v.Schema().Columns
	if v.JoinType == base.LeftOuterSemiJoin || v.JoinType == base.AntiLeftOuterSemiJoin {
		colsFromChildren = colsFromChildren[:len(colsFromChildren)-1]
	}
	childrenUsedSchema := markChildrenUsedCols(colsFromChildren, v.Children()[0].Schema(), v.Children()[1].Schema())
	e.Joiner = join.NewJoiner(b.ctx, v.JoinType, v.InnerChildIdx == 0, defaultValues, v.OtherConditions, leftTypes, rightTypes, childrenUsedSchema, false)
	outerKeyCols := make([]int, len(v.OuterJoinKeys))
	for i := range v.OuterJoinKeys {
		outerKeyCols[i] = v.OuterJoinKeys[i].Index
	}
	innerKeyCols := make([]int, len(v.InnerJoinKeys))
	innerKeyColIDs := make([]int64, len(v.InnerJoinKeys))
	keyCollators := make([]collate.Collator, 0, len(v.InnerJoinKeys))
	for i := range v.InnerJoinKeys {
		innerKeyCols[i] = v.InnerJoinKeys[i].Index
		innerKeyColIDs[i] = v.InnerJoinKeys[i].ID
		keyCollators = append(keyCollators, collate.GetCollator(v.InnerJoinKeys[i].RetType.GetCollate()))
	}
	e.OuterCtx.KeyCols = outerKeyCols
	e.InnerCtx.KeyCols = innerKeyCols
	e.InnerCtx.KeyColIDs = innerKeyColIDs
	e.InnerCtx.KeyCollators = keyCollators

	outerHashCols, innerHashCols := make([]int, len(v.OuterHashKeys)), make([]int, len(v.InnerHashKeys))
	hashCollators := make([]collate.Collator, 0, len(v.InnerHashKeys))
	for i := range v.OuterHashKeys {
		outerHashCols[i] = v.OuterHashKeys[i].Index
	}
	for i := range v.InnerHashKeys {
		innerHashCols[i] = v.InnerHashKeys[i].Index
		hashCollators = append(hashCollators, collate.GetCollator(v.InnerHashKeys[i].RetType.GetCollate()))
	}
	e.OuterCtx.HashCols = outerHashCols
	e.InnerCtx.HashCols = innerHashCols
	e.InnerCtx.HashCollators = hashCollators

	e.JoinResult = exec.TryNewCacheChunk(e)
	executor_metrics.ExecutorCounterIndexLookUpJoin.Inc()
	return e
}

func (b *executorBuilder) buildIndexLookUpMergeJoin(v *physicalop.PhysicalIndexMergeJoin) exec.Executor {
	outerExec := b.build(v.Children()[1-v.InnerChildIdx])
	if b.err != nil {
		return nil
	}
	outerTypes := exec.RetTypes(outerExec)
	innerPlan := v.Children()[v.InnerChildIdx]
	innerTypes := make([]*types.FieldType, innerPlan.Schema().Len())
	for i, col := range innerPlan.Schema().Columns {
		innerTypes[i] = col.RetType.Clone()
		// The `innerTypes` would be called for `Datum.ConvertTo` when converting the columns from outer table
		// to build hash map or construct lookup keys. So we need to modify its flen otherwise there would be
		// truncate error. See issue https://github.com/pingcap/tidb/issues/21232 for example.
		if innerTypes[i].EvalType() == types.ETString {
			innerTypes[i].SetFlen(types.UnspecifiedLength)
		}
	}
	var (
		outerFilter           []expression.Expression
		leftTypes, rightTypes []*types.FieldType
	)
	if v.InnerChildIdx == 0 {
		leftTypes, rightTypes = innerTypes, outerTypes
		outerFilter = v.RightConditions
		if len(v.LeftConditions) > 0 {
			b.err = errors.Annotate(exeerrors.ErrBuildExecutor, "join's inner condition should be empty")
			return nil
		}
	} else {
		leftTypes, rightTypes = outerTypes, innerTypes
		outerFilter = v.LeftConditions
		if len(v.RightConditions) > 0 {
			b.err = errors.Annotate(exeerrors.ErrBuildExecutor, "join's inner condition should be empty")
			return nil
		}
	}
	defaultValues := v.DefaultValues
	if defaultValues == nil {
		defaultValues = make([]types.Datum, len(innerTypes))
	}
	outerKeyCols := make([]int, len(v.OuterJoinKeys))
	for i := range v.OuterJoinKeys {
		outerKeyCols[i] = v.OuterJoinKeys[i].Index
	}
	innerKeyCols := make([]int, len(v.InnerJoinKeys))
	keyCollators := make([]collate.Collator, 0, len(v.InnerJoinKeys))
	for i := range v.InnerJoinKeys {
		innerKeyCols[i] = v.InnerJoinKeys[i].Index
		keyCollators = append(keyCollators, collate.GetCollator(v.InnerJoinKeys[i].RetType.GetCollate()))
	}
	executor_metrics.ExecutorCounterIndexLookUpJoin.Inc()

	readerBuilder, err := b.newDataReaderBuilder(innerPlan)
	if err != nil {
		b.err = err
		return nil
	}

	e := &join.IndexLookUpMergeJoin{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), outerExec),
		OuterMergeCtx: join.OuterMergeCtx{
			RowTypes:      outerTypes,
			Filter:        outerFilter,
			JoinKeys:      v.OuterJoinKeys,
			KeyCols:       outerKeyCols,
			NeedOuterSort: v.NeedOuterSort,
			CompareFuncs:  v.OuterCompareFuncs,
		},
		InnerMergeCtx: join.InnerMergeCtx{
			ReaderBuilder:           readerBuilder,
			RowTypes:                innerTypes,
			JoinKeys:                v.InnerJoinKeys,
			KeyCols:                 innerKeyCols,
			KeyCollators:            keyCollators,
			CompareFuncs:            v.CompareFuncs,
			ColLens:                 v.IdxColLens,
			Desc:                    v.Desc,
			KeyOff2KeyOffOrderByIdx: v.KeyOff2KeyOffOrderByIdx,
		},
		WorkerWg:      new(sync.WaitGroup),
		IsOuterJoin:   v.JoinType.IsOuterJoin(),
		IndexRanges:   v.Ranges,
		KeyOff2IdxOff: v.KeyOff2IdxOff,
		LastColHelper: v.CompareFilters,
	}
	colsFromChildren := v.Schema().Columns
	if v.JoinType == base.LeftOuterSemiJoin || v.JoinType == base.AntiLeftOuterSemiJoin {
		colsFromChildren = colsFromChildren[:len(colsFromChildren)-1]
	}
	childrenUsedSchema := markChildrenUsedCols(colsFromChildren, v.Children()[0].Schema(), v.Children()[1].Schema())
	joiners := make([]join.Joiner, e.Ctx().GetSessionVars().IndexLookupJoinConcurrency())
	for i := range joiners {
		joiners[i] = join.NewJoiner(b.ctx, v.JoinType, v.InnerChildIdx == 0, defaultValues, v.OtherConditions, leftTypes, rightTypes, childrenUsedSchema, false)
	}
	e.Joiners = joiners
	return e
}

func (b *executorBuilder) buildIndexNestedLoopHashJoin(v *physicalop.PhysicalIndexHashJoin) exec.Executor {
	joinExec := b.buildIndexLookUpJoin(&(v.PhysicalIndexJoin))
	if b.err != nil {
		return nil
	}
	e := joinExec.(*join.IndexLookUpJoin)
	idxHash := &join.IndexNestedLoopHashJoin{
		IndexLookUpJoin: *e,
		KeepOuterOrder:  v.KeepOuterOrder,
	}
	concurrency := e.Ctx().GetSessionVars().IndexLookupJoinConcurrency()
	idxHash.Joiners = make([]join.Joiner, concurrency)
	for i := range concurrency {
		idxHash.Joiners[i] = e.Joiner.Clone()
	}
	return idxHash
}

