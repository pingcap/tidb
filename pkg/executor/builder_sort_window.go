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
	"unsafe"

	"github.com/pingcap/tidb/pkg/executor/aggfuncs"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/vecgroupchecker"
	"github.com/pingcap/tidb/pkg/executor/join"
	executor_metrics "github.com/pingcap/tidb/pkg/executor/metrics"
	"github.com/pingcap/tidb/pkg/executor/sortexec"
	"github.com/pingcap/tidb/pkg/executor/unionexec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/util/coreusage"
	"github.com/pingcap/tidb/pkg/types"
)

func (b *executorBuilder) buildSort(v *physicalop.PhysicalSort) exec.Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	sortExec := sortexec.SortExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), childExec),
		ByItems:      v.ByItems,
		ExecSchema:   v.Schema(),
	}
	executor_metrics.ExecutorCounterSortExec.Inc()
	return &sortExec
}

func (b *executorBuilder) buildTopN(v *physicalop.PhysicalTopN) exec.Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	sortExec := sortexec.SortExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), childExec),
		ByItems:      v.ByItems,
		ExecSchema:   v.Schema(),
	}
	executor_metrics.ExecutorCounterTopNExec.Inc()
	t := &sortexec.TopNExec{
		SortExec:    sortExec,
		Limit:       &physicalop.PhysicalLimit{Count: v.Count, Offset: v.Offset},
		Concurrency: b.ctx.GetSessionVars().Concurrency.ExecutorConcurrency,
	}
	columnIdxsUsedByChild, columnMissing := retrieveColumnIdxsUsedByChild(v.Schema(), v.Children()[0].Schema())
	if columnIdxsUsedByChild != nil && columnMissing {
		// In the expected cases colMissing will never happen.
		// However, suppose that childSchema contains generatedCol and is cloned by selfSchema.
		// Then childSchema.generatedCol.UniqueID will not be equal to selfSchema.generatedCol.UniqueID.
		// In this case, colMissing occurs, but it is not wrong.
		// So here we cancel the inline projection, take all of columns from child.
		// If the inline projection directly generates some error causes colMissing,
		// notice that the error feedback given would be inaccurate.
		columnIdxsUsedByChild = nil
		// TODO: If there is valid verification logic, please uncomment the following code
		// b.err = errors.Annotate(ErrBuildExecutor, "Inline projection occurs when `buildTopN` exectutor, columns should not missing in the child schema")
		// return nil
	}
	t.ColumnIdxsUsedByChild = columnIdxsUsedByChild
	return t
}

func (b *executorBuilder) buildApply(v *physicalop.PhysicalApply) exec.Executor {
	var (
		innerPlan base.PhysicalPlan
		outerPlan base.PhysicalPlan
	)
	if v.InnerChildIdx == 0 {
		innerPlan = v.Children()[0]
		outerPlan = v.Children()[1]
	} else {
		innerPlan = v.Children()[1]
		outerPlan = v.Children()[0]
	}
	v.OuterSchema = coreusage.ExtractCorColumnsBySchema4PhysicalPlan(innerPlan, outerPlan.Schema())
	leftChild := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	rightChild := b.build(v.Children()[1])
	if b.err != nil {
		return nil
	}
	// test is in the explain/naaj.test#part5.
	// although we prepared the NAEqualConditions, but for Apply mode, we still need move it to other conditions like eq condition did here.
	otherConditions := append(expression.ScalarFuncs2Exprs(v.EqualConditions), expression.ScalarFuncs2Exprs(v.NAEqualConditions)...)
	otherConditions = append(otherConditions, v.OtherConditions...)
	defaultValues := v.DefaultValues
	if defaultValues == nil {
		defaultValues = make([]types.Datum, v.Children()[v.InnerChildIdx].Schema().Len())
	}
	outerExec, innerExec := leftChild, rightChild
	outerFilter, innerFilter := v.LeftConditions, v.RightConditions
	if v.InnerChildIdx == 0 {
		outerExec, innerExec = rightChild, leftChild
		outerFilter, innerFilter = v.RightConditions, v.LeftConditions
	}
	tupleJoiner := join.NewJoiner(b.ctx, v.JoinType, v.InnerChildIdx == 0,
		defaultValues, otherConditions, exec.RetTypes(leftChild), exec.RetTypes(rightChild), nil, false)

	constructSerialExec := func() exec.Executor {
		serialExec := &join.NestedLoopApplyExec{
			BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), outerExec, innerExec),
			InnerExec:    innerExec,
			OuterExec:    outerExec,
			OuterFilter:  outerFilter,
			InnerFilter:  innerFilter,
			Outer:        v.JoinType != base.InnerJoin,
			Joiner:       tupleJoiner,
			OuterSchema:  v.OuterSchema,
			Sctx:         b.ctx,
			CanUseCache:  v.CanUseCache,
		}
		executor_metrics.ExecutorCounterNestedLoopApplyExec.Inc()
		return serialExec
	}

	// try parallel mode
	if v.Concurrency > 1 {
		innerExecs := make([]exec.Executor, 0, v.Concurrency)
		innerFilters := make([]expression.CNFExprs, 0, v.Concurrency)
		corCols := make([][]*expression.CorrelatedColumn, 0, v.Concurrency)
		joiners := make([]join.Joiner, 0, v.Concurrency)
		for range v.Concurrency {
			clonedInnerPlan, err := physicalop.SafeClone(v.SCtx(), innerPlan)
			if err != nil {
				b.err = nil
				return constructSerialExec()
			}
			corCol := coreusage.ExtractCorColumnsBySchema4PhysicalPlan(clonedInnerPlan, outerPlan.Schema())
			clonedInnerExec := b.build(clonedInnerPlan)
			if b.err != nil {
				b.err = nil
				return constructSerialExec()
			}
			innerExecs = append(innerExecs, clonedInnerExec)
			corCols = append(corCols, corCol)
			innerFilters = append(innerFilters, innerFilter.Clone())
			joiners = append(joiners, join.NewJoiner(b.ctx, v.JoinType, v.InnerChildIdx == 0,
				defaultValues, otherConditions, exec.RetTypes(leftChild), exec.RetTypes(rightChild), nil, false))
		}

		allExecs := append([]exec.Executor{outerExec}, innerExecs...)

		return &ParallelNestedLoopApplyExec{
			BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), allExecs...),
			innerExecs:   innerExecs,
			outerExec:    outerExec,
			outerFilter:  outerFilter,
			innerFilter:  innerFilters,
			outer:        v.JoinType != base.InnerJoin,
			joiners:      joiners,
			corCols:      corCols,
			concurrency:  v.Concurrency,
			useCache:     v.CanUseCache,
		}
	}
	return constructSerialExec()
}

func (b *executorBuilder) buildMaxOneRow(v *physicalop.PhysicalMaxOneRow) exec.Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	base := exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), childExec)
	base.SetInitCap(2)
	base.SetMaxChunkSize(2)
	e := &MaxOneRowExec{BaseExecutor: base}
	return e
}

func (b *executorBuilder) buildUnionAll(v *physicalop.PhysicalUnionAll) exec.Executor {
	childExecs := make([]exec.Executor, len(v.Children()))
	for i, child := range v.Children() {
		childExecs[i] = b.build(child)
		if b.err != nil {
			return nil
		}
	}
	e := &unionexec.UnionExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), childExecs...),
		Concurrency:  b.ctx.GetSessionVars().UnionConcurrency(),
	}
	return e
}

func (b *executorBuilder) buildWindow(v *physicalop.PhysicalWindow) exec.Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	base := exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), childExec)
	groupByItems := make([]expression.Expression, 0, len(v.PartitionBy))
	for _, item := range v.PartitionBy {
		groupByItems = append(groupByItems, item.Col)
	}
	orderByCols := make([]*expression.Column, 0, len(v.OrderBy))
	for _, item := range v.OrderBy {
		orderByCols = append(orderByCols, item.Col)
	}
	windowFuncs := make([]aggfuncs.AggFunc, 0, len(v.WindowFuncDescs))
	partialResults := make([]aggfuncs.PartialResult, 0, len(v.WindowFuncDescs))
	resultColIdx := v.Schema().Len() - len(v.WindowFuncDescs)
	exprCtx := b.ctx.GetExprCtx()
	for _, desc := range v.WindowFuncDescs {
		aggDesc, err := aggregation.NewAggFuncDescForWindowFunc(exprCtx, desc, false)
		if err != nil {
			b.err = err
			return nil
		}
		agg := aggfuncs.BuildWindowFunctions(exprCtx, aggDesc, resultColIdx, orderByCols)
		windowFuncs = append(windowFuncs, agg)
		partialResult, _ := agg.AllocPartialResult()
		partialResults = append(partialResults, partialResult)
		resultColIdx++
	}

	var err error
	if b.ctx.GetSessionVars().EnablePipelinedWindowExec {
		exec := &PipelinedWindowExec{
			BaseExecutor:   base,
			groupChecker:   vecgroupchecker.NewVecGroupChecker(b.ctx.GetExprCtx().GetEvalCtx(), b.ctx.GetSessionVars().EnableVectorizedExpression, groupByItems),
			numWindowFuncs: len(v.WindowFuncDescs),
			windowFuncs:    windowFuncs,
			partialResults: partialResults,
		}
		exec.slidingWindowFuncs = make([]aggfuncs.SlidingWindowAggFunc, len(exec.windowFuncs))
		for i, windowFunc := range exec.windowFuncs {
			if slidingWindowAggFunc, ok := windowFunc.(aggfuncs.SlidingWindowAggFunc); ok {
				exec.slidingWindowFuncs[i] = slidingWindowAggFunc
			}
		}
		if v.Frame == nil {
			exec.start = &logicalop.FrameBound{
				Type:      ast.Preceding,
				UnBounded: true,
			}
			exec.end = &logicalop.FrameBound{
				Type:      ast.Following,
				UnBounded: true,
			}
		} else {
			exec.start = v.Frame.Start
			exec.end = v.Frame.End
			if v.Frame.Type == ast.Ranges {
				cmpResult := int64(-1)
				if len(v.OrderBy) > 0 && v.OrderBy[0].Desc {
					cmpResult = 1
				}
				exec.orderByCols = orderByCols
				exec.expectedCmpResult = cmpResult
				exec.isRangeFrame = true
				err = exec.start.UpdateCompareCols(b.ctx, exec.orderByCols)
				if err != nil {
					return nil
				}
				err = exec.end.UpdateCompareCols(b.ctx, exec.orderByCols)
				if err != nil {
					return nil
				}
			}
		}
		return exec
	}
	var processor windowProcessor
	if v.Frame == nil {
		processor = &aggWindowProcessor{
			windowFuncs:    windowFuncs,
			partialResults: partialResults,
		}
	} else if v.Frame.Type == ast.Rows {
		processor = &rowFrameWindowProcessor{
			windowFuncs:    windowFuncs,
			partialResults: partialResults,
			start:          v.Frame.Start,
			end:            v.Frame.End,
		}
	} else {
		cmpResult := int64(-1)
		if len(v.OrderBy) > 0 && v.OrderBy[0].Desc {
			cmpResult = 1
		}
		tmpProcessor := &rangeFrameWindowProcessor{
			windowFuncs:       windowFuncs,
			partialResults:    partialResults,
			start:             v.Frame.Start,
			end:               v.Frame.End,
			orderByCols:       orderByCols,
			expectedCmpResult: cmpResult,
		}

		err = tmpProcessor.start.UpdateCompareCols(b.ctx, orderByCols)
		if err != nil {
			return nil
		}
		err = tmpProcessor.end.UpdateCompareCols(b.ctx, orderByCols)
		if err != nil {
			return nil
		}

		processor = tmpProcessor
	}
	return &WindowExec{
		BaseExecutor:   base,
		processor:      processor,
		groupChecker:   vecgroupchecker.NewVecGroupChecker(b.ctx.GetExprCtx().GetEvalCtx(), b.ctx.GetSessionVars().EnableVectorizedExpression, groupByItems),
		numWindowFuncs: len(v.WindowFuncDescs),
	}
}

func (b *executorBuilder) buildShuffle(v *physicalop.PhysicalShuffle) *ShuffleExec {
	base := exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID())
	shuffle := &ShuffleExec{
		BaseExecutor: base,
		concurrency:  v.Concurrency,
	}

	// 1. initialize the splitters
	splitters := make([]partitionSplitter, len(v.ByItemArrays))
	switch v.SplitterType {
	case physicalop.PartitionHashSplitterType:
		for i, byItems := range v.ByItemArrays {
			splitters[i] = buildPartitionHashSplitter(shuffle.concurrency, byItems)
		}
	case physicalop.PartitionRangeSplitterType:
		for i, byItems := range v.ByItemArrays {
			splitters[i] = buildPartitionRangeSplitter(b.ctx, shuffle.concurrency, byItems)
		}
	default:
		panic("Not implemented. Should not reach here.")
	}
	shuffle.splitters = splitters

	// 2. initialize the data sources (build the data sources from physical plan to executors)
	shuffle.dataSources = make([]exec.Executor, len(v.DataSources))
	for i, dataSource := range v.DataSources {
		shuffle.dataSources[i] = b.build(dataSource)
		if b.err != nil {
			return nil
		}
	}

	// 3. initialize the workers
	head := v.Children()[0]
	// A `PhysicalShuffleReceiverStub` for every worker have the same `DataSource` but different `Receiver`.
	// We preallocate `PhysicalShuffleReceiverStub`s here and reuse them below.
	stubs := make([]*physicalop.PhysicalShuffleReceiverStub, 0, len(v.DataSources))
	for _, dataSource := range v.DataSources {
		stub := physicalop.PhysicalShuffleReceiverStub{
			DataSource: dataSource,
		}.Init(b.ctx.GetPlanCtx(), dataSource.StatsInfo(), dataSource.QueryBlockOffset(), nil)
		stub.SetSchema(dataSource.Schema())
		stubs = append(stubs, stub)
	}
	shuffle.workers = make([]*shuffleWorker, shuffle.concurrency)
	for i := range shuffle.workers {
		receivers := make([]*shuffleReceiver, len(v.DataSources))
		for j, dataSource := range v.DataSources {
			receivers[j] = &shuffleReceiver{
				BaseExecutor: exec.NewBaseExecutor(b.ctx, dataSource.Schema(), stubs[j].ID()),
			}
		}

		w := &shuffleWorker{
			receivers: receivers,
		}

		for j := range v.DataSources {
			stub := stubs[j]
			stub.Receiver = (unsafe.Pointer)(receivers[j])
			v.Tails[j].SetChildren(stub)
		}

		w.childExec = b.build(head)
		if b.err != nil {
			return nil
		}

		shuffle.workers[i] = w
	}

	return shuffle
}

func (*executorBuilder) buildShuffleReceiverStub(v *physicalop.PhysicalShuffleReceiverStub) *shuffleReceiver {
	return (*shuffleReceiver)(v.Receiver)
}

