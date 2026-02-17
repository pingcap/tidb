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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/aggfuncs"
	"github.com/pingcap/tidb/pkg/executor/aggregate"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/vecgroupchecker"
	executor_metrics "github.com/pingcap/tidb/pkg/executor/metrics"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
)
func (b *executorBuilder) buildHashAgg(v *physicalop.PhysicalHashAgg) exec.Executor {
	src := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	return b.buildHashAggFromChildExec(src, v)
}

func (b *executorBuilder) buildHashAggFromChildExec(childExec exec.Executor, v *physicalop.PhysicalHashAgg) *aggregate.HashAggExec {
	sessionVars := b.ctx.GetSessionVars()
	e := &aggregate.HashAggExec{
		BaseExecutor:    exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), childExec),
		Sc:              sessionVars.StmtCtx,
		PartialAggFuncs: make([]aggfuncs.AggFunc, 0, len(v.AggFuncs)),
		GroupByItems:    v.GroupByItems,
	}
	// We take `create table t(a int, b int);` as example.
	//
	// 1. If all the aggregation functions are FIRST_ROW, we do not need to set the defaultVal for them:
	// e.g.
	// mysql> select distinct a, b from t;
	// 0 rows in set (0.00 sec)
	//
	// 2. If there exists group by items, we do not need to set the defaultVal for them either:
	// e.g.
	// mysql> select avg(a) from t group by b;
	// Empty set (0.00 sec)
	//
	// mysql> select avg(a) from t group by a;
	// +--------+
	// | avg(a) |
	// +--------+
	// |  NULL  |
	// +--------+
	// 1 row in set (0.00 sec)
	if len(v.GroupByItems) != 0 || aggregation.IsAllFirstRow(v.AggFuncs) {
		e.DefaultVal = nil
	} else if v.IsFinalAgg() {
		e.DefaultVal = e.AllocPool.Alloc(exec.RetTypes(e), 1, 1)
	}
	for _, aggDesc := range v.AggFuncs {
		if aggDesc.HasDistinct || len(aggDesc.OrderByItems) > 0 {
			e.IsUnparallelExec = true
		}
	}
	// When we set both tidb_hashagg_final_concurrency and tidb_hashagg_partial_concurrency to 1,
	// we do not need to parallelly execute hash agg,
	// and this action can be a workaround when meeting some unexpected situation using parallelExec.
	if finalCon, partialCon := sessionVars.HashAggFinalConcurrency(), sessionVars.HashAggPartialConcurrency(); finalCon <= 0 || partialCon <= 0 || finalCon == 1 && partialCon == 1 {
		e.IsUnparallelExec = true
	}
	partialOrdinal := 0
	exprCtx := b.ctx.GetExprCtx()
	for i, aggDesc := range v.AggFuncs {
		if e.IsUnparallelExec {
			e.PartialAggFuncs = append(e.PartialAggFuncs, aggfuncs.Build(exprCtx, aggDesc, i))
		} else {
			ordinal := []int{partialOrdinal}
			partialOrdinal++
			if aggDesc.Name == ast.AggFuncAvg {
				ordinal = append(ordinal, partialOrdinal+1)
				partialOrdinal++
			}
			partialAggDesc, finalDesc := aggDesc.Split(ordinal)
			partialAggFunc := aggfuncs.Build(exprCtx, partialAggDesc, i)
			finalAggFunc := aggfuncs.Build(exprCtx, finalDesc, i)
			e.PartialAggFuncs = append(e.PartialAggFuncs, partialAggFunc)
			e.FinalAggFuncs = append(e.FinalAggFuncs, finalAggFunc)
			if partialAggDesc.Name == ast.AggFuncGroupConcat {
				// For group_concat, finalAggFunc and partialAggFunc need shared `truncate` flag to do duplicate.
				finalAggFunc.(interface{ SetTruncated(t *int32) }).SetTruncated(
					partialAggFunc.(interface{ GetTruncated() *int32 }).GetTruncated(),
				)
			}
		}
		if e.DefaultVal != nil {
			value := aggDesc.GetDefaultValue()
			e.DefaultVal.AppendDatum(i, &value)
		}
	}

	executor_metrics.ExecutorCounterHashAggExec.Inc()
	return e
}

func (b *executorBuilder) buildStreamAgg(v *physicalop.PhysicalStreamAgg) exec.Executor {
	src := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	return b.buildStreamAggFromChildExec(src, v)
}

func (b *executorBuilder) buildStreamAggFromChildExec(childExec exec.Executor, v *physicalop.PhysicalStreamAgg) *aggregate.StreamAggExec {
	exprCtx := b.ctx.GetExprCtx()
	e := &aggregate.StreamAggExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), childExec),
		GroupChecker: vecgroupchecker.NewVecGroupChecker(exprCtx.GetEvalCtx(), b.ctx.GetSessionVars().EnableVectorizedExpression, v.GroupByItems),
		AggFuncs:     make([]aggfuncs.AggFunc, 0, len(v.AggFuncs)),
	}

	if len(v.GroupByItems) != 0 || aggregation.IsAllFirstRow(v.AggFuncs) {
		e.DefaultVal = nil
	} else {
		// Only do this for final agg, see issue #35295, #30923
		if v.IsFinalAgg() {
			e.DefaultVal = e.AllocPool.Alloc(exec.RetTypes(e), 1, 1)
		}
	}
	for i, aggDesc := range v.AggFuncs {
		aggFunc := aggfuncs.Build(exprCtx, aggDesc, i)
		e.AggFuncs = append(e.AggFuncs, aggFunc)
		if e.DefaultVal != nil {
			value := aggDesc.GetDefaultValue()
			e.DefaultVal.AppendDatum(i, &value)
		}
	}

	executor_metrics.ExecutorStreamAggExec.Inc()
	return e
}

func (b *executorBuilder) buildSelection(v *physicalop.PhysicalSelection) exec.Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	e := &SelectionExec{
		selectionExecutorContext: newSelectionExecutorContext(b.ctx),
		BaseExecutorV2:           exec.NewBaseExecutorV2(b.ctx.GetSessionVars(), v.Schema(), v.ID(), childExec),
		filters:                  v.Conditions,
	}
	return e
}

func (b *executorBuilder) buildExpand(v *physicalop.PhysicalExpand) exec.Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	levelES := make([]*expression.EvaluatorSuite, 0, len(v.LevelExprs))
	for _, exprs := range v.LevelExprs {
		// column evaluator can always refer others inside expand.
		// grouping column's nullability change should be seen as a new column projecting.
		// since input inside expand logic should be targeted and reused for N times.
		// column evaluator's swapping columns logic will pollute the input data.
		levelE := expression.NewEvaluatorSuite(exprs, true)
		levelES = append(levelES, levelE)
	}
	e := &ExpandExec{
		BaseExecutor:        exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), childExec),
		numWorkers:          int64(b.ctx.GetSessionVars().ProjectionConcurrency()),
		levelEvaluatorSuits: levelES,
	}

	// If the calculation row count for this Projection operator is smaller
	// than a Chunk size, we turn back to the un-parallel Projection
	// implementation to reduce the goroutine overhead.
	if int64(v.StatsCount()) < int64(b.ctx.GetSessionVars().MaxChunkSize) {
		e.numWorkers = 0
	}

	// Use un-parallel projection for query that write on memdb to avoid data race.
	// See also https://github.com/pingcap/tidb/issues/26832
	if b.inUpdateStmt || b.inDeleteStmt || b.inInsertStmt || b.hasLock {
		e.numWorkers = 0
	}
	return e
}

func (b *executorBuilder) buildProjection(v *physicalop.PhysicalProjection) exec.Executor {
	childExec := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	return b.newProjectionExec(childExec, v)
}

func (b *executorBuilder) newProjectionExec(childExec exec.Executor, v *physicalop.PhysicalProjection) *ProjectionExec {
	e := &ProjectionExec{
		projectionExecutorContext: newProjectionExecutorContext(b.ctx),
		BaseExecutorV2:            exec.NewBaseExecutorV2(b.ctx.GetSessionVars(), v.Schema(), v.ID(), childExec),
		numWorkers:                int64(b.ctx.GetSessionVars().ProjectionConcurrency()),
		evaluatorSuit:             expression.NewEvaluatorSuite(v.Exprs, v.AvoidColumnEvaluator),
		calculateNoDelay:          v.CalculateNoDelay,
	}

	// If the calculation row count for this Projection operator is smaller
	// than a Chunk size, we turn back to the un-parallel Projection
	// implementation to reduce the goroutine overhead.
	// index join would use the same logic.
	if int64(v.StatsCount()) < int64(b.ctx.GetSessionVars().MaxChunkSize) {
		e.numWorkers = 0
	}

	// Use un-parallel projection for query that write on memdb to avoid data race.
	// See also https://github.com/pingcap/tidb/issues/26832
	if b.inUpdateStmt || b.inDeleteStmt || b.inInsertStmt || b.hasLock {
		e.numWorkers = 0
	}
	return e
}

func (b *executorBuilder) buildTableDual(v *physicalop.PhysicalTableDual) exec.Executor {
	if v.RowCount != 0 && v.RowCount != 1 {
		b.err = errors.Errorf("buildTableDual failed, invalid row count for dual table: %v", v.RowCount)
		return nil
	}
	base := exec.NewBaseExecutorV2(b.ctx.GetSessionVars(), v.Schema(), v.ID())
	base.SetInitCap(v.RowCount)
	e := &TableDualExec{
		BaseExecutorV2: base,
		numDualRows:    v.RowCount,
	}
	return e
}
