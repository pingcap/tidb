// Copyright 2017 PingCAP, Inc.
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

package core

import (
	"bytes"
	"context"
	"fmt"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

// MaxMinEliminator tries to eliminate max/min aggregate function.
// For SQL like `select max(id) from t;`, we could optimize it to `select max(id) from (select id from t order by id desc limit 1 where id is not null) t;`.
// For SQL like `select min(id) from t;`, we could optimize it to `select min(id) from (select id from t order by id limit 1 where id is not null) t;`.
// For SQL like `select max(id), min(id) from t;`, we could optimize it to the cartesianJoin result of the two queries above if `id` has an index.
type MaxMinEliminator struct {
}

// Optimize implements base.LogicalOptRule.<0th> interface.
func (a *MaxMinEliminator) Optimize(_ context.Context, p base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	planChanged := false
	return a.eliminateMaxMin(p, opt), planChanged, nil
}

// composeAggsByInnerJoin composes the scalar aggregations by cartesianJoin.
func (*MaxMinEliminator) composeAggsByInnerJoin(originAgg *logicalop.LogicalAggregation, aggs []*logicalop.LogicalAggregation, opt *optimizetrace.LogicalOptimizeOp) (plan base.LogicalPlan) {
	plan = aggs[0]
	sctx := plan.SCtx()
	joins := make([]*logicalop.LogicalJoin, 0)
	for i := 1; i < len(aggs); i++ {
		join := logicalop.LogicalJoin{JoinType: logicalop.InnerJoin}.Init(sctx, plan.QueryBlockOffset())
		join.SetChildren(plan, aggs[i])
		join.SetSchema(logicalop.BuildLogicalJoinSchema(logicalop.InnerJoin, join))
		join.CartesianJoin = true
		plan = join
		joins = append(joins, join)
	}
	appendEliminateMultiMinMaxTraceStep(originAgg, aggs, joins, opt)
	return
}

// checkColCanUseIndex checks whether there is an AccessPath satisfy the conditions:
// 1. all of the selection's condition can be pushed down as AccessConds of the path.
// 2. the path can keep order for `col` after pushing down the conditions.
func (a *MaxMinEliminator) checkColCanUseIndex(plan base.LogicalPlan, col *expression.Column, conditions []expression.Expression) bool {
	switch p := plan.(type) {
	case *logicalop.LogicalSelection:
		conditions = append(conditions, p.Conditions...)
		return a.checkColCanUseIndex(p.Children()[0], col, conditions)
	case *DataSource:
		// Check whether there is an AccessPath can use index for col.
		for _, path := range p.PossibleAccessPaths {
			if path.IsIntHandlePath {
				// Since table path can contain accessConds of at most one column,
				// we only need to check if all of the conditions can be pushed down as accessConds
				// and `col` is the handle column.
				if p.HandleCols != nil && col.EqualColumn(p.HandleCols.GetCol(0)) {
					if _, filterConds := ranger.DetachCondsForColumn(p.SCtx().GetRangerCtx(), conditions, col); len(filterConds) != 0 {
						return false
					}
					return true
				}
			} else {
				indexCols, indexColLen := path.FullIdxCols, path.FullIdxColLens
				if path.IsCommonHandlePath {
					indexCols, indexColLen = p.CommonHandleCols, p.CommonHandleLens
				}
				// 1. whether all of the conditions can be pushed down as accessConds.
				// 2. whether the AccessPath can satisfy the order property of `col` with these accessConds.
				result, err := ranger.DetachCondAndBuildRangeForIndex(p.SCtx().GetRangerCtx(), conditions, indexCols, indexColLen, p.SCtx().GetSessionVars().RangeMaxSize)
				if err != nil || len(result.RemainedConds) != 0 {
					continue
				}
				for i := 0; i <= result.EqCondCount; i++ {
					if i < len(indexCols) && col.EqualColumn(indexCols[i]) {
						return true
					}
				}
			}
		}
		return false
	default:
		return false
	}
}

// cloneSubPlans shallow clones the subPlan. We only consider `Selection` and `DataSource` here,
// because we have restricted the subPlan in `checkColCanUseIndex`.
func (a *MaxMinEliminator) cloneSubPlans(plan base.LogicalPlan) base.LogicalPlan {
	switch p := plan.(type) {
	case *logicalop.LogicalSelection:
		newConditions := make([]expression.Expression, len(p.Conditions))
		copy(newConditions, p.Conditions)
		sel := logicalop.LogicalSelection{Conditions: newConditions}.Init(p.SCtx(), p.QueryBlockOffset())
		sel.SetChildren(a.cloneSubPlans(p.Children()[0]))
		return sel
	case *DataSource:
		// Quick clone a DataSource.
		// ReadOnly fields uses a shallow copy, while the fields which will be overwritten must use a deep copy.
		newDs := *p
		newDs.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(p.SCtx(), p.TP(), &newDs, p.QueryBlockOffset())
		newDs.SetSchema(p.Schema().Clone())
		newDs.Columns = make([]*model.ColumnInfo, len(p.Columns))
		copy(newDs.Columns, p.Columns)
		newAccessPaths := make([]*util.AccessPath, 0, len(p.PossibleAccessPaths))
		for _, path := range p.PossibleAccessPaths {
			newPath := *path
			newAccessPaths = append(newAccessPaths, &newPath)
		}
		newDs.PossibleAccessPaths = newAccessPaths
		return &newDs
	}
	// This won't happen, because we have checked the subtree.
	return nil
}

// splitAggFuncAndCheckIndices splits the agg to multiple aggs and check whether each agg needs a sort
// after the transformation. For example, we firstly split the sql: `select max(a), min(a), max(b) from t` ->
// `select max(a) from t` + `select min(a) from t` + `select max(b) from t`.
// Then we check whether `a` and `b` have indices. If any of the used column has no index, we cannot eliminate
// this aggregation.
func (a *MaxMinEliminator) splitAggFuncAndCheckIndices(agg *logicalop.LogicalAggregation, opt *optimizetrace.LogicalOptimizeOp) (aggs []*logicalop.LogicalAggregation, canEliminate bool) {
	for _, f := range agg.AggFuncs {
		// We must make sure the args of max/min is a simple single column.
		col, ok := f.Args[0].(*expression.Column)
		if !ok {
			return nil, false
		}
		if !a.checkColCanUseIndex(agg.Children()[0], col, make([]expression.Expression, 0)) {
			return nil, false
		}
	}
	aggs = make([]*logicalop.LogicalAggregation, 0, len(agg.AggFuncs))
	// we can split the aggregation only if all of the aggFuncs pass the check.
	for i, f := range agg.AggFuncs {
		newAgg := logicalop.LogicalAggregation{AggFuncs: []*aggregation.AggFuncDesc{f}}.Init(agg.SCtx(), agg.QueryBlockOffset())
		newAgg.SetChildren(a.cloneSubPlans(agg.Children()[0]))
		newAgg.SetSchema(expression.NewSchema(agg.Schema().Columns[i]))
		// Since LogicalAggregation doesn’t use the parent base.LogicalPlan, passing an incorrect parameter here won’t affect subsequent optimizations.
		var (
			p   base.LogicalPlan
			err error
		)
		if p, err = newAgg.PruneColumns([]*expression.Column{newAgg.Schema().Columns[0]}, opt); err != nil {
			return nil, false
		}
		newAgg = p.(*logicalop.LogicalAggregation)
		aggs = append(aggs, newAgg)
	}
	return aggs, true
}

// eliminateSingleMaxMin tries to convert a single max/min to Limit+Sort operators.
func (*MaxMinEliminator) eliminateSingleMaxMin(agg *logicalop.LogicalAggregation, opt *optimizetrace.LogicalOptimizeOp) *logicalop.LogicalAggregation {
	f := agg.AggFuncs[0]
	child := agg.Children()[0]
	ctx := agg.SCtx()

	var sel *logicalop.LogicalSelection
	var sort *logicalop.LogicalSort
	// If there's no column in f.GetArgs()[0], we still need limit and read data from real table because the result should be NULL if the input is empty.
	if len(expression.ExtractColumns(f.Args[0])) > 0 {
		// If it can be NULL, we need to filter NULL out first.
		if !mysql.HasNotNullFlag(f.Args[0].GetType(ctx.GetExprCtx().GetEvalCtx()).GetFlag()) {
			sel = logicalop.LogicalSelection{}.Init(ctx, agg.QueryBlockOffset())
			isNullFunc := expression.NewFunctionInternal(ctx.GetExprCtx(), ast.IsNull, types.NewFieldType(mysql.TypeTiny), f.Args[0])
			notNullFunc := expression.NewFunctionInternal(ctx.GetExprCtx(), ast.UnaryNot, types.NewFieldType(mysql.TypeTiny), isNullFunc)
			sel.Conditions = []expression.Expression{notNullFunc}
			sel.SetChildren(agg.Children()[0])
			child = sel
		}

		// Add Sort and Limit operators.
		// For max function, the sort order should be desc.
		desc := f.Name == ast.AggFuncMax
		// Compose Sort operator.
		sort = logicalop.LogicalSort{}.Init(ctx, agg.QueryBlockOffset())
		sort.ByItems = append(sort.ByItems, &util.ByItems{Expr: f.Args[0], Desc: desc})
		sort.SetChildren(child)
		child = sort
	}

	// Compose Limit operator.
	li := logicalop.LogicalLimit{Count: 1}.Init(ctx, agg.QueryBlockOffset())
	li.SetChildren(child)

	// If no data in the child, we need to return NULL instead of empty. This cannot be done by sort and limit themselves.
	// Since now there would be at most one row returned, the remained agg operator is not expensive anymore.
	agg.SetChildren(li)
	appendEliminateSingleMaxMinTrace(agg, sel, sort, li, opt)
	return agg
}

// eliminateMaxMin tries to convert max/min to Limit+Sort operators.
func (a *MaxMinEliminator) eliminateMaxMin(p base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) base.LogicalPlan {
	// CTE's logical optimization is indenpent.
	if _, ok := p.(*LogicalCTE); ok {
		return p
	}
	newChildren := make([]base.LogicalPlan, 0, len(p.Children()))
	for _, child := range p.Children() {
		newChildren = append(newChildren, a.eliminateMaxMin(child, opt))
	}
	p.SetChildren(newChildren...)
	if agg, ok := p.(*logicalop.LogicalAggregation); ok {
		if len(agg.GroupByItems) != 0 {
			return agg
		}
		// Make sure that all of the aggFuncs are Max or Min.
		for _, aggFunc := range agg.AggFuncs {
			if aggFunc.Name != ast.AggFuncMax && aggFunc.Name != ast.AggFuncMin {
				return agg
			}
		}
		// Limit+Sort operators are sorted by value, but ENUM/SET field types are sorted by name.
		cols := agg.GetUsedCols()
		for _, col := range cols {
			if col.RetType.GetType() == mysql.TypeEnum || col.RetType.GetType() == mysql.TypeSet {
				return agg
			}
		}
		if len(agg.AggFuncs) == 1 {
			// If there is only one aggFunc, we don't need to guarantee that the child of it is a data
			// source, or whether the sort can be eliminated. This transformation won't be worse than previous.
			return a.eliminateSingleMaxMin(agg, opt)
		}
		// If we have more than one aggFunc, we can eliminate this agg only if all of the aggFuncs can benefit from
		// their column's index.
		aggs, canEliminate := a.splitAggFuncAndCheckIndices(agg, opt)
		if !canEliminate {
			return agg
		}
		for i := range aggs {
			aggs[i] = a.eliminateSingleMaxMin(aggs[i], opt)
		}
		return a.composeAggsByInnerJoin(agg, aggs, opt)
	}
	return p
}

// Name implements base.LogicalOptRule.<1st> interface.
func (*MaxMinEliminator) Name() string {
	return "max_min_eliminate"
}

func appendEliminateSingleMaxMinTrace(agg *logicalop.LogicalAggregation, sel *logicalop.LogicalSelection, sort *logicalop.LogicalSort, limit *logicalop.LogicalLimit, opt *optimizetrace.LogicalOptimizeOp) {
	action := func() string {
		buffer := bytes.NewBufferString("")
		if sel != nil {
			fmt.Fprintf(buffer, "add %v_%v,", sel.TP(), sel.ID())
		}
		if sort != nil {
			fmt.Fprintf(buffer, "add %v_%v,", sort.TP(), sort.ID())
		}
		fmt.Fprintf(buffer, "add %v_%v during eliminating %v_%v %s function", limit.TP(), limit.ID(), agg.TP(), agg.ID(), agg.AggFuncs[0].Name)
		return buffer.String()
	}
	reason := func() string {
		buffer := bytes.NewBufferString(fmt.Sprintf("%v_%v has only one function[%s] without group by", agg.TP(), agg.ID(), agg.AggFuncs[0].Name))
		if sel != nil {
			fmt.Fprintf(buffer, ", the columns in %v_%v shouldn't be NULL and needs NULL to be filtered out", agg.TP(), agg.ID())
		}
		if sort != nil {
			fmt.Fprintf(buffer, ", the columns in %v_%v should be sorted", agg.TP(), agg.ID())
		}
		return buffer.String()
	}
	opt.AppendStepToCurrent(agg.ID(), agg.TP(), reason, action)
}

func appendEliminateMultiMinMaxTraceStep(originAgg *logicalop.LogicalAggregation, aggs []*logicalop.LogicalAggregation, joins []*logicalop.LogicalJoin, opt *optimizetrace.LogicalOptimizeOp) {
	action := func() string {
		buffer := bytes.NewBufferString(fmt.Sprintf("%v_%v splited into [", originAgg.TP(), originAgg.ID()))
		for i, agg := range aggs {
			if i > 0 {
				buffer.WriteString(",")
			}
			fmt.Fprintf(buffer, "%v_%v", agg.TP(), agg.ID())
		}
		buffer.WriteString("], and add [")
		for i, join := range joins {
			if i > 0 {
				buffer.WriteString(",")
			}
			fmt.Fprintf(buffer, "%v_%v", join.TP(), join.ID())
		}
		fmt.Fprintf(buffer, "] to connect them during eliminating %v_%v multi min/max functions", originAgg.TP(), originAgg.ID())
		return buffer.String()
	}
	reason := func() string {
		buffer := bytes.NewBufferString("each column is sorted and can benefit from index/primary key in [")
		for i, agg := range aggs {
			if i > 0 {
				buffer.WriteString(",")
			}
			fmt.Fprintf(buffer, "%v_%v", agg.TP(), agg.ID())
		}
		buffer.WriteString("] and none of them has group by clause")
		return buffer.String()
	}
	opt.AppendStepToCurrent(originAgg.ID(), originAgg.TP(), reason, action)
}
