// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
// // Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"context"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/ranger"
)

// maxMinEliminator tries to eliminate max/min aggregate function.
// For SQL like `select max(id) from t;`, we could optimize it to `select max(id) from (select id from t order by id desc limit 1 where id is not null) t;`.
// For SQL like `select min(id) from t;`, we could optimize it to `select max(id) from (select id from t order by id limit 1 where id is not null) t;`.
// For SQL like `select max(id), min(id) from t;`, we could optimize it to the cartesianJoin result of the two queries above if `id` has an index.
type maxMinEliminator struct {
}

func (a *maxMinEliminator) optimize(ctx context.Context, p LogicalPlan) (LogicalPlan, error) {
	return a.eliminateMaxMin(p), nil
}

// Compose the scalar aggregations by cartesianJoin.
func (a *maxMinEliminator) composeAggsByInnerJoin(aggs []*LogicalAggregation) (plan LogicalPlan) {
	plan = aggs[0]
	sctx := plan.SCtx()
	for i := 1; i < len(aggs); i++ {
		join := LogicalJoin{JoinType: InnerJoin}.Init(sctx)
		join.SetChildren(plan, aggs[i])
		join.schema = buildLogicalJoinSchema(InnerJoin, join)
		join.cartesianJoin = true
		plan = join
	}
	return
}

// checkColCanUseIndex checks the following conditions:
// 1. whether the col is the prefix of an index.
// 2. whether all of the selection conditions can be pushed down to the index range.
func (a *maxMinEliminator) checkColCanUseIndex(plan LogicalPlan, col *expression.Column) bool {
	switch p := plan.(type) {
	case *LogicalSelection:
		// Check whether all of the conditions can be pushed down as accessConds.
		if _, filterConds := ranger.DetachCondsForColumn(p.ctx, p.Conditions, col); len(filterConds) != 0 {
			return false
		}
		return a.checkColCanUseIndex(p.children[0], col)
	case *DataSource:
		// Check whether there is an accessPath can use index for col.
		for _, path := range p.possibleAccessPaths {
			if path.isTablePath {
				if col == p.handleCol {
					return true
				}
			} else {
				if col.ColName.L == path.index.Columns[0].Name.L {
					return true
				}
			}
		}
		return false
	default:
		return false
	}
}

// cloneSubPlans clones the subPlan. We only consider `Selection` and `DataSource` here,
// because we have restricted the subPlan in `checkColCanUseIndex`.
func (a *maxMinEliminator) cloneSubPlans(plan LogicalPlan) LogicalPlan {
	switch p := plan.(type) {
	case *LogicalSelection:
		conditions := make([]expression.Expression, 0, len(p.Conditions))
		for _, cond := range p.Conditions {
			conditions = append(conditions, cond.Clone())
		}
		sel := LogicalSelection{Conditions: conditions}.Init(p.ctx)
		sel.SetChildren(a.cloneSubPlans(p.children[0]))
		return sel
	case *DataSource:
		// This is a shallow clone which may not be safe.
		newDs := *p
		newDs.self = &newDs
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
func (a *maxMinEliminator) splitAggFuncAndCheckIndices(agg *LogicalAggregation) (aggs []*LogicalAggregation, canEliminate bool) {
	aggs = make([]*LogicalAggregation, 0, len(agg.AggFuncs))
	for _, f := range agg.AggFuncs {
		// We must make sure the args of max/min is a simple single column.
		col, ok := f.Args[0].(*expression.Column)
		if !ok {
			return nil, false
		}
		if !a.checkColCanUseIndex(agg.children[0], col) {
			return nil, false
		}
	}
	// we can split the aggregation only if all of the aggFuncs pass the check.
	for i, f := range agg.AggFuncs {
		newAgg := LogicalAggregation{AggFuncs: []*aggregation.AggFuncDesc{f}}.Init(agg.ctx)
		newAgg.SetChildren(a.cloneSubPlans(agg.children[0]))
		newAgg.schema = expression.NewSchema(agg.schema.Columns[i])
		aggs = append(aggs, newAgg)
	}
	return aggs, true
}

// Try to convert a single max/min to Limit+Sort operators.
func (a *maxMinEliminator) eliminateSingleMaxMin(agg *LogicalAggregation) *LogicalAggregation {
	f := agg.AggFuncs[0]
	child := agg.Children()[0]
	ctx := agg.SCtx()

	// If there's no column in f.GetArgs()[0], we still need limit and read data from real table because the result should NULL if the below is empty.
	if len(expression.ExtractColumns(f.Args[0])) > 0 {
		// If it can be NULL, we need to filter NULL out first.
		if !mysql.HasNotNullFlag(f.Args[0].GetType().Flag) {
			sel := LogicalSelection{}.Init(ctx)
			isNullFunc := expression.NewFunctionInternal(ctx, ast.IsNull, types.NewFieldType(mysql.TypeTiny), f.Args[0])
			notNullFunc := expression.NewFunctionInternal(ctx, ast.UnaryNot, types.NewFieldType(mysql.TypeTiny), isNullFunc)
			sel.Conditions = []expression.Expression{notNullFunc}
			sel.SetChildren(agg.Children()[0])
			child = sel
		}

		// Add Sort and Limit operators.
		// For max function, the sort order should be desc.
		desc := f.Name == ast.AggFuncMax
		// Compose Sort operator.
		sort := LogicalSort{}.Init(ctx)
		sort.ByItems = append(sort.ByItems, &ByItems{f.Args[0], desc})
		sort.SetChildren(child)
		child = sort
	}

	// Compose Limit operator.
	li := LogicalLimit{Count: 1}.Init(ctx)
	li.SetChildren(child)

	// If no data in the child, we need to return NULL instead of empty. This cannot be done by sort and limit themselves.
	// Since now it's almost one row returned, a agg operator is okay to do this.
	agg.SetChildren(li)
	return agg
}

// Try to convert max/min to Limit+Sort operators.
func (a *maxMinEliminator) eliminateMaxMin(p LogicalPlan) LogicalPlan {
	if agg, ok := p.(*LogicalAggregation); ok {
		if len(agg.GroupByItems) != 0 {
			return agg
		}
		// Make sure that all of the aggFuncs are Max or Min.
		for _, aggFunc := range agg.AggFuncs {
			if aggFunc.Name != ast.AggFuncMax && aggFunc.Name != ast.AggFuncMin {
				return agg
			}
		}
		if len(agg.AggFuncs) == 1 {
			// If there is only one aggFunc, we don't need to guarantee that the child of it is a data
			// source, or whether the sort can be eliminated. This transformation won't be worse than previous.
			return a.eliminateSingleMaxMin(agg)
		}
		// If we have more than one aggFunc, we can eliminate this agg only if all of the aggFuncs can benefit from
		// their column's index.
		aggs, canEliminate := a.splitAggFuncAndCheckIndices(agg)
		if !canEliminate {
			return agg
		}
		for i := range aggs {
			aggs[i] = a.eliminateSingleMaxMin(aggs[i])
		}
		return a.composeAggsByInnerJoin(aggs)
	}

	newChildren := make([]LogicalPlan, 0, len(p.Children()))
	for _, child := range p.Children() {
		newChildren = append(newChildren, a.eliminateMaxMin(child))
	}
	p.SetChildren(newChildren...)
	return p
}

func (*maxMinEliminator) name() string {
	return "max_min_eliminate"
}
