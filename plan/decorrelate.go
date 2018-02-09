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
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/types"
)

// extractCorColumnsBySchema only extracts the correlated columns that match the outer plan's schema.
// e.g. If the correlated columns from inner plan are [t1.a, t2.a, t3.a] and outer plan's schema is [t2.a, t2.b, t2.c],
// only [t2.a] is treated as this apply's correlated column.
func (la *LogicalApply) extractCorColumnsBySchema() {
	schema := la.children[0].Schema()
	corCols := la.children[1].extractCorrelatedCols()
	resultCorCols := make([]*expression.CorrelatedColumn, schema.Len())
	for _, corCol := range corCols {
		idx := schema.ColumnIndex(&corCol.Column)
		if idx != -1 {
			if resultCorCols[idx] == nil {
				resultCorCols[idx] = &expression.CorrelatedColumn{
					Column: *schema.Columns[idx],
					Data:   new(types.Datum),
				}
			}
			corCol.Data = resultCorCols[idx].Data
		}
	}
	// Shrink slice. e.g. [col1, nil, col2, nil] will be changed to [col1, col2].
	length := 0
	for _, col := range resultCorCols {
		if col != nil {
			resultCorCols[length] = col
			length++
		}
	}
	la.corCols = resultCorCols[:length]
}

// canPullUpAgg checks if an apply can pull an aggregation up.
func (la *LogicalApply) canPullUpAgg() bool {
	if la.JoinType != InnerJoin && la.JoinType != LeftOuterJoin {
		return false
	}
	if len(la.EqualConditions)+len(la.LeftConditions)+len(la.RightConditions)+len(la.OtherConditions) > 0 {
		return false
	}
	return len(la.children[0].Schema().Keys) > 0
}

// canPullUp checks if an aggregation can be pulled up. An aggregate function like count(*) cannot be pulled up.
func (la *LogicalAggregation) canPullUp() bool {
	if len(la.GroupByItems) > 0 {
		return false
	}
	for _, f := range la.AggFuncs {
		for _, arg := range f.Args {
			expr := expression.EvaluateExprWithNull(la.ctx, la.children[0].Schema(), arg)
			if con, ok := expr.(*expression.Constant); !ok || !con.Value.IsNull() {
				return false
			}
		}
	}
	return true
}

// decorrelateSolver tries to convert apply plan to join plan.
type decorrelateSolver struct{}

// optimize implements logicalOptRule interface.
func (s *decorrelateSolver) optimize(p LogicalPlan) (LogicalPlan, error) {
	if apply, ok := p.(*LogicalApply); ok {
		outerPlan := apply.children[0]
		innerPlan := apply.children[1]
		apply.extractCorColumnsBySchema()
		if len(apply.corCols) == 0 {
			// If the inner plan is non-correlated, the apply will be simplified to join.
			join := &apply.LogicalJoin
			join.self = join
			p = join
		} else if sel, ok := innerPlan.(*LogicalSelection); ok {
			// If the inner plan is a selection, we add this condition to join predicates.
			// Notice that no matter what kind of join is, it's always right.
			newConds := make([]expression.Expression, 0, len(sel.Conditions))
			for _, cond := range sel.Conditions {
				newConds = append(newConds, cond.Decorrelate(outerPlan.Schema()))
			}
			apply.attachOnConds(newConds)
			innerPlan = sel.children[0]
			apply.SetChildren(outerPlan, innerPlan)
			return s.optimize(p)
		} else if m, ok := innerPlan.(*LogicalMaxOneRow); ok {
			if m.children[0].MaxOneRow() {
				innerPlan = m.children[0]
				apply.SetChildren(outerPlan, innerPlan)
				return s.optimize(p)
			}
		} else if proj, ok := innerPlan.(*LogicalProjection); ok {
			for i, expr := range proj.Exprs {
				proj.Exprs[i] = expr.Decorrelate(outerPlan.Schema())
			}
			apply.columnSubstitute(proj.Schema(), proj.Exprs)
			innerPlan = proj.children[0]
			apply.SetChildren(outerPlan, innerPlan)
			if apply.JoinType != SemiJoin && apply.JoinType != LeftOuterSemiJoin && apply.JoinType != AntiSemiJoin && apply.JoinType != AntiLeftOuterSemiJoin {
				proj.SetSchema(apply.Schema())
				proj.Exprs = append(expression.Column2Exprs(outerPlan.Schema().Clone().Columns), proj.Exprs...)
				apply.SetSchema(expression.MergeSchema(outerPlan.Schema(), innerPlan.Schema()))
				np, err := s.optimize(p)
				if err != nil {
					return nil, errors.Trace(err)
				}
				proj.SetChildren(np)
				return proj, nil
			}
			return s.optimize(p)
		} else if agg, ok := innerPlan.(*LogicalAggregation); ok {
			if apply.canPullUpAgg() && agg.canPullUp() {
				innerPlan = agg.children[0]
				apply.JoinType = LeftOuterJoin
				apply.SetChildren(outerPlan, innerPlan)
				agg.SetSchema(apply.Schema())
				agg.GroupByItems = expression.Column2Exprs(outerPlan.Schema().Keys[0])
				newAggFuncs := make([]*aggregation.AggFuncDesc, 0, apply.Schema().Len())
				for _, col := range outerPlan.Schema().Columns {
					first := aggregation.NewAggFuncDesc(agg.ctx, ast.AggFuncFirstRow, []expression.Expression{col}, false)
					newAggFuncs = append(newAggFuncs, first)
				}
				newAggFuncs = append(newAggFuncs, agg.AggFuncs...)
				agg.AggFuncs = newAggFuncs
				apply.SetSchema(expression.MergeSchema(outerPlan.Schema(), innerPlan.Schema()))
				np, err := s.optimize(p)
				if err != nil {
					return nil, errors.Trace(err)
				}
				agg.SetChildren(np)
				// TODO: Add a Projection if any argument of aggregate funcs or group by items are scala functions.
				// agg.buildProjectionIfNecessary()
				agg.collectGroupByColumns()
				return agg, nil
			}
		}
	}
	newChildren := make([]LogicalPlan, 0, len(p.Children()))
	for _, child := range p.Children() {
		np, err := s.optimize(child)
		if err != nil {
			return nil, errors.Trace(err)
		}
		newChildren = append(newChildren, np)
	}
	p.SetChildren(newChildren...)
	return p, nil
}
