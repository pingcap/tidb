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
	"context"
	"math"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/core/rule"
	ruleutil "github.com/pingcap/tidb/pkg/planner/core/rule/util"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/coreusage"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

// ExtractOuterApplyCorrelatedCols only extract the correlated columns whose corresponding Apply operator is outside the plan.
// For Plan-1, ExtractOuterApplyCorrelatedCols(CTE-1) will return cor_col_1.
// Plan-1:
//
//	Apply_1
//	 |_ outerSide
//	 |_CTEExec(CTE-1)
//
//	CTE-1
//	 |_Selection(cor_col_1)
//
// For Plan-2, the result of ExtractOuterApplyCorrelatedCols(CTE-2) will not return cor_col_3.
// Because Apply_3 is inside CTE-2.
// Plan-2:
//
//	Apply_2
//	 |_ outerSide
//	 |_ Selection(cor_col_2)
//	     |_CTEExec(CTE-2)
//	CTE-2
//	 |_ Apply_3
//	     |_ outerSide
//	     |_ innerSide(cor_col_3)
func ExtractOuterApplyCorrelatedCols(p base.PhysicalPlan) []*expression.CorrelatedColumn {
	corCols, _ := extractOuterApplyCorrelatedColsHelper(p)
	return corCols
}

func extractOuterApplyCorrelatedColsHelper(p base.PhysicalPlan) ([]*expression.CorrelatedColumn, []*expression.Schema) {
	if p == nil {
		return nil, nil
	}

	// allCorCols store all sub plan's correlated columns.
	// allOuterSchemas store all child Apply's outer side schemas.
	allCorCols := p.ExtractCorrelatedCols()
	allOuterSchemas := []*expression.Schema{}

	handler := func(child base.PhysicalPlan) {
		childCorCols, childOuterSchemas := extractOuterApplyCorrelatedColsHelper(child)
		allCorCols = append(allCorCols, childCorCols...)
		allOuterSchemas = append(allOuterSchemas, childOuterSchemas...)
	}

	switch v := p.(type) {
	case *physicalop.PhysicalApply:
		var outerPlan base.PhysicalPlan
		if v.InnerChildIdx == 0 {
			outerPlan = v.Children()[1]
		} else {
			outerPlan = v.Children()[0]
		}
		allOuterSchemas = append(allOuterSchemas, outerPlan.Schema())
		handler(v.Children()[0])
		handler(v.Children()[1])
	case *physicalop.PhysicalCTE:
		handler(v.SeedPlan)
		handler(v.RecurPlan)
	default:
		for _, child := range p.Children() {
			handler(child)
		}
	}

	resCorCols := make([]*expression.CorrelatedColumn, 0, len(allCorCols))

	// If one correlated column is found in allOuterSchemas, it means this correlated column is corresponding to an Apply inside `p`.
	// However, we only need the correlated columns that correspond to the Apply of the parent node of `p`.
	for _, corCol := range allCorCols {
		var found bool
		for _, outerSchema := range allOuterSchemas {
			if outerSchema.ColumnIndex(&corCol.Column) != -1 {
				found = true
				break
			}
		}
		if !found {
			resCorCols = append(resCorCols, corCol)
		}
	}
	return resCorCols, allOuterSchemas
}

// DecorrelateSolver tries to convert apply plan to join plan.
type DecorrelateSolver struct{}

func (*DecorrelateSolver) aggDefaultValueMap(agg *logicalop.LogicalAggregation) map[int]*expression.Constant {
	defaultValueMap := make(map[int]*expression.Constant, len(agg.AggFuncs))
	for i, f := range agg.AggFuncs {
		switch f.Name {
		case ast.AggFuncBitOr, ast.AggFuncBitXor, ast.AggFuncCount:
			defaultValueMap[i] = expression.NewZero()
		case ast.AggFuncBitAnd:
			defaultValueMap[i] = &expression.Constant{Value: types.NewUintDatum(math.MaxUint64), RetType: types.NewFieldType(mysql.TypeLonglong)}
		}
	}
	return defaultValueMap
}

// pruneRedundantApply: Removes the Apply operator if the parent SELECT clause does not filter any rows from the source.
// Example: SELECT 1 FROM t1 AS tab WHERE 1 = 1 OR (EXISTS(SELECT 1 FROM t2 WHERE a2 = a1))
// In this case, the subquery can be removed entirely since the WHERE clause always evaluates to True.
// This results in a SELECT node with a True condition and an Apply operator as its child.
// If this pattern is detected, we remove both the SELECT and Apply nodes, returning the left child of the Apply operator as the result.
// For the example above, the result would be a table scan on t1.
func pruneRedundantApply(p base.LogicalPlan, groupByColumn map[*expression.Column]struct{}) (base.LogicalPlan, bool) {
	// Check if the current plan is a LogicalSelection
	logicalSelection, ok := p.(*logicalop.LogicalSelection)
	if !ok {
		return nil, false
	}

	// Retrieve the child of LogicalSelection
	selectSource := logicalSelection.Children()[0]

	// Check if the child is a LogicalApply
	apply, ok := selectSource.(*logicalop.LogicalApply)
	if !ok {
		return nil, false
	}

	// Ensure the Apply operator is of a suitable join type to match the required pattern.
	// Only LeftOuterJoin or LeftOuterSemiJoin are considered valid here.
	if apply.JoinType != base.LeftOuterJoin && apply.JoinType != base.LeftOuterSemiJoin {
		return nil, false
	}
	// add a strong limit for fix the https://github.com/pingcap/tidb/issues/58451. we can remove it when to have better implememnt.
	// But this problem has affected tiflash CI.
	// Simplify predicates from the LogicalSelection
	simplifiedPredicates := ruleutil.ApplyPredicateSimplification(p.SCtx(), logicalSelection.Conditions,
		true, nil)

	// Determine if this is a "true selection"
	trueSelection := false
	if len(simplifiedPredicates) == 0 {
		trueSelection = true
	} else if len(simplifiedPredicates) == 1 {
		_, simplifiedPredicatesType := rule.FindPredicateType(p.SCtx(), simplifiedPredicates[0])
		if simplifiedPredicatesType == rule.TruePredicate {
			trueSelection = true
		}
	}

	if trueSelection {
		finalResult := apply

		// Traverse through LogicalApply nodes to find the last one
		for {
			child := finalResult.Children()[0]
			nextApply, ok := child.(*logicalop.LogicalApply)
			if !ok {
				if len(groupByColumn) == 0 {
					return child, true
				}
				for col := range groupByColumn {
					if apply.Schema().Contains(col) && !child.Schema().Contains(col) {
						return nil, false
					}
				}
				return child, true // Return the child of the last LogicalApply
			}
			finalResult = nextApply
		}
	}

	return nil, false
}

// Optimize implements base.LogicalOptRule.<0th> interface.
func (s *DecorrelateSolver) Optimize(ctx context.Context, p base.LogicalPlan) (base.LogicalPlan, bool, error) {
	return s.optimize(ctx, p, nil)
}

func (s *DecorrelateSolver) optimize(ctx context.Context, p base.LogicalPlan, groupByColumn map[*expression.Column]struct{}) (base.LogicalPlan, bool, error) {
	if groupByColumn == nil {
		groupByColumn = make(map[*expression.Column]struct{})
	}
	if agg, ok := p.(*logicalop.LogicalAggregation); ok {
		for _, groupByItems := range agg.GroupByItems {
			for _, column := range expression.ExtractColumns(groupByItems) {
				groupByColumn[column] = struct{}{}
			}
		}
	}

	if optimizedPlan, planChanged := pruneRedundantApply(p, groupByColumn); planChanged {
		return optimizedPlan, planChanged, nil
	}
	planChanged := false
	if apply, ok := p.(*logicalop.LogicalApply); ok {
		outerPlan := apply.Children()[0]
		innerPlan := apply.Children()[1]
		apply.CorCols = coreusage.ExtractCorColumnsBySchema4LogicalPlan(apply.Children()[1], apply.Children()[0].Schema())
		if len(apply.CorCols) == 0 {
			// If the inner plan is non-correlated, the apply will be simplified to join.
			join := &apply.LogicalJoin
			join.SetSelf(join)
			join.SetTP(plancodec.TypeJoin)
			join.SetFlag(logicalop.JoinGenFromApplyFlag)
			p = join
		} else if apply.NoDecorrelate {
			goto NoOptimize
		} else if sel, ok := innerPlan.(*logicalop.LogicalSelection); ok {
			// If the inner plan is a selection, we add this condition to join predicates.
			// Notice that no matter what kind of join is, it's always right.
			newConds := make([]expression.Expression, 0, len(sel.Conditions))
			for _, cond := range sel.Conditions {
				newConds = append(newConds, cond.Decorrelate(outerPlan.Schema()))
			}
			apply.AttachOnConds(newConds)
			innerPlan = sel.Children()[0]
			apply.SetChildren(outerPlan, innerPlan)
			return s.optimize(ctx, p, groupByColumn)
		} else if m, ok := innerPlan.(*logicalop.LogicalMaxOneRow); ok {
			if m.Children()[0].MaxOneRow() {
				innerPlan = m.Children()[0]
				apply.SetChildren(outerPlan, innerPlan)
				return s.optimize(ctx, p, groupByColumn)
			}
		} else if proj, ok := innerPlan.(*logicalop.LogicalProjection); ok {
			// After the column pruning, some expressions in the projection operator may be pruned.
			// In this situation, we can decorrelate the apply operator.
			if apply.JoinType == base.LeftOuterJoin {
				if skipDecorrelateProjectionForLeftOuterApply(apply, proj) {
					goto NoOptimize
				}
			}

			// step1: substitute the all the schema with new expressions (including correlated column maybe, but it doesn't affect the collation infer inside)
			// eg: projection: constant("guo") --> column8, once upper layer substitution failed here, the lower layer behind
			// projection can't supply column8 anymore.
			//
			//	upper OP (depend on column8)   --> projection(constant "guo" --> column8)  --> lower layer OP
			//	          |                                                       ^
			//	          +-------------------------------------------------------+
			//
			//	upper OP (depend on column8)   --> lower layer OP
			//	          |                             ^
			//	          +-----------------------------+      // Fail: lower layer can't supply column8 anymore.
			hasFail := apply.ColumnSubstituteAll(proj.Schema(), proj.Exprs)
			if hasFail {
				goto NoOptimize
			}
			// step2: when it can be substituted all, we then just do the de-correlation (apply conditions included).
			for i, expr := range proj.Exprs {
				proj.Exprs[i] = expr.Decorrelate(outerPlan.Schema())
			}
			apply.Decorrelate(outerPlan.Schema())

			innerPlan = proj.Children()[0]
			apply.SetChildren(outerPlan, innerPlan)
			if apply.JoinType != base.SemiJoin && apply.JoinType != base.LeftOuterSemiJoin && apply.JoinType != base.AntiSemiJoin && apply.JoinType != base.AntiLeftOuterSemiJoin {
				proj.SetSchema(apply.Schema())
				proj.Exprs = append(expression.Column2Exprs(outerPlan.Schema().Clone().Columns), proj.Exprs...)
				apply.SetSchema(expression.MergeSchema(outerPlan.Schema(), innerPlan.Schema()))
				np, planChanged, err := s.optimize(ctx, p, groupByColumn)
				if err != nil {
					return nil, planChanged, err
				}
				proj.SetChildren(np)
				return proj, planChanged, nil
			}
			return s.optimize(ctx, p, groupByColumn)
		} else if li, ok := innerPlan.(*logicalop.LogicalLimit); ok {
			// The presence of 'limit' in 'exists' will make the plan not optimal, so we need to decorrelate the 'limit' of subquery in optimization.
			// e.g. select count(*) from test t1 where exists (select value from test t2 where t1.id = t2.id limit 1); When using 'limit' in subquery, the plan will not optimal.
			// If apply is not SemiJoin, the output of it might be expanded even though we are `limit 1`.
			if apply.JoinType != base.SemiJoin && apply.JoinType != base.LeftOuterSemiJoin && apply.JoinType != base.AntiSemiJoin && apply.JoinType != base.AntiLeftOuterSemiJoin {
				goto NoOptimize
			}
			// If subquery has some filter condition, we will not optimize limit.
			if len(apply.LeftConditions) > 0 || len(apply.RightConditions) > 0 || len(apply.OtherConditions) > 0 || len(apply.EqualConditions) > 0 {
				goto NoOptimize
			}
			// Limit with non-0 offset will conduct an impact of itself on the final result set from its sub-child, consequently determining the bool value of the exist subquery.
			if li.Offset == 0 {
				innerPlan = li.Children()[0]
				apply.SetChildren(outerPlan, innerPlan)
				return s.optimize(ctx, p, groupByColumn)
			}
		} else if agg, ok := innerPlan.(*logicalop.LogicalAggregation); ok {
			if apply.CanPullUpAgg() && agg.CanPullUp() {
				innerPlan = agg.Children()[0]
				apply.JoinType = base.LeftOuterJoin
				apply.SetChildren(outerPlan, innerPlan)
				agg.SetSchema(apply.Schema())
				agg.GroupByItems = expression.Column2Exprs(outerPlan.Schema().PKOrUK[0])
				newAggFuncs := make([]*aggregation.AggFuncDesc, 0, apply.Schema().Len())

				outerColsInSchema := make([]*expression.Column, 0, outerPlan.Schema().Len())
				for i, col := range outerPlan.Schema().Columns {
					first, err := aggregation.NewAggFuncDesc(agg.SCtx().GetExprCtx(), ast.AggFuncFirstRow, []expression.Expression{col}, false)
					if err != nil {
						return nil, planChanged, err
					}
					newAggFuncs = append(newAggFuncs, first)

					outerCol, _ := outerPlan.Schema().Columns[i].Clone().(*expression.Column)
					outerCol.RetType = first.RetTp
					outerColsInSchema = append(outerColsInSchema, outerCol)
				}
				apply.SetSchema(expression.MergeSchema(expression.NewSchema(outerColsInSchema...), innerPlan.Schema()))
				util.ResetNotNullFlag(apply.Schema(), outerPlan.Schema().Len(), apply.Schema().Len())
				for i, aggFunc := range agg.AggFuncs {
					aggArgs := make([]expression.Expression, 0, len(aggFunc.Args))
					for _, arg := range aggFunc.Args {
						switch expr := arg.(type) {
						case *expression.Column:
							if idx := apply.Schema().ColumnIndex(expr); idx != -1 {
								aggArgs = append(aggArgs, apply.Schema().Columns[idx])
							} else {
								aggArgs = append(aggArgs, expr)
							}
						case *expression.ScalarFunction:
							expr.RetType = expr.RetType.Clone()
							expr.RetType.DelFlag(mysql.NotNullFlag)
							aggArgs = append(aggArgs, expr)
						default:
							aggArgs = append(aggArgs, expr)
						}
					}
					desc, err := aggregation.NewAggFuncDesc(agg.SCtx().GetExprCtx(), agg.AggFuncs[i].Name, aggArgs, agg.AggFuncs[i].HasDistinct)
					if err != nil {
						return nil, planChanged, err
					}
					newAggFuncs = append(newAggFuncs, desc)
				}
				agg.AggFuncs = newAggFuncs
				np, planChanged, err := s.optimize(ctx, p, groupByColumn)
				if err != nil {
					return nil, planChanged, err
				}
				agg.SetChildren(np)
				// TODO: Add a Projection if any argument of aggregate funcs or group by items are scalar functions.
				// agg.buildProjectionIfNecessary()
				return agg, planChanged, nil
			}
			// We can pull up the equal conditions below the aggregation as the join key of the apply, if only
			// the equal conditions contain the correlated column of this apply.
			if sel, ok := agg.Children()[0].(*logicalop.LogicalSelection); ok && apply.JoinType == base.LeftOuterJoin {
				var (
					eqCondWithCorCol []*expression.ScalarFunction
					remainedExpr     []expression.Expression
				)
				// Extract the equal condition.
				for _, cond := range sel.Conditions {
					if expr := apply.DeCorColFromEqExpr(cond); expr != nil {
						eqCondWithCorCol = append(eqCondWithCorCol, expr.(*expression.ScalarFunction))
					} else {
						remainedExpr = append(remainedExpr, cond)
					}
				}
				if len(eqCondWithCorCol) > 0 {
					originalExpr := sel.Conditions
					sel.Conditions = remainedExpr
					apply.CorCols = coreusage.ExtractCorColumnsBySchema4LogicalPlan(apply.Children()[1], apply.Children()[0].Schema())
					// There's no other correlated column.
					groupByCols := expression.NewSchema(agg.GetGroupByCols()...)
					if len(apply.CorCols) == 0 {
						appendedGroupByCols := expression.NewSchema()
						var appendedAggFuncs []*aggregation.AggFuncDesc

						join := &apply.LogicalJoin
						join.EqualConditions = append(join.EqualConditions, eqCondWithCorCol...)
						for _, eqCond := range eqCondWithCorCol {
							clonedCol := eqCond.GetArgs()[1].(*expression.Column)
							// If the join key is not in the aggregation's schema, add first row function.
							if agg.Schema().ColumnIndex(eqCond.GetArgs()[1].(*expression.Column)) == -1 {
								newFunc, err := aggregation.NewAggFuncDesc(apply.SCtx().GetExprCtx(), ast.AggFuncFirstRow, []expression.Expression{clonedCol}, false)
								if err != nil {
									return nil, planChanged, err
								}
								agg.AggFuncs = append(agg.AggFuncs, newFunc)
								agg.Schema().Append(clonedCol)
								agg.Schema().Columns[agg.Schema().Len()-1].RetType = newFunc.RetTp
								appendedAggFuncs = append(appendedAggFuncs, newFunc)
							}
							// If group by cols don't contain the join key, add it into this.
							if !groupByCols.Contains(clonedCol) {
								agg.GroupByItems = append(agg.GroupByItems, clonedCol)
								groupByCols.Append(clonedCol)
								appendedGroupByCols.Append(clonedCol)
							}
						}
						// The selection may be useless, check and remove it.
						if len(sel.Conditions) == 0 {
							agg.SetChildren(sel.Children()[0])
						}
						defaultValueMap := s.aggDefaultValueMap(agg)
						// We should use it directly, rather than building a projection.
						if len(defaultValueMap) > 0 {
							proj := logicalop.LogicalProjection{}.Init(agg.SCtx(), agg.QueryBlockOffset())
							proj.SetSchema(apply.Schema())
							proj.Exprs = expression.Column2Exprs(apply.Schema().Columns)
							for i, val := range defaultValueMap {
								pos := proj.Schema().ColumnIndex(agg.Schema().Columns[i])
								ifNullFunc := expression.NewFunctionInternal(agg.SCtx().GetExprCtx(), ast.Ifnull, types.NewFieldType(mysql.TypeLonglong), agg.Schema().Columns[i], val)
								proj.Exprs[pos] = ifNullFunc
							}
							proj.SetChildren(apply)
							p = proj
						}
						return s.optimize(ctx, p, groupByColumn)
					}
					sel.Conditions = originalExpr
					apply.CorCols = coreusage.ExtractCorColumnsBySchema4LogicalPlan(apply.Children()[1], apply.Children()[0].Schema())
				}
			}
		} else if sort, ok := innerPlan.(*logicalop.LogicalSort); ok {
			// Since we only pull up Selection, Projection, Aggregation, MaxOneRow,
			// the top level Sort has no effect on the subquery's result.
			innerPlan = sort.Children()[0]
			apply.SetChildren(outerPlan, innerPlan)
			return s.optimize(ctx, p, groupByColumn)
		}
	}
NoOptimize:
	// CTE's logical optimization is independent.
	if _, ok := p.(*logicalop.LogicalCTE); ok {
		return p, planChanged, nil
	}
	newChildren := make([]base.LogicalPlan, 0, len(p.Children()))
	for _, child := range p.Children() {
		np, planChanged, err := s.optimize(ctx, child, groupByColumn)
		if err != nil {
			return nil, planChanged, err
		}
		newChildren = append(newChildren, np)
	}
	p.SetChildren(newChildren...)
	return p, planChanged, nil
}

// Name implements base.LogicalOptRule.<1st> interface.
func (*DecorrelateSolver) Name() string {
	return "decorrelate"
}

// Return true if we should skip decorrelation for LeftOuterApply + Projection.
func skipDecorrelateProjectionForLeftOuterApply(apply *logicalop.LogicalApply, proj *logicalop.LogicalProjection) bool {
	allConst := len(proj.Exprs) > 0
	for _, expr := range proj.Exprs {
		if len(expression.ExtractCorColumns(expr)) > 0 || !expression.ExtractColumnSet(expr).IsEmpty() {
			allConst = false
			break
		}
	}
	if allConst {
		// If the projection just references some constant. We cannot directly pull it up when the APPLY is an outer join.
		//  e.g. select (select 1 from t1 where t1.a=t2.a) from t2; When the t1.a=t2.a is false the join's output is NULL.
		//       But if we pull the projection upon the APPLY. It will return 1 since the projection is evaluated after the join.
		// We disable the decorrelation directly for now.
		// TODO: Actually, it can be optimized. We need to first push the projection down to the selection. And then the APPLY can be decorrelated.
		return true
	}

	// If proj.Exprs are all from outerPlan, we cannot make sure the output row of projection is always null,
	// which may break the semantics of LeftOuterJoin.
	// Because the right side of output row of LeftOuterJoin is always null when join conditions are not met.
	// TODO: should also disable decorrelate when proj.Exprs use columns from innerPlan and its expression is not null-rejective.
	outerPlan := apply.Children()[0]
	for _, expr := range proj.Exprs {
		cols := expression.ExtractColumns(expr)
		if outerPlan.Schema().ColumnsIndices(cols) != nil {
			return true
		}
	}
	return false
}
