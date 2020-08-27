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

package core

import (
	"context"
	"math"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/types"
)

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

// deCorColFromEqExpr checks whether it's an equal condition of form `col = correlated col`. If so we will change the decorrelated
// column to normal column to make a new equal condition.
func (la *LogicalApply) deCorColFromEqExpr(expr expression.Expression) expression.Expression {
	sf, ok := expr.(*expression.ScalarFunction)
	if !ok || sf.FuncName.L != ast.EQ {
		return nil
	}
	if col, lOk := sf.GetArgs()[0].(*expression.Column); lOk {
		if corCol, rOk := sf.GetArgs()[1].(*expression.CorrelatedColumn); rOk {
			ret := corCol.Decorrelate(la.Schema())
			if _, ok := ret.(*expression.CorrelatedColumn); ok {
				return nil
			}
			// We should make sure that the equal condition's left side is the join's left join key, right is the right key.
			return expression.NewFunctionInternal(la.ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), ret, col)
		}
	}
	if corCol, lOk := sf.GetArgs()[0].(*expression.CorrelatedColumn); lOk {
		if col, rOk := sf.GetArgs()[1].(*expression.Column); rOk {
			ret := corCol.Decorrelate(la.Schema())
			if _, ok := ret.(*expression.CorrelatedColumn); ok {
				return nil
			}
			// We should make sure that the equal condition's left side is the join's left join key, right is the right key.
			return expression.NewFunctionInternal(la.ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), ret, col)
		}
	}
	return nil
}

// ExtractCorrelatedCols4LogicalPlan recursively extracts all of the correlated columns
// from a plan tree by calling LogicalPlan.ExtractCorrelatedCols.
func ExtractCorrelatedCols4LogicalPlan(p LogicalPlan) []*expression.CorrelatedColumn {
	corCols := p.ExtractCorrelatedCols()
	for _, child := range p.Children() {
		corCols = append(corCols, ExtractCorrelatedCols4LogicalPlan(child)...)
	}
	return corCols
}

// ExtractCorrelatedCols4PhysicalPlan recursively extracts all of the correlated columns
// from a plan tree by calling PhysicalPlan.ExtractCorrelatedCols.
func ExtractCorrelatedCols4PhysicalPlan(p PhysicalPlan) []*expression.CorrelatedColumn {
	corCols := p.ExtractCorrelatedCols()
	for _, child := range p.Children() {
		corCols = append(corCols, ExtractCorrelatedCols4PhysicalPlan(child)...)
	}
	return corCols
}

// decorrelateSolver tries to convert apply plan to join plan.
type decorrelateSolver struct{}

func (s *decorrelateSolver) aggDefaultValueMap(agg *LogicalAggregation) map[int]*expression.Constant {
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

// optimize implements logicalOptRule interface.
func (s *decorrelateSolver) optimize(ctx context.Context, p LogicalPlan) (LogicalPlan, error) {
	if apply, ok := p.(*LogicalApply); ok {
		outerPlan := apply.children[0]
		innerPlan := apply.children[1]
		apply.CorCols = extractCorColumnsBySchema4LogicalPlan(apply.children[1], apply.children[0].Schema())
		if len(apply.CorCols) == 0 {
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
			apply.AttachOnConds(newConds)
			innerPlan = sel.children[0]
			apply.SetChildren(outerPlan, innerPlan)
			return s.optimize(ctx, p)
		} else if m, ok := innerPlan.(*LogicalMaxOneRow); ok {
			if m.children[0].MaxOneRow() {
				innerPlan = m.children[0]
				apply.SetChildren(outerPlan, innerPlan)
				return s.optimize(ctx, p)
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
				np, err := s.optimize(ctx, p)
				if err != nil {
					return nil, err
				}
				proj.SetChildren(np)
				return proj, nil
			}
			return s.optimize(ctx, p)
		} else if agg, ok := innerPlan.(*LogicalAggregation); ok {
			if apply.canPullUpAgg() && agg.canPullUp() {
				innerPlan = agg.children[0]
				apply.JoinType = LeftOuterJoin
				apply.SetChildren(outerPlan, innerPlan)
				agg.SetSchema(apply.Schema())
				agg.GroupByItems = expression.Column2Exprs(outerPlan.Schema().Keys[0])
				newAggFuncs := make([]*aggregation.AggFuncDesc, 0, apply.Schema().Len())

				outerColsInSchema := make([]*expression.Column, 0, outerPlan.Schema().Len())
				for i, col := range outerPlan.Schema().Columns {
					first, err := aggregation.NewAggFuncDesc(agg.ctx, ast.AggFuncFirstRow, []expression.Expression{col}, false)
					if err != nil {
						return nil, err
					}
					newAggFuncs = append(newAggFuncs, first)

					outerCol, _ := outerPlan.Schema().Columns[i].Clone().(*expression.Column)
					outerCol.RetType = first.RetTp
					outerColsInSchema = append(outerColsInSchema, outerCol)
				}
				apply.SetSchema(expression.MergeSchema(expression.NewSchema(outerColsInSchema...), innerPlan.Schema()))
				resetNotNullFlag(apply.schema,outerPlan.Schema().Len(),apply.schema.Len())

				for i, aggFunc := range agg.AggFuncs {
					if idx := apply.schema.ColumnIndex(aggFunc.Args[0].(*expression.Column)); idx  != -1{
						first, err := aggregation.NewAggFuncDesc(agg.ctx, agg.AggFuncs[i].Name, []expression.Expression{apply.schema.Columns[idx]}, false)
						if err != nil {
							return nil, err
						}
						newAggFuncs = append(newAggFuncs, first)
					}
				}
				agg.AggFuncs = newAggFuncs
				np, err := s.optimize(ctx, p)
				if err != nil {
					return nil, err
				}
				agg.SetChildren(np)
				// TODO: Add a Projection if any argument of aggregate funcs or group by items are scalar functions.
				// agg.buildProjectionIfNecessary()
				agg.collectGroupByColumns()
				return agg, nil
			}
			// We can pull up the equal conditions below the aggregation as the join key of the apply, if only
			// the equal conditions contain the correlated column of this apply.
			if sel, ok := agg.children[0].(*LogicalSelection); ok && apply.JoinType == LeftOuterJoin {
				var (
					eqCondWithCorCol []*expression.ScalarFunction
					remainedExpr     []expression.Expression
				)
				// Extract the equal condition.
				for _, cond := range sel.Conditions {
					if expr := apply.deCorColFromEqExpr(cond); expr != nil {
						eqCondWithCorCol = append(eqCondWithCorCol, expr.(*expression.ScalarFunction))
					} else {
						remainedExpr = append(remainedExpr, cond)
					}
				}
				if len(eqCondWithCorCol) > 0 {
					originalExpr := sel.Conditions
					sel.Conditions = remainedExpr
					apply.CorCols = extractCorColumnsBySchema4LogicalPlan(apply.children[1], apply.children[0].Schema())
					// There's no other correlated column.
					groupByCols := expression.NewSchema(agg.groupByCols...)
					if len(apply.CorCols) == 0 {
						join := &apply.LogicalJoin
						join.EqualConditions = append(join.EqualConditions, eqCondWithCorCol...)
						for _, eqCond := range eqCondWithCorCol {
							clonedCol := eqCond.GetArgs()[1].(*expression.Column)
							// If the join key is not in the aggregation's schema, add first row function.
							if agg.schema.ColumnIndex(eqCond.GetArgs()[1].(*expression.Column)) == -1 {
								newFunc, err := aggregation.NewAggFuncDesc(apply.ctx, ast.AggFuncFirstRow, []expression.Expression{clonedCol}, false)
								if err != nil {
									return nil, err
								}
								agg.AggFuncs = append(agg.AggFuncs, newFunc)
								agg.schema.Append(clonedCol)
								agg.schema.Columns[agg.schema.Len()-1].RetType = newFunc.RetTp
							}
							// If group by cols don't contain the join key, add it into this.
							if !groupByCols.Contains(clonedCol) {
								agg.GroupByItems = append(agg.GroupByItems, clonedCol)
								groupByCols.Append(clonedCol)
							}
						}
						agg.collectGroupByColumns()
						// The selection may be useless, check and remove it.
						if len(sel.Conditions) == 0 {
							agg.SetChildren(sel.children[0])
						}
						defaultValueMap := s.aggDefaultValueMap(agg)
						// We should use it directly, rather than building a projection.
						if len(defaultValueMap) > 0 {
							proj := LogicalProjection{}.Init(agg.ctx, agg.blockOffset)
							proj.SetSchema(apply.schema)
							proj.Exprs = expression.Column2Exprs(apply.schema.Columns)
							for i, val := range defaultValueMap {
								pos := proj.schema.ColumnIndex(agg.schema.Columns[i])
								ifNullFunc := expression.NewFunctionInternal(agg.ctx, ast.Ifnull, types.NewFieldType(mysql.TypeLonglong), agg.schema.Columns[i], val)
								proj.Exprs[pos] = ifNullFunc
							}
							proj.SetChildren(apply)
							p = proj
						}
						return s.optimize(ctx, p)
					}
					sel.Conditions = originalExpr
					apply.CorCols = extractCorColumnsBySchema4LogicalPlan(apply.children[1], apply.children[0].Schema())
				}
			}
		} else if sort, ok := innerPlan.(*LogicalSort); ok {
			// Since we only pull up Selection, Projection, Aggregation, MaxOneRow,
			// the top level Sort has no effect on the subquery's result.
			innerPlan = sort.children[0]
			apply.SetChildren(outerPlan, innerPlan)
			return s.optimize(ctx, p)
		}
	}
	newChildren := make([]LogicalPlan, 0, len(p.Children()))
	for _, child := range p.Children() {
		np, err := s.optimize(ctx, child)
		if err != nil {
			return nil, err
		}
		newChildren = append(newChildren, np)
	}
	p.SetChildren(newChildren...)
	return p, nil
}

func (*decorrelateSolver) name() string {
	return "decorrelate"
}
