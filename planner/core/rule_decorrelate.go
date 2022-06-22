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
	"math"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/plancodec"
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
func (s *decorrelateSolver) optimize(ctx context.Context, p LogicalPlan, opt *logicalOptimizeOp) (LogicalPlan, error) {
	if apply, ok := p.(*LogicalApply); ok {
		outerPlan := apply.children[0]
		innerPlan := apply.children[1]
		apply.CorCols = extractCorColumnsBySchema4LogicalPlan(apply.children[1], apply.children[0].Schema())
		if len(apply.CorCols) == 0 {
			// If the inner plan is non-correlated, the apply will be simplified to join.
			join := &apply.LogicalJoin
			join.self = join
			join.tp = plancodec.TypeJoin
			p = join
			appendApplySimplifiedTraceStep(apply, join, opt)
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
			appendRemoveSelectionTraceStep(apply, sel, opt)
			return s.optimize(ctx, p, opt)
		} else if m, ok := innerPlan.(*LogicalMaxOneRow); ok {
			if m.children[0].MaxOneRow() {
				innerPlan = m.children[0]
				apply.SetChildren(outerPlan, innerPlan)
				appendRemoveMaxOneRowTraceStep(m, opt)
				return s.optimize(ctx, p, opt)
			}
		} else if proj, ok := innerPlan.(*LogicalProjection); ok {
			allConst := true
			for _, expr := range proj.Exprs {
				if len(expression.ExtractCorColumns(expr)) > 0 || !expression.ExtractColumnSet(expr).IsEmpty() {
					allConst = false
					break
				}
			}
			if allConst && apply.JoinType == LeftOuterJoin {
				// If the projection just references some constant. We cannot directly pull it up when the APPLY is an outer join.
				//  e.g. select (select 1 from t1 where t1.a=t2.a) from t2; When the t1.a=t2.a is false the join's output is NULL.
				//       But if we pull the projection upon the APPLY. It will return 1 since the projection is evaluated after the join.
				// We disable the decorrelation directly for now.
				// TODO: Actually, it can be optimized. We need to first push the projection down to the selection. And then the APPLY can be decorrelated.
				goto NoOptimize
			}
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
				np, err := s.optimize(ctx, p, opt)
				if err != nil {
					return nil, err
				}
				proj.SetChildren(np)
				appendMoveProjTraceStep(apply, np, proj, opt)
				return proj, nil
			}
			appendRemoveProjTraceStep(apply, proj, opt)
			return s.optimize(ctx, p, opt)
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
				resetNotNullFlag(apply.schema, outerPlan.Schema().Len(), apply.schema.Len())
				for i, aggFunc := range agg.AggFuncs {
					aggArgs := make([]expression.Expression, 0, len(aggFunc.Args))
					for _, arg := range aggFunc.Args {
						switch expr := arg.(type) {
						case *expression.Column:
							if idx := apply.schema.ColumnIndex(expr); idx != -1 {
								aggArgs = append(aggArgs, apply.schema.Columns[idx])
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
					desc, err := aggregation.NewAggFuncDesc(agg.ctx, agg.AggFuncs[i].Name, aggArgs, agg.AggFuncs[i].HasDistinct)
					if err != nil {
						return nil, err
					}
					newAggFuncs = append(newAggFuncs, desc)
				}
				agg.AggFuncs = newAggFuncs
				np, err := s.optimize(ctx, p, opt)
				if err != nil {
					return nil, err
				}
				agg.SetChildren(np)
				appendPullUpAggTraceStep(apply, np, agg, opt)
				// TODO: Add a Projection if any argument of aggregate funcs or group by items are scalar functions.
				// agg.buildProjectionIfNecessary()
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
					groupByCols := expression.NewSchema(agg.GetGroupByCols()...)
					if len(apply.CorCols) == 0 {
						appendedGroupByCols := expression.NewSchema()
						var appendedAggFuncs []*aggregation.AggFuncDesc

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
							agg.SetChildren(sel.children[0])
							appendRemoveSelectionTraceStep(agg, sel, opt)
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
							appendAddProjTraceStep(apply, proj, opt)
						}
						appendModifyAggTraceStep(outerPlan, apply, agg, sel, appendedGroupByCols, appendedAggFuncs, eqCondWithCorCol, opt)
						return s.optimize(ctx, p, opt)
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
			appendRemoveSortTraceStep(sort, opt)
			return s.optimize(ctx, p, opt)
		}
	}
NoOptimize:
	newChildren := make([]LogicalPlan, 0, len(p.Children()))
	for _, child := range p.Children() {
		np, err := s.optimize(ctx, child, opt)
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

func appendApplySimplifiedTraceStep(p *LogicalApply, j *LogicalJoin, opt *logicalOptimizeOp) {
	action := func() string {
		return fmt.Sprintf("%v_%v simplified into %v_%v", plancodec.TypeApply, p.ID(), plancodec.TypeJoin, j.ID())
	}
	reason := func() string {
		return fmt.Sprintf("%v_%v hasn't any corelated column, thus the inner plan is non-correlated", p.TP(), p.ID())
	}
	opt.appendStepToCurrent(p.ID(), p.TP(), reason, action)
}

func appendRemoveSelectionTraceStep(p LogicalPlan, s *LogicalSelection, opt *logicalOptimizeOp) {
	action := func() string {
		return fmt.Sprintf("%v_%v removed from plan tree", s.TP(), s.ID())
	}
	reason := func() string {
		return fmt.Sprintf("%v_%v's conditions have been pushed into %v_%v", s.TP(), s.ID(), p.TP(), p.ID())
	}
	opt.appendStepToCurrent(s.ID(), s.TP(), reason, action)
}

func appendRemoveMaxOneRowTraceStep(m *LogicalMaxOneRow, opt *logicalOptimizeOp) {
	action := func() string {
		return fmt.Sprintf("%v_%v removed from plan tree", m.TP(), m.ID())
	}
	reason := func() string {
		return ""
	}
	opt.appendStepToCurrent(m.ID(), m.TP(), reason, action)
}

func appendRemoveProjTraceStep(p *LogicalApply, proj *LogicalProjection, opt *logicalOptimizeOp) {
	action := func() string {
		return fmt.Sprintf("%v_%v removed from plan tree", proj.TP(), proj.ID())
	}
	reason := func() string {
		return fmt.Sprintf("%v_%v's columns all substituted into %v_%v", proj.TP(), proj.ID(), p.TP(), p.ID())
	}
	opt.appendStepToCurrent(proj.ID(), proj.TP(), reason, action)
}

func appendMoveProjTraceStep(p *LogicalApply, np LogicalPlan, proj *LogicalProjection, opt *logicalOptimizeOp) {
	action := func() string {
		return fmt.Sprintf("%v_%v is moved as %v_%v's parent", proj.TP(), proj.ID(), np.TP(), np.ID())
	}
	reason := func() string {
		return fmt.Sprintf("%v_%v's join type is %v, not semi join", p.TP(), p.ID(), p.JoinType.String())
	}
	opt.appendStepToCurrent(proj.ID(), proj.TP(), reason, action)
}

func appendRemoveSortTraceStep(sort *LogicalSort, opt *logicalOptimizeOp) {
	action := func() string {
		return fmt.Sprintf("%v_%v removed from plan tree", sort.TP(), sort.ID())
	}
	reason := func() string {
		return ""
	}
	opt.appendStepToCurrent(sort.ID(), sort.TP(), reason, action)
}

func appendPullUpAggTraceStep(p *LogicalApply, np LogicalPlan, agg *LogicalAggregation, opt *logicalOptimizeOp) {
	action := func() string {
		return fmt.Sprintf("%v_%v pulled up as %v_%v's parent, and %v_%v's join type becomes %v",
			agg.TP(), agg.ID(), np.TP(), np.ID(), p.TP(), p.ID(), p.JoinType.String())
	}
	reason := func() string {
		return fmt.Sprintf("%v_%v's functions haven't any group by items and %v_%v's join type isn't %v or %v, and hasn't any conditions",
			agg.TP(), agg.ID(), p.TP(), p.ID(), InnerJoin.String(), LeftOuterJoin.String())
	}
	opt.appendStepToCurrent(agg.ID(), agg.TP(), reason, action)
}

func appendAddProjTraceStep(p *LogicalApply, proj *LogicalProjection, opt *logicalOptimizeOp) {
	action := func() string {
		return fmt.Sprintf("%v_%v is added as %v_%v's parent", proj.TP(), proj.ID(), p.TP(), p.ID())
	}
	reason := func() string {
		return ""
	}
	opt.appendStepToCurrent(proj.ID(), proj.TP(), reason, action)
}

func appendModifyAggTraceStep(outerPlan LogicalPlan, p *LogicalApply, agg *LogicalAggregation, sel *LogicalSelection,
	appendedGroupByCols *expression.Schema, appendedAggFuncs []*aggregation.AggFuncDesc,
	eqCondWithCorCol []*expression.ScalarFunction, opt *logicalOptimizeOp) {
	action := func() string {
		buffer := bytes.NewBufferString(fmt.Sprintf("%v_%v's groupby items added [", agg.TP(), agg.ID()))
		for i, col := range appendedGroupByCols.Columns {
			if i > 0 {
				buffer.WriteString(",")
			}
			buffer.WriteString(col.String())
		}
		buffer.WriteString("], and functions added [")
		for i, f := range appendedAggFuncs {
			if i > 0 {
				buffer.WriteString(",")
			}
			buffer.WriteString(f.String())
		}
		buffer.WriteString(fmt.Sprintf("], and %v_%v's conditions added [", p.TP(), p.ID()))
		for i, cond := range eqCondWithCorCol {
			if i > 0 {
				buffer.WriteString(",")
			}
			buffer.WriteString(cond.String())
		}
		buffer.WriteString("]")
		return buffer.String()
	}
	reason := func() string {
		buffer := bytes.NewBufferString(fmt.Sprintf("%v_%v's equal conditions [", sel.TP(), sel.ID()))
		for i, cond := range eqCondWithCorCol {
			if i > 0 {
				buffer.WriteString(",")
			}
			buffer.WriteString(cond.String())
		}
		buffer.WriteString(fmt.Sprintf("] are correlated to %v_%v and pulled up as %v_%v's join key",
			outerPlan.TP(), outerPlan.ID(), p.TP(), p.ID()))
		return buffer.String()
	}
	opt.appendStepToCurrent(agg.ID(), agg.TP(), reason, action)
}
