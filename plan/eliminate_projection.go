// Copyright 2016 PingCAP, Inc.
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
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
)

// canLogicalProjectionBeEliminated checks if a PROJECTION in a logical plan can be eliminated.
// Projection p can be eliminated when all the following conditions are satisfied:
// 1. it's not the root operator
// 2. canProjectionBeEliminated(p) returns true
func canLogicalProjectionBeEliminated(p *Projection) bool {
	if len(p.Parents()) == 0 {
		return false
	}
	return canProjectionBeEliminated(p)
}

// canProjectionBeEliminated checks if a PROJECTION in a physical plan can be eliminated.
// Projection p can be eliminated when all the following conditions are satisfied:
// 1. fields of PROJECTION are all columns
// 2. fields of PROJECTION are just the same as the schema of the child operator (including order, amount, etc.).
// expressions like following cases can not be eliminated:
// "SELECT b, a FROM t",
// or "SELECT c AS a, c AS b FROM t WHERE d = 1",
// or "SELECT t1.a, t2.b, t1.b, t2.a FROM t1, t2 WHERE t1.a < 0 AND t2.b > 0".
func canProjectionBeEliminated(p *Projection) bool {
	child := p.Children()[0]
	if p.Schema().Len() != child.Schema().Len() {
		return false
	}
	childCols := child.Schema().Columns
	for i, expr := range p.Exprs {
		projCol, ok := expr.(*expression.Column)
		if !ok || !projCol.Equal(childCols[i], nil) {
			return false
		}
	}
	return true
}

func resolveColumnAndReplace(origin *expression.Column, replace map[string]*expression.Column) {
	dst := replace[string(origin.HashCode())]
	if dst != nil {
		*origin = *dst
	}
}

func resolveExprAndReplace(origin expression.Expression, replace map[string]*expression.Column) {
	switch expr := origin.(type) {
	case *expression.Column:
		resolveColumnAndReplace(expr, replace)
	case *expression.ScalarFunction:
		for _, arg := range expr.GetArgs() {
			resolveExprAndReplace(arg, replace)
		}
	}
}

func eliminateRootProjection(p Plan) Plan {
	plan, ok := p.(*Projection)
	if ok && canProjectionBeEliminated(plan) {
		child := plan.Children()[0]
		childColumns := child.Schema().Columns
		for i, parentColumn := range plan.Schema().Columns {
			childColumns[i].ColName = parentColumn.ColName
		}
		RemovePlan(p)
		return child
	}
	return p
}

type nonRootProjectionEliminater struct {
}

func (pe *nonRootProjectionEliminater) optimize(lp LogicalPlan, _ context.Context, _ *idAllocator) (LogicalPlan, error) {
	return pe.eliminate(lp, make(map[string]*expression.Column)).(LogicalPlan), nil
}

func (pe *nonRootProjectionEliminater) eliminate(p Plan, replace map[string]*expression.Column) Plan {
	children := make([]Plan, 0, len(p.Children()))
	for _, child := range p.Children() {
		children = append(children, pe.eliminate(child, replace))
	}
	p.SetChildren(children...)
	for _, dst := range p.Schema().Columns {
		resolveColumnAndReplace(dst, replace)
	}
	for _, key := range p.Schema().Keys {
		for _, keyCol := range key {
			resolveColumnAndReplace(keyCol, replace)
		}
	}
	p.(LogicalPlan).replaceExprColumns(replace)
	plan, ok := p.(*Projection)
	if ok && canLogicalProjectionBeEliminated(plan) {
		child := plan.Children()[0]
		childColumns := child.Schema().Columns
		for i, parentColumn := range plan.Schema().Columns {
			childColumns[i].ColName = parentColumn.ColName
			replace[string(parentColumn.HashCode())] = childColumns[i]
		}
		RemovePlan(p)
		return child
	}
	return p
}

func (p *LogicalJoin) replaceExprColumns(replace map[string]*expression.Column) {
	for _, equalExpr := range p.EqualConditions {
		resolveExprAndReplace(equalExpr, replace)
	}
	for _, leftExpr := range p.LeftConditions {
		resolveExprAndReplace(leftExpr, replace)
	}
	for _, rightExpr := range p.RightConditions {
		resolveExprAndReplace(rightExpr, replace)
	}
	for _, otherExpr := range p.OtherConditions {
		resolveExprAndReplace(otherExpr, replace)
	}
	for _, leftKey := range p.LeftJoinKeys {
		resolveColumnAndReplace(leftKey, replace)
	}
	for _, rightKey := range p.RightJoinKeys {
		resolveColumnAndReplace(rightKey, replace)
	}
	for _, leftProp := range p.leftProperties {
		for _, prop := range leftProp {
			resolveColumnAndReplace(prop, replace)
		}
	}
	for _, rightProp := range p.rightProperties {
		for _, prop := range rightProp {
			resolveColumnAndReplace(prop, replace)
		}
	}
}

func (p *Projection) replaceExprColumns(replace map[string]*expression.Column) {
	for _, expr := range p.Exprs {
		resolveExprAndReplace(expr, replace)
	}
}

func (p *LogicalAggregation) replaceExprColumns(replace map[string]*expression.Column) {
	for _, agg := range p.AggFuncs {
		for _, aggExpr := range agg.GetArgs() {
			resolveExprAndReplace(aggExpr, replace)
		}
	}
	for _, gbyItem := range p.GroupByItems {
		resolveExprAndReplace(gbyItem, replace)
	}
	p.collectGroupByColumns()
}

func (p *Selection) replaceExprColumns(replace map[string]*expression.Column) {
	for _, expr := range p.Conditions {
		resolveExprAndReplace(expr, replace)
	}
}

func (p *LogicalApply) replaceExprColumns(replace map[string]*expression.Column) {
	p.LogicalJoin.replaceExprColumns(replace)
	for _, coCol := range p.corCols {
		dst := replace[string(coCol.Column.HashCode())]
		if dst != nil {
			coCol.Column = *dst
		}
	}
}

func (p *Sort) replaceExprColumns(replace map[string]*expression.Column) {
	for _, byItem := range p.ByItems {
		resolveExprAndReplace(byItem.Expr, replace)
	}
}

func (p *TopN) replaceExprColumns(replace map[string]*expression.Column) {
	for _, byItem := range p.ByItems {
		resolveExprAndReplace(byItem.Expr, replace)
	}
}
