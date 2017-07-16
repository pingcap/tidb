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
	"github.com/pingcap/tidb/model"
)

func canProjectionBeEliminatedLoose(p *Projection) bool {
	child := p.Children()[0]
	if p.Schema().Len() != child.Schema().Len() {
		return false
	}
	for _, expr := range p.Exprs {
		_, ok := expr.(*expression.Column)
		if !ok {
			return false
		}
	}
	return true
}

func canProjectionBeEliminatedStrict(p *Projection) bool {
	child := p.Children()[0]
	if p.Schema().Len() != child.Schema().Len() {
		return false
	}
	for i, expr := range p.Exprs {
		col, ok := expr.(*expression.Column)
		if !ok || !col.Equal(child.Schema().Columns[i], nil) {
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

func eliminatePhysicalProjection(p Plan, replace map[string]*expression.Column) Plan {
	children := make([]Plan, 0, len(p.Children()))
	for _, child := range p.Children() {
		children = append(children, eliminatePhysicalProjection(child, replace))
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
	p.replaceExprColumns(replace)

	proj, isProj := p.(*Projection)
	if !isProj || !canProjectionBeEliminatedStrict(proj) {
		return p
	}
	child := p.Children()[0]
	exprs := proj.Exprs
	childCols := child.Schema().Columns
	for i, parentColumn := range proj.Schema().Columns {
		col, _ := exprs[i].(*expression.Column)
		col.DBName = parentColumn.DBName
		col.TblName = parentColumn.TblName
		col.ColName = parentColumn.ColName
		childCols[i].ColName = parentColumn.ColName
		replace[string(parentColumn.HashCode())] = col
	}
	RemovePlan(p)
	return child
}

type projectionEliminater struct {
}

func (pe *projectionEliminater) optimize(lp LogicalPlan, _ context.Context, _ *idAllocator) (LogicalPlan, error) {
	root := pe.eliminate(lp, make(map[string]*expression.Column), false)
	return root.(LogicalPlan), nil
}

func (pe *projectionEliminater) eliminate(p Plan, replace map[string]*expression.Column, hasOrderKeeper bool) Plan {
	proj, isProj := p.(*Projection)
	_, isJoin := p.(*LogicalJoin)
	_, isUnion := p.(*Union)
	_, isSort := p.(*Sort)

	canChangeOrder := isProj || isJoin
	haveToKeepOrder := isUnion || isSort

	children := make([]Plan, 0, len(p.Children()))
	for _, child := range p.Children() {
		if haveToKeepOrder {
			children = append(children, pe.eliminate(child, replace, false))
		} else {
			children = append(children, pe.eliminate(child, replace, hasOrderKeeper || canChangeOrder))
		}

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
	p.replaceExprColumns(replace)

	if !isProj {
		return p
	}
	child := p.Children()[0]

	_, isChildJoin := child.(*LogicalJoin)
	if !hasOrderKeeper && isChildJoin {
		return p
	}

	changeOrder := false
	if isProj && !canProjectionBeEliminatedStrict(proj) && canProjectionBeEliminatedLoose(proj) {
		changeOrder = true
	}

	if (changeOrder && hasOrderKeeper) || canProjectionBeEliminatedStrict(proj) {
		colNameMap := make(map[string]model.CIStr)
		exprs := proj.Exprs
		for i, parentColumn := range proj.Schema().Columns {
			col, _ := exprs[i].(*expression.Column)
			col.DBName = parentColumn.DBName
			col.TblName = parentColumn.TblName
			col.ColName = parentColumn.ColName
			replace[string(parentColumn.HashCode())] = col
			colNameMap[string(col.HashCode())] = parentColumn.ColName
		}
		for _, childCol := range child.Schema().Columns {
			if aliasColName, exist := colNameMap[string(childCol.HashCode())]; exist {
				childCol.ColName = aliasColName
			}
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
