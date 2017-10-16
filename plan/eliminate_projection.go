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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/terror"
)

// canProjectionBeEliminatedLoose checks whether a projection can be eliminated, returns true if
// every expression is a single column.
func canProjectionBeEliminatedLoose(p *Projection) bool {
	for _, expr := range p.Exprs {
		_, ok := expr.(*expression.Column)
		if !ok {
			return false
		}
	}
	return true
}

// canProjectionBeEliminatedStrict checks whether a projection can be eliminated, returns true if
// the projection just copy its child's output.
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
		colName := origin.ColName
		*origin = *dst
		origin.ColName = colName
	}
}

func resolveExprAndReplace(origin expression.Expression, replace map[string]*expression.Column) {
	switch expr := origin.(type) {
	case *expression.Column:
		resolveColumnAndReplace(expr, replace)
	case *expression.CorrelatedColumn:
		resolveColumnAndReplace(&expr.Column, replace)
	case *expression.ScalarFunction:
		for _, arg := range expr.GetArgs() {
			resolveExprAndReplace(arg, replace)
		}
	}
}

func doPhysicalProjectionElimination(p PhysicalPlan) PhysicalPlan {
	children := make([]Plan, 0, len(p.Children()))
	for _, child := range p.Children() {
		newChild := doPhysicalProjectionElimination(child.(PhysicalPlan))
		children = append(children, newChild)
	}
	setParentAndChildren(p, children...)

	proj, isProj := p.(*Projection)
	if !isProj || !canProjectionBeEliminatedStrict(proj) {
		return p
	}
	child := p.Children()[0]
	err := RemovePlan(p)
	terror.Log(errors.Trace(err))
	return child.(PhysicalPlan)
}

// eliminatePhysicalProjection should be called after physical optimization to eliminate the redundant projection
// left after logical projection elimination.
func eliminatePhysicalProjection(p PhysicalPlan) PhysicalPlan {
	oldRoot := p
	newRoot := doPhysicalProjectionElimination(p)
	if oldRoot.ID() != newRoot.ID() {
		newCols := newRoot.Schema().Columns
		for i, oldCol := range oldRoot.Schema().Columns {
			newCols[i].DBName = oldCol.DBName
			newCols[i].TblName = oldCol.TblName
			newCols[i].ColName = oldCol.ColName
			newCols[i].OrigTblName = oldCol.OrigTblName
		}
	}
	return newRoot
}

type projectionEliminater struct {
}

// optimize implements the logicalOptRule interface.
func (pe *projectionEliminater) optimize(lp LogicalPlan, _ context.Context, _ *idAllocator) (LogicalPlan, error) {
	root := pe.eliminate(lp, make(map[string]*expression.Column), false)
	return root.(LogicalPlan), nil
}

// eliminate eliminates the redundant projection in a logical plan.
func (pe *projectionEliminater) eliminate(p LogicalPlan, replace map[string]*expression.Column, canEliminate bool) LogicalPlan {
	proj, isProj := p.(*Projection)
	children := make([]Plan, 0, len(p.Children()))

	childFlag := canEliminate
	if _, isUnion := p.(*Union); isUnion {
		childFlag = false
	} else if isProj {
		childFlag = true
	}
	for _, child := range p.Children() {
		children = append(children, pe.eliminate(child.(LogicalPlan), replace, childFlag))
	}
	setParentAndChildren(p, children...)

	switch p.(type) {
	case *Sort, *TopN, *Limit, *Selection, *MaxOneRow, *Update, *SelectLock:
		p.SetSchema(p.Children()[0].Schema())
	case *LogicalJoin, *LogicalApply:
		var joinTp JoinType
		if _, isApply := p.(*LogicalApply); isApply {
			joinTp = p.(*LogicalApply).JoinType
		} else {
			joinTp = p.(*LogicalJoin).JoinType
		}
		switch joinTp {
		case InnerJoin, LeftOuterJoin, RightOuterJoin:
			p.SetSchema(expression.MergeSchema(p.Children()[0].Schema(), p.Children()[1].Schema()))
		case SemiJoin:
			p.SetSchema(p.Children()[0].Schema().Clone())
		case LeftOuterSemiJoin:
			newSchema := p.Children()[0].Schema().Clone()
			newSchema.Append(p.Schema().Columns[len(p.Schema().Columns)-1])
			p.SetSchema(newSchema)
		}
	default:
		for _, dst := range p.Schema().Columns {
			resolveColumnAndReplace(dst, replace)
		}
	}
	p.replaceExprColumns(replace)

	if !(isProj && canEliminate && canProjectionBeEliminatedLoose(proj)) {
		return p
	}

	child := p.Children()[0]
	exprs := proj.Exprs
	for i, col := range proj.Schema().Columns {
		replace[string(col.HashCode())] = exprs[i].(*expression.Column)
	}
	err := RemovePlan(p)
	terror.Log(errors.Trace(err))
	return child.(LogicalPlan)
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
