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
	"github.com/pingcap/tidb/expression"
)

// canProjectionBeEliminatedLoose checks whether a projection can be eliminated, returns true if
// every expression is a single column.
func canProjectionBeEliminatedLoose(p *LogicalProjection) bool {
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
func canProjectionBeEliminatedStrict(p *PhysicalProjection) bool {
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
	for i, child := range p.Children() {
		p.Children()[i] = doPhysicalProjectionElimination(child)
	}

	proj, isProj := p.(*PhysicalProjection)
	if !isProj || !canProjectionBeEliminatedStrict(proj) {
		return p
	}
	child := p.Children()[0]
	return child
}

// eliminatePhysicalProjection should be called after physical optimization to eliminate the redundant projection
// left after logical projection elimination.
func eliminatePhysicalProjection(p PhysicalPlan) PhysicalPlan {
	oldSchema := p.Schema()
	newRoot := doPhysicalProjectionElimination(p)
	newCols := newRoot.Schema().Columns
	for i, oldCol := range oldSchema.Columns {
		newCols[i].DBName = oldCol.DBName
		newCols[i].TblName = oldCol.TblName
		newCols[i].ColName = oldCol.ColName
		newCols[i].OrigTblName = oldCol.OrigTblName
	}
	return newRoot
}

type projectionEliminater struct {
}

// optimize implements the logicalOptRule interface.
func (pe *projectionEliminater) optimize(lp LogicalPlan) (LogicalPlan, error) {
	root := pe.eliminate(lp, make(map[string]*expression.Column), false)
	return root, nil
}

// eliminate eliminates the redundant projection in a logical plan.
func (pe *projectionEliminater) eliminate(p LogicalPlan, replace map[string]*expression.Column, canEliminate bool) LogicalPlan {
	proj, isProj := p.(*LogicalProjection)
	childFlag := canEliminate
	if _, isUnion := p.(*LogicalUnionAll); isUnion {
		childFlag = false
	} else if _, isAgg := p.(*LogicalAggregation); isAgg || isProj {
		childFlag = true
	}
	for i, child := range p.Children() {
		p.Children()[i] = pe.eliminate(child, replace, childFlag)
	}

	switch x := p.(type) {
	case *LogicalJoin:
		x.schema = buildLogicalJoinSchema(x.JoinType, x)
	case *LogicalApply:
		x.schema = buildLogicalJoinSchema(x.JoinType, x)
	default:
		for _, dst := range p.Schema().Columns {
			resolveColumnAndReplace(dst, replace)
		}
	}
	p.replaceExprColumns(replace)

	if !(isProj && canEliminate && canProjectionBeEliminatedLoose(proj)) {
		return p
	}
	exprs := proj.Exprs
	for i, col := range proj.Schema().Columns {
		replace[string(col.HashCode())] = exprs[i].(*expression.Column)
	}
	return p.Children()[0]
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

func (p *LogicalProjection) replaceExprColumns(replace map[string]*expression.Column) {
	for _, expr := range p.Exprs {
		resolveExprAndReplace(expr, replace)
	}
}

func (la *LogicalAggregation) replaceExprColumns(replace map[string]*expression.Column) {
	for _, agg := range la.AggFuncs {
		for _, aggExpr := range agg.Args {
			resolveExprAndReplace(aggExpr, replace)
		}
	}
	for _, gbyItem := range la.GroupByItems {
		resolveExprAndReplace(gbyItem, replace)
	}
	la.collectGroupByColumns()
}

func (p *LogicalSelection) replaceExprColumns(replace map[string]*expression.Column) {
	for _, expr := range p.Conditions {
		resolveExprAndReplace(expr, replace)
	}
}

func (la *LogicalApply) replaceExprColumns(replace map[string]*expression.Column) {
	la.LogicalJoin.replaceExprColumns(replace)
	for _, coCol := range la.corCols {
		dst := replace[string(coCol.Column.HashCode())]
		if dst != nil {
			coCol.Column = *dst
		}
	}
}

func (ls *LogicalSort) replaceExprColumns(replace map[string]*expression.Column) {
	for _, byItem := range ls.ByItems {
		resolveExprAndReplace(byItem.Expr, replace)
	}
}

func (lt *LogicalTopN) replaceExprColumns(replace map[string]*expression.Column) {
	for _, byItem := range lt.ByItems {
		resolveExprAndReplace(byItem.Expr, replace)
	}
}
