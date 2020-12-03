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

package core

import (
	"context"

	"github.com/pingcap/tidb/expression"
)

// canProjectionBeEliminatedLoose checks whether a projection can be eliminated,
// returns true if every expression is a single column.
func canProjectionBeEliminatedLoose(p *LogicalProjection) bool {
	for _, expr := range p.Exprs {
		_, ok := expr.(*expression.Column)
		if !ok {
			return false
		}
	}
	return true
}

// canProjectionBeEliminatedStrict checks whether a projection can be
// eliminated, returns true if the projection just copy its child's output.
func canProjectionBeEliminatedStrict(p *PhysicalProjection) bool {
	// If this projection is specially added for `DO`, we keep it.
	if p.CalculateNoDelay {
		return false
	}
	if p.Schema().Len() == 0 {
		return true
	}
	child := p.Children()[0]
	if p.Schema().Len() != child.Schema().Len() {
		return false
	}
	for i, expr := range p.Exprs {
		col, ok := expr.(*expression.Column)
		if !ok || !col.Equal(nil, child.Schema().Columns[i]) {
			return false
		}
	}
	return true
}

func resolveColumnAndReplace(origin *expression.Column, replace map[string]*expression.Column) {
	dst := replace[string(origin.HashCode(nil))]
	if dst != nil {
		retType, inOperand := origin.RetType, origin.InOperand
		*origin = *dst
		origin.RetType, origin.InOperand = retType, inOperand
	}
}

// ResolveExprAndReplace replaces columns fields of expressions by children logical plans.
func ResolveExprAndReplace(origin expression.Expression, replace map[string]*expression.Column) {
	switch expr := origin.(type) {
	case *expression.Column:
		resolveColumnAndReplace(expr, replace)
	case *expression.CorrelatedColumn:
		resolveColumnAndReplace(&expr.Column, replace)
	case *expression.ScalarFunction:
		for _, arg := range expr.GetArgs() {
			ResolveExprAndReplace(arg, replace)
		}
	}
}

func doPhysicalProjectionElimination(p PhysicalPlan) PhysicalPlan {
	for i, child := range p.Children() {
		p.Children()[i] = doPhysicalProjectionElimination(child)
	}

	// eliminate projection in a coprocessor task
	tableReader, isTableReader := p.(*PhysicalTableReader)
	if isTableReader {
		tableReader.tablePlan = eliminatePhysicalProjection(tableReader.tablePlan)
		tableReader.TablePlans = flattenPushDownPlan(tableReader.tablePlan)
		return p
	}

	proj, isProj := p.(*PhysicalProjection)
	if !isProj || !canProjectionBeEliminatedStrict(proj) {
		return p
	}
	child := p.Children()[0]
	return child
}

// eliminatePhysicalProjection should be called after physical optimization to
// eliminate the redundant projection left after logical projection elimination.
func eliminatePhysicalProjection(p PhysicalPlan) PhysicalPlan {
	oldSchema := p.Schema()
	newRoot := doPhysicalProjectionElimination(p)
	newCols := newRoot.Schema().Columns
	for i, oldCol := range oldSchema.Columns {
		oldCol.Index = newCols[i].Index
		oldCol.ID = newCols[i].ID
		oldCol.UniqueID = newCols[i].UniqueID
		oldCol.VirtualExpr = newCols[i].VirtualExpr
		newRoot.Schema().Columns[i] = oldCol
	}
	return newRoot
}

type projectionEliminator struct {
}

// optimize implements the logicalOptRule interface.
func (pe *projectionEliminator) optimize(ctx context.Context, lp LogicalPlan) (LogicalPlan, error) {
	root := pe.eliminate(lp, make(map[string]*expression.Column), false)
	return root, nil
}

// eliminate eliminates the redundant projection in a logical plan.
func (pe *projectionEliminator) eliminate(p LogicalPlan, replace map[string]*expression.Column, canEliminate bool) LogicalPlan {
	proj, isProj := p.(*LogicalProjection)
	childFlag := canEliminate
	if _, isUnion := p.(*LogicalUnionAll); isUnion {
		childFlag = false
	} else if _, isAgg := p.(*LogicalAggregation); isAgg || isProj {
		childFlag = true
	} else if _, isWindow := p.(*LogicalWindow); isWindow {
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
	if isProj {
		if child, ok := p.Children()[0].(*LogicalProjection); ok && !ExprsHasSideEffects(child.Exprs) {
			for i := range proj.Exprs {
				proj.Exprs[i] = expression.FoldConstant(ReplaceColumnOfExpr(proj.Exprs[i], child, child.Schema()))
			}
			p.Children()[0] = child.Children()[0]
		}
	}

	if !(isProj && canEliminate && canProjectionBeEliminatedLoose(proj)) {
		return p
	}
	exprs := proj.Exprs
	for i, col := range proj.Schema().Columns {
		replace[string(col.HashCode(nil))] = exprs[i].(*expression.Column)
	}
	return p.Children()[0]
}

// ReplaceColumnOfExpr replaces column of expression by another LogicalProjection.
func ReplaceColumnOfExpr(expr expression.Expression, proj *LogicalProjection, schema *expression.Schema) expression.Expression {
	switch v := expr.(type) {
	case *expression.Column:
		idx := schema.ColumnIndex(v)
		if idx != -1 && idx < len(proj.Exprs) {
			return proj.Exprs[idx]
		}
	case *expression.ScalarFunction:
		for i := range v.GetArgs() {
			v.GetArgs()[i] = ReplaceColumnOfExpr(v.GetArgs()[i], proj, schema)
		}
	}
	return expr
}

func (p *LogicalJoin) replaceExprColumns(replace map[string]*expression.Column) {
	for _, equalExpr := range p.EqualConditions {
		ResolveExprAndReplace(equalExpr, replace)
	}
	for _, leftExpr := range p.LeftConditions {
		ResolveExprAndReplace(leftExpr, replace)
	}
	for _, rightExpr := range p.RightConditions {
		ResolveExprAndReplace(rightExpr, replace)
	}
	for _, otherExpr := range p.OtherConditions {
		ResolveExprAndReplace(otherExpr, replace)
	}
}

func (p *LogicalProjection) replaceExprColumns(replace map[string]*expression.Column) {
	for _, expr := range p.Exprs {
		ResolveExprAndReplace(expr, replace)
	}
}

func (la *LogicalAggregation) replaceExprColumns(replace map[string]*expression.Column) {
	for _, agg := range la.AggFuncs {
		for _, aggExpr := range agg.Args {
			ResolveExprAndReplace(aggExpr, replace)
		}
	}
	for _, gbyItem := range la.GroupByItems {
		ResolveExprAndReplace(gbyItem, replace)
	}
}

func (p *LogicalSelection) replaceExprColumns(replace map[string]*expression.Column) {
	for _, expr := range p.Conditions {
		ResolveExprAndReplace(expr, replace)
	}
}

func (la *LogicalApply) replaceExprColumns(replace map[string]*expression.Column) {
	la.LogicalJoin.replaceExprColumns(replace)
	for _, coCol := range la.CorCols {
		dst := replace[string(coCol.Column.HashCode(nil))]
		if dst != nil {
			coCol.Column = *dst
		}
	}
}

func (ls *LogicalSort) replaceExprColumns(replace map[string]*expression.Column) {
	for _, byItem := range ls.ByItems {
		ResolveExprAndReplace(byItem.Expr, replace)
	}
}

func (lt *LogicalTopN) replaceExprColumns(replace map[string]*expression.Column) {
	for _, byItem := range lt.ByItems {
		ResolveExprAndReplace(byItem.Expr, replace)
	}
}

func (p *LogicalWindow) replaceExprColumns(replace map[string]*expression.Column) {
	for _, desc := range p.WindowFuncDescs {
		for _, arg := range desc.Args {
			ResolveExprAndReplace(arg, replace)
		}
	}
	for _, item := range p.PartitionBy {
		resolveColumnAndReplace(item.Col, replace)
	}
	for _, item := range p.OrderBy {
		resolveColumnAndReplace(item.Col, replace)
	}
}

func (*projectionEliminator) name() string {
	return "projection_eliminate"
}
