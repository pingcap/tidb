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
	"github.com/pingcap/tidb/model"
)

// EliminateProjection eliminate projection operator to avoid the cost of memory copy in the iterator of projection.
func EliminateProjection(p LogicalPlan, reorder bool, orderedSchema expression.Schema) LogicalPlan {
	switch plan := p.(type) {
	case *Projection:
		if !projectionCanBeEliminated(plan) {
			break
		}
		child := p.GetChildByIndex(0).(LogicalPlan)
		reorder = false
		orderedSchema = make(expression.Schema, len(plan.Exprs))
		newSchema := make(expression.Schema, len(child.GetSchema()))
		for j, col := range child.GetSchema() {
			for i, expr := range plan.Exprs {
				if col.FromID == expr.(*expression.Column).FromID && col.Position == expr.(*expression.Column).Position {
					if i != j {
						reorder = true
					}
					orderedSchema[i] = expr.(*expression.Column)
					newSchema[i] = shallowCopyColumn(plan.GetSchema()[i], col)
					break
				}
			}
		}
		newSchema.InitIndices()
		child.SetSchema(newSchema)
		RemovePlan(p)
		p = EliminateProjection(child, reorder, orderedSchema)
	case *DataSource:
		if sel, ok := plan.GetParentByIndex(0).(*Selection); ok { // reorder schema.
			plan.SetSchema(sel.GetSchema())
			for i, cond := range sel.Conditions {
				sel.Conditions[i], _ = retrieveColumnsInExpression(cond, plan.GetSchema())
			}
		}
		if !reorder {
			break
		}
		newColumns := make([]*model.ColumnInfo, 0, len(plan.Columns))
		for _, s := range orderedSchema { // reorder DataSource's columns.
			for _, col := range plan.Columns {
				if s.ColName == col.Name {
					newColumns = append(newColumns, col)
					break
				}
			}
		}
		plan.Columns = newColumns
	}
	children := make([]Plan, 0, len(p.GetChildren()))
	for _, child := range p.GetChildren() {
		children = append(children, EliminateProjection(child.(LogicalPlan), reorder, orderedSchema))
	}
	p.SetChildren(children...)
	return p
}

func shallowCopyColumn(colDest, colSrc *expression.Column) *expression.Column {
	colDest.Correlated = colSrc.Correlated
	colDest.FromID = colSrc.FromID
	colDest.Position = colSrc.Position
	colDest.ID = colSrc.ID
	colDest.IsAggOrSubq = colSrc.IsAggOrSubq
	colDest.RetType = colSrc.RetType

	return colDest
}

func projectionCanBeEliminated(p *Projection) bool {
	child := p.GetChildByIndex(0).(LogicalPlan)
	// only fields in PROJECTION are all Columns might be eliminated.
	for _, expr := range p.Exprs {
		if _, ok := expr.(*expression.Column); !ok {
			return false
		}
	}
	// PROJECTION above Join can not be eliminated.
	for i := 0; i < len(p.Exprs)-1; i++ {
		if p.Exprs[i].(*expression.Column).TblName != p.Exprs[i+1].(*expression.Column).TblName {
			return false
		}
	}
	// detect expression like "SELECT c AS a, c AS b FROM t" which cannot be eliminated.
	if len(p.GetSchema()) != len(child.GetSchema()) {
		return false
	}
	// detect expression like "SELECT c AS a, c AS b FROM t WHERE d = 1" which cannot be eliminated.
	canBeEliminated := true
	for _, col := range child.GetSchema() {
		for _, expr := range p.Exprs {
			if col.FromID == expr.(*expression.Column).FromID && col.Position == expr.(*expression.Column).Position {
				canBeEliminated = true
				break
			}
			canBeEliminated = false
		}
		if !canBeEliminated {
			return false
		}
	}
	return true
}
