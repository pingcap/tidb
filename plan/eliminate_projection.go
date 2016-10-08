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

// EliminateProjection eliminates projection operator to avoid the cost of memory copy in the iterator of projection.
func EliminateProjection(p LogicalPlan) LogicalPlan {
	switch plan := p.(type) {
	case *Projection:
		if !projectionCanBeEliminated(plan) {
			break
		}
		child := p.GetChildByIndex(0).(LogicalPlan)
		// pointer of schema in PROJECTION operator may be referenced by parent operator,
		// and attributes of child operator may be used later, so here we shallow copy child's schema
		// to the schema of PROJECTION, and reset the child's schema as the schema of PROJECTION.
		for i, col := range plan.GetSchema() {
			plan.GetSchema()[i] = shallowCopyColumn(col, child.GetSchema()[i])
		}
		child.SetSchema(plan.GetSchema())
		RemovePlan(p)
		p = EliminateProjection(child)
	case *DataSource:
		// predicates may be pushed down when build physical plan, and the schema of Selection operator is
		// always the same as the child operator, so here we copy the schema of Selection to DataSource.
		if sel, ok := plan.GetParentByIndex(0).(*Selection); ok {
			plan.SetSchema(sel.GetSchema())
			for i, cond := range sel.Conditions {
				sel.Conditions[i], _ = retrieveColumnsInExpression(cond, plan.GetSchema())
			}
		}
	}
	if len(p.GetChildren()) == 1 {
		child := p.GetChildByIndex(0)
		p.ReplaceChild(child, EliminateProjection(child.(LogicalPlan)))
	} else {
		children := make([]Plan, 0, len(p.GetChildren()))
		for _, child := range p.GetChildren() {
			children = append(children, EliminateProjection(child.(LogicalPlan)))
		}
		p.SetChildren(children...)
	}
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

// projectionCanBeEliminated checks if a PROJECTION operator can be eliminated.
// PROJECTION operator can be eliminated when meet the following conditions at the same time:
// 1. fields of PROJECTION are all columns
// 2. fields of PROJECTION are just the same as the schema of the child operator (including order, amount, etc.).
// expressions like following cases can not be eliminated:
// "SELECT b, a from t",
// or "SELECT c AS a, c AS b FROM t WHERE d = 1",
// or "select t1.a, t2.b, t1.b, t2.a from t1, t2 where t1.a < 0 and t2.b > 0".
func projectionCanBeEliminated(p *Projection) bool {
	child := p.GetChildByIndex(0).(LogicalPlan)
	if len(p.GetSchema()) != len(child.GetSchema()) {
		return false
	}
	for i, expr := range p.Exprs {
		col, ok := expr.(*expression.Column)
		if !ok || col.Correlated {
			return false
		}
		if col.FromID != child.GetSchema()[i].FromID || col.Position != child.GetSchema()[i].Position {
			return false
		}
	}
	return true
}
