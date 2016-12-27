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
func EliminateProjection(p PhysicalPlan) PhysicalPlan {
	switch plan := p.(type) {
	case *Projection:
		if !projectionCanBeEliminated(plan) {
			break
		}
		child := p.GetChildByIndex(0).(PhysicalPlan)
		child.SetSchema(plan.GetSchema())
		RemovePlan(p)
		p = EliminateProjection(child)
	}
	children := make([]Plan, 0, len(p.GetChildren()))
	for _, child := range p.GetChildren() {
		children = append(children, EliminateProjection(child.(PhysicalPlan)))
	}
	p.SetChildren(children...)
	return p
}

// projectionCanBeEliminated checks if a PROJECTION operator can be eliminated.
// PROJECTION operator can be eliminated when meet the following conditions at the same time:
// 1. fields of PROJECTION are all columns
// 2. fields of PROJECTION are just the same as the schema of the child operator (including order, amount, etc.).
// expressions like following cases can not be eliminated:
// "SELECT b, a FROM t",
// or "SELECT c AS a, c AS b FROM t WHERE d = 1",
// or "SELECT t1.a, t2.b, t1.b, t2.a FROM t1, t2 WHERE t1.a < 0 AND t2.b > 0".
func projectionCanBeEliminated(p *Projection) bool {
	child := p.GetChildByIndex(0).(PhysicalPlan)
	if p.GetSchema().Len() != child.GetSchema().Len() {
		return false
	}
	for i, expr := range p.Exprs {
		col, ok := expr.(*expression.Column)
		if !ok {
			return false
		}
		if col.FromID != child.GetSchema().Columns[i].FromID || col.Position != child.GetSchema().Columns[i].Position {
			return false
		}
	}
	return true
}
