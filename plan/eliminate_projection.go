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
func EliminateProjection(p LogicalPlan) LogicalPlan {
	children := make([]Plan, 0, len(p.GetChildren()))
	for _, child := range p.GetChildren() {
		children = append(children, EliminateProjection(child.(LogicalPlan)))
	}
	p.SetChildren(children...)
	if _, ok := p.(*Projection); !ok {
		return p
	}
	exprs := p.(*Projection).Exprs
	newSchema := make(expression.Schema, 0, len(exprs))
	canBeEliminated := true
	// only fields in PROJECTION are all Columns might be eliminated.
	for _, expr := range exprs {
		if col, ok := expr.(*expression.Column); ok {
			newSchema = append(newSchema, col.DeepCopy().(*expression.Column))
		} else {
			canBeEliminated = false
			break
		}
	}
	if !canBeEliminated {
		return p
	}
	child := p.GetChildByIndex(0)
	// detect expression like "SELECT c AS a, c AS b FROM t" which cannot be eliminated.
	if len(newSchema) != len(child.GetSchema()) {
		canBeEliminated = false
	}
	if !canBeEliminated {
		return p
	}
	// detect expression like "SELECT c AS a, c AS b FROM t WHERE d = 1" which cannot be eliminated.
	for _, col := range child.GetSchema() {
		for j, v := range newSchema {
			if v.TblName == col.TblName && v.ColName == col.ColName {
				canBeEliminated = true
				newSchema[j].FromID = col.FromID
				newSchema[j].Position = col.Position
				newSchema[j].Index = col.Index
				newSchema[j].TblName = p.GetSchema()[j].TblName
				newSchema[j].ColName = p.GetSchema()[j].ColName
				break
			}
			canBeEliminated = false
		}
		if !canBeEliminated {
			return p
		}
	}
	if v, ok := child.(*DataSource); ok { // reorder columns of DataSource
		newColumn := make([]*model.ColumnInfo, len(v.Columns))
		for i, x := range newSchema {
			for _, col := range v.Columns {
				if col.Offset == x.Position {
					newColumn[i] = col
				}
			}
		}
		v.Columns = newColumn
	}
	child.SetSchema(newSchema)
	if len(p.GetParents()) != 0 {
		if parent, ok := p.GetParents()[0].(*Projection); ok {
			originFromID := p.GetID()
			for i, expr := range parent.Exprs {
				parent.Exprs[i] = resetParentExprs(expr, originFromID, newSchema)
				parent.Exprs[i], _ = retrieveColumnsInExpression(parent.Exprs[i], newSchema)
			}
		}
	}
	RemovePlan(p)
	return child.(LogicalPlan)
}

func resetParentExprs(expr expression.Expression, origin string, schema expression.Schema) expression.Expression {
	switch v := expr.(type) {
	case *expression.ScalarFunction:
		for i, arg := range v.Args {
			newExpr := resetParentExprs(arg, origin, schema)
			v.Args[i] = newExpr
		}
	case *expression.Column:
		if v.FromID == origin {
			v.FromID = schema[v.Index].FromID
			v.Position = schema[v.Index].Position
			return v
		}
	}
	return expr
}
