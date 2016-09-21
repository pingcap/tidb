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
	if reorder {
		newSchema := make(expression.Schema, len(orderedSchema))
		for i, v1 := range orderedSchema {
			for j, v2 := range p.GetSchema() {
				if v1.TblName == v2.TblName && v1.ColName == v2.ColName {
					newSchema[i] = p.GetSchema()[j]
					break
				}
			}
		}
		p.SetSchema(newSchema)
	}
	switch plan := p.(type) {
	case *Projection:
		canBeEliminated := true
		child := p.GetChildByIndex(0).(LogicalPlan)
		// only fields in PROJECTION are all Columns might be eliminated.
		for _, expr := range plan.Exprs {
			if _, ok := expr.(*expression.Column); !ok {
				canBeEliminated = false
				break
			}
		}
		// detect expression like "SELECT c AS a, c AS b FROM t"which cannot be eliminated.
		if canBeEliminated && len(plan.GetSchema()) != len(child.GetSchema()) {
			canBeEliminated = false
		}
		if !canBeEliminated {
			break
		}
		// detect expression like "SELECT c AS a, c AS b FROM t WHERE d = 1" which cannot be eliminated.
		for _, col := range child.GetSchema() {
			if !canBeEliminated {
				break
			}
			for _, expr := range plan.Exprs {
				if col.TblName == expr.(*expression.Column).TblName && col.ColName == expr.(*expression.Column).ColName {
					canBeEliminated = true
					break
				}
				canBeEliminated = false
			}
		}
		if !canBeEliminated {
			break
		}
		// eliminate projection.
		reorder = false
		orderedSchema = make(expression.Schema, len(plan.Exprs))
		newSchema := make(expression.Schema, len(child.GetSchema()))
		for j, col := range child.GetSchema() {
			for i, expr := range plan.Exprs {
				if col.TblName == expr.(*expression.Column).TblName && col.ColName == expr.(*expression.Column).ColName {
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
		if !reorder {
			break
		}
		newColumns := make([]*model.ColumnInfo, len(plan.Columns))
		for i, s := range orderedSchema { // reorder DataSource's columns.
			isMultiTable := false
			for _, col := range plan.Columns {
				if s.TblName.O == plan.Table.Name.O && s.ColName == col.Name {
					newColumns[i] = col
					isMultiTable = false
					break
				}
				isMultiTable = true
			}
			if isMultiTable {
				return p
			}
		}
		plan.Columns = newColumns
	default:
		children := make([]Plan, 0, len(p.GetChildren()))
		for _, child := range p.GetChildren() {
			children = append(children, EliminateProjection(child.(LogicalPlan), reorder, orderedSchema))
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
