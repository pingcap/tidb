// Copyright 2017 PingCAP, Inc.
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
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/planner/util"
)

// preparePossibleProperties traverses the plan tree by a post-order method,
// recursively calls LogicalPlan PreparePossibleProperties interface.
func preparePossibleProperties(lp LogicalPlan) [][]*expression.Column {
	childrenProperties := make([][][]*expression.Column, 0, len(lp.Children()))
	for _, child := range lp.Children() {
		childrenProperties = append(childrenProperties, preparePossibleProperties(child))
	}
	return lp.PreparePossibleProperties(lp.Schema(), childrenProperties...)
}

// PreparePossibleProperties implements LogicalPlan PreparePossibleProperties interface.
func (ds *DataSource) PreparePossibleProperties(schema *expression.Schema, childrenProperties ...[][]*expression.Column) [][]*expression.Column {
	result := make([][]*expression.Column, 0, len(ds.possibleAccessPaths))

	for _, path := range ds.possibleAccessPaths {
		if path.IsTablePath() {
			col := ds.getPKIsHandleCol()
			if col != nil {
				result = append(result, []*expression.Column{col})
			}
			continue
		}

		if len(path.IdxCols) == 0 {
			continue
		}
		result = append(result, make([]*expression.Column, len(path.IdxCols)))
		copy(result[len(result)-1], path.IdxCols)
		for i := 0; i < path.EqCondCount && i+1 < len(path.IdxCols); i++ {
			result = append(result, make([]*expression.Column, len(path.IdxCols)-i-1))
			copy(result[len(result)-1], path.IdxCols[i+1:])
		}
	}
	return result
}

// PreparePossibleProperties implements LogicalPlan PreparePossibleProperties interface.
func (ts *LogicalTableScan) PreparePossibleProperties(schema *expression.Schema, childrenProperties ...[][]*expression.Column) [][]*expression.Column {
	if ts.HandleCols != nil {
		cols := make([]*expression.Column, ts.HandleCols.NumCols())
		for i := 0; i < ts.HandleCols.NumCols(); i++ {
			cols[i] = ts.HandleCols.GetCol(i)
		}
		return [][]*expression.Column{cols}
	}
	return nil
}

// PreparePossibleProperties implements LogicalPlan PreparePossibleProperties interface.
func (is *LogicalIndexScan) PreparePossibleProperties(schema *expression.Schema, childrenProperties ...[][]*expression.Column) [][]*expression.Column {
	if len(is.IdxCols) == 0 {
		return nil
	}
	result := make([][]*expression.Column, 0, is.EqCondCount+1)
	for i := 0; i <= is.EqCondCount; i++ {
		result = append(result, make([]*expression.Column, len(is.IdxCols)-i))
		copy(result[i], is.IdxCols[i:])
	}
	return result
}

// PreparePossibleProperties implements LogicalPlan PreparePossibleProperties interface.
func (p *TiKVSingleGather) PreparePossibleProperties(schema *expression.Schema, childrenProperties ...[][]*expression.Column) [][]*expression.Column {
	return childrenProperties[0]
}

// PreparePossibleProperties implements LogicalPlan PreparePossibleProperties interface.
func (p *LogicalSelection) PreparePossibleProperties(schema *expression.Schema, childrenProperties ...[][]*expression.Column) [][]*expression.Column {
	return childrenProperties[0]
}

// PreparePossibleProperties implements LogicalPlan PreparePossibleProperties interface.
func (p *LogicalWindow) PreparePossibleProperties(schema *expression.Schema, childrenProperties ...[][]*expression.Column) [][]*expression.Column {
	result := make([]*expression.Column, 0, len(p.PartitionBy)+len(p.OrderBy))
	for i := range p.PartitionBy {
		result = append(result, p.PartitionBy[i].Col)
	}
	for i := range p.OrderBy {
		result = append(result, p.OrderBy[i].Col)
	}
	return [][]*expression.Column{result}
}

// PreparePossibleProperties implements LogicalPlan PreparePossibleProperties interface.
func (p *LogicalSort) PreparePossibleProperties(schema *expression.Schema, childrenProperties ...[][]*expression.Column) [][]*expression.Column {
	propCols := getPossiblePropertyFromByItems(p.ByItems)
	if len(propCols) == 0 {
		return nil
	}
	return [][]*expression.Column{propCols}
}

// PreparePossibleProperties implements LogicalPlan PreparePossibleProperties interface.
func (p *LogicalTopN) PreparePossibleProperties(schema *expression.Schema, childrenProperties ...[][]*expression.Column) [][]*expression.Column {
	propCols := getPossiblePropertyFromByItems(p.ByItems)
	if len(propCols) == 0 {
		return nil
	}
	return [][]*expression.Column{propCols}
}

func getPossiblePropertyFromByItems(items []*util.ByItems) []*expression.Column {
	cols := make([]*expression.Column, 0, len(items))
	for _, item := range items {
		if col, ok := item.Expr.(*expression.Column); ok {
			cols = append(cols, col)
		} else {
			break
		}
	}
	return cols
}

// PreparePossibleProperties implements LogicalPlan PreparePossibleProperties interface.
func (p *baseLogicalPlan) PreparePossibleProperties(schema *expression.Schema, childrenProperties ...[][]*expression.Column) [][]*expression.Column {
	return nil
}

// PreparePossibleProperties implements LogicalPlan PreparePossibleProperties interface.
func (p *LogicalProjection) PreparePossibleProperties(schema *expression.Schema, childrenProperties ...[][]*expression.Column) [][]*expression.Column {
	childProperties := childrenProperties[0]
	oldCols := make([]*expression.Column, 0, p.schema.Len())
	newCols := make([]*expression.Column, 0, p.schema.Len())
	for i, expr := range p.Exprs {
		if col, ok := expr.(*expression.Column); ok {
			newCols = append(newCols, p.schema.Columns[i])
			oldCols = append(oldCols, col)
		}
	}
	tmpSchema := expression.NewSchema(oldCols...)
	for i := len(childProperties) - 1; i >= 0; i-- {
		for j, col := range childProperties[i] {
			pos := tmpSchema.ColumnIndex(col)
			if pos >= 0 {
				childProperties[i][j] = newCols[pos]
			} else {
				childProperties[i] = childProperties[i][:j]
				break
			}
		}
		if len(childProperties[i]) == 0 {
			childProperties = append(childProperties[:i], childProperties[i+1:]...)
		}
	}
	return childProperties
}

// PreparePossibleProperties implements LogicalPlan PreparePossibleProperties interface.
func (p *LogicalJoin) PreparePossibleProperties(schema *expression.Schema, childrenProperties ...[][]*expression.Column) [][]*expression.Column {
	leftProperties := childrenProperties[0]
	rightProperties := childrenProperties[1]
	// TODO: We should consider properties propagation.
	p.leftProperties = leftProperties
	p.rightProperties = rightProperties
	if p.JoinType == LeftOuterJoin || p.JoinType == LeftOuterSemiJoin {
		rightProperties = nil
	} else if p.JoinType == RightOuterJoin {
		leftProperties = nil
	}
	resultProperties := make([][]*expression.Column, len(leftProperties)+len(rightProperties))
	for i, cols := range leftProperties {
		resultProperties[i] = make([]*expression.Column, len(cols))
		copy(resultProperties[i], cols)
	}
	leftLen := len(leftProperties)
	for i, cols := range rightProperties {
		resultProperties[leftLen+i] = make([]*expression.Column, len(cols))
		copy(resultProperties[leftLen+i], cols)
	}
	return resultProperties
}

// PreparePossibleProperties implements LogicalPlan PreparePossibleProperties interface.
func (la *LogicalAggregation) PreparePossibleProperties(schema *expression.Schema, childrenProperties ...[][]*expression.Column) [][]*expression.Column {
	childProps := childrenProperties[0]
	// If there's no group-by item, the stream aggregation could have no order property. So we can add an empty property
	// when its group-by item is empty.
	if len(la.GroupByItems) == 0 {
		la.possibleProperties = [][]*expression.Column{nil}
		return nil
	}
	resultProperties := make([][]*expression.Column, 0, len(childProps))
	for _, possibleChildProperty := range childProps {
		sortColOffsets := getMaxSortPrefix(possibleChildProperty, la.groupByCols)
		if len(sortColOffsets) == len(la.groupByCols) {
			resultProperties = append(resultProperties, possibleChildProperty[:len(la.groupByCols)])
		}
	}
	la.possibleProperties = resultProperties
	return la.possibleProperties
}
