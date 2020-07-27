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

func (ds *DataSource) preparePossibleProperties() [][]*expression.Column {
	result := make([][]*expression.Column, 0, len(ds.possibleAccessPaths))

	for _, path := range ds.possibleAccessPaths {
		if path.isTablePath {
			col := ds.getPKIsHandleCol()
			if col != nil {
				result = append(result, []*expression.Column{col})
			}
			continue
		}
		if len(path.idxCols) > 0 {
			result = append(result, path.idxCols)
		}
	}
	return result
}

func (p *LogicalSelection) preparePossibleProperties() (result [][]*expression.Column) {
	return p.children[0].preparePossibleProperties()
}

func (p *LogicalSort) preparePossibleProperties() [][]*expression.Column {
	p.children[0].preparePossibleProperties()
	propCols := getPossiblePropertyFromByItems(p.ByItems)
	if len(propCols) == 0 {
		return nil
	}
	return [][]*expression.Column{propCols}
}

func (p *LogicalTopN) preparePossibleProperties() [][]*expression.Column {
	p.children[0].preparePossibleProperties()
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

func (p *baseLogicalPlan) preparePossibleProperties() [][]*expression.Column {
	for _, ch := range p.children {
		ch.preparePossibleProperties()
	}
	return nil
}

func (p *LogicalProjection) preparePossibleProperties() [][]*expression.Column {
	childProperties := p.children[0].preparePossibleProperties()
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

func (p *LogicalJoin) preparePossibleProperties() [][]*expression.Column {
	leftProperties := p.children[0].preparePossibleProperties()
	rightProperties := p.children[1].preparePossibleProperties()
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

func (la *LogicalAggregation) preparePossibleProperties() [][]*expression.Column {
	childProps := la.children[0].preparePossibleProperties()
	// If there's no group-by item, the stream aggregation could have no order property. So we can add an empty property
	// when its group-by item is empty.
	if len(la.GroupByItems) == 0 {
		la.possibleProperties = [][]*expression.Column{nil}
	} else {
		la.possibleProperties = childProps
	}
	return nil
}
