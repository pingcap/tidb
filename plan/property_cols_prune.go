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

package plan

import (
	"github.com/pingcap/tidb/expression"
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
		cols, _ := expression.IndexInfo2Cols(ds.schema.Columns, path.index)
		if len(cols) > 0 {
			result = append(result, cols)
		}
	}
	return result
}

func (p *LogicalSelection) preparePossibleProperties() (result [][]*expression.Column) {
	return p.children[0].preparePossibleProperties()
}

func (p *baseLogicalPlan) preparePossibleProperties() [][]*expression.Column {
	for _, ch := range p.children {
		ch.preparePossibleProperties()
	}
	return nil
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
	resultProperties := make([][]*expression.Column, len(leftProperties), len(leftProperties)+len(rightProperties))
	copy(resultProperties, leftProperties)
	resultProperties = append(resultProperties, rightProperties...)
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
