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

func (p *DataSource) preparePossibleProperties() (result [][]*expression.Column) {
	indices := p.availableIndices.indices
	includeTS := p.availableIndices.includeTableScan
	if includeTS {
		col := p.getPKIsHandleCol()
		if col != nil {
			result = append(result, []*expression.Column{col})
		}
	}
	for _, idx := range indices {
		cols, _ := expression.IndexInfo2Cols(p.schema.Columns, idx)
		if len(cols) > 0 {
			result = append(result, cols)
		}
	}
	return
}

func (p *LogicalSelection) preparePossibleProperties() (result [][]*expression.Column) {
	return p.children[0].(LogicalPlan).preparePossibleProperties()
}

func (p *baseLogicalPlan) preparePossibleProperties() [][]*expression.Column {
	if len(p.basePlan.children) > 0 {
		p.basePlan.children[0].(LogicalPlan).preparePossibleProperties()
	}
	return nil
}

func (p *LogicalJoin) preparePossibleProperties() [][]*expression.Column {
	leftProperties := p.children[0].(LogicalPlan).preparePossibleProperties()
	rightProperties := p.children[1].(LogicalPlan).preparePossibleProperties()
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

func (p *LogicalAggregation) preparePossibleProperties() [][]*expression.Column {
	p.possibleProperties = p.children[0].(LogicalPlan).preparePossibleProperties()
	return nil
}
