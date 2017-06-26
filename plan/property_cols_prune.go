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

import "github.com/pingcap/tidb/expression"

func (p *DataSource) getAllPossibleProperties() (result [][]*expression.Column) {
	indices, includeTS := availableIndices(p.indexHints, p.tableInfo)
	if includeTS {
		col := p.getPKIsHandleCol()
		if col != nil {
			result = append(result, []*expression.Column{col})
		}
	}
	for _, idx := range indices {
		var cols []*expression.Column
		for _, idxCol := range idx.Columns {
			found := false
			for _, col := range p.schema.Columns {
				if col.ColName.L == idxCol.Name.L {
					cols = append(cols, col)
					found = true
					break
				}
			}
			if !found {
				cols = nil
				break
			}
		}
		if len(cols) > 0 {
			result = append(result, cols)
		}
	}
	return
}

func (p *Selection) getAllPossibleProperties() (result [][]*expression.Column) {
	return p.children[0].(LogicalPlan).getAllPossibleProperties()
}

func (p *baseLogicalPlan) getAllPossibleProperties() [][]*expression.Column {
	return nil
}

func (p *LogicalJoin) getAllPossibleOrderCols() [][]*expression.Column {
	leftProperties := p.children[0].(LogicalPlan).getAllPossibleProperties()
	rightProperties := p.children[1].(LogicalPlan).getAllPossibleProperties()
	// TODO: We should consider properties propagation.
	p.leftProperties = leftProperties
	p.rightProperties = rightProperties
	resultProperties := make([][]*expression.Column, len(leftProperties), len(leftProperties)+len(rightProperties))
	copy(resultProperties, leftProperties)
	resultProperties = append(resultProperties, rightProperties...)
	return resultProperties
}
