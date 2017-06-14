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

func (p *DataSource) getAllPossibleOrderCols() (result [][]int) {
	indices, includeTS := availableIndices(p.indexHints, p.tableInfo)
	if includeTS {
		col := p.getPKIsHandleCol()
		if col != nil {
			pkOffset := p.schema.ColumnIndex(col)
			if pkOffset != -1 {
				result = append(result, []int{pkOffset})
			}
		}
	}
	for _, idx := range indices {
		var offsets []int
		for _, col := range idx.Columns {
			found := false
			for i := range p.schema.Columns {
				if p.schema.Columns[i].ColName.L == col.Name.L {
					offsets = append(offsets, i)
					found = true
					break
				}
			}
			if !found {
				break
			}
		}
		if len(offsets) > 0 {
			result = append(result, offsets)
		}
	}
	return
}

func (p *Selection) getAllPossibleOrderCols() (result [][]int) {
	return p.children[0].(LogicalPlan).getAllPossibleOrderCols()
}

func (p *baseLogicalPlan) getAllPossibleOrderCols() [][]int {
	return nil
}

func colOffsetsMatch(keyOffsets, idxOffsets []int) (order []int) {
	for _, orderOffset := range idxOffsets {
		found := false
		for i, keyOffset := range keyOffsets {
			if orderOffset == keyOffset {
				order = append(order, i)
				found = true
				break
			}
		}
		if !found {
			return nil
		}
	}
	return
}

func (p *LogicalJoin) getAllPossibleOrderCols() [][]int {
	leftIdxResult := p.children[0].(LogicalPlan).getAllPossibleOrderCols()
	rightIdxResult := p.children[1].(LogicalPlan).getAllPossibleOrderCols()
	var leftMatchedOrder, rightMatchedOrder [][]int
	leftKeyOffsets := p.children[0].Schema().ColumnsIndices(p.LeftJoinKeys)
	rightKeyOffsets := p.children[1].Schema().ColumnsIndices(p.RightJoinKeys)
	for _, leftIdxCols := range leftIdxResult {
		if order := colOffsetsMatch(leftKeyOffsets, leftIdxCols); order != nil {
			leftMatchedOrder = append(leftMatchedOrder, order)
		}
	}
	for _, rightIdxCols := range rightIdxResult {
		if order := colOffsetsMatch(rightKeyOffsets, rightIdxCols); order != nil {
			rightMatchedOrder = append(rightMatchedOrder, order)
		}
	}
	for _, leftOrder := range leftMatchedOrder {
		for _, rightOrder := range rightMatchedOrder {
			if len(leftOrder) == len(rightOrder) {
				matched := true
				for i := range leftOrder {
					if leftOrder[i] != rightOrder[i] {
						matched = false
						break
					}
				}
				if matched {
					p.allPossibleOrder = append(p.allPossibleOrder, leftOrder)
				}
			}
		}
	}
	for _, rightCols := range rightIdxResult {
		for i := range rightCols {
			rightCols[i] += p.children[0].Schema().Len()
		}
		leftIdxResult = append(leftIdxResult, rightCols)
	}
	return leftIdxResult
}
