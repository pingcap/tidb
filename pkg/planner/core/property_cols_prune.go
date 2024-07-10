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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util"
)

// preparePossibleProperties traverses the plan tree by a post-order method,
// recursively calls base.LogicalPlan PreparePossibleProperties interface.
func preparePossibleProperties(lp base.LogicalPlan) [][]*expression.Column {
	childrenProperties := make([][][]*expression.Column, 0, len(lp.Children()))
	for _, child := range lp.Children() {
		childrenProperties = append(childrenProperties, preparePossibleProperties(child))
	}
	return lp.PreparePossibleProperties(lp.Schema(), childrenProperties...)
}

// PreparePossibleProperties implements base.LogicalPlan PreparePossibleProperties interface.
func (ds *DataSource) PreparePossibleProperties(_ *expression.Schema, _ ...[][]*expression.Column) [][]*expression.Column {
	result := make([][]*expression.Column, 0, len(ds.PossibleAccessPaths))

	for _, path := range ds.PossibleAccessPaths {
		if path.IsIntHandlePath {
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

func getPossiblePropertyFromByItems(items []*util.ByItems) []*expression.Column {
	cols := make([]*expression.Column, 0, len(items))
	for _, item := range items {
		col, ok := item.Expr.(*expression.Column)
		if !ok {
			break
		}
		cols = append(cols, col)
	}
	return cols
}

// PreparePossibleProperties implements base.LogicalPlan PreparePossibleProperties interface.
func (p *LogicalJoin) PreparePossibleProperties(_ *expression.Schema, childrenProperties ...[][]*expression.Column) [][]*expression.Column {
	leftProperties := childrenProperties[0]
	rightProperties := childrenProperties[1]
	// TODO: We should consider properties propagation.
	p.LeftProperties = leftProperties
	p.RightProperties = rightProperties
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
