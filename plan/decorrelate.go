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
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/util/types"
)

func extractCorColumnsBySchema(schema expression.Schema, innerPlan LogicalPlan) []*expression.CorrelatedColumn {
	corCols := innerPlan.extractCorrelatedCols()
	resultCorCols := make([]*expression.CorrelatedColumn, len(schema))
	for _, corCol := range corCols {
		idx := schema.GetIndex(&corCol.Column)
		if idx != -1 {
			if resultCorCols[idx] == nil {
				resultCorCols[idx] = &expression.CorrelatedColumn{
					Column: *schema[idx],
					Data:   new(types.Datum),
				}
			}
			corCol.Data = resultCorCols[idx].Data
		}
	}
	// Shrink slice. e.g. [col1, nil, col2, nil] will be changed to [col1, col2]
	length := 0
	for _, col := range resultCorCols {
		if col != nil {
			resultCorCols[length] = col
			length++
		}
	}
	return resultCorCols
}

func decorrelate(ctx context.Context, p LogicalPlan) LogicalPlan {
	if apply, ok := p.(*Apply); ok {
		outerPlan := apply.children[0]
		innerPlan := apply.children[1].(LogicalPlan)
		apply.corCols = extractCorColumnsBySchema(outerPlan.GetSchema(), innerPlan)
		if len(apply.corCols) == 0 {
			join := &apply.Join
			innerPlan.SetParents(&join)
			outerPlan.SetParents(&join)
			return join
		}
		if sel, ok := innerPlan.(*Selection); ok {
			innerPlan = sel.children[0].(LogicalPlan)
			sel.SetChildren(apply)
			apply.SetParents(sel)
			apply.SetChildren(outerPlan, innerPlan)
			innerPlan.SetParents(apply)
			p = sel
		}
	}
}