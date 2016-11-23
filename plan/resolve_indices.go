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
	"github.com/pingcap/tidb/util/types"
)

// ResolveIndicesAndCorCols implements LogicalPlan interface.
func (p *Projection) ResolveIndicesAndCorCols() {
	p.baseLogicalPlan.ResolveIndicesAndCorCols()
	for _, expr := range p.Exprs {
		expr.ResolveIndices(p.GetChildByIndex(0).GetSchema())
	}
}

// ResolveIndicesAndCorCols implements LogicalPlan interface.
func (p *Join) ResolveIndicesAndCorCols() {
	p.baseLogicalPlan.ResolveIndicesAndCorCols()
	lSchema := p.GetChildByIndex(0).GetSchema()
	rSchema := p.GetChildByIndex(1).GetSchema()
	for _, fun := range p.EqualConditions {
		fun.Args[0].ResolveIndices(lSchema)
		fun.Args[1].ResolveIndices(rSchema)
	}
	for _, expr := range p.LeftConditions {
		expr.ResolveIndices(lSchema)
	}
	for _, expr := range p.RightConditions {
		expr.ResolveIndices(rSchema)
	}
	for _, expr := range p.OtherConditions {
		expr.ResolveIndices(append(lSchema, rSchema...))
	}
}

// ResolveIndicesAndCorCols implements LogicalPlan interface.
func (p *Selection) ResolveIndicesAndCorCols() {
	p.baseLogicalPlan.ResolveIndicesAndCorCols()
	for _, expr := range p.Conditions {
		expr.ResolveIndices(p.GetChildByIndex(0).GetSchema())
	}
}

// ResolveIndicesAndCorCols implements LogicalPlan interface.
func (p *Aggregation) ResolveIndicesAndCorCols() {
	p.baseLogicalPlan.ResolveIndicesAndCorCols()
	for _, aggFun := range p.AggFuncs {
		for _, arg := range aggFun.GetArgs() {
			arg.ResolveIndices(p.GetChildByIndex(0).GetSchema())
		}
	}
	for _, item := range p.GroupByItems {
		item.ResolveIndices(p.GetChildByIndex(0).GetSchema())
	}
}

// ResolveIndicesAndCorCols implements LogicalPlan interface.
func (p *Sort) ResolveIndicesAndCorCols() {
	p.baseLogicalPlan.ResolveIndicesAndCorCols()
	for _, item := range p.ByItems {
		item.Expr.ResolveIndices(p.GetChildByIndex(0).GetSchema())
	}
}

// ResolveIndicesAndCorCols implements LogicalPlan interface.
func (p *Apply) ResolveIndicesAndCorCols() {
	p.baseLogicalPlan.ResolveIndicesAndCorCols()
	p.InnerPlan.ResolveIndicesAndCorCols()
	corCols := p.InnerPlan.extractCorrelatedCols()
	childSchema := p.children[0].GetSchema()
	resultCorCols := make([]*expression.CorrelatedColumn, len(childSchema))
	for _, corCol := range corCols {
		idx := childSchema.GetIndex(&corCol.Column)
		if idx != -1 {
			if resultCorCols[idx] == nil {
				resultCorCols[idx] = &expression.CorrelatedColumn{
					Column: *childSchema[idx],
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
	p.corCols = resultCorCols[:length]

	if p.Checker != nil {
		p.Checker.Condition.ResolveIndices(append(childSchema, p.InnerPlan.GetSchema()...))
	}
}

// ResolveIndicesAndCorCols implements LogicalPlan interface.
func (p *Update) ResolveIndicesAndCorCols() {
	p.baseLogicalPlan.ResolveIndicesAndCorCols()
	orderedList := make([]*expression.Assignment, len(p.OrderedList))
	schema := p.GetChildByIndex(0).GetSchema()
	for _, v := range p.OrderedList {
		if v == nil {
			continue
		}
		orderedList[schema.GetIndex(v.Col)] = v
	}
	for i := 0; i < len(orderedList); i++ {
		if orderedList[i] == nil {
			continue
		}
		orderedList[i].Col.ResolveIndices(schema)
		orderedList[i].Expr.ResolveIndices(schema)
	}
	p.OrderedList = orderedList
}
