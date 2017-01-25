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
)

// ResolveIndicesAndCorCols implements LogicalPlan interface.
func (p *Projection) ResolveIndicesAndCorCols() {
	p.baseLogicalPlan.ResolveIndicesAndCorCols()
	for _, expr := range p.Exprs {
		expr.ResolveIndices(p.children[0].Schema())
	}
}

// ResolveIndicesAndCorCols implements LogicalPlan interface.
func (p *Join) ResolveIndicesAndCorCols() {
	p.baseLogicalPlan.ResolveIndicesAndCorCols()
	lSchema := p.children[0].Schema()
	rSchema := p.children[1].Schema()
	for _, fun := range p.EqualConditions {
		fun.GetArgs()[0].ResolveIndices(lSchema)
		fun.GetArgs()[1].ResolveIndices(rSchema)
	}
	for _, expr := range p.LeftConditions {
		expr.ResolveIndices(lSchema)
	}
	for _, expr := range p.RightConditions {
		expr.ResolveIndices(rSchema)
	}
	for _, expr := range p.OtherConditions {
		expr.ResolveIndices(expression.MergeSchema(lSchema, rSchema))
	}
}

// ResolveIndicesAndCorCols implements LogicalPlan interface.
func (p *Selection) ResolveIndicesAndCorCols() {
	p.baseLogicalPlan.ResolveIndicesAndCorCols()
	for _, expr := range p.Conditions {
		expr.ResolveIndices(p.children[0].Schema())
	}
}

// ResolveIndicesAndCorCols implements LogicalPlan interface.
func (p *Aggregation) ResolveIndicesAndCorCols() {
	p.baseLogicalPlan.ResolveIndicesAndCorCols()
	for _, aggFun := range p.AggFuncs {
		for _, arg := range aggFun.GetArgs() {
			arg.ResolveIndices(p.children[0].Schema())
		}
	}
	for _, item := range p.GroupByItems {
		item.ResolveIndices(p.children[0].Schema())
	}
}

// ResolveIndicesAndCorCols implements LogicalPlan interface.
func (p *Sort) ResolveIndicesAndCorCols() {
	p.baseLogicalPlan.ResolveIndicesAndCorCols()
	for _, item := range p.ByItems {
		item.Expr.ResolveIndices(p.children[0].Schema())
	}
}

// ResolveIndicesAndCorCols implements LogicalPlan interface.
func (p *Apply) ResolveIndicesAndCorCols() {
	p.Join.ResolveIndicesAndCorCols()
	for _, col := range p.corCols {
		col.Column.ResolveIndices(p.children[0].Schema())
	}
}

// ResolveIndicesAndCorCols implements LogicalPlan interface.
func (p *Update) ResolveIndicesAndCorCols() {
	p.baseLogicalPlan.ResolveIndicesAndCorCols()
	orderedList := make([]*expression.Assignment, len(p.OrderedList))
	schema := p.children[0].Schema()
	for _, v := range p.OrderedList {
		if v == nil {
			continue
		}
		orderedList[schema.ColumnIndex(v.Col)] = v
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

// ResolveIndicesAndCorCols implements LogicalPlan interface.
func (p *Insert) ResolveIndicesAndCorCols() {
	p.baseLogicalPlan.ResolveIndicesAndCorCols()
	for _, asgn := range p.OnDuplicate {
		asgn.Expr.ResolveIndices(p.tableSchema)
	}
}
