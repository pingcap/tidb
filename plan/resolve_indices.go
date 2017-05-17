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

// ResolveIndices implements Plan interface.
func (p *Projection) ResolveIndices() {
	p.basePlan.ResolveIndices()
	for _, expr := range p.Exprs {
		expr.ResolveIndices(p.children[0].Schema())
	}
}

// ResolveIndices implements Plan interface.
func (p *LogicalJoin) ResolveIndices() {
	p.basePlan.ResolveIndices()
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

// ResolveIndices implements Plan interface.
func (p *PhysicalHashJoin) ResolveIndices() {
	p.basePlan.ResolveIndices()
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

// ResolveIndices implements Plan interface.
func (p *PhysicalHashSemiJoin) ResolveIndices() {
	p.basePlan.ResolveIndices()
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

// ResolveIndices implements Plan interface.
func (p *PhysicalMergeJoin) ResolveIndices() {
	p.basePlan.ResolveIndices()
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

// ResolveIndices implements Plan interface.
func (p *PhysicalUnionScan) ResolveIndices() {
	for _, expr := range p.Conditions {
		expr.ResolveIndices(p.children[0].Schema())
	}
}

// ResolveIndices implements Plan interface.
func (p *PhysicalIndexReader) ResolveIndices() {
	p.indexPlan.ResolveIndices()
	for _, col := range p.OutputColumns {
		col.ResolveIndices(p.indexPlan.Schema())
	}
}

// ResolveIndices implements Plan interface.
func (p *Selection) ResolveIndices() {
	p.basePlan.ResolveIndices()
	for _, expr := range p.Conditions {
		expr.ResolveIndices(p.children[0].Schema())
	}
}

// ResolveIndices implements Plan interface.
func (p *LogicalAggregation) ResolveIndices() {
	p.basePlan.ResolveIndices()
	for _, aggFun := range p.AggFuncs {
		for _, arg := range aggFun.GetArgs() {
			arg.ResolveIndices(p.children[0].Schema())
		}
	}
	for _, item := range p.GroupByItems {
		item.ResolveIndices(p.children[0].Schema())
	}
}

// ResolveIndices implements Plan interface.
func (p *PhysicalAggregation) ResolveIndices() {
	p.basePlan.ResolveIndices()
	for _, aggFun := range p.AggFuncs {
		for _, arg := range aggFun.GetArgs() {
			arg.ResolveIndices(p.children[0].Schema())
		}
	}
	for _, item := range p.GroupByItems {
		item.ResolveIndices(p.children[0].Schema())
	}
}

// ResolveIndices implements Plan interface.
func (p *Sort) ResolveIndices() {
	p.basePlan.ResolveIndices()
	for _, item := range p.ByItems {
		item.Expr.ResolveIndices(p.children[0].Schema())
	}
}

// ResolveIndices implements Plan interface.
func (p *TopN) ResolveIndices() {
	p.basePlan.ResolveIndices()
	for _, item := range p.ByItems {
		item.Expr.ResolveIndices(p.children[0].Schema())
	}
}

// ResolveIndices implements Plan interface.
func (p *LogicalApply) ResolveIndices() {
	p.LogicalJoin.ResolveIndices()
	for _, col := range p.corCols {
		col.Column.ResolveIndices(p.children[0].Schema())
	}
}

// ResolveIndices implements Plan interface.
func (p *PhysicalApply) ResolveIndices() {
	p.PhysicalJoin.ResolveIndices()
	for _, col := range p.OuterSchema {
		col.Column.ResolveIndices(p.children[0].Schema())
	}
}

// ResolveIndices implements Plan interface.
func (p *Update) ResolveIndices() {
	p.basePlan.ResolveIndices()
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

// ResolveIndices implements Plan interface.
func (p *Insert) ResolveIndices() {
	p.basePlan.ResolveIndices()
	for _, asgn := range p.OnDuplicate {
		asgn.Expr.ResolveIndices(p.tableSchema)
	}
}

// ResolveIndices implements Plan interface.
func (p *basePlan) ResolveIndices() {
	for _, child := range p.children {
		child.ResolveIndices()
	}
}
