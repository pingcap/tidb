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
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/util/disjointset"
)

// ResolveIndices implements Plan interface.
func (p *PhysicalProjection) ResolveIndices() {
	p.physicalSchemaProducer.ResolveIndices()
	for _, expr := range p.Exprs {
		expr.ResolveIndices(p.children[0].Schema())
	}
	childProj, isProj := p.children[0].(*PhysicalProjection)
	if !isProj {
		return
	}
	refine4NeighbourProj(p, childProj)
}

// refine4NeighbourProj refines the index for p.Exprs whose type is *Column when
// there is two neighbouring Projections.
// This function is introduced because that different childProj.Expr may refer
// to the same index of childProj.Schema, so we need to keep this relation
// between the specified expressions in the parent Projection.
func refine4NeighbourProj(p, childProj *PhysicalProjection) {
	inputIdx2OutputIdxes := make(map[int][]int)
	for i, expr := range childProj.Exprs {
		col, isCol := expr.(*expression.Column)
		if !isCol {
			continue
		}
		inputIdx2OutputIdxes[col.Index] = append(inputIdx2OutputIdxes[col.Index], i)
	}
	childSchemaUnionSet := disjointset.NewIntSet(childProj.schema.Len())
	for _, outputIdxes := range inputIdx2OutputIdxes {
		if len(outputIdxes) <= 1 {
			continue
		}
		for i := 1; i < len(outputIdxes); i++ {
			childSchemaUnionSet.Union(outputIdxes[0], outputIdxes[i])
		}
	}
	for _, expr := range p.Exprs {
		col, isCol := expr.(*expression.Column)
		if !isCol {
			continue
		}
		col.Index = childSchemaUnionSet.FindRoot(col.Index)

	}
}

// ResolveIndices implements Plan interface.
func (p *PhysicalHashJoin) ResolveIndices() {
	p.physicalSchemaProducer.ResolveIndices()
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
	p.physicalSchemaProducer.ResolveIndices()
	lSchema := p.children[0].Schema()
	rSchema := p.children[1].Schema()
	for _, col := range p.LeftKeys {
		col.ResolveIndices(lSchema)
	}
	for _, col := range p.RightKeys {
		col.ResolveIndices(rSchema)
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
func (p *PhysicalIndexJoin) ResolveIndices() {
	p.physicalSchemaProducer.ResolveIndices()
	lSchema := p.children[0].Schema()
	rSchema := p.children[1].Schema()
	for i := range p.InnerJoinKeys {
		p.OuterJoinKeys[i].ResolveIndices(p.children[p.OuterIndex].Schema())
		p.InnerJoinKeys[i].ResolveIndices(p.children[1-p.OuterIndex].Schema())
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
	p.basePhysicalPlan.ResolveIndices()
	for _, expr := range p.Conditions {
		expr.ResolveIndices(p.children[0].Schema())
	}
}

// ResolveIndices implements Plan interface.
func (p *PhysicalTableReader) ResolveIndices() {
	p.tablePlan.ResolveIndices()
}

// ResolveIndices implements Plan interface.
func (p *PhysicalIndexReader) ResolveIndices() {
	p.physicalSchemaProducer.ResolveIndices()
	p.indexPlan.ResolveIndices()
	for _, col := range p.OutputColumns {
		if col.ID != model.ExtraHandleID {
			col.ResolveIndices(p.indexPlan.Schema())
		} else {
			// If this is extra handle, then it must be the tail.
			col.Index = len(p.OutputColumns) - 1
		}
	}
}

// ResolveIndices implements Plan interface.
func (p *PhysicalIndexLookUpReader) ResolveIndices() {
	p.tablePlan.ResolveIndices()
	p.indexPlan.ResolveIndices()
}

// ResolveIndices implements Plan interface.
func (p *PhysicalSelection) ResolveIndices() {
	p.basePhysicalPlan.ResolveIndices()
	for _, expr := range p.Conditions {
		expr.ResolveIndices(p.children[0].Schema())
	}
}

// ResolveIndices implements Plan interface.
func (p *basePhysicalAgg) ResolveIndices() {
	p.physicalSchemaProducer.ResolveIndices()
	for _, aggFun := range p.AggFuncs {
		for _, arg := range aggFun.Args {
			arg.ResolveIndices(p.children[0].Schema())
		}
	}
	for _, item := range p.GroupByItems {
		item.ResolveIndices(p.children[0].Schema())
	}
}

// ResolveIndices implements Plan interface.
func (p *PhysicalSort) ResolveIndices() {
	p.basePhysicalPlan.ResolveIndices()
	for _, item := range p.ByItems {
		item.Expr.ResolveIndices(p.children[0].Schema())
	}
}

// ResolveIndices implements Plan interface.
func (p *PhysicalTopN) ResolveIndices() {
	p.basePhysicalPlan.ResolveIndices()
	for _, item := range p.ByItems {
		item.Expr.ResolveIndices(p.children[0].Schema())
	}
}

// ResolveIndices implements Plan interface.
func (p *PhysicalApply) ResolveIndices() {
	p.PhysicalJoin.ResolveIndices()
	for _, col := range p.schema.Columns {
		col.ResolveIndices(p.PhysicalJoin.schema)
	}
	for _, col := range p.OuterSchema {
		col.Column.ResolveIndices(p.children[0].Schema())
	}
}

// ResolveIndices implements Plan interface.
func (p *Update) ResolveIndices() {
	schema := p.SelectPlan.Schema()
	for _, assign := range p.OrderedList {
		assign.Col.ResolveIndices(schema)
		assign.Expr.ResolveIndices(schema)
	}
}

// ResolveIndices implements Plan interface.
func (p *Insert) ResolveIndices() {
	for _, asgn := range p.OnDuplicate {
		asgn.Col.ResolveIndices(p.tableSchema)
		asgn.Expr.ResolveIndices(p.tableSchema)
	}
	for _, set := range p.Setlist {
		set.Col.ResolveIndices(p.tableSchema)
		set.Expr.ResolveIndices(p.tableSchema)
	}
	for _, expr := range p.GenCols.Exprs {
		expr.ResolveIndices(p.tableSchema)
	}
	for _, asgn := range p.GenCols.OnDuplicates {
		asgn.Col.ResolveIndices(p.tableSchema)
		asgn.Expr.ResolveIndices(p.tableSchema)
	}
}

// ResolveIndices implements Plan interface.
func (p *Show) ResolveIndices() {
	for _, expr := range p.Conditions {
		expr.ResolveIndices(p.schema)
	}
}

func (p *physicalSchemaProducer) ResolveIndices() {
	p.basePhysicalPlan.ResolveIndices()
	if p.schema != nil {
		for _, cols := range p.schema.TblID2Handle {
			for _, col := range cols {
				col.ResolveIndices(p.schema)
			}
		}
	}
}

// ResolveIndices implements Plan interface.
func (p *basePhysicalPlan) ResolveIndices() {
	for _, child := range p.children {
		child.ResolveIndices()
	}
}
