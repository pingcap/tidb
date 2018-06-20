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
)

// ResolveIndices implements Plan interface.
func (p *PhysicalProjection) ResolveIndices() {
	p.physicalSchemaProducer.ResolveIndices()
	for i, expr := range p.Exprs {
		expr = expr.Clone()
		expr.ResolveIndices(p.children[0].Schema())
		p.Exprs[i] = expr
	}
}

// ResolveIndices implements Plan interface.
func (p *PhysicalHashJoin) ResolveIndices() {
	p.physicalSchemaProducer.ResolveIndices()
	lSchema := p.children[0].Schema()
	rSchema := p.children[1].Schema()
	for i, fun := range p.EqualConditions {
		fun = fun.Clone().(*expression.ScalarFunction)
		fun.GetArgs()[0].ResolveIndices(lSchema)
		fun.GetArgs()[1].ResolveIndices(rSchema)
		p.EqualConditions[i] = fun
	}
	for i, expr := range p.LeftConditions {
		expr = expr.Clone()
		expr.ResolveIndices(lSchema)
		p.LeftConditions[i] = expr
	}
	for i, expr := range p.RightConditions {
		expr = expr.Clone()
		expr.ResolveIndices(rSchema)
		p.RightConditions[i] = expr
	}
	for i, expr := range p.OtherConditions {
		expr = expr.Clone()
		expr.ResolveIndices(expression.MergeSchema(lSchema, rSchema))
		p.OtherConditions[i] = expr
	}
}

// ResolveIndices implements Plan interface.
func (p *PhysicalMergeJoin) ResolveIndices() {
	p.physicalSchemaProducer.ResolveIndices()
	lSchema := p.children[0].Schema()
	rSchema := p.children[1].Schema()
	for i, col := range p.LeftKeys {
		col = col.Clone().(*expression.Column)
		col.ResolveIndices(lSchema)
		p.LeftKeys[i] = col
	}
	for i, col := range p.RightKeys {
		col = col.Clone().(*expression.Column)
		col.ResolveIndices(rSchema)
		p.RightKeys[i] = col
	}
	for i, expr := range p.LeftConditions {
		expr = expr.Clone()
		expr.ResolveIndices(lSchema)
		p.LeftConditions[i] = expr
	}
	for i, expr := range p.RightConditions {
		expr = expr.Clone()
		expr.ResolveIndices(rSchema)
		p.RightConditions[i] = expr
	}
	for i, expr := range p.OtherConditions {
		expr = expr.Clone()
		expr.ResolveIndices(expression.MergeSchema(lSchema, rSchema))
		p.OtherConditions[i] = expr
	}
}

// ResolveIndices implements Plan interface.
func (p *PhysicalIndexJoin) ResolveIndices() {
	p.physicalSchemaProducer.ResolveIndices()
	lSchema := p.children[0].Schema()
	rSchema := p.children[1].Schema()
	for i := range p.InnerJoinKeys {
		p.OuterJoinKeys[i] = p.OuterJoinKeys[i].Clone().(*expression.Column)
		p.OuterJoinKeys[i].ResolveIndices(p.children[p.OuterIndex].Schema())
		p.InnerJoinKeys[i] = p.InnerJoinKeys[i].Clone().(*expression.Column)
		p.InnerJoinKeys[i].ResolveIndices(p.children[1-p.OuterIndex].Schema())
	}
	for i, expr := range p.LeftConditions {
		expr = expr.Clone()
		expr.ResolveIndices(lSchema)
		p.LeftConditions[i] = expr
	}
	for i, expr := range p.RightConditions {
		expr = expr.Clone()
		expr.ResolveIndices(rSchema)
		p.RightConditions[i] = expr
	}
	for i, expr := range p.OtherConditions {
		expr = expr.Clone()
		expr.ResolveIndices(expression.MergeSchema(lSchema, rSchema))
		p.OtherConditions[i] = expr
	}
}

// ResolveIndices implements Plan interface.
func (p *PhysicalUnionScan) ResolveIndices() {
	p.basePhysicalPlan.ResolveIndices()
	for i, expr := range p.Conditions {
		expr = expr.Clone()
		expr.ResolveIndices(p.children[0].Schema())
		p.Conditions[i] = expr
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
	for i, col := range p.OutputColumns {
		col = col.Clone().(*expression.Column)
		if col.ID != model.ExtraHandleID {
			col.ResolveIndices(p.indexPlan.Schema())
		} else {
			// If this is extra handle, then it must be the tail.
			col.Index = len(p.OutputColumns) - 1
		}
		p.OutputColumns[i] = col
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
	for i, expr := range p.Conditions {
		expr = expr.Clone()
		expr.ResolveIndices(p.children[0].Schema())
		p.Conditions[i] = expr
	}
}

// ResolveIndices implements Plan interface.
func (p *basePhysicalAgg) ResolveIndices() {
	p.physicalSchemaProducer.ResolveIndices()
	for _, aggFun := range p.AggFuncs {
		for i, arg := range aggFun.Args {
			arg = arg.Clone()
			arg.ResolveIndices(p.children[0].Schema())
			aggFun.Args[i] = arg
		}
	}
	for i, item := range p.GroupByItems {
		item = item.Clone()
		item.ResolveIndices(p.children[0].Schema())
		p.GroupByItems[i] = item
	}
}

// ResolveIndices implements Plan interface.
func (p *PhysicalSort) ResolveIndices() {
	p.basePhysicalPlan.ResolveIndices()
	for i, item := range p.ByItems {
		item = item.Clone()
		item.Expr.ResolveIndices(p.children[0].Schema())
		p.ByItems[i] = item
	}
}

// ResolveIndices implements Plan interface.
func (p *PhysicalTopN) ResolveIndices() {
	p.basePhysicalPlan.ResolveIndices()
	for i, item := range p.ByItems {
		item = item.Clone()
		item.Expr.ResolveIndices(p.children[0].Schema())
		p.ByItems[i] = item
	}
}

// ResolveIndices implements Plan interface.
func (p *PhysicalApply) ResolveIndices() {
	p.physicalSchemaProducer.ResolveIndices()
	p.PhysicalJoin.ResolveIndices()
	for i, col := range p.schema.Columns {
		col = col.Clone().(*expression.Column)
		col.ResolveIndices(p.PhysicalJoin.schema)
		p.schema.Columns[i] = col
	}
	for _, col := range p.OuterSchema {
		col.Column = *col.Column.Clone().(*expression.Column)
		col.Column.ResolveIndices(p.children[0].Schema())
	}
}

// ResolveIndices implements Plan interface.
func (p *Update) ResolveIndices() {
	p.baseSchemaProducer.ResolveIndices()
	schema := p.SelectPlan.Schema()
	for _, assign := range p.OrderedList {
		assign.Col = assign.Col.Clone().(*expression.Column)
		assign.Col.ResolveIndices(schema)
		assign.Expr = assign.Expr.Clone()
		assign.Expr.ResolveIndices(schema)
	}
}

// ResolveIndices implements Plan interface.
func (p *Insert) ResolveIndices() {
	p.baseSchemaProducer.ResolveIndices()
	for _, asgn := range p.OnDuplicate {
		asgn.Col = asgn.Col.Clone().(*expression.Column)
		asgn.Col.ResolveIndices(p.tableSchema)
		asgn.Expr = asgn.Expr.Clone()
		asgn.Expr.ResolveIndices(p.tableSchema)
	}
	for _, set := range p.SetList {
		set.Col = set.Col.Clone().(*expression.Column)
		set.Col.ResolveIndices(p.tableSchema)
		set.Expr = set.Expr.Clone()
		set.Expr.ResolveIndices(p.tableSchema)
	}
	for i, expr := range p.GenCols.Exprs {
		expr = expr.Clone()
		expr.ResolveIndices(p.tableSchema)
		p.GenCols.Exprs[i] = expr
	}
	for _, asgn := range p.GenCols.OnDuplicates {
		asgn.Col = asgn.Col.Clone().(*expression.Column)
		asgn.Col.ResolveIndices(p.tableSchema)
		asgn.Expr = asgn.Expr.Clone()
		asgn.Expr.ResolveIndices(p.tableSchema)
	}
}

// ResolveIndices implements Plan interface.
func (p *Show) ResolveIndices() {
	for i, expr := range p.Conditions {
		expr = expr.Clone()
		expr.ResolveIndices(p.schema)
		p.Conditions[i] = expr
	}
}

func (p *physicalSchemaProducer) ResolveIndices() {
	p.basePhysicalPlan.ResolveIndices()
	if p.schema != nil {
		for i, cols := range p.schema.TblID2Handle {
			for j, col := range cols {
				col = col.Clone().(*expression.Column)
				col.ResolveIndices(p.schema)
				p.schema.TblID2Handle[i][j] = col
			}
		}
	}
}

func (p *baseSchemaProducer) ResolveIndices() {
	if p.schema != nil {
		for i, cols := range p.schema.TblID2Handle {
			for j, col := range cols {
				col = col.Clone().(*expression.Column)
				col.ResolveIndices(p.schema)
				p.schema.TblID2Handle[i][j] = col
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
