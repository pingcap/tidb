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
		p.Exprs[i] = expr.ResolveIndices(p.children[0].Schema())
	}
}

// ResolveIndices implements Plan interface.
func (p *PhysicalHashJoin) ResolveIndices() {
	p.physicalSchemaProducer.ResolveIndices()
	lSchema := p.children[0].Schema()
	rSchema := p.children[1].Schema()
	for i, fun := range p.EqualConditions {
		lArg := fun.GetArgs()[0].ResolveIndices(lSchema)
		rArg := fun.GetArgs()[1].ResolveIndices(rSchema)
		p.EqualConditions[i] = expression.NewFunctionInternal(fun.GetCtx(), fun.FuncName.L, fun.GetType(), lArg, rArg).(*expression.ScalarFunction)
	}
	for i, expr := range p.LeftConditions {
		p.LeftConditions[i] = expr.ResolveIndices(lSchema)
	}
	for i, expr := range p.RightConditions {
		p.RightConditions[i] = expr.ResolveIndices(rSchema)
	}
	for i, expr := range p.OtherConditions {
		p.OtherConditions[i] = expr.ResolveIndices(expression.MergeSchema(lSchema, rSchema))
	}
}

// ResolveIndices implements Plan interface.
func (p *PhysicalMergeJoin) ResolveIndices() {
	p.physicalSchemaProducer.ResolveIndices()
	lSchema := p.children[0].Schema()
	rSchema := p.children[1].Schema()
	for i, col := range p.LeftKeys {
		p.LeftKeys[i] = col.ResolveIndices(lSchema).(*expression.Column)
	}
	for i, col := range p.RightKeys {
		p.RightKeys[i] = col.ResolveIndices(rSchema).(*expression.Column)
	}
	for i, expr := range p.LeftConditions {
		p.LeftConditions[i] = expr.ResolveIndices(lSchema)
	}
	for i, expr := range p.RightConditions {
		p.RightConditions[i] = expr.ResolveIndices(rSchema)
	}
	for i, expr := range p.OtherConditions {
		p.OtherConditions[i] = expr.ResolveIndices(expression.MergeSchema(lSchema, rSchema))
	}
}

// ResolveIndices implements Plan interface.
func (p *PhysicalIndexJoin) ResolveIndices() {
	p.physicalSchemaProducer.ResolveIndices()
	lSchema := p.children[0].Schema()
	rSchema := p.children[1].Schema()
	for i := range p.InnerJoinKeys {
		p.OuterJoinKeys[i] = p.OuterJoinKeys[i].ResolveIndices(p.children[p.OuterIndex].Schema()).(*expression.Column)
		p.InnerJoinKeys[i] = p.InnerJoinKeys[i].ResolveIndices(p.children[1-p.OuterIndex].Schema()).(*expression.Column)
	}
	for i, expr := range p.LeftConditions {
		p.LeftConditions[i] = expr.ResolveIndices(lSchema)
	}
	for i, expr := range p.RightConditions {
		p.RightConditions[i] = expr.ResolveIndices(rSchema)
	}
	for i, expr := range p.OtherConditions {
		p.OtherConditions[i] = expr.ResolveIndices(expression.MergeSchema(lSchema, rSchema))
	}
}

// ResolveIndices implements Plan interface.
func (p *PhysicalUnionScan) ResolveIndices() {
	p.basePhysicalPlan.ResolveIndices()
	for i, expr := range p.Conditions {
		p.Conditions[i] = expr.ResolveIndices(p.children[0].Schema())
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
		if col.ID != model.ExtraHandleID {
			p.OutputColumns[i] = col.ResolveIndices(p.indexPlan.Schema()).(*expression.Column)
		} else {
			p.OutputColumns[i] = col.Clone().(*expression.Column)
			// If this is extra handle, then it must be the tail.
			p.OutputColumns[i].Index = len(p.OutputColumns) - 1
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
	for i, expr := range p.Conditions {
		p.Conditions[i] = expr.ResolveIndices(p.children[0].Schema())
	}
}

// ResolveIndices implements Plan interface.
func (p *basePhysicalAgg) ResolveIndices() {
	p.physicalSchemaProducer.ResolveIndices()
	for _, aggFun := range p.AggFuncs {
		for i, arg := range aggFun.Args {
			aggFun.Args[i] = arg.ResolveIndices(p.children[0].Schema())
		}
	}
	for i, item := range p.GroupByItems {
		p.GroupByItems[i] = item.ResolveIndices(p.children[0].Schema())
	}
}

// ResolveIndices implements Plan interface.
func (p *PhysicalSort) ResolveIndices() {
	p.basePhysicalPlan.ResolveIndices()
	for _, item := range p.ByItems {
		item.Expr = item.Expr.ResolveIndices(p.children[0].Schema())
	}
}

// ResolveIndices implements Plan interface.
func (p *PhysicalTopN) ResolveIndices() {
	p.basePhysicalPlan.ResolveIndices()
	for _, item := range p.ByItems {
		item.Expr = item.Expr.ResolveIndices(p.children[0].Schema())
	}
}

// ResolveIndices implements Plan interface.
func (p *PhysicalApply) ResolveIndices() {
	p.physicalSchemaProducer.ResolveIndices()
	p.PhysicalJoin.ResolveIndices()
	for i, col := range p.schema.Columns {
		p.schema.Columns[i] = col.ResolveIndices(p.PhysicalJoin.schema).(*expression.Column)
	}
	for _, col := range p.OuterSchema {
		col.Column = *col.Column.ResolveIndices(p.children[0].Schema()).(*expression.Column)
	}
}

// ResolveIndices implements Plan interface.
func (p *Update) ResolveIndices() {
	p.baseSchemaProducer.ResolveIndices()
	schema := p.SelectPlan.Schema()
	for _, assign := range p.OrderedList {
		assign.Col = assign.Col.ResolveIndices(schema).(*expression.Column)
		assign.Expr = assign.Expr.ResolveIndices(schema)
	}
}

// ResolveIndices implements Plan interface.
func (p *Insert) ResolveIndices() {
	p.baseSchemaProducer.ResolveIndices()
	for _, asgn := range p.OnDuplicate {
		asgn.Col = asgn.Col.ResolveIndices(p.tableSchema).(*expression.Column)
		asgn.Expr = asgn.Expr.ResolveIndices(p.Schema4OnDuplicate)
	}
	for _, set := range p.SetList {
		set.Col = set.Col.ResolveIndices(p.tableSchema).(*expression.Column)
		set.Expr = set.Expr.ResolveIndices(p.tableSchema)
	}
	for i, expr := range p.GenCols.Exprs {
		p.GenCols.Exprs[i] = expr.ResolveIndices(p.tableSchema)
	}
	for _, asgn := range p.GenCols.OnDuplicates {
		asgn.Col = asgn.Col.ResolveIndices(p.tableSchema).(*expression.Column)
		asgn.Expr = asgn.Expr.ResolveIndices(p.Schema4OnDuplicate)
	}
}

// ResolveIndices implements Plan interface.
func (p *Show) ResolveIndices() {
	for i, expr := range p.Conditions {
		p.Conditions[i] = expr.ResolveIndices(p.schema)
	}
}

func (p *physicalSchemaProducer) ResolveIndices() {
	p.basePhysicalPlan.ResolveIndices()
	if p.schema != nil {
		for i, cols := range p.schema.TblID2Handle {
			for j, col := range cols {
				p.schema.TblID2Handle[i][j] = col.ResolveIndices(p.schema).(*expression.Column)
			}
		}
	}
}

func (p *baseSchemaProducer) ResolveIndices() {
	if p.schema != nil {
		for i, cols := range p.schema.TblID2Handle {
			for j, col := range cols {
				p.schema.TblID2Handle[i][j] = col.ResolveIndices(p.schema).(*expression.Column)
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
