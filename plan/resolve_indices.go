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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
)

// ResolveIndices implements Plan interface.
func (p *PhysicalProjection) ResolveIndices() (err error) {
	err = p.physicalSchemaProducer.ResolveIndices()
	if err != nil {
		return errors.Trace(err)
	}
	for i, expr := range p.Exprs {
		p.Exprs[i], err = expr.ResolveIndices(p.children[0].Schema())
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// ResolveIndices implements Plan interface.
func (p *PhysicalHashJoin) ResolveIndices() (err error) {
	err = p.physicalSchemaProducer.ResolveIndices()
	if err != nil {
		return errors.Trace(err)
	}
	lSchema := p.children[0].Schema()
	rSchema := p.children[1].Schema()
	for i, fun := range p.EqualConditions {
		lArg, err := fun.GetArgs()[0].ResolveIndices(lSchema)
		if err != nil {
			return errors.Trace(err)
		}
		rArg, err := fun.GetArgs()[1].ResolveIndices(rSchema)
		if err != nil {
			return errors.Trace(err)
		}
		p.EqualConditions[i] = expression.NewFunctionInternal(fun.GetCtx(), fun.FuncName.L, fun.GetType(), lArg, rArg).(*expression.ScalarFunction)
	}
	for i, expr := range p.LeftConditions {
		p.LeftConditions[i], err = expr.ResolveIndices(lSchema)
		if err != nil {
			return errors.Trace(err)
		}
	}
	for i, expr := range p.RightConditions {
		p.RightConditions[i], err = expr.ResolveIndices(rSchema)
		if err != nil {
			return errors.Trace(err)
		}
	}
	for i, expr := range p.OtherConditions {
		p.OtherConditions[i], err = expr.ResolveIndices(expression.MergeSchema(lSchema, rSchema))
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// ResolveIndices implements Plan interface.
func (p *PhysicalMergeJoin) ResolveIndices() (err error) {
	err = p.physicalSchemaProducer.ResolveIndices()
	if err != nil {
		return errors.Trace(err)
	}
	lSchema := p.children[0].Schema()
	rSchema := p.children[1].Schema()
	for i, col := range p.LeftKeys {
		newKey, err := col.ResolveIndices(lSchema)
		p.LeftKeys[i] = newKey.(*expression.Column)
		if err != nil {
			return errors.Trace(err)
		}
	}
	for i, col := range p.RightKeys {
		newKey, err := col.ResolveIndices(rSchema)
		p.RightKeys[i] = newKey.(*expression.Column)
		if err != nil {
			return errors.Trace(err)
		}
	}
	for i, expr := range p.LeftConditions {
		p.LeftConditions[i], err = expr.ResolveIndices(lSchema)
		if err != nil {
			return errors.Trace(err)
		}
	}
	for i, expr := range p.RightConditions {
		p.RightConditions[i], err = expr.ResolveIndices(rSchema)
		if err != nil {
			return errors.Trace(err)
		}
	}
	for i, expr := range p.OtherConditions {
		p.OtherConditions[i], err = expr.ResolveIndices(expression.MergeSchema(lSchema, rSchema))
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// ResolveIndices implements Plan interface.
func (p *PhysicalIndexJoin) ResolveIndices() (err error) {
	err = p.physicalSchemaProducer.ResolveIndices()
	if err != nil {
		return errors.Trace(err)
	}
	lSchema := p.children[0].Schema()
	rSchema := p.children[1].Schema()
	for i := range p.InnerJoinKeys {
		newKey, err := p.OuterJoinKeys[i].ResolveIndices(p.children[p.OuterIndex].Schema())
		if err != nil {
			return errors.Trace(err)
		}
		p.OuterJoinKeys[i] = newKey.(*expression.Column)
		newKey, err = p.InnerJoinKeys[i].ResolveIndices(p.children[1-p.OuterIndex].Schema())
		if err != nil {
			return errors.Trace(err)
		}
		p.InnerJoinKeys[i] = newKey.(*expression.Column)
	}
	for i, expr := range p.LeftConditions {
		p.LeftConditions[i], err = expr.ResolveIndices(lSchema)
		if err != nil {
			return errors.Trace(err)
		}
	}
	for i, expr := range p.RightConditions {
		p.RightConditions[i], err = expr.ResolveIndices(rSchema)
		if err != nil {
			return errors.Trace(err)
		}
	}
	for i, expr := range p.OtherConditions {
		p.OtherConditions[i], err = expr.ResolveIndices(expression.MergeSchema(lSchema, rSchema))
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// ResolveIndices implements Plan interface.
func (p *PhysicalUnionScan) ResolveIndices() (err error) {
	err = p.basePhysicalPlan.ResolveIndices()
	if err != nil {
		return errors.Trace(err)
	}
	for i, expr := range p.Conditions {
		p.Conditions[i], err = expr.ResolveIndices(p.children[0].Schema())
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// ResolveIndices implements Plan interface.
func (p *PhysicalTableReader) ResolveIndices() (err error) {
	return errors.Trace(p.tablePlan.ResolveIndices())
}

// ResolveIndices implements Plan interface.
func (p *PhysicalIndexReader) ResolveIndices() (err error) {
	err = p.physicalSchemaProducer.ResolveIndices()
	if err != nil {
		return errors.Trace(err)
	}
	err = p.indexPlan.ResolveIndices()
	if err != nil {
		return errors.Trace(err)
	}
	for i, col := range p.OutputColumns {
		if col.ID != model.ExtraHandleID {
			newCol, err := col.ResolveIndices(p.indexPlan.Schema())
			if err != nil {
				return errors.Trace(err)
			}
			p.OutputColumns[i] = newCol.(*expression.Column)
		} else {
			p.OutputColumns[i] = col.Clone().(*expression.Column)
			// If this is extra handle, then it must be the tail.
			p.OutputColumns[i].Index = len(p.OutputColumns) - 1
		}
	}
	return nil
}

// ResolveIndices implements Plan interface.
func (p *PhysicalIndexLookUpReader) ResolveIndices() (err error) {
	err = p.tablePlan.ResolveIndices()
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(p.indexPlan.ResolveIndices())
}

// ResolveIndices implements Plan interface.
func (p *PhysicalSelection) ResolveIndices() (err error) {
	err = p.basePhysicalPlan.ResolveIndices()
	if err != nil {
		return errors.Trace(err)
	}
	for i, expr := range p.Conditions {
		p.Conditions[i], err = expr.ResolveIndices(p.children[0].Schema())
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// ResolveIndices implements Plan interface.
func (p *basePhysicalAgg) ResolveIndices() (err error) {
	err = p.physicalSchemaProducer.ResolveIndices()
	if err != nil {
		return errors.Trace(err)
	}
	for _, aggFun := range p.AggFuncs {
		for i, arg := range aggFun.Args {
			aggFun.Args[i], err = arg.ResolveIndices(p.children[0].Schema())
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	for i, item := range p.GroupByItems {
		p.GroupByItems[i], err = item.ResolveIndices(p.children[0].Schema())
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// ResolveIndices implements Plan interface.
func (p *PhysicalSort) ResolveIndices() (err error) {
	err = p.basePhysicalPlan.ResolveIndices()
	if err != nil {
		return errors.Trace(err)
	}
	for _, item := range p.ByItems {
		item.Expr, err = item.Expr.ResolveIndices(p.children[0].Schema())
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// ResolveIndices implements Plan interface.
func (p *PhysicalTopN) ResolveIndices() (err error) {
	err = p.basePhysicalPlan.ResolveIndices()
	if err != nil {
		return errors.Trace(err)
	}
	for _, item := range p.ByItems {
		item.Expr, err = item.Expr.ResolveIndices(p.children[0].Schema())
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// ResolveIndices implements Plan interface.
func (p *PhysicalApply) ResolveIndices() (err error) {
	err = p.physicalSchemaProducer.ResolveIndices()
	if err != nil {
		return errors.Trace(err)
	}
	err = p.PhysicalJoin.ResolveIndices()
	if err != nil {
		return errors.Trace(err)
	}
	for i, col := range p.schema.Columns {
		newCol, err := col.ResolveIndices(p.PhysicalJoin.schema)
		if err != nil {
			return errors.Trace(err)
		}
		p.schema.Columns[i] = newCol.(*expression.Column)
	}
	for _, col := range p.OuterSchema {
		newCol, err := col.Column.ResolveIndices(p.children[0].Schema())
		if err != nil {
			return errors.Trace(err)
		}
		col.Column = *newCol.(*expression.Column)
	}
	return nil
}

// ResolveIndices implements Plan interface.
func (p *Update) ResolveIndices() (err error) {
	err = p.baseSchemaProducer.ResolveIndices()
	if err != nil {
		return errors.Trace(err)
	}
	schema := p.SelectPlan.Schema()
	for _, assign := range p.OrderedList {
		newCol, err := assign.Col.ResolveIndices(schema)
		assign.Col = newCol.(*expression.Column)
		assign.Expr, err = assign.Expr.ResolveIndices(schema)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// ResolveIndices implements Plan interface.
func (p *Insert) ResolveIndices() (err error) {
	err = p.baseSchemaProducer.ResolveIndices()
	if err != nil {
		return errors.Trace(err)
	}
	for _, asgn := range p.OnDuplicate {
		newCol, err := asgn.Col.ResolveIndices(p.tableSchema)
		if err != nil {
			return errors.Trace(err)
		}
		asgn.Col = newCol.(*expression.Column)
		asgn.Expr, err = asgn.Expr.ResolveIndices(p.tableSchema)
		if err != nil {
			return errors.Trace(err)
		}
	}
	for _, set := range p.SetList {
		newCol, err := set.Col.ResolveIndices(p.tableSchema)
		if err != nil {
			return errors.Trace(err)
		}
		set.Col = newCol.(*expression.Column)
		set.Expr, err = set.Expr.ResolveIndices(p.tableSchema)
		if err != nil {
			return errors.Trace(err)
		}
	}
	for i, expr := range p.GenCols.Exprs {
		p.GenCols.Exprs[i], err = expr.ResolveIndices(p.tableSchema)
		if err != nil {
			return errors.Trace(err)
		}
	}
	for _, asgn := range p.GenCols.OnDuplicates {
		newCol, err := asgn.Col.ResolveIndices(p.tableSchema)
		if err != nil {
			return errors.Trace(err)
		}
		asgn.Col = newCol.(*expression.Column)
		asgn.Expr, err = asgn.Expr.ResolveIndices(p.tableSchema)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return errors.Trace(err)
}

// ResolveIndices implements Plan interface.
func (p *Show) ResolveIndices() (err error) {
	for i, expr := range p.Conditions {
		p.Conditions[i], err = expr.ResolveIndices(p.schema)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (p *physicalSchemaProducer) ResolveIndices() (err error) {
	err = p.basePhysicalPlan.ResolveIndices()
	if err != nil {
		return errors.Trace(err)
	}
	if p.schema != nil {
		for i, cols := range p.schema.TblID2Handle {
			for j, col := range cols {
				newCol, err := col.ResolveIndices(p.schema)
				if err != nil {
					return errors.Trace(err)
				}
				p.schema.TblID2Handle[i][j] = newCol.(*expression.Column)
			}
		}
	}
	return nil
}

func (p *baseSchemaProducer) ResolveIndices() (err error) {
	if p.schema != nil {
		for i, cols := range p.schema.TblID2Handle {
			for j, col := range cols {
				newCol, err := col.ResolveIndices(p.schema)
				if err != nil {
					return errors.Trace(err)
				}
				p.schema.TblID2Handle[i][j] = newCol.(*expression.Column)
			}
		}
	}
	return nil
}

// ResolveIndices implements Plan interface.
func (p *basePhysicalPlan) ResolveIndices() (err error) {
	for _, child := range p.children {
		err = child.ResolveIndices()
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}
