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

package core

import (
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/util/disjointset"
)

// ResolveIndices implements Plan interface.
func (p *PhysicalProjection) ResolveIndices() (err error) {
	err = p.physicalSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	for i, expr := range p.Exprs {
		p.Exprs[i], err = expr.ResolveIndices(p.children[0].Schema())
		if err != nil {
			return err
		}
	}
	childProj, isProj := p.children[0].(*PhysicalProjection)
	if !isProj {
		return
	}
	refine4NeighbourProj(p, childProj)
	return
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
func (p *PhysicalHashJoin) ResolveIndices() (err error) {
	err = p.physicalSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	lSchema := p.children[0].Schema()
	rSchema := p.children[1].Schema()
	for i, fun := range p.EqualConditions {
		lArg, err := fun.GetArgs()[0].ResolveIndices(lSchema)
		if err != nil {
			return err
		}
		p.LeftJoinKeys[i] = lArg.(*expression.Column)
		rArg, err := fun.GetArgs()[1].ResolveIndices(rSchema)
		if err != nil {
			return err
		}
		p.RightJoinKeys[i] = rArg.(*expression.Column)
		p.EqualConditions[i] = expression.NewFunctionInternal(fun.GetCtx(), fun.FuncName.L, fun.GetType(), lArg, rArg).(*expression.ScalarFunction)
	}
	for i, expr := range p.LeftConditions {
		p.LeftConditions[i], err = expr.ResolveIndices(lSchema)
		if err != nil {
			return err
		}
	}
	for i, expr := range p.RightConditions {
		p.RightConditions[i], err = expr.ResolveIndices(rSchema)
		if err != nil {
			return err
		}
	}
	for i, expr := range p.OtherConditions {
		p.OtherConditions[i], err = expr.ResolveIndices(expression.MergeSchema(lSchema, rSchema))
		if err != nil {
			return err
		}
	}
	return
}

// ResolveIndices implements Plan interface.
func (p *PhysicalBroadCastJoin) ResolveIndices() (err error) {
	err = p.physicalSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	lSchema := p.children[0].Schema()
	rSchema := p.children[1].Schema()
	for i, col := range p.LeftJoinKeys {
		newKey, err := col.ResolveIndices(lSchema)
		if err != nil {
			return err
		}
		p.LeftJoinKeys[i] = newKey.(*expression.Column)
	}
	for i, col := range p.RightJoinKeys {
		newKey, err := col.ResolveIndices(rSchema)
		if err != nil {
			return err
		}
		p.RightJoinKeys[i] = newKey.(*expression.Column)
	}
	for i, expr := range p.LeftConditions {
		p.LeftConditions[i], err = expr.ResolveIndices(lSchema)
		if err != nil {
			return err
		}
	}
	for i, expr := range p.RightConditions {
		p.RightConditions[i], err = expr.ResolveIndices(rSchema)
		if err != nil {
			return err
		}
	}
	for i, expr := range p.OtherConditions {
		p.OtherConditions[i], err = expr.ResolveIndices(expression.MergeSchema(lSchema, rSchema))
		if err != nil {
			return err
		}
	}
	return
}

// ResolveIndices implements Plan interface.
func (p *PhysicalMergeJoin) ResolveIndices() (err error) {
	err = p.physicalSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	lSchema := p.children[0].Schema()
	rSchema := p.children[1].Schema()
	for i, col := range p.LeftJoinKeys {
		newKey, err := col.ResolveIndices(lSchema)
		if err != nil {
			return err
		}
		p.LeftJoinKeys[i] = newKey.(*expression.Column)
	}
	for i, col := range p.RightJoinKeys {
		newKey, err := col.ResolveIndices(rSchema)
		if err != nil {
			return err
		}
		p.RightJoinKeys[i] = newKey.(*expression.Column)
	}
	for i, expr := range p.LeftConditions {
		p.LeftConditions[i], err = expr.ResolveIndices(lSchema)
		if err != nil {
			return err
		}
	}
	for i, expr := range p.RightConditions {
		p.RightConditions[i], err = expr.ResolveIndices(rSchema)
		if err != nil {
			return err
		}
	}
	for i, expr := range p.OtherConditions {
		p.OtherConditions[i], err = expr.ResolveIndices(expression.MergeSchema(lSchema, rSchema))
		if err != nil {
			return err
		}
	}
	return
}

// ResolveIndices implements Plan interface.
func (p *PhysicalIndexJoin) ResolveIndices() (err error) {
	err = p.physicalSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	lSchema := p.children[0].Schema()
	rSchema := p.children[1].Schema()
	for i := range p.InnerJoinKeys {
		newOuterKey, err := p.OuterJoinKeys[i].ResolveIndices(p.children[1-p.InnerChildIdx].Schema())
		if err != nil {
			return err
		}
		p.OuterJoinKeys[i] = newOuterKey.(*expression.Column)
		newInnerKey, err := p.InnerJoinKeys[i].ResolveIndices(p.children[p.InnerChildIdx].Schema())
		if err != nil {
			return err
		}
		p.InnerJoinKeys[i] = newInnerKey.(*expression.Column)
	}
	for i, expr := range p.LeftConditions {
		p.LeftConditions[i], err = expr.ResolveIndices(lSchema)
		if err != nil {
			return err
		}
	}
	for i, expr := range p.RightConditions {
		p.RightConditions[i], err = expr.ResolveIndices(rSchema)
		if err != nil {
			return err
		}
	}
	mergedSchema := expression.MergeSchema(lSchema, rSchema)
	for i, expr := range p.OtherConditions {
		p.OtherConditions[i], err = expr.ResolveIndices(mergedSchema)
		if err != nil {
			return err
		}
	}
	if p.CompareFilters != nil {
		err = p.CompareFilters.resolveIndices(p.children[1-p.InnerChildIdx].Schema())
		if err != nil {
			return err
		}
		for i := range p.CompareFilters.affectedColSchema.Columns {
			resolvedCol, err1 := p.CompareFilters.affectedColSchema.Columns[i].ResolveIndices(p.children[1-p.InnerChildIdx].Schema())
			if err1 != nil {
				return err1
			}
			p.CompareFilters.affectedColSchema.Columns[i] = resolvedCol.(*expression.Column)
		}
	}
	return
}

// ResolveIndices implements Plan interface.
func (p *PhysicalUnionScan) ResolveIndices() (err error) {
	err = p.basePhysicalPlan.ResolveIndices()
	if err != nil {
		return err
	}
	for i, expr := range p.Conditions {
		p.Conditions[i], err = expr.ResolveIndices(p.children[0].Schema())
		if err != nil {
			return err
		}
	}
	resolvedHandleCol, err := p.HandleCol.ResolveIndices(p.children[0].Schema())
	if err != nil {
		return err
	}
	p.HandleCol = resolvedHandleCol.(*expression.Column)
	return
}

// resolveIndicesForVirtualColumn resolves dependent columns's indices for virtual columns.
func resolveIndicesForVirtualColumn(result []*expression.Column, schema *expression.Schema) error {
	for _, col := range result {
		if col.VirtualExpr != nil {
			newExpr, err := col.VirtualExpr.ResolveIndices(schema)
			if err != nil {
				return err
			}
			col.VirtualExpr = newExpr
		}
	}
	return nil
}

// ResolveIndices implements Plan interface.
func (p *PhysicalTableReader) ResolveIndices() error {
	err := resolveIndicesForVirtualColumn(p.schema.Columns, p.schema)
	if err != nil {
		return err
	}
	return p.tablePlan.ResolveIndices()
}

// ResolveIndices implements Plan interface.
func (p *PhysicalIndexReader) ResolveIndices() (err error) {
	err = p.physicalSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	err = p.indexPlan.ResolveIndices()
	if err != nil {
		return err
	}
	for i, col := range p.OutputColumns {
		newCol, err := col.ResolveIndices(p.indexPlan.Schema())
		if err != nil {
			return err
		}
		p.OutputColumns[i] = newCol.(*expression.Column)
	}
	return
}

// ResolveIndices implements Plan interface.
func (p *PhysicalIndexLookUpReader) ResolveIndices() (err error) {
	err = resolveIndicesForVirtualColumn(p.tablePlan.Schema().Columns, p.schema)
	if err != nil {
		return err
	}
	err = p.tablePlan.ResolveIndices()
	if err != nil {
		return err
	}
	err = p.indexPlan.ResolveIndices()
	if err != nil {
		return err
	}
	if p.ExtraHandleCol != nil {
		newCol, err := p.ExtraHandleCol.ResolveIndices(p.tablePlan.Schema())
		if err != nil {
			return err
		}
		p.ExtraHandleCol = newCol.(*expression.Column)
	}
	return
}

// ResolveIndices implements Plan interface.
func (p *PhysicalIndexMergeReader) ResolveIndices() (err error) {
	err = resolveIndicesForVirtualColumn(p.tablePlan.Schema().Columns, p.schema)
	if err != nil {
		return err
	}
	if p.tablePlan != nil {
		err = p.tablePlan.ResolveIndices()
		if err != nil {
			return err
		}
	}
	for i := 0; i < len(p.partialPlans); i++ {
		err = p.partialPlans[i].ResolveIndices()
		if err != nil {
			return err
		}
	}
	return nil
}

// ResolveIndices implements Plan interface.
func (p *PhysicalSelection) ResolveIndices() (err error) {
	err = p.basePhysicalPlan.ResolveIndices()
	if err != nil {
		return err
	}
	for i, expr := range p.Conditions {
		p.Conditions[i], err = expr.ResolveIndices(p.children[0].Schema())
		if err != nil {
			return err
		}
	}
	return
}

// ResolveIndices implements Plan interface.
func (p *basePhysicalAgg) ResolveIndices() (err error) {
	err = p.physicalSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	for _, aggFun := range p.AggFuncs {
		for i, arg := range aggFun.Args {
			aggFun.Args[i], err = arg.ResolveIndices(p.children[0].Schema())
			if err != nil {
				return err
			}
		}
		for _, byItem := range aggFun.OrderByItems {
			byItem.Expr, err = byItem.Expr.ResolveIndices(p.children[0].Schema())
			if err != nil {
				return err
			}
		}
	}
	for i, item := range p.GroupByItems {
		p.GroupByItems[i], err = item.ResolveIndices(p.children[0].Schema())
		if err != nil {
			return err
		}
	}
	return
}

// ResolveIndices implements Plan interface.
func (p *PhysicalSort) ResolveIndices() (err error) {
	err = p.basePhysicalPlan.ResolveIndices()
	if err != nil {
		return err
	}
	for _, item := range p.ByItems {
		item.Expr, err = item.Expr.ResolveIndices(p.children[0].Schema())
		if err != nil {
			return err
		}
	}
	return err
}

// ResolveIndices implements Plan interface.
func (p *PhysicalWindow) ResolveIndices() (err error) {
	err = p.physicalSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	for i := 0; i < len(p.Schema().Columns)-len(p.WindowFuncDescs); i++ {
		col := p.Schema().Columns[i]
		newCol, err := col.ResolveIndices(p.children[0].Schema())
		if err != nil {
			return err
		}
		p.Schema().Columns[i] = newCol.(*expression.Column)
	}
	for i, item := range p.PartitionBy {
		newCol, err := item.Col.ResolveIndices(p.children[0].Schema())
		if err != nil {
			return err
		}
		p.PartitionBy[i].Col = newCol.(*expression.Column)
	}
	for i, item := range p.OrderBy {
		newCol, err := item.Col.ResolveIndices(p.children[0].Schema())
		if err != nil {
			return err
		}
		p.OrderBy[i].Col = newCol.(*expression.Column)
	}
	for _, desc := range p.WindowFuncDescs {
		for i, arg := range desc.Args {
			desc.Args[i], err = arg.ResolveIndices(p.children[0].Schema())
			if err != nil {
				return err
			}
		}
	}
	if p.Frame != nil {
		for i := range p.Frame.Start.CalcFuncs {
			p.Frame.Start.CalcFuncs[i], err = p.Frame.Start.CalcFuncs[i].ResolveIndices(p.children[0].Schema())
			if err != nil {
				return err
			}
		}
		for i := range p.Frame.End.CalcFuncs {
			p.Frame.End.CalcFuncs[i], err = p.Frame.End.CalcFuncs[i].ResolveIndices(p.children[0].Schema())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// ResolveIndices implements Plan interface.
func (p *PhysicalShuffle) ResolveIndices() (err error) {
	err = p.basePhysicalPlan.ResolveIndices()
	if err != nil {
		return err
	}
	for i := range p.HashByItems {
		// "Shuffle" get value of items from `DataSource`, other than children[0].
		p.HashByItems[i], err = p.HashByItems[i].ResolveIndices(p.DataSource.Schema())
		if err != nil {
			return err
		}
	}
	return err
}

// ResolveIndices implements Plan interface.
func (p *PhysicalTopN) ResolveIndices() (err error) {
	err = p.basePhysicalPlan.ResolveIndices()
	if err != nil {
		return err
	}
	for _, item := range p.ByItems {
		item.Expr, err = item.Expr.ResolveIndices(p.children[0].Schema())
		if err != nil {
			return err
		}
	}
	return
}

// ResolveIndices implements Plan interface.
func (p *PhysicalApply) ResolveIndices() (err error) {
	err = p.PhysicalHashJoin.ResolveIndices()
	if err != nil {
		return err
	}
	// p.OuterSchema may have duplicated CorrelatedColumns,
	// we deduplicate it here.
	dedupCols := make(map[int64]*expression.CorrelatedColumn, len(p.OuterSchema))
	for _, col := range p.OuterSchema {
		dedupCols[col.UniqueID] = col
	}
	p.OuterSchema = make([]*expression.CorrelatedColumn, 0, len(dedupCols))
	for _, col := range dedupCols {
		newCol, err := col.Column.ResolveIndices(p.children[0].Schema())
		if err != nil {
			return err
		}
		col.Column = *newCol.(*expression.Column)
		p.OuterSchema = append(p.OuterSchema, col)
	}
	// Resolve index for equal conditions again, because apply is different from
	// hash join on the fact that equal conditions are evaluated against the join result,
	// so columns from equal conditions come from merged schema of children, instead of
	// single child's schema.
	joinedSchema := expression.MergeSchema(p.children[0].Schema(), p.children[1].Schema())
	for i, cond := range p.PhysicalHashJoin.EqualConditions {
		newSf, err := cond.ResolveIndices(joinedSchema)
		if err != nil {
			return err
		}
		p.PhysicalHashJoin.EqualConditions[i] = newSf.(*expression.ScalarFunction)
	}
	return
}

// ResolveIndices implements Plan interface.
func (p *Update) ResolveIndices() (err error) {
	err = p.baseSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	schema := p.SelectPlan.Schema()
	for _, assign := range p.OrderedList {
		newCol, err := assign.Col.ResolveIndices(schema)
		if err != nil {
			return err
		}
		assign.Col = newCol.(*expression.Column)
		assign.Expr, err = assign.Expr.ResolveIndices(schema)
		if err != nil {
			return err
		}
	}
	return
}

// ResolveIndices implements Plan interface.
func (p *PhysicalLock) ResolveIndices() (err error) {
	err = p.basePhysicalPlan.ResolveIndices()
	if err != nil {
		return err
	}
	for i, cols := range p.TblID2Handle {
		for j, col := range cols {
			resolvedCol, err := col.ResolveIndices(p.children[0].Schema())
			if err != nil {
				return err
			}
			p.TblID2Handle[i][j] = resolvedCol.(*expression.Column)
		}
	}
	return nil
}

// ResolveIndices implements Plan interface.
func (p *Insert) ResolveIndices() (err error) {
	err = p.baseSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	for _, asgn := range p.OnDuplicate {
		newCol, err := asgn.Col.ResolveIndices(p.tableSchema)
		if err != nil {
			return err
		}
		asgn.Col = newCol.(*expression.Column)
		asgn.Expr, err = asgn.Expr.ResolveIndices(p.Schema4OnDuplicate)
		if err != nil {
			return err
		}
	}
	for _, set := range p.SetList {
		newCol, err := set.Col.ResolveIndices(p.tableSchema)
		if err != nil {
			return err
		}
		set.Col = newCol.(*expression.Column)
		set.Expr, err = set.Expr.ResolveIndices(p.tableSchema)
		if err != nil {
			return err
		}
	}
	for i, expr := range p.GenCols.Exprs {
		p.GenCols.Exprs[i], err = expr.ResolveIndices(p.tableSchema)
		if err != nil {
			return err
		}
	}
	for _, asgn := range p.GenCols.OnDuplicates {
		newCol, err := asgn.Col.ResolveIndices(p.tableSchema)
		if err != nil {
			return err
		}
		asgn.Col = newCol.(*expression.Column)
		asgn.Expr, err = asgn.Expr.ResolveIndices(p.Schema4OnDuplicate)
		if err != nil {
			return err
		}
	}
	return
}

func (p *physicalSchemaProducer) ResolveIndices() (err error) {
	err = p.basePhysicalPlan.ResolveIndices()
	return err
}

func (p *baseSchemaProducer) ResolveIndices() (err error) {
	return
}

// ResolveIndices implements Plan interface.
func (p *basePhysicalPlan) ResolveIndices() (err error) {
	for _, child := range p.children {
		err = child.ResolveIndices()
		if err != nil {
			return err
		}
	}
	return
}
