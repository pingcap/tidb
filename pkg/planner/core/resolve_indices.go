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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/util/disjointset"
)

// resolveIndicesItself resolve indices for PhysicalPlan itself
func resolveIndicesItself4PhysicalProjection(p *physicalop.PhysicalProjection) (err error) {
	for i, expr := range p.Exprs {
		p.Exprs[i], err = expr.ResolveIndices(p.Children()[0].Schema())
		if err != nil {
			return err
		}
	}
	childProj, isProj := p.Children()[0].(*physicalop.PhysicalProjection)
	if !isProj {
		return
	}
	refine4NeighbourProj(p, childProj)
	return
}

// resolveIndices4PhysicalProjection implements Plan interface.
func resolveIndices4PhysicalProjection(pp base.PhysicalPlan) (err error) {
	p := pp.(*physicalop.PhysicalProjection)
	err = p.PhysicalSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	return resolveIndicesItself4PhysicalProjection(p)
}

// refine4NeighbourProj refines the index for p.Exprs whose type is *Column when
// there is two neighbouring Projections.
// This function is introduced because that different childProj.Expr may refer
// to the same index of childProj.Schema, so we need to keep this relation
// between the specified expressions in the parent Projection.
func refine4NeighbourProj(p, childProj *physicalop.PhysicalProjection) {
	inputIdx2OutputIdxes := make(map[int][]int)
	for i, expr := range childProj.Exprs {
		col, isCol := expr.(*expression.Column)
		if !isCol {
			continue
		}
		inputIdx2OutputIdxes[col.Index] = append(inputIdx2OutputIdxes[col.Index], i)
	}
	childSchemaUnionSet := disjointset.NewIntSet(childProj.Schema().Len())
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

// resolveIndices4PhysicalUnionScan implements Plan interface.
func resolveIndices4PhysicalUnionScan(pp base.PhysicalPlan) (err error) {
	p := pp.(*physicalop.PhysicalUnionScan)
	err = p.BasePhysicalPlan.ResolveIndices()
	if err != nil {
		return err
	}
	for i, expr := range p.Conditions {
		p.Conditions[i], err = expr.ResolveIndices(p.Children()[0].Schema())
		if err != nil {
			return err
		}
	}
	resolvedHandleCol, err := p.HandleCols.ResolveIndices(p.Children()[0].Schema())
	if err != nil {
		return err
	}
	p.HandleCols = resolvedHandleCol
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
	err := resolveIndicesForVirtualColumn(p.Schema().Columns, p.Schema())
	if err != nil {
		return err
	}
	return p.tablePlan.ResolveIndices()
}

// ResolveIndices implements Plan interface.
func (p *PhysicalIndexReader) ResolveIndices() (err error) {
	err = p.PhysicalSchemaProducer.ResolveIndices()
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
			// Check if there is duplicate virtual expression column matched.
			sctx := p.SCtx()
			newExprCol, isOK := col.ResolveIndicesByVirtualExpr(sctx.GetExprCtx().GetEvalCtx(), p.indexPlan.Schema())
			if isOK {
				p.OutputColumns[i] = newExprCol.(*expression.Column)
				continue
			}
			return err
		}
		p.OutputColumns[i] = newCol.(*expression.Column)
	}
	return
}

// ResolveIndices implements Plan interface.
func (p *PhysicalIndexLookUpReader) ResolveIndices() (err error) {
	err = resolveIndicesForVirtualColumn(p.tablePlan.Schema().Columns, p.Schema())
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
	for i, commonHandleCol := range p.CommonHandleCols {
		newCol, err := commonHandleCol.ResolveIndices(p.TablePlans[0].Schema())
		if err != nil {
			return err
		}
		p.CommonHandleCols[i] = newCol.(*expression.Column)
	}
	return
}

// ResolveIndices implements Plan interface.
func (p *PhysicalIndexMergeReader) ResolveIndices() (err error) {
	err = resolveIndicesForVirtualColumn(p.tablePlan.Schema().Columns, p.Schema())
	if err != nil {
		return err
	}
	if p.tablePlan != nil {
		err = p.tablePlan.ResolveIndices()
		if err != nil {
			return err
		}
	}
	for i := range p.partialPlans {
		err = p.partialPlans[i].ResolveIndices()
		if err != nil {
			return err
		}
	}
	if p.HandleCols != nil && p.KeepOrder {
		p.HandleCols, err = p.HandleCols.ResolveIndices(p.Schema())
		if err != nil {
			return err
		}
	}
	return nil
}

// resolveIndices4PhysicalSelection implements Plan interface.
func resolveIndices4PhysicalSelection(pp base.PhysicalPlan) (err error) {
	p := pp.(*physicalop.PhysicalSelection)
	err = p.BasePhysicalPlan.ResolveIndices()
	if err != nil {
		return err
	}
	for i, expr := range p.Conditions {
		p.Conditions[i], err = expr.ResolveIndices(p.Children()[0].Schema())
		if err != nil {
			// Check if there is duplicate virtual expression column matched.
			newCond, isOk := expr.ResolveIndicesByVirtualExpr(p.SCtx().GetExprCtx().GetEvalCtx(), p.Children()[0].Schema())
			if isOk {
				p.Conditions[i] = newCond
				continue
			}
			return err
		}
	}
	return nil
}

// ResolveIndicesItself resolve indices for PhysicalPlan itself
func (p *PhysicalExchangeSender) ResolveIndicesItself() (err error) {
	return p.ResolveIndicesItselfWithSchema(p.Children()[0].Schema())
}

// ResolveIndicesItselfWithSchema is added for test usage
func (p *PhysicalExchangeSender) ResolveIndicesItselfWithSchema(inputSchema *expression.Schema) (err error) {
	for i, hashCol := range p.HashCols {
		newHashCol, err := hashCol.ResolveIndices(inputSchema)
		if err != nil {
			return err
		}
		p.HashCols[i] = newHashCol
	}
	return
}

// ResolveIndices implements Plan interface.
func (p *PhysicalExchangeSender) ResolveIndices() (err error) {
	err = p.BasePhysicalPlan.ResolveIndices()
	if err != nil {
		return err
	}
	return p.ResolveIndicesItself()
}

// ResolveIndicesItself resolve indices for PhysicalPlan itself
func (p *PhysicalExpand) ResolveIndicesItself() (err error) {
	// for version 1
	for _, gs := range p.GroupingSets {
		for _, groupingExprs := range gs {
			for k, groupingExpr := range groupingExprs {
				gExpr, err := groupingExpr.ResolveIndices(p.Children()[0].Schema())
				if err != nil {
					return err
				}
				groupingExprs[k] = gExpr
			}
		}
	}
	// for version 2
	for i, oneLevel := range p.LevelExprs {
		for j, expr := range oneLevel {
			// expr in expand level-projections only contains column ref and literal constant projection.
			p.LevelExprs[i][j], err = expr.ResolveIndices(p.Children()[0].Schema())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// ResolveIndices implements Plan interface.
func (p *PhysicalExpand) ResolveIndices() (err error) {
	err = p.PhysicalSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	return p.ResolveIndicesItself()
}

// resolveIndicesForSort is a helper function to resolve indices for sort operators.
func resolveIndicesForSort(pp base.PhysicalPlan) (err error) {
	p := pp.(*physicalop.BasePhysicalPlan)
	err = p.ResolveIndices()
	if err != nil {
		return err
	}

	var byItems []*util.ByItems
	switch x := p.Self.(type) {
	case *physicalop.PhysicalSort:
		byItems = x.ByItems
	case *physicalop.NominalSort:
		byItems = x.ByItems
	default:
		return errors.Errorf("expect PhysicalSort or NominalSort, but got %s", p.TP())
	}
	for _, item := range byItems {
		item.Expr, err = item.Expr.ResolveIndices(p.Children()[0].Schema())
		if err != nil {
			return err
		}
	}
	return err
}

// resolveIndexForInlineProjection ensures that during the execution of the physical plan, the column index can
// be correctly mapped to the column in its subplan.
func resolveIndexForInlineProjection(p *physicalop.PhysicalSchemaProducer) error {
	// To avoid that two plan shares the same column slice.
	shallowColSlice := make([]*expression.Column, p.Schema().Len())
	copy(shallowColSlice, p.Schema().Columns)
	p.SetSchema(expression.NewSchema(shallowColSlice...))
	foundCnt := 0
	// The two column sets are all ordered. And the colsNeedResolving is the subset of the mergedSchema.
	// So we can just move forward j if there's no matching is found.
	// We don't use the normal ResolvIndices here since there might be duplicate columns in the schema.
	//   e.g. The schema of child_0 is [col0, col0, col1]
	//        ResolveIndices will only resolve all col0 reference of the current plan to the first col0.
	for i, j := 0, 0; i < p.Schema().Len() && j < p.Children()[0].Schema().Len(); {
		if !p.Schema().Columns[i].Equal(nil, p.Children()[0].Schema().Columns[j]) {
			j++
			continue
		}
		p.Schema().Columns[i] = p.Schema().Columns[i].Clone().(*expression.Column)
		p.Schema().Columns[i].Index = j
		i++
		j++
		foundCnt++
	}
	if foundCnt < p.Schema().Len() {
		return errors.Errorf("Some columns of %v cannot find the reference from its child(ren)", p.ExplainID().String())
	}
	return nil
}

// resolveIndices4PhysicalTopN implements Plan interface.
func resolveIndices4PhysicalTopN(pp base.PhysicalPlan) (err error) {
	p := pp.(*physicalop.PhysicalTopN)
	err = p.PhysicalSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	for _, item := range p.ByItems {
		item.Expr, err = item.Expr.ResolveIndices(p.Children()[0].Schema())
		if err != nil {
			return err
		}
	}
	for i, item := range p.PartitionBy {
		newCol, err := item.Col.ResolveIndices(p.Children()[0].Schema())
		if err != nil {
			return err
		}
		p.PartitionBy[i].Col = newCol.(*expression.Column)
	}
	if err := resolveIndexForInlineProjection(&p.PhysicalSchemaProducer); err != nil {
		return err
	}
	return
}

// resolveIndices4PhysicalLimit implements Plan interface.
func resolveIndices4PhysicalLimit(pp base.PhysicalPlan) (err error) {
	p := pp.(*physicalop.PhysicalLimit)
	err = p.BasePhysicalPlan.ResolveIndices()
	if err != nil {
		return err
	}
	for i, item := range p.PartitionBy {
		newCol, err := item.Col.ResolveIndices(p.Children()[0].Schema())
		if err != nil {
			return err
		}
		p.PartitionBy[i].Col = newCol.(*expression.Column)
	}
	if err := resolveIndexForInlineProjection(&p.PhysicalSchemaProducer); err != nil {
		return err
	}
	return
}

// ResolveIndices implements Plan interface.
func (p *Update) ResolveIndices() (err error) {
	err = p.SimpleSchemaProducer.ResolveIndices()
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
func (p *Insert) ResolveIndices() (err error) {
	err = p.SimpleSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	for _, asgn := range p.OnDuplicate {
		newCol, err := asgn.Col.ResolveIndices(p.tableSchema)
		if err != nil {
			return err
		}
		asgn.Col = newCol.(*expression.Column)
		// Once the asgn.lazyErr exists, asgn.Expr here is nil.
		if asgn.Expr != nil {
			asgn.Expr, err = asgn.Expr.ResolveIndices(p.Schema4OnDuplicate)
			if err != nil {
				return err
			}
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
