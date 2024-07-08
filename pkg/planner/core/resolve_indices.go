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
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/util/disjointset"
)

// ResolveIndicesItself resolve indices for PhysicalPlan itself
func (p *PhysicalProjection) ResolveIndicesItself() (err error) {
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

// ResolveIndices implements Plan interface.
func (p *PhysicalProjection) ResolveIndices() (err error) {
	err = p.physicalSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	return p.ResolveIndicesItself()
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

// ResolveIndicesItself resolve indices for PhyicalPlan itself
func (p *PhysicalHashJoin) ResolveIndicesItself() (err error) {
	lSchema := p.children[0].Schema()
	rSchema := p.children[1].Schema()
	ctx := p.SCtx()
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
		p.EqualConditions[i] = expression.NewFunctionInternal(ctx.GetExprCtx(), fun.FuncName.L, fun.GetStaticType(), lArg, rArg).(*expression.ScalarFunction)
	}
	for i, fun := range p.NAEqualConditions {
		lArg, err := fun.GetArgs()[0].ResolveIndices(lSchema)
		if err != nil {
			return err
		}
		p.LeftNAJoinKeys[i] = lArg.(*expression.Column)
		rArg, err := fun.GetArgs()[1].ResolveIndices(rSchema)
		if err != nil {
			return err
		}
		p.RightNAJoinKeys[i] = rArg.(*expression.Column)
		p.NAEqualConditions[i] = expression.NewFunctionInternal(ctx.GetExprCtx(), fun.FuncName.L, fun.GetStaticType(), lArg, rArg).(*expression.ScalarFunction)
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

	colsNeedResolving := p.schema.Len()
	// The last output column of this two join is the generated column to indicate whether the row is matched or not.
	if p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		colsNeedResolving--
	}
	// To avoid that two plan shares the same column slice.
	shallowColSlice := make([]*expression.Column, p.schema.Len())
	copy(shallowColSlice, p.schema.Columns)
	p.schema = expression.NewSchema(shallowColSlice...)
	foundCnt := 0

	// Here we want to resolve all join schema columns directly as a merged schema, and you know same name
	// col in join schema should be separately redirected to corresponded same col in child schema. But two
	// column sets are **NOT** always ordered, see comment: https://github.com/pingcap/tidb/pull/45831#discussion_r1481031471
	// we are using mapping mechanism instead of moving j forward.
	marked := make([]bool, mergedSchema.Len())
	for i := 0; i < colsNeedResolving; i++ {
		findIdx := -1
		for j := 0; j < len(mergedSchema.Columns); j++ {
			if !p.schema.Columns[i].EqualColumn(mergedSchema.Columns[j]) || marked[j] {
				continue
			}
			// resolve to a same unique id one, and it not being marked.
			findIdx = j
			break
		}
		if findIdx != -1 {
			// valid one.
			p.schema.Columns[i] = p.schema.Columns[i].Clone().(*expression.Column)
			p.schema.Columns[i].Index = findIdx
			marked[findIdx] = true
			foundCnt++
		}
	}
	if foundCnt < colsNeedResolving {
		return errors.Errorf("Some columns of %v cannot find the reference from its child(ren)", p.ExplainID().String())
	}
	return
}

// ResolveIndices implements Plan interface.
func (p *PhysicalHashJoin) ResolveIndices() (err error) {
	err = p.physicalSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	return p.ResolveIndicesItself()
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

	mergedSchema := expression.MergeSchema(lSchema, rSchema)

	for i, expr := range p.OtherConditions {
		p.OtherConditions[i], err = expr.ResolveIndices(mergedSchema)
		if err != nil {
			return err
		}
	}

	colsNeedResolving := p.schema.Len()
	// The last output column of this two join is the generated column to indicate whether the row is matched or not.
	if p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		colsNeedResolving--
	}
	// To avoid that two plan shares the same column slice.
	shallowColSlice := make([]*expression.Column, p.schema.Len())
	copy(shallowColSlice, p.schema.Columns)
	p.schema = expression.NewSchema(shallowColSlice...)
	foundCnt := 0
	// The two column sets are all ordered. And the colsNeedResolving is the subset of the mergedSchema.
	// So we can just move forward j if there's no matching is found.
	// We don't use the normal ResolvIndices here since there might be duplicate columns in the schema.
	//   e.g. The schema of child_0 is [col0, col0, col1]
	//        ResolveIndices will only resolve all col0 reference of the current plan to the first col0.
	for i, j := 0, 0; i < colsNeedResolving && j < len(mergedSchema.Columns); {
		if !p.schema.Columns[i].EqualColumn(mergedSchema.Columns[j]) {
			j++
			continue
		}
		p.schema.Columns[i] = p.schema.Columns[i].Clone().(*expression.Column)
		p.schema.Columns[i].Index = j
		i++
		j++
		foundCnt++
	}
	if foundCnt < colsNeedResolving {
		return errors.Errorf("Some columns of %v cannot find the reference from its child(ren)", p.ExplainID().String())
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
	for i := range p.OuterHashKeys {
		outerKey, err := p.OuterHashKeys[i].ResolveIndices(p.children[1-p.InnerChildIdx].Schema())
		if err != nil {
			return err
		}
		innerKey, err := p.InnerHashKeys[i].ResolveIndices(p.children[p.InnerChildIdx].Schema())
		if err != nil {
			return err
		}
		p.OuterHashKeys[i], p.InnerHashKeys[i] = outerKey.(*expression.Column), innerKey.(*expression.Column)
	}

	colsNeedResolving := p.schema.Len()
	// The last output column of this two join is the generated column to indicate whether the row is matched or not.
	if p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		colsNeedResolving--
	}
	// To avoid that two plan shares the same column slice.
	shallowColSlice := make([]*expression.Column, p.schema.Len())
	copy(shallowColSlice, p.schema.Columns)
	p.schema = expression.NewSchema(shallowColSlice...)
	foundCnt := 0
	// The two column sets are all ordered. And the colsNeedResolving is the subset of the mergedSchema.
	// So we can just move forward j if there's no matching is found.
	// We don't use the normal ResolvIndices here since there might be duplicate columns in the schema.
	//   e.g. The schema of child_0 is [col0, col0, col1]
	//        ResolveIndices will only resolve all col0 reference of the current plan to the first col0.
	for i, j := 0, 0; i < colsNeedResolving && j < len(mergedSchema.Columns); {
		if !p.schema.Columns[i].EqualColumn(mergedSchema.Columns[j]) {
			j++
			continue
		}
		p.schema.Columns[i] = p.schema.Columns[i].Clone().(*expression.Column)
		p.schema.Columns[i].Index = j
		i++
		j++
		foundCnt++
	}
	if foundCnt < colsNeedResolving {
		return errors.Errorf("Some columns of %v cannot find the reference from its child(ren)", p.ExplainID().String())
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
	resolvedHandleCol, err := p.HandleCols.ResolveIndices(p.children[0].Schema())
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
	if p.HandleCols != nil && p.KeepOrder {
		p.HandleCols, err = p.HandleCols.ResolveIndices(p.schema)
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
			// Check if there is duplicate virtual expression column matched.
			newCond, isOk := expr.ResolveIndicesByVirtualExpr(p.SCtx().GetExprCtx().GetEvalCtx(), p.children[0].Schema())
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
	for i, col := range p.HashCols {
		colExpr, err1 := col.Col.ResolveIndices(p.children[0].Schema())
		if err1 != nil {
			return err1
		}
		p.HashCols[i].Col, _ = colExpr.(*expression.Column)
	}
	return
}

// ResolveIndices implements Plan interface.
func (p *PhysicalExchangeSender) ResolveIndices() (err error) {
	err = p.basePhysicalPlan.ResolveIndices()
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
				gExpr, err := groupingExpr.ResolveIndices(p.children[0].Schema())
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
			p.LevelExprs[i][j], err = expr.ResolveIndices(p.children[0].Schema())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// ResolveIndices implements Plan interface.
func (p *PhysicalExpand) ResolveIndices() (err error) {
	err = p.physicalSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	return p.ResolveIndicesItself()
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

func resolveIndicesForSort(p basePhysicalPlan) (err error) {
	err = p.ResolveIndices()
	if err != nil {
		return err
	}

	var byItems []*util.ByItems
	switch x := p.self.(type) {
	case *PhysicalSort:
		byItems = x.ByItems
	case *NominalSort:
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

// ResolveIndices implements Plan interface.
func (p *PhysicalSort) ResolveIndices() (err error) {
	return resolveIndicesForSort(p.basePhysicalPlan)
}

// ResolveIndices implements Plan interface.
func (p *NominalSort) ResolveIndices() (err error) {
	return resolveIndicesForSort(p.basePhysicalPlan)
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
	// There may be one or more DataSource
	for i := range p.ByItemArrays {
		// Each DataSource has an array of HashByItems
		for j := range p.ByItemArrays[i] {
			// "Shuffle" get value of items from `DataSource`, other than children[0].
			p.ByItemArrays[i][j], err = p.ByItemArrays[i][j].ResolveIndices(p.DataSources[i].Schema())
			if err != nil {
				return err
			}
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
	for i, item := range p.PartitionBy {
		newCol, err := item.Col.ResolveIndices(p.children[0].Schema())
		if err != nil {
			return err
		}
		p.PartitionBy[i].Col = newCol.(*expression.Column)
	}
	return
}

// ResolveIndices implements Plan interface.
func (p *PhysicalLimit) ResolveIndices() (err error) {
	err = p.basePhysicalPlan.ResolveIndices()
	if err != nil {
		return err
	}
	for i, item := range p.PartitionBy {
		newCol, err := item.Col.ResolveIndices(p.children[0].Schema())
		if err != nil {
			return err
		}
		p.PartitionBy[i].Col = newCol.(*expression.Column)
	}
	// To avoid that two plan shares the same column slice.
	shallowColSlice := make([]*expression.Column, p.schema.Len())
	copy(shallowColSlice, p.schema.Columns)
	p.schema = expression.NewSchema(shallowColSlice...)
	foundCnt := 0
	// The two column sets are all ordered. And the colsNeedResolving is the subset of the mergedSchema.
	// So we can just move forward j if there's no matching is found.
	// We don't use the normal ResolvIndices here since there might be duplicate columns in the schema.
	//   e.g. The schema of child_0 is [col0, col0, col1]
	//        ResolveIndices will only resolve all col0 reference of the current plan to the first col0.
	for i, j := 0, 0; i < p.schema.Len() && j < p.children[0].Schema().Len(); {
		if !p.schema.Columns[i].EqualColumn(p.children[0].Schema().Columns[j]) {
			j++
			continue
		}
		p.schema.Columns[i] = p.schema.Columns[i].Clone().(*expression.Column)
		p.schema.Columns[i].Index = j
		i++
		j++
		foundCnt++
	}
	if foundCnt < p.schema.Len() {
		return errors.Errorf("Some columns of %v cannot find the reference from its child(ren)", p.ExplainID().String())
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
	for i, cond := range p.PhysicalHashJoin.NAEqualConditions {
		newSf, err := cond.ResolveIndices(joinedSchema)
		if err != nil {
			return err
		}
		p.PhysicalHashJoin.NAEqualConditions[i] = newSf.(*expression.ScalarFunction)
	}
	return
}

// ResolveIndices implements Plan interface.
func (p *PhysicalTableScan) ResolveIndices() (err error) {
	err = p.physicalSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	return p.ResolveIndicesItself()
}

// ResolveIndicesItself implements PhysicalTableScan interface.
func (p *PhysicalTableScan) ResolveIndicesItself() (err error) {
	for i, column := range p.schema.Columns {
		column.Index = i
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
			p.TblID2Handle[i][j] = resolvedCol
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

func (p *physicalSchemaProducer) ResolveIndices() (err error) {
	err = p.basePhysicalPlan.ResolveIndices()
	return err
}

func (*baseSchemaProducer) ResolveIndices() (err error) {
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
