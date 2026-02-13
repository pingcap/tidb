// Copyright 2026 PingCAP, Inc.
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

package logicalop

import (
	"maps"
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/property"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	hintutil "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/ranger"
	sliceutil "github.com/pingcap/tidb/pkg/util/slice"
)

func cloneBaseLogicalPlan(op *BaseLogicalPlan, newSelf base.LogicalPlan) BaseLogicalPlan {
	cloned := *op
	cloned.self = newSelf
	if op.taskMap != nil {
		cloned.taskMap = maps.Clone(op.taskMap)
	}
	cloned.taskMapBak = slices.Clone(op.taskMapBak)
	cloned.taskMapBakTS = slices.Clone(op.taskMapBakTS)
	if op.children != nil {
		cloned.children = make([]base.LogicalPlan, len(op.children))
		for i, child := range op.children {
			if child != nil {
				cloned.children[i] = child.DeepClone()
			}
		}
	}
	return cloned
}

func cloneLogicalSchemaProducer(op *LogicalSchemaProducer, newSelf base.LogicalPlan) LogicalSchemaProducer {
	cloned := *op
	cloned.BaseLogicalPlan = cloneBaseLogicalPlan(&op.BaseLogicalPlan, newSelf)
	cloned.schema = cloneSchemaPtr(op.schema)
	cloned.names = cloneNameSlice(op.names)
	return cloned
}

func cloneLogicalJoinAs(op *LogicalJoin, newSelf base.LogicalPlan) LogicalJoin {
	cloned := *op
	cloned.LogicalSchemaProducer = cloneLogicalSchemaProducer(&op.LogicalSchemaProducer, newSelf)
	cloned.EqualConditions = cloneScalarFunctionSlice(op.EqualConditions)
	cloned.NAEQConditions = cloneScalarFunctionSlice(op.NAEQConditions)
	cloned.LeftConditions = cloneCNFExprs(op.LeftConditions)
	cloned.RightConditions = cloneCNFExprs(op.RightConditions)
	cloned.OtherConditions = cloneCNFExprs(op.OtherConditions)
	cloned.LeftProperties = cloneColumn2DSlice(op.LeftProperties)
	cloned.RightProperties = cloneColumn2DSlice(op.RightProperties)
	cloned.DefaultValues = cloneDatumSlice(op.DefaultValues)
	cloned.FullSchema = cloneSchemaPtr(op.FullSchema)
	cloned.FullNames = cloneNameSlice(op.FullNames)
	cloned.allJoinLeaf = cloneSchemaSlice(op.allJoinLeaf)
	return cloned
}

func cloneLogicalUnionAllAs(op *LogicalUnionAll, newSelf base.LogicalPlan) LogicalUnionAll {
	cloned := *op
	cloned.LogicalSchemaProducer = cloneLogicalSchemaProducer(&op.LogicalSchemaProducer, newSelf)
	return cloned
}

func cloneShowContents(src ShowContents) ShowContents {
	cloned := src
	cloned.Roles = slices.Clone(src.Roles)
	return cloned
}

func cloneCTEClass(src *CTEClass) *CTEClass {
	if src == nil {
		return nil
	}
	cloned := new(CTEClass)
	*cloned = *src
	if src.SeedPartLogicalPlan != nil {
		cloned.SeedPartLogicalPlan = src.SeedPartLogicalPlan.DeepClone()
	}
	if src.RecursivePartLogicalPlan != nil {
		cloned.RecursivePartLogicalPlan = src.RecursivePartLogicalPlan.DeepClone()
	}
	cloned.PushDownPredicates = cloneExprSlice(src.PushDownPredicates)
	cloned.ColumnMap = cloneColumnMap(src.ColumnMap)
	return cloned
}

func cloneWindowFrame(src *WindowFrame) *WindowFrame {
	if src == nil {
		return nil
	}
	return src.Clone()
}

func cloneExprSlice(src []expression.Expression) []expression.Expression {
	return expression.DeepCopyExprs(src)
}

func cloneCNFExprs(src expression.CNFExprs) expression.CNFExprs {
	return expression.DeepCopyCNFExprs(src)
}

func cloneExpr2DSlice(src [][]expression.Expression) [][]expression.Expression {
	if src == nil {
		return nil
	}
	cloned := make([][]expression.Expression, len(src))
	for i, one := range src {
		cloned[i] = cloneExprSlice(one)
	}
	return cloned
}

func cloneScalarFunctionSlice(src []*expression.ScalarFunction) []*expression.ScalarFunction {
	if src == nil {
		return nil
	}
	cloned := make([]*expression.ScalarFunction, len(src))
	for i, one := range src {
		if one != nil {
			cloned[i] = expression.DeepCopyExpr(one).(*expression.ScalarFunction)
		}
	}
	return cloned
}

func cloneCorrelatedColumnSlice(src []*expression.CorrelatedColumn) []*expression.CorrelatedColumn {
	if src == nil {
		return nil
	}
	cloned := make([]*expression.CorrelatedColumn, len(src))
	for i, one := range src {
		if one != nil {
			cloned[i] = expression.DeepCopyExpr(one).(*expression.CorrelatedColumn)
		}
	}
	return cloned
}

func cloneAggFuncDescSlice(src []*aggregation.AggFuncDesc) []*aggregation.AggFuncDesc {
	if src == nil {
		return nil
	}
	cloned := make([]*aggregation.AggFuncDesc, len(src))
	for i, one := range src {
		if one != nil {
			clonedOne := one.Clone()
			clonedOne.Args = expression.DeepCopyExprs(one.Args)
			if one.RetTp != nil {
				clonedOne.RetTp = one.RetTp.DeepCopy()
			}
			if len(one.OrderByItems) > 0 {
				clonedOne.OrderByItems = make([]*plannerutil.ByItems, len(one.OrderByItems))
				for j, byItem := range one.OrderByItems {
					if byItem != nil {
						clonedOne.OrderByItems[j] = &plannerutil.ByItems{
							Expr: expression.DeepCopyExpr(byItem.Expr),
							Desc: byItem.Desc,
						}
					}
				}
			}
			cloned[i] = clonedOne
		}
	}
	return cloned
}

func cloneWindowFuncDescSlice(src []*aggregation.WindowFuncDesc) []*aggregation.WindowFuncDesc {
	if src == nil {
		return nil
	}
	cloned := make([]*aggregation.WindowFuncDesc, len(src))
	for i, one := range src {
		if one != nil {
			clonedOne := one.Clone()
			clonedOne.Args = expression.DeepCopyExprs(one.Args)
			if one.RetTp != nil {
				clonedOne.RetTp = one.RetTp.DeepCopy()
			}
			cloned[i] = clonedOne
		}
	}
	return cloned
}

func cloneColumnSlice(src []*expression.Column) []*expression.Column {
	if src == nil {
		return nil
	}
	cloned := make([]*expression.Column, len(src))
	for i, one := range src {
		if one != nil {
			cloned[i] = expression.DeepCopyExpr(one).(*expression.Column)
		}
	}
	return cloned
}

func cloneColumn2DSlice(src [][]*expression.Column) [][]*expression.Column {
	if src == nil {
		return nil
	}
	cloned := make([][]*expression.Column, len(src))
	for i, one := range src {
		cloned[i] = cloneColumnSlice(one)
	}
	return cloned
}

func cloneColumnPtr(src *expression.Column) *expression.Column {
	if src == nil {
		return nil
	}
	return expression.DeepCopyExpr(src).(*expression.Column)
}

func cloneSchemaPtr(src *expression.Schema) *expression.Schema {
	if src == nil {
		return nil
	}
	return expression.DeepCopySchema(src)
}

func cloneSchemaSlice(src []*expression.Schema) []*expression.Schema {
	if src == nil {
		return nil
	}
	cloned := make([]*expression.Schema, len(src))
	for i, one := range src {
		cloned[i] = cloneSchemaPtr(one)
	}
	return cloned
}

func cloneByItemsSlice(src []*plannerutil.ByItems) []*plannerutil.ByItems {
	if src == nil {
		return nil
	}
	cloned := make([]*plannerutil.ByItems, len(src))
	for i, one := range src {
		if one != nil {
			cloned[i] = &plannerutil.ByItems{
				Expr: expression.DeepCopyExpr(one.Expr),
				Desc: one.Desc,
			}
		}
	}
	return cloned
}

func cloneSortItems(src []property.SortItem) []property.SortItem {
	if src == nil {
		return nil
	}
	cloned := make([]property.SortItem, len(src))
	for i, one := range src {
		cloned[i] = property.SortItem{
			Col:  cloneColumnPtr(one.Col),
			Desc: one.Desc,
		}
	}
	return cloned
}

func cloneRangeSlice(src []*ranger.Range) []*ranger.Range {
	return sliceutil.DeepClone(src)
}

func cloneAccessPathSlice(src []*plannerutil.AccessPath) []*plannerutil.AccessPath {
	return sliceutil.DeepClone(src)
}

func cloneDatumSlice(src []types.Datum) []types.Datum {
	return plannerutil.CloneDatums(src)
}

func cloneFieldNameSlice(src []*types.FieldName) []*types.FieldName {
	return plannerutil.CloneFieldNames(src)
}

func cloneNameSlice(src types.NameSlice) types.NameSlice {
	return types.NameSlice(plannerutil.CloneFieldNames([]*types.FieldName(src)))
}

func cloneHandleCols(src plannerutil.HandleCols) plannerutil.HandleCols {
	if src == nil {
		return nil
	}
	return src.Clone()
}

func cloneModelColumnInfoSlice(src []*model.ColumnInfo) []*model.ColumnInfo {
	if src == nil {
		return nil
	}
	cloned := make([]*model.ColumnInfo, len(src))
	for i, one := range src {
		if one != nil {
			cloned[i] = one.Clone()
		}
	}
	return cloned
}

func cloneMapInt64Column(src map[int64]*expression.Column) map[int64]*expression.Column {
	if src == nil {
		return nil
	}
	cloned := make(map[int64]*expression.Column, len(src))
	for k, v := range src {
		cloned[k] = cloneColumnPtr(v)
	}
	return cloned
}

func cloneMapInt64HandleCols(src map[int64][]plannerutil.HandleCols) map[int64][]plannerutil.HandleCols {
	if src == nil {
		return nil
	}
	cloned := make(map[int64][]plannerutil.HandleCols, len(src))
	for k, v := range src {
		cloned[k] = plannerutil.CloneHandleCols(v)
	}
	return cloned
}

func cloneMapIntCIStrSlice(src map[int][]ast.CIStr) map[int][]ast.CIStr {
	if src == nil {
		return nil
	}
	cloned := make(map[int][]ast.CIStr, len(src))
	for k, v := range src {
		cloned[k] = slices.Clone(v)
	}
	return cloned
}

func cloneMapIntUint64Set(src map[int]map[uint64]struct{}) map[int]map[uint64]struct{} {
	if src == nil {
		return nil
	}
	cloned := make(map[int]map[uint64]struct{}, len(src))
	for k, v := range src {
		if v == nil {
			cloned[k] = nil
			continue
		}
		clonedV := make(map[uint64]struct{}, len(v))
		for kk := range v {
			clonedV[kk] = struct{}{}
		}
		cloned[k] = clonedV
	}
	return cloned
}

func cloneGroupingSets(src expression.GroupingSets) expression.GroupingSets {
	if src == nil {
		return nil
	}
	cloned := make(expression.GroupingSets, len(src))
	for i, oneSet := range src {
		clonedSet := make(expression.GroupingSet, len(oneSet))
		for j, oneExprs := range oneSet {
			clonedSet[j] = expression.GroupingExprs(cloneExprSlice(oneExprs))
		}
		cloned[i] = clonedSet
	}
	return cloned
}

func cloneColumnMap(src map[string]*expression.Column) map[string]*expression.Column {
	if src == nil {
		return nil
	}
	cloned := make(map[string]*expression.Column, len(src))
	for k, v := range src {
		cloned[k] = cloneColumnPtr(v)
	}
	return cloned
}

func cloneIndexHintSlice(src []*ast.IndexHint) []*ast.IndexHint {
	return slices.Clone(src)
}

func cloneHintedIndexSlice(src []hintutil.HintedIndex) []hintutil.HintedIndex {
	return slices.Clone(src)
}

func cloneCIStrSlice(src []ast.CIStr) []ast.CIStr {
	return slices.Clone(src)
}

func cloneStringSlice(src []string) []string {
	return slices.Clone(src)
}

func cloneIntSlice(src []int) []int {
	return slices.Clone(src)
}

func cloneUint64Slice(src []uint64) []uint64 {
	return slices.Clone(src)
}
