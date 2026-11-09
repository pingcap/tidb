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

package core

import (
	"context"
	"math"
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/joinorder"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
)

type orderAwareMode uint8

const (
	orderAwareDetect orderAwareMode = iota
	orderAwareAnnotate
)

type orderAwareCandidateDetector struct{}

func (*orderAwareCandidateDetector) Optimize(_ context.Context, p base.LogicalPlan, _ *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	found, err := (&orderAwareOptimizer{mode: orderAwareDetect}).optimize(p)
	if err != nil {
		return p, false, err
	}
	if found {
		p.SCtx().GetSessionVars().StmtCtx.MarkAlternativeLogicalPlanOrderAwareJoinReorder()
	}
	return p, false, nil
}

func (*orderAwareCandidateDetector) Name() string { return "order_aware_join_reorder_detector" }

type orderAwareJoinReorder struct{}

func (*orderAwareJoinReorder) Optimize(_ context.Context, p base.LogicalPlan, _ *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	changed, err := (&orderAwareOptimizer{mode: orderAwareAnnotate}).optimize(p)
	return p, changed, err
}

func (*orderAwareJoinReorder) Name() string { return "order_aware_join_reorder" }

type orderAwareOptimizer struct {
	mode    orderAwareMode
	invalid bool
}

type orderAwareContext struct {
	requiredOrder []property.SortItem
	filters       []expression.Expression
}

func (o *orderAwareOptimizer) optimize(root base.LogicalPlan) (bool, error) {
	annotations := make([]*joinorder.OrderedLeadingAnnotation, 0)
	o.collect(root, &annotations)
	if o.invalid {
		return false, nil
	}
	annotations = combineOrderAwareAnnotations(annotations)
	if len(annotations) == 0 {
		return false, nil
	}
	if o.mode == orderAwareAnnotate {
		joinorder.ApplyOrderedLeadingAnnotations(annotations)
		return true, nil
	}
	return true, nil
}

// collect discovers each independent TopN region in one traversal. Validation
// of a region's required order must not restart this discovery traversal.
func (o *orderAwareOptimizer) collect(plan base.LogicalPlan, annotations *[]*joinorder.OrderedLeadingAnnotation) {
	if plan == nil {
		return
	}
	if topN, ok := plan.(*logicalop.LogicalTopN); ok {
		order, valid := extractOrderAwareTopNOrder(topN)
		if valid {
			o.collectForOrder(topN.Children()[0], orderAwareContext{requiredOrder: order}, annotations)
		}
	}
	for _, child := range plan.Children() {
		o.collect(child, annotations)
	}
}

// collectForOrder validates only ctx.requiredOrder and collects annotations
// along its carrier path. A nested TopN is a boundary; collect visits that
// region independently, so failures here must not rescan any subtrees.
func (o *orderAwareOptimizer) collectForOrder(plan base.LogicalPlan, ctx orderAwareContext, annotations *[]*joinorder.OrderedLeadingAnnotation) bool {
	if plan == nil {
		return false
	}

	switch p := plan.(type) {
	case *logicalop.LogicalProjection:
		childOrder, ok := rewriteOrderAwareProjection(p, ctx.requiredOrder)
		if !ok {
			return false
		}
		return o.collectForOrder(p.Children()[0], orderAwareContext{requiredOrder: childOrder, filters: ctx.filters}, annotations)
	case *logicalop.LogicalSelection:
		if slices.ContainsFunc(p.Conditions, func(expr expression.Expression) bool {
			return expression.IsMutableEffectsExpr(expr) || expression.CheckNonDeterministic(expr)
		}) {
			return false
		}
		filters := append(slices.Clone(ctx.filters), p.Conditions...)
		return o.collectForOrder(p.Children()[0], orderAwareContext{requiredOrder: ctx.requiredOrder, filters: filters}, annotations)
	case *logicalop.LogicalLimit:
		return o.collectForOrder(p.Children()[0], ctx, annotations)
	case *logicalop.LogicalJoin:
		choice, ok := joinorder.FindOrderedLeadingChoice(p, ctx.requiredOrder)
		if !ok {
			return false
		}
		if !o.collectForOrder(choice.Carrier, ctx, annotations) {
			return false
		}
		annotation, ok := joinorder.BuildOrderedLeadingAnnotation(p, choice)
		if !ok {
			o.invalid = true
			return false
		}
		*annotations = append(*annotations, annotation)
		return true
	case *logicalop.DataSource:
		return hasStrictOrderedAccessPath(p, ctx.requiredOrder, ctx.filters)
	default:
		return false
	}
}

func extractOrderAwareTopNOrder(topN *logicalop.LogicalTopN) ([]property.SortItem, bool) {
	if topN == nil || topN.Count == 0 || len(topN.PartitionBy) > 0 || topN.Offset > math.MaxUint64-topN.Count {
		return nil, false
	}
	items := make([]property.SortItem, 0, len(topN.ByItems))
	for _, item := range topN.ByItems {
		col, ok := item.Expr.(*expression.Column)
		if !ok || col == nil {
			return nil, false
		}
		items = append(items, property.SortItem{Col: col, Desc: item.Desc})
	}
	if len(items) == 0 {
		return nil, false
	}
	for _, item := range items[1:] {
		if item.Desc != items[0].Desc {
			return nil, false
		}
	}
	return items, true
}

func rewriteOrderAwareProjection(proj *logicalop.LogicalProjection, order []property.SortItem) ([]property.SortItem, bool) {
	if proj == nil || len(proj.Children()) != 1 {
		return nil, false
	}
	childOrder := make([]property.SortItem, 0, len(order))
	used := make(map[int64]struct{}, len(order))
	for _, item := range order {
		position := -1
		for i, col := range proj.Schema().Columns {
			if col != nil && item.Col != nil && col.EqualColumn(item.Col) {
				if position != -1 {
					return nil, false
				}
				position = i
			}
		}
		if position < 0 || position >= len(proj.Exprs) {
			return nil, false
		}
		col, ok := proj.Exprs[position].(*expression.Column)
		if !ok || col == nil {
			return nil, false
		}
		if _, exists := used[col.UniqueID]; exists {
			return nil, false
		}
		used[col.UniqueID] = struct{}{}
		childOrder = append(childOrder, property.SortItem{Col: col, Desc: item.Desc})
	}
	return childOrder, true
}

func combineOrderAwareAnnotations(input []*joinorder.OrderedLeadingAnnotation) []*joinorder.OrderedLeadingAnnotation {
	if len(input) == 0 {
		return nil
	}
	byAnchor := make(map[*logicalop.LogicalJoin]*joinorder.OrderedLeadingAnnotation, len(input))
	result := make([]*joinorder.OrderedLeadingAnnotation, 0, len(input))
	for _, annotation := range input {
		if annotation == nil || annotation.Anchor == nil || annotation.Choice == nil || annotation.Hints == nil {
			return nil
		}
		if old, exists := byAnchor[annotation.Anchor]; exists {
			if old.Choice.Carrier != annotation.Choice.Carrier {
				return nil
			}
			continue
		}
		byAnchor[annotation.Anchor] = annotation
		result = append(result, annotation)
	}
	return result
}

func hasStrictOrderedAccessPath(ds *logicalop.DataSource, order []property.SortItem, filters []expression.Expression) bool {
	if ds == nil || ds.TableInfo == nil || ds.TableInfo.Partition != nil || ds.TableInfo.IsCommonHandle {
		return false
	}
	// PossibleAccessPaths already reflects USE/FORCE/IGNORE INDEX semantics.
	paths := ds.PossibleAccessPaths
	allFilters := append(slices.Clone(filters), ds.PushedDownConds...)
	fixed := fixedOrderColumns(allFilters)
	for _, path := range paths {
		if path == nil || path.StoreType != kv.TiKV || path.Index == nil || path.Index.State != model.StatePublic ||
			path.Index.Tp != pmodel.IndexTypeBtree || path.Index.ConditionExprString != "" || path.Index.Invisible ||
			path.Index.MVIndex || path.Index.Global || path.IsIntHandlePath || path.IsCommonHandlePath ||
			path.ForceNoKeepOrder || len(path.PartialIndexPaths) > 0 || len(path.PartialAlternativeIndexPaths) > 0 {
			continue
		}
		if hasGeneratedOrderColumn(ds, path.FullIdxCols, order) {
			continue
		}
		constCols := make([]bool, len(path.FullIdxCols))
		for i, col := range path.FullIdxCols {
			if col != nil {
				_, constCols[i] = fixed[col.UniqueID]
			}
		}
		if matchStrictIndexOrder(path.FullIdxCols, path.FullIdxColLens, constCols, order) {
			return true
		}
	}
	return false
}

func fixedOrderColumns(filters []expression.Expression) map[int64]struct{} {
	fixed := make(map[int64]struct{})
	for _, filter := range filters {
		sf, ok := filter.(*expression.ScalarFunction)
		if !ok || sf == nil {
			continue
		}
		args := sf.GetArgs()
		// Single-value IN is rewritten to EQ. Multi-value IN may need a merge
		// sort across ranges, so only literal equality fixes an index column
		// for strict ordering.
		if sf.FuncName.L == ast.EQ && len(args) == 2 {
			if col, ok := args[0].(*expression.Column); ok && isLiteralOrderConstant(args[1]) {
				fixed[col.UniqueID] = struct{}{}
			}
			if col, ok := args[1].(*expression.Column); ok && isLiteralOrderConstant(args[0]) {
				fixed[col.UniqueID] = struct{}{}
			}
		}
	}
	return fixed
}

func isLiteralOrderConstant(expr expression.Expression) bool {
	constant, ok := expr.(*expression.Constant)
	return ok && constant != nil && constant.ParamMarker == nil && constant.DeferredExpr == nil
}

func hasGeneratedOrderColumn(ds *logicalop.DataSource, indexCols []*expression.Column, order []property.SortItem) bool {
	orderIDs := make(map[int64]struct{}, len(order))
	for _, item := range order {
		if item.Col != nil {
			orderIDs[item.Col.UniqueID] = struct{}{}
		}
	}
	for _, col := range indexCols {
		if col == nil {
			continue
		}
		if _, relevant := orderIDs[col.UniqueID]; !relevant {
			continue
		}
		for i, tableCol := range ds.TblCols {
			if tableCol == nil || tableCol.UniqueID != col.UniqueID || i >= len(ds.Columns) {
				continue
			}
			if ds.Columns[i].IsGenerated() {
				return true
			}
		}
	}
	return false
}
