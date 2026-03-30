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
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/joinorder"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
)

// OrderAwareJoinReorder annotates a join group with an internal leading
// preference when a TopN above the group can benefit from keeping one leaf's
// index order alive.
type OrderAwareJoinReorder struct{}

// Optimize implements the base.LogicalOptRule.<0th> interface.
func (r *OrderAwareJoinReorder) Optimize(_ context.Context, p base.LogicalPlan) (base.LogicalPlan, bool, error) {
	changed, err := r.optimizeRecursive(p, nil, nil)
	return p, changed, err
}

func (r *OrderAwareJoinReorder) optimizeRecursive(
	p base.LogicalPlan,
	orderCols []*expression.Column,
	midFilters []expression.Expression,
) (bool, error) {
	if p == nil {
		return false, nil
	}
	if _, ok := p.(*logicalop.LogicalCTE); ok {
		return false, nil
	}
	if len(orderCols) == 0 {
		// only selection filter under order requirement can be used.
		midFilters = nil
	}

	changed := false
	switch node := p.(type) {
	// for TopN and Sort, we extract the ordering columns and pass down to children so they can annotate join groups with leading preferences!
	case *logicalop.LogicalTopN:
		childChanged, err := r.optimizeChildren(node.Children(), extractOrderingColumns(node.ByItems), nil, 0)
		return changed || childChanged, err
	case *logicalop.LogicalSort:
		childChanged, err := r.optimizeChildren(node.Children(), extractOrderingColumns(node.ByItems), nil, 0)
		return changed || childChanged, err
	case *logicalop.LogicalProjection:
		childChanged, err := r.optimizeChildren(node.Children(), rewriteOrderingForProjection(node, orderCols), midFilters, 0)
		return changed || childChanged, err
	case *logicalop.LogicalLimit:
		childChanged, err := r.optimizeChildren(node.Children(), orderCols, midFilters, 0)
		return changed || childChanged, err
	case *logicalop.LogicalSelection:
		canPushThroughSelection := node.SCtx().GetSessionVars().TiDBOptJoinReorderThroughSel &&
			!slices.ContainsFunc(node.Conditions, expression.IsMutableEffectsExpr)
		if canPushThroughSelection {
			var annotated bool
			var localChoice *joinorder.OrderedLeadingChoice
			if len(orderCols) > 0 && shouldUseCDCBasedJoinReorder(node) {
				localChoice, annotated = joinorder.AnnotateOrderedLeading(node, orderCols, midFilters)
				if annotated {
					changed = true
				}
			}
			var err error
			var childChanged bool
			var accumulatedFilters []expression.Expression
			if len(orderCols) > 0 {
				// These filters may help child groups preserve the same ordering
				// by fixing leading index columns above the current anchor join.
				accumulatedFilters = append(slices.Clone(midFilters), node.Conditions...)
			}
			if localChoice != nil && len(localChoice.Vertices) > 0 {
				// order leading is applicable in detected join group, go down through vertex.
				childChanged, err = r.optimizeChildren(localChoice.Vertices, orderCols, accumulatedFilters, localChoice.CarrierVertex.ID())
			} else {
				// order leading is not applicable in detected join group, still go down through children.
				childChanged, err = r.optimizeChildren(node.Children(), nil, nil, 0)
			}
			return changed || childChanged, err
		}
		childChanged, err := r.optimizeChildren(node.Children(), nil, nil, 0)
		return changed || childChanged, err
	case *logicalop.LogicalJoin:
		var annotated bool
		var localChoice *joinorder.OrderedLeadingChoice
		if len(orderCols) > 0 && shouldUseCDCBasedJoinReorder(node) {
			localChoice, annotated = joinorder.AnnotateOrderedLeading(node, orderCols, midFilters)
			if annotated {
				changed = true
			}
		}
		if localChoice != nil && len(localChoice.Vertices) > 0 {
			// order leading is applicable in detected join group, go down through vertex.
			childChanged, err := r.optimizeChildren(localChoice.Vertices, orderCols, midFilters, localChoice.CarrierVertex.ID())
			return changed || childChanged, err
		}
		// order leading is not applicable in detected join group, still go down through children.
		childChanged, err := r.optimizeChildren(node.Children(), nil, nil, 0)
		return changed || childChanged, err
	default:
		childChanged, err := r.optimizeChildren(p.Children(), nil, nil, 0)
		return changed || childChanged, err
	}
}

func (r *OrderAwareJoinReorder) optimizeChildren(
	children []base.LogicalPlan,
	orderCols []*expression.Column,
	parentFilters []expression.Expression,
	vertexIDShouldFollowOrder int,
) (bool, error) {
	changed := false
	for _, child := range children {
		nextOrderCols := orderCols
		nextParentFilters := parentFilters
		if vertexIDShouldFollowOrder == 0 || vertexIDShouldFollowOrder == child.ID() {
			// A zero vertex ID means we are still walking the tree to find the
			// first anchor join, so every child keeps the current requirement.
			// Once a join group chooses one carrier vertex, only that vertex
			// continues to inherit the ordering and accumulated filters.
			childChanged, err := r.optimizeRecursive(child, nextOrderCols, nextParentFilters)
			if err != nil {
				return false, err
			}
			changed = changed || childChanged
			continue
		}
		childChanged, err := r.optimizeRecursive(child, nil, nil)
		if err != nil {
			return false, err
		}
		changed = changed || childChanged
	}
	return changed, nil
}

func shouldUseCDCBasedJoinReorder(p base.LogicalPlan) bool {
	vars := p.SCtx().GetSessionVars()
	return vars.TiDBOptEnableAdvancedJoinReorder && vars.TiDBOptJoinReorderThreshold <= 0
}

func extractOrderingColumns(items []*plannerutil.ByItems) []*expression.Column {
	if len(items) == 0 {
		return nil
	}
	cols := make([]*expression.Column, 0, len(items))
	for _, item := range items {
		// The current matcher only reasons about forward index order, so bail out
		// once ORDER BY contains a descending item instead of silently treating it
		// as ascending.
		if item.Desc {
			return nil
		}
		col, ok := item.Expr.(*expression.Column)
		if !ok {
			return nil
		}
		cols = append(cols, col)
	}
	return cols
}

func rewriteOrderingForProjection(
	proj *logicalop.LogicalProjection,
	orderCols []*expression.Column,
) []*expression.Column {
	if proj == nil || len(orderCols) == 0 {
		return nil
	}
	rewritten := make([]*expression.Column, 0, len(orderCols))
	for _, col := range orderCols {
		offset := proj.Schema().ColumnIndex(col)
		if offset < 0 {
			return nil
		}
		mappedCol, ok := proj.Exprs[offset].(*expression.Column)
		if !ok {
			return nil
		}
		rewritten = append(rewritten, mappedCol)
	}
	return rewritten
}

// Name implements the base.LogicalOptRule.<1st> interface.
func (*OrderAwareJoinReorder) Name() string {
	return "order_aware_join_reorder"
}
