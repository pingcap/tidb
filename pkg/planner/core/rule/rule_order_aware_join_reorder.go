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

package rule

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
	changed, _, err := r.optimizeRecursive(p, nil, nil)
	return p, changed, err
}

func (r *OrderAwareJoinReorder) optimizeRecursive(
	p base.LogicalPlan,
	orderCols []*expression.Column,
	midFilters []expression.Expression,
) (changed bool, ordered bool, err error) {
	if p == nil {
		return false, false, nil
	}
	if _, ok := p.(*logicalop.LogicalCTE); ok {
		return false, false, nil
	}
	if len(orderCols) == 0 {
		// only selection filter under order requirement can be used.
		midFilters = nil
	}

	switch node := p.(type) {
	// for TopN and Sort, we extract the ordering columns and pass down to children so they can annotate join groups with leading preferences!
	case *logicalop.LogicalTopN:
		extractedOrder := extractOrderingColumns(node.ByItems)
		childChanged, childOrdered, err := r.optimizeChildren(node.Children(), extractedOrder, nil, 0)
		if err != nil {
			return false, false, err
		}
		if len(orderCols) > 0 {
			return changed || childChanged, sameOrderingColumns(orderCols, extractedOrder), nil
		}
		return changed || childChanged, childOrdered, nil
	case *logicalop.LogicalSort:
		extractedOrder := extractOrderingColumns(node.ByItems)
		childChanged, childOrdered, err := r.optimizeChildren(node.Children(), extractedOrder, nil, 0)
		if err != nil {
			return false, false, err
		}
		if len(orderCols) > 0 {
			return changed || childChanged, sameOrderingColumns(orderCols, extractedOrder), nil
		}
		return changed || childChanged, childOrdered, nil
	case *logicalop.LogicalProjection:
		rewrittenOrder := rewriteOrderingForProjection(node, orderCols)
		childChanged, childOrdered, err := r.optimizeChildren(node.Children(), rewrittenOrder, midFilters, 0)
		if err != nil {
			return false, false, err
		}
		if len(orderCols) > 0 {
			return changed || childChanged, len(rewrittenOrder) > 0 && childOrdered, nil
		}
		return changed || childChanged, childOrdered, nil
	case *logicalop.LogicalLimit:
		childChanged, childOrdered, err := r.optimizeChildren(node.Children(), orderCols, midFilters, 0)
		return changed || childChanged, childOrdered, err
	case *logicalop.LogicalSelection:
		canPushThroughSelection := node.SCtx().GetSessionVars().TiDBOptJoinReorderThroughSel &&
			!slices.ContainsFunc(node.Conditions, expression.IsMutableEffectsExpr)
		if canPushThroughSelection {
			var accumulatedFilters []expression.Expression
			if len(orderCols) > 0 {
				// These filters may help child groups preserve the same ordering
				// by fixing leading index columns above the current anchor join.
				accumulatedFilters = append(slices.Clone(midFilters), node.Conditions...)
			}
			var localChoice *joinorder.OrderedLeadingChoice
			if len(orderCols) > 0 && shouldUseCDCBasedJoinReorder(node) {
				localChoice = joinorder.FindOrderedLeadingChoice(node, orderCols)
			}
			if localChoice != nil && len(localChoice.Vertices) > 0 {
				// This subtree owns the required ordering columns, so recurse into it
				// first and only annotate the current anchor after the chosen child
				// proves it can still satisfy the order requirement.
				childChanged, childOrdered, err := r.optimizeChildren(localChoice.Vertices, orderCols, accumulatedFilters, localChoice.CarrierVertex.ID())
				if err != nil {
					return false, false, err
				}
				if childOrdered && joinorder.TryAnnotateOrderedLeading(node, localChoice) {
					changed = true
					return changed || childChanged, true, nil
				}
				return changed || childChanged, false, nil
			}
			childChanged, childOrdered, err := r.optimizeChildren(node.Children(), orderCols, accumulatedFilters, 0)
			return changed || childChanged, childOrdered, err
		}
		childChanged, _, err := r.optimizeChildren(node.Children(), nil, nil, 0)
		return changed || childChanged, false, err
	case *logicalop.LogicalJoin:
		var localChoice *joinorder.OrderedLeadingChoice
		if len(orderCols) > 0 && shouldUseCDCBasedJoinReorder(node) {
			localChoice = joinorder.FindOrderedLeadingChoice(node, orderCols)
		}
		if localChoice != nil && len(localChoice.Vertices) > 0 {
			childChanged, childOrdered, err := r.optimizeChildren(localChoice.Vertices, orderCols, midFilters, localChoice.CarrierVertex.ID())
			if err != nil {
				return false, false, err
			}
			if childOrdered && joinorder.TryAnnotateOrderedLeading(node, localChoice) {
				changed = true
				return changed || childChanged, true, nil
			}
			return changed || childChanged, false, nil
		}
		// The order requirement does not belong to any vertex of this join group,
		// so there is no subtree worth propagating into from this point.
		childChanged, _, err := r.optimizeChildren(node.Children(), nil, nil, 0)
		return changed || childChanged, false, err
	case *logicalop.DataSource:
		if len(orderCols) == 0 {
			return false, false, nil
		}
		return false, joinorder.DsSatisfiesOrdering(node, orderCols, midFilters), nil
	default:
		childChanged, _, err := r.optimizeChildren(p.Children(), nil, nil, 0)
		return changed || childChanged, false, err
	}
}

func (r *OrderAwareJoinReorder) optimizeChildren(
	children []base.LogicalPlan,
	orderCols []*expression.Column,
	parentFilters []expression.Expression,
	vertexIDShouldFollowOrder int,
) (changed bool, ordered bool, err error) {
	for _, child := range children {
		nextOrderCols := orderCols
		nextParentFilters := parentFilters
		if vertexIDShouldFollowOrder == 0 || vertexIDShouldFollowOrder == child.ID() {
			// A zero vertex ID means we are still walking the tree to find the
			// first anchor join, so every child keeps the current requirement.
			// Once a join group chooses one carrier vertex, only that vertex
			// continues to inherit the ordering and accumulated filters.
			childChanged, childOrdered, err := r.optimizeRecursive(child, nextOrderCols, nextParentFilters)
			if err != nil {
				return false, false, err
			}
			changed = changed || childChanged
			ordered = ordered || childOrdered
			continue
		}
		childChanged, _, err := r.optimizeRecursive(child, nil, nil)
		if err != nil {
			return false, false, err
		}
		changed = changed || childChanged
	}
	return changed, ordered, nil
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

func sameOrderingColumns(left, right []*expression.Column) bool {
	if len(left) == 0 || len(right) == 0 {
		return false
	}
	return slices.EqualFunc(left, right, func(leftCol, rightCol *expression.Column) bool {
		return leftCol != nil && rightCol != nil && leftCol.UniqueID == rightCol.UniqueID
	})
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
