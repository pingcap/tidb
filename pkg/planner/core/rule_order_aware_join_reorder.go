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
)

// OrderAwareJoinReorder annotates a join group with an internal leading
// preference when a TopN above the group can benefit from keeping one leaf's
// index order alive.
type OrderAwareJoinReorder struct{}

// Optimize implements the base.LogicalOptRule.<0th> interface.
func (r *OrderAwareJoinReorder) Optimize(_ context.Context, p base.LogicalPlan) (base.LogicalPlan, bool, error) {
	changed, err := r.optimizeRecursive(p, nil)
	return p, changed, err
}

func (r *OrderAwareJoinReorder) optimizeRecursive(p base.LogicalPlan, parentFilters []expression.Expression) (bool, error) {
	if p == nil {
		return false, nil
	}
	if _, ok := p.(*logicalop.LogicalCTE); ok {
		return false, nil
	}

	changed := false
	switch node := p.(type) {
	case *logicalop.LogicalSelection:
		canPushThroughSelection := node.SCtx().GetSessionVars().TiDBOptJoinReorderThroughSel &&
			!slices.ContainsFunc(node.Conditions, expression.IsMutableEffectsExpr)
		if canPushThroughSelection {
			if orderedJoin := findOrderedJoinUnder(node); orderedJoin != nil && shouldUseCDCBasedJoinReorder(node) {
				if joinorder.AnnotateOrderedLeading(node, orderedJoin.OrderProperties, nil) {
					changed = true
				}
			}
			accumulatedFilters := append(slices.Clone(parentFilters), node.Conditions...)
			childChanged, err := r.optimizeChildren(node, accumulatedFilters)
			return changed || childChanged, err
		}
		childChanged, err := r.optimizeChildren(node, nil)
		return changed || childChanged, err
	case *logicalop.LogicalJoin:
		if len(node.OrderProperties) > 0 && shouldUseCDCBasedJoinReorder(node) {
			if joinorder.AnnotateOrderedLeading(node, node.OrderProperties, parentFilters) {
				changed = true
			}
		}
		childChanged, err := r.optimizeChildren(node, nil)
		return changed || childChanged, err
	case *logicalop.LogicalProjection, *logicalop.LogicalLimit, *logicalop.LogicalSort:
		childChanged, err := r.optimizeChildren(p, parentFilters)
		return changed || childChanged, err
	default:
		childChanged, err := r.optimizeChildren(p, nil)
		return changed || childChanged, err
	}
}

func (r *OrderAwareJoinReorder) optimizeChildren(p base.LogicalPlan, parentFilters []expression.Expression) (bool, error) {
	changed := false
	for _, child := range p.Children() {
		childChanged, err := r.optimizeRecursive(child, parentFilters)
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

func findOrderedJoinUnder(root base.LogicalPlan) *logicalop.LogicalJoin {
	for root != nil {
		switch node := root.(type) {
		case *logicalop.LogicalJoin:
			if len(node.OrderProperties) > 0 {
				return node
			}
			return nil
		case *logicalop.LogicalSelection, *logicalop.LogicalProjection, *logicalop.LogicalLimit, *logicalop.LogicalSort:
			children := root.Children()
			if len(children) != 1 {
				return nil
			}
			root = children[0]
		default:
			return nil
		}
	}
	return nil
}

// Name implements the base.LogicalOptRule.<1st> interface.
func (*OrderAwareJoinReorder) Name() string {
	return "order_aware_join_reorder"
}
