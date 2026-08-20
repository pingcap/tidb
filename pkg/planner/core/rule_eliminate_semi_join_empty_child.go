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

	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
)

// EliminateSemiJoinEmptyChild is a logical optimization rule that folds a
// SemiJoin/AntiSemiJoin into an empty TableDual when the join result is
// statically known to be empty because one of its children is an empty
// TableDual. This is the join shape TiDB builds for INTERSECT/EXCEPT, so it
// lets `A INTERSECT empty` and `empty EXCEPT B` skip scanning the non-empty
// side instead of executing the join first.
//
// The rule is gated by FlagEliminateSemiJoinEmptyChild, set only when
// buildSemiJoinForSetOperator builds a join for INTERSECT/EXCEPT (mirroring
// EliminateUnionAllDualItem's FlagEliminateUnionAllDualItem gating for UNION
// ALL). That flag is statement-wide, so once enabled the rule still walks the
// whole plan; it additionally requires LogicalJoin.FromSetOperator on each
// candidate join, so an ordinary IN/EXISTS semi-join elsewhere in the same
// statement is left untouched even if it happens to also be statically
// empty.
type EliminateSemiJoinEmptyChild struct {
}

// Name implements the LogicalOptRule's name.
func (*EliminateSemiJoinEmptyChild) Name() string {
	return "eliminate_semi_join_empty_child"
}

// Optimize implements LogicalOptRule's Optimize.
func (*EliminateSemiJoinEmptyChild) Optimize(_ context.Context, p base.LogicalPlan) (base.LogicalPlan, bool, error) {
	p, planChanged := eliminateSemiJoinEmptyChild(p)
	return p, planChanged, nil
}

// isStaticallyEmpty reports whether p is provably empty regardless of the
// data underneath it. Besides a bare empty TableDual, it recognizes the two
// wrapper shapes that INTERSECT/EXCEPT operands built from a WHERE FALSE
// branch retain after predicate pushdown:
//   - a column-aliasing Projection directly over an empty TableDual, and
//   - the grouped Aggregation that buildDistinct wraps each set-operator
//     operand in, which produces zero groups when its child is empty (this
//     does not hold for a scalar aggregate, i.e. one with no GroupByItems).
func isStaticallyEmpty(p base.LogicalPlan) bool {
	if dual, ok := p.(*logicalop.LogicalTableDual); ok {
		return dual.RowCount == 0
	}
	if proj, ok := p.(*logicalop.LogicalProjection); ok {
		return isStaticallyEmpty(proj.Children()[0])
	}
	if agg, ok := p.(*logicalop.LogicalAggregation); ok && len(agg.GroupByItems) > 0 {
		return isStaticallyEmpty(agg.Children()[0])
	}
	return false
}

func emptyJoinResultDual(join *logicalop.LogicalJoin) base.LogicalPlan {
	dual := logicalop.LogicalTableDual{}.Init(join.SCtx(), 0)
	dual.SetSchema(join.Schema())
	dual.SetOutputNames(join.OutputNames())
	return dual
}

func eliminateSemiJoinEmptyChild(p base.LogicalPlan) (base.LogicalPlan, bool) {
	// Recurse first so a chained set operator such as
	// `(A INTERSECT empty) INTERSECT B` folds its inner join into an empty
	// TableDual before the outer join is checked against it; checking the
	// outer join first would miss the now-empty child and leave B scanned.
	planChanged := false
	for i, child := range p.Children() {
		newChild, changed := eliminateSemiJoinEmptyChild(child)
		p.Children()[i] = newChild
		planChanged = planChanged || changed
	}

	if join, ok := p.(*logicalop.LogicalJoin); ok && join.FromSetOperator {
		left, right := join.Children()[0], join.Children()[1]
		switch join.JoinType {
		case base.SemiJoin:
			// A semi join outputs a left row only if it has a match on the
			// right. An empty side on either input means no output.
			if isStaticallyEmpty(left) || isStaticallyEmpty(right) {
				return emptyJoinResultDual(join), true
			}
		case base.AntiSemiJoin:
			// An anti semi join outputs a left row only if it has no match on
			// the right, so an empty left side alone forces an empty result;
			// an empty right side instead means every left row qualifies.
			if isStaticallyEmpty(left) {
				return emptyJoinResultDual(join), true
			}
		}
	}
	return p, planChanged
}
