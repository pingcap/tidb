// Copyright 2025 PingCAP, Inc.
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

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
)

// LimitOptimization removes LIMIT 1 when it's already guaranteed by unique constraints.
type LimitOptimization struct {
}

// Name returns the name of the rule.
func (*LimitOptimization) Name() string {
	return "limit_optimization"
}

// Optimize implements base.LogicalOptRule interface.
func (*LimitOptimization) Optimize(_ context.Context, lp base.LogicalPlan, _ *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	planChanged := false

	// Apply the optimization recursively
	newPlan, changed, err := optimizeLimitRecursively(lp)
	if err != nil {
		return lp, false, err
	}
	planChanged = planChanged || changed

	return newPlan, planChanged, nil
}

// optimizeLimitRecursively applies LIMIT optimization recursively to the plan tree.
func optimizeLimitRecursively(p base.LogicalPlan) (base.LogicalPlan, bool, error) {
	planChanged := false

	// First, optimize children
	for i, child := range p.Children() {
		newChild, changed, err := optimizeLimitRecursively(child)
		if err != nil {
			return p, false, err
		}
		if changed {
			p.SetChild(i, newChild)
			planChanged = true
		}
	}

	// Then, optimize this node if it's a LIMIT
	if limit, ok := p.(*logicalop.LogicalLimit); ok {
		if canRemoveLimit1(limit) {
			// Remove the LIMIT 1 by returning its child
			return limit.Children()[0], true, nil
		}
	}

	return p, planChanged, nil
}

// canRemoveLimit1 checks if a LIMIT 1 can be safely removed.
func canRemoveLimit1(limit *logicalop.LogicalLimit) bool {
	// Only optimize LIMIT 1 (count = 1, offset = 0)
	if limit.Count != 1 || limit.Offset != 0 {
		return false
	}

	// Check if the limit has children
	children := limit.Children()
	if len(children) == 0 {
		return false
	}

	// Check if the child plan guarantees at most one row
	child := children[0]
	return hasMaxOneRowGuarantee(child)
}

// hasMaxOneRowGuarantee checks if a logical plan guarantees at most one row.
func hasMaxOneRowGuarantee(p base.LogicalPlan) bool {
	if p == nil {
		return false
	}

	// Use the existing HasMaxOneRow function with child information
	childMaxOneRow := make([]bool, len(p.Children()))
	for i, child := range p.Children() {
		childMaxOneRow[i] = hasMaxOneRowGuarantee(child)
	}

	// For joins, we need to check if both sides guarantee at most one row
	// or if the join conditions ensure uniqueness
	if join, ok := p.(*logicalop.LogicalJoin); ok {
		return hasJoinMaxOneRowGuarantee(join, childMaxOneRow)
	}

	// For other plan types, use the existing HasMaxOneRow logic
	return logicalop.HasMaxOneRow(p, childMaxOneRow)
}

// hasJoinMaxOneRowGuarantee checks if a join guarantees at most one row.
func hasJoinMaxOneRowGuarantee(join *logicalop.LogicalJoin, childMaxOneRow []bool) bool {
	// If both children guarantee at most one row, the join will too
	if len(childMaxOneRow) >= 2 && childMaxOneRow[0] && childMaxOneRow[1] {
		return true
	}

	// For inner joins, check if the join conditions ensure uniqueness
	if join.JoinType == base.InnerJoin && len(join.EqualConditions) > 0 {
		// Check if the join conditions involve unique keys from both sides
		leftSchema := join.Children()[0].Schema()
		rightSchema := join.Children()[1].Schema()

		// Extract columns involved in join conditions
		leftJoinCols := make(map[int64]bool)
		rightJoinCols := make(map[int64]bool)

		for _, eqCond := range join.EqualConditions {
			if len(eqCond.GetArgs()) >= 2 {
				if leftCol, ok := eqCond.GetArgs()[0].(*expression.Column); ok {
					leftJoinCols[leftCol.UniqueID] = true
				}
				if rightCol, ok := eqCond.GetArgs()[1].(*expression.Column); ok {
					rightJoinCols[rightCol.UniqueID] = true
				}
			}
		}

		// Check if left side join columns form a unique key
		leftHasUniqueKey := checkJoinColumnsFormUniqueKey(leftJoinCols, leftSchema)
		// Check if right side join columns form a unique key
		rightHasUniqueKey := checkJoinColumnsFormUniqueKey(rightJoinCols, rightSchema)

		// If either side has a unique key on the join columns, the result is unique
		return leftHasUniqueKey || rightHasUniqueKey
	}

	// For semi-joins, if the left side guarantees at most one row, so does the result
	if join.JoinType == base.SemiJoin || join.JoinType == base.AntiSemiJoin ||
		join.JoinType == base.LeftOuterSemiJoin || join.JoinType == base.AntiLeftOuterSemiJoin {
		return childMaxOneRow[0]
	}

	return false
}

// checkJoinColumnsFormUniqueKey checks if the join columns form a unique key.
func checkJoinColumnsFormUniqueKey(joinCols map[int64]bool, schema *expression.Schema) bool {
	if schema == nil {
		return false
	}

	// Check primary key
	for _, pk := range schema.PKOrUK {
		if isSubsetOfJoinColumns(pk, joinCols) {
			return true
		}
	}

	// Check unique keys
	for _, uk := range schema.NullableUK {
		if isSubsetOfJoinColumns(uk, joinCols) {
			return true
		}
	}

	return false
}

// isSubsetOfJoinColumns checks if a key is a subset of join columns.
func isSubsetOfJoinColumns(key expression.KeyInfo, joinCols map[int64]bool) bool {
	for _, col := range key {
		if !joinCols[col.UniqueID] {
			return false
		}
	}
	return true
}
