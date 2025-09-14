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
	"github.com/pingcap/tidb/pkg/planner/core/rule/util"
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
			// Remove the limit by returning its child
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

// hasSchemaMaxOneRowGuarantee checks if the schema guarantees at most one row.
func hasSchemaMaxOneRowGuarantee(p base.LogicalPlan) bool {
	schema := p.Schema()
	if schema == nil {
		return false
	}

	// Check if there are any primary or unique keys
	// If there are any unique constraints, the plan could potentially return at most one row
	// depending on the selection conditions
	if len(schema.PKOrUK) > 0 || len(schema.NullableUK) > 0 {
		// For DataSource, if there are selection conditions that use unique keys,
		// it guarantees at most one row
		if ds, ok := p.(*logicalop.DataSource); ok {
			return hasDataSourceUniqueSelection(ds)
		}

		// For other plan types with unique constraints, we need to be more conservative
		// Only return true if we can be certain about uniqueness
		return false
	}

	return false
}

// hasDataSourceUniqueSelection checks if a DataSource with selection conditions guarantees at most one row.
func hasDataSourceUniqueSelection(ds *logicalop.DataSource) bool {
	if len(ds.AllConds) == 0 {
		return false // No conditions means no uniqueness guarantee
	}

	// Extract columns used in selection conditions (equal conditions only)
	eqColIDs := make(map[int64]struct{})
	for _, cond := range ds.AllConds {
		// Only check equality conditions for uniqueness
		if sf, ok := cond.(*expression.ScalarFunction); ok && sf.FuncName.L == "eq" {
			cols := expression.ExtractColumns(cond)
			for _, col := range cols {
				eqColIDs[col.UniqueID] = struct{}{}
			}
		}
	}

	// Use the existing CheckMaxOneRowCond function
	return util.CheckMaxOneRowCond(eqColIDs, ds.Schema())
}

// hasMaxOneRowGuarantee checks if a logical plan guarantees at most one row.
func hasMaxOneRowGuarantee(p base.LogicalPlan) bool {
	if p == nil {
		return false
	}

	// First check if the plan structure itself guarantees at most one row
	childMaxOneRow := make([]bool, len(p.Children()))
	for i, child := range p.Children() {
		childMaxOneRow[i] = hasMaxOneRowGuarantee(child)
	}

	// For joins, we need to check if both sides guarantee at most one row
	// or if the join conditions ensure uniqueness
	if join, ok := p.(*logicalop.LogicalJoin); ok {
		return hasJoinMaxOneRowGuarantee(join, childMaxOneRow)
	}

	// For Apply (correlated subqueries), we need to check if the inner child
	// with correlation conditions guarantees at most one row
	if apply, ok := p.(*logicalop.LogicalApply); ok {
		return hasApplyMaxOneRowGuarantee(apply, childMaxOneRow)
	}

	// Check if the plan structure guarantees at most one row
	if logicalop.HasMaxOneRow(p, childMaxOneRow) {
		return true
	}

	// Check if the schema has unique constraints that guarantee at most one row
	return hasSchemaMaxOneRowGuarantee(p)
}

// hasJoinMaxOneRowGuarantee checks if a join guarantees at most one row.
func hasJoinMaxOneRowGuarantee(join *logicalop.LogicalJoin, childMaxOneRow []bool) bool {
	// If both children guarantee at most one row, the join will too
	if len(childMaxOneRow) >= 2 && childMaxOneRow[0] && childMaxOneRow[1] {
		return true
	}

	// For inner joins, check if the join conditions ensure uniqueness
	if join.JoinType == base.InnerJoin && len(join.EqualConditions) > 0 {
		// Extract join keys for both sides
		leftJoinKeys := join.ExtractJoinKeys(0)
		rightJoinKeys := join.ExtractJoinKeys(1)

		// Check if left side join keys contain a unique key
		leftHasUniqueKey, _ := isJoinKeysContainUniqueKey(join.Children()[0], leftJoinKeys)
		// Check if right side join keys contain a unique key
		rightHasUniqueKey, _ := isJoinKeysContainUniqueKey(join.Children()[1], rightJoinKeys)

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

// isJoinKeysContainUniqueKey checks whether one of unique keys sets is contained by join keys.
// This is based on the existing isInnerJoinKeysContainUniqueKey function.
func isJoinKeysContainUniqueKey(plan base.LogicalPlan, joinKeys *expression.Schema) (bool, error) {
	if joinKeys == nil || len(joinKeys.Columns) == 0 {
		return false, nil
	}

	for _, keyInfo := range plan.Schema().PKOrUK {
		joinKeysContainKeyInfo := true
		for _, col := range keyInfo {
			if !joinKeys.Contains(col) {
				joinKeysContainKeyInfo = false
				break
			}
		}
		if joinKeysContainKeyInfo {
			return true, nil
		}
	}
	return false, nil
}

// hasApplyMaxOneRowGuarantee checks if an Apply (correlated subquery) guarantees at most one row.
func hasApplyMaxOneRowGuarantee(apply *logicalop.LogicalApply, childMaxOneRow []bool) bool {
	// For Apply, we need to check if the inner child guarantees at most one row
	// when considering the correlation conditions
	if len(childMaxOneRow) < 2 {
		return false
	}

	// If the inner child already guarantees at most one row, the Apply will too
	if childMaxOneRow[1] {
		return true
	}

	// For Apply with correlation conditions, check if the correlation conditions
	// combined with the inner child's conditions guarantee at most one row
	if len(apply.EqualConditions) > 0 {
		// Extract correlation columns from the inner child
		innerPlan := apply.Children()[1]
		corCols := make(map[int64]bool)

		// Get all correlated columns used in this Apply
		for _, corCol := range apply.CorCols {
			corCols[corCol.UniqueID] = true
		}

		// Check if the correlation conditions involve unique keys from the inner child
		if ds, ok := innerPlan.(*logicalop.DataSource); ok {
			return hasDataSourceCorrelationUniqueSelection(ds, corCols)
		}

		// For other inner plan types, check if join conditions involve unique keys
		innerJoinKeys := apply.ExtractJoinKeys(1)
		if innerJoinKeys != nil && len(innerJoinKeys.Columns) > 0 {
			hasUniqueKey, _ := isJoinKeysContainUniqueKey(innerPlan, innerJoinKeys)
			return hasUniqueKey
		}
	}

	// Fall back to the existing HasMaxOneRow logic for Apply
	return childMaxOneRow[1]
}

// hasDataSourceCorrelationUniqueSelection checks if a DataSource with correlation conditions guarantees at most one row.
func hasDataSourceCorrelationUniqueSelection(ds *logicalop.DataSource, corCols map[int64]bool) bool {
	if len(ds.AllConds) == 0 {
		return false // No conditions means no uniqueness guarantee
	}

	// Extract columns used in selection conditions (equal conditions only)
	eqColIDs := make(map[int64]struct{})
	for _, cond := range ds.AllConds {
		// Only check equality conditions for uniqueness
		if sf, ok := cond.(*expression.ScalarFunction); ok && sf.FuncName.L == "eq" {
			cols := expression.ExtractColumns(cond)
			for _, col := range cols {
				eqColIDs[col.UniqueID] = struct{}{}
			}
		}
	}

	// Add correlation columns to the equality conditions
	for corColID := range corCols {
		eqColIDs[corColID] = struct{}{}
	}

	// Use the existing CheckMaxOneRowCond function
	return util.CheckMaxOneRowCond(eqColIDs, ds.Schema())
}
