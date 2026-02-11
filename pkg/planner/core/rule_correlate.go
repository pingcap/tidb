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
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/types"
)

// CorrelateSolver tries to convert semi-join LogicalJoin back to correlated LogicalApply.
// This is the reverse of DecorrelateSolver and is useful when a correlated nested-loop
// (index lookup per outer row) might be more efficient than a hash semi-join.
type CorrelateSolver struct{}

// Optimize implements base.LogicalOptRule.<0th> interface.
func (s *CorrelateSolver) Optimize(ctx context.Context, p base.LogicalPlan) (base.LogicalPlan, bool, error) {
	return s.correlate(ctx, p)
}

func (s *CorrelateSolver) correlate(ctx context.Context, p base.LogicalPlan) (base.LogicalPlan, bool, error) {
	// CTE's logical optimization is independent.
	if _, ok := p.(*logicalop.LogicalCTE); ok {
		return p, false, nil
	}

	// First recurse into children.
	planChanged := false
	newChildren := make([]base.LogicalPlan, 0, len(p.Children()))
	for _, child := range p.Children() {
		np, changed, err := s.correlate(ctx, child)
		if err != nil {
			return nil, false, err
		}
		planChanged = planChanged || changed
		newChildren = append(newChildren, np)
	}
	p.SetChildren(newChildren...)

	// Check if this node is a LogicalApply — if so, skip (already correlated).
	if _, isApply := p.(*logicalop.LogicalApply); isApply {
		return p, planChanged, nil
	}

	// Check if this node is a LogicalJoin with a semi-join type.
	join, isJoin := p.(*logicalop.LogicalJoin)
	if !isJoin || !join.JoinType.IsSemiJoin() {
		return p, planChanged, nil
	}

	// Must have EqualConditions to correlate (skip if only NAEQConditions).
	if len(join.EqualConditions) == 0 {
		return p, planChanged, nil
	}

	// For v1: skip null-aware conditions, LeftConditions, and OtherConditions.
	if len(join.NAEQConditions) > 0 || len(join.LeftConditions) > 0 || len(join.OtherConditions) > 0 {
		return p, planChanged, nil
	}

	leftSchema := join.Children()[0].Schema()
	rightSchema := join.Children()[1].Schema()

	selConds := make([]expression.Expression, 0, len(join.EqualConditions)+len(join.RightConditions))
	corCols := make([]*expression.CorrelatedColumn, 0, len(join.EqualConditions))

	// Convert EqualConditions to correlated conditions.
	for _, eqCond := range join.EqualConditions {
		cond, corCol := s.buildCorrelatedCond(eqCond, leftSchema, rightSchema, join)
		if cond == nil {
			// Can't correlate this condition; abort.
			return p, planChanged, nil
		}
		selConds = append(selConds, cond)
		corCols = append(corCols, corCol)
	}

	// Move RightConditions to the selection (they reference only the inner side).
	selConds = append(selConds, join.RightConditions...)

	// Build the LogicalSelection on the inner (right) child.
	innerChild := join.Children()[1]
	sel := logicalop.LogicalSelection{Conditions: selConds}.Init(join.SCtx(), join.QueryBlockOffset())
	sel.SetChildren(innerChild)

	// Run predicate push-down on the inner subtree so the new correlated
	// predicates reach the DataSource (for index access path selection).
	// PPD has already finished by the time this rule runs, so without this
	// local pass the predicates would stay in the Selection and the inner
	// side could only do full scans.
	_, innerPlan, err := sel.PredicatePushDown(nil)
	if err != nil {
		return nil, false, err
	}

	// Build the LogicalApply.
	ap := logicalop.LogicalApply{}.Init(join.SCtx(), join.QueryBlockOffset())
	ap.JoinType = join.JoinType
	ap.CorCols = corCols
	ap.SetChildren(join.Children()[0], innerPlan)
	ap.SetSchema(join.Schema().Clone())
	ap.SetOutputNames(join.OutputNames())

	return ap, true, nil
}

// buildCorrelatedCond converts an equal condition from the join into a correlated condition
// for the inner selection. It identifies which column comes from the left (outer) side and
// creates a CorrelatedColumn for it, then builds a new condition: rightCol <op> CorCol(leftCol).
func (*CorrelateSolver) buildCorrelatedCond(
	eqCond *expression.ScalarFunction,
	leftSchema *expression.Schema,
	rightSchema *expression.Schema,
	join *logicalop.LogicalJoin,
) (expression.Expression, *expression.CorrelatedColumn) {
	col0, col1, ok := expression.IsColOpCol(eqCond)
	if !ok {
		return nil, nil
	}

	// Determine which column is from the left (outer) side and which from the right (inner).
	leftCol := leftSchema.RetrieveColumn(col0)
	rightCol := rightSchema.RetrieveColumn(col1)
	if leftCol == nil || rightCol == nil {
		// Try swapped order.
		leftCol = leftSchema.RetrieveColumn(col1)
		rightCol = rightSchema.RetrieveColumn(col0)
	}
	if leftCol == nil || rightCol == nil {
		return nil, nil
	}

	// Create a CorrelatedColumn for the outer (left) column.
	// Data must be initialized (non-nil) to avoid panics during physical planning.
	corCol := &expression.CorrelatedColumn{Column: *leftCol, Data: new(types.Datum)}

	// Create the correlated condition: rightCol <op> CorCol(leftCol).
	cond := expression.NewFunctionInternal(
		join.SCtx().GetExprCtx(),
		eqCond.FuncName.L,
		types.NewFieldType(mysql.TypeTiny),
		rightCol, corCol,
	)

	return cond, corCol
}

// Name implements base.LogicalOptRule.<1st> interface.
func (*CorrelateSolver) Name() string {
	return "correlate"
}
