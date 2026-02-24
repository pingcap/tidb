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

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// CorrelateSolver tries to convert semi-join LogicalJoin back to correlated LogicalApply.
// This is the reverse of DecorrelateSolver and is useful when a correlated nested-loop
// (index lookup per outer row) might be more efficient than a hash semi-join.
type CorrelateSolver struct{}

// Optimize implements base.LogicalOptRule.<0th> interface.
func (s *CorrelateSolver) Optimize(ctx context.Context, p base.LogicalPlan) (retPlan base.LogicalPlan, retChanged bool, retErr error) {
	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Warn("CorrelateSolver panic, returning original plan",
				zap.Any("recover", r),
				zap.Stack("stack"))
			retPlan = p
			retChanged = false
			retErr = nil
		}
	}()
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

	// Check if this node is a LogicalJoin with a semi-join type that was
	// marked for re-correlation (from a non-correlated IN subquery).
	join, isJoin := p.(*logicalop.LogicalJoin)
	if !isJoin || !join.JoinType.IsSemiJoin() || !join.PreferCorrelate {
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

	// Left outer semi joins (scalar IN / NOT IN) require 3-valued NULL
	// semantics: the joiner must distinguish "no match" (→ 0) from "unknown
	// due to NULL" (→ NULL). It does this by evaluating the equality join
	// condition and tracking whether any comparison returned NULL.
	//
	// When we push the equality into the inner side as a correlated filter
	// (rightCol = CorCol(leftCol)), two problems arise:
	//  1. If the inner column is nullable, NULL inner values are silently
	//     filtered out (NULL = X → NULL → filtered), so the joiner never
	//     sees them and returns 0 instead of NULL.
	//  2. If the outer column is nullable and its value is NULL, the
	//     correlated filter becomes rightCol = NULL, which filters out all
	//     inner rows, and the joiner returns 0 instead of NULL.
	//
	// Skip unless ALL equality columns on both sides are proven NOT NULL.
	if join.JoinType == base.LeftOuterSemiJoin || join.JoinType == base.AntiLeftOuterSemiJoin {
		for _, eqCond := range join.EqualConditions {
			col0, col1, ok := expression.IsColOpCol(eqCond)
			if !ok {
				return p, planChanged, nil
			}
			leftCol := leftSchema.RetrieveColumn(col0)
			rightCol := rightSchema.RetrieveColumn(col1)
			if leftCol == nil || rightCol == nil {
				leftCol = leftSchema.RetrieveColumn(col1)
				rightCol = rightSchema.RetrieveColumn(col0)
			}
			if leftCol == nil || rightCol == nil {
				return p, planChanged, nil
			}
			if !mysql.HasNotNullFlag(leftCol.RetType.GetFlag()) || !mysql.HasNotNullFlag(rightCol.RetType.GetFlag()) {
				return p, planChanged, nil
			}
		}
	}

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

	// Clone the inner subtree so PPD can modify the clone without affecting
	// the Join's inner child (which must retain its original conditions).
	// If the subtree contains an unhandled operator type, abort to avoid corruption.
	clonedInner, ok := cloneLogicalSubtree(join.Children()[1])
	if !ok {
		return p, planChanged, nil
	}

	// Lift DataSource conditions back into Selection nodes. The original PPD
	// pushed conditions all the way into DataSource.AllConds and cleared them
	// from ancestor operators (e.g., Join.RightConditions). When we re-run PPD
	// below, the Join re-collects conditions from its own fields (not from
	// DataSource.AllConds), so conditions that were pushed past the Join would
	// be lost. Wrapping each DataSource in a Selection restores the pre-PPD
	// state so the re-run can properly redistribute all conditions.
	clonedInner = liftDataSourceConds(clonedInner)

	sel := logicalop.LogicalSelection{Conditions: selConds}.Init(join.SCtx(), join.QueryBlockOffset())
	sel.SetChildren(clonedInner)

	// Run predicate push-down on the inner subtree so the new correlated
	// predicates reach the DataSource (for index access path selection).
	// PPD has already finished by the time this rule runs, so without this
	// local pass the predicates would stay in the Selection and the inner
	// side could only do full scans.
	_, innerPlan, err := sel.PredicatePushDown(nil)
	if err != nil {
		// PPD failed (e.g., conditions reference columns pruned from the
		// DataSource schema); abort the correlate optimization.
		return p, planChanged, nil
	}

	// Reset stats on DataSources that received correlated conditions so DeriveStats
	// re-runs during physical optimization. This is necessary because the original
	// DeriveStats ran before the correlate rule added correlated conditions, so the
	// index access paths were built without them.
	resetStatsForCorrelatedDS(innerPlan)

	// For semi-join semantics (EXISTS/IN and NOT EXISTS/NOT IN), add Limit 1 on
	// the inner side. The Apply executor materializes all inner rows per outer
	// key via fetchAllInners; a Limit 1 enables early exit since semi/anti-semi
	// joins only need to know whether any matching row exists.
	// This mirrors what expression_rewriter does for NO_DECORRELATE EXISTS.
	if !hasLimit(innerPlan) {
		limit := logicalop.LogicalLimit{Count: 1}.Init(join.SCtx(), join.QueryBlockOffset())
		limit.SetChildren(innerPlan)
		innerPlan = limit
	}

	// Build the LogicalApply.
	ap := logicalop.LogicalApply{}.Init(join.SCtx(), join.QueryBlockOffset())
	ap.JoinType = join.JoinType
	ap.CorCols = corCols
	// Copy hint fields so hint behavior is preserved in the alternative.
	ap.HintInfo = join.HintInfo
	ap.PreferJoinType = join.PreferJoinType
	ap.PreferJoinOrder = join.PreferJoinOrder
	ap.LeftPreferJoinType = join.LeftPreferJoinType
	ap.RightPreferJoinType = join.RightPreferJoinType
	ap.SetChildren(join.Children()[0], innerPlan)
	ap.SetSchema(join.Schema().Clone())
	ap.SetOutputNames(join.OutputNames())

	// Store the Apply alternative on the Join for cost-based selection during
	// physical optimization, rather than unconditionally choosing Apply.
	join.CorrelateAlternative = ap
	return p, true, nil
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

// cloneLogicalSubtree creates a shallow clone of the logical plan subtree,
// ensuring each node has a fresh plan ID and independent mutable state (children,
// conditions, AllConds). Immutable data (table info, column info, etc.) is shared.
// This is used to build the Apply alternative's inner plan without modifying the
// Join's original inner subtree when PPD pushes correlated conditions down.
// Returns (clone, true) on success, or (nil, false) if an unhandled operator type
// is encountered. In the failure case, the caller must abort the correlate
// optimization to avoid corrupting the original subtree.
func cloneLogicalSubtree(p base.LogicalPlan) (base.LogicalPlan, bool) {
	switch op := p.(type) {
	case *logicalop.DataSource:
		return cloneDataSource(op), true
	case *logicalop.LogicalJoin:
		return cloneJoin(op)
	case *logicalop.LogicalSelection:
		return cloneSelection(op)
	case *logicalop.LogicalProjection:
		return cloneProjection(op)
	case *logicalop.LogicalAggregation:
		return cloneAggregation(op)
	case *logicalop.LogicalLimit:
		return cloneLimit(op)
	default:
		// Unknown operator type — cannot safely clone. Return failure
		// so the caller aborts the correlate optimization.
		return nil, false
	}
}

func cloneWithChildren(p base.LogicalPlan) ([]base.LogicalPlan, bool) {
	children := make([]base.LogicalPlan, len(p.Children()))
	for i, child := range p.Children() {
		cloned, ok := cloneLogicalSubtree(child)
		if !ok {
			return nil, false
		}
		children[i] = cloned
	}
	return children, true
}

func cloneDataSource(ds *logicalop.DataSource) *logicalop.DataSource {
	clone := *ds
	clone.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(
		ds.SCtx(), ds.TP(), &clone, ds.QueryBlockOffset())
	clone.SetSchema(ds.Schema().Clone())
	// Independent slices that PPD replaces.
	clone.AllConds = append([]expression.Expression(nil), ds.AllConds...)
	clone.PushedDownConds = append([]expression.Expression(nil), ds.PushedDownConds...)
	// Deep-clone AccessPaths so the Join and Apply alternatives have fully
	// independent path objects. Stats derivation (fillIndexPath, etc.) mutates
	// AccessPath fields in place; without deep cloning, costing one alternative
	// can corrupt the other and destabilize CBO.
	clone.AllPossibleAccessPaths = make([]*util.AccessPath, len(ds.AllPossibleAccessPaths))
	for i, ap := range ds.AllPossibleAccessPaths {
		clone.AllPossibleAccessPaths[i] = ap.Clone()
	}
	clone.PossibleAccessPaths = make([]*util.AccessPath, len(ds.PossibleAccessPaths))
	for i, ap := range ds.PossibleAccessPaths {
		clone.PossibleAccessPaths[i] = ap.Clone()
	}
	// Preserve original stats so DeriveStats returns early for DataSources
	// that don't receive correlated conditions. Without this, DeriveStats
	// re-runs fillIndexPath on all DataSources, which fails when conditions
	// reference columns that column pruning removed from the schema.
	if origStats := ds.StatsInfo(); origStats != nil {
		clone.SetStats(origStats)
	}
	return &clone
}

func cloneJoin(j *logicalop.LogicalJoin) (*logicalop.LogicalJoin, bool) {
	children, ok := cloneWithChildren(j)
	if !ok {
		return nil, false
	}
	clone := *j
	clone.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(
		j.SCtx(), j.TP(), &clone, j.QueryBlockOffset())
	clone.SetSchema(j.Schema().Clone())
	// Independent condition slices that PPD may modify.
	clone.EqualConditions = append([]*expression.ScalarFunction(nil), j.EqualConditions...)
	clone.LeftConditions = append(expression.CNFExprs(nil), j.LeftConditions...)
	clone.RightConditions = append(expression.CNFExprs(nil), j.RightConditions...)
	clone.OtherConditions = append(expression.CNFExprs(nil), j.OtherConditions...)
	// Clear correlate state. The alternative was built for the original join's
	// children; retaining it would point physical planning at uncloned nodes.
	clone.CorrelateAlternative = nil
	clone.PreferCorrelate = false
	clone.SetChildren(children...)
	return &clone, true
}

func cloneSelection(s *logicalop.LogicalSelection) (*logicalop.LogicalSelection, bool) {
	children, ok := cloneWithChildren(s)
	if !ok {
		return nil, false
	}
	clone := *s
	clone.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(
		s.SCtx(), s.TP(), &clone, s.QueryBlockOffset())
	clone.Conditions = append(expression.CNFExprs(nil), s.Conditions...)
	clone.SetChildren(children...)
	return &clone, true
}

func cloneProjection(proj *logicalop.LogicalProjection) (*logicalop.LogicalProjection, bool) {
	children, ok := cloneWithChildren(proj)
	if !ok {
		return nil, false
	}
	clone := *proj
	clone.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(
		proj.SCtx(), proj.TP(), &clone, proj.QueryBlockOffset())
	clone.SetSchema(proj.Schema().Clone())
	clone.SetChildren(children...)
	return &clone, true
}

func cloneAggregation(agg *logicalop.LogicalAggregation) (*logicalop.LogicalAggregation, bool) {
	children, ok := cloneWithChildren(agg)
	if !ok {
		return nil, false
	}
	clone := *agg
	clone.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(
		agg.SCtx(), agg.TP(), &clone, agg.QueryBlockOffset())
	clone.SetSchema(agg.Schema().Clone())
	clone.SetChildren(children...)
	return &clone, true
}

func cloneLimit(lim *logicalop.LogicalLimit) (*logicalop.LogicalLimit, bool) {
	children, ok := cloneWithChildren(lim)
	if !ok {
		return nil, false
	}
	clone := *lim
	clone.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(
		lim.SCtx(), lim.TP(), &clone, lim.QueryBlockOffset())
	clone.SetSchema(lim.Schema().Clone())
	clone.SetChildren(children...)
	return &clone, true
}

// freshAccessPath creates a new AccessPath with only the structural identity
// fields from the source path (Index, StoreType, handle flags, hint flags).
// Analysis fields (Ranges, AccessConds, IdxCols, etc.) are left at zero so
// that fillIndexPath / deriveTablePathStats start from a clean state.
func freshAccessPath(src *util.AccessPath) *util.AccessPath {
	return &util.AccessPath{
		Index:              src.Index,
		StoreType:          src.StoreType,
		IsIntHandlePath:    src.IsIntHandlePath,
		IsCommonHandlePath: src.IsCommonHandlePath,
		Forced:             src.Forced,
		ForceKeepOrder:     src.ForceKeepOrder,
		ForceNoKeepOrder:   src.ForceNoKeepOrder,
		ForcePartialOrder:  src.ForcePartialOrder,
		IsUkShardIndexPath: src.IsUkShardIndexPath,
	}
}

// liftDataSourceConds walks the plan tree and for each DataSource with
// non-empty AllConds, wraps it in a Selection node containing those conditions.
// This "un-pushes" conditions that the original PPD pushed into DataSources,
// so that a subsequent PPD re-run (in correlate()) can properly redistribute
// all conditions — including those that would otherwise be silently dropped
// when DataSource.PredicatePushDown overwrites AllConds.
func liftDataSourceConds(p base.LogicalPlan) base.LogicalPlan {
	// Recurse into children first, potentially replacing them.
	for i, child := range p.Children() {
		newChild := liftDataSourceConds(child)
		if newChild != child {
			p.Children()[i] = newChild
		}
	}

	// If this is a DataSource with AllConds, wrap it in a Selection.
	if ds, ok := p.(*logicalop.DataSource); ok && len(ds.AllConds) > 0 {
		sel := logicalop.LogicalSelection{
			Conditions: ds.AllConds,
		}.Init(ds.SCtx(), ds.QueryBlockOffset())
		sel.SetChildren(ds)

		// Clear DataSource conditions; the PPD re-run will push them back.
		ds.AllConds = nil
		ds.PushedDownConds = nil

		return sel
	}

	return p
}

// resetStatsForCorrelatedDS walks the inner subtree and clears StatsInfo on
// DataSources that have correlated conditions in AllConds, plus all ancestor
// plan nodes up to the root. This forces DeriveStats to re-run during physical
// optimization so that index access paths are rebuilt with the correlated
// conditions.
//
// For correlated DataSources, fresh AccessPaths are created so fillIndexPath
// starts from a clean state with the new correlated conditions. Non-correlated
// DataSources retain their deep-cloned AccessPaths and stats (set during
// cloning) so DeriveStats returns early — this avoids failures when conditions
// reference columns that column pruning removed from the DataSource's schema.
func resetStatsForCorrelatedDS(p base.LogicalPlan) bool {
	hasCorrelated := false

	// Check if this is a DataSource with correlated conditions.
	if ds, ok := p.(*logicalop.DataSource); ok {
		for _, cond := range ds.AllConds {
			if len(expression.ExtractCorColumns(cond)) > 0 {
				hasCorrelated = true
				break
			}
		}
		if hasCorrelated {
			// Create fresh AccessPaths so fillIndexPath rebuilds them with the
			// correlated conditions from a clean state.
			origPaths := ds.AllPossibleAccessPaths
			ds.AllPossibleAccessPaths = make([]*util.AccessPath, len(origPaths))
			for i, ap := range origPaths {
				ds.AllPossibleAccessPaths[i] = freshAccessPath(ap)
			}
			ds.PossibleAccessPaths = append([]*util.AccessPath(nil), ds.AllPossibleAccessPaths...)
		}
	}

	// Recurse into children.
	for _, child := range p.Children() {
		if resetStatsForCorrelatedDS(child) {
			hasCorrelated = true
		}
	}

	// Reset stats on this node if it or any descendant has correlated conditions.
	// This ensures DeriveStats re-runs for the affected subtree path.
	if hasCorrelated {
		if blp, ok := p.GetBaseLogicalPlan().(*logicalop.BaseLogicalPlan); ok {
			blp.SetStats(nil)
		}
	}

	return hasCorrelated
}

// Name implements base.LogicalOptRule.<1st> interface.
func (*CorrelateSolver) Name() string {
	return "correlate"
}
