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
	"fmt"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
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
			logutil.BgLogger().Warn("CorrelateSolver panic",
				zap.Any("recover", r),
				zap.Stack("stack"))
			retPlan = nil
			retChanged = false
			retErr = fmt.Errorf("CorrelateSolver panic: %v", r)
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

	join, isJoin := p.(*logicalop.LogicalJoin)
	if !isJoin {
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

	switch {
	case join.JoinType.IsSemiJoin():
		// Semi-joins are correlated only when the builder marked them, because the
		// plan shape alone cannot tell that this join came from a non-correlated IN
		// subquery rather than from a join the user wrote.
		if !join.PreferCorrelate {
			return p, planChanged, nil
		}
		// Fall through to the semi-join construction below.
	case join.JoinType == base.InnerJoin || join.JoinType == base.LeftOuterJoin:
		// The inner side aggregating on the join key is itself the trigger, so this
		// path matches on shape and needs no builder mark — which also keeps it
		// working after join reorder rebuilds the join node.
		if newPlan, ok := s.correlateMinMaxAgg(join); ok {
			return newPlan, true, nil
		}
		return p, planChanged, nil
	default:
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

	// Replace the Join with the Apply. In the alternative logical plans framework,
	// this round produces a complete plan; the top-level cost comparison across
	// rounds selects the winner.
	return ap, true, nil
}

// correlateMinMaxAgg converts a join against an aggregated inner side into a
// LogicalApply that computes the aggregate for a single group per outer row.
//
// The transformation is value-preserving when the equal-join conditions cover
// every grouping key: each outer row then matches at most one group, so pushing
// `groupCol = CorCol(outerCol)` beneath the aggregation selects exactly the group
// the join would have matched. Once the group is pinned, MIN/MAX is the first row
// in the ordering of its argument, so the aggregation is replaced outright by a
// TopN(1) plus a projection that reproduces the aggregation's output columns.
//
// Emitting TopN(1) here rather than leaving a single-group aggregation for
// MaxMinEliminator is deliberate: that rule sits at FlagMaxMinEliminate in
// optRuleFlags and has long finished by the time FlagCorrelate runs.
//
// Replacing the aggregation (instead of keeping it over the correlated filter)
// also keeps outer-join semantics intact without any per-aggregate reasoning: an
// empty group yields zero rows, so LeftOuterJoin NULL-extends exactly as the
// grouped aggregation did. A retained aggregation with no GroupByItems would
// instead emit one row and turn `NULL` into `0` for COUNT.
//
// Returns (nil, false) when the shape does not match, in which case the caller
// leaves the join untouched.
func (*CorrelateSolver) correlateMinMaxAgg(join *logicalop.LogicalJoin) (base.LogicalPlan, bool) {
	// RightConditions filter aggregated output (HAVING-style) and have no meaning
	// below the aggregation, so they cannot be carried into the correlated form.
	if len(join.RightConditions) > 0 {
		return nil, false
	}
	// v1 handles the aggregation on the right only. That covers LeftOuterJoin by
	// construction; an InnerJoin whose aggregated side ends up on the left after
	// join reorder is simply not matched.
	agg, isAgg := join.Children()[1].(*logicalop.LogicalAggregation)
	if !isAgg || len(agg.GroupByItems) == 0 {
		return nil, false
	}

	groupCols := make([]*expression.Column, 0, len(agg.GroupByItems))
	for _, item := range agg.GroupByItems {
		col, isCol := item.(*expression.Column)
		if !isCol {
			return nil, false
		}
		groupCols = append(groupCols, col)
	}

	minMaxCol, desc, matched := matchSingleMinMaxAgg(agg, groupCols)
	if !matched {
		return nil, false
	}

	leftSchema := join.Children()[0].Schema()
	aggSchema := agg.Schema()
	selConds := make([]expression.Expression, 0, len(join.EqualConditions))
	corCols := make([]*expression.CorrelatedColumn, 0, len(join.EqualConditions))
	pinned := make(map[int64]struct{}, len(groupCols))

	for _, eqCond := range join.EqualConditions {
		col0, col1, isColOpCol := expression.IsColOpCol(eqCond)
		if !isColOpCol {
			return nil, false
		}
		outerCol := leftSchema.RetrieveColumn(col0)
		innerCol := aggSchema.RetrieveColumn(col1)
		if outerCol == nil || innerCol == nil {
			outerCol = leftSchema.RetrieveColumn(col1)
			innerCol = aggSchema.RetrieveColumn(col0)
		}
		if outerCol == nil || innerCol == nil {
			return nil, false
		}
		// The join references the aggregation's output column; the correlated
		// filter has to be expressed on the underlying grouping column instead.
		groupCol, ok := groupColBehindAggOutput(agg, innerCol)
		if !ok {
			return nil, false
		}
		corCol := &expression.CorrelatedColumn{Column: *outerCol, Data: new(types.Datum)}
		cond := expression.NewFunctionInternal(
			join.SCtx().GetExprCtx(),
			eqCond.FuncName.L,
			types.NewFieldType(mysql.TypeTiny),
			groupCol, corCol,
		)
		selConds = append(selConds, cond)
		corCols = append(corCols, corCol)
		pinned[groupCol.UniqueID] = struct{}{}
	}

	// Every grouping key must be pinned by the join. A free grouping key would let
	// one outer row match several groups, and TopN(1) would then collapse them into
	// a single arbitrary row instead of producing one row per group.
	for _, groupCol := range groupCols {
		if _, ok := pinned[groupCol.UniqueID]; !ok {
			return nil, false
		}
	}

	// Clone the aggregation's input so the local PPD below cannot disturb the
	// original subtree, which the competing join alternative still uses.
	clonedInput, ok := cloneLogicalSubtree(agg.Children()[0])
	if !ok {
		return nil, false
	}
	clonedInput = liftDataSourceConds(clonedInput)

	sel := logicalop.LogicalSelection{Conditions: selConds}.Init(join.SCtx(), join.QueryBlockOffset())
	sel.SetChildren(clonedInput)

	// Push the correlated equalities down so they reach the DataSource and can be
	// picked up as access conditions; without this the inner side can only scan.
	_, innerPlan, err := sel.PredicatePushDown(nil)
	if err != nil {
		return nil, false
	}
	resetStatsForCorrelatedDS(innerPlan)

	topN := logicalop.LogicalTopN{
		ByItems: []*util.ByItems{{Expr: minMaxCol, Desc: desc}},
		Count:   1,
	}.Init(join.SCtx(), join.QueryBlockOffset())
	topN.SetChildren(innerPlan)

	// One row now survives per outer row, and it is the group's MIN/MAX row. Every
	// aggregate's argument evaluated on that row equals what the aggregate would
	// have produced: the MIN/MAX argument by construction, and each firstrow
	// argument because matchSingleMinMaxAgg proved it constant within the group.
	projExprs := make([]expression.Expression, 0, len(agg.AggFuncs))
	for _, aggFunc := range agg.AggFuncs {
		projExprs = append(projExprs, aggFunc.Args[0].Clone())
	}
	proj := logicalop.LogicalProjection{Exprs: projExprs}.Init(join.SCtx(), join.QueryBlockOffset())
	proj.SetSchema(aggSchema.Clone())
	proj.SetChildren(topN)

	ap := logicalop.LogicalApply{}.Init(join.SCtx(), join.QueryBlockOffset())
	ap.JoinType = join.JoinType
	ap.CorCols = corCols
	// Copy hint fields so hint behavior is preserved in the alternative.
	ap.HintInfo = join.HintInfo
	ap.PreferJoinType = join.PreferJoinType
	ap.PreferJoinOrder = join.PreferJoinOrder
	ap.LeftPreferJoinType = join.LeftPreferJoinType
	ap.RightPreferJoinType = join.RightPreferJoinType
	ap.SetChildren(join.Children()[0], proj)
	ap.SetSchema(join.Schema().Clone())
	ap.SetOutputNames(join.OutputNames())
	return ap, true
}

// matchSingleMinMaxAgg reports whether agg computes exactly one MIN or MAX over a
// NOT NULL column, with every remaining aggregate a firstrow over a grouping key.
//
// The NOT NULL requirement is what lets the caller replace the aggregate with an
// ordered TopN(1): MIN/MAX skip NULLs, whereas an ascending TopN would return a
// NULL row first. MaxMinEliminator handles nullable columns by adding an IS NOT
// NULL filter; this rule does not yet do that.
//
// Restricting the remaining aggregates to firstrow over grouping keys makes their
// value constant within the group, so reading them off the MIN/MAX row is
// equivalent to whatever row the aggregation would have picked.
func matchSingleMinMaxAgg(agg *logicalop.LogicalAggregation, groupCols []*expression.Column) (
	minMaxCol *expression.Column, desc bool, matched bool) {
	for _, aggFunc := range agg.AggFuncs {
		if aggFunc.HasDistinct || len(aggFunc.Args) != 1 {
			return nil, false, false
		}
		switch aggFunc.Name {
		case ast.AggFuncFirstRow:
			col, isCol := aggFunc.Args[0].(*expression.Column)
			if !isCol || !containsColumn(groupCols, col) {
				return nil, false, false
			}
		case ast.AggFuncMin, ast.AggFuncMax:
			if matched {
				// More than one MIN/MAX needs a separate probe per aggregate, which
				// this single-TopN rewrite cannot express.
				return nil, false, false
			}
			col, isCol := aggFunc.Args[0].(*expression.Column)
			if !isCol || !mysql.HasNotNullFlag(col.RetType.GetFlag()) {
				return nil, false, false
			}
			minMaxCol, desc, matched = col, aggFunc.Name == ast.AggFuncMax, true
		default:
			return nil, false, false
		}
	}
	return minMaxCol, desc, matched
}

// groupColBehindAggOutput maps an aggregation output column back to the grouping
// column it exposes, which is the column the correlated filter must be built on.
func groupColBehindAggOutput(agg *logicalop.LogicalAggregation, outCol *expression.Column) (*expression.Column, bool) {
	pos := agg.Schema().ColumnIndex(outCol)
	if pos < 0 || pos >= len(agg.AggFuncs) {
		return nil, false
	}
	aggFunc := agg.AggFuncs[pos]
	if aggFunc.Name != ast.AggFuncFirstRow || len(aggFunc.Args) != 1 {
		return nil, false
	}
	col, isCol := aggFunc.Args[0].(*expression.Column)
	return col, isCol
}

func containsColumn(cols []*expression.Column, target *expression.Column) bool {
	for _, col := range cols {
		if col.UniqueID == target.UniqueID {
			return true
		}
	}
	return false
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
