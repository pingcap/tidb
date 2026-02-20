// Copyright 2018 PingCAP, Inc.
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

package old

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/cascades/pattern"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/memo"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

// PushSelDownTableScan pushes the selection down to TableScan.
type PushSelDownTableScan struct {
	baseRule
}

// NewRulePushSelDownTableScan creates a new Transformation PushSelDownTableScan.
// The pattern of this rule is: `Selection -> TableScan`
func NewRulePushSelDownTableScan() Transformation {
	rule := &PushSelDownTableScan{}
	ts := pattern.NewPattern(pattern.OperandTableScan, pattern.EngineTiKVOrTiFlash)
	p := pattern.BuildPattern(pattern.OperandSelection, pattern.EngineTiKVOrTiFlash, ts)
	rule.pattern = p
	return rule
}

// OnTransform implements Transformation interface.
//
// It transforms `sel -> ts` to one of the following new exprs:
// 1. `newSel -> newTS`
// 2. `newTS`
//
// Filters of the old `sel` operator are removed if they are used to calculate
// the key ranges of the `ts` operator.
func (*PushSelDownTableScan) OnTransform(old *memo.ExprIter) (newExprs []*memo.GroupExpr, eraseOld bool, eraseAll bool, err error) {
	sel := old.GetExpr().ExprNode.(*logicalop.LogicalSelection)
	ts := old.Children[0].GetExpr().ExprNode.(*logicalop.LogicalTableScan)
	if ts.HandleCols == nil {
		return nil, false, false, nil
	}
	accesses, remained := ranger.DetachCondsForColumn(ts.SCtx().GetRangerCtx(), sel.Conditions, ts.HandleCols.GetCol(0))
	if accesses == nil {
		return nil, false, false, nil
	}
	newTblScan := logicalop.LogicalTableScan{
		Source:      ts.Source,
		HandleCols:  ts.HandleCols,
		AccessConds: ts.AccessConds.Shallow(),
	}.Init(ts.SCtx(), ts.QueryBlockOffset())
	newTblScan.AccessConds = append(newTblScan.AccessConds, accesses...)
	tblScanExpr := memo.NewGroupExpr(newTblScan)
	if len(remained) == 0 {
		// `sel -> ts` is transformed to `newTS`.
		return []*memo.GroupExpr{tblScanExpr}, true, false, nil
	}
	schema := old.GetExpr().Group.Prop.Schema
	tblScanGroup := memo.NewGroupWithSchema(tblScanExpr, schema)
	newSel := logicalop.LogicalSelection{Conditions: remained}.Init(sel.SCtx(), sel.QueryBlockOffset())
	selExpr := memo.NewGroupExpr(newSel)
	selExpr.Children = append(selExpr.Children, tblScanGroup)
	// `sel -> ts` is transformed to `newSel ->newTS`.
	return []*memo.GroupExpr{selExpr}, true, false, nil
}

// PushSelDownIndexScan pushes a Selection down to IndexScan.
type PushSelDownIndexScan struct {
	baseRule
}

// NewRulePushSelDownIndexScan creates a new Transformation PushSelDownIndexScan.
// The pattern of this rule is `Selection -> IndexScan`.
func NewRulePushSelDownIndexScan() Transformation {
	rule := &PushSelDownIndexScan{}
	rule.pattern = pattern.BuildPattern(
		pattern.OperandSelection,
		pattern.EngineTiKVOnly,
		pattern.NewPattern(pattern.OperandIndexScan, pattern.EngineTiKVOnly),
	)
	return rule
}

// OnTransform implements Transformation interface.
// It will transform `Selection -> IndexScan` to:
//
//	  `IndexScan(with a new access range)` or
//	  `Selection -> IndexScan(with a new access range)`
//		 or just keep the two GroupExprs unchanged.
func (*PushSelDownIndexScan) OnTransform(old *memo.ExprIter) (newExprs []*memo.GroupExpr, eraseOld bool, eraseAll bool, err error) {
	sel := old.GetExpr().ExprNode.(*logicalop.LogicalSelection)
	is := old.Children[0].GetExpr().ExprNode.(*logicalop.LogicalIndexScan)
	if len(is.IdxCols) == 0 {
		return nil, false, false, nil
	}
	conditions := sel.Conditions
	if is.AccessConds != nil {
		// If we have already pushed some conditions down here,
		// we merge old AccessConds with new conditions,
		// to make sure this rule can be applied more than once.
		conditions = make([]expression.Expression, len(sel.Conditions)+len(is.AccessConds))
		copy(conditions, sel.Conditions)
		copy(conditions[len(sel.Conditions):], is.AccessConds)
	}
	res, err := ranger.DetachCondAndBuildRangeForIndex(is.SCtx().GetRangerCtx(), conditions, is.IdxCols, is.IdxColLens, is.SCtx().GetSessionVars().RangeMaxSize)
	if err != nil {
		return nil, false, false, err
	}
	if len(res.AccessConds) == len(is.AccessConds) {
		// There is no condition can be pushed down as range,
		// or the pushed down conditions are the same with before.
		sameConds := true
		for i := range res.AccessConds {
			if !res.AccessConds[i].Equal(is.SCtx().GetExprCtx().GetEvalCtx(), is.AccessConds[i]) {
				sameConds = false
				break
			}
		}
		if sameConds {
			return nil, false, false, nil
		}
	}
	// TODO: `res` still has some unused fields: EqOrInCount, IsDNFCond.
	newIs := logicalop.LogicalIndexScan{
		Source:         is.Source,
		IsDoubleRead:   is.IsDoubleRead,
		EqCondCount:    res.EqCondCount,
		AccessConds:    res.AccessConds,
		Ranges:         res.Ranges,
		Index:          is.Index,
		Columns:        is.Columns,
		FullIdxCols:    is.FullIdxCols,
		FullIdxColLens: is.FullIdxColLens,
		IdxCols:        is.IdxCols,
		IdxColLens:     is.IdxColLens,
	}.Init(is.SCtx(), is.QueryBlockOffset())
	isExpr := memo.NewGroupExpr(newIs)

	if len(res.RemainedConds) == 0 {
		return []*memo.GroupExpr{isExpr}, true, false, nil
	}
	isGroup := memo.NewGroupWithSchema(isExpr, old.Children[0].GetExpr().Group.Prop.Schema)
	newSel := logicalop.LogicalSelection{Conditions: res.RemainedConds}.Init(sel.SCtx(), sel.QueryBlockOffset())
	selExpr := memo.NewGroupExpr(newSel)
	selExpr.SetChildren(isGroup)
	return []*memo.GroupExpr{selExpr}, true, false, nil
}

// PushSelDownTiKVSingleGather pushes the selection down to child of TiKVSingleGather.
type PushSelDownTiKVSingleGather struct {
	baseRule
}

// NewRulePushSelDownTiKVSingleGather creates a new Transformation PushSelDownTiKVSingleGather.
// The pattern of this rule is `Selection -> TiKVSingleGather -> Any`.
func NewRulePushSelDownTiKVSingleGather() Transformation {
	any1 := pattern.NewPattern(pattern.OperandAny, pattern.EngineTiKVOrTiFlash)
	tg := pattern.BuildPattern(pattern.OperandTiKVSingleGather, pattern.EngineTiDBOnly, any1)
	p := pattern.BuildPattern(pattern.OperandSelection, pattern.EngineTiDBOnly, tg)

	rule := &PushSelDownTiKVSingleGather{}
	rule.pattern = p
	return rule
}

// OnTransform implements Transformation interface.
//
// It transforms `oldSel -> oldTg -> any` to one of the following new exprs:
// 1. `newTg -> pushedSel -> any`
// 2. `remainedSel -> newTg -> pushedSel -> any`
func (*PushSelDownTiKVSingleGather) OnTransform(old *memo.ExprIter) (newExprs []*memo.GroupExpr, eraseOld bool, eraseAll bool, err error) {
	sel := old.GetExpr().ExprNode.(*logicalop.LogicalSelection)
	sg := old.Children[0].GetExpr().ExprNode.(*logicalop.TiKVSingleGather)
	childGroup := old.Children[0].Children[0].Group
	var pushed, remained []expression.Expression
	sctx := sg.SCtx()
	pushed, remained = expression.PushDownExprs(util.GetPushDownCtx(sctx), sel.Conditions, kv.TiKV)
	if len(pushed) == 0 {
		return nil, false, false, nil
	}
	pushedSel := logicalop.LogicalSelection{Conditions: pushed}.Init(sctx, sel.QueryBlockOffset())
	pushedSelExpr := memo.NewGroupExpr(pushedSel)
	pushedSelExpr.Children = append(pushedSelExpr.Children, childGroup)
	pushedSelGroup := memo.NewGroupWithSchema(pushedSelExpr, childGroup.Prop.Schema).SetEngineType(childGroup.EngineType)
	// The field content of TiKVSingleGather would not be modified currently, so we
	// just reference the same tg instead of making a copy of it.
	//
	// TODO: if we save pushed filters later in TiKVSingleGather, in order to do partition
	//       pruning or skyline pruning, we need to make a copy of the TiKVSingleGather here.
	tblGatherExpr := memo.NewGroupExpr(sg)
	tblGatherExpr.Children = append(tblGatherExpr.Children, pushedSelGroup)
	if len(remained) == 0 {
		// `oldSel -> oldTg -> any` is transformed to `newTg -> pushedSel -> any`.
		return []*memo.GroupExpr{tblGatherExpr}, true, false, nil
	}
	tblGatherGroup := memo.NewGroupWithSchema(tblGatherExpr, pushedSelGroup.Prop.Schema)
	remainedSel := logicalop.LogicalSelection{Conditions: remained}.Init(sel.SCtx(), sel.QueryBlockOffset())
	remainedSelExpr := memo.NewGroupExpr(remainedSel)
	remainedSelExpr.Children = append(remainedSelExpr.Children, tblGatherGroup)
	// `oldSel -> oldTg -> any` is transformed to `remainedSel -> newTg -> pushedSel -> any`.
	return []*memo.GroupExpr{remainedSelExpr}, true, false, nil
}

// EnumeratePaths converts DataSource to table scan and index scans.
type EnumeratePaths struct {
	baseRule
}

// NewRuleEnumeratePaths creates a new Transformation EnumeratePaths.
// The pattern of this rule is: `DataSource`.
func NewRuleEnumeratePaths() Transformation {
	rule := &EnumeratePaths{}
	rule.pattern = pattern.NewPattern(pattern.OperandDataSource, pattern.EngineTiDBOnly)
	return rule
}

// OnTransform implements Transformation interface.
func (*EnumeratePaths) OnTransform(old *memo.ExprIter) (newExprs []*memo.GroupExpr, eraseOld bool, eraseAll bool, err error) {
	ds := old.GetExpr().ExprNode.(*logicalop.DataSource)
	gathers := ds.Convert2Gathers()
	for _, gather := range gathers {
		expr := memo.Convert2GroupExpr(gather)
		expr.Children[0].SetEngineType(pattern.EngineTiKV)
		newExprs = append(newExprs, expr)
	}
	return newExprs, true, false, nil
}

// PushAggDownGather splits Aggregation to two stages, final and partial1,
// and pushed the partial Aggregation down to the child of TiKVSingleGather.
type PushAggDownGather struct {
	baseRule
}

// NewRulePushAggDownGather creates a new Transformation PushAggDownGather.
// The pattern of this rule is: `Aggregation -> TiKVSingleGather`.
func NewRulePushAggDownGather() Transformation {
	rule := &PushAggDownGather{}
	rule.pattern = pattern.BuildPattern(
		pattern.OperandAggregation,
		pattern.EngineTiDBOnly,
		pattern.NewPattern(pattern.OperandTiKVSingleGather, pattern.EngineTiDBOnly),
	)
	return rule
}

// Match implements Transformation interface.
func (r *PushAggDownGather) Match(expr *memo.ExprIter) bool {
	if expr.GetExpr().HasAppliedRule(r) {
		return false
	}
	agg := expr.GetExpr().ExprNode.(*logicalop.LogicalAggregation)
	for _, aggFunc := range agg.AggFuncs {
		if aggFunc.Mode != aggregation.CompleteMode {
			return false
		}
	}
	if agg.HasDistinct() {
		// TODO: remove this logic after the cost estimation of distinct pushdown is implemented.
		// If AllowDistinctAggPushDown is set to true, we should not consider RootTask.
		if !agg.SCtx().GetSessionVars().AllowDistinctAggPushDown {
			return false
		}
	}
	childEngine := expr.Children[0].GetExpr().Children[0].EngineType
	if childEngine != pattern.EngineTiKV {
		// TODO: Remove this check when we have implemented TiFlashAggregation.
		return false
	}
	return physicalop.CheckAggCanPushCop(agg.SCtx(), agg.AggFuncs, agg.GroupByItems, kv.TiKV)
}

// OnTransform implements Transformation interface.
// It will transform `Agg->Gather` to `Agg(Final) -> Gather -> Agg(Partial1)`.
func (r *PushAggDownGather) OnTransform(old *memo.ExprIter) (newExprs []*memo.GroupExpr, eraseOld bool, eraseAll bool, err error) {
	agg := old.GetExpr().ExprNode.(*logicalop.LogicalAggregation)
	aggSchema := old.GetExpr().Group.Prop.Schema
	gather := old.Children[0].GetExpr().ExprNode.(*logicalop.TiKVSingleGather)
	childGroup := old.Children[0].GetExpr().Children[0]
	// The old Aggregation should stay unchanged for other transformation.
	// So we build a new LogicalAggregation for the partialAgg.
	aggFuncs := make([]*aggregation.AggFuncDesc, len(agg.AggFuncs))
	for i := range agg.AggFuncs {
		aggFuncs[i] = agg.AggFuncs[i].Clone()
	}
	gbyItems := make([]expression.Expression, len(agg.GroupByItems))
	copy(gbyItems, agg.GroupByItems)

	partialPref, finalPref, firstRowFuncMap := physicalop.BuildFinalModeAggregation(agg.SCtx(),
		&physicalop.AggInfo{
			AggFuncs:     aggFuncs,
			GroupByItems: gbyItems,
			Schema:       aggSchema,
		}, true, false)
	if partialPref == nil {
		return nil, false, false, nil
	}
	// Remove unnecessary FirstRow.
	partialPref.AggFuncs =
		physicalop.RemoveUnnecessaryFirstRow(agg.SCtx(), finalPref.GroupByItems, partialPref.AggFuncs, partialPref.GroupByItems, partialPref.Schema, firstRowFuncMap)

	partialAgg := logicalop.LogicalAggregation{
		AggFuncs:     partialPref.AggFuncs,
		GroupByItems: partialPref.GroupByItems,
	}.Init(agg.SCtx(), agg.QueryBlockOffset())
	partialAgg.CopyAggHints(agg)

	finalAgg := logicalop.LogicalAggregation{
		AggFuncs:     finalPref.AggFuncs,
		GroupByItems: finalPref.GroupByItems,
	}.Init(agg.SCtx(), agg.QueryBlockOffset())
	finalAgg.CopyAggHints(agg)

	partialAggExpr := memo.NewGroupExpr(partialAgg)
	partialAggExpr.SetChildren(childGroup)
	partialAggGroup := memo.NewGroupWithSchema(partialAggExpr, partialPref.Schema).SetEngineType(childGroup.EngineType)
	gatherExpr := memo.NewGroupExpr(gather)
	gatherExpr.SetChildren(partialAggGroup)
	gatherGroup := memo.NewGroupWithSchema(gatherExpr, partialPref.Schema)
	finalAggExpr := memo.NewGroupExpr(finalAgg)
	finalAggExpr.SetChildren(gatherGroup)
	finalAggExpr.AddAppliedRule(r)
	// We don't erase the old complete mode Aggregation because
	// this transformation would not always be better.
	return []*memo.GroupExpr{finalAggExpr}, false, false, nil
}

