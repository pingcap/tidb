// Copyright 2019 PingCAP, Inc.
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
	"github.com/pingcap/tidb/pkg/planner/cascades/pattern"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/memo"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/coreusage"
)

type InjectProjectionBelowTopN struct {
	baseRule
}

// NewRuleInjectProjectionBelowTopN creates a new Transformation InjectProjectionBelowTopN.
// It will extract the ScalarFunctions of `ByItems` into a Projection and injects it below TopN.
// When a Projection is injected as the child of TopN, we need to add another Projection upon
// TopN to prune the extra Columns.
// The reason why we need this rule is that, TopNExecutor in TiDB does not support ScalarFunction
// as `ByItem`. So we have to use a Projection to calculate the ScalarFunctions in advance.
// The pattern of this rule is: a single TopN
func NewRuleInjectProjectionBelowTopN() Transformation {
	rule := &InjectProjectionBelowTopN{}
	rule.pattern = pattern.BuildPattern(
		pattern.OperandTopN,
		pattern.EngineTiDBOnly,
	)
	return rule
}

// Match implements Transformation interface.
func (*InjectProjectionBelowTopN) Match(expr *memo.ExprIter) bool {
	topN := expr.GetExpr().ExprNode.(*logicalop.LogicalTopN)
	for _, item := range topN.ByItems {
		if _, ok := item.Expr.(*expression.ScalarFunction); ok {
			return true
		}
	}
	return false
}

// OnTransform implements Transformation interface.
// It will convert `TopN -> X` to `Projection -> TopN -> Projection -> X`.
func (*InjectProjectionBelowTopN) OnTransform(old *memo.ExprIter) (newExprs []*memo.GroupExpr, eraseOld bool, eraseAll bool, err error) {
	topN := old.GetExpr().ExprNode.(*logicalop.LogicalTopN)
	ectx := topN.SCtx().GetExprCtx().GetEvalCtx()
	oldTopNSchema := old.GetExpr().Schema()

	// Construct top Projection.
	topProjExprs := make([]expression.Expression, oldTopNSchema.Len())
	for i := range oldTopNSchema.Columns {
		topProjExprs[i] = oldTopNSchema.Columns[i]
	}
	topProj := logicalop.LogicalProjection{
		Exprs: topProjExprs,
	}.Init(topN.SCtx(), topN.QueryBlockOffset())
	topProj.SetSchema(oldTopNSchema)

	// Construct bottom Projection.
	bottomProjExprs := make([]expression.Expression, 0, oldTopNSchema.Len()+len(topN.ByItems))
	bottomProjSchema := make([]*expression.Column, 0, oldTopNSchema.Len()+len(topN.ByItems))
	for _, col := range oldTopNSchema.Columns {
		bottomProjExprs = append(bottomProjExprs, col)
		bottomProjSchema = append(bottomProjSchema, col)
	}
	newByItems := make([]*util.ByItems, 0, len(topN.ByItems))
	for _, item := range topN.ByItems {
		itemExpr := item.Expr
		if _, isScalarFunc := itemExpr.(*expression.ScalarFunction); !isScalarFunc {
			newByItems = append(newByItems, item)
			continue
		}
		bottomProjExprs = append(bottomProjExprs, itemExpr)
		newCol := &expression.Column{
			UniqueID: topN.SCtx().GetSessionVars().AllocPlanColumnID(),
			RetType:  itemExpr.GetType(ectx),
		}
		bottomProjSchema = append(bottomProjSchema, newCol)
		newByItems = append(newByItems, &util.ByItems{Expr: newCol, Desc: item.Desc})
	}
	bottomProj := logicalop.LogicalProjection{
		Exprs: bottomProjExprs,
	}.Init(topN.SCtx(), topN.QueryBlockOffset())
	newSchema := expression.NewSchema(bottomProjSchema...)
	bottomProj.SetSchema(newSchema)

	newTopN := logicalop.LogicalTopN{
		ByItems: newByItems,
		Offset:  topN.Offset,
		Count:   topN.Count,
	}.Init(topN.SCtx(), topN.QueryBlockOffset())

	// Construct GroupExpr, Group (TopProj -> TopN -> BottomProj -> Child)
	bottomProjGroupExpr := memo.NewGroupExpr(bottomProj)
	bottomProjGroupExpr.SetChildren(old.GetExpr().Children[0])
	bottomProjGroup := memo.NewGroupWithSchema(bottomProjGroupExpr, newSchema)

	topNGroupExpr := memo.NewGroupExpr(newTopN)
	topNGroupExpr.SetChildren(bottomProjGroup)
	topNGroup := memo.NewGroupWithSchema(topNGroupExpr, newSchema)

	topProjGroupExpr := memo.NewGroupExpr(topProj)
	topProjGroupExpr.SetChildren(topNGroup)
	return []*memo.GroupExpr{topProjGroupExpr}, true, false, nil
}

// InjectProjectionBelowAgg injects Projection below Agg if Agg's AggFuncDesc.Args or
// Agg's GroupByItem contain ScalarFunctions.
type InjectProjectionBelowAgg struct {
	baseRule
}

// NewRuleInjectProjectionBelowAgg creates a new Transformation NewRuleInjectProjectionBelowAgg.
// It will extract the ScalarFunctions of `AggFuncDesc` and `GroupByItems` into a Projection and injects it below Agg.
// The reason why we need this rule is that, AggExecutor in TiDB does not support ScalarFunction
// as `AggFuncDesc.Arg` and `GroupByItem`. So we have to use a Projection to calculate the ScalarFunctions in advance.
// The pattern of this rule is: a single Aggregation.
func NewRuleInjectProjectionBelowAgg() Transformation {
	rule := &InjectProjectionBelowAgg{}
	rule.pattern = pattern.BuildPattern(
		pattern.OperandAggregation,
		pattern.EngineTiDBOnly,
	)
	return rule
}

// Match implements Transformation interface.
func (*InjectProjectionBelowAgg) Match(expr *memo.ExprIter) bool {
	agg := expr.GetExpr().ExprNode.(*logicalop.LogicalAggregation)
	return agg.IsCompleteModeAgg()
}

// OnTransform implements Transformation interface.
// It will convert `Agg -> X` to `Agg -> Proj -> X`.
func (*InjectProjectionBelowAgg) OnTransform(old *memo.ExprIter) (newExprs []*memo.GroupExpr, eraseOld bool, eraseAll bool, err error) {
	agg := old.GetExpr().ExprNode.(*logicalop.LogicalAggregation)
	ectx := agg.SCtx().GetExprCtx().GetEvalCtx()

	hasScalarFunc := false
	copyFuncs := make([]*aggregation.AggFuncDesc, 0, len(agg.AggFuncs))
	for _, aggFunc := range agg.AggFuncs {
		copyFunc := aggFunc.Clone()
		// WrapCastForAggArgs will modify AggFunc, so we should clone AggFunc.
		copyFunc.WrapCastForAggArgs(agg.SCtx().GetExprCtx())
		copyFuncs = append(copyFuncs, copyFunc)
		for _, arg := range copyFunc.Args {
			_, isScalarFunc := arg.(*expression.ScalarFunction)
			hasScalarFunc = hasScalarFunc || isScalarFunc
		}
	}

	for i := 0; !hasScalarFunc && i < len(agg.GroupByItems); i++ {
		_, isScalarFunc := agg.GroupByItems[i].(*expression.ScalarFunction)
		hasScalarFunc = hasScalarFunc || isScalarFunc
	}
	if !hasScalarFunc {
		return nil, false, false, nil
	}

	projSchemaCols := make([]*expression.Column, 0, len(copyFuncs)+len(agg.GroupByItems))
	projExprs := make([]expression.Expression, 0, cap(projSchemaCols))

	for _, f := range copyFuncs {
		for i, arg := range f.Args {
			switch expr := arg.(type) {
			case *expression.Constant:
				continue
			case *expression.Column:
				projExprs = append(projExprs, expr)
				projSchemaCols = append(projSchemaCols, expr)
			default:
				projExprs = append(projExprs, expr)
				newArg := &expression.Column{
					UniqueID: agg.SCtx().GetSessionVars().AllocPlanColumnID(),
					RetType:  arg.GetType(ectx),
				}
				projSchemaCols = append(projSchemaCols, newArg)
				f.Args[i] = newArg
			}
		}
	}

	newGroupByItems := make([]expression.Expression, len(agg.GroupByItems))
	for i, item := range agg.GroupByItems {
		switch expr := item.(type) {
		case *expression.Constant:
			newGroupByItems[i] = expr
		case *expression.Column:
			newGroupByItems[i] = expr
			projExprs = append(projExprs, expr)
			projSchemaCols = append(projSchemaCols, expr)
		default:
			projExprs = append(projExprs, expr)
			newArg := &expression.Column{
				UniqueID: agg.SCtx().GetSessionVars().AllocPlanColumnID(),
				RetType:  item.GetType(ectx),
			}
			projSchemaCols = append(projSchemaCols, newArg)
			newGroupByItems[i] = newArg
		}
	}

	// Construct GroupExpr, Group (Agg -> Proj -> Child).
	proj := logicalop.LogicalProjection{
		Exprs: projExprs,
	}.Init(agg.SCtx(), agg.QueryBlockOffset())
	projSchema := expression.NewSchema(projSchemaCols...)
	proj.SetSchema(projSchema)
	projExpr := memo.NewGroupExpr(proj)
	projExpr.SetChildren(old.GetExpr().Children[0])
	projGroup := memo.NewGroupWithSchema(projExpr, projSchema)

	newAgg := logicalop.LogicalAggregation{
		AggFuncs:     copyFuncs,
		GroupByItems: newGroupByItems,
	}.Init(agg.SCtx(), agg.QueryBlockOffset())
	newAgg.CopyAggHints(agg)
	newAggExpr := memo.NewGroupExpr(newAgg)
	newAggExpr.SetChildren(projGroup)

	return []*memo.GroupExpr{newAggExpr}, true, false, nil
}

// TransformApplyToJoin transforms a LogicalApply to LogicalJoin if it's
// inner children has no correlated columns from it's outer schema.
type TransformApplyToJoin struct {
	baseRule
}

// NewRuleTransformApplyToJoin creates a new Transformation TransformApplyToJoin.
// The pattern of this rule is: `Apply -> (X, Y)`.
func NewRuleTransformApplyToJoin() Transformation {
	rule := &TransformApplyToJoin{}
	rule.pattern = pattern.NewPattern(pattern.OperandApply, pattern.EngineTiDBOnly)
	return rule
}

// OnTransform implements Transformation interface.
func (r *TransformApplyToJoin) OnTransform(old *memo.ExprIter) (newExprs []*memo.GroupExpr, eraseOld bool, eraseAll bool, err error) {
	apply := old.GetExpr().ExprNode.(*logicalop.LogicalApply)
	groupExpr := old.GetExpr()
	// It's safe to use the old apply instead of creating a new LogicalApply here,
	// Because apply.CorCols will only be used and updated by this rule during Transformation.
	apply.CorCols = r.extractCorColumnsBySchema(groupExpr.Children[1], groupExpr.Children[0].Prop.Schema)
	if len(apply.CorCols) != 0 {
		return nil, false, false, nil
	}

	join := apply.LogicalJoin.Shallow()
	joinGroupExpr := memo.NewGroupExpr(join)
	joinGroupExpr.SetChildren(groupExpr.Children...)
	return []*memo.GroupExpr{joinGroupExpr}, true, false, nil
}

func (r *TransformApplyToJoin) extractCorColumnsBySchema(innerGroup *memo.Group, outerSchema *expression.Schema) []*expression.CorrelatedColumn {
	corCols := r.extractCorColumnsFromGroup(innerGroup)
	return coreusage.ExtractCorColumnsBySchema(corCols, outerSchema, false)
}

func (r *TransformApplyToJoin) extractCorColumnsFromGroup(g *memo.Group) []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0)
	for elem := g.Equivalents.Front(); elem != nil; elem = elem.Next() {
		expr := elem.Value.(*memo.GroupExpr)
		corCols = append(corCols, expr.ExprNode.ExtractCorrelatedCols()...)
		for _, child := range expr.Children {
			corCols = append(corCols, r.extractCorColumnsFromGroup(child)...)
		}
	}
	// We may have duplicate CorrelatedColumns here, but it won't influence
	// the logic of the transformation. Apply.CorCols will be deduplicated in
	// `ResolveIndices`.
	return corCols
}

// PullSelectionUpApply pulls up the inner-side Selection into Apply as
// its join condition.
type PullSelectionUpApply struct {
	baseRule
}

// NewRulePullSelectionUpApply creates a new Transformation PullSelectionUpApply.
// The pattern of this rule is: `Apply -> (Any<outer>, Selection<inner>)`.
func NewRulePullSelectionUpApply() Transformation {
	rule := &PullSelectionUpApply{}
	rule.pattern = pattern.BuildPattern(
		pattern.OperandApply,
		pattern.EngineTiDBOnly,
		pattern.NewPattern(pattern.OperandAny, pattern.EngineTiDBOnly),       // outer child
		pattern.NewPattern(pattern.OperandSelection, pattern.EngineTiDBOnly), // inner child
	)
	return rule
}

// OnTransform implements Transformation interface.
// This rule tries to pull up the inner side Selection, and add these conditions
// to Join condition inside the Apply.
func (*PullSelectionUpApply) OnTransform(old *memo.ExprIter) (newExprs []*memo.GroupExpr, eraseOld bool, eraseAll bool, err error) {
	apply := old.GetExpr().ExprNode.(*logicalop.LogicalApply)
	outerChildGroup := old.Children[0].Group
	innerChildGroup := old.Children[1].Group
	sel := old.Children[1].GetExpr().ExprNode.(*logicalop.LogicalSelection)
	newConds := make([]expression.Expression, 0, len(sel.Conditions))
	for _, cond := range sel.Conditions {
		newConds = append(newConds, cond.Clone().Decorrelate(outerChildGroup.Prop.Schema))
	}
	newApply := logicalop.LogicalApply{
		LogicalJoin: *(apply.LogicalJoin.Shallow()),
		CorCols:     apply.CorCols,
	}.Init(apply.SCtx(), apply.QueryBlockOffset())
	// Update Join conditions.
	eq, left, right, other := newApply.LogicalJoin.ExtractOnCondition(newConds, outerChildGroup.Prop.Schema, innerChildGroup.Prop.Schema, false, false)
	newApply.LogicalJoin.AppendJoinConds(eq, left, right, other)

	newApplyGroupExpr := memo.NewGroupExpr(newApply)
	newApplyGroupExpr.SetChildren(outerChildGroup, old.Children[1].GetExpr().Children[0])
	return []*memo.GroupExpr{newApplyGroupExpr}, false, false, nil
}

// MergeAdjacentWindow merge adjacent Window.
type MergeAdjacentWindow struct {
	baseRule
}

// NewRuleMergeAdjacentWindow creates a new Transformation MergeAdjacentWindow.
// The pattern of this rule is `Window -> Window`.
func NewRuleMergeAdjacentWindow() Transformation {
	rule := &MergeAdjacentWindow{}
	rule.pattern = pattern.BuildPattern(
		pattern.OperandWindow,
		pattern.EngineAll,
		pattern.NewPattern(pattern.OperandWindow, pattern.EngineAll),
	)
	return rule
}

// Match implements Transformation interface.
func (*MergeAdjacentWindow) Match(expr *memo.ExprIter) bool {
	curWinPlan := expr.GetExpr().ExprNode.(*logicalop.LogicalWindow)
	nextGroupExpr := expr.Children[0].GetExpr()
	nextWinPlan := nextGroupExpr.ExprNode.(*logicalop.LogicalWindow)
	nextGroupChildren := nextGroupExpr.Children
	ctx := expr.GetExpr().ExprNode.SCtx()

	// Whether Partition, OrderBy and Frame parts are the same.
	if !(curWinPlan.EqualPartitionBy(nextWinPlan) &&
		curWinPlan.EqualOrderBy(ctx.GetExprCtx().GetEvalCtx(), nextWinPlan) &&
		curWinPlan.EqualFrame(ctx.GetExprCtx().GetEvalCtx(), nextWinPlan)) {
		return false
	}

	// Whether the first window uses the unsettled columns in the next window.

	// `select a, b, sum(bb) over (partition by a) as 'sum_bb' from (select a, b, max(b) over (partition by a) as 'bb' from t) as tt`
	// The adjacent windows in the above sql statement cannot be merged.
	// The reason is that the first one uses an unsettled column `bb` from the second one.
	nextWindowChildrenExistedCols := make(map[int64]struct{})
	for _, ngc := range nextGroupChildren {
		for _, c := range ngc.Prop.Schema.Columns {
			nextWindowChildrenExistedCols[c.UniqueID] = struct{}{}
		}
	}
	for _, funDesc := range curWinPlan.WindowFuncDescs {
		for _, arg := range funDesc.Args {
			cols := expression.ExtractColumns(arg)
			for _, c := range cols {
				if _, ok := nextWindowChildrenExistedCols[c.UniqueID]; !ok {
					return false
				}
			}
		}
	}
	return true
}

// OnTransform implements Transformation interface.
// This rule will transform `window -> window -> x` to `window -> x`
func (*MergeAdjacentWindow) OnTransform(old *memo.ExprIter) (newExprs []*memo.GroupExpr, eraseOld bool, eraseAll bool, err error) {
	curWinPlan := old.GetExpr().ExprNode.(*logicalop.LogicalWindow)
	nextWinPlan := old.Children[0].GetExpr().ExprNode.(*logicalop.LogicalWindow)
	ctx := old.GetExpr().ExprNode.SCtx()

	newWindowFuncs := make([]*aggregation.WindowFuncDesc, 0, len(curWinPlan.WindowFuncDescs)+len(nextWinPlan.WindowFuncDescs))
	newWindowFuncs = append(newWindowFuncs, curWinPlan.WindowFuncDescs...)
	newWindowFuncs = append(newWindowFuncs, nextWinPlan.WindowFuncDescs...)
	newWindowPlan := logicalop.LogicalWindow{
		WindowFuncDescs: newWindowFuncs,
		PartitionBy:     curWinPlan.PartitionBy,
		OrderBy:         curWinPlan.OrderBy,
		Frame:           curWinPlan.Frame,
	}.Init(ctx, curWinPlan.QueryBlockOffset())
	newWindowGroupExpr := memo.NewGroupExpr(newWindowPlan)
	newWindowGroupExpr.SetChildren(old.Children[0].GetExpr().Children...)
	return []*memo.GroupExpr{newWindowGroupExpr}, true, false, nil
}
