// Copyright 2024 PingCAP, Inc.
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

package predicate

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/cascades/memo"
	"github.com/pingcap/tidb/pkg/planner/cascades/rule"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/pattern"
)

type PushPredicateThroughProjection struct {
	*rule.BaseRule
}

// NewPushPredicateThroughProjection creates a new PushPredicateThroughProjection rule.
func NewPushPredicateThroughProjection() *PushPredicateThroughProjection {
	pa := pattern.BuildPattern(
		pattern.OperandSelection,
		pattern.EngineTiDBOnly,
		pattern.NewPattern(pattern.OperandProjection, pattern.EngineTiDBOnly),
	)
	return &PushPredicateThroughProjection{
		BaseRule: rule.NewBaseRule(rule.XF_PUSH_DOWN_PREDICATE_THROUGH_PROJECTION, pa),
	}
}

// XForm implements rule's transformation action interface.
// It will transform `selection -> projection -> x` to
// 1. `projection -> selection -> x` or
// 2. `selection -> projection -> selection -> x` or
// 3. just keep unchanged.
func (*PushPredicateThroughProjection) XForm(holder *memo.MemoExpression, mm *memo.Memo) ([]*memo.MemoExpression, error) {
	// the match function can guarantee the tree pattern is that we used as below.
	sel := holder.GetLogicalPlan().(*logicalop.LogicalSelection)
	projMExpr := holder.GetInputs()[0]
	proj := projMExpr.GetLogicalPlan().(*logicalop.LogicalProjection)

	for _, expr := range proj.Exprs {
		if expression.HasAssignSetVarFunc(expr) {
			return nil, nil
		}
	}
	// Same logic copied from normalized predicate_push_down rule.
	canBePushed, canNotBePushed := logicalop.BreakDownPredicates(proj, sel.Conditions)
	if len(canBePushed) == 0 {
		return nil, nil
	}
	newBottomSel := logicalop.LogicalSelection{Conditions: canBePushed}.Init(sel.SCtx(), sel.QueryBlockOffset())
	newBottomSelMExpr := memo.NewMemoExpressionFromPlanAndInputs(newBottomSel, projMExpr.GetInputs())

	newProjMExpr := memo.NewMemoExpressionFromPlanAndInputs(proj, []*memo.MemoExpression{newBottomSelMExpr})
	if len(canNotBePushed) == 0 {
		// proj is equivalent to original sel, we can add it to old selGroup, and remove old sel.
		// todo: remove old sel.
		return []*memo.MemoExpression{newProjMExpr}, nil
	}
	// if there are some conditions can not be pushed down, we need to add a new selection.
	// for this incomplete proj, we need to add it to a new group, which means target is nil.
	newTopSel := logicalop.LogicalSelection{Conditions: canNotBePushed}.Init(sel.SCtx(), sel.QueryBlockOffset())
	newTopSelMExpr := memo.NewMemoExpressionFromPlanAndInputs(newTopSel, []*memo.MemoExpression{newProjMExpr})
	return []*memo.MemoExpression{newTopSelMExpr}, nil
}

func (*PushPredicateThroughProjection) XForm1(holder *rule.GroupExprHolder, mm *memo.Memo) ([]*memo.GroupExpression, error) {
	// the match function can guarantee the tree pattern is that we used as below.
	sel := holder.Cur.LogicalPlan().(*logicalop.LogicalSelection)
	selGE := holder.Cur
	proj := holder.Subs[0].Cur.LogicalPlan().(*logicalop.LogicalProjection)
	projGE := holder.Subs[0].Cur

	for _, expr := range proj.Exprs {
		if expression.HasAssignSetVarFunc(expr) {
			return nil, nil
		}
	}
	// Same logic copied from normalized predicate_push_down rule.
	canBePushed, canNotBePushed := logicalop.BreakDownPredicates(proj, sel.Conditions)
	if len(canBePushed) == 0 {
		return nil, nil
	}
	newBottomSel := logicalop.LogicalSelection{Conditions: canBePushed}.Init(sel.SCtx(), sel.QueryBlockOffset())

	hasher := mm.GetHasher()
	selGroupExpr := memo.NewGroupExpression(newBottomSel, projGE.Inputs)
	selGroupExpr.Init(hasher)

	// once insert fail, it means the same group expression with same input's group id already exists. Abort this XForm.
	// here target group is nil, because we don't know what one is the logical equivalent class of the partial newBottomSel.
	var target *memo.Group
	if len(canNotBePushed) == 0 {
		target = selGE.GetGroup()
	}
	// the logical equivalent group, we don't care about inside,
	if !mm.InsertGroupExpression(selGroupExpr, target) {
		return nil, nil
	}

	// get the sel's new/old group.
	selG := selGroupExpr.GetGroup()
	hasher = mm.GetHasher()
	projGroupExpr := memo.NewGroupExpression(proj, []*memo.Group{selG})
	projGroupExpr.Init(hasher)

	if len(canNotBePushed) == 0 {
		// proj is equivalent to original sel, we can add it to old selGroup, and remove old sel.
		if !mm.InsertGroupExpression(projGroupExpr, selGE.GetGroup()) {
			return nil, nil
		}
		//// todo: remove old sel.
		return []*memo.GroupExpression{projGroupExpr}, nil
	}
	// if there are some conditions can not be pushed down, we need to add a new selection.
	// for this incomplete proj, we need to add it to a new group, which means target is nil.
	if !mm.InsertGroupExpression(projGroupExpr, nil) {
		return nil, nil
	}
	// get the proj's  new group.
	projNewGroup := projGroupExpr.GetGroup()
	newTopSel := logicalop.LogicalSelection{Conditions: canNotBePushed}.Init(sel.SCtx(), sel.QueryBlockOffset())
	hasher = mm.GetHasher()
	newSelGroupExpr := memo.NewGroupExpression(newTopSel, []*memo.Group{projNewGroup})
	newSelGroupExpr.Init(hasher)
	return []*memo.GroupExpression{newSelGroupExpr}, nil
}
