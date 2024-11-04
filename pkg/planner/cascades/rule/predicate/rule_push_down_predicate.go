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
func (*PushPredicateThroughProjection) XForm(holder *memo.MemoExpression) ([]*memo.MemoExpression, error) {
	// the match function can guarantee the tree pattern is that we used as below.
	sel := holder.GE.LogicalPlan().(*logicalop.LogicalSelection)
	projMExpr := holder.Inputs[0]
	proj := projMExpr.GE.LogicalPlan().(*logicalop.LogicalProjection)

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
	newBottomSelMExpr := memo.NewMemoExpressionFromPlanAndInputs(newBottomSel, projMExpr.Inputs)

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
