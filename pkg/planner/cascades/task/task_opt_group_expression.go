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

package task

import (
	"io"

	"github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/planner/cascades/memo"
	"github.com/pingcap/tidb/pkg/planner/cascades/rule"
	"github.com/pingcap/tidb/pkg/planner/pattern"
)

var _ base.Task = &OptGroupTask{}

type OptGroupExpressionTask struct {
	BaseTask

	groupExpression *memo.GroupExpression
	// currently for each opt expression, it should be explore-type.
}

// NewOptGroupExpressionTask return a targeting optimizing group expression task.
func NewOptGroupExpressionTask(mctx *memo.MemoContext, ge *memo.GroupExpression) *OptGroupExpressionTask {
	return &OptGroupExpressionTask{
		BaseTask:        BaseTask{mctx: mctx},
		groupExpression: ge,
	}
}

// Execute implements the task.Execute interface.
func (ge *OptGroupExpressionTask) Execute() error {
	ruleList := ge.getValidRules()
	for _, one := range ruleList {
		ge.Push(NewApplyRuleTask(ge.mctx, ge.groupExpression, one))
	}
	// since it's a stack-order, LUFO, when we want to apply a rule for a specific group expression,
	// the pre-condition is that this group expression's child group has been fully explored.
	for i := len(ge.groupExpression.Inputs) - 1; i >= 0; i-- {
		ge.Push(NewOptGroupTask(ge.mctx, ge.groupExpression.Inputs[i]))
	}
	return nil
}

// Desc implements the task.Desc interface.
func (ge *OptGroupExpressionTask) Desc(w io.StringWriter) {
	w.WriteString("OptGroupExpressionTask{ge:")
	ge.groupExpression.String(w)
	w.WriteString("}")
}

// getValidRules filter the allowed rule from session variable, and system config.
func (ge *OptGroupExpressionTask) getValidRules() []rule.Rule {
	r, ok := rule.XFormRuleSet[pattern.GetOperand(ge.groupExpression.LogicalPlan)]
	if !ok {
		return nil
	}
	ruleList := make([]rule.Rule, 0, len(r))
	for _, oneR := range r {
		// todo: session variable control
		ruleList = append(ruleList, oneR)
	}
	return ruleList
}
