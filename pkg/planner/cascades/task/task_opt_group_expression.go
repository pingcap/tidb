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
	"github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/planner/cascades/base/cascadesctx"
	"github.com/pingcap/tidb/pkg/planner/cascades/memo"
	"github.com/pingcap/tidb/pkg/planner/cascades/pattern"
	"github.com/pingcap/tidb/pkg/planner/cascades/rule"
	"github.com/pingcap/tidb/pkg/planner/cascades/rule/ruleset"
	"github.com/pingcap/tidb/pkg/planner/cascades/util"
)

var _ base.Task = &OptGroupTask{}

// OptGroupExpressionTask is a wrapper of running logic of exploring group expression.
type OptGroupExpressionTask struct {
	BaseTask

	groupExpression *memo.GroupExpression
	// currently for each opt expression, it should be explore-type.
}

// NewOptGroupExpressionTask return a targeting optimizing group expression task.
func NewOptGroupExpressionTask(ctx cascadesctx.Context, ge *memo.GroupExpression) *OptGroupExpressionTask {
	return &OptGroupExpressionTask{
		BaseTask:        BaseTask{ctx: ctx},
		groupExpression: ge,
	}
}

// Execute implements the task.Execute interface.
func (ge *OptGroupExpressionTask) Execute() error {
	ruleMap := ge.getValidRules()
	for _, one := range ruleMap[pattern.GetOperand(ge.groupExpression.GetWrappedLogicalPlan())] {
		ge.Push(NewApplyRuleTask(ge.ctx, ge.groupExpression, one))
	}
	// since it's a stack-order, LIFO, when we want to apply a rule for a specific group expression,
	// the pre-condition is that this group expression's child group has been fully explored.
	for i := len(ge.groupExpression.Inputs) - 1; i >= 0; i-- {
		ge.Push(NewOptGroupTask(ge.ctx, ge.groupExpression.Inputs[i]))
	}
	return nil
}

// Desc implements the task.Desc interface.
func (ge *OptGroupExpressionTask) Desc(w util.StrBufferWriter) {
	w.WriteString("OptGroupExpressionTask{ge:")
	ge.groupExpression.String(w)
	w.WriteString("}")
}

// getValidRules filter the allowed rule from session variable, and system config.
func (*OptGroupExpressionTask) getValidRules() map[pattern.Operand][]rule.Rule {
	return ruleset.DefaultRuleSet
}
