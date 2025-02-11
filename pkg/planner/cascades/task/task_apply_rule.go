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
	"github.com/pingcap/tidb/pkg/planner/cascades/rule"
	"github.com/pingcap/tidb/pkg/planner/cascades/util"
)

var _ base.Task = &ApplyRuleTask{}

// Document:
// Currently we introduce stack-based task scheduler for running the memo optimizing. This way is
// lightweight for call deeper chain, especially when the tree is beyond the recursive limit. Besides,
// all the optimizing logic is encapsulated as Task unit, which is running transparent and resource
// isolated internally.
//
// First, we are optimizing the root node from the memo tree downward, at the beginning we got the only
// one task as OptGroupTask{root}, inside which, the consecutive downward Tasks will be triggered and
// encapsulated and pushed into the singleton stack continuously. Different task type may trigger an
// additional task generation depend on how the Execute interface is implemented.
//
// Currently, here is how we work.
//
// Singleton Task Stack
//     ┌────┬────┬────┐
//     │    │    │    │
// ┌───┼────┼────┼────┼────────────────────────────────────────
// │ ┌─┼─┐┌─▼─┐┌─▼─┐┌─▼─┐┌───┐┌───┐┌───┐
// │ │ A ││ B ││ B ││ B ││ C ││ C ││ A │      Open Task Stack...
// │ └───┘└───┘└───┘└─┼─┘└─▲─┘└─▲─┘└─▲─┘
// └──────────────────┼────┼────┼────┼─────────────────────────
//                    │    │    │    │
//                    └────┴────┴────┘
// Symbol means:
// A represent OptGroupTask
// B represent OptGroupExpressionTask
// C represent ApplyRuleTask
//
// When memo init is done, the only targeted task is OptGroupTask, say we got 3 group expression inside
// this group, it will trigger and push additional 3 OptGroupExpressionTask into the stack when running
// A. Then task A is wiped out from the stack. With the FILO rule, the stack-top B will be popped out and
// run, from which it will find valid rules for its member group expression and encapsulate ApplyRuleTask
// for each of those valid rules. Say we got two valid rules here, so it will push another two task with
// type C into the stack, note, since current B's child group hasn't been optimized yet, so the cascaded
// task A will be triggered and pushed into the stack as well, and they are queued after rule tasks. then
// the old toppest B is wiped out from the stack.
//
// At last, when the stack is running out of task calling internally, or forcible mechanism is called from
// the outside, this stack running will be stopped.
//
// State Flow:
//                                                ┌── Opt 4 New Group Expression ──┐
//                                                │                                │
//     ┌────────────────┐            ┌────────────▼───────────┐            ┌───────┴───────┐
//     │ optGroupTask   │  ───────►  │ optGroupExpressionTask │  ───────►  │ ApplyRuleTask │
//     └──────▲─────────┘            └────────────┬───────────┘            └───────────────┘
//            │                                   │
//            └───── Child Opt Group Trigger ─────┘
//

// ApplyRuleTask is a wrapper of running basic logic union of scheduling apply rule.
type ApplyRuleTask struct {
	BaseTask

	gE   *memo.GroupExpression
	rule rule.Rule
	// currently we are all explore type tasks.
}

// NewApplyRuleTask return a new apply rule task.
func NewApplyRuleTask(ctx cascadesctx.Context, gE *memo.GroupExpression, r rule.Rule) *ApplyRuleTask {
	return &ApplyRuleTask{
		BaseTask: BaseTask{
			ctx: ctx,
		},
		gE:   gE,
		rule: r,
	}
}

// Execute implements the task.Execute interface.
func (a *ApplyRuleTask) Execute() error {
	// check whether this rule has been applied in this gE or this gE is abandoned.
	if a.gE.IsExplored(a.rule.ID()) || a.gE.IsAbandoned() {
		return nil
	}
	pa := a.rule.Pattern()
	binder := rule.NewBinder(pa, a.gE)
	holder := binder.Next()
	for ; holder != nil; holder = binder.Next() {
		if !a.rule.PreCheck(holder) {
			continue
		}
		newExprs, err := a.rule.XForm(holder)
		if err != nil {
			return err
		}
		for _, ne := range newExprs {
			newGroupExpr, err := a.ctx.GetMemo().CopyIn(a.gE.GetGroup(), ne)
			if err != nil {
				return err
			}
			// YAMS only care about logical plan now.
			a.Push(NewOptGroupExpressionTask(a.ctx, newGroupExpr))
		}
	}
	a.gE.SetExplored(a.rule.ID())
	return nil
}

// Desc implements the task.Desc interface.
func (a *ApplyRuleTask) Desc(w util.StrBufferWriter) {
	w.WriteString("ApplyRuleTask{gE:")
	a.gE.String(w)
	w.WriteString(", rule:")
	a.rule.String(w)
	w.WriteString("}")
}
