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
	"github.com/pingcap/tidb/pkg/planner/cascades/memo"
	"github.com/pingcap/tidb/pkg/planner/cascades/rule"
	"github.com/pingcap/tidb/pkg/sessionctx"
)

type ApplyRuleTask struct {
	BaseTask

	gE      *memo.GroupExpression
	rule    rule.Rule
	explore bool
}

func (a *ApplyRuleTask) execute() error {
	// check whether this rule has been applied in this gE or this gE is abandoned.
	if a.gE.IsExplored(a.rule.ID()) || a.gE.IsAbandoned() {
		return nil
	}
	pa := a.rule.Pattern()
	binder := rule.NewBinder(pa, a.gE)

	for binder.Next() {
		holder := binder.GetHolder()
		if !a.rule.PreCheck(holder, a.ctx) {
			continue
		}
		substitutions, err := a.rule.XForm(holder, a.ctx)
		if err != nil {
			return err
		}
		for _, sub := range substitutions {
			newGroupExpr = a.mm.CopyIn(a.gE.GetGroup(), sub)
			if !ok {
				continue
			}
			// YAMS only care about logical plan.
			a.Push(&OptExpressionTask{BaseTask{a.mm, a.mCtx}, newGroupExpr, false})
		}
	}
}

type MemoContext struct {
	sStx  sessionctx.Context
	stack *taskStack
}

func NewMemoContext(sctx sessionctx.Context) *MemoContext {
	return &MemoContext{sctx, StackTaskPool.Get().(*taskStack)}
}

func (m *MemoContext) destroy() {
	m.stack.Destroy()
}

func (m *MemoContext) getStack() Stack {
	return m.stack
}

func (m *MemoContext) pushTask(task Task) {
	m.stack.Push(task)
}
