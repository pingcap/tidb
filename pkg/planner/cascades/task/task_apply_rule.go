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
	"io"
)

type ApplyRuleTask struct {
	BaseTask

	gE   *memo.GroupExpression
	rule rule.Rule
	// currently we are all explore type tasks.
}

// NewApplyRuleTask return a new apply rule task.
func NewApplyRuleTask(mctx *MemoContext, gE *memo.GroupExpression, r rule.Rule) *ApplyRuleTask {
	return &ApplyRuleTask{
		BaseTask: BaseTask{
			mctx: mctx,
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
	for binder.Next() {
		holder := binder.GetHolder()
		if !a.rule.PreCheck(holder, a.ctx) {
			continue
		}
		memoExprs, err := a.rule.XForm(holder, a.ctx)
		if err != nil {
			return err
		}
		for _, me := range memoExprs {
			newGroupExpr, err := a.mm.CopyIn(a.gE.GetGroup(), me)
			if err != nil {
				return err
			}
			// YAMS only care about logical plan now.
			a.Push(NewOptGroupExpressionTask(a.mm, a.ctx, newGroupExpr))
		}
	}
	a.gE.SetExplored(a.rule.ID())
	return nil
}

// Desc implements the task.Desc interface.
func (a *ApplyRuleTask) Desc(w io.StringWriter) {
	w.WriteString("ApplyRuleTask{gE:")
	a.gE.String(w)
	w.WriteString(", rule:")
	a.rule.String(w)
	w.WriteString("}")
}

type MemoContext struct {
	mm        *memo.Memo
	sctx      sessionctx.Context
	stack     *taskStack
	originSql string
}

// NewMemoContext returns a new memo context responsible for manage all the stuff in YAMS opt.
func NewMemoContext(sctx sessionctx.Context) *MemoContext {
	return &MemoContext{sctx: sctx, stack: StackTaskPool.Get().(*taskStack), originSql: sctx.GetSessionVars().StmtCtx.OriginalSQL}
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
