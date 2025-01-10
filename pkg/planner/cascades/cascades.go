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

package cascades

import (
	"github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/planner/cascades/base/cascadesctx"
	"github.com/pingcap/tidb/pkg/planner/cascades/memo"
	"github.com/pingcap/tidb/pkg/planner/cascades/task"
	corebase "github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// Optimizer is a basic cascades search framework portal, driven by Context.
type Optimizer struct {
	logic corebase.LogicalPlan
	ctx   cascadesctx.Context
}

// NewCascades return a new cascades obj for logical alternative searching.
func NewCascades(lp corebase.LogicalPlan) (*Optimizer, error) {
	cas := &Optimizer{
		logic: lp,
		ctx:   NewContext(lp.SCtx()),
	}
	ge, err := cas.ctx.GetMemo().Init(lp)
	intest.Assert(err == nil)
	intest.Assert(ge != nil)
	if err != nil {
		return nil, err
	}
	cas.ctx.GetScheduler().PushTask(task.NewOptGroupTask(cas.ctx, ge.GetGroup()))
	return cas, err
}

// Execute run the yams search flow inside, returns error if it happened.
func (c *Optimizer) Execute() error {
	return c.ctx.GetScheduler().ExecuteTasks()
}

// Destroy clean and reset basic elements inside.
func (c *Optimizer) Destroy() {
	c.ctx.Destroy()
}

// GetMemo returns the memo structure inside cascades.
func (c *Optimizer) GetMemo() *memo.Memo {
	return c.ctx.GetMemo()
}

// Context includes all the context stuff when go through memo optimizing.
type Context struct {
	// pctx variable awareness.
	pctx corebase.PlanContext
	// memo management.
	mm *memo.Memo
	// task pool management.
	scheduler base.Scheduler
}

// NewContext returns a new memo context responsible for manage all the stuff in cascades opt.
func NewContext(pctx corebase.PlanContext) *Context {
	return &Context{
		pctx: pctx,
		// memo init with capacity.
		mm: memo.NewMemo(pctx.GetSessionVars().StmtCtx.OperatorNum),
		// task pool management.
		scheduler: task.NewSimpleTaskScheduler(),
	}
}

// Destroy the memo context, which will clean the resource allocated during this phase.
func (c *Context) Destroy() {
	// when a memo optimizing phase is done for a session,
	// we should put the stack back and clean the memo.
	c.mm.Destroy()
	c.scheduler.Destroy()
}

// GetScheduler return the stack inside this memo context.
func (c *Context) GetScheduler() base.Scheduler {
	return c.scheduler
}

// PushTask puts a task into the stack structure inside.
func (c *Context) PushTask(task base.Task) {
	c.scheduler.PushTask(task)
}

// GetMemo returns the basic memo structure.
func (c *Context) GetMemo() *memo.Memo {
	return c.mm
}
