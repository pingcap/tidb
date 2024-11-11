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
	"github.com/pingcap/tidb/pkg/planner/cascades/base/yctx"
	"github.com/pingcap/tidb/pkg/planner/cascades/memo"
	"github.com/pingcap/tidb/pkg/planner/cascades/task"
	base2 "github.com/pingcap/tidb/pkg/planner/core/base"
)

var _ yctx.YamsContext = &YamsContext{}

// YamsContext includes all the context stuff when go through memo optimizing.
type YamsContext struct {
	// pctx variable awareness.
	pctx base2.PlanContext
	// memo pool management.
	mm *memo.Memo
	// task pool management.
	scheduler base.Scheduler
	// originalSql for debug facility.
	originSql string
}

// NewYamsContext returns a new memo context responsible for manage all the stuff in YAMS opt.
func NewYamsContext(pctx base2.PlanContext) *YamsContext {
	return &YamsContext{
		pctx: pctx,
		// memo pool management.
		mm: memo.MemoPool.Get().(*memo.Memo),
		// task pool management.
		scheduler: task.NewSimpleTaskScheduler(),
		// originalSql for debug facility.
		originSql: pctx.GetSessionVars().StmtCtx.OriginalSQL,
	}
}

// Destroy the memo context, which will clean the resource allocated during this phase.
func (m *YamsContext) Destroy() {
	// when a memo optimizing phase is done for a session,
	// we should put the stack and memo back to the pool management for reuse.
	m.mm.Destroy()
	m.scheduler.Destroy()
}

// GetScheduler return the stack inside this memo context.
func (mc *YamsContext) GetScheduler() base.Scheduler {
	return mc.scheduler
}

// PushTask puts a task into the stack structure inside.
func (mc *YamsContext) PushTask(task base.Task) {
	mc.scheduler.PushTask(task)
}

// GetMemo returns the basic memo structure.
func (mc *YamsContext) GetMemo() *memo.Memo {
	return mc.mm
}
