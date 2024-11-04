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

package memo

import (
	"sync"

	"github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/sessionctx"
)

// StackPoolRef is introduced to avoid
var StackPoolRef *sync.Pool

// MemoContext includes all the context stuff when go through memo optimizing.
type MemoContext struct {
	// sctx variable awareness.
	sctx sessionctx.Context
	// memo pool management.
	mm *Memo
	// task pool management.
	stack base.Stack
	// originalSql for debug facility.
	originSql string
}

// NewMemoContext returns a new memo context responsible for manage all the stuff in YAMS opt.
func NewMemoContext(sctx sessionctx.Context) *MemoContext {
	return &MemoContext{
		sctx: sctx,
		// memo pool management.
		mm: MemoPool.Get().(*Memo),
		// task pool management.
		stack: StackPoolRef.Get().(base.Stack),
		// originalSql for debug facility.
		originSql: sctx.GetSessionVars().StmtCtx.OriginalSQL,
	}
}

// Destroy the memo context, which will clean the resource allocated during this phase.
func (m *MemoContext) Destroy() {
	// when a memo optimizing phase is done for a session,
	// we should put the stack and memo back to the pool management for reuse.
	m.mm.Destroy()
	m.stack.Destroy()
}

// GetStack return the stack inside this memo context.
func (mc *MemoContext) GetStack() base.Stack {
	return mc.stack
}

// PushTask puts a task into the stack structure inside.
func (mc *MemoContext) PushTask(task base.Task) {
	mc.stack.Push(task)
}

// GetMemo returns the basic memo structure.
func (mc *MemoContext) GetMemo() *Memo {
	return mc.mm
}
