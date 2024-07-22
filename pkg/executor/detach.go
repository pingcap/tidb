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

package executor

import (
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression/contextsession"
)

// Detach detaches the current executor from the session context. After detaching, the session context
// can be used to execute another statement while this executor is still running. The returning value
// shows whether this executor is able to be detached.
//
// NOTE: the implementation of `Detach` should guarantee that no matter whether it returns true or false,
// both the original executor and the returning executor should be able to be used correctly. This restriction
// is to make sure that if `Detach(a)` returns `true`, while other children of `a`'s parent returns `false`,
// the caller can still use the original one.
func Detach(originalExecutor exec.Executor) (exec.Executor, bool) {
	newExecutor, ok := originalExecutor.Detach()
	if !ok {
		return nil, false
	}

	children := originalExecutor.AllChildren()
	newChildren := make([]exec.Executor, len(children))
	for i, child := range children {
		detached, ok := Detach(child)
		if !ok {
			return nil, false
		}
		newChildren[i] = detached
	}
	newExecutor.SetAllChildren(newChildren)

	return newExecutor, true
}

func (treCtx tableReaderExecutorContext) Detach() tableReaderExecutorContext {
	newCtx := treCtx

	if ctx, ok := treCtx.ectx.(*contextsession.SessionExprContext); ok {
		staticExprCtx := ctx.IntoStatic()

		newCtx.dctx = newCtx.dctx.Detach()
		newCtx.rctx = newCtx.rctx.Detach(staticExprCtx)
		newCtx.buildPBCtx = newCtx.buildPBCtx.Detach(staticExprCtx)
		newCtx.ectx = staticExprCtx
		return newCtx
	}

	return treCtx
}

// Detach detaches the current executor from the session context.
func (e *TableReaderExecutor) Detach() (exec.Executor, bool) {
	newExec := new(TableReaderExecutor)
	*newExec = *e

	newExec.tableReaderExecutorContext = newExec.tableReaderExecutorContext.Detach()

	return newExec, true
}
