// Copyright 2026 PingCAP, Inc.
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
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/sessionctx"
)

// cteOverrideEvalExprCtx overrides EvalCtx while delegating all other ExprContext methods.
type cteOverrideEvalExprCtx struct {
	exprctx.ExprContext
	evalCtx exprctx.EvalContext
}

func (c *cteOverrideEvalExprCtx) GetEvalCtx() exprctx.EvalContext {
	return c.evalCtx
}

// cteOverrideExprSessionCtx is a thin wrapper to override ExprCtx without mutating the original session.
type cteOverrideExprSessionCtx struct {
	sessionctx.Context
	exprCtx exprctx.ExprContext
}

func (c *cteOverrideExprSessionCtx) GetExprCtx() exprctx.ExprContext {
	return c.exprCtx
}

// makeCTEStrictTruncateErrSessionCtx wraps the session context and overrides only the ExprCtx,
// so recursive CTE worktable writes treat truncation like INSERT in strict SQL mode.
func makeCTEStrictTruncateErrSessionCtx(sctx sessionctx.Context) sessionctx.Context {
	origExprCtx := sctx.GetExprCtx()
	// Recursive CTE in MySQL behaves like writing into an internal worktable, so truncation is handled like INSERT.
	overrideBuildCtx := exprctx.CtxWithHandleTruncateErrLevel(origExprCtx, errctx.LevelError)
	overrideExprCtx := &cteOverrideEvalExprCtx{
		ExprContext: origExprCtx,
		evalCtx:     overrideBuildCtx.GetEvalCtx(),
	}
	return &cteOverrideExprSessionCtx{
		Context: sctx,
		exprCtx: overrideExprCtx,
	}
}
