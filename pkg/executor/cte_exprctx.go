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
	"sync"

	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/sessionctx"
	rangerctx "github.com/pingcap/tidb/pkg/util/ranger/context"
)

// sessionCtxExprOverrideWrapper scopes an ExprCtx override to a subtree without mutating the original session.
type sessionCtxExprOverrideWrapper struct {
	sessionctx.Context
	exprCtx exprctx.ExprContext

	planCtxOnce sync.Once
	planCtx     planctx.PlanContext
}

func (c *sessionCtxExprOverrideWrapper) GetExprCtx() exprctx.ExprContext {
	return c.exprCtx
}

func (c *sessionCtxExprOverrideWrapper) GetPlanCtx() planctx.PlanContext {
	c.planCtxOnce.Do(func() {
		c.planCtx = planctx.WithExprCtx(c.Context.GetPlanCtx(), c.exprCtx)
	})
	return c.planCtx
}

func (c *sessionCtxExprOverrideWrapper) GetRangerCtx() *rangerctx.RangerContext {
	return c.GetPlanCtx().GetRangerCtx()
}

func (c *sessionCtxExprOverrideWrapper) GetBuildPBCtx() *planctx.BuildPBContext {
	return c.GetPlanCtx().GetBuildPBCtx()
}

// wrapSessionCtxWithExprCtx wraps the session context and overrides only the ExprCtx (and derived plan/ranger contexts),
// so the behavior change is scoped to the returned context.
func wrapSessionCtxWithExprCtx(sctx sessionctx.Context, overrideExprCtx exprctx.ExprContext) sessionctx.Context {
	if overrideExprCtx == nil || overrideExprCtx == sctx.GetExprCtx() {
		return sctx
	}
	return &sessionCtxExprOverrideWrapper{
		Context: sctx,
		exprCtx: overrideExprCtx,
	}
}

// wrapSessionCtxForCTEStrictTruncateErr wraps the session context and overrides only the ExprCtx,
// so recursive CTE worktable writes treat truncation like INSERT in strict SQL mode.
func wrapSessionCtxForCTEStrictTruncateErr(sctx sessionctx.Context) sessionctx.Context {
	origExprCtx := sctx.GetExprCtx()
	// Recursive CTE in MySQL behaves like writing into an internal worktable, so truncation is handled like INSERT.
	overrideExprCtx := exprctx.WithHandleTruncateErrLevel(origExprCtx, errctx.LevelError)
	return wrapSessionCtxWithExprCtx(sctx, overrideExprCtx)
}
