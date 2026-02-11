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

package sessionctx

import (
	"sync"

	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	rangerctx "github.com/pingcap/tidb/pkg/util/ranger/context"
)

// exprCtxOverrideSessionWrapper scopes an ExprCtx override to a subtree without mutating the original session.
type exprCtxOverrideSessionWrapper struct {
	Context
	exprCtx exprctx.ExprContext

	planCtxOnce sync.Once
	planCtx     planctx.PlanContext
}

// UnwrapAsInternalSctx returns the underlying internal session context as `any`.
func (c *exprCtxOverrideSessionWrapper) UnwrapAsInternalSctx() any {
	if u, ok := c.Context.(planctx.InternalSctxUnwrapper); ok {
		return u.UnwrapAsInternalSctx()
	}
	return c.Context
}

func (c *exprCtxOverrideSessionWrapper) GetExprCtx() exprctx.ExprContext {
	return c.exprCtx
}

func (c *exprCtxOverrideSessionWrapper) GetPlanCtx() planctx.PlanContext {
	c.planCtxOnce.Do(func() {
		c.planCtx = planctx.WithExprCtx(c.Context.GetPlanCtx(), c.exprCtx)
	})
	return c.planCtx
}

func (c *exprCtxOverrideSessionWrapper) GetRangerCtx() *rangerctx.RangerContext {
	return c.GetPlanCtx().GetRangerCtx()
}

func (c *exprCtxOverrideSessionWrapper) GetBuildPBCtx() *planctx.BuildPBContext {
	return c.GetPlanCtx().GetBuildPBCtx()
}

// WithExprCtx wraps the session context and overrides only the ExprCtx (and derived plan/ranger contexts),
// so the behavior change is scoped to the returned context.
func WithExprCtx(sctx Context, overrideExprCtx exprctx.ExprContext) Context {
	if overrideExprCtx == nil || overrideExprCtx == sctx.GetExprCtx() {
		return sctx
	}
	return &exprCtxOverrideSessionWrapper{
		Context: sctx,
		exprCtx: overrideExprCtx,
	}
}

// WithTruncateErrLevel wraps the session context and overrides ExprCtx to handle truncation errors at the specified level.
//
// It is a scoped change that does not mutate the original context.
func WithTruncateErrLevel(sctx Context, level errctx.Level) Context {
	origExprCtx := sctx.GetExprCtx()
	overrideExprCtx := exprctx.WithHandleTruncateErrLevel(origExprCtx, level)
	if overrideExprCtx == origExprCtx {
		return sctx
	}
	return WithExprCtx(sctx, overrideExprCtx)
}
