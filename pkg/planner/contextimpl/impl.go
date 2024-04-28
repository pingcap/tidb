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

package contextimpl

import (
	exprctx "github.com/pingcap/tidb/pkg/expression/context"
	"github.com/pingcap/tidb/pkg/planner/context"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/util/intest"
)

var _ context.PlanContext = struct {
	sessionctx.Context
	*PlanCtxExtendedImpl
}{}

// PlanCtxExtendedImpl provides extended method for session context to implement `PlanContext`
type PlanCtxExtendedImpl struct {
	sctx                   sessionctx.Context
	nullRejectCheckExprCtx *exprctx.NullRejectCheckExprContext
}

// NewPlanCtxExtendedImpl creates a new PlanCtxExtendedImpl.
func NewPlanCtxExtendedImpl(sctx sessionctx.Context) *PlanCtxExtendedImpl {
	return &PlanCtxExtendedImpl{
		sctx:                   sctx,
		nullRejectCheckExprCtx: exprctx.WithNullRejectCheck(sctx.GetExprCtx()),
	}
}

// GetNullRejectCheckExprCtx returns a context with null rejected check
func (ctx *PlanCtxExtendedImpl) GetNullRejectCheckExprCtx() exprctx.ExprContext {
	intest.AssertFunc(func() bool {
		// assert `sctx.GetExprCtx()` should keep the same to avoid some unexpected behavior.
		return ctx.nullRejectCheckExprCtx.ExprContext == ctx.sctx.GetExprCtx()
	})
	return ctx.nullRejectCheckExprCtx
}

// AdviseTxnWarmup advises the txn to warm up.
func (ctx *PlanCtxExtendedImpl) AdviseTxnWarmup() error {
	return sessiontxn.GetTxnManager(ctx.sctx).AdviseWarmup()
}
