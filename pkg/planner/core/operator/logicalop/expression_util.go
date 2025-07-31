// Copyright 2025 PingCAP, Inc.
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

package logicalop

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
)

// Conds2TableDual builds a LogicalTableDual if cond is constant false or null.
func Conds2TableDual(p base.LogicalPlan, conds []expression.Expression) base.LogicalPlan {
	if len(conds) == 0 {
		return nil
	}
	exprCtx := p.SCtx().GetExprCtx()
	for _, cond := range conds {
		if expression.IsConstNull(cond) {
			if expression.MaybeOverOptimized4PlanCache(exprCtx, conds...) {
				return nil
			}
			dual := LogicalTableDual{}.Init(p.SCtx(), p.QueryBlockOffset())
			dual.SetSchema(p.Schema())
			return dual
		}
	}
	if len(conds) != 1 {
		return nil
	}
	if expression.MaybeOverOptimized4PlanCache(exprCtx, conds...) {
		return nil
	}
	sc := p.SCtx().GetSessionVars().StmtCtx
	if IsConstFalse(sc, conds[0]) {
		dual := LogicalTableDual{}.Init(p.SCtx(), p.QueryBlockOffset())
		dual.SetSchema(p.Schema())
		return dual
	}
	return nil
}

// IsConstFalse is to whether the expression is the const-false value
func IsConstFalse(sc *stmtctx.StatementContext, cond expression.Expression) bool {
	con, ok := cond.(*expression.Constant)
	if !ok {
		return false
	}
	if con.Value.IsNull() {
		return true
	}
	isTrue, err := con.Value.ToBool(sc.TypeCtxOrDefault())
	return (err == nil && isTrue == 0)
}
