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

package constraint

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
)

// DeleteTrueExprs deletes the surely true expressions
func DeleteTrueExprs(p base.LogicalPlan, conds []expression.Expression) []expression.Expression {
	newConds := make([]expression.Expression, 0, len(conds))
	for _, cond := range conds {
		con, ok := cond.(*expression.Constant)
		if !ok {
			newConds = append(newConds, cond)
			continue
		}
		if expression.MaybeOverOptimized4PlanCache(p.SCtx().GetExprCtx(), []expression.Expression{con}) {
			newConds = append(newConds, cond)
			continue
		}
		sc := p.SCtx().GetSessionVars().StmtCtx
		if isTrue, err := con.Value.ToBool(sc.TypeCtx()); err == nil && isTrue == 1 {
			continue
		}
		newConds = append(newConds, cond)
	}
	return newConds
}
