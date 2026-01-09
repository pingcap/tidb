// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package partidx

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/pingcap/tidb/pkg/util/ranger/context"
)

type PreCondChecker struct {
}

// CheckConstraints checks whether the filters can meet the constraints from the predefined predicates in index meta.
// If the prePredicates has comparision filters like =, >, <, >=, <=, IN, IS NULL, IS NOT NULL on single column, we can try our best to match them.
// If the prePredicates has other filters like `sin(a) > 0`, we can only try to match them exactly.
// TODO: now we only support the comparision filters on single column.
func CheckConstraints(sctx planctx.PlanContext, prePredicates []expression.Expression, filters []expression.Expression) bool {
	if len(prePredicates) == 0 {
		return true
	}
	intest.Assert(len(prePredicates) == 1)
	if exactMatch(sctx.GetExprCtx().GetEvalCtx(), prePredicates, filters) {
		return true
	}

	if canBeImpliedFromExprs(sctx.GetRangerCtx(), prePredicates[0], filters) {
		return true
	}

	return false
}

func exactMatch(evalctx expression.EvalContext, prePredicates []expression.Expression, filters []expression.Expression) bool {
	matched := make([]bool, len(filters))
	for _, pre := range prePredicates {
		found := false
		for i, filter := range filters {
			if matched[i] {
				continue
			}
			if pre.Equal(evalctx, filter) {
				matched[i] = true
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func canBeImpliedFromExprs(
	rangerctx *context.RangerContext,
	pre expression.Expression,
	filters []expression.Expression,
) bool {
	sf := pre.(*expression.ScalarFunction)

	if sf.FuncName.L == ast.UnaryNot {
		nf, ok := sf.GetArgs()[0].(*expression.ScalarFunction)
		if !ok || nf.FuncName.L != ast.IsNull {
			return false
		}
		col, ok := nf.GetArgs()[0].(*expression.Column)
		if !ok {
			return false
		}
		return implIsNotNull(rangerctx, col, filters)
	}

	if _, ok := expression.CompareOpMap[sf.FuncName.L]; !ok {
		return false
	}
	return implCompareExpr(rangerctx, sf, filters)
}

func implCompareExpr(rangerctx *context.RangerContext, pre *expression.ScalarFunction, filters []expression.Expression) bool {
	var col *expression.Column
	if _, ok := pre.GetArgs()[0].(*expression.Column); ok {
		col = pre.GetArgs()[0].(*expression.Column)
	} else if _, ok := pre.GetArgs()[1].(*expression.Column); ok {
		col = pre.GetArgs()[1].(*expression.Column)
	} else {
		return false
	}
	ranges, _, _, err := ranger.BuildColumnRange([]expression.Expression{pre}, rangerctx, col.RetType, -1, 0)
	if len(ranges) == 0 || err != nil {
		return false
	}
	columnConds := ranger.ExtractAccessConditionsForColumn(rangerctx, filters, col)
	if len(columnConds) == 0 {
		return false
	}
	rangesFromFilters, _, _, err := ranger.BuildColumnRange(columnConds, rangerctx, col.RetType, -1, 0)
	if len(rangesFromFilters) == 0 || err != nil {
		return false
	}
	unionedBuff := make([]*ranger.Range, len(rangesFromFilters)+len(ranges))
	copy(unionedBuff, rangesFromFilters)
	copy(unionedBuff[len(rangesFromFilters):], ranges)
	unionedRange, err := ranger.UnionRanges(rangerctx, unionedBuff, false)
	if err != nil {
		return false
	}
	if len(unionedRange) != len(ranges) {
		return false
	}
	for i, ran := range unionedRange {
		if !ran.Equal(ranges[i]) {
			return false
		}
	}
	return true
}

func implIsNotNull(rangerctx *context.RangerContext, targetCol *expression.Column, filters []expression.Expression) bool {
	columnConds := ranger.ExtractAccessConditionsForColumn(rangerctx, filters, targetCol)
	if len(columnConds) == 0 {
		return false
	}
	rangesFromFilters, _, _, err := ranger.BuildColumnRange(columnConds, rangerctx, targetCol.RetType, -1, 0)
	if len(rangesFromFilters) == 0 || err != nil {
		return false
	}
	for _, ran := range rangesFromFilters {
		if ran.LowVal[0].IsNull() && !ran.LowExclude {
			return false
		}
	}
	return true
}

// AlwaysMeetConstraints checks whether the filters always meet the constraints from the predefined predicates in index meta.
// This check is only applied to the pre condition that is single IS NULL.
// e.g. for partial index idx(b) where a IS NULL, if the filter is b = ? and a is null/a > 10, then we can guarantee that a IS NULL is always true.
// Because the `IsNullRejected` has correctness issues. So we implement a simpler version here.
func AlwaysMeetConstraints(sctx planctx.PlanContext, prePredicates, filters []expression.Expression) bool {
	if len(prePredicates) != 1 {
		return false
	}
	sf, ok := prePredicates[0].(*expression.ScalarFunction)
	if !ok || sf.FuncName.L != ast.UnaryNot {
		return false
	}
	innerSf, ok := sf.GetArgs()[0].(*expression.ScalarFunction)
	if !ok || innerSf.FuncName.L != ast.IsNull {
		return false
	}
	col, ok := innerSf.GetArgs()[0].(*expression.Column)
	if !ok {
		return false
	}
	// When the index is chosen, the givenfilters can not be empty.
	for _, filter := range filters {
		sf, ok := filter.(*expression.ScalarFunction)
		if !ok {
			continue
		}
		if checkIsNullRejected(sctx, col, sf) {
			return true
		}
	}
	return false
}

func checkIsNullRejected(sctx planctx.PlanContext, targetCol *expression.Column, filter *expression.ScalarFunction) bool {
	if filter.FuncName.L == ast.LogicOr {
		leavesAllRejected := true
		for _, arg := range filter.GetArgs() {
			sf, ok := arg.(*expression.ScalarFunction)
			if !ok || !checkIsNullRejected(sctx, targetCol, sf) {
				leavesAllRejected = false
				break
			}
		}
		return leavesAllRejected
	}
	if filter.FuncName.L == ast.LogicAnd {
		for _, arg := range filter.GetArgs() {
			sf, ok := arg.(*expression.ScalarFunction)
			if ok && checkIsNullRejected(sctx, targetCol, sf) {
				return true
			}
		}
		return false
	}
	if filter.FuncName.L == ast.IsNull {
		col, ok := filter.GetArgs()[0].(*expression.Column)
		if ok && col.Equal(sctx.GetExprCtx().GetEvalCtx(), targetCol) {
			return true
		}
	}
	if _, ok := expression.CompareOpMap[filter.FuncName.L]; ok {
		if filter.FuncName.L == ast.NullEQ {
			return false
		}
		col, ok := filter.GetArgs()[0].(*expression.Column)
		if !ok {
			col, ok = filter.GetArgs()[1].(*expression.Column)
		}
		if ok && col.Equal(sctx.GetExprCtx().GetEvalCtx(), targetCol) {
			return true
		}
	}
	return false
}
