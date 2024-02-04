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

package core

import (
	"context"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
)

// convertOuterToInnerJoin converts outer to inner joins if the unmtaching rows are filtered.
type convertOuterToInnerJoin struct {
}

func (*convertOuterToInnerJoin) optimize(_ context.Context, p LogicalPlan, _ *logicalOptimizeOp) (LogicalPlan, bool, error) {
	planChanged := false
	return p.convertOuterToInnerJoin(nil), planChanged, nil
}

func (s *baseLogicalPlan) convertOuterToInnerJoin(predicates []expression.Expression) LogicalPlan {
	p := s.self
	for i, child := range p.Children() {
		newChild := child.convertOuterToInnerJoin(predicates)
		p.SetChild(i, newChild)
	}
	return p
}

func (s *LogicalJoin) convertOuterToInnerJoin(predicates []expression.Expression) LogicalPlan {
	convertOuter2InnerJoins(s, predicates)
	return s
}

func (*convertOuterToInnerJoin) name() string {
	return "convert_outer_to_inner_joins"
}

func (s *LogicalSelection) convertOuterToInnerJoin(predicates []expression.Expression) LogicalPlan {
	p := s.self.(*LogicalSelection)
	child := p.Children()[0]
	child = child.convertOuterToInnerJoin(p.Conditions)
	p.SetChildren(child)
	return p
}

// convertOuter2InnerJoins transforms "LeftOuterJoin/RightOuterJoin" to "InnerJoin" if predicates filters out unmatching rows.
func convertOuter2InnerJoins(p *LogicalJoin, predicates []expression.Expression) {
	if p.JoinType != LeftOuterJoin && p.JoinType != RightOuterJoin && p.JoinType != InnerJoin {
		return
	}

	innerTable := p.children[0]
	outerTable := p.children[1]
	if p.JoinType == LeftOuterJoin {
		innerTable, outerTable = outerTable, innerTable
	}

	// first simplify embedded outer join.
	if innerPlan, ok := innerTable.(*LogicalJoin); ok {
		convertOuter2InnerJoins(innerPlan, predicates)
	}
	if outerPlan, ok := outerTable.(*LogicalJoin); ok {
		convertOuter2InnerJoins(outerPlan, predicates)
	}

	if p.JoinType == InnerJoin {
		return
	}
	// then simplify embedding outer join.
	canBeSimplified := false
	for _, expr := range predicates {
		// avoid the case where the expr only refers to the schema of outerTable
		if expression.ExprFromSchema(expr, outerTable.Schema()) {
			continue
		}
		isOk := isNullFiltered(p.self.SCtx(), innerTable.Schema(), expr)
		if isOk {
			canBeSimplified = true
			break
		}
	}
	if canBeSimplified {
		p.JoinType = InnerJoin
	}
}

// isNullFiltered check whether a condition is null-rejected
// A condition would be null-rejected in one of following cases:
// If it is a predicate containing a reference to an inner table that evaluates to UNKNOWN or FALSE when one of its arguments is NULL.
// If it is a conjunction containing a null-rejected condition as a conjunct.
// If it is a disjunction of null-rejected conditions.
func isNullFiltered(ctx sessionctx.Context, schema *expression.Schema, expr expression.Expression) bool {
	expr = expression.PushDownNot(ctx, expr)
	if expression.ContainOuterNot(expr) {
		return false
	}
	sc := ctx.GetSessionVars().StmtCtx
	sc.InNullRejectCheck = true
	defer func() {
		sc.InNullRejectCheck = false
	}()
	for _, cond := range expression.SplitCNFItems(expr) {
		if isNullFilteredSpecialCase(ctx, schema, expr) {
			return true
		}

		result := expression.EvaluateExprWithNull(ctx, schema, cond)
		x, ok := result.(*expression.Constant)
		if !ok {
			continue
		}
		if x.Value.IsNull() {
			return true
		} else if isTrue, err := x.Value.ToBool(sc.TypeCtxOrDefault()); err == nil && isTrue == 0 {
			return true
		}
	}
	return false
}

// isNullFilteredSpecialCase handles some null-rejected cases specially, since the current in
// EvaluateExprWithNull is too strict for some cases, e.g. #49616.
func isNullFilteredSpecialCase(ctx sessionctx.Context, schema *expression.Schema, expr expression.Expression) bool {
	return isNullFilteredSpecialCase1(ctx, schema, expr) // only 1 case now
}

// isNullFilteredSpecialCase1 is mainly for #49616.
// Case1 specially handles `null-rejected OR (null-rejected AND {others})`, then no matter what the result
// of `{others}` is (True, False or Null), the result of this predicate is null, so this predicate is null-rejected.
func isNullFilteredSpecialCase1(ctx sessionctx.Context, schema *expression.Schema, expr expression.Expression) bool {
	isFunc := func(e expression.Expression, lowerFuncName string) *expression.ScalarFunction {
		f, ok := e.(*expression.ScalarFunction)
		if !ok {
			return nil
		}
		if f.FuncName.L == lowerFuncName {
			return f
		}
		return nil
	}
	orFunc := isFunc(expr, ast.LogicOr)
	if orFunc == nil {
		return false
	}
	for i := 0; i < 2; i++ {
		andFunc := isFunc(orFunc.GetArgs()[i], ast.LogicAnd)
		if andFunc == nil {
			continue
		}
		if !isNullFiltered(ctx, schema, orFunc.GetArgs()[1-i]) {
			continue // the other side should be null-rejected: null-rejected OR (... AND ...)
		}
		for _, andItem := range expression.SplitCNFItems(andFunc) {
			if isNullFiltered(ctx, schema, andItem) {
				return true // hit the case in the comment: null-rejected OR (null-rejected AND ...)
			}
		}
	}
	return false
}
