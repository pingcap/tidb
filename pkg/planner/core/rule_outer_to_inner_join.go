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
		isOk := isNullFiltered(p.self.SCtx(), innerTable.Schema(), expr, outerTable.Schema())
		if isOk {
			canBeSimplified = true
			break
		}
	}
	if canBeSimplified {
		p.JoinType = InnerJoin
	}
}

func isNullFiltered(ctx sessionctx.Context, innerSchema *expression.Schema, predicate expression.Expression, outerSchema *expression.Schema) bool {
	// avoid the case where the predicate only refers to the schema of outerTable
	if expression.ExprFromSchema(predicate, outerSchema) {
		return false
	}

	switch expr := predicate.(type) {
	case *expression.ScalarFunction:
		if expr.FuncName.L == ast.LogicAnd {
			if isNullFiltered(ctx, innerSchema, expr.GetArgs()[0], outerSchema) {
				return true
			} else {
				return isNullFiltered(ctx, innerSchema, expr.GetArgs()[1], outerSchema)
			}
		} else if expr.FuncName.L == ast.LogicOr {
			if !(isNullFiltered(ctx, innerSchema, expr.GetArgs()[0], outerSchema)) {
				return false
			} else {
				return isNullFiltered(ctx, innerSchema, expr.GetArgs()[1], outerSchema)
			}
		} else {
			return isNullFilteredOneExpr(ctx, innerSchema, expr)
		}
	default:
		return isNullFilteredOneExpr(ctx, innerSchema, expr)
	}
}

// isNullFilteredOneExpr check whether a condition is null-rejected
// A condition would be null-rejected in one of following cases:
// If it is a predicate containing a reference to an inner table that evaluates to UNKNOWN or FALSE when one of its arguments is NULL.
// If it is a conjunction containing a null-rejected condition as a conjunct.
// If it is a disjunction of null-rejected conditions.
func isNullFilteredOneExpr(ctx sessionctx.Context, schema *expression.Schema, expr expression.Expression) bool {
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
