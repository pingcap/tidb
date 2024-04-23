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
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util/coreusage"
)

func mergeOnClausePredicates(p *LogicalJoin, predicates []expression.Expression) []expression.Expression {
	combinedCond := make([]expression.Expression, 0,
		len(p.LeftConditions)+len(p.RightConditions)+
			len(p.EqualConditions)+len(p.OtherConditions)+
			len(predicates))
	combinedCond = append(combinedCond, p.LeftConditions...)
	combinedCond = append(combinedCond, p.RightConditions...)
	combinedCond = append(combinedCond, expression.ScalarFuncs2Exprs(p.EqualConditions)...)
	combinedCond = append(combinedCond, p.OtherConditions...)
	combinedCond = append(combinedCond, predicates...)
	return combinedCond
}

// convertOuterToInnerJoin converts outer to inner joins if the unmtaching rows are filtered.
type convertOuterToInnerJoin struct {
}

func (*convertOuterToInnerJoin) optimize(_ context.Context, p base.LogicalPlan, _ *coreusage.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	planChanged := false
	return p.ConvertOuterToInnerJoin(nil), planChanged, nil
}

// ConvertOuterToInnerJoin implements base.LogicalPlan ConvertOuterToInnerJoin interface.
func (s *baseLogicalPlan) ConvertOuterToInnerJoin(predicates []expression.Expression) base.LogicalPlan {
	p := s.self
	for i, child := range p.Children() {
		newChild := child.ConvertOuterToInnerJoin(predicates)
		p.SetChild(i, newChild)
	}
	return p
}

// ConvertOuterToInnerJoin implements base.LogicalPlan ConvertOuterToInnerJoin interface.
func (p *LogicalJoin) ConvertOuterToInnerJoin(predicates []expression.Expression) base.LogicalPlan {
	innerTable := p.Children()[0]
	outerTable := p.Children()[1]
	switchChild := false

	if p.JoinType == LeftOuterJoin {
		innerTable, outerTable = outerTable, innerTable
		switchChild = true
	}

	combinedCond := mergeOnClausePredicates(p, predicates)
	innerTable = innerTable.ConvertOuterToInnerJoin(combinedCond)
	if p.JoinType == InnerJoin {
		outerTable = outerTable.ConvertOuterToInnerJoin(combinedCond)
	} else {
		outerTable = outerTable.ConvertOuterToInnerJoin(predicates)
	}

	if switchChild {
		p.SetChild(0, outerTable)
		p.SetChild(1, innerTable)
	} else {
		p.SetChild(0, innerTable)
		p.SetChild(1, outerTable)
	}

	canBeSimplified := false
	for _, expr := range predicates {
		isOk := isNullFiltered(p.SCtx(), innerTable.Schema(), expr, outerTable.Schema())
		if isOk {
			canBeSimplified = true
			break
		}
	}
	if canBeSimplified {
		p.JoinType = InnerJoin
	}

	return p
}

func (*convertOuterToInnerJoin) name() string {
	return "convert_outer_to_inner_joins"
}

// ConvertOuterToInnerJoin implements base.LogicalPlan ConvertOuterToInnerJoin interface.
func (s *LogicalSelection) ConvertOuterToInnerJoin(predicates []expression.Expression) base.LogicalPlan {
	p := s.self.(*LogicalSelection)
	combinedCond := append(predicates, p.Conditions...)
	child := p.Children()[0]
	child = child.ConvertOuterToInnerJoin(combinedCond)
	p.SetChildren(child)
	return p
}

// allConstants checks if only the expression has only constants.
func allConstants(expr expression.Expression) bool {
	switch v := expr.(type) {
	case *expression.ScalarFunction:
		for _, arg := range v.GetArgs() {
			if !allConstants(arg) {
				return false
			}
		}
		return true
	case *expression.Constant:
		return true
	}
	return false
}

// isNullFiltered takes care of complex predicates like this:
// isNullFiltered(A OR B) = isNullFiltered(A) AND isNullFiltered(B)
// isNullFiltered(A AND B) = isNullFiltered(A) OR isNullFiltered(B)
func isNullFiltered(ctx base.PlanContext, innerSchema *expression.Schema, predicate expression.Expression, outerSchema *expression.Schema) bool {
	// The expression should reference at least one field in innerSchema or all constants.
	if !expression.ExprReferenceSchema(predicate, innerSchema) && !allConstants(predicate) {
		return false
	}

	switch expr := predicate.(type) {
	case *expression.ScalarFunction:
		if expr.FuncName.L == ast.LogicAnd {
			if isNullFiltered(ctx, innerSchema, expr.GetArgs()[0], outerSchema) {
				return true
			}
			return isNullFiltered(ctx, innerSchema, expr.GetArgs()[0], outerSchema)
		} else if expr.FuncName.L == ast.LogicOr {
			if !(isNullFiltered(ctx, innerSchema, expr.GetArgs()[0], outerSchema)) {
				return false
			}
			return isNullFiltered(ctx, innerSchema, expr.GetArgs()[1], outerSchema)
		} else {
			return isNullFilteredOneExpr(ctx, innerSchema, expr)
		}
	default:
		return isNullFilteredOneExpr(ctx, innerSchema, predicate)
	}
}

// isNullFilteredOneExpr check whether a a single condition is null-filtered. A condition is null-filtered
// boolean expression evaluates to FALSE or UNKNOWN if all the arguments are null.
// The code goes over all the conditions and returns true if at least one is null filtering. For this module,
// We just need to check one simple condition but the routine is also used as a general utility for checking if
// any list of condition (in CNF form) is null-filtered.
func isNullFilteredOneExpr(ctx base.PlanContext, schema *expression.Schema, expr expression.Expression) bool {
	exprCtx := ctx.GetExprCtx()
	expr = expression.PushDownNot(exprCtx, expr)
	if expression.ContainOuterNot(expr) {
		return false
	}
	sc := ctx.GetSessionVars().StmtCtx
	if !exprCtx.IsInNullRejectCheck() {
		exprCtx.SetInNullRejectCheck(true)
		defer exprCtx.SetInNullRejectCheck(false)
	}
	for _, cond := range expression.SplitCNFItems(expr) {
		result := expression.EvaluateExprWithNull(exprCtx, schema, cond)
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
