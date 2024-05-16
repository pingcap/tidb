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
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
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

// convertOuterToInnerJoin is refactoring of the outer to inner join logic that used to be part of predicate push down.
// The rewrite passes down predicates from selection (WHERE clause) and join predicates (ON clause).
// All nodes except LogicalJoin are pass through where the rewrite is done for the child and nothing for the node itself.
// The main logic is applied for joins:
//  1. Traversal is preorder and the passed down predicate is checked for the left/right after join
//  2. The ON clause and passed down predicate (from higher selects or joins) are comined and applied to join children.
//     This logic depends on the join type with the following logic:
//     - For left/right outer joins, the ON clause an be applied only on the inner side (null producing side)
//     - For inner/semi joins, the ON clause can be applied on both children
//     - For anti semi joins, ON clause applied only on left side
//     - For all other cases, do not pass ON clause.
func (*convertOuterToInnerJoin) optimize(_ context.Context, p base.LogicalPlan, _ *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	planChanged := false
	return p.ConvertOuterToInnerJoin(nil), planChanged, nil
}

// LogicalAggregation just works since schema = child + aggregate expressions. No need to map predicates.
// Also, predicates involving aggregate expressions are not null filtering. IsNullReject always returns
// false for those cases.

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

	// First, simplify this join
	if p.JoinType == LeftOuterJoin || p.JoinType == RightOuterJoin {
		canBeSimplified := false
		for _, expr := range predicates {
			isOk := isNullFiltered(p.SCtx(), innerTable.Schema(), expr)
			if isOk {
				canBeSimplified = true
				break
			}
		}
		if canBeSimplified {
			p.JoinType = InnerJoin
		}
	}

	// Next simplify join children

	combinedCond := mergeOnClausePredicates(p, predicates)
	if p.JoinType == LeftOuterJoin || p.JoinType == RightOuterJoin {
		innerTable = innerTable.ConvertOuterToInnerJoin(combinedCond)
		outerTable = outerTable.ConvertOuterToInnerJoin(predicates)
	} else if p.JoinType == InnerJoin || p.JoinType == SemiJoin {
		innerTable = innerTable.ConvertOuterToInnerJoin(combinedCond)
		outerTable = outerTable.ConvertOuterToInnerJoin(combinedCond)
	} else if p.JoinType == AntiSemiJoin {
		innerTable = innerTable.ConvertOuterToInnerJoin(predicates)
		outerTable = outerTable.ConvertOuterToInnerJoin(combinedCond)
	} else {
		innerTable = innerTable.ConvertOuterToInnerJoin(predicates)
		outerTable = outerTable.ConvertOuterToInnerJoin(predicates)
	}

	if switchChild {
		p.SetChild(0, outerTable)
		p.SetChild(1, innerTable)
	} else {
		p.SetChild(0, innerTable)
		p.SetChild(1, outerTable)
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

// ConvertOuterToInnerJoin implements base.LogicalPlan ConvertOuterToInnerJoin interface.
func (s *LogicalProjection) ConvertOuterToInnerJoin(predicates []expression.Expression) base.LogicalPlan {
	p := s.self.(*LogicalProjection)
	canBePushed, _ := BreakDownPredicates(p, predicates)
	child := p.Children()[0]
	child = child.ConvertOuterToInnerJoin(canBePushed)
	p.SetChildren(child)
	return p
}

// allConstants checks if only the expression has only constants.
func allConstants(ctx expression.BuildContext, expr expression.Expression) bool {
	if expression.MaybeOverOptimized4PlanCache(ctx, []expression.Expression{expr}) {
		return false // expression contains non-deterministic parameter
	}
	switch v := expr.(type) {
	case *expression.ScalarFunction:
		for _, arg := range v.GetArgs() {
			if !allConstants(ctx, arg) {
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
func isNullFiltered(ctx base.PlanContext, innerSchema *expression.Schema, predicate expression.Expression) bool {
	// The expression should reference at least one field in innerSchema or all constants.
	if !expression.ExprReferenceSchema(predicate, innerSchema) && !allConstants(ctx.GetExprCtx(), predicate) {
		return false
	}

	switch expr := predicate.(type) {
	case *expression.ScalarFunction:
		if expr.FuncName.L == ast.LogicAnd {
			if isNullFiltered(ctx, innerSchema, expr.GetArgs()[0]) {
				return true
			}
			return isNullFiltered(ctx, innerSchema, expr.GetArgs()[0])
		} else if expr.FuncName.L == ast.LogicOr {
			if !(isNullFiltered(ctx, innerSchema, expr.GetArgs()[0])) {
				return false
			}
			return isNullFiltered(ctx, innerSchema, expr.GetArgs()[1])
		} else {
			return util.IsNullRejected(ctx, innerSchema, expr)
		}
	default:
		return util.IsNullRejected(ctx, innerSchema, predicate)
	}
}
