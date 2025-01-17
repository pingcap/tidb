// Copyright 2023 PingCAP, Inc.
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
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/types"
)

// PredicateSimplification consolidates different predcicates on a column and its equivalence classes.  Initial out is for
//  1. in-list and not equal list intersection.
//  2. Drop OR predicates if they are empty for this pattern: P AND (P1 OR P2 ... OR Pn)
//     Pi is removed if P & Pi is false/empty.
//  3. Simplify predicates with logical constants (True/False).
type PredicateSimplification struct {
}

type predicateType = byte

const (
	inListPredicate predicateType = iota
	notEqualPredicate
	equalPredicate
	lessThanPredicate
	greaterThanPredicate
	lessThanOrEqualPredicate
	greaterThanOrEqualPredicate
	orPredicate
	andPredicate
	scalarPredicate
	falsePredicate
	truePredicate
	otherPredicate
)

func logicalConstant(bc base.PlanContext, cond expression.Expression) predicateType {
	sc := bc.GetSessionVars().StmtCtx
	con, ok := cond.(*expression.Constant)
	if !ok {
		return otherPredicate
	}
	if expression.MaybeOverOptimized4PlanCache(bc.GetExprCtx(), []expression.Expression{con}) {
		return otherPredicate
	}
	isTrue, err := con.Value.ToBool(sc.TypeCtxOrDefault())
	if err == nil {
		if isTrue == 0 {
			return falsePredicate
		}
		return truePredicate
	}
	return otherPredicate
}

// FindPredicateType determines the type of predicate represented by a given expression.
// It analyzes the provided expression and returns a column (if applicable) and a corresponding predicate type.
// The function handles different expression types, including constants, scalar functions, and their specific cases:
// - Logical operators (`OR` and `AND`).
// - Comparison operators (`EQ`, `NE`, `LT`, `GT`, `LE`, `GE`).
// - IN predicates with a list of constants.
// If the expression doesn't match any of these recognized patterns, it returns an `otherPredicate` type.
func FindPredicateType(bc base.PlanContext, expr expression.Expression) (*expression.Column, predicateType) {
	switch v := expr.(type) {
	case *expression.Constant:
		return nil, logicalConstant(bc, expr)
	case *expression.ScalarFunction:
		if v.FuncName.L == ast.LogicOr {
			return nil, orPredicate
		}
		if v.FuncName.L == ast.LogicAnd {
			return nil, andPredicate
		}
		args := v.GetArgs()
		if len(args) == 0 {
			return nil, otherPredicate
		}
		col, colOk := args[0].(*expression.Column)
		if !colOk {
			return nil, otherPredicate
		}
		if len(args) > 1 {
			if _, ok := args[1].(*expression.Constant); !ok {
				return nil, otherPredicate
			}
		}
		if v.FuncName.L == ast.NE {
			return col, notEqualPredicate
		} else if v.FuncName.L == ast.EQ {
			return col, equalPredicate
		} else if v.FuncName.L == ast.LT {
			return col, lessThanPredicate
		} else if v.FuncName.L == ast.GT {
			return col, greaterThanPredicate
		} else if v.FuncName.L == ast.LE {
			return col, lessThanOrEqualPredicate
		} else if v.FuncName.L == ast.GE {
			return col, greaterThanOrEqualPredicate
		} else if v.FuncName.L == ast.In {
			for _, value := range args[1:] {
				if _, ok := value.(*expression.Constant); !ok {
					return nil, otherPredicate
				}
			}
			return col, inListPredicate
		} else {
			return nil, otherPredicate
		}
	default:
		return nil, otherPredicate
	}
}

// Optimize implements base.LogicalOptRule.<0th> interface.
func (*PredicateSimplification) Optimize(_ context.Context, p base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	planChanged := false
	return p.PredicateSimplification(opt), planChanged, nil
}

// updateInPredicate applies intersection of an in list with <> value. It returns updated In list and a flag for
// a special case if an element in the inlist is not removed to keep the list not empty.
func updateInPredicate(ctx base.PlanContext, inPredicate expression.Expression, notEQPredicate expression.Expression) (expression.Expression, bool) {
	_, inPredicateType := FindPredicateType(ctx, inPredicate)
	_, notEQPredicateType := FindPredicateType(ctx, notEQPredicate)
	if inPredicateType != inListPredicate || notEQPredicateType != notEqualPredicate {
		return inPredicate, true
	}
	v := inPredicate.(*expression.ScalarFunction)
	notEQValue := notEQPredicate.(*expression.ScalarFunction).GetArgs()[1].(*expression.Constant)
	// do not simplify != NULL since it is always false.
	if notEQValue.Value.IsNull() {
		return inPredicate, true
	}
	newValues := make([]expression.Expression, 0, len(v.GetArgs()))
	var lastValue *expression.Constant
	for _, element := range v.GetArgs() {
		value, valueOK := element.(*expression.Constant)
		redundantValue := valueOK && value.Equal(ctx.GetExprCtx().GetEvalCtx(), notEQValue)
		if !redundantValue {
			newValues = append(newValues, element)
		}
		if valueOK {
			lastValue = value
		}
	}
	// Special case if all IN list values are prunned. Ideally, this is False condition
	// which can be optimized with LogicalDual. But, this is already done. TODO: the false
	// optimization and its propagation through query tree will be added part of predicate simplification.
	specialCase := false
	if len(newValues) < 2 {
		newValues = append(newValues, lastValue)
		specialCase = true
	}
	newPred := expression.NewFunctionInternal(ctx.GetExprCtx(), v.FuncName.L, v.RetType, newValues...)
	return newPred, specialCase
}

// splitCNF converts AND to list using SplitCNFItems. It is needed since simplification may lead to AND at the top level.
// Several optimizations are based on a list of predicates and AND will block those.
func splitCNF(conditions []expression.Expression) []expression.Expression {
	newConditions := make([]expression.Expression, 0, len(conditions))
	for _, cond := range conditions {
		newConditions = append(newConditions, expression.SplitCNFItems(cond)...)
	}
	return newConditions
}

func applyPredicateSimplification(sctx base.PlanContext, predicates []expression.Expression) []expression.Expression {
	simplifiedPredicate := shortCircuitLogicalConstants(sctx, predicates)
	simplifiedPredicate = mergeInAndNotEQLists(sctx, simplifiedPredicate)
	pruneEmptyORBranches(sctx, simplifiedPredicate)
	simplifiedPredicate = splitCNF(simplifiedPredicate)
	return simplifiedPredicate
}

func mergeInAndNotEQLists(sctx base.PlanContext, predicates []expression.Expression) []expression.Expression {
	if len(predicates) <= 1 {
		return predicates
	}
	specialCase := false
	removeValues := make([]int, 0, len(predicates))
	for i := range predicates {
		for j := i + 1; j < len(predicates); j++ {
			ithPredicate := predicates[i]
			jthPredicate := predicates[j]
			iCol, iType := FindPredicateType(sctx, ithPredicate)
			jCol, jType := FindPredicateType(sctx, jthPredicate)
			if iCol == jCol {
				if iType == notEqualPredicate && jType == inListPredicate {
					predicates[j], specialCase = updateInPredicate(sctx, jthPredicate, ithPredicate)
					sctx.GetSessionVars().StmtCtx.SetSkipPlanCache("NE/INList simplification is triggered")
					if !specialCase {
						removeValues = append(removeValues, i)
					}
				} else if iType == inListPredicate && jType == notEqualPredicate {
					predicates[i], specialCase = updateInPredicate(sctx, ithPredicate, jthPredicate)
					sctx.GetSessionVars().StmtCtx.SetSkipPlanCache("NE/INList simplification is triggered")
					if !specialCase {
						removeValues = append(removeValues, j)
					}
				}
			}
		}
	}
	newValues := make([]expression.Expression, 0, len(predicates))
	for i, value := range predicates {
		if !(slices.Contains(removeValues, i)) {
			newValues = append(newValues, value)
		}
	}
	return newValues
}

// Check for constant false condition.
func unsatisfiableExpression(ctx base.PlanContext, p expression.Expression) bool {
	if constExpr, ok := p.(*expression.Constant); ok {
		if b, err := constExpr.Value.ToBool(ctx.GetSessionVars().StmtCtx.TypeCtx()); err == nil && b == 0 {
			return true
		}
	}
	return false
}

func unsatisfiable(ctx base.PlanContext, p1, p2 expression.Expression) bool {
	var equalPred expression.Expression
	var otherPred expression.Expression
	col1, p1Type := FindPredicateType(ctx, p1)
	col2, p2Type := FindPredicateType(ctx, p2)
	if col1 != col2 || col1 == nil {
		return false
	}
	if p1Type == equalPredicate {
		equalPred = p1
		otherPred = p2
	} else if p2Type == equalPredicate {
		equalPred = p2
		otherPred = p1
	}
	if equalPred == nil || otherPred == nil {
		return false
	}
	// Copy constant from equal predicate into other predicate.
	equalValue := equalPred.(*expression.ScalarFunction)
	otherValue := otherPred.(*expression.ScalarFunction)
	newPred, err := expression.NewFunction(ctx.GetExprCtx(), otherValue.FuncName.L, otherValue.RetType, equalValue.GetArgs()[1], otherValue.GetArgs()[1])
	if err != nil {
		return false
	}
	newPredList := make([]expression.Expression, 0, 1)
	newPredList = append(newPredList, newPred)
	newPredList = expression.PropagateConstant(ctx.GetExprCtx(), newPredList)
	return unsatisfiableExpression(ctx, newPredList[0])
}

func comparisonPred(predType predicateType) predicateType {
	if predType == equalPredicate || predType == lessThanPredicate ||
		predType == greaterThanPredicate || predType == lessThanOrEqualPredicate ||
		predType == greaterThanOrEqualPredicate {
		return scalarPredicate
	}
	return predType
}

// updateOrPredicate simplifies OR predicates by dropping OR predicates if they are empty.
// It is applied for this pattern: P AND (P1 OR P2 ... OR Pn)
// Pi is removed if P & Pi is false/empty.
func updateOrPredicate(ctx base.PlanContext, orPredicateList expression.Expression, scalarPredicatePtr expression.Expression) expression.Expression {
	_, orPredicateType := FindPredicateType(ctx, orPredicateList)
	_, scalarPredicateType := FindPredicateType(ctx, scalarPredicatePtr)
	scalarPredicateType = comparisonPred(scalarPredicateType)
	if orPredicateType != orPredicate || scalarPredicateType != scalarPredicate {
		return orPredicateList
	}
	v := orPredicateList.(*expression.ScalarFunction)
	firstCondition := v.GetArgs()[0]
	secondCondition := v.GetArgs()[1]
	_, firstConditionType := FindPredicateType(ctx, firstCondition)
	_, secondConditionType := FindPredicateType(ctx, secondCondition)
	emptyFirst := false
	emptySecond := false
	if comparisonPred(firstConditionType) == scalarPredicate {
		emptyFirst = unsatisfiable(ctx, firstCondition, scalarPredicatePtr)
	} else if firstConditionType == orPredicate {
		firstCondition = updateOrPredicate(ctx, firstCondition, scalarPredicatePtr)
	}
	if comparisonPred(secondConditionType) == scalarPredicate {
		emptySecond = unsatisfiable(ctx, secondCondition, scalarPredicatePtr)
	} else if secondConditionType == orPredicate {
		secondCondition = updateOrPredicate(ctx, secondCondition, scalarPredicatePtr)
	}
	emptyFirst = emptyFirst || unsatisfiableExpression(ctx, firstCondition)
	emptySecond = emptySecond || unsatisfiableExpression(ctx, secondCondition)
	if emptyFirst && !emptySecond {
		return secondCondition
	} else if !emptyFirst && emptySecond {
		return firstCondition
	} else if emptyFirst && emptySecond {
		return &expression.Constant{Value: types.NewIntDatum(0), RetType: types.NewFieldType(mysql.TypeTiny)}
	}
	newPred, err := expression.NewFunction(ctx.GetExprCtx(), ast.LogicOr, v.RetType, firstCondition, secondCondition)
	if err != nil {
		return orPredicateList
	}
	return newPred
}

// pruneEmptyORBranches applies iteratively updateOrPredicate for each pair of OR predicate
// and another scalar predicate.
func pruneEmptyORBranches(sctx base.PlanContext, predicates []expression.Expression) {
	if len(predicates) <= 1 {
		return
	}
	for i := range predicates {
		for j := i + 1; j < len(predicates); j++ {
			ithPredicate := predicates[i]
			jthPredicate := predicates[j]
			_, iType := FindPredicateType(sctx, ithPredicate)
			_, jType := FindPredicateType(sctx, jthPredicate)
			iType = comparisonPred(iType)
			jType = comparisonPred(jType)
			if iType == scalarPredicate && jType == orPredicate {
				predicates[j] = updateOrPredicate(sctx, jthPredicate, ithPredicate)
				sctx.GetSessionVars().StmtCtx.SetSkipPlanCache("OR predicate simplification is triggered")
			} else if iType == orPredicate && jType == scalarPredicate {
				predicates[i] = updateOrPredicate(sctx, ithPredicate, jthPredicate)
				sctx.GetSessionVars().StmtCtx.SetSkipPlanCache("OR predicate simplification is triggered")
			}
		}
	}
}

// shortCircuitANDORLogicalConstants simplifies logical expressions by performing short-circuit evaluation
// based on the logical AND/OR nature of the predicate and constant truth/falsehood values.
func shortCircuitANDORLogicalConstants(sctx base.PlanContext, predicate expression.Expression, orCase bool) (expression.Expression, bool) {
	con, _ := predicate.(*expression.ScalarFunction)
	args := con.GetArgs()
	firstCondition, secondCondition := args[0], args[1]

	// Recursively process first and second conditions
	firstCondition, firstType := processCondition(sctx, firstCondition)
	secondCondition, secondType := processCondition(sctx, secondCondition)

	switch {
	case firstType == truePredicate && orCase:
		return firstCondition, true
	case secondType == truePredicate && orCase:
		return secondCondition, true
	case firstType == falsePredicate && orCase:
		return secondCondition, true
	case secondType == falsePredicate && orCase:
		return firstCondition, true
	case firstType == truePredicate && !orCase:
		return secondCondition, true
	case secondType == truePredicate && !orCase:
		return firstCondition, true
	case firstType == falsePredicate && !orCase:
		return firstCondition, true
	case secondType == falsePredicate && !orCase:
		return secondCondition, true
	default:
		if firstCondition != args[0] || secondCondition != args[1] {
			finalResult := expression.NewFunctionInternal(sctx.GetExprCtx(), con.FuncName.L, con.GetStaticType(), firstCondition, secondCondition)
			return finalResult, true
		}
		return predicate, false
	}
}

// processCondition handles individual predicate evaluation for logical AND/OR cases
// and returns the potentially simplified condition and its updated type.
func processCondition(sctx base.PlanContext, condition expression.Expression) (expression.Expression, predicateType) {
	applied := false
	_, conditionType := FindPredicateType(sctx, condition)

	if conditionType == orPredicate {
		condition, applied = shortCircuitANDORLogicalConstants(sctx, condition, true)
	} else if conditionType == andPredicate {
		condition, applied = shortCircuitANDORLogicalConstants(sctx, condition, false)
	}

	if applied {
		sctx.GetSessionVars().StmtCtx.SetSkipPlanCache("True/False predicate simplification is triggered")
	}

	_, conditionType = FindPredicateType(sctx, condition)
	return condition, conditionType
}

// shortCircuitLogicalConstants evaluates a list of predicates, applying short-circuit logic
// to simplify the list and eliminate redundant or trivially true/false predicates.
func shortCircuitLogicalConstants(sctx base.PlanContext, predicates []expression.Expression) []expression.Expression {
	finalResult := make([]expression.Expression, 0, len(predicates))

	for _, predicate := range predicates {
		predicate, predicateType := processCondition(sctx, predicate)

		if predicateType == falsePredicate {
			return []expression.Expression{predicate}
		}

		if predicateType != truePredicate {
			finalResult = append(finalResult, predicate)
		}
	}

	return finalResult
}

// Name implements base.LogicalOptRule.<1st> interface.
func (*PredicateSimplification) Name() string {
	return "predicate_simplification"
}
