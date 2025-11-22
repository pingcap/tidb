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

package rule

import (
	"context"
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/constraint"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/intest"
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
	// TruePredicate TODO: make it lower case after rule_decorrelate is migrated.
	TruePredicate
	otherPredicate
)

func logicalConstant(bc base.PlanContext, cond expression.Expression) predicateType {
	sc := bc.GetSessionVars().StmtCtx
	con, ok := cond.(*expression.Constant)
	if !ok {
		return otherPredicate
	}
	if expression.MaybeOverOptimized4PlanCache(bc.GetExprCtx(), con) {
		return otherPredicate
	}
	if con.Value.IsNull() {
		return falsePredicate
	}
	isTrue, err := con.Value.ToBool(sc.TypeCtxOrDefault())
	if err == nil {
		if isTrue == 0 {
			return falsePredicate
		}
		return TruePredicate
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
		}
		return nil, otherPredicate
	default:
		return nil, otherPredicate
	}
}

// Optimize implements base.LogicalOptRule.<0th> interface.
func (*PredicateSimplification) Optimize(_ context.Context, p base.LogicalPlan) (base.LogicalPlan, bool, error) {
	planChanged := false
	return p.PredicateSimplification(), planChanged, nil
}

// updateInPredicate applies intersection of an in list with <> value. It returns updated In list and a flag for
// a special case if an element in the inlist is not removed to keep the list not empty.
func updateInPredicate(ctx base.PlanContext, inPredicate expression.Expression, notEQPredicate expression.Expression) (expression.Expression, bool) {
	intest.AssertFunc(func() bool {
		_, inPredicateType := FindPredicateType(ctx, inPredicate)
		_, notEQPredicateType := FindPredicateType(ctx, notEQPredicate)
		return inPredicateType == inListPredicate && notEQPredicateType == notEqualPredicate
	}, "Input's paredicate types are not as expected.")
	v := inPredicate.(*expression.ScalarFunction)
	notEQValue := notEQPredicate.(*expression.ScalarFunction).GetArgs()[1].(*expression.Constant)
	// do not simplify != NULL since it is always false.
	if notEQValue.Value.IsNull() {
		return inPredicate, true
	}
	newValues := make([]expression.Expression, 0, len(v.GetArgs()))
	evalCtx := ctx.GetExprCtx().GetEvalCtx()
	var lastValue *expression.Constant
	for _, element := range v.GetArgs() {
		value, valueOK := element.(*expression.Constant)
		redundantValue := valueOK && value.Equal(evalCtx, notEQValue)
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

func applyPredicateSimplificationForJoin(sctx base.PlanContext, predicates []expression.Expression,
	schema1, schema2 *expression.Schema,
	propagateConstant bool, filter expression.VaildConstantPropagationExpressionFuncType) []expression.Expression {
	return applyPredicateSimplificationHelper(sctx, predicates, schema1, schema2, true, false, propagateConstant, filter)
}

func applyPredicateSimplification(sctx base.PlanContext, predicates []expression.Expression, propagateConstant, isDataSourcePushedDownConds bool,
	vaildConstantPropagationExpressionFunc expression.VaildConstantPropagationExpressionFuncType) []expression.Expression {
	return applyPredicateSimplificationHelper(sctx, predicates, nil, nil,
		false, propagateConstant, isDataSourcePushedDownConds, vaildConstantPropagationExpressionFunc)
}

func applyPredicateSimplificationHelper(sctx base.PlanContext, predicates []expression.Expression,
	schema1, schema2 *expression.Schema, forJoin, propagateConstant, isDataSourcePushedDownConds bool,
	vaildConstantPropagationExpressionFunc expression.VaildConstantPropagationExpressionFuncType) []expression.Expression {
	if len(predicates) == 0 {
		return predicates
	}
	simplifiedPredicate := predicates
	exprCtx := sctx.GetExprCtx()
	simplifiedPredicate = PushDownNot(sctx.GetExprCtx(), simplifiedPredicate)
	// In some scenarios, we need to perform constant propagation,
	// while in others, we merely aim to achieve simplification.
	// Thus, we utilize a switch to govern this particular logic.
	if propagateConstant {
		if forJoin {
			simplifiedPredicate = expression.PropagateConstantForJoin(exprCtx, sctx.GetSessionVars().AlwaysKeepJoinKey,
				schema1, schema2, vaildConstantPropagationExpressionFunc, simplifiedPredicate...)
		} else {
			simplifiedPredicate = expression.PropagateConstant(exprCtx, vaildConstantPropagationExpressionFunc, simplifiedPredicate...)
		}
	} else {
		exprs := expression.PropagateConstant(exprCtx, vaildConstantPropagationExpressionFunc, simplifiedPredicate...)
		if len(exprs) == 1 {
			simplifiedPredicate = exprs
		}
	}
	simplifiedPredicate = shortCircuitLogicalConstants(sctx, simplifiedPredicate, isDataSourcePushedDownConds)
	simplifiedPredicate = mergeInAndNotEQLists(sctx, simplifiedPredicate)
	removeRedundantORBranch(sctx, simplifiedPredicate)
	simplifiedPredicate = pruneEmptyORBranches(sctx, simplifiedPredicate)
	simplifiedPredicate = constraint.DeleteTrueExprs(exprCtx, sctx.GetSessionVars().StmtCtx, simplifiedPredicate)
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
			maybeOverOptimized4PlanCache := expression.MaybeOverOptimized4PlanCache(
				sctx.GetExprCtx(),
				ithPredicate,
				jthPredicate)
			if iCol.Equals(jCol) {
				if iType == notEqualPredicate && jType == inListPredicate {
					predicates[j], specialCase = updateInPredicate(sctx, jthPredicate, ithPredicate)
					if maybeOverOptimized4PlanCache {
						sctx.GetSessionVars().StmtCtx.SetSkipPlanCache("NE/INList simplification is triggered")
					}
					if !specialCase {
						removeValues = append(removeValues, i)
					}
				} else if iType == inListPredicate && jType == notEqualPredicate {
					predicates[i], specialCase = updateInPredicate(sctx, ithPredicate, jthPredicate)
					if maybeOverOptimized4PlanCache {
						sctx.GetSessionVars().StmtCtx.SetSkipPlanCache("NE/INList simplification is triggered")
					}
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
	sc := ctx.GetSessionVars().StmtCtx
	return logicalop.IsConstFalse(sc, p)
}

func unsatisfiable(ctx base.PlanContext, p1, p2 expression.Expression) bool {
	var equalPred expression.Expression
	var otherPred expression.Expression
	col1, p1Type := FindPredicateType(ctx, p1)
	col2, p2Type := FindPredicateType(ctx, p2)
	if col1 == nil || !col1.Equals(col2) {
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
	equalValueConst, ok1 := equalValue.GetArgs()[1].(*expression.Constant)
	otherValueConst, ok2 := otherValue.GetArgs()[1].(*expression.Constant)
	if ok1 && ok2 {
		evalCtx := ctx.GetExprCtx().GetEvalCtx()
		// We have checked the equivalence between col1 and col2, so we can safely use col1's collation.
		colCollate := col1.GetType(evalCtx).GetCollate()
		// Different connection collations can affect the results here, leading to different simplified results and ultimately impacting the execution outcomes.
		// Observing MySQL v8.0.31, this area does not perform string simplification, so we can directly skip it.
		// If the collation is not compatible, we cannot simplify the expression.
		if equalValueType := equalValueConst.GetType(evalCtx); equalValueType.EvalType() == types.ETString &&
			!collate.CompatibleCollate(equalValueType.GetCollate(), colCollate) {
			return false
		}
		if otherValueType := otherValueConst.GetType(evalCtx); otherValueType.EvalType() == types.ETString &&
			!collate.CompatibleCollate(otherValueType.GetCollate(), colCollate) {
			return false
		}
		newPred, err := expression.NewFunction(ctx.GetExprCtx(), otherValue.FuncName.L, otherValue.RetType, equalValueConst, otherValueConst)
		if err != nil {
			return false
		}
		newPredList := expression.PropagateConstant(ctx.GetExprCtx(), nil, newPred)
		return unsatisfiableExpression(ctx, newPredList[0])
	}
	return false
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
//
// The second bool parameter in the return indicates whether the OR expression has changed.
// If this is a parameter with a param marker, the plan cache needs to be skipped to avoid issues.
func updateOrPredicate(ctx base.PlanContext, orPredicateList expression.Expression, scalarPredicatePtr expression.Expression) (expression.Expression, bool) {
	_, orPredicateType := FindPredicateType(ctx, orPredicateList)
	_, scalarPredicateType := FindPredicateType(ctx, scalarPredicatePtr)
	scalarPredicateType = comparisonPred(scalarPredicateType)
	if orPredicateType != orPredicate || scalarPredicateType != scalarPredicate {
		return orPredicateList, false
	}
	v := orPredicateList.(*expression.ScalarFunction)
	firstCondition := v.GetArgs()[0]
	secondCondition := v.GetArgs()[1]
	_, firstConditionType := FindPredicateType(ctx, firstCondition)
	_, secondConditionType := FindPredicateType(ctx, secondCondition)
	emptyFirst := false
	emptySecond := false
	var isFirstConditionChanged, isSecondConditionChanged bool
	if comparisonPred(firstConditionType) == scalarPredicate {
		emptyFirst = unsatisfiable(ctx, firstCondition, scalarPredicatePtr)
	} else if firstConditionType == orPredicate {
		firstCondition, isFirstConditionChanged = updateOrPredicate(ctx, firstCondition, scalarPredicatePtr)
	}
	if comparisonPred(secondConditionType) == scalarPredicate {
		emptySecond = unsatisfiable(ctx, secondCondition, scalarPredicatePtr)
	} else if secondConditionType == orPredicate {
		secondCondition, isSecondConditionChanged = updateOrPredicate(ctx, secondCondition, scalarPredicatePtr)
	}
	emptyFirst = emptyFirst || unsatisfiableExpression(ctx, firstCondition)
	emptySecond = emptySecond || unsatisfiableExpression(ctx, secondCondition)
	if emptyFirst && !emptySecond {
		return secondCondition, true
	} else if !emptyFirst && emptySecond {
		return firstCondition, true
	} else if emptyFirst && emptySecond {
		return &expression.Constant{Value: types.NewIntDatum(0), RetType: types.NewFieldType(mysql.TypeTiny)}, true
	}
	newPred, err := expression.NewFunction(ctx.GetExprCtx(), ast.LogicOr, v.RetType, firstCondition, secondCondition)
	if err != nil {
		return orPredicateList, false
	}
	return newPred, isFirstConditionChanged || isSecondConditionChanged
}

// pruneEmptyORBranches applies iteratively updateOrPredicate for each pair of OR predicate
// and another scalar predicate.
func pruneEmptyORBranches(sctx base.PlanContext, predicates []expression.Expression) []expression.Expression {
	if len(predicates) <= 1 {
		return predicates
	}
	exprCtx := sctx.GetExprCtx()
	for i := range predicates {
		for j := i + 1; j < len(predicates); j++ {
			ithPredicate := predicates[i]
			jthPredicate := predicates[j]
			_, iType := FindPredicateType(sctx, ithPredicate)
			_, jType := FindPredicateType(sctx, jthPredicate)
			iType = comparisonPred(iType)
			jType = comparisonPred(jType)
			var isChanged bool
			if iType == scalarPredicate && jType == orPredicate {
				maybeOverOptimized4PlanCache := expression.MaybeOverOptimized4PlanCache(
					exprCtx,
					jthPredicate)
				predicates[j], isChanged = updateOrPredicate(sctx, jthPredicate, ithPredicate)
				if maybeOverOptimized4PlanCache && isChanged {
					sctx.GetSessionVars().StmtCtx.SetSkipPlanCache("OR predicate simplification is triggered")
				}
				if unsatisfiableExpression(sctx, predicates[j]) {
					return []expression.Expression{predicates[j]}
				}
			} else if iType == orPredicate && jType == scalarPredicate {
				maybeOverOptimized4PlanCache := expression.MaybeOverOptimized4PlanCache(
					exprCtx,
					ithPredicate)
				predicates[i], isChanged = updateOrPredicate(sctx, ithPredicate, jthPredicate)
				if maybeOverOptimized4PlanCache && isChanged {
					sctx.GetSessionVars().StmtCtx.SetSkipPlanCache("OR predicate simplification is triggered")
				}
				if unsatisfiableExpression(sctx, predicates[i]) {
					return []expression.Expression{predicates[i]}
				}
			}
		}
	}
	return predicates
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
	case firstType == TruePredicate && orCase:
		return firstCondition, true
	case secondType == TruePredicate && orCase:
		return secondCondition, true
	case firstType == falsePredicate && orCase:
		return secondCondition, true
	case secondType == falsePredicate && orCase:
		return firstCondition, true
	case firstType == TruePredicate && !orCase:
		return secondCondition, true
	case secondType == TruePredicate && !orCase:
		return firstCondition, true
	case firstType == falsePredicate && !orCase:
		return firstCondition, true
	case secondType == falsePredicate && !orCase:
		return secondCondition, true
	default:
		if !firstCondition.Equal(sctx.GetExprCtx().GetEvalCtx(), args[0]) || !secondCondition.Equal(sctx.GetExprCtx().GetEvalCtx(), args[1]) {
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
	maybeOverOptimized4PlanCache := expression.MaybeOverOptimized4PlanCache(sctx.GetExprCtx(), condition)
	_, conditionType := FindPredicateType(sctx, condition)

	if conditionType == orPredicate {
		condition, applied = shortCircuitANDORLogicalConstants(sctx, condition, true)
	} else if conditionType == andPredicate {
		condition, applied = shortCircuitANDORLogicalConstants(sctx, condition, false)
	}

	if applied && maybeOverOptimized4PlanCache {
		sctx.GetSessionVars().StmtCtx.SetSkipPlanCache("True/False predicate simplification is triggered")
	}

	_, conditionType = FindPredicateType(sctx, condition)
	return condition, conditionType
}

// shortCircuitLogicalConstants evaluates a list of predicates, applying short-circuit logic
// to simplify the list and eliminate redundant or trivially true/false predicates.
func shortCircuitLogicalConstants(sctx base.PlanContext, predicates []expression.Expression, isDataSourcePushedDownConds bool) []expression.Expression {
	finalResult := make([]expression.Expression, 0, len(predicates))
	exprCtx := sctx.GetExprCtx()
	for _, predicate := range predicates {
		predicate, predicateType := processCondition(sctx, predicate)
		if predicateType == falsePredicate {
			return []expression.Expression{predicate}
		}
		if predicateType != TruePredicate {
			if isDataSourcePushedDownConds {
				// EliminateNoPrecisionCast here can convert query 'cast(c<int> as bigint) = 1' to 'c = 1' to leverage access range.
				finalResult = append(finalResult, expression.EliminateNoPrecisionLossCast(exprCtx, predicate))
			} else {
				finalResult = append(finalResult, predicate)
			}
		}
	}

	return finalResult
}

// removeRedundantORBranch recursively iterates over a list of predicates, try to find OR lists and remove redundant in
// each OR list.
// It modifies the input slice in place.
func removeRedundantORBranch(sctx base.PlanContext, predicates []expression.Expression) {
	for i, predicate := range predicates {
		predicates[i] = recursiveRemoveRedundantORBranch(sctx, predicate)
	}
}

func recursiveRemoveRedundantORBranch(sctx base.PlanContext, predicate expression.Expression) expression.Expression {
	_, tp := FindPredicateType(sctx, predicate)
	if tp != orPredicate {
		return predicate
	}
	orFunc := predicate.(*expression.ScalarFunction)
	orList := expression.SplitDNFItems(orFunc)

	dedupMap := make(map[string]struct{}, len(orList))
	newORList := make([]expression.Expression, 0, len(orList))

	for _, orItem := range orList {
		_, tp := FindPredicateType(sctx, orItem)
		// 1. If it's an AND predicate, we recursively call removeRedundantORBranch() on it.
		if tp == andPredicate {
			andFunc := orItem.(*expression.ScalarFunction)
			andList := expression.SplitCNFItems(andFunc)
			removeRedundantORBranch(sctx, andList)
			newORList = append(newORList, expression.ComposeCNFCondition(sctx.GetExprCtx(), andList...))
		} else {
			// 2. Otherwise, we check if it's a duplicate predicate by checking HashCode().
			hashCode := string(orItem.HashCode())
			// 2-1. If it's not a duplicate, we need to keep this predicate.
			if _, ok := dedupMap[hashCode]; !ok {
				dedupMap[hashCode] = struct{}{}
				newORList = append(newORList, orItem)
			} else if expression.IsMutableEffectsExpr(orItem) {
				// 2-2. If it's a duplicate, but it's nondeterministic or has side effects, we also need to keep it.
				newORList = append(newORList, orItem)
			}
			// 2-3. Otherwise, we remove it.
		}
	}
	return expression.ComposeDNFCondition(sctx.GetExprCtx(), newORList...)
}

// Name implements base.LogicalOptRule.<1st> interface.
func (*PredicateSimplification) Name() string {
	return "predicate_simplification"
}
