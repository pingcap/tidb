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
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// PredicateSimplification performs logical rewrites to consolidate and simplify
// predicates on columns and their equivalence classes. It applies the following:
//
// 1. Equivalence-class analysis
//   - Build equivalence classes from column equalities (e.g., t.a = tt.a).
//     These are used by later rules to reason across interchangeable columns.
//
// 2. Normalize IN-lists into OR-lists
//   - Convert IN-lists (col IN (...)) into explicit OR-lists
//     so that all simplification rules apply uniformly.
//
// 3. In-list / not-equal intersection
//   - Remove values from IN-lists if excluded by NOT EQUAL or inequality constraints.
//
// 4. Empty-branch pruning
//   - For patterns of the form:  P AND (P1 OR P2 ... OR Pn)
//     drop disjuncts Pi if (P AND Pi) is unsatisfiable/false (using equivalence info).
//
// 5. Constant folding
//   - Simplify predicates involving logical constants (TRUE / FALSE).
//
// 6. Fold OR-lists of equalities back into IN-lists
//   - If an OR-list is of the form (col = c1) OR (col = c2) OR ...,
//     rewrite as col IN (c1, c2, ...).
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

// simplifyContext holds shared state for predicate simplification.
// - planContext: planner context for expression evaluation.
// - ColEqClasses: column equivalence classes from equalities (e.g. t.a = tt.a).
type simplifyContext struct {
	planContext  base.PlanContext
	ColEqClasses [][]*expression.Column
}

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
func (*PredicateSimplification) Optimize(_ context.Context, p base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	planChanged := false
	return p.PredicateSimplification(opt), planChanged, nil
}

const maxInListToExpand = 50

// inListToOrList rewrites a predicate of the form
//
//	col IN (c1, c2, ..., cn)
//
// into
//
//	(col = c1 OR col = c2 OR ... OR col = cn).
//
// If the IN list has a single constant, it's simplified to col = c1.
// Only applies if:
//   - The left-hand side is a Column
//   - All list items are constant expressions
//
// Otherwise, it returns the original expression.

// Objective of this function:
//
// 1. Ensure that all simplification rules apply uniformly to both OR-lists
//    and IN-lists by normalizing IN into OR form first.
//
// 2. Avoid regressions caused by rewriting OR into IN too early. For example:
//      SELECT 1 FROM t1 JOIN t2 ON a1 = a2
//      WHERE (a1 = 1 OR a1 = 2 OR a1 = 3) AND a2 > 2
//
//    → rewritten early as:
//      SELECT 1 FROM t1 JOIN t2 ON a1 = a2
//      WHERE a1 IN (1,2,3) AND a2 > 2
//
//    → after constant propagation, this becomes:
//      SELECT 1 FROM t1 JOIN t2 ON a1 = a2
//      WHERE a1 IN (1,2,3) AND a2 > 2 AND a1 > 2 AND a2 IN (1,2,3)
//
//    When the second round of predicate simplification is called,
//    pruneEmptyORBranches cannot simplify this form any further.
//    If the predicate had remained in IN-list form, it could have been reduced
//    to just "a1 = 3" and "a2 = 3".

func inListToOrList(ctx base.PlanContext, expr expression.Expression) expression.Expression {
	sf, ok := expr.(*expression.ScalarFunction)
	if !ok || sf.FuncName.L != ast.In {
		return expr
	}

	// First argument should be the column
	col, ok := sf.GetArgs()[0].(*expression.Column)
	if !ok {
		return expr
	}

	// Quick size check: expanding a large IN-list into an OR-tree can cause significant overhead.
	// If the number of values exceeds maxInListToExpand, skip the rewrite.
	if len(sf.GetArgs())-1 > maxInListToExpand {
		return expr
	}

	// Ensure all other arguments are constants
	constants := make([]*expression.Constant, 0, len(sf.GetArgs())-1)
	for _, arg := range sf.GetArgs()[1:] {
		c, ok := arg.(*expression.Constant)
		if !ok {
			return expr
		}
		constants = append(constants, c)
	}

	// Single constant → just col = constant
	if len(constants) == 1 {
		return expression.NewFunctionInternal(ctx.GetExprCtx(),
			ast.EQ, sf.RetType, col, constants[0])
	}

	// Multiple constants → expand into OR of equalities
	orExpr := expression.NewFunctionInternal(ctx.GetExprCtx(),
		ast.EQ, sf.RetType, col, constants[0])
	for _, c := range constants[1:] {
		eq := expression.NewFunctionInternal(ctx.GetExprCtx(),
			ast.EQ, sf.RetType, col, c)
		orExpr = expression.NewFunctionInternal(ctx.GetExprCtx(),
			ast.LogicOr, sf.RetType, orExpr, eq)
	}
	return orExpr
}

// updateInPredicate applies intersection of an in list with <> value. It returns updated In list and a flag for
// a special case if an element in the inlist is not removed to keep the list not empty.
func updateInPredicate(ctx simplifyContext, inPredicate expression.Expression, notEQPredicate expression.Expression) (expression.Expression, bool) {
	intest.AssertFunc(func() bool {
		_, inPredicateType := FindPredicateType(ctx.planContext, inPredicate)
		_, notEQPredicateType := FindPredicateType(ctx.planContext, notEQPredicate)
		return inPredicateType == inListPredicate && notEQPredicateType == notEqualPredicate
	}, "Input's paredicate types are not as expected.")
	v := inPredicate.(*expression.ScalarFunction)
	notEQValue := notEQPredicate.(*expression.ScalarFunction).GetArgs()[1].(*expression.Constant)
	// do not simplify != NULL since it is always false.
	if notEQValue.Value.IsNull() {
		return inPredicate, true
	}
	newValues := make([]expression.Expression, 0, len(v.GetArgs()))
	evalCtx := ctx.planContext.GetExprCtx().GetEvalCtx()
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
	newPred := expression.NewFunctionInternal(ctx.planContext.GetExprCtx(), v.FuncName.L, v.RetType, newValues...)
	return newPred, specialCase
}

func applyPredicateSimplification(sctx base.PlanContext, predicates []expression.Expression, propagateConstant bool,
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
		simplifiedPredicate = expression.PropagateConstant(exprCtx, vaildConstantPropagationExpressionFunc, simplifiedPredicate...)
	} else {
		exprs := expression.PropagateConstant(exprCtx, vaildConstantPropagationExpressionFunc, simplifiedPredicate...)
		if len(exprs) == 1 {
			simplifiedPredicate = exprs
		}
	}

	eqClasses := buildEquivalenceClasses(simplifiedPredicate)
	ctx := simplifyContext{
		planContext:  sctx,
		ColEqClasses: eqClasses,
	}

	simplifiedPredicate = inListToOrListAll(sctx, simplifiedPredicate)
	simplifiedPredicate = shortCircuitLogicalConstants(ctx, simplifiedPredicate)
	simplifiedPredicate = orList2InListAll(ctx, simplifiedPredicate)
	simplifiedPredicate = mergeInAndNotEQLists(ctx, simplifiedPredicate)
	simplifiedPredicate = inListToOrListAll(sctx, simplifiedPredicate)
	removeRedundantORBranch(ctx, simplifiedPredicate)
	simplifiedPredicate = pruneEmptyORBranches(ctx, simplifiedPredicate)
	simplifiedPredicate = constraint.DeleteTrueExprs(exprCtx, sctx.GetSessionVars().StmtCtx, simplifiedPredicate)
	simplifiedPredicate = orList2InListAll(ctx, simplifiedPredicate)
	return simplifiedPredicate
}

func mergeInAndNotEQLists(ctx simplifyContext, predicates []expression.Expression) []expression.Expression {
	sctx := ctx.planContext
	if len(predicates) <= 1 {
		return predicates
	}
	specialCase := false
	removeValues := make([]int, 0, len(predicates))
	for i := range predicates {
		for j := i + 1; j < len(predicates); j++ {
			ithPredicate := predicates[i]
			jthPredicate := predicates[j]
			iCol, iType := FindPredicateType(ctx.planContext, ithPredicate)
			jCol, jType := FindPredicateType(ctx.planContext, jthPredicate)
			maybeOverOptimized4PlanCache := expression.MaybeOverOptimized4PlanCache(
				sctx.GetExprCtx(),
				ithPredicate,
				jthPredicate)
			if iCol.Equals(jCol) {
				if iType == notEqualPredicate && jType == inListPredicate {
					predicates[j], specialCase = updateInPredicate(ctx, jthPredicate, ithPredicate)
					if maybeOverOptimized4PlanCache {
						sctx.GetSessionVars().StmtCtx.SetSkipPlanCache("NE/INList simplification is triggered")
					}
					if !specialCase {
						removeValues = append(removeValues, i)
					}
				} else if iType == inListPredicate && jType == notEqualPredicate {
					predicates[i], specialCase = updateInPredicate(ctx, ithPredicate, jthPredicate)
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

// colsEquivalent returns true if the two columns are the same
// (according to Equals) or if they belong to the same equivalence class.
func colsEquivalent(ctx simplifyContext, a, b *expression.Column) bool {
	if a == nil || b == nil {
		return false
	}
	if a.Equals(b) {
		return true
	}
	return sameEquivalenceClass(a, b, ctx.ColEqClasses)
}

// Check for constant false condition.
func unsatisfiableExpression(ctx simplifyContext, p expression.Expression) bool {
	sc := ctx.planContext.GetSessionVars().StmtCtx
	return logicalop.IsConstFalse(sc, p)
}

func unsatisfiable(ctx simplifyContext, p1, p2 expression.Expression) bool {
	var equalPred expression.Expression
	var otherPred expression.Expression
	col1, p1Type := FindPredicateType(ctx.planContext, p1)
	col2, p2Type := FindPredicateType(ctx.planContext, p2)
	if col1 == nil || !colsEquivalent(ctx, col1, col2) {
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
		evalCtx := ctx.planContext.GetExprCtx().GetEvalCtx()
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
		newPred, err := expression.NewFunction(ctx.planContext.GetExprCtx(), otherValue.FuncName.L, otherValue.RetType, equalValueConst, otherValueConst)
		if err != nil {
			return false
		}
		newPredList := expression.PropagateConstant(ctx.planContext.GetExprCtx(), nil, newPred)
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
func updateOrPredicate(ctx simplifyContext, orPredicateList expression.Expression, scalarPredicatePtr expression.Expression) expression.Expression {
	_, orPredicateType := FindPredicateType(ctx.planContext, orPredicateList)
	_, scalarPredicateType := FindPredicateType(ctx.planContext, scalarPredicatePtr)
	scalarPredicateType = comparisonPred(scalarPredicateType)
	if orPredicateType != orPredicate || scalarPredicateType != scalarPredicate {
		return orPredicateList
	}
	v := orPredicateList.(*expression.ScalarFunction)
	firstCondition := v.GetArgs()[0]
	secondCondition := v.GetArgs()[1]
	_, firstConditionType := FindPredicateType(ctx.planContext, firstCondition)
	_, secondConditionType := FindPredicateType(ctx.planContext, secondCondition)
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
	newPred, err := expression.NewFunction(ctx.planContext.GetExprCtx(), ast.LogicOr, v.RetType, firstCondition, secondCondition)
	if err != nil {
		return orPredicateList
	}
	return newPred
}

// pruneEmptyORBranches applies iteratively updateOrPredicate for each pair of OR predicate
// and another scalar predicate.
func pruneEmptyORBranches(ctx simplifyContext, predicates []expression.Expression) []expression.Expression {
	if len(predicates) <= 1 {
		return predicates
	}
	for i := range predicates {
		for j := i + 1; j < len(predicates); j++ {
			ithPredicate := predicates[i]
			jthPredicate := predicates[j]
			_, iType := FindPredicateType(ctx.planContext, ithPredicate)
			_, jType := FindPredicateType(ctx.planContext, jthPredicate)
			iType = comparisonPred(iType)
			jType = comparisonPred(jType)
			maybeOverOptimized4PlanCache := expression.MaybeOverOptimized4PlanCache(
				ctx.planContext.GetExprCtx(),
				ithPredicate,
				jthPredicate)
			if iType == scalarPredicate && jType == orPredicate {
				orPredicateOriginalSize := len(expression.SplitDNFItems(predicates[j]))
				predicates[j] = updateOrPredicate(ctx, jthPredicate, ithPredicate)
				orPredicateNewSize := len(expression.SplitDNFItems(predicates[j]))
				applied := orPredicateOriginalSize > orPredicateNewSize
				if maybeOverOptimized4PlanCache && applied {
					ctx.planContext.GetSessionVars().StmtCtx.SetSkipPlanCache("OR predicate simplification is triggered")
				}
				if unsatisfiableExpression(ctx, predicates[j]) {
					return []expression.Expression{predicates[j]}
				}
			} else if iType == orPredicate && jType == scalarPredicate {
				orPredicateOriginalSize := len(expression.SplitDNFItems(predicates[i]))
				predicates[i] = updateOrPredicate(ctx, ithPredicate, jthPredicate)
				orPredicateNewSize := len(expression.SplitDNFItems(predicates[i]))
				applied := orPredicateOriginalSize > orPredicateNewSize
				if maybeOverOptimized4PlanCache && applied {
					ctx.planContext.GetSessionVars().StmtCtx.SetSkipPlanCache("OR predicate simplification is triggered")
				}
				if unsatisfiableExpression(ctx, predicates[i]) {
					return []expression.Expression{predicates[i]}
				}
			}
		}
	}
	return predicates
}

// shortCircuitANDORLogicalConstants simplifies logical expressions by performing short-circuit evaluation
// based on the logical AND/OR nature of the predicate and constant truth/falsehood values.
func shortCircuitANDORLogicalConstants(ctx simplifyContext, predicate expression.Expression, orCase bool) (expression.Expression, bool) {
	con, _ := predicate.(*expression.ScalarFunction)
	args := con.GetArgs()
	firstCondition, secondCondition := args[0], args[1]

	// Recursively process first and second conditions
	firstCondition, firstType := processCondition(ctx, firstCondition)
	secondCondition, secondType := processCondition(ctx, secondCondition)

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
		if !firstCondition.Equal(ctx.planContext.GetExprCtx().GetEvalCtx(), args[0]) || !secondCondition.Equal(ctx.planContext.GetExprCtx().GetEvalCtx(), args[1]) {
			finalResult := expression.NewFunctionInternal(ctx.planContext.GetExprCtx(), con.FuncName.L, con.GetStaticType(), firstCondition, secondCondition)
			return finalResult, true
		}
		return predicate, false
	}
}

// processCondition handles individual predicate evaluation for logical AND/OR cases
// and returns the potentially simplified condition and its updated type.
func processCondition(ctx simplifyContext, condition expression.Expression) (expression.Expression, predicateType) {
	applied := false
	maybeOverOptimized4PlanCache := expression.MaybeOverOptimized4PlanCache(ctx.planContext.GetExprCtx(), condition)
	_, conditionType := FindPredicateType(ctx.planContext, condition)

	if conditionType == orPredicate {
		condition, applied = shortCircuitANDORLogicalConstants(ctx, condition, true)
	} else if conditionType == andPredicate {
		condition, applied = shortCircuitANDORLogicalConstants(ctx, condition, false)
	}

	if applied && maybeOverOptimized4PlanCache {
		ctx.planContext.GetSessionVars().StmtCtx.SetSkipPlanCache("True/False predicate simplification is triggered")
	}

	_, conditionType = FindPredicateType(ctx.planContext, condition)
	return condition, conditionType
}

// shortCircuitLogicalConstants evaluates a list of predicates, applying short-circuit logic
// to simplify the list and eliminate redundant or trivially true/false predicates.
func shortCircuitLogicalConstants(ctx simplifyContext, predicates []expression.Expression) []expression.Expression {
	finalResult := make([]expression.Expression, 0, len(predicates))

	for _, predicate := range predicates {
		predicate, predicateType := processCondition(ctx, predicate)

		if predicateType == falsePredicate {
			return []expression.Expression{predicate}
		}

		if predicateType != TruePredicate {
			finalResult = append(finalResult, predicate)
		}
	}

	return finalResult
}

// removeRedundantORBranch recursively iterates over a list of predicates, try to find OR lists and remove redundant in
// each OR list.
// It modifies the input slice in place.
func removeRedundantORBranch(ctx simplifyContext, predicates []expression.Expression) {
	for i, predicate := range predicates {
		predicates[i] = recursiveRemoveRedundantORBranch(ctx, predicate)
	}
}

func recursiveRemoveRedundantORBranch(ctx simplifyContext, predicate expression.Expression) expression.Expression {
	_, tp := FindPredicateType(ctx.planContext, predicate)
	if tp != orPredicate {
		return predicate
	}
	orFunc := predicate.(*expression.ScalarFunction)
	orList := expression.SplitDNFItems(orFunc)
	maybeOverOptimized4PlanCache := expression.MaybeOverOptimized4PlanCache(ctx.planContext.GetExprCtx(), predicate)

	dedupMap := make(map[string]struct{}, len(orList))
	newORList := make([]expression.Expression, 0, len(orList))

	for _, orItem := range orList {
		// Skip "col = NULL" cases
		if sf, ok := orItem.(*expression.ScalarFunction); ok && sf.FuncName.L == ast.EQ {
			if len(sf.GetArgs()) == 2 {
				if constArg, ok := sf.GetArgs()[1].(*expression.Constant); ok && constArg.Value.IsNull() {
					if maybeOverOptimized4PlanCache {
						ctx.planContext.GetSessionVars().StmtCtx.SetSkipPlanCache("OR simplification is triggered")
					}
					continue
				}
			}
		}
		_, tp := FindPredicateType(ctx.planContext, orItem)
		// 1. If it's an AND predicate, we recursively call removeRedundantORBranch() on it.
		if tp == andPredicate {
			andFunc := orItem.(*expression.ScalarFunction)
			andList := expression.SplitCNFItems(andFunc)
			removeRedundantORBranch(ctx, andList)
			newORList = append(newORList, expression.ComposeCNFCondition(ctx.planContext.GetExprCtx(), andList...))
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

	// If no OR branches remain, return FALSE constant
	if len(newORList) == 0 {
		if maybeOverOptimized4PlanCache {
			ctx.planContext.GetSessionVars().StmtCtx.SetSkipPlanCache("OR simplification is triggered")
		}
		return &expression.Constant{
			Value:   types.NewDatum(false),
			RetType: types.NewFieldType(mysql.TypeTiny),
		}
	}

	return expression.ComposeDNFCondition(ctx.planContext.GetExprCtx(), newORList...)
}

// Name implements base.LogicalOptRule.<1st> interface.
func (*PredicateSimplification) Name() string {
	return "predicate_simplification"
}

// orList2InList scans a DNF OR-list and rewrites
// (col = const1 OR col = const2 OR ...)
// into (col IN (const1,const2,...)) if all disjuncts
// are equalities on the same column with constants.
func orList2InList(ctx simplifyContext, expr expression.Expression) expression.Expression {
	// Only process OR predicates
	_, tp := FindPredicateType(ctx.planContext, expr)
	if tp != orPredicate {
		return expr
	}

	// Break down OR into DNF items
	orFunc := expr.(*expression.ScalarFunction)
	orList := expression.SplitDNFItems(orFunc)

	var col *expression.Column
	constants := make([]expression.Expression, 0, len(orList))

	for _, orItem := range orList {
		c, predType := FindPredicateType(ctx.planContext, orItem)
		// Must be an equality with a valid column
		if predType != equalPredicate || c == nil {
			return expr
		}

		// All equalities must be on the same column
		if col == nil {
			col = c
		} else if !col.Equals(c) {
			return expr
		}

		// RHS must be a constant
		sf := orItem.(*expression.ScalarFunction)
		constVal, ok := sf.GetArgs()[1].(*expression.Constant)
		if !ok {
			return expr
		}
		constants = append(constants, constVal)
	}

	// Guard: if no valid column or no constants found, abort
	if col == nil || len(constants) == 0 {
		return expr
	}

	// Build new "col IN (const1, const2, ...)" expression
	args := make([]expression.Expression, 0, len(constants)+1)
	args = append(args, col)
	args = append(args, constants...)
	newPred, err := expression.NewFunctionBase(
		ctx.planContext.GetExprCtx(),
		ast.In,
		types.NewFieldType(mysql.TypeTiny),
		args...,
	)
	if err != nil {
		// If building the new IN predicate fails, return the original expression
		return expr
	}
	return newPred
}

// orList2InListAll iterates over a slice of predicates
// and applies orList2InList() on each element in place.
func orList2InListAll(ctx simplifyContext, predicates []expression.Expression) []expression.Expression {
	for i, predicate := range predicates {
		predicates[i] = orList2InList(ctx, predicate)
	}
	return predicates
}

// buildEquivalenceClasses groups columns into equivalence classes based on col=col predicates.
// Example: t.a = tt.a, tt.a = t.b => {t.a, tt.a, t.b}
func buildEquivalenceClasses(predicates []expression.Expression) [][]*expression.Column {
	adj := make(map[int64][]*expression.Column)
	cols := make(map[int64]*expression.Column)

	for _, pred := range predicates {
		sf, ok := pred.(*expression.ScalarFunction)
		if !ok || sf.FuncName.L != ast.EQ {
			continue
		}
		lCol, lok := sf.GetArgs()[0].(*expression.Column)
		rCol, rok := sf.GetArgs()[1].(*expression.Column)
		if !lok || !rok {
			continue
		}
		adj[lCol.UniqueID] = append(adj[lCol.UniqueID], rCol)
		adj[rCol.UniqueID] = append(adj[rCol.UniqueID], lCol)
		cols[lCol.UniqueID] = lCol
		cols[rCol.UniqueID] = rCol
	}

	visited := make(map[int64]bool)
	var result [][]*expression.Column
	for id, col := range cols {
		if visited[id] {
			continue
		}
		var stack = []*expression.Column{col}
		var group []*expression.Column
		for len(stack) > 0 {
			c := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if visited[c.UniqueID] {
				continue
			}
			visited[c.UniqueID] = true
			group = append(group, c)
			for _, nei := range adj[c.UniqueID] {
				if !visited[nei.UniqueID] {
					stack = append(stack, nei)
				}
			}
		}
		if len(group) > 1 {
			result = append(result, group)
		}
	}
	return result
}

// sameEquivalenceClass returns true if columns a and b belong to the same equivalence class.
func sameEquivalenceClass(a, b *expression.Column, classes [][]*expression.Column) bool {
	if a == nil || b == nil {
		return false
	}
	for _, group := range classes {
		foundA, foundB := false, false
		for _, col := range group {
			if col.UniqueID == a.UniqueID {
				foundA = true
			}
			if col.UniqueID == b.UniqueID {
				foundB = true
			}
		}
		if foundA && foundB {
			return true
		}
	}
	return false
}

// inListToOrListAll applies inListToOrList on a slice of predicates.
// It normalizes any "col IN (...)" into explicit OR-form so that
// all subsequent simplification rules can work uniformly.
// Example:
//
//	[ col1 IN (1,2), col2 = 5 ]
//
// → [ (col1 = 1 OR col1 = 2), col2 = 5 ]
func inListToOrListAll(ctx base.PlanContext, exprs []expression.Expression) []expression.Expression {
	newExprs := make([]expression.Expression, len(exprs))
	for i, e := range exprs {
		newExprs[i] = inListToOrList(ctx, e)
	}
	return newExprs
}
