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
	"errors"
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/util"
)

// predicateSimplification consolidates different predcicates on a column and its equivalence classes.  Initial out is for
// in-list and not equal list intersection.
type predicateSimplification struct {
}

type predicateType = byte

const (
	inListPredicate predicateType = iota
	notEqualPredicate
	otherPredicate
)

func findPredicateType(expr expression.Expression) (*expression.Column, predicateType) {
	switch v := expr.(type) {
	case *expression.ScalarFunction:
		args := v.GetArgs()
		col, colOk := args[0].(*expression.Column)
		if !colOk {
			return nil, otherPredicate
		}
		if v.FuncName.L == ast.NE {
			if _, ok := args[1].(*expression.Constant); !ok {
				return nil, otherPredicate
			}
			return col, notEqualPredicate
		}
		if v.FuncName.L == ast.In {
			for _, value := range args[1:] {
				if _, ok := value.(*expression.Constant); !ok {
					return nil, otherPredicate
				}
			}
			return col, inListPredicate
		}
	default:
		return nil, otherPredicate
	}
	return nil, otherPredicate
}

func (*predicateSimplification) optimize(_ context.Context, p LogicalPlan, opt *util.LogicalOptimizeOp) (LogicalPlan, bool, error) {
	planChanged := false
	return p.predicateSimplification(opt), planChanged, nil
}

func (s *baseLogicalPlan) predicateSimplification(opt *util.LogicalOptimizeOp) LogicalPlan {
	p := s.self
	for i, child := range p.Children() {
		newChild := child.predicateSimplification(opt)
		p.SetChild(i, newChild)
	}
	return p
}

// updateInPredicate applies intersection of an in list with <> value. It returns updated In list and a flag for
// a special case if an element in the inlist is not removed to keep the list not empty.
func updateInPredicate(ctx PlanContext, inPredicate expression.Expression, notEQPredicate expression.Expression) (expression.Expression, bool) {
	_, inPredicateType := findPredicateType(inPredicate)
	_, notEQPredicateType := findPredicateType(notEQPredicate)
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

func applyPredicateSimplification(sctx PlanContext, predicates []expression.Expression) []expression.Expression {
	if len(predicates) <= 1 {
		return predicates
	}
	specialCase := false
	removeValues := make([]int, 0, len(predicates))
	for i := range predicates {
		for j := i + 1; j < len(predicates); j++ {
			ithPredicate := predicates[i]
			jthPredicate := predicates[j]
<<<<<<< HEAD
			iCol, iType := findPredicateType(ithPredicate)
			jCol, jType := findPredicateType(jthPredicate)
			maybeOverOptimized4PlanCache := expression.MaybeOverOptimized4PlanCacheForMultiExpression(
				sctx.GetExprCtx(),
				ithPredicate,
				jthPredicate)
			if iCol == jCol {
=======
			iCol, iType := FindPredicateType(sctx, ithPredicate)
			jCol, jType := FindPredicateType(sctx, jthPredicate)
			maybeOverOptimized4PlanCache := expression.MaybeOverOptimized4PlanCache(
				sctx.GetExprCtx(),
				ithPredicate,
				jthPredicate)
			if iCol.Equals(jCol) {
>>>>>>> 35c1e21115c (planner,expression: fix wrong copy args to avoid breaking origin expression when to EvaluateExprWithNull (#61630))
				if iType == notEqualPredicate && jType == inListPredicate {
					predicates[j], specialCase = updateInPredicate(sctx, jthPredicate, ithPredicate)
					if maybeOverOptimized4PlanCache {
						sctx.GetSessionVars().StmtCtx.SetSkipPlanCache(errors.New("NE/INList simplification is triggered"))
					}
					if !specialCase {
						removeValues = append(removeValues, i)
					}
				} else if iType == inListPredicate && jType == notEqualPredicate {
					predicates[i], specialCase = updateInPredicate(sctx, ithPredicate, jthPredicate)
					if maybeOverOptimized4PlanCache {
						sctx.GetSessionVars().StmtCtx.SetSkipPlanCache(errors.New("NE/INList simplification is triggered"))
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

func (ds *DataSource) predicateSimplification(*util.LogicalOptimizeOp) LogicalPlan {
	p := ds.self.(*DataSource)
	p.pushedDownConds = applyPredicateSimplification(p.SCtx(), p.pushedDownConds)
	p.allConds = applyPredicateSimplification(p.SCtx(), p.allConds)
	return p
}

<<<<<<< HEAD
func (*predicateSimplification) name() string {
=======
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
			maybeOverOptimized4PlanCache := expression.MaybeOverOptimized4PlanCache(
				sctx.GetExprCtx(),
				ithPredicate,
				jthPredicate)
			if iType == scalarPredicate && jType == orPredicate {
				predicates[j] = updateOrPredicate(sctx, jthPredicate, ithPredicate)
				if maybeOverOptimized4PlanCache {
					sctx.GetSessionVars().StmtCtx.SetSkipPlanCache("OR predicate simplification is triggered")
				}
			} else if iType == orPredicate && jType == scalarPredicate {
				predicates[i] = updateOrPredicate(sctx, ithPredicate, jthPredicate)
				if maybeOverOptimized4PlanCache {
					sctx.GetSessionVars().StmtCtx.SetSkipPlanCache("OR predicate simplification is triggered")
				}
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
>>>>>>> 35c1e21115c (planner,expression: fix wrong copy args to avoid breaking origin expression when to EvaluateExprWithNull (#61630))
	return "predicate_simplification"
}
