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
	scalarPredicate
	otherPredicate
)

func findPredicateType(expr expression.Expression) (*expression.Column, predicateType) {
	switch v := expr.(type) {
	case *expression.ScalarFunction:
		if v.FuncName.L == ast.LogicOr {
			return nil, orPredicate
		}
		args := v.GetArgs()
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
	simplifiedPredicate := mergeInAndNotEQLists(sctx, predicates)
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
			iCol, iType := findPredicateType(ithPredicate)
			jCol, jType := findPredicateType(jthPredicate)
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
	col1, p1Type := findPredicateType(p1)
	col2, p2Type := findPredicateType(p2)
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
	newPred := expression.NewFunctionInternal(ctx.GetExprCtx(), otherValue.FuncName.L, otherValue.RetType, equalValue.GetArgs()[1], otherValue.GetArgs()[1])
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
	_, orPredicateType := findPredicateType(orPredicateList)
	_, scalarPredicateType := findPredicateType(scalarPredicatePtr)
	scalarPredicateType = comparisonPred(scalarPredicateType)
	if orPredicateType != orPredicate || scalarPredicateType != scalarPredicate {
		return orPredicateList
	}
	v := orPredicateList.(*expression.ScalarFunction)
	firstCondition := v.GetArgs()[0]
	secondCondition := v.GetArgs()[1]
	_, firstConditionType := findPredicateType(firstCondition)
	_, secondConditionType := findPredicateType(secondCondition)
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
	return expression.NewFunctionInternal(ctx.GetExprCtx(), ast.LogicOr, v.RetType, firstCondition, secondCondition)
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
			_, iType := findPredicateType(ithPredicate)
			_, jType := findPredicateType(jthPredicate)
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

// Name implements base.LogicalOptRule.<1st> interface.
func (*PredicateSimplification) Name() string {
	return "predicate_simplification"
}
