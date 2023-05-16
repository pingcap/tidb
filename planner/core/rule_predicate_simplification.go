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

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
	"golang.org/x/exp/slices"
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

func (s *predicateSimplification) optimize(_ context.Context, p LogicalPlan, opt *logicalOptimizeOp) (LogicalPlan, error) {
	return p.predicateSimplification(opt), nil
}

func (s *baseLogicalPlan) predicateSimplification(opt *logicalOptimizeOp) LogicalPlan {
	p := s.self
	for i, child := range p.Children() {
		newChild := child.predicateSimplification(opt)
		p.SetChild(i, newChild)
	}
	return p
}

// updateInPredicate applies intersection of an in list with <> value. It returns updated In list and a flag for
// a special case if an element in the inlist is not removed to keep the list not empty.
func updateInPredicate(inPredicate expression.Expression, notEQPredicate expression.Expression) (expression.Expression, bool) {
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
		redundantValue := valueOK && value.Equal(v.GetCtx(), notEQValue)
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
	newPred := expression.NewFunctionInternal(v.GetCtx(), v.FuncName.L, v.RetType, newValues...)
	return newPred, specialCase
}

func applyPredicateSimplification(sctx sessionctx.Context, predicates []expression.Expression) []expression.Expression {
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
					predicates[j], specialCase = updateInPredicate(jthPredicate, ithPredicate)
					sctx.GetSessionVars().StmtCtx.SetSkipPlanCache(errors.New("NE/INList simplification is triggered"))
					if !specialCase {
						removeValues = append(removeValues, i)
					}
				} else if iType == inListPredicate && jType == notEqualPredicate {
					predicates[i], specialCase = updateInPredicate(ithPredicate, jthPredicate)
					sctx.GetSessionVars().StmtCtx.SetSkipPlanCache(errors.New("NE/INList simplification is triggered"))
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

func (s *DataSource) predicateSimplification(opt *logicalOptimizeOp) LogicalPlan {
	p := s.self.(*DataSource)
	p.pushedDownConds = applyPredicateSimplification(p.ctx, p.pushedDownConds)
	p.allConds = applyPredicateSimplification(p.ctx, p.allConds)
	return p
}

func (*predicateSimplification) name() string {
	return "predicate_simplification"
}
