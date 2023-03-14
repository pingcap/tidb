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

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/ast"
)

// predicateSimplification consolidates different predcicates on a column and its equivalence classes.  Initial out is for
// in-list and not equal list intersection.
type predicateSimplification struct {
}

type PredicateType = byte

const (
	InPredicate       PredicateType = 0x00
	NotEqualPredicate PredicateType = 0x01
	OtherPredicate    PredicateType = 0x02
)

func findPredicateType(expr expression.Expression) (*expression.Column, PredicateType) {
	switch v := expr.(type) {
	case *expression.ScalarFunction:
		args := v.GetArgs()
		col, colOk := args[0].(*expression.Column)
		if !colOk {
			return nil, OtherPredicate
		}
		if v.FuncName.L == ast.NE {
			if _, ok := args[1].(*expression.Constant); !ok {
				return nil, OtherPredicate
			} else {
				return col, NotEqualPredicate
			}
		}
		if v.FuncName.L == ast.In {
			for _, value := range args[1:] {
				if _, ok := value.(*expression.Constant); !ok {
					return nil, OtherPredicate
				}
			}
			return col, InPredicate
		}
	default:
		return nil, OtherPredicate
	}
	return nil, OtherPredicate
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

func updateInPredicate(inPredicate expression.Expression, notEQPredicate expression.Expression) expression.Expression {
	_, inPredicateType := findPredicateType(inPredicate)
	_, notEQPredicateType := findPredicateType(notEQPredicate)
	if inPredicateType != InPredicate || notEQPredicateType != NotEqualPredicate {
		return inPredicate
	}
	v := inPredicate.(*expression.ScalarFunction)
	notEQValue := notEQPredicate.(*expression.ScalarFunction).GetArgs()[1].(*expression.Constant)
	newValues := make([]expression.Expression, 0, len(v.GetArgs()))
	var lastValue *expression.Constant
	for _, element := range v.GetArgs() {
		value, valueOK := element.(*expression.Constant)
		redudantValue := valueOK && value.Equal(v.GetCtx(), notEQValue)
		if !redudantValue {
			newValues = append(newValues, element)
		}
		if valueOK {
			lastValue = value
		}
	}
	// Special case if all IN list values are prunned. Ideally, this is False condition
	// which can be optimized with LogicalDual. But, this ia already done. TODO: the false
	// optimization and its propagation through query tree will be added part of predicate simplification.
	if len(newValues) < 2 {
		newValues = append(newValues, lastValue)
	}
	newPred := expression.NewFunctionInternal(v.GetCtx(), v.FuncName.L, v.RetType, newValues...)
	return newPred
}

func indexInSlice(index int, list []int) bool {
	for _, v := range list {
		if v == index {
			return true
			break
		}
	}
	return false
}

func applyPredicateSimplification(predicates []expression.Expression) []expression.Expression {
	if len(predicates) <= 1 {
		return predicates
	}
	removeValues := make([]int, 0, len(predicates))
	for i := range predicates {
		for j := i + 1; j < len(predicates); j++ {
			ithPredicate := predicates[i]
			jthPredicate := predicates[j]
			iCol, iType := findPredicateType(ithPredicate)
			jCol, jType := findPredicateType(jthPredicate)
			if iCol == jCol {
				if iType == NotEqualPredicate && jType == InPredicate {
					predicates[j] = updateInPredicate(jthPredicate, ithPredicate)
					removeValues = append(removeValues, i)
				} else if iType == InPredicate && jType == NotEqualPredicate {
					predicates[i] = updateInPredicate(ithPredicate, jthPredicate)
					removeValues = append(removeValues, j)
				}
			}
		}
	}
	newValues := make([]expression.Expression, 0, len(predicates))
	for i, value := range predicates {
		if !(indexInSlice(i, removeValues)) {
			newValues = append(newValues, value)
		}
	}
	return newValues
}

func (s *DataSource) predicateSimplification(opt *logicalOptimizeOp) LogicalPlan {
	p := s.self.(*DataSource)
	p.pushedDownConds = applyPredicateSimplification(p.pushedDownConds)
	p.allConds = applyPredicateSimplification(p.allConds)
	return p
}

func (*predicateSimplification) name() string {
	return "predicate_simplification"
}
