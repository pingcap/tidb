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

// constantPropagationSolver can support constant propagated cross-query block.
// This is a logical optimize rule.
// It mainly used for the sub query in FromList and propagated the constant predicate
// from sub query to outer query.
// In the future, it will support propagate constant in WhereClause and SelectList.
//
// Example 1:
// Query: select * from t, (select * from s where s.id>1) tmp where t.id=tmp.id
// Optimized: select * from t, (select * from s where s.id>1) tmp where t.id=tmp.id and tmp.id>1
//
// MatchCondition:
// 1. The tree pattern must satisfy function 'xxx'
// 2. The original predicate should be a constant and compare predicate, such as 'a.id>1'
// 3. The original predicate column should be the equal join predicate column, such as 'a.id=b.id'
//
// When the three conditions has been matched, the following optimizations will be performed.
// 1. The new selection will be created with the new constant predicate, such as 'b.id>1'.
// In the future, it will remove equal join predicate if the constant predicate is equal predicate.
// todo: flag
type constantPropagationSolver struct {
	parentPlan      LogicalPlan
	currentChildIdx int
	root            LogicalPlan
}

func (cp *constantPropagationSolver) optimize(ctx context.Context, p LogicalPlan, opt *logicalOptimizeOp) (LogicalPlan, error) {
	// init solver, the first optimize of this query plan
	if cp.root == nil {
		cp.parentPlan = nil
		cp.root = p
		defer func() {
			cp.parentPlan = nil
			cp.root = nil
		}()
	}
	// match conditions
	candidateConstantPredicates := cp.matchConditions(p)
	if len(candidateConstantPredicates) > 0 {
		// constant propagation
		cp.pullUpCandidateConstantPredicates(p, candidateConstantPredicates, opt)
	}
	// recursive optimize
	for i, children := range p.Children() {
		cp.currentChildIdx = i
		cp.parentPlan = p
		_, err := cp.optimize(ctx, children, opt)
		if err != nil {
			return cp.root, err
		}
	}
	return cp.root, nil
}

func (*constantPropagationSolver) name() string {
	return "constant_propagation"
}

// todo
func (*constantPropagationSolver) matchConditions(p LogicalPlan) []expression.Expression {
	// step1: match tree condition
	var tryToMatchTreePattern1 bool
	var tryToMatchTreePattern2 bool
	logicalJoin, isLogicalJoin := p.(*LogicalJoin)
	if isLogicalJoin {
		switch logicalJoin.JoinType {
		case LeftOuterJoin:
			tryToMatchTreePattern1 = true
		case RightOuterJoin:
			tryToMatchTreePattern2 = true
		case InnerJoin:
			tryToMatchTreePattern1 = true
			tryToMatchTreePattern2 = true
		default:
			return nil
		}
	}
	var matchTreePattern bool
	var candidatePredicates []expression.Expression
	// Match tree pattern 1
	if tryToMatchTreePattern1 {
		_, leftChildIsSelection := logicalJoin.children[0].(*LogicalSelection)
		_, rightChildIsTableScan := logicalJoin.children[1].(*DataSource)
		if leftChildIsSelection && rightChildIsTableScan {
			logicalSelection, _ := logicalJoin.children[0].(*LogicalSelection)
			candidatePredicates = logicalSelection.Conditions
			matchTreePattern = true
		}
	}
	// Match tree pattern 2
	if tryToMatchTreePattern2 {
		_, leftChildIsTableScan := logicalJoin.children[0].(*DataSource)
		_, rightChildIsSelection := logicalJoin.children[1].(*LogicalSelection)
		if leftChildIsTableScan && rightChildIsSelection {
			logicalSelection, _ := logicalJoin.children[1].(*LogicalSelection)
			candidatePredicates = logicalSelection.Conditions
			matchTreePattern = true
		}
	}
	if !matchTreePattern {
		return nil
	}

	// step2: match predicate condition
	var result []expression.Expression
	for _, candidatePredicate := range candidatePredicates {
		// the candidate predicate should be a constant and compare predicate
		match := validCompareConstantPredicate(candidatePredicate)
		if match {
			result = append(result, candidatePredicate)
		}
	}
	return result
}

// validComparePredicate checks if the predicate is an expression like [column '>'|'>='|'<'|'<='|'=' constant].
// return param1: return true, if the predicate is a compare constant predicate.
// return param2: return the column side of predicate.
func validCompareConstantPredicate(candidatePredicate expression.Expression) bool {
	scalarFunction, ok := candidatePredicate.(*expression.ScalarFunction)
	if !ok {
		return false
	}
	if scalarFunction.FuncName.L != ast.GT && scalarFunction.FuncName.L != ast.GE &&
		scalarFunction.FuncName.L != ast.LT && scalarFunction.FuncName.L != ast.LE &&
		scalarFunction.FuncName.L != ast.EQ {
		return false
	}
	column, _ := expression.ValidCompareConstantPredicateHelper(scalarFunction, true)
	if column == nil {
		column, _ = expression.ValidCompareConstantPredicateHelper(scalarFunction, false)
	}
	if column == nil {
		return false
	}
	return true
}

// todo
func (cp *constantPropagationSolver) pullUpCandidateConstantPredicates(p LogicalPlan, candidatePredicates []expression.Expression, opt *logicalOptimizeOp) {
	// generate a new selection for candidatePredicates
	selection := LogicalSelection{Conditions: candidatePredicates}.Init(p.SCtx(), p.SelectBlockOffset())
	// add selection above of p
	if cp.parentPlan == nil {
		cp.root = selection
	} else {
		cp.parentPlan.SetChild(cp.currentChildIdx, selection)
	}
	selection.SetChildren(p)
	appendAddSelectionTraceStep(cp.parentPlan, p, selection, opt)
}
