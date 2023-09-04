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
type constantPropagationSolver struct {
}

func (cp *constantPropagationSolver) optimize(_ context.Context, p LogicalPlan, opt *logicalOptimizeOp) (LogicalPlan, error) {
	// constant propagation root plan
	newRoot := p.constantPropagation(nil, 0, opt)

	// recursive optimize
	for i, children := range p.Children() {
		cp.execOptimize(children, p, i, opt)
	}

	if newRoot == nil {
		return p, nil
	}
	return newRoot, nil
}

// execOptimize optimize constant propagation exclude root plan node
func (cp *constantPropagationSolver) execOptimize(currentPlan LogicalPlan, parentPlan LogicalPlan, currentChildIdx int, opt *logicalOptimizeOp) {
	if parentPlan == nil {
		// Attention: The function 'execOptimize' could not handle the root plan, so the parent plan could not be nil.
		return
	}
	// constant propagation
	currentPlan.constantPropagation(parentPlan, currentChildIdx, opt)
	// recursive optimize
	for i, children := range currentPlan.Children() {
		cp.execOptimize(children, currentPlan, i, opt)
	}
}

func (*constantPropagationSolver) name() string {
	return "constant_propagation"
}

func (*baseLogicalPlan) constantPropagation(_ LogicalPlan, _ int, _ *logicalOptimizeOp) (newRoot LogicalPlan) {
	// Only LogicalJoin can apply constant propagation
	// Other Logical plan do nothing
	return nil
}

// 1. recursiveGetConstantPredicates
// 2. add selection above of LogicalJoin
// Return nil if the root of plan has not been changed
// Return new root if the root of plan is changed to selection
func (logicalJoin *LogicalJoin) constantPropagation(parentPlan LogicalPlan, currentChildIdx int, opt *logicalOptimizeOp) (newRoot LogicalPlan) {
	// step1: get constant predicate from left or right according to the JoinType
	var getConstantPredicateFromLeft bool
	var getConstantPredicateFromRight bool
	switch logicalJoin.JoinType {
	case LeftOuterJoin:
		getConstantPredicateFromLeft = true
	case RightOuterJoin:
		getConstantPredicateFromRight = true
	case InnerJoin:
		getConstantPredicateFromLeft = true
		getConstantPredicateFromRight = true
	default:
		return
	}
	var candidateConstantPredicates []expression.Expression
	if getConstantPredicateFromLeft {
		candidateConstantPredicates = logicalJoin.children[0].recursiveGetConstantPredicates()
	}
	if getConstantPredicateFromRight {
		candidateConstantPredicates = append(candidateConstantPredicates, logicalJoin.children[1].recursiveGetConstantPredicates()...)
	}
	if len(candidateConstantPredicates) == 0 {
		return
	}

	// step2: add selection above of LogicalJoin
	return pullUpCandidateConstantPredicates(logicalJoin, currentChildIdx, parentPlan, candidateConstantPredicates, opt)
}

func (*baseLogicalPlan) recursiveGetConstantPredicates() []expression.Expression {
	// Only LogicalProjection and LogicalSelection can get constant predicates
	// Other Logical plan return nil
	return nil
}

func (selection *LogicalSelection) recursiveGetConstantPredicates() []expression.Expression {
	var result []expression.Expression
	for _, candidatePredicate := range selection.Conditions {
		// the candidate predicate should be a constant and compare predicate
		match := validCompareConstantPredicate(candidatePredicate)
		if match {
			result = append(result, candidatePredicate)
		}
	}
	return result
}

func (projection *LogicalProjection) recursiveGetConstantPredicates() []expression.Expression {
	// projection has no column expr
	if !canProjectionBeEliminatedLoose(projection) {
		return nil
	}
	candidateConstantPredicates := projection.children[0].recursiveGetConstantPredicates()
	// replace predicate by projection expr
	// candidate predicate : a=1
	// projection: a as a'
	// result predicate : a'=1
	replace := make(map[string]*expression.Column)
	for i, expr := range projection.Exprs {
		replace[string(expr.HashCode(nil))] = projection.Schema().Columns[i]
	}
	result := make([]expression.Expression, 0, len(candidateConstantPredicates))
	for _, predicate := range candidateConstantPredicates {
		// The column of predicate must exist in projection exprs
		columns := expression.ExtractColumns(predicate)
		// The number of columns in candidate predicate must be 1.
		if len(columns) != 1 {
			continue
		}
		if replace[string(columns[0].HashCode(nil))] == nil {
			// The column of predicate will not appear on the upper level
			// This means that this predicate does not apply to the constant propagation optimization rule
			// For example: select * from t, (select b from s where s.a=1) tmp where t.b=s.b
			continue
		}
		clonePredicate := predicate.Clone()
		ResolveExprAndReplace(clonePredicate, replace)
		result = append(result, clonePredicate)
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
func pullUpCandidateConstantPredicates(currentPlan LogicalPlan, currentChildIdx int, parentPlan LogicalPlan,
	candidatePredicates []expression.Expression, opt *logicalOptimizeOp) (newRoot LogicalPlan) {
	// generate a new selection for candidatePredicates
	selection := LogicalSelection{Conditions: candidatePredicates}.Init(currentPlan.SCtx(), currentPlan.SelectBlockOffset())
	// add selection above of p
	if parentPlan == nil {
		newRoot = selection
	} else {
		parentPlan.SetChild(currentChildIdx, selection)
	}
	selection.SetChildren(currentPlan)
	appendAddSelectionTraceStep(parentPlan, currentPlan, selection, opt)
	if parentPlan == nil {
		return newRoot
	}
	return nil
}
