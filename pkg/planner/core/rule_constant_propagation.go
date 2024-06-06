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

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
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
// Process:
//  1. Match the Join + selection pattern and find the candidate constant predicate, such as 's.id>1'
//  2. Pull up the candidate constant predicate, above of Join node.
//     The new selection will be created with the new constant predicate. 'tmp.id>1'
//
// Steps 1 and 2 will be called recursively
//  3. (ppdSolver in rule_predicate_push_down.go) Push down constant predicate
//     and propagate constant predicate into other side. 't.id>1'
type constantPropagationSolver struct {
}

// **Preorder traversal** of logic tree
// Step1: constant propagation current plan node
// Step2: optimize all of child
//
// For step1, different logical plan have their own logic for constant propagation,
// which is mainly implemented in the interface "constantPropagation" of LogicalPlan.
// Currently only the Logical Join implements this function. (Used for the subquery in FROM List)
// In the future, the Logical Apply will implements this function. (Used for the subquery in WHERE or SELECT list)
func (cp *constantPropagationSolver) optimize(_ context.Context, p base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	planChanged := false
	// constant propagation root plan
	newRoot := p.ConstantPropagation(nil, 0, opt)

	// recursive optimize
	for i, children := range p.Children() {
		cp.execOptimize(children, p, i, opt)
	}

	if newRoot == nil {
		return p, planChanged, nil
	}
	return newRoot, planChanged, nil
}

// execOptimize optimize constant propagation exclude root plan node
func (cp *constantPropagationSolver) execOptimize(currentPlan base.LogicalPlan, parentPlan base.LogicalPlan, currentChildIdx int, opt *optimizetrace.LogicalOptimizeOp) {
	if parentPlan == nil {
		// Attention: The function 'execOptimize' could not handle the root plan, so the parent plan could not be nil.
		return
	}
	// constant propagation
	currentPlan.ConstantPropagation(parentPlan, currentChildIdx, opt)
	// recursive optimize
	for i, children := range currentPlan.Children() {
		cp.execOptimize(children, currentPlan, i, opt)
	}
}

func (*constantPropagationSolver) name() string {
	return "constant_propagation"
}

// ConstantPropagation implemented the logic of constant propagation in From List
// Query: select * from t, (select a, b from s where s.a>1) tmp where tmp.a=t.a
// Origin logical plan:
/*
            +----------------+
            |  LogicalJoin   |
            +-------^--------+
                    |
      +-------------+--------------+
      |                            |
+-----+------+              +------+------+
| Projection |              | TableScan   |
+-----^------+              +-------------+
      |
      |
+-----+------+
| Selection  |
|   s.a>1    |
+------------+
*/
//  1. 'PullUpConstantPredicates': Call this function until find selection and pull up the constant predicate layer by layer
//     LogicalSelection: find the s.a>1
//     LogicalProjection: get the s.a>1 and pull up it, changed to tmp.a>1
//  2. 'addCandidateSelection': Add selection above of LogicalJoin,
//     put all predicates pulled up from the lower layer into the current new selection.
//     LogicalSelection: tmp.a >1
//
// Optimized plan:
/*
            +----------------+
            |  Selection     |
            |    tmp.a>1     |
            +-------^--------+
                    |
            +-------+--------+
            |  LogicalJoin   |
            +-------^--------+
                    |
      +-------------+--------------+
      |                            |
+-----+------+              +------+------+
| Projection |              | TableScan   |
+-----^------+              +-------------+
      |
      |
+-----+------+
| Selection  |
|   s.a>1    |
+------------+
*/
// Return nil if the root of plan has not been changed
// Return new root if the root of plan is changed to selection
func (logicalJoin *LogicalJoin) ConstantPropagation(parentPlan base.LogicalPlan, currentChildIdx int, opt *optimizetrace.LogicalOptimizeOp) (newRoot base.LogicalPlan) {
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
		candidateConstantPredicates = logicalJoin.Children()[0].PullUpConstantPredicates()
	}
	if getConstantPredicateFromRight {
		candidateConstantPredicates = append(candidateConstantPredicates, logicalJoin.Children()[1].PullUpConstantPredicates()...)
	}
	if len(candidateConstantPredicates) == 0 {
		return
	}

	// step2: add selection above of LogicalJoin
	return addCandidateSelection(logicalJoin, currentChildIdx, parentPlan, candidateConstantPredicates, opt)
}

// PullUpConstantPredicates implements the LogicalPlan interface.
func (selection *LogicalSelection) PullUpConstantPredicates() []expression.Expression {
	var result []expression.Expression
	for _, candidatePredicate := range selection.Conditions {
		// the candidate predicate should be a constant and compare predicate
		match := validCompareConstantPredicate(selection.SCtx().GetExprCtx().GetEvalCtx(), candidatePredicate)
		if match {
			result = append(result, candidatePredicate)
		}
	}
	return result
}

// PullUpConstantPredicates implements LogicalPlan interface.
func (projection *LogicalProjection) PullUpConstantPredicates() []expression.Expression {
	// projection has no column expr
	if !canProjectionBeEliminatedLoose(projection) {
		return nil
	}
	candidateConstantPredicates := projection.Children()[0].PullUpConstantPredicates()
	// replace predicate by projection expr
	// candidate predicate : a=1
	// projection: a as a'
	// result predicate : a'=1
	replace := make(map[string]*expression.Column)
	for i, expr := range projection.Exprs {
		replace[string(expr.HashCode())] = projection.Schema().Columns[i]
	}
	result := make([]expression.Expression, 0, len(candidateConstantPredicates))
	for _, predicate := range candidateConstantPredicates {
		// The column of predicate must exist in projection exprs
		columns := expression.ExtractColumns(predicate)
		// The number of columns in candidate predicate must be 1.
		if len(columns) != 1 {
			continue
		}
		if replace[string(columns[0].HashCode())] == nil {
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
func validCompareConstantPredicate(ctx expression.EvalContext, candidatePredicate expression.Expression) bool {
	scalarFunction, ok := candidatePredicate.(*expression.ScalarFunction)
	if !ok {
		return false
	}
	if scalarFunction.FuncName.L != ast.GT && scalarFunction.FuncName.L != ast.GE &&
		scalarFunction.FuncName.L != ast.LT && scalarFunction.FuncName.L != ast.LE &&
		scalarFunction.FuncName.L != ast.EQ {
		return false
	}
	column, _ := expression.ValidCompareConstantPredicateHelper(ctx, scalarFunction, true)
	if column == nil {
		column, _ = expression.ValidCompareConstantPredicateHelper(ctx, scalarFunction, false)
	}
	if column == nil {
		return false
	}
	return true
}

// Add a new selection between parent plan and current plan with candidate predicates
/*
+-------------+                                    +-------------+
| parentPlan  |                                    | parentPlan  |
+-----^-------+                                    +-----^-------+
      |           --addCandidateSelection--->            |
+-----+-------+                              +-----------+--------------+
| currentPlan |                              |        selection         |
+-------------+                              |   candidate predicate    |
                                             +-----------^--------------+
                                                         |
                                                         |
                                                    +----+--------+
                                                    | currentPlan |
                                                    +-------------+
*/
// If the currentPlan at the top of query plan, return new root plan (selection)
// Else return nil
func addCandidateSelection(currentPlan base.LogicalPlan, currentChildIdx int, parentPlan base.LogicalPlan,
	candidatePredicates []expression.Expression, opt *optimizetrace.LogicalOptimizeOp) (newRoot base.LogicalPlan) {
	// generate a new selection for candidatePredicates
	selection := LogicalSelection{Conditions: candidatePredicates}.Init(currentPlan.SCtx(), currentPlan.QueryBlockOffset())
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
