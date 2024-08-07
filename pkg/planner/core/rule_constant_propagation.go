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

	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
)

// ConstantPropagationSolver can support constant propagated cross-query block.
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
type ConstantPropagationSolver struct {
}

// Optimize implements base.LogicalOptRule.<0th> interface.
// **Preorder traversal** of logic tree
// Step1: constant propagation current plan node
// Step2: optimize all of child
//
// For step1, different logical plan have their own logic for constant propagation,
// which is mainly implemented in the interface "constantPropagation" of LogicalPlan.
// Currently only the Logical Join implements this function. (Used for the subquery in FROM List)
// In the future, the Logical Apply will implements this function. (Used for the subquery in WHERE or SELECT list)
func (cp *ConstantPropagationSolver) Optimize(_ context.Context, p base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
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
func (cp *ConstantPropagationSolver) execOptimize(currentPlan base.LogicalPlan, parentPlan base.LogicalPlan, currentChildIdx int, opt *optimizetrace.LogicalOptimizeOp) {
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

// Name implements base.LogicalOptRule.<1st> interface.
func (*ConstantPropagationSolver) Name() string {
	return "constant_propagation"
}
