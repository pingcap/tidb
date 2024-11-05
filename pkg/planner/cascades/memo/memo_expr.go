// Copyright 2024 PingCAP, Inc.
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

package memo

import (
	"github.com/pingcap/tidb/pkg/planner/core/base"
)

// MemoExpression is a unified representation of logical and group expression.
// 1: when stepping into memo optimization, we may convert a logical plan tree into a memoExpr,
//
//	so each node of them is simply an LP.
//
// 2: when XForm is called from a specific rule, substitution happened, the output may be a mixture
//
//	of logical plan and group expression.
//
// # Thus, memoExpr is responsible for representing both of them, leveraging the unified portal of
//
// MemoExpression managing memo group generation and hashing functionality.
//
//revive:disable:exported
type MemoExpression struct {
	LP base.LogicalPlan

	GE *GroupExpression

	Inputs []*MemoExpression
}

// IsLogicalPlan checks whether this me is a logical plan.
func (me *MemoExpression) IsLogicalPlan() bool {
	return me.LP != nil && me.GE == nil
}

// IsGroupExpression checks whether this me is a group expression.
func (me *MemoExpression) IsGroupExpression() bool {
	return me.LP == nil && me.GE != nil
}

// NewMemoExpressionFromPlan init a memeExpression with a logical plan.
func NewMemoExpressionFromPlan(plan base.LogicalPlan) *MemoExpression {
	return &MemoExpression{
		LP: plan,
	}
}

// NewMemoExpressionFromGroupExpression inits a memoExpression with an existed group expression.
func NewMemoExpressionFromGroupExpression(ge *GroupExpression) *MemoExpression {
	return &MemoExpression{
		GE: ge,
	}
}

// NewMemoExpressionFromPlanAndInputs inits a memoExpression with mixed logical plan as current node,
// and certain memoExpressions as its input.
func NewMemoExpressionFromPlanAndInputs(plan base.LogicalPlan, inputs []*MemoExpression) *MemoExpression {
	return &MemoExpression{
		LP:     plan,
		Inputs: inputs,
	}
}
