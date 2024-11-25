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
	"io"

	base2 "github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/pattern"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// GroupExpression is a single expression from the equivalent list classes inside a group.
// it is a node in the expression tree, while it takes groups as inputs. This kind of loose
// coupling between Group and GroupExpression is the key to the success of the memory compact
// of representing a forest.
type GroupExpression struct {
	// group is the Group that this GroupExpression belongs to.
	group *Group

	// inputs stores the Groups that this GroupExpression based on.
	Inputs []*Group

	// logicalPlan is internal logical expression stands for this groupExpr.
	logicalPlan base.LogicalPlan

	// hash64 is the unique fingerprint of the GroupExpression.
	hash64 uint64
}

// GetLogicalPlan returns the logical plan of the GroupExpression.
func (e *GroupExpression) GetLogicalPlan() base.LogicalPlan {
	return e.logicalPlan
}

// GetGroup returns the Group that this GroupExpression belongs to.
func (e *GroupExpression) GetGroup() *Group {
	return e.group
}

// String implements the fmt.Stringer interface.
func (e *GroupExpression) String(w io.Writer) {
	e.GetLogicalPlan().ExplainID()
	_, _ = w.Write([]byte("GE:" + e.GetLogicalPlan().ExplainID().String() + "{"))
	for i, input := range e.Inputs {
		if i != 0 {
			_, _ = w.Write([]byte(", "))
		}
		input.String(w)
	}
	_, _ = w.Write([]byte("}"))
}

// Sum64 returns the cached hash64 of the GroupExpression.
func (e *GroupExpression) Sum64() uint64 {
	intest.Assert(e.hash64 != 0, "hash64 should not be 0")
	return e.hash64
}

// Hash64 implements the Hash64 interface.
func (e *GroupExpression) Hash64(h base2.Hasher) {
	// logical plan hash.
	e.logicalPlan.Hash64(h)
	// children group hash.
	for _, child := range e.Inputs {
		child.Hash64(h)
	}
}

// Equals implements the Equals interface.
func (e *GroupExpression) Equals(other any) bool {
	e2, ok := other.(*GroupExpression)
	if !ok {
		return false
	}
	if e == nil {
		return e2 == nil
	}
	if e2 == nil {
		return false
	}
	if len(e.Inputs) != len(e2.Inputs) {
		return false
	}
	if pattern.GetOperand(e.logicalPlan) != pattern.GetOperand(e2.logicalPlan) {
		return false
	}
	// current logical operator meta cmp, logical plan don't care logicalPlan's children.
	// when we convert logicalPlan to GroupExpression, we will set children to nil.
	if !e.logicalPlan.Equals(e2.logicalPlan) {
		return false
	}
	// if one of the children is different, then the two GroupExpressions are different.
	for i, one := range e.Inputs {
		if !one.Equals(e2.Inputs[i]) {
			return false
		}
	}
	return true
}

// NewGroupExpression creates a new GroupExpression with the given logical plan and children.
func NewGroupExpression(lp base.LogicalPlan, inputs []*Group) *GroupExpression {
	return &GroupExpression{
		group:       nil,
		Inputs:      inputs,
		logicalPlan: lp,
		hash64:      0,
	}
}

// Init initializes the GroupExpression with the given group and hasher.
func (e *GroupExpression) Init(h base2.Hasher) {
	e.Hash64(h)
	e.hash64 = h.Sum64()
}
