// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package cascades

import (
	"fmt"

	plannercore "github.com/pingcap/tidb/planner/core"
)

// GroupExpr is used to store all the logically equivalent expressions which
// have the same root operator. Different from a normal expression, the
// children of a group expression are expression Groups, not expressions.
// Another property of group expression is that the child group references will
// never be changed once the group expression is created.
type GroupExpr struct {
	exprNode plannercore.LogicalPlan
	children []*Group
	explored bool

	selfFingerprint string
}

// NewGroupExpr creates a GroupExpr based on a logical plan node.
func NewGroupExpr(node plannercore.LogicalPlan) *GroupExpr {
	return &GroupExpr{
		exprNode: node,
		children: nil,
		explored: false,
	}
}

// FingerPrint get the unique fingerprint of the group expression.
func (e *GroupExpr) FingerPrint() string {
	if e.selfFingerprint == "" {
		e.selfFingerprint = fmt.Sprintf("%v", e.exprNode.ID())
		for i := range e.children {
			e.selfFingerprint += e.children[i].FingerPrint()
		}
	}
	return e.selfFingerprint
}
