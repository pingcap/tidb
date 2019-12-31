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

package memo

import (
	"fmt"
	"reflect"

	"github.com/pingcap/tidb/expression"
	plannercore "github.com/pingcap/tidb/planner/core"
)

// GroupExpr is used to store all the logically equivalent expressions which
// have the same root operator. Different from a normal expression, the
// Children of a Group expression are expression Groups, not expressions.
// Another property of Group expression is that the child Group references will
// never be changed once the Group expression is created.
type GroupExpr struct {
	ExprNode plannercore.LogicalPlan
	Children []*Group
	Explored bool
	Group    *Group

	selfFingerprint string
	// appliedRuleSet saves transformation rules which have been applied to this
	// GroupExpr, and will not be applied again. Use `uint64` which should be the
	// id of a Transformation instead of `Transformation` itself to avoid import cycle.
	appliedRuleSet map[uint64]struct{}
}

// NewGroupExpr creates a GroupExpr based on a logical plan node.
func NewGroupExpr(node plannercore.LogicalPlan) *GroupExpr {
	return &GroupExpr{
		ExprNode:       node,
		Children:       nil,
		Explored:       false,
		appliedRuleSet: make(map[uint64]struct{}),
	}
}

// FingerPrint gets the unique fingerprint of the Group expression.
func (e *GroupExpr) FingerPrint() string {
	if e.selfFingerprint == "" {
		e.selfFingerprint = fmt.Sprintf("%v", e.ExprNode.ID())
		for i := range e.Children {
			e.selfFingerprint += e.Children[i].FingerPrint()
		}
	}
	return e.selfFingerprint
}

// SetChildren sets Children of the GroupExpr.
func (e *GroupExpr) SetChildren(children ...*Group) {
	e.Children = children
}

// Schema gets GroupExpr's Schema.
func (e *GroupExpr) Schema() *expression.Schema {
	return e.Group.Prop.Schema
}

// AddAppliedRule adds a rule into the appliedRuleSet.
func (e *GroupExpr) AddAppliedRule(rule interface{}) {
	ruleID := reflect.ValueOf(rule).Pointer()
	e.appliedRuleSet[uint64(ruleID)] = struct{}{}
}

// HasAppliedRule returns if the rule has been applied.
func (e *GroupExpr) HasAppliedRule(rule interface{}) bool {
	ruleID := reflect.ValueOf(rule).Pointer()
	_, ok := e.appliedRuleSet[uint64(ruleID)]
	return ok
}
