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
	Group    *Group

	// ExploreMark is uses to mark whether this GroupExpr has been fully
	// explored by a transformation rule batch in a certain round.
	ExploreMark

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
		appliedRuleSet: make(map[uint64]struct{}),
	}
}

// FingerPrint gets the unique fingerprint of the Group expression.
func (e *GroupExpr) FingerPrint() string {
	if len(e.selfFingerprint) == 0 {
		planHash := e.ExprNode.HashCode()
		//buffer := make([]byte, 2, 2+len(e.Children)*8+len(planHash))
		//binary.BigEndian.PutUint16(buffer, uint16(len(e.Children)))
		//for _, child := range e.Children {
		//	var buf [8]byte
		//	binary.BigEndian.PutUint64(buf[:], uint64(reflect.ValueOf(child).Pointer()))
		//	buffer = append(buffer, buf[:]...)
		//}
		//buffer = append(buffer, planHash...)
		e.selfFingerprint = string(planHash)
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

// CopyAppliedRules copies the appliedRuleSet of one GroupExpr to another.
func (e *GroupExpr) CopyAppliedRules(expr *GroupExpr) {
	for ruleID := range expr.appliedRuleSet {
		e.appliedRuleSet[ruleID] = struct{}{}
	}
}

// Clone clones a GroupExpr.
func (e *GroupExpr) Clone() *GroupExpr {
	newExpr := NewGroupExpr(e.ExprNode)
	newExpr.SetChildren(e.Children...)
	newExpr.CopyAppliedRules(e)
	return newExpr
}
