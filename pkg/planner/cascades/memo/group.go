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
	"container/list"

	"github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/planner/pattern"
	"github.com/pingcap/tidb/pkg/planner/property"
)

var _ base.HashEquals = &Group{}

// Group is basic infra to store all the logically equivalent expressions
// for one logical operator in current context.
type Group struct {
	// groupID indicates the uniqueness of this group, also for encoding.
	groupID GroupID

	// logicalExpressions indicates the logical equiv classes for this group.
	logicalExpressions *list.List

	// operand2FirstExpr is used to locate to the first same type logical expression
	// in list above instead of traverse them all.
	operand2FirstExpr map[pattern.Operand]*list.Element

	// hash2GroupExpr is used to de-duplication in the list.
	hash2GroupExpr map[uint64]*list.Element

	// logicalProp indicates the logical property.
	logicalProp *property.LogicalProperty

	// explored indicates whether this group has been explored.
	explored bool
}

// ******************************************* start of HashEqual methods *******************************************

// Hash64 implements the HashEquals.<0th> interface.
func (g *Group) Hash64(h base.Hasher) {
	h.HashUint64(uint64(g.groupID))
}

// Equals implements the HashEquals.<1st> interface.
func (g *Group) Equals(other any) bool {
	if other == nil {
		return false
	}
	switch x := other.(type) {
	case *Group:
		return g.groupID == x.groupID
	case Group:
		return g.groupID == x.groupID
	default:
		return false
	}
}

// ******************************************* end of HashEqual methods *******************************************

// Exists checks whether a Group expression existed in a Group.
func (g *Group) Exists(hash64u uint64) bool {
	_, ok := g.hash2GroupExpr[hash64u]
	return ok
}

// Insert adds a GroupExpression to the Group.
func (g *Group) Insert(e *GroupExpression) bool {
	if e == nil {
		return false
	}
	// GroupExpressions hash should be initialized within Init(xxx) method.
	hash64 := e.Sum64()
	if g.Exists(hash64) {
		return false
	}
	operand := pattern.GetOperand(e.logicalPlan)
	var newEquiv *list.Element
	mark, ok := g.operand2FirstExpr[operand]
	if ok {
		// cluster same operands together.
		newEquiv = g.logicalExpressions.InsertAfter(e, mark)
	} else {
		// otherwise, put it at the end.
		newEquiv = g.logicalExpressions.PushBack(e)
		g.operand2FirstExpr[operand] = newEquiv
	}
	g.hash2GroupExpr[hash64] = newEquiv
	e.group = g
	return true
}

// NewGroup creates a new Group with given logical prop.
func NewGroup(prop *property.LogicalProperty) *Group {
	g := &Group{
		logicalExpressions: list.New(),
		hash2GroupExpr:     make(map[uint64]*list.Element),
		operand2FirstExpr:  make(map[pattern.Operand]*list.Element),
		logicalProp:        prop,
	}
	return g
}
