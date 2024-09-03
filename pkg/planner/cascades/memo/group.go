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
	"github.com/pingcap/tidb/pkg/util/intest"
)

var _ base.HashEquals = &Group{}

// Group is basic infra to store all the logically equivalent expressions
// for one logical operator in current context.
type Group struct {
	// groupID indicates the uniqueness of this group, also for encoding.
	GroupID GroupID

	// LogicalExpressions indicates the logical equiv classes for this group.
	LogicalExpressions *list.List

	// Operand2FirstExpr is used to locate to the first same type logical expression
	// in list above instead of traverse them all.
	Operand2FirstExpr map[pattern.Operand]*list.Element

	// Hash2GroupExpr is used to de-duplication in the list.
	Hash2GroupExpr map[uint64]*list.Element

	// LogicalProp indicates the logical property.
	LogicalProp *property.LogicalProperty

	// Explored indicates whether this group has been explored.
	Explored bool
}

// ******************************************* start of HashEqual methods *******************************************

// Hash64 implements the HashEquals.<0th> interface.
func (g *Group) Hash64(h base.Hasher) {
	h.HashUint64(uint64(g.GroupID))
}

// Equals implements the HashEquals.<1st> interface.
func (g *Group) Equals(other any) bool {
	if other == nil {
		return false
	}
	switch x := other.(type) {
	case *Group:
		return g.GroupID == x.GroupID
	case Group:
		return g.GroupID == x.GroupID
	default:
		return false
	}
}

// ******************************************* end of HashEqual methods *******************************************

// Exists checks whether a Group expression existed in a Group.
func (g *Group) Exists(hash64u uint64) bool {
	_, ok := g.Hash2GroupExpr[hash64u]
	return ok
}

// Insert adds a GroupExpression to the Group.
func (g *Group) Insert(e *GroupExpression) bool {
	if e == nil {
		return false
	}
	// GroupExpressions hash should be initialized within Init(xxx) method.
	hash64 := e.Sum64()
	intest.Assert(hash64 != 0, "hash64 should not be 0")
	if g.Exists(hash64) {
		return false
	}
	operand := pattern.GetOperand(e.LogicalPlan)
	var newEquiv *list.Element
	mark, ok := g.Operand2FirstExpr[operand]
	if ok {
		// cluster same operands together.
		newEquiv = g.LogicalExpressions.InsertAfter(e, mark)
	} else {
		// otherwise, put it at the end.
		newEquiv = g.LogicalExpressions.PushBack(e)
		g.Operand2FirstExpr[operand] = newEquiv
	}
	g.Hash2GroupExpr[hash64] = newEquiv
	e.Group = g
	return true
}

// NewGroup creates a new Group with given logical prop.
func NewGroup(prop *property.LogicalProperty) *Group {
	g := &Group{
		LogicalExpressions: list.New(),
		Hash2GroupExpr:     make(map[uint64]*list.Element),
		Operand2FirstExpr:  make(map[pattern.Operand]*list.Element),
		LogicalProp:        prop,
	}
	return g
}
