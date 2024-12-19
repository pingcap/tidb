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
	"fmt"

	"github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/planner/cascades/pattern"
	"github.com/pingcap/tidb/pkg/planner/cascades/util"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/zyedidia/generic/hashmap"
)

var _ base.HashEquals = &Group{}

// Group is basic infra to store all the logically equivalent expressions
// for one logical operator in current context.
type Group struct {
	// groupID indicates the uniqueness of this group, also for encoding.
	groupID GroupID

	// logicalExpressions indicates the logical equiv classes for this group.
	logicalExpressions *list.List

	// Operand2FirstExpr is used to locate to the first same type logical expression
	// in list above instead of traverse them all.
	Operand2FirstExpr map[pattern.Operand]*list.Element

	// hash2GroupExpr is used to de-duplication in the list.
	hash2GroupExpr *hashmap.Map[*GroupExpression, *list.Element]

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
	g2, ok := other.(*Group)
	if !ok {
		return false
	}
	if g == nil {
		return g2 == nil
	}
	if g2 == nil {
		return false
	}
	return g.groupID == g2.groupID
}

// ******************************************* end of HashEqual methods *******************************************

// Insert adds a GroupExpression to the Group.
func (g *Group) Insert(e *GroupExpression) bool {
	if e == nil {
		return false
	}
	// GroupExpressions hash should be initialized within Init(xxx) method.
	if _, ok := g.hash2GroupExpr.Get(e); ok {
		return false
	}
	operand := pattern.GetOperand(e.LogicalPlan)
	var newEquiv *list.Element
	mark, ok := g.Operand2FirstExpr[operand]
	if ok {
		// cluster same operands together.
		newEquiv = g.logicalExpressions.InsertAfter(e, mark)
	} else {
		// otherwise, put it at the end.
		newEquiv = g.logicalExpressions.PushBack(e)
		g.Operand2FirstExpr[operand] = newEquiv
	}
	g.hash2GroupExpr.Put(e, newEquiv)
	e.group = g
	return true
}

// GetGroupID gets the group id.
func (g *Group) GetGroupID() GroupID {
	return g.groupID
}

// GetLogicalExpressions gets the logical expressions list.
func (g *Group) GetLogicalExpressions() *list.List {
	return g.logicalExpressions
}

// GetFirstElem returns the first Group expression which matches the Operand.
// Return a nil pointer if there isn't.
func (g *Group) GetFirstElem(operand pattern.Operand) *list.Element {
	if operand == pattern.OperandAny {
		return g.logicalExpressions.Front()
	}
	return g.Operand2FirstExpr[operand]
}

// HasLogicalProperty check whether current group has the logical property.
func (g *Group) HasLogicalProperty() bool {
	return g.logicalProp != nil
}

// GetLogicalProperty return this group's logical property.
func (g *Group) GetLogicalProperty() *property.LogicalProperty {
	intest.Assert(g.logicalProp != nil)
	return g.logicalProp
}

// SetLogicalProperty set this group's logical property.
func (g *Group) SetLogicalProperty(prop *property.LogicalProperty) {
	g.logicalProp = prop
}

// IsExplored returns whether this group is explored.
func (g *Group) IsExplored() bool {
	return g.explored
}

// SetExplored set the group as tagged as explored.
func (g *Group) SetExplored() {
	g.explored = true
}

// String implements fmt.Stringer interface.
func (g *Group) String(w util.StrBufferWriter) {
	w.WriteString(fmt.Sprintf("GID:%d", int(g.groupID)))
}

// ForEachGE traverse the inside group expression with f call on them each.
func (g *Group) ForEachGE(f func(ge *GroupExpression) bool) {
	var next bool
	for elem := g.logicalExpressions.Front(); elem != nil; elem = elem.Next() {
		expr := elem.Value.(*GroupExpression)
		next = f(expr)
		if !next {
			break
		}
	}
}

// NewGroup creates a new Group with given logical prop.
func NewGroup(prop *property.LogicalProperty) *Group {
	g := &Group{
		logicalExpressions: list.New(),
		Operand2FirstExpr:  make(map[pattern.Operand]*list.Element),
		logicalProp:        prop,
		hash2GroupExpr: hashmap.New[*GroupExpression, *list.Element](
			4,
			func(a, b *GroupExpression) bool {
				return a.Equals(b)
			},
			func(t *GroupExpression) uint64 {
				return t.GetHash64()
			},
		),
	}
	return g
}
