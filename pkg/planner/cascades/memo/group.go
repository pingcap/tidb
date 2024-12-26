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
	"unsafe"

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

	// Operand2FirstExpr is used to locate to the first same type logicalExpression list.
	Operand2FirstExpr map[pattern.Operand]*list.Element

	// hash2GroupExpr is used to de-duplication in the list.
	hash2GroupExpr *hashmap.Map[*GroupExpression, *list.Element]

	// hash2ParentGroupExpr is reverted pointer back from current Group to parent referred GEs.
	// in group merge case, the src group parent GE may be changed as one GE of exact the same
	// hash64 and equals with one in dst parent group GE. In this case, we re-insert this tmp
	// GE back to parent GE's owner group without registering in the hash2GlobalGroupExpr because
	// of existence already and another group merge of this two will be triggered later.
	// But for maintenance parent GE refs here, we should keep both two links: one is from dst
	// back to tmp GE, one is from dst back to existedGE. since this two has exactly the hash and
	// equals, we use the ref address as the hash key, the GE as its value here.
	//
	//  G1:ge1   G2:ge2         G1:tmpGE  G2:ge2
	//   ▲         ▲                 ▲     ▲
	//   │         │                  \    │        tmpGE.equals(ge2), wait for another group merge G1 and G2 recursively.
	//   │         │                    \  │
	//  srcG ---> dstG          srcG ---> dstG
	//
	hash2ParentGroupExpr *hashmap.Map[unsafe.Pointer, *GroupExpression]

	// logicalProp indicates the logical property.
	logicalProp *property.LogicalProperty

	// explored indicates whether this group has been explored.
	explored bool
}

// GroupPair is a pair of *Group
type GroupPair struct {
	first  *Group
	second *Group
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
	// first: judge the e's existence from the hash map.
	// GroupExpressions hash should be initialized within Init(xxx) method.
	if _, ok := g.hash2GroupExpr.Get(e); ok {
		return false
	}
	// second: insert it into the logicalExpressions list and maintain the Operand2FirstExpr
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
	// third: insert the list.element into the map.
	g.hash2GroupExpr.Put(e, newEquiv)
	e.group = g
	return true
}

// Delete an existing Group expression
func (g *Group) Delete(e *GroupExpression) {
	// first: del it from map and get its list element if any.
	existElem, ok := g.hash2GroupExpr.Get(e)
	if !ok {
		// not exist at all.
		return
	}
	g.hash2GroupExpr.Remove(e)
	// second: maintain the Operand2FirstExpr
	operand := pattern.GetOperand(existElem.Value.(*GroupExpression).LogicalPlan)
	if g.Operand2FirstExpr[operand] == existElem {
		// The target GroupExpr is the first Element of the same Operand.
		// We need to change the FirstExpr to the next Expr, or delete the FirstExpr.
		nextElem := existElem.Next()
		if nextElem != nil && pattern.GetOperand(nextElem.Value.(*GroupExpression).LogicalPlan) == operand {
			// next elem still the same operand, just move it forward.
			g.Operand2FirstExpr[operand] = nextElem
		} else {
			// There is no more same GE of the Operand, so we should delete the FirstExpr of this Operand.
			delete(g.Operand2FirstExpr, operand)
		}
	}
	// third: just remove this element from logicalExpression list.
	g.logicalExpressions.Remove(existElem)
	e.group = nil
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

// removeParentGEs remove the current Group's parent GE ref which is pointed to parent.
func (g *Group) removeParentGEs(parent *GroupExpression) {
	addr := unsafe.Pointer(parent)
	_, ok := g.hash2ParentGroupExpr.Get(addr)
	intest.Assert(ok)
	g.hash2ParentGroupExpr.Remove(addr)
}

// addParentGEs is used to maintain the reverted parent pointer from Group to parent GEs.
func (g *Group) addParentGEs(parent *GroupExpression) {
	addr := unsafe.Pointer(parent)
	_, ok := g.hash2ParentGroupExpr.Get(addr)
	intest.Assert(!ok)
	g.hash2ParentGroupExpr.Put(addr, parent)
}

// mergeTo will merge src group's element into target group.
func (g *Group) mergeTo(target *Group) {
	// maintain target group's parent GE refs, except the triggering src GE.
	g.hash2ParentGroupExpr.Each(func(key unsafe.Pointer, val *GroupExpression) {
		target.hash2ParentGroupExpr.Put(key, val)
	})
	// maintain the ge migration, two groups may have the equivalent two,
	// check the duplication when inserting, merge their GE's state together.
	g.ForEachGE(func(ge *GroupExpression) bool {
		existedElem, ok := target.hash2GroupExpr.Get(ge)
		if ok {
			existedGE := existedElem.Value.(*GroupExpression)
			// not a same object
			intest.Assert(ge != existedGE)
			ge.mergeTo(existedGE)
		} else {
			// change the owner group inside.
			// since ge's is migrated from src group, the underlying parentGE
			// ref from child groups is not necessary to change.
			target.Insert(ge)
		}
		// note:
		// 1: for distinct GE among two groups: since GE's global register info, mm.hash2GlobalGroupExpr has already done
		// when inserting themselves into src group or dst group, nothing to do here. childG's parentGERefs is also maintained
		// before, nothing to do here.
		// 2: for the duplicated GE in two groups, which is the root cause for a recursive group merge of this two groups, as
		// the same way, mm.hash2GlobalGroupExpr is done once before, the re-inserted GE doesn't do it again, so nothing to do
		// here. childG's parentGERefs is done for re-inserted GE, but it's cleared ge.MergeTo(existedGE) here.
		return true
	})
	g.Clear()
}

// Clear clean and release the group element in the container.
func (g *Group) Clear() {
	// clean the list.
	g.logicalExpressions.Init()
	// clean the map.
	g.hash2GroupExpr.Clear()
	g.hash2ParentGroupExpr.Clear()
	clear(g.Operand2FirstExpr)
	g.logicalProp = nil
}

// Check is used in test for self check.
func (g *Group) Check() {
	intest.Assert(g.groupID > 0)
	intest.Assert(g.logicalExpressions.Len() == g.hash2GroupExpr.Size())
	// assert existence.
	hashMap := make(map[uint64]struct{}, g.logicalExpressions.Len())
	g.ForEachGE(func(ge *GroupExpression) bool {
		_, ok := g.hash2GroupExpr.Get(ge)
		intest.Assert(ok)
		hashMap[ge.hash64] = struct{}{}
		return true
	})
	g.hash2GroupExpr.Each(func(key *GroupExpression, _ *list.Element) {
		_, ok := hashMap[key.hash64]
		intest.Assert(ok)
	})
	for _, v := range g.Operand2FirstExpr {
		_, ok := hashMap[v.Value.(*GroupExpression).hash64]
		intest.Assert(ok)
	}
	// assert the parent GE ref.
	g.hash2ParentGroupExpr.Each(func(_ unsafe.Pointer, val *GroupExpression) {
		found := false
		for _, childG := range val.Inputs {
			if childG.GetGroupID() == g.GetGroupID() {
				found = true
				break
			}
		}
		intest.Assert(found)
	})
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
		hash2ParentGroupExpr: hashmap.New[unsafe.Pointer, *GroupExpression](
			4,
			func(a, b unsafe.Pointer) bool {
				return a == b
			},
			func(t unsafe.Pointer) uint64 {
				return uint64(uintptr(t))
			},
		),
	}
	return g
}
