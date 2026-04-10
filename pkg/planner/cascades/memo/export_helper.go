// Copyright 2026 PingCAP, Inc.
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
	"unsafe"

	corebase "github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/zyedidia/generic/hashmap"
)

// Exported types for testing

// ExportedGroup exports Group for testing
type ExportedGroup = Group

// ExportedGroupID exports GroupID for testing
type ExportedGroupID = GroupID

// ExportedGroupExpression exports GroupExpression for testing
type ExportedGroupExpression = GroupExpression

// ExportedNewGroup creates a new Group for testing
func ExportedNewGroup(prop *property.LogicalProperty) *Group {
	return NewGroup(prop)
}

// ExportedGetGroupID gets the groupID field from a Group
func ExportedGetGroupID(g *Group) GroupID {
	return g.groupID
}

// ExportedSetGroupID sets the groupID field for a Group
func ExportedSetGroupID(g *Group, id GroupID) {
	g.groupID = id
}

// ExportedGetGroupExpressionHash64 gets the hash64 field from a GroupExpression
func ExportedGetGroupExpressionHash64(ge *GroupExpression) uint64 {
	return ge.hash64
}

// ExportedSetGroupExpressionHash64 sets the hash64 field for a GroupExpression
func ExportedSetGroupExpressionHash64(ge *GroupExpression, h uint64) {
	ge.hash64 = h
}

// ExportedGetGroupExpressionGroup gets the group field from a GroupExpression
func ExportedGetGroupExpressionGroup(ge *GroupExpression) *Group {
	return ge.group
}

// ExportedGetGroupExpressionInputs gets the Inputs field from a GroupExpression
func ExportedGetGroupExpressionInputs(ge *GroupExpression) []*Group {
	return ge.Inputs
}

// ExportedGetLogicalExpressions gets the logicalExpressions list from a Group
func ExportedGetLogicalExpressions(g *Group) *list.List {
	return g.logicalExpressions
}

// ExportedGetHash2GroupExpr gets the hash2GroupExpr map from a Group
func ExportedGetHash2GroupExpr(g *Group) *hashmap.Map[*GroupExpression, *list.Element] {
	return g.hash2GroupExpr
}

// ExportedGetHash2ParentGroupExpr gets the hash2ParentGroupExpr map from a Group
func ExportedGetHash2ParentGroupExpr(g *Group) *hashmap.Map[unsafe.Pointer, *GroupExpression] {
	return g.hash2ParentGroupExpr
}

// ExportedGetGroupExpressionAddr gets the address of a GroupExpression for use as a key
func ExportedGetGroupExpressionAddr(ge *GroupExpression) unsafe.Pointer {
	return ge.addr()
}

// ExportedNewMemo creates a new Memo for testing
func ExportedNewMemo() *Memo {
	return NewMemo()
}

// ExportedSetGroupExpressionGroup sets the group field for a GroupExpression
func ExportedSetGroupExpressionGroup(ge *GroupExpression, g *Group) {
	ge.group = g
}

// ExportedNewGroupExpressionWithGroup creates a GroupExpression with a specified group
func ExportedNewGroupExpressionWithGroup(g *Group, inputs []*Group, plan corebase.LogicalPlan) *GroupExpression {
	return &GroupExpression{
		group:       g,
		Inputs:      inputs,
		LogicalPlan: plan,
	}
}

// ExportedMemoRootGroup gets the rootGroup field from a Memo
func ExportedMemoRootGroup(m *Memo) *Group {
	return m.rootGroup
}

// ExportedMemoGetHash2GlobalGroupExpr gets the hash2GlobalGroupExpr from a Memo
func ExportedMemoGetHash2GlobalGroupExpr(m *Memo) *hashmap.Map[*GroupExpression, *GroupExpression] {
	return m.hash2GlobalGroupExpr
}

// ExportedGroupIDGenerator exports GroupIDGenerator for testing
type ExportedGroupIDGenerator = GroupIDGenerator

// ExportedNewGroupIDGenerator creates a new GroupIDGenerator
func ExportedNewGroupIDGenerator() *GroupIDGenerator {
	return &GroupIDGenerator{}
}

// ExportedSetGroupIDGeneratorID sets the id field for a GroupIDGenerator
func ExportedSetGroupIDGeneratorID(g *GroupIDGenerator, id uint64) {
	g.id = id
}

// ExportedSetMemoRootGroup sets the rootGroup field for a Memo
func ExportedSetMemoRootGroup(m *Memo, g *Group) {
	m.rootGroup = g
}

// ExportedIteratorLP exports IteratorLP for testing
type ExportedIteratorLP = IteratorLP

// ExportedNewIterator creates a new IteratorLP from a Memo
func ExportedNewIterator(m *Memo) *IteratorLP {
	return m.NewIterator()
}
