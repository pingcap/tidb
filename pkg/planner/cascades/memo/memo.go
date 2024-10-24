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

	base2 "github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// Memo is the main structure of the memo package.
type Memo struct {
	// ctx is the context of the memo.
	sCtx sessionctx.Context

	// groupIDGen is the incremental group id for internal usage.
	groupIDGen GroupIDGenerator

	// rootGroup is the root group of the memo.
	rootGroup *Group

	// groups is the list of all groups in the memo.
	groups *list.List

	// groupID2Group is the map from group id to group.
	groupID2Group map[GroupID]*list.Element

	// hash2GroupExpr is the map from hash to group expression.
	hash2GroupExpr map[uint64]*list.Element

	// hasher is the pointer of hasher.
	hasher base2.Hasher
}

// NewMemo creates a new memo.
func NewMemo(ctx sessionctx.Context) *Memo {
	return &Memo{
		sCtx:          ctx,
		groupIDGen:    GroupIDGenerator{id: 0},
		groups:        list.New(),
		groupID2Group: make(map[GroupID]*list.Element),
		hasher:        base2.NewHashEqualer(),
	}
}

// GetHasher gets a hasher from the memo that ready to use.
func (m *Memo) GetHasher() base2.Hasher {
	m.hasher.Reset()
	return m.hasher
}

// CopyIn copies a logical plan into the memo with format as GroupExpression.
func (m *Memo) CopyIn(target *Group, lp base.LogicalPlan) *GroupExpression {
	// Group the children first.
	childGroups := make([]*Group, 0, len(lp.Children()))
	for _, child := range lp.Children() {
		// todo: child.getGroupExpression.GetGroup directly
		groupExpr := m.CopyIn(nil, child)
		group := groupExpr.group
		intest.Assert(group != nil)
		intest.Assert(group != target)
		childGroups = append(childGroups, group)
	}

	hasher := m.GetHasher()
	groupExpr := NewGroupExpression(lp, childGroups)
	groupExpr.Init(hasher)
	m.insertGroupExpression(groupExpr, target)
	// todo: new group need to derive the logical property.
	return groupExpr
}

// GetGroups gets all groups in the memo.
func (m *Memo) GetGroups() *list.List {
	return m.groups
}

// GetGroupID2Group gets the map from group id to group.
func (m *Memo) GetGroupID2Group() map[GroupID]*list.Element {
	return m.groupID2Group
}

// GetRootGroup gets the root group of the memo.
func (m *Memo) GetRootGroup() *Group {
	return m.rootGroup
}

// @bool indicates whether the groupExpr is inserted to a new group.
func (m *Memo) insertGroupExpression(groupExpr *GroupExpression, target *Group) bool {
	// for group merge, here groupExpr is the new groupExpr with undetermined belonged group.
	// we need to use groupExpr hash to find whether there is same groupExpr existed before.
	// if existed and the existed groupExpr.Group is not same with target, we should merge them up.
	// todo: merge group
	if target == nil {
		target = m.NewGroup()
		m.groups.PushBack(target)
		m.groupID2Group[target.groupID] = m.groups.Back()
	}
	target.Insert(groupExpr)
	return true
}

// NewGroup creates a new group.
func (m *Memo) NewGroup() *Group {
	group := NewGroup(nil)
	group.groupID = m.groupIDGen.NextGroupID()
	return group
}

// Init initializes the memo with a logical plan, converting logical plan tree format into group tree.
func (m *Memo) Init(plan base.LogicalPlan) *GroupExpression {
	intest.Assert(m.groups.Len() == 0)
	gE := m.CopyIn(nil, plan)
	m.rootGroup = gE.GetGroup()
	return gE
}
