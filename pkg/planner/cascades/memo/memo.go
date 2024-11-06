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
	"sync"

	base2 "github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// MemoPool is initialized for memory saving by reusing TaskStack.
var MemoPool = sync.Pool{
	New: func() any {
		return NewMemo()
	},
}

// Destroy indicates that when stack itself is useless like in the end of optimizing phase, we can destroy ourselves.
func (mm *Memo) Destroy() {
	// when a TaskStack itself is useless, we can destroy itself actively.
	mm.groupIDGen.id = 0
	mm.rootGroup = nil
	mm.groups.Init()
	clear(mm.groupID2Group)
	clear(mm.hash2GroupExpr)
	mm.hasher.Reset()
	MemoPool.Put(mm)
}

// Memo is the main structure of the memo package.
type Memo struct {
	// groupIDGen is the incremental group id for internal usage.
	groupIDGen *GroupIDGenerator

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
func NewMemo() *Memo {
	return &Memo{
		groupIDGen:    &GroupIDGenerator{id: 0},
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

// CopyIn copies a MemoExpression representation into the memo with format as GroupExpression inside.
// The generic logical forest inside memo is represented as memo group expression tree, while for entering
// and re-feeding the memo, we use the memoExpression as the currency：
//
// entering(init memo)
//
//	  lp                 ME{lp}                       ┌──────────┐
//	 /  \                /   \                        │ memo:    │
//	lp   lp      ->  ME{lp}   ME{lp}    --copyIN->    │    GE    │
//	    /  \                  /  \                    │   /  \   │
//	  ...  ...             ...   ...                  │  G    G  │
//	                                                  └──────────┘
//
// re-feeding (intake XForm output)
//
//	  GE                 ME{GE}                       ┌──────────┐
//	 /  \                 /  \                        │ memo:    │
//	GE  lp       ->   ME{GE}  ME{lp}    --copyIN->    │    GE    │
//	     |                     |                      │   /  \   │
//	    GE                    ME{GE}                  │  G    G  │
//	                                                  └──────────┘
func (m *Memo) CopyIn(target *Group, lp base.LogicalPlan) (*GroupExpression, error) {
	// Group the children first.
	childGroups := make([]*Group, 0, len(lp.Children()))
	for _, child := range lp.Children() {
		var (
			currentChildG *Group
		)
		if ge, ok := child.(*GroupExpression); ok {
			// which means its mixed memoExpression from rule XForm.
			currentChildG = ge.GetGroup()
		} else {
			// here means it's a raw logical op, downward to get its input groups.
			ge, err := m.CopyIn(nil, child)
			if err != nil {
				return nil, err
			}
			currentChildG = ge.GetGroup()
		}
		intest.Assert(currentChildG != nil)
		intest.Assert(currentChildG != target)
		childGroups = append(childGroups, currentChildG)
	}

	// lp's original children is useless since now, prepare it as nil for later appending as group expressions if any.
	lp.SetChildren(nil)
	hasher := m.GetHasher()
	groupExpr := NewGroupExpression(lp, childGroups)
	groupExpr.Init(hasher)
	if m.InsertGroupExpression(groupExpr, target) && target == nil {
		// derive logical property for new group.
		err := groupExpr.DeriveLogicalProp()
		if err != nil {
			return nil, err
		}
	}
	return groupExpr, nil
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

// InsertGroupExpression indicates whether the groupExpr is inserted to a new group.
func (m *Memo) InsertGroupExpression(groupExpr *GroupExpression, target *Group) bool {
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
func (m *Memo) Init(plan base.LogicalPlan) (*GroupExpression, error) {
	intest.Assert(m.groups.Len() == 0)
	gE, err := m.CopyIn(nil, ToMemoExprTree(plan))
	if err != nil {
		return nil, err
	}
	m.rootGroup = gE.GetGroup()
	return gE, nil
}

func ToMemoExprTree(plan base.LogicalPlan) *MemoExpression {
	if len(plan.Children()) == 0 {
		return &MemoExpression{
			LP: plan,
		}
	}
	child := make([]*MemoExpression, 0, len(plan.Children()))
	for _, one := range plan.Children() {
		child = append(child, ToMemoExprTree(one))
	}
	return &MemoExpression{
		LP:     plan,
		Inputs: child,
	}
}
