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

	"github.com/bits-and-blooms/bitset"
	base2 "github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/zyedidia/generic/hashmap"
)

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

	// hash2GlobalGroupExpr is the map from hash to each all group's groupExpression.
	// two same hash64 and equals GE means a group merge trigger.
	hash2GlobalGroupExpr *hashmap.Map[*GroupExpression, *GroupExpression]

	// hasher is the pointer of hasher.
	hasher base2.Hasher
}

// NewMemo creates a new memo.
func NewMemo() *Memo {
	return &Memo{
		groupIDGen:    &GroupIDGenerator{id: 0},
		groups:        list.New(),
		groupID2Group: make(map[GroupID]*list.Element),
		hash2GlobalGroupExpr: hashmap.New[*GroupExpression, *GroupExpression](
			// todo: feel the operator count at the prev normalization rule.
			4,
			func(a, b *GroupExpression) bool {
				return a.Equals(b)
			},
			func(ge *GroupExpression) uint64 {
				return ge.GetHash64()
			}),
		hasher: base2.NewHashEqualer(),
	}
}

// GetHasher gets a hasher from the memo that ready to use.
func (mm *Memo) GetHasher() base2.Hasher {
	mm.hasher.Reset()
	return mm.hasher
}

// CopyIn copies a MemoExpression representation into the memo with format as GroupExpression inside.
// The generic logical forest inside memo is represented as memo group expression tree, while for entering
// and re-feeding the memo, we use the memoExpression as the currency：
//
// entering(init memo)
//
//	  lp                          ┌──────────┐
//	 /  \                         │ memo:    │
//	lp   lp       --copyIN->      │  G(ge)   │
//	    /  \                      │   /  \   │
//	  ...  ...                    │  G    G  │
//	                              └──────────┘
//
// re-feeding (intake XForm output)
//
//	  lp                          ┌──────────┐
//	 /  \                         │ memo:    │
//	GE  lp        --copyIN->      │  G(ge)   │
//	     |                        │   /  \   │
//	    GE                        │  G    G  │
//	                              └──────────┘
//
// the bare lp means the new created logical op or that whose child has changed which invalidate it's original
// old belonged group, make it back to bare-lp for re-inserting again in copyIn.
func (mm *Memo) CopyIn(target *Group, lp base.LogicalPlan) (*GroupExpression, error) {
	// Group the children first.
	childGroups := make([]*Group, 0, len(lp.Children()))
	for _, child := range lp.Children() {
		var currentChildG *Group
		if ge, ok := child.(*GroupExpression); ok {
			// which means it's the earliest unchanged GroupExpression from rule XForm.
			currentChildG = ge.GetGroup()
		} else {
			// which means it's a new/changed logical op, downward to get its input group ids to complete it.
			ge, err := mm.CopyIn(nil, child)
			if err != nil {
				return nil, err
			}
			currentChildG = ge.GetGroup()
		}
		intest.Assert(currentChildG != nil)
		intest.Assert(currentChildG != target)
		childGroups = append(childGroups, currentChildG)
	}
	var (
		ok        bool
		groupExpr *GroupExpression
	)
	groupExpr = mm.NewGroupExpression(lp, childGroups)
	groupExpr, ok = mm.InsertGroupExpression(groupExpr, target)
	if ok && target == nil {
		// derive logical property for new group.
		err := groupExpr.DeriveLogicalProp()
		if err != nil {
			return nil, err
		}
	}
	return groupExpr, nil
}

// GetGroups gets all groups in the memo.
func (mm *Memo) GetGroups() *list.List {
	return mm.groups
}

// GetGroupID2Group gets the map from group id to group.
func (mm *Memo) GetGroupID2Group() map[GroupID]*list.Element {
	return mm.groupID2Group
}

// GetRootGroup gets the root group of the memo.
func (mm *Memo) GetRootGroup() *Group {
	return mm.rootGroup
}

// InsertGroupExpression insert ge into a target group.
// @GroupExpression indicates the returned group expression, which may be the existed one or the newly inserted.
// @bool indicates whether the groupExpr is inserted to a new group.
func (mm *Memo) InsertGroupExpression(groupExpr *GroupExpression, target *Group) (*GroupExpression, bool) {
	// for group merge, here groupExpr is the new groupExpr with undetermined belonged group.
	// we need to use groupExpr hash to find whether there is same groupExpr existed before.
	// if existed and the existed groupExpr.Group is not same with target, we should merge them up.
	if existedGE, ok := mm.hash2GlobalGroupExpr.Get(groupExpr); ok {
		existedGroup := existedGE.GetGroup()
		mm.mergeGroup(existedGroup, target)
		return existedGE, false
	}
	if target == nil {
		target = mm.NewGroup()
		mm.groups.PushBack(target)
		mm.groupID2Group[target.groupID] = mm.groups.Back()
	}
	// if target has already existed a same groupExpr, it should exit above and return existedGE. Here just safely add it.
	target.Insert(groupExpr)
	// record them in the global GE map.
	mm.hash2GlobalGroupExpr.Put(groupExpr, groupExpr)
	return groupExpr, true
}

// NewGroup creates a new group.
func (mm *Memo) NewGroup() *Group {
	group := NewGroup(nil)
	group.groupID = mm.groupIDGen.NextGroupID()
	return group
}

// Init initializes the memo with a logical plan, converting logical plan tree format into group tree.
func (mm *Memo) Init(plan base.LogicalPlan) (*GroupExpression, error) {
	intest.Assert(mm.groups.Len() == 0)
	gE, err := mm.CopyIn(nil, plan)
	if err != nil {
		return nil, err
	}
	mm.rootGroup = gE.GetGroup()
	return gE, nil
}

// ForEachGroup traverse the inside group expression with f call on them each.
func (mm *Memo) ForEachGroup(f func(g *Group) bool) {
	var next bool
	for elem := mm.GetGroups().Front(); elem != nil; elem = elem.Next() {
		expr := elem.Value.(*Group)
		next = f(expr)
		if !next {
			break
		}
	}
}

// NewGroupExpression creates a new GroupExpression with the given logical plan and children.
func (mm *Memo) NewGroupExpression(lp base.LogicalPlan, inputs []*Group) *GroupExpression {
	ge := &GroupExpression{
		group:       nil,
		Inputs:      inputs,
		LogicalPlan: lp,
		hash64:      0,
		// todo: add rule set length
		mask: bitset.New(1),
	}
	// init hasher
	h := mm.GetHasher()
	ge.Init(h)
	// maintain the parentGE refs.
	for _, childG := range inputs {
		childG.addParentGEs(ge)
	}
	return ge
}

// mergeGroup will merge two equivalent group together if the following meets.
// two group expression from two groups: dst, src share the same hash64 means:
// 1: this two GEs has the same output schema.
// 2: this two GEs has the same input groups.
// 3: this two GEs has the same operator info.
// from the 3 above, we could say this two group expression are equivalent,
// and their groups are equivalent as well from the equivalent transitive rule.
func (*Memo) mergeGroup(_, _ *Group) {
	// todo: in next pull request
}
