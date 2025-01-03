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
	"unsafe"

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
func NewMemo(caps ...uint64) *Memo {
	// default capacity is 4.
	capacity := uint64(4)
	if len(caps) > 1 {
		capacity = caps[0]
	}
	return &Memo{
		groupIDGen:    &GroupIDGenerator{id: 0},
		groups:        list.New(),
		groupID2Group: make(map[GroupID]*list.Element),
		hash2GlobalGroupExpr: hashmap.New[*GroupExpression, *GroupExpression](
			capacity,
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
func (mm *Memo) InsertGroupExpression(groupExpr *GroupExpression, target *Group) (_ *GroupExpression, inserted bool) {
	defer func() {
		if inserted {
			// maintain the parentGE refs after being successfully inserted.
			for _, childG := range groupExpr.Inputs {
				childG.addParentGEs(groupExpr)
			}
		}
	}()
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
	mm.groups.PushBack(group)
	mm.groupID2Group[group.groupID] = mm.groups.Back()
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
	// since we can't ensure this new group expression can be successfully inserted in target group,
	// it may be duplicated, so we move the maintenance of parentGE refs after insert action.
	return ge
}

// mergeGroup will merge two equivalent group together if the following meets.
// two group expression from two groups: dst, src share the same hash64 means:
// 1: this two GEs has the same output schema.
// 2: this two GEs has the same input groups.
// 3: this two GEs has the same operator info.
// from the 3 above, we could say this two group expression are equivalent,
// and their groups are equivalent as well from the equivalent transitive rule.
func (mm *Memo) mergeGroup(src, dst *Group) {
	// two groups should be different at group id.
	needMerge := dst != nil && dst.GetGroupID() != src.GetGroupID()
	if !needMerge {
		return
	}
	// step1: remove src group from the global register map and list, it may have been merged.
	srcGroupElem, ok := mm.groupID2Group[src.GetGroupID()]
	if !ok {
		return
	}
	mm.groups.Remove(srcGroupElem)
	delete(mm.groupID2Group, src.GetGroupID())
	// reset the root group which has been remove above.
	if src.GetGroupID() == mm.rootGroup.GetGroupID() {
		mm.rootGroup = dst
	}
	// record <src, dst> pair for latter call if any.
	lazyCallPair := make([]*GroupPair, 0)
	// step2: change src group's parent GE's child group id and reinsert them.
	// for each src group's parent groupExpression, we need modify their input group.
	src.hash2ParentGroupExpr.Each(func(_ unsafe.Pointer, val *GroupExpression) {
		if val.group.Equals(dst) {
			// child GE in child group is equivalent with one in parent Group.
			return
		}
		// when GE's child group is changed, its hash64 is changed as well, re-insert them.
		mm.hash2GlobalGroupExpr.Remove(val)
		// keep the original owner group, otherwise, it will be set to nil when delete the key from it.
		parentOwnerG := val.GetGroup()
		parentOwnerG.Delete(val)
		// parentGE's input group has been modified, reinsert them.
		reInsertGE := mm.replaceGEChild(val, src, dst)
		// insert it back to group, but we are not sure if they are a global equivalent one, check below.
		// in group merge recursive case, when a re-inserted parent GE has a global equivalent one, we
		// temporarily add them back to group to keep the GE's state.
		parentOwnerG.Insert(reInsertGE)

		existedGE, ok := mm.hash2GlobalGroupExpr.Get(reInsertGE)
		if ok {
			intest.Assert(existedGE.GetGroup() != nil)
			// group expression is already in the Memo's groupExpressions, this indicates that reInsertGE is a redundant
			// group Expression, and it should be removed. With the concern that this redundant group expression may be
			// already in the TaskScheduler stack for some already pushed task types, we should set it be skipped for a signal.
			reInsertGE.SetAbandoned()
			if existedGE.GetGroup().Equals(reInsertGE.GetGroup()) {
				// equiv one and re-insert one are in same group, merge them.
				reInsertGE.mergeTo(existedGE)
			} else {
				// the reinsertGE and existedGE share the same hash64 while not in the same group, it triggers another
				// group merge action upward. we don't do it recursively here, cause parentOwnerG is not state complete yet.
				// register group pair for lazy call.
				lazyCallPair = append(lazyCallPair, &GroupPair{first: reInsertGE.GetGroup(), second: existedGE.GetGroup()})
			}
		} else {
			mm.hash2GlobalGroupExpr.Put(reInsertGE, reInsertGE)
		}
	})
	// step3: merge two groups' element together.
	src.mergeTo(dst)
	// step4: call the lazy call for group merge if any after dst group state is complete.
	for _, pair := range lazyCallPair {
		mm.mergeGroup(pair.first, pair.second)
	}
}

func (mm *Memo) replaceGEChild(ge *GroupExpression, older, newer *Group) *GroupExpression {
	// maintain the old group's parentGEs
	older.removeParentGEs(ge)
	for i, childGroup := range ge.Inputs {
		if childGroup.GetGroupID() == older.GetGroupID() {
			ge.Inputs[i] = newer
		}
	}
	// recompute the hash
	hasher := mm.GetHasher()
	ge.Hash64(hasher)
	ge.hash64 = hasher.Sum64()
	// maintain the new group's parentGEs
	newer.addParentGEs(ge)
	return ge
}

// IteratorLP serves as iterator to get all logical plan inside memo.
type IteratorLP struct {
	root      *Group
	stackInfo []*list.Element
	// traceID is the unique id mark of stepping into a group, traced from the root group as stack calling.
	traceID int
}

// Next return valid logical plan implied in memo without duplication.
func (it *IteratorLP) Next() (logic base.LogicalPlan) {
	for {
		// when non-first time loop here, we should reset traceID back to -1.
		it.traceID = -1
		if len(it.stackInfo) != 0 {
			// when state stack is not empty, we need to pick the next group expression from the top of stack .
			continueGroup := len(it.stackInfo) - 1
			continueGroupElement := it.stackInfo[continueGroup]
			// auto inc gE offset inside group to make sure the next iteration will start from the next group expression.
			it.stackInfo[continueGroup] = continueGroupElement.Next()
		}
		logic = it.dfs(it.root)
		if logic != nil || len(it.stackInfo) == 0 {
			break
		}
	}
	return logic
}

func (it *IteratorLP) dfs(target *Group) base.LogicalPlan {
	// when stepping into a new group, trace the path.
	it.traceIn(target)
	ge := it.pickGroupExpression()
	if ge == nil {
		return nil
	}
	lp := ge.LogicalPlan
	// clean the children to avoid pollution.
	children := lp.Children()[:0]
	for _, childGroup := range ge.Inputs {
		lp := it.dfs(childGroup)
		// one child is nil, quick fail over to iterating next.
		if lp == nil {
			return nil
		}
		children = append(children, lp)
	}
	lp.SetChildren(children...)
	return lp
}

func (it *IteratorLP) traceIn(g *Group) {
	it.traceID++
	// complement the missing stackInfo when stepping into a new group.
	for i := len(it.stackInfo); i <= it.traceID; i++ {
		// for a new stepped-in group, the start iterating index set the first element.
		it.stackInfo = append(it.stackInfo, g.logicalExpressions.Front())
	}
}

// pickGroupExpression tries to find the next matched group expression from the current group.
func (it *IteratorLP) pickGroupExpression() *GroupExpression {
	currentGroup := it.traceID
	currentGroupElement := it.stackInfo[currentGroup]
	if currentGroupElement == nil {
		// current group has been exhausted, pop out the current group trace info(*element thing) from stackInfo.
		it.stackInfo = it.stackInfo[:currentGroup]
		return nil
	}
	// get the current group expression's logical plan
	return currentGroupElement.Value.(*GroupExpression)
}
