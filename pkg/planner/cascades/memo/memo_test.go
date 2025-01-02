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
	"testing"
	"unsafe"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestMemo(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/planner/cascades/memo/MockPlanSkipMemoDeriveStats", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/planner/cascades/memo/MockPlanSkipMemoDeriveStats"))
	}()
	ctx := mock.NewContext()
	t1 := logicalop.DataSource{}.Init(ctx, 0)
	t2 := logicalop.DataSource{}.Init(ctx, 0)
	join := logicalop.LogicalJoin{}.Init(ctx, 0)
	join.SetChildren(t1, t2)

	mm := NewMemo()
	mm.Init(join)
	require.Equal(t, 3, mm.GetGroups().Len())
	require.Equal(t, 3, len(mm.GetGroupID2Group()))

	// iter memo.groups to assert group ids.
	cnt := 1
	for e := mm.GetGroups().Front(); e != nil; e = e.Next() {
		group := e.Value.(*Group)
		require.NotNil(t, group)
		require.Equal(t, GroupID(cnt), group.groupID)
		cnt++
	}
}

func TestInsertGE(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/planner/cascades/memo/MockPlanSkipMemoDeriveStats", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/planner/cascades/memo/MockPlanSkipMemoDeriveStats"))
	}()
	ctx := mock.NewContext()
	t1 := logicalop.DataSource{}.Init(ctx, 0)
	t2 := logicalop.DataSource{}.Init(ctx, 0)
	join := logicalop.LogicalJoin{}.Init(ctx, 0)
	join.SetChildren(t1, t2)

	mm := NewMemo()
	mm.Init(join)
	require.Equal(t, 3, mm.GetGroups().Len())
	require.Equal(t, 3, len(mm.GetGroupID2Group()))

	// prepare a new group expression with join's group as its children.
	limit := logicalop.LogicalLimit{}.Init(ctx, 0)
	limit.SetID(-4)
	hasher := mm.GetHasher()
	groupExpr := mm.NewGroupExpression(limit, []*Group{mm.GetRootGroup()})
	groupExpr.Init(hasher)

	// Insert a new group with a new expression.
	mm.InsertGroupExpression(groupExpr, nil)
	require.Equal(t, 4, mm.GetGroups().Len())
	require.Equal(t, 4, len(mm.GetGroupID2Group()))

	// iter memo.groups to assert group ids.
	cnt := 1
	for e := mm.GetGroups().Front(); e != nil; e = e.Next() {
		group := e.Value.(*Group)
		require.NotNil(t, group)
		require.Equal(t, GroupID(cnt), group.GetGroupID())
		cnt++
	}
	require.Equal(t, mm.GetGroups().Back().Value.(*Group).GetGroupID(), GroupID(cnt-1))
}

// TestMergeGroup test the group merge logic inside one memo when detecting two group expression
// are logical equivalent while they are belonged to two different group. for this case:
//
//	--------------------┌ sort3 ┐  (XForm trigger group merge)
//
// ┌────────────────────┼───────┼──┐     ┌──────────────────────┐
// │                    │       ▼  │     │                      │
// │srcParentGroup   dstParentGroup│     │                      │
// │  ┌───────┐         ┌───────┐  │     │  ┌───────┐ ┌───────┐ │
// │  │5      │         │6      │  │     │  │ 5     │ │ 6     │ │
// │  │ limit1│         │ limit2│  │     │  │ limit1│ │ limit2│ │
// │  └───┼───┘         └───┼───┘  │     │  └───┼───┘ └───┼───┘ │
// │Memo  │                 │      ├────►│Memo  └────┼────┘     │
// │  ┌───▼───┐         ┌───▼───┐  │     │       ┌───▼───┐      │
// │  │ 3:src │         │ 4:dst │  │     │       │ 4:dst │      │
// │  │ sort1 │         │ sort2 │  │     │       │ sort1 │      │
// │  │       │         │       │  │     │       │ sort2 │      │
// │  └───┼───┘         └───┼───┘  │     │       └───┼───┘      │
// │      │                 │      │     │           |          │
// │    childG            childG   │     │        childG        │
// └───────────────────────────────┘     └──────────────────────┘
func TestMergeGroup(t *testing.T) {
	mm := NewMemo()
	ctx := mock.NewContext()

	mm.rootGroup = mm.NewGroup()
	childG1 := mm.NewGroup()
	// sort2 is a super set of sort1, but from hash64 we couldn't tell that, only some rules happened to simplify this,
	// and add a new sort3 (simplified as {Expr: expression.NewOne(), Desc: true} as said here) into the dstG, then we
	// can detect this two groups are equivalent via sort3 = sort1 from the hash64, and thus merged two groups.
	sort1 := logicalop.LogicalSort{ByItems: []*util.ByItems{{Expr: expression.NewOne(), Desc: true}}}.Init(ctx, 0)
	sort2 := logicalop.LogicalSort{ByItems: []*util.ByItems{{Expr: expression.NewOne(), Desc: true}, {Expr: expression.NewOne(), Desc: true}}}.Init(ctx, 0)

	srcG := mm.NewGroup()
	mm.InsertGroupExpression(mm.NewGroupExpression(sort1, []*Group{childG1}), srcG)
	dstG := mm.NewGroup()
	mm.InsertGroupExpression(mm.NewGroupExpression(sort2, []*Group{childG1}), dstG)

	// since limit1 and limit2 have different offset, so they are not in the equivalent class at final.
	limit1 := logicalop.LogicalLimit{Offset: 1}.Init(ctx, 0)
	limit2 := logicalop.LogicalLimit{Offset: 2}.Init(ctx, 0)
	srcParentGE := mm.NewGroupExpression(limit1, []*Group{srcG})
	srcParentGroup := mm.NewGroup()
	mm.InsertGroupExpression(srcParentGE, srcParentGroup)

	dstParentGE := mm.NewGroupExpression(limit2, []*Group{dstG})
	dstParentGroup := mm.NewGroup()
	mm.InsertGroupExpression(dstParentGE, dstParentGroup)

	// say we got a sort3 which is simplified from XForm sort2 here, and it will be inserted into equivalent-class group dstG here.
	sort3 := logicalop.LogicalSort{ByItems: []*util.ByItems{{Expr: expression.NewOne(), Desc: true}}}.Init(ctx, 0)
	sort3GE := mm.NewGroupExpression(sort3, []*Group{childG1})
	existedGE, inserted := mm.InsertGroupExpression(sort3GE, dstG)
	require.False(t, inserted)
	require.NotNil(t, existedGE)
	require.Equal(t, existedGE.GetGroup().groupID, GroupID(4))
	require.True(t, existedGE.Inputs[0].Equals(childG1))
	require.Equal(t, existedGE.hash64, sort3GE.hash64)
	require.Equal(t, existedGE.GetGroup().logicalExpressions.Len(), 2)
	existedGE.GetGroup().Check()
	require.Equal(t, existedGE.GetGroup().hash2ParentGroupExpr.Size(), 2)
	mask := [2]bool{}
	existedGE.GetGroup().hash2ParentGroupExpr.Each(func(key unsafe.Pointer, val *GroupExpression) {
		if val.GetGroup().GetGroupID() == srcParentGroup.GetGroupID() {
			mask[0] = true
		}
		if val.GetGroup().GetGroupID() == dstParentGroup.GetGroupID() {
			mask[1] = true
		}
	})
	require.True(t, mask[0])
	require.True(t, mask[1])

	// assert srcG
	require.Equal(t, srcG.hash2ParentGroupExpr.Size(), 0)
	require.Equal(t, srcG.GetLogicalExpressions().Len(), 0)
	require.Equal(t, srcG.hash2GroupExpr.Size(), 0)
	require.Equal(t, len(srcG.Operand2FirstExpr), 0)

	// assert dstG
	require.Equal(t, dstG.hash2ParentGroupExpr.Size(), 2)
	require.Equal(t, dstG.GetLogicalExpressions().Len(), 2)
	require.Equal(t, dstG.hash2GroupExpr.Size(), 2)
	require.Equal(t, len(dstG.Operand2FirstExpr), 1)
	mask = [2]bool{}
	dstG.hash2ParentGroupExpr.Each(func(key unsafe.Pointer, val *GroupExpression) {
		if val.GetGroup().GetGroupID() == srcParentGroup.GetGroupID() {
			mask[0] = true
		}
		if val.GetGroup().GetGroupID() == dstParentGroup.GetGroupID() {
			mask[1] = true
		}
	})
	require.True(t, mask[0])
	require.True(t, mask[1])

	// assert memo
	require.Equal(t, mm.groups.Len(), 5)
	require.Equal(t, len(mm.groupID2Group), 5)
	require.Equal(t, mm.hash2GlobalGroupExpr.Size(), 4)

	// assert childG
	require.Equal(t, childG1.hash2ParentGroupExpr.Size(), 2)
	mask = [2]bool{}
	childG1.hash2ParentGroupExpr.Each(func(key unsafe.Pointer, val *GroupExpression) {
		if val.GetGroup().GetGroupID() == srcG.GetGroupID() {
			mask[0] = true
		}
		if val.GetGroup().GetGroupID() == dstG.GetGroupID() {
			mask[1] = true
		}
	})
	require.False(t, mask[0])
	require.True(t, mask[1])
}

// TestRecursiveMergeGroup test the group merge logic inside one memo when one group
// merge will trigger another group merge inside memo, we hope this kind of recursive
// group merge can work well under this circumstance.
//
//	--------------------┌ sort2 ┐  (XForm trigger group merge)
//
// ┌─────────────rootG──┼───────┼──┐            ┌─────────rootG────────┐             ┌───────rootG───────┐
// │                    │       ▼  │            │                      │             │                   │
// │srcParentGroup   dstParentGroup│            │srcPGroup    dstPGroup│             │     dstPGroup     │
// │  ┌───────┐         ┌───────┐  │            │  ┌───────┐ ┌───────┐ │             │     ┌───────┐     │
// │  │6      │         │7      │  │            │  │ 5     │ │ 6     │ │             │     │ 6     │     │
// │  │ limit1│         │ limit2│  │            │  │ limit1│ │ limit2│ │             │     │ limit2│     │
// │  └───┼───┘         └───┼───┘  │            │  └───┼───┘ └───┼───┘ │             │     └───┼───┘     │
// │Memo  │                 │      ├──G-merge1─►│Memo  └────┼────┘     │──G-merge2─► │  ┌──────▼──────┐  │
// │  ┌───▼───┐         ┌───▼───┐  │            │    ┌──────▼──────┐   │             │  │ 5:dst       │  │
// │  │ 4:src │         │ 5:dst │  │            │    │ 5:dst       │   │             │  │ sort1 sort2 │  │
// │  │ sort1 │         │ sort1 │  │            │    │ sort1 sort2 │   │             │  └───┼─────┼───┘  │
// │  │       │         │       │  │            │    └───┼─────┼───┘   │             │      │     │      │
// │  └───┼───┘         └───┼───┘  │            │        │     │       │             │      │  ┌──┼──┐   │
// │      │             ┌───┼───┐  │            │        │  ┌──┼──┐    │             │      │  │projG│   │
// │      │             │ projG │  │            │        │  │projG│    │             │      │  └──┼──┘   │
// │      │             └───┼───┘  │            │        │  └──┼──┘    │             │      └──┼──┘      │
// │      └───────┼─────────┘      │            │        └──┼──┘       │             │       childG      │
// │            childG             │            │         childG       │             └───────────────────┘
// └───────────────────────────────┘            └──────────────────────┘
// this case is quite like TestMergeGroup, say one rule here eliminate the sort1's children projection
// (meaningless like proj * which is what's childG output) underline, then re-insert sort1 with childG
// back to group:4 like what the pic shows, it will trigger a group merge since between group:3 and group:4.
func TestRecursiveMergeGroup(t *testing.T) {
	mm := NewMemo()
	ctx := mock.NewContext()

	mm.rootGroup = mm.NewGroup()
	childG1 := mm.NewGroup()

	projG2 := mm.NewGroup()
	proj := logicalop.LogicalProjection{Exprs: []expression.Expression{expression.NewOne()}}.Init(ctx, 0)
	mm.InsertGroupExpression(mm.NewGroupExpression(proj, []*Group{childG1}), projG2)
	sort1 := logicalop.LogicalSort{ByItems: []*util.ByItems{{Expr: expression.NewOne(), Desc: true}}}.Init(ctx, 0)

	srcG := mm.NewGroup()
	mm.InsertGroupExpression(mm.NewGroupExpression(sort1, []*Group{childG1}), srcG)
	dstG := mm.NewGroup()
	mm.InsertGroupExpression(mm.NewGroupExpression(sort1, []*Group{projG2}), dstG)

	// since limit1 and limit2 have same offset, once limit1's child group has changed from srcG to dstG, it will trigger
	// another group merge logic upward.
	limit1 := logicalop.LogicalLimit{Offset: 1}.Init(ctx, 0)
	limit2 := logicalop.LogicalLimit{Offset: 1}.Init(ctx, 0)
	srcParentGE := mm.NewGroupExpression(limit1, []*Group{srcG})
	srcParentGroup := mm.NewGroup()
	mm.InsertGroupExpression(srcParentGE, srcParentGroup)

	dstParentGE := mm.NewGroupExpression(limit2, []*Group{dstG})
	dstParentGroup := mm.NewGroup()
	mm.InsertGroupExpression(dstParentGE, dstParentGroup)

	// say we got a sort2 which is simplified from XForm sort1 in dstG here, and it just eliminates the underlying proj, and
	// make sort2 linked with childG directly, which will be re-insert to dstG in return, it will trigger the group merge logic
	// as a start.
	sort2 := logicalop.LogicalSort{ByItems: []*util.ByItems{{Expr: expression.NewOne(), Desc: true}}}.Init(ctx, 0)
	sort2GE := mm.NewGroupExpression(sort2, []*Group{childG1})
	existedGE, inserted := mm.InsertGroupExpression(sort2GE, dstG)
	require.False(t, inserted)
	require.NotNil(t, existedGE)
	require.Equal(t, existedGE.GetGroup().groupID, GroupID(5))
	require.True(t, existedGE.Inputs[0].Equals(childG1))
	require.Equal(t, existedGE.hash64, sort2GE.hash64)
	require.Equal(t, existedGE.GetGroup().logicalExpressions.Len(), 2)
	existedGE.GetGroup().Check()
	require.Equal(t, existedGE.GetGroup().hash2ParentGroupExpr.Size(), 1)
	var mask bool
	existedGE.GetGroup().hash2ParentGroupExpr.Each(func(key unsafe.Pointer, val *GroupExpression) {
		if val.GetGroup().GetGroupID() == dstParentGroup.GetGroupID() {
			mask = true
		}
	})
	require.True(t, mask)

	// assert srcG
	require.Equal(t, srcG.hash2ParentGroupExpr.Size(), 0)
	require.Equal(t, srcG.GetLogicalExpressions().Len(), 0)
	require.Equal(t, srcG.hash2GroupExpr.Size(), 0)
	require.Equal(t, len(srcG.Operand2FirstExpr), 0)

	// assert dstG
	require.Equal(t, dstG.hash2ParentGroupExpr.Size(), 1)
	require.Equal(t, dstG.GetLogicalExpressions().Len(), 2)
	require.Equal(t, dstG.hash2GroupExpr.Size(), 2)
	require.Equal(t, len(dstG.Operand2FirstExpr), 1)
	mask = false
	dstG.hash2ParentGroupExpr.Each(func(key unsafe.Pointer, val *GroupExpression) {
		if val.GetGroup().GetGroupID() == dstParentGroup.GetGroupID() {
			mask = true
		}
	})
	require.True(t, mask)

	// assert memo
	require.Equal(t, mm.groups.Len(), 5)
	require.Equal(t, len(mm.groupID2Group), 5)
	require.Equal(t, mm.hash2GlobalGroupExpr.Size(), 4)

	// assert srcParentGroup and dstParentGroup
	srcParentGroup.Check()
	dstParentGroup.Check()
	require.Equal(t, srcParentGroup.hash2ParentGroupExpr.Size(), 0)
	require.Equal(t, srcParentGroup.hash2GroupExpr.Size(), 0)
	require.Equal(t, dstParentGroup.hash2ParentGroupExpr.Size(), 0)
	require.Equal(t, dstParentGroup.hash2GroupExpr.Size(), 1)
	dstParentGroup.hash2GroupExpr.Each(func(key *GroupExpression, val *list.Element) {
		require.True(t, key.LogicalPlan == limit2)
	})

	// assert projG
	projG2.Check()
	require.Equal(t, projG2.hash2ParentGroupExpr.Size(), 1)
	projG2.hash2ParentGroupExpr.Each(func(key unsafe.Pointer, val *GroupExpression) {
		if val.GetGroup().GetGroupID() == dstG.GetGroupID() {
			mask = true
		}
	})
	require.True(t, mask)

	// assert childG
	childG1.Check()
	require.Equal(t, childG1.hash2ParentGroupExpr.Size(), 2)
	mask2 := [2]bool{}
	childG1.hash2ParentGroupExpr.Each(func(key unsafe.Pointer, val *GroupExpression) {
		if val.GetGroup().GetGroupID() == srcG.GetGroupID() {
			mask2[0] = true
		}
		if val.GetGroup().GetGroupID() == dstG.GetGroupID() {
			mask2[1] = true
		}
		_, ok := dstG.hash2GroupExpr.Get(val)
		_, ok2 := projG2.hash2GroupExpr.Get(val)
		require.True(t, ok || ok2)
	})
	require.False(t, mask2[0])
	require.True(t, mask2[1])
}
