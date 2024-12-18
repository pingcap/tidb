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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	"github.com/zyedidia/generic/hashmap"
)

func TestRawHashMap(t *testing.T) {
	type A struct {
		a uint64
		s string
	}
	hash2GroupExpr := hashmap.New[*A, *A](
		4,
		func(a, b *A) bool {
			return a.a == b.a && a.s == b.s
		},
		func(t *A) uint64 {
			return t.a
		})
	a1 := &A{1, "1"}
	hash2GroupExpr.Put(a1, a1)
	res, ok := hash2GroupExpr.Get(a1)
	require.True(t, ok)
	require.Equal(t, res.a, uint64(1))
	require.Equal(t, res.s, "1")

	a2 := &A{1, "2"}
	hash2GroupExpr.Put(a2, a2)
	require.Equal(t, hash2GroupExpr.Size(), 2)

	res, ok = hash2GroupExpr.Get(a2)
	require.True(t, ok)
	require.Equal(t, res.a, uint64(1))
	require.Equal(t, res.s, "2")
}

func TestGroupExpressionHashCollision(t *testing.T) {
	child1 := &Group{groupID: 1}
	child2 := &Group{groupID: 2}
	a := &GroupExpression{
		Inputs:      []*Group{child1, child2},
		LogicalPlan: &logicalop.LogicalProjection{Exprs: []expression.Expression{expression.NewOne()}},
	}
	b := &GroupExpression{
		// root group should change the hash.
		Inputs:      []*Group{child2, child1},
		LogicalPlan: &logicalop.LogicalProjection{Exprs: []expression.Expression{expression.NewOne()}},
	}
	// manually set this two group expression's hash64 to be the same to mock hash collision while equals is not.
	a.hash64 = 1
	b.hash64 = 1
	root := NewGroup(nil)
	root.groupID = 5
	require.True(t, root.Insert(a))
	require.True(t, root.Insert(b))
	require.Equal(t, root.logicalExpressions.Len(), 2)

	res, ok := root.hash2GroupExpr.Get(a)
	require.True(t, ok)
	require.Equal(t, res.Value.(*GroupExpression).hash64, uint64(1))
	require.Equal(t, res.Value.(*GroupExpression).group.groupID, GroupID(5))
	require.Equal(t, res.Value.(*GroupExpression).Inputs[0].groupID, GroupID(1))
	require.Equal(t, res.Value.(*GroupExpression).Inputs[1].groupID, GroupID(2))

	res, ok = root.hash2GroupExpr.Get(b)
	require.True(t, ok)
	require.Equal(t, res.Value.(*GroupExpression).hash64, uint64(1))
	require.Equal(t, res.Value.(*GroupExpression).group.groupID, GroupID(5))
	require.Equal(t, res.Value.(*GroupExpression).Inputs[0].groupID, GroupID(2))
	require.Equal(t, res.Value.(*GroupExpression).Inputs[1].groupID, GroupID(1))
}

func TestGroupExpressionDelete(t *testing.T) {
	hasher1 := base.NewHashEqualer()
	hasher2 := base.NewHashEqualer()
	child1 := &Group{groupID: 1}
	child2 := &Group{groupID: 2}
	a := &GroupExpression{
		Inputs:      []*Group{child1, child2},
		LogicalPlan: &logicalop.LogicalProjection{Exprs: []expression.Expression{expression.NewOne()}},
	}
	b := &GroupExpression{
		// root group should change the hash.
		Inputs:      []*Group{child2, child1},
		LogicalPlan: &logicalop.LogicalProjection{Exprs: []expression.Expression{expression.NewOne()}},
	}
	a.Hash64(hasher1)
	a.hash64 = hasher1.Sum64()
	b.Hash64(hasher2)
	b.hash64 = hasher2.Sum64()
	root := NewGroup(nil)
	root.groupID = 3
	require.True(t, root.Insert(a))
	require.True(t, root.Insert(b))
	require.Equal(t, root.logicalExpressions.Len(), 2)

	mock := &GroupExpression{
		Inputs:      []*Group{child1},
		LogicalPlan: &logicalop.LogicalProjection{Exprs: []expression.Expression{expression.NewOne()}},
	}
	hasher1.Reset()
	mock.Hash64(hasher1)
	mock.hash64 = hasher1.Sum64()

	root.Delete(mock)
	require.Equal(t, root.logicalExpressions.Len(), 2)

	root.Delete(a)
	require.Equal(t, root.logicalExpressions.Len(), 1)
	require.Equal(t, root.GetLogicalExpressions().Front().Value.(*GroupExpression), b)

	root.Delete(b)
	require.Equal(t, root.logicalExpressions.Len(), 0)
	require.Equal(t, root.GetLogicalExpressions().Len(), 0)
}

func TestGroupHashEquals(t *testing.T) {
	hasher1 := base.NewHashEqualer()
	hasher2 := base.NewHashEqualer()
	a := Group{groupID: 1}
	b := Group{groupID: 1}
	a.Hash64(hasher1)
	b.Hash64(hasher2)
	require.Equal(t, hasher1.Sum64(), hasher2.Sum64())
	require.True(t, a.Equals(&b))
	require.True(t, (&a).Equals(&b))
	require.False(t, a.Equals(b))
	require.False(t, (&a).Equals(b))
	// change the id.
	b.groupID = 2
	hasher2.Reset()
	b.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, a.Equals(&b))
	require.False(t, (&a).Equals(&b))
}

func TestGroupExpressionHashEquals(t *testing.T) {
	hasher1 := base.NewHashEqualer()
	hasher2 := base.NewHashEqualer()
	child1 := &Group{groupID: 1}
	child2 := &Group{groupID: 2}
	a := GroupExpression{
		group:       &Group{groupID: 3},
		Inputs:      []*Group{child1, child2},
		LogicalPlan: &logicalop.LogicalProjection{Exprs: []expression.Expression{expression.NewOne()}},
	}
	b := GroupExpression{
		// root group should change the hash.
		group:       &Group{groupID: 4},
		Inputs:      []*Group{child1, child2},
		LogicalPlan: &logicalop.LogicalProjection{Exprs: []expression.Expression{expression.NewOne()}},
	}
	a.Hash64(hasher1)
	b.Hash64(hasher2)
	require.Equal(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, a.Equals(b))
	require.True(t, a.Equals(&b))

	// change the children order, like join commutative.
	b.Inputs = []*Group{child2, child1}
	hasher2.Reset()
	b.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, a.Equals(b))
	require.False(t, a.Equals(&b))
}

func TestGroupParentGERefs(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/planner/cascades/memo/MockPlanSkipMemoDeriveStats", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/planner/cascades/memo/MockPlanSkipMemoDeriveStats"))
	}()
	col1 := &expression.Column{
		UniqueID: 1,
	}
	col2 := &expression.Column{
		UniqueID: 2,
	}
	ctx := mock.NewContext()
	t1 := logicalop.DataSource{}.Init(ctx, 0)
	t1.SetSchema(expression.NewSchema(col1))
	t2 := logicalop.DataSource{}.Init(ctx, 0)
	t2.SetSchema(expression.NewSchema(col2))
	join := logicalop.LogicalJoin{}.Init(ctx, 0)
	join.SetSchema(expression.NewSchema(col1, col2))
	join.SetChildren(t1, t2)

	mm := NewMemo()
	mm.Init(join)
	require.Equal(t, 3, mm.GetGroups().Len())
	require.Equal(t, 3, len(mm.GetGroupID2Group()))

	require.Equal(t, mm.rootGroup.hash2ParentGroupExpr.Size(), 0)
	require.Equal(t, mm.rootGroup.hash2GroupExpr.Size(), 1)
	var (
		j, j1, j2 *GroupExpression
		elem      *list.Element
	)
	mm.rootGroup.hash2GroupExpr.Each(func(key *GroupExpression, val *list.Element) {
		j = key
		elem = val
	})
	require.NotNil(t, elem)
	require.NotNil(t, j)
	require.Equal(t, elem.Value.(*GroupExpression), j)
	require.Equal(t, mm.rootGroup.logicalExpressions.Len(), 1)
	require.Equal(t, mm.rootGroup.logicalExpressions.Front(), elem)
	require.True(t, j.LogicalPlan.Equals(join))

	// left child group
	leftGroup := j.Inputs[0]
	require.Equal(t, leftGroup.hash2ParentGroupExpr.Size(), 1)
	ge, ok := leftGroup.hash2ParentGroupExpr.Get(j.addr())
	require.True(t, ok)
	require.NotNil(t, ge)
	require.Equal(t, leftGroup.hash2GroupExpr.Size(), 1)
	leftGroup.hash2GroupExpr.Each(func(key *GroupExpression, val *list.Element) {
		j1 = key
		elem = val
	})
	require.NotNil(t, elem)
	require.NotNil(t, j1)
	require.Equal(t, elem.Value.(*GroupExpression), j1)
	require.True(t, j1.LogicalPlan.Equals(t1))

	// right child group
	rightGroup := j.Inputs[1]
	require.Equal(t, rightGroup.hash2ParentGroupExpr.Size(), 1)
	ge, ok = rightGroup.hash2ParentGroupExpr.Get(j.addr())
	require.True(t, ok)
	require.NotNil(t, ge)
	require.Equal(t, rightGroup.hash2GroupExpr.Size(), 1)
	rightGroup.hash2GroupExpr.Each(func(key *GroupExpression, val *list.Element) {
		j2 = key
		elem = val
	})
	require.NotNil(t, elem)
	require.NotNil(t, j2)
	require.Equal(t, elem.Value.(*GroupExpression), j2)
	require.True(t, j2.LogicalPlan.Equals(t2))

	// assert global memo
	require.Equal(t, mm.groups.Len(), 3)
	require.Equal(t, mm.hash2GlobalGroupExpr.Size(), 3)
	found := [3]bool{}
	mm.hash2GlobalGroupExpr.Each(func(key *GroupExpression, val *GroupExpression) {
		if key.Equals(j) {
			found[0] = true
		}
		if key.Equals(j1) {
			found[1] = true
		}
		if key.Equals(j2) {
			found[2] = true
		}
	})
	require.True(t, found[0])
	require.True(t, found[1])
	require.True(t, found[2])
}
