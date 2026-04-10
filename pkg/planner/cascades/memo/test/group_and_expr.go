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

package memo_test

import (
	"container/list"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/planner/cascades/memo"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	"github.com/zyedidia/generic/hashmap"
)

func RunRawHashMap(t *testing.T) {
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

func RunGroupExpressionHashCollision(t *testing.T) {
	child1 := &memo.ExportedGroup{}
	memo.ExportedSetGroupID(child1, 1)
	child2 := &memo.ExportedGroup{}
	memo.ExportedSetGroupID(child2, 2)
	a := &memo.ExportedGroupExpression{
		Inputs:      []*memo.ExportedGroup{child1, child2},
		LogicalPlan: &logicalop.LogicalProjection{Exprs: []expression.Expression{expression.NewOne()}},
	}
	b := &memo.ExportedGroupExpression{
		// root group should change the hash.
		Inputs:      []*memo.ExportedGroup{child2, child1},
		LogicalPlan: &logicalop.LogicalProjection{Exprs: []expression.Expression{expression.NewOne()}},
	}
	// manually set this two group expression's hash64 to be the same to mock hash collision while equals is not.
	memo.ExportedSetGroupExpressionHash64(a, 1)
	memo.ExportedSetGroupExpressionHash64(b, 1)
	root := memo.ExportedNewGroup(nil)
	memo.ExportedSetGroupID(root, 5)
	require.True(t, root.Insert(a))
	require.True(t, root.Insert(b))
	require.Equal(t, memo.ExportedGetLogicalExpressions(root).Len(), 2)

	res, ok := memo.ExportedGetHash2GroupExpr(root).Get(a)
	require.True(t, ok)
	require.Equal(t, memo.ExportedGetGroupExpressionHash64(res.Value.(*memo.ExportedGroupExpression)), uint64(1))
	require.Equal(t, memo.ExportedGetGroupID(memo.ExportedGetGroupExpressionGroup(res.Value.(*memo.ExportedGroupExpression))), memo.ExportedGroupID(5))
	require.Equal(t, memo.ExportedGetGroupID(memo.ExportedGetGroupExpressionInputs(res.Value.(*memo.ExportedGroupExpression))[0]), memo.ExportedGroupID(1))
	require.Equal(t, memo.ExportedGetGroupID(memo.ExportedGetGroupExpressionInputs(res.Value.(*memo.ExportedGroupExpression))[1]), memo.ExportedGroupID(2))

	res, ok = memo.ExportedGetHash2GroupExpr(root).Get(b)
	require.True(t, ok)
	require.Equal(t, memo.ExportedGetGroupExpressionHash64(res.Value.(*memo.ExportedGroupExpression)), uint64(1))
	require.Equal(t, memo.ExportedGetGroupID(memo.ExportedGetGroupExpressionGroup(res.Value.(*memo.ExportedGroupExpression))), memo.ExportedGroupID(5))
	require.Equal(t, memo.ExportedGetGroupID(memo.ExportedGetGroupExpressionInputs(res.Value.(*memo.ExportedGroupExpression))[0]), memo.ExportedGroupID(2))
	require.Equal(t, memo.ExportedGetGroupID(memo.ExportedGetGroupExpressionInputs(res.Value.(*memo.ExportedGroupExpression))[1]), memo.ExportedGroupID(1))
}

func RunGroupExpressionDelete(t *testing.T) {
	hasher1 := base.NewHashEqualer()
	hasher2 := base.NewHashEqualer()
	child1 := &memo.ExportedGroup{}
	memo.ExportedSetGroupID(child1, 1)
	child2 := &memo.ExportedGroup{}
	memo.ExportedSetGroupID(child2, 2)
	a := &memo.ExportedGroupExpression{
		Inputs:      []*memo.ExportedGroup{child1, child2},
		LogicalPlan: &logicalop.LogicalProjection{Exprs: []expression.Expression{expression.NewOne()}},
	}
	b := &memo.ExportedGroupExpression{
		// root group should change the hash.
		Inputs:      []*memo.ExportedGroup{child2, child1},
		LogicalPlan: &logicalop.LogicalProjection{Exprs: []expression.Expression{expression.NewOne()}},
	}
	a.Hash64(hasher1)
	memo.ExportedSetGroupExpressionHash64(a, hasher1.Sum64())
	b.Hash64(hasher2)
	memo.ExportedSetGroupExpressionHash64(b, hasher2.Sum64())
	root := memo.ExportedNewGroup(nil)
	memo.ExportedSetGroupID(root, 3)
	require.True(t, root.Insert(a))
	require.True(t, root.Insert(b))
	require.Equal(t, memo.ExportedGetLogicalExpressions(root).Len(), 2)

	mock := &memo.ExportedGroupExpression{
		Inputs:      []*memo.ExportedGroup{child1},
		LogicalPlan: &logicalop.LogicalProjection{Exprs: []expression.Expression{expression.NewOne()}},
	}
	hasher1.Reset()
	mock.Hash64(hasher1)
	memo.ExportedSetGroupExpressionHash64(mock, hasher1.Sum64())

	root.Delete(mock)
	require.Equal(t, memo.ExportedGetLogicalExpressions(root).Len(), 2)

	root.Delete(a)
	require.Equal(t, memo.ExportedGetLogicalExpressions(root).Len(), 1)
	require.Equal(t, root.GetLogicalExpressions().Front().Value.(*memo.ExportedGroupExpression), b)

	root.Delete(b)
	require.Equal(t, memo.ExportedGetLogicalExpressions(root).Len(), 0)
	require.Equal(t, root.GetLogicalExpressions().Len(), 0)
}

func RunGroupHashEquals(t *testing.T) {
	hasher1 := base.NewHashEqualer()
	hasher2 := base.NewHashEqualer()
	a := memo.ExportedGroup{}
	memo.ExportedSetGroupID(&a, 1)
	b := memo.ExportedGroup{}
	memo.ExportedSetGroupID(&b, 1)
	a.Hash64(hasher1)
	b.Hash64(hasher2)
	require.Equal(t, hasher1.Sum64(), hasher2.Sum64())
	require.True(t, a.Equals(&b))
	require.True(t, (&a).Equals(&b))
	require.False(t, a.Equals(b))
	require.False(t, (&a).Equals(b))
	// change the id.
	memo.ExportedSetGroupID(&b, 2)
	hasher2.Reset()
	b.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, a.Equals(&b))
	require.False(t, (&a).Equals(&b))
}

func RunGroupExpressionHashEquals(t *testing.T) {
	hasher1 := base.NewHashEqualer()
	hasher2 := base.NewHashEqualer()
	child1 := &memo.ExportedGroup{}
	memo.ExportedSetGroupID(child1, 1)
	child2 := &memo.ExportedGroup{}
	memo.ExportedSetGroupID(child2, 2)
	groupA := &memo.ExportedGroup{}
	memo.ExportedSetGroupID(groupA, 3)
	groupB := &memo.ExportedGroup{}
	memo.ExportedSetGroupID(groupB, 4)
	a := memo.ExportedNewGroupExpressionWithGroup(groupA, []*memo.ExportedGroup{child1, child2}, &logicalop.LogicalProjection{Exprs: []expression.Expression{expression.NewOne()}})
	b := memo.ExportedNewGroupExpressionWithGroup(groupB, []*memo.ExportedGroup{child1, child2}, &logicalop.LogicalProjection{Exprs: []expression.Expression{expression.NewOne()}})
	a.Hash64(hasher1)
	b.Hash64(hasher2)
	require.Equal(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, a.Equals(b))
	require.True(t, a.Equals(&b))

	// change the children order, like join commutative.
	b.Inputs = []*memo.ExportedGroup{child2, child1}
	hasher2.Reset()
	b.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, a.Equals(b))
	require.False(t, a.Equals(&b))
}

func RunGroupParentGERefs(t *testing.T) {
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

	mm := memo.ExportedNewMemo()
	mm.Init(join)
	require.Equal(t, 3, mm.GetGroups().Len())
	require.Equal(t, 3, len(mm.GetGroupID2Group()))

	require.Equal(t, memo.ExportedGetHash2ParentGroupExpr(memo.ExportedMemoRootGroup(mm)).Size(), 0)
	require.Equal(t, memo.ExportedGetHash2GroupExpr(memo.ExportedMemoRootGroup(mm)).Size(), 1)
	var (
		j, j1, j2 *memo.ExportedGroupExpression
		elem      *list.Element
	)
	memo.ExportedGetHash2GroupExpr(memo.ExportedMemoRootGroup(mm)).Each(func(key *memo.ExportedGroupExpression, val *list.Element) {
		j = key
		elem = val
	})
	require.NotNil(t, elem)
	require.NotNil(t, j)
	require.Equal(t, elem.Value.(*memo.ExportedGroupExpression), j)
	require.Equal(t, memo.ExportedGetLogicalExpressions(memo.ExportedMemoRootGroup(mm)).Len(), 1)
	require.Equal(t, memo.ExportedGetLogicalExpressions(memo.ExportedMemoRootGroup(mm)).Front(), elem)
	require.True(t, j.LogicalPlan.Equals(join))

	// left child group
	leftGroup := memo.ExportedGetGroupExpressionInputs(j)[0]
	require.Equal(t, memo.ExportedGetHash2ParentGroupExpr(leftGroup).Size(), 1)
	ge, ok := memo.ExportedGetHash2ParentGroupExpr(leftGroup).Get(memo.ExportedGetGroupExpressionAddr(j))
	require.True(t, ok)
	require.NotNil(t, ge)
	require.Equal(t, memo.ExportedGetHash2GroupExpr(leftGroup).Size(), 1)
	memo.ExportedGetHash2GroupExpr(leftGroup).Each(func(key *memo.ExportedGroupExpression, val *list.Element) {
		j1 = key
		elem = val
	})
	require.NotNil(t, elem)
	require.NotNil(t, j1)
	require.Equal(t, elem.Value.(*memo.ExportedGroupExpression), j1)
	require.True(t, j1.LogicalPlan.Equals(t1))

	// right child group
	rightGroup := memo.ExportedGetGroupExpressionInputs(j)[1]
	require.Equal(t, memo.ExportedGetHash2ParentGroupExpr(rightGroup).Size(), 1)
	ge, ok = memo.ExportedGetHash2ParentGroupExpr(rightGroup).Get(memo.ExportedGetGroupExpressionAddr(j))
	require.True(t, ok)
	require.NotNil(t, ge)
	require.Equal(t, memo.ExportedGetHash2GroupExpr(rightGroup).Size(), 1)
	memo.ExportedGetHash2GroupExpr(rightGroup).Each(func(key *memo.ExportedGroupExpression, val *list.Element) {
		j2 = key
		elem = val
	})
	require.NotNil(t, elem)
	require.NotNil(t, j2)
	require.Equal(t, elem.Value.(*memo.ExportedGroupExpression), j2)
	require.True(t, j2.LogicalPlan.Equals(t2))

	// assert global memo
	require.Equal(t, mm.GetGroups().Len(), 3)
	require.Equal(t, memo.ExportedMemoGetHash2GlobalGroupExpr(mm).Size(), 3)
	found := [3]bool{}
	memo.ExportedMemoGetHash2GlobalGroupExpr(mm).Each(func(key *memo.ExportedGroupExpression, val *memo.ExportedGroupExpression) {
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
