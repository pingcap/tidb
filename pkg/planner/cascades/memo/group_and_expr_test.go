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
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
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
