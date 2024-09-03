package memo

import (
	"testing"

	"github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/baseimpl"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestGroupHashEquals(t *testing.T) {
	hasher1 := base.NewHashEqualer()
	hasher2 := base.NewHashEqualer()
	a := Group{GroupID: 1}
	b := Group{GroupID: 1}
	a.Hash64(hasher1)
	b.Hash64(hasher2)
	require.Equal(t, hasher1.Sum64(), hasher2.Sum64())
	require.True(t, a.Equals(b))
	require.True(t, a.Equals(&b))

	// change the id.
	b.GroupID = 2
	hasher2.Reset()
	b.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, a.Equals(b))
	require.False(t, a.Equals(&b))
}

func TestGroupExpressionHashEquals(t *testing.T) {
	hasher1 := base.NewHashEqualer()
	hasher2 := base.NewHashEqualer()
	child1 := &Group{GroupID: 1}
	child2 := &Group{GroupID: 2}
	ctx1 := mock.NewContext()
	ctx2 := mock.NewContext()
	a := GroupExpression{
		Group:       &Group{GroupID: 3},
		Children:    []*Group{child1, child2},
		LogicalPlan: &logicalop.BaseLogicalPlan{Plan: baseimpl.NewBasePlan(ctx1, "test", 1)},
	}
	b := GroupExpression{
		// root group should change the hash.
		Group:       &Group{GroupID: 4},
		Children:    []*Group{child1, child2},
		LogicalPlan: &logicalop.BaseLogicalPlan{Plan: baseimpl.NewBasePlan(ctx2, "test", 1)},
	}
	a.Hash64(hasher1)
	b.Hash64(hasher2)
	require.Equal(t, hasher1.Sum64(), hasher2.Sum64())
	require.True(t, a.Equals(b))
	require.True(t, a.Equals(&b))

	// change the children order, like join commutative.
	b.Children = []*Group{child2, child1}
	hasher2.Reset()
	b.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, a.Equals(b))
	require.False(t, a.Equals(&b))
}
