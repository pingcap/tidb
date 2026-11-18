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

package joinorder

import (
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/coretestsdk"
	"github.com/stretchr/testify/require"
)

func TestChooseBestGreedyStart(t *testing.T) {
	t.Run("pick lowest cost", func(t *testing.T) {
		best, startIdx, err := chooseBestGreedyStart(2, func(startIdx int) (*Node, error) {
			costs := []float64{100, 10}
			return &Node{cumCost: costs[startIdx]}, nil
		})
		require.NoError(t, err)
		require.NotNil(t, best)
		require.Equal(t, 1, startIdx)
		require.Equal(t, float64(10), best.cumCost)
	})

	t.Run("skip nil candidate", func(t *testing.T) {
		best, startIdx, err := chooseBestGreedyStart(2, func(startIdx int) (*Node, error) {
			if startIdx == 0 {
				return nil, nil
			}
			return &Node{cumCost: 10}, nil
		})
		require.NoError(t, err)
		require.NotNil(t, best)
		require.Equal(t, 1, startIdx)
		require.Equal(t, float64(10), best.cumCost)
	})

	t.Run("keep earlier start for floating point noise", func(t *testing.T) {
		best, startIdx, err := chooseBestGreedyStart(2, func(startIdx int) (*Node, error) {
			costs := []float64{14166.666666666668, 14166.666666666666}
			return &Node{cumCost: costs[startIdx]}, nil
		})
		require.NoError(t, err)
		require.NotNil(t, best)
		require.Equal(t, 0, startIdx)
		require.Equal(t, 14166.666666666668, best.cumCost)
	})
}

func TestCloneNodesForGreedyStartIsolation(t *testing.T) {
	ctx := coretestsdk.MockContext()
	t.Cleanup(func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	})

	original := []*Node{{
		cumCost:   7,
		usedEdges: map[uint64]struct{}{1: {}},
	}}
	cloned := cloneNodesForGreedyStart(original)
	require.Len(t, cloned, 1)
	require.NotSame(t, original[0], cloned[0])

	delete(cloned[0].usedEdges, 1)
	cloned[0].usedEdges[2] = struct{}{}
	require.Contains(t, original[0].usedEdges, uint64(1))
	require.NotContains(t, original[0].usedEdges, uint64(2))

	cloned[0].p = logicalop.LogicalTableDual{RowCount: 1}.Init(ctx, 0)
	require.Nil(t, original[0].p)
	require.NotNil(t, cloned[0].p)
}

func TestJoinTreeShape(t *testing.T) {
	ctx := coretestsdk.MockContext()
	t.Cleanup(func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	})

	input := logicalop.LogicalTableDual{}.Init(ctx, 0)
	input.SetStats(&property.StatsInfo{RowCount: 8_000_000})
	agg := logicalop.LogicalAggregation{}.Init(ctx, 0)
	agg.SetChildren(input)
	agg.SetStats(&property.StatsInfo{RowCount: 10})
	require.Equal(t, 8_000_010.0, LegacyLeafCost(agg))
	require.Equal(t, 10.0, outputRowsLeafCost(agg))

	a := logicalop.LogicalTableDual{}.Init(ctx, 0)
	b := logicalop.LogicalTableDual{}.Init(ctx, 0)
	group := &joinGroup{vertexes: []base.LogicalPlan{a, b, agg}}
	join := func(tp base.JoinType, left, right base.LogicalPlan) base.LogicalPlan {
		j := logicalop.LogicalJoin{JoinType: tp}.Init(ctx, 0)
		j.SetChildren(left, right)
		return j
	}
	shape := func(p base.LogicalPlan) string { return joinTreeShape(group, p) }
	abc := shape(join(base.InnerJoin, join(base.InnerJoin, a, b), agg))
	require.Equal(t, "inner join(2,inner join(0,1))", abc)
	// The physical optimizer tries both sides of an inner join.
	require.Equal(t, abc, shape(join(base.InnerJoin, agg, join(base.InnerJoin, b, a))))
	require.NotEqual(t, abc, shape(join(base.InnerJoin, join(base.InnerJoin, a, agg), b)))
	require.NotEqual(t,
		shape(join(base.LeftOuterJoin, join(base.InnerJoin, a, b), agg)),
		shape(join(base.LeftOuterJoin, agg, join(base.InnerJoin, a, b))))
}
