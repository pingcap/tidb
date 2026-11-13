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
	"github.com/pingcap/tidb/pkg/planner/util/coretestsdk"
	"github.com/pingcap/tidb/pkg/util/intset"
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

// TestMakeJoinWithDetectorDeadEnd verifies that makeJoinWithDetector
// returns (nil, nil) when two nodes have no connecting edge — neither
// a real join edge nor a valid cartesian fallback.
// Returning an error in this case would incorrectly abort the caller's
// plan construction, discarding partial results already computed by the
// earlier optimization phase.
func TestMakeJoinWithDetectorDeadEnd(t *testing.T) {
	detector := &ConflictDetector{
		allInnerJoin:  false,
		innerEdges:    []*edge{},
		nonInnerEdges: []*edge{},
	}

	left := &Node{
		bitSet:    intset.NewFastIntSet(0),
		usedEdges: map[uint64]struct{}{},
	}
	right := &Node{
		bitSet:    intset.NewFastIntSet(1),
		usedEdges: map[uint64]struct{}{},
	}

	result, err := makeJoinWithDetector(detector, left, right, nil)
	require.NoError(t, err,
		"makeJoinWithDetector must not return an error when fragments can't connect")
	require.Nil(t, result,
		"makeJoinWithDetector must return nil result when no connecting edge exists and allInnerJoin=false")
}

// TestMakeBushyTreeDeadEnd verifies that makeBushyTree returns (nil, nil)
// when forest fragments cannot be pairwise stitched — real edges do not span
// the current subset S and cartesian fallback is disabled by allInnerJoin=false.
func TestMakeBushyTreeDeadEnd(t *testing.T) {
	ctx := coretestsdk.MockContext()
	t.Cleanup(func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	})

	detector := &ConflictDetector{
		allInnerJoin: false,
		innerEdges: []*edge{
			{tes: intset.NewFastIntSet(0, 1), idx: 0, joinType: base.InnerJoin, skipRules: false},
			{
				tes:           intset.NewFastIntSet(0, 1, 2, 3),
				idx:           2,
				joinType:      base.InnerJoin,
				skipRules:     false,
				leftVertexes:  intset.NewFastIntSet(0, 1),
				rightVertexes: intset.NewFastIntSet(2, 3),
			},
		},
		nonInnerEdges: []*edge{
			{tes: intset.NewFastIntSet(2, 3), idx: 1, joinType: base.LeftOuterJoin, skipRules: false,
				leftVertexes: intset.NewFastIntSet(2), rightVertexes: intset.NewFastIntSet(3)},
		},
	}

	forest := []*Node{
		{bitSet: intset.NewFastIntSet(2, 3), usedEdges: map[uint64]struct{}{1: {}}},
		{bitSet: intset.NewFastIntSet(0), usedEdges: map[uint64]struct{}{}},
		{bitSet: intset.NewFastIntSet(1), usedEdges: map[uint64]struct{}{}},
	}

	result, err := makeBushyTree(ctx, detector, forest, nil, false)
	require.NoError(t, err,
		"makeBushyTree must not return an error when forest fragments can't connect")
	require.Nil(t, result,
		"makeBushyTree must return nil when forest fragments have a dead end")
}
