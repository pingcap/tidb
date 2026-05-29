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

package core

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChooseBestGreedyStart(t *testing.T) {
	t.Run("pick lowest cost", func(t *testing.T) {
		best, startIdx, err := chooseBestGreedyStart(2, func(startIdx int) (*jrNode, error) {
			costs := []float64{100, 10}
			return &jrNode{cumCost: costs[startIdx]}, nil
		})
		require.NoError(t, err)
		require.NotNil(t, best)
		require.Equal(t, 1, startIdx)
		require.Equal(t, float64(10), best.cumCost)
	})

	t.Run("skip nil candidate", func(t *testing.T) {
		best, startIdx, err := chooseBestGreedyStart(2, func(startIdx int) (*jrNode, error) {
			if startIdx == 0 {
				return nil, nil
			}
			return &jrNode{cumCost: 10}, nil
		})
		require.NoError(t, err)
		require.NotNil(t, best)
		require.Equal(t, 1, startIdx)
		require.Equal(t, float64(10), best.cumCost)
	})

	t.Run("keep earlier start for floating point noise", func(t *testing.T) {
		best, startIdx, err := chooseBestGreedyStart(2, func(startIdx int) (*jrNode, error) {
			costs := []float64{14166.666666666668, 14166.666666666666}
			return &jrNode{cumCost: costs[startIdx]}, nil
		})
		require.NoError(t, err)
		require.NotNil(t, best)
		require.Equal(t, 0, startIdx)
		require.Equal(t, 14166.666666666668, best.cumCost)
	})
}

func TestCloneJRNodesForGreedyStartIsolation(t *testing.T) {
	original := []*jrNode{{cumCost: 7}}
	cloned := cloneJRNodesForGreedyStart(original)
	require.Len(t, cloned, 1)
	require.NotSame(t, original[0], cloned[0])

	cloned[0].cumCost = 42
	require.Equal(t, float64(7), original[0].cumCost)
	require.Equal(t, float64(42), cloned[0].cumCost)
}

func TestMoveGreedyStartToFront(t *testing.T) {
	nodes := []*jrNode{{cumCost: 1}, {cumCost: 2}, {cumCost: 3}}
	reordered := moveGreedyStartToFront(nodes, 1)
	require.Equal(t, []float64{2, 1, 3}, []float64{
		reordered[0].cumCost,
		reordered[1].cumCost,
		reordered[2].cumCost,
	})
}
