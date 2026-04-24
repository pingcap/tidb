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

package statistics

import (
	"testing"

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

// newFMSketchFromHashValues builds an FM sketch by directly inserting hash values.
// With a large maxSize and few values, the sketch gives exact NDV (mask stays 0).
func newFMSketchFromHashValues(vals ...uint64) *FMSketch {
	s := NewFMSketch(1000)
	for _, v := range vals {
		s.insertHashValue(v)
	}
	return s
}

// newFMSketchesFromSamples builds a pair of sketches from the same sample rows:
// the NDV sketch sees every sample, while the singleton sketch keeps only
// values that occur exactly once in that sample set.
func newFMSketchesFromSamples(t *testing.T, maxSize int, samples ...int64) (*FMSketch, *FMSketch) {
	t.Helper()
	ctx := mock.NewContext()
	ndvSketch := NewFMSketch(maxSize)
	singletonSketch := NewFMSketch(maxSize)
	counts := make(map[int64]int, len(samples))
	for _, v := range samples {
		counts[v]++
		err := ndvSketch.InsertValue(ctx.GetSessionVars().StmtCtx, types.NewIntDatum(v))
		require.NoError(t, err)
	}
	for _, v := range samples {
		if counts[v] != 1 {
			continue
		}
		err := singletonSketch.InsertValue(ctx.GetSessionVars().StmtCtx, types.NewIntDatum(v))
		require.NoError(t, err)
		delete(counts, v)
	}
	return ndvSketch, singletonSketch
}

func TestEstimateGlobalSingletonBySketches(t *testing.T) {
	// Use distinct hash values to represent distinct data values.
	// With maxSize=1000 and few insertions, mask stays 0 so NDV = len(hashset) (exact).
	const (
		a = uint64(100)
		b = uint64(200)
		c = uint64(300)
		d = uint64(400)
		e = uint64(500)
		f = uint64(600)
	)

	t.Run("DocCommentExample", func(t *testing.T) {
		// Node 0: all distinct = {a, b, c}, local singletons = {a, b, c}
		// Node 1: all distinct = {b, c, d}, local singletons = {b, d}
		// Node 2: all distinct = {c, e, f}, local singletons = {e, f}
		// Global singletons = {a, d, e, f} = 4
		ndvSketches := []*FMSketch{
			newFMSketchFromHashValues(a, b, c),
			newFMSketchFromHashValues(b, c, d),
			newFMSketchFromHashValues(c, e, f),
		}
		singletonSketches := []*FMSketch{
			newFMSketchFromHashValues(a, b, c),
			newFMSketchFromHashValues(b, d),
			newFMSketchFromHashValues(e, f),
		}
		got := EstimateGlobalSingletonBySketches(ndvSketches, singletonSketches)
		require.Equal(t, uint64(4), got)
	})

	t.Run("SingleNode", func(t *testing.T) {
		// With one node, all local singletons are global singletons.
		ndvSketches := []*FMSketch{
			newFMSketchFromHashValues(a, b, c),
		}
		singletonSketches := []*FMSketch{
			newFMSketchFromHashValues(a, b, c),
		}
		got := EstimateGlobalSingletonBySketches(ndvSketches, singletonSketches)
		require.Equal(t, uint64(3), got)
	})

	t.Run("NoOverlap", func(t *testing.T) {
		// Nodes have disjoint values. All local singletons are global singletons.
		ndvSketches := []*FMSketch{
			newFMSketchFromHashValues(a, b),
			newFMSketchFromHashValues(c, d),
			newFMSketchFromHashValues(e, f),
		}
		singletonSketches := []*FMSketch{
			newFMSketchFromHashValues(a, b),
			newFMSketchFromHashValues(c, d),
			newFMSketchFromHashValues(e, f),
		}
		got := EstimateGlobalSingletonBySketches(ndvSketches, singletonSketches)
		require.Equal(t, uint64(6), got)
	})

	t.Run("FullOverlap", func(t *testing.T) {
		// Every local singleton also appears in another node's NDV.
		// Node 0: all = {a, b}, singletons = {a, b}
		// Node 1: all = {a, b}, singletons = {a, b}
		// No value is unique to a single node → 0 global singletons.
		ndvSketches := []*FMSketch{
			newFMSketchFromHashValues(a, b),
			newFMSketchFromHashValues(a, b),
		}
		singletonSketches := []*FMSketch{
			newFMSketchFromHashValues(a, b),
			newFMSketchFromHashValues(a, b),
		}
		got := EstimateGlobalSingletonBySketches(ndvSketches, singletonSketches)
		require.Equal(t, uint64(0), got)
	})

	t.Run("NegativeContributionIsClamped", func(t *testing.T) {
		// Both sketches are built from the same local samples.
		//
		// Node 0 samples: [0]
		// Node 1 samples: [0, 0, 0, 1, 1, 4, 7]
		//
		// The true global singleton set is {4, 7}, so the result should be 2.
		// Before the fix, node 0's contribution could become negative due to FM
		// sketch merge behavior, making the final estimate incorrect.
		ndv0, singleton0 := newFMSketchesFromSamples(t, 3, 0)
		ndv1, singleton1 := newFMSketchesFromSamples(t, 3, 0, 0, 0, 1, 1, 4, 7)
		ndvSketches := []*FMSketch{ndv0, ndv1}
		singletonSketches := []*FMSketch{singleton0, singleton1}

		got := EstimateGlobalSingletonBySketches(ndvSketches, singletonSketches)
		require.Equal(t, uint64(2), got)
	})

	t.Run("NilEntry", func(t *testing.T) {
		require.PanicsWithValue(t, "assert failed, ndvSketches must not contain nil entries", func() {
			EstimateGlobalSingletonBySketches(
				[]*FMSketch{nil, newFMSketchFromHashValues(c, d)},
				[]*FMSketch{newFMSketchFromHashValues(a, b), newFMSketchFromHashValues(c, d)},
			)
		})
		require.PanicsWithValue(t, "assert failed, singletonSketches must not contain nil entries", func() {
			EstimateGlobalSingletonBySketches(
				[]*FMSketch{newFMSketchFromHashValues(a, b), newFMSketchFromHashValues(c, d)},
				[]*FMSketch{nil, newFMSketchFromHashValues(c, d)},
			)
		})
	})

	t.Run("EmptyInput", func(t *testing.T) {
		require.PanicsWithValue(t, "assert failed, ndvSketches shouldn't be empty", func() {
			EstimateGlobalSingletonBySketches(nil, nil)
		})
	})

	t.Run("MismatchedLengths", func(t *testing.T) {
		ndvSketches := []*FMSketch{newFMSketchFromHashValues(a)}
		singletonSketches := []*FMSketch{newFMSketchFromHashValues(a), newFMSketchFromHashValues(b)}
		require.PanicsWithValue(t, "assert failed, ndvSketches and singletonSketches should have the same length", func() {
			EstimateGlobalSingletonBySketches(ndvSketches, singletonSketches)
		})
	})
}
