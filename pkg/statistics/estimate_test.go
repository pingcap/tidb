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

	"github.com/stretchr/testify/require"
)

// hllValue maps a small id to a 64-bit hash that lands in a distinct HLL register
// (top 14 bits = id) with rank 1. Distinct ids therefore occupy distinct registers,
// so an HLL over k such values estimates exactly k via linear counting. This keeps
// the leave-one-out assertions exact while still exercising the real HLL path.
func hllValue(id uint64) uint64 {
	return (id << 50) | (1 << 49)
}

// newHLLFromHashValues builds an HLL by inserting the given hashes.
func newHLLFromHashValues(vals ...uint64) *HLL {
	h := NewHLL(DefaultHLLPrecision)
	for _, v := range vals {
		h.InsertHash(v)
	}
	return h
}

func TestEstimateGlobalNDVBySketches(t *testing.T) {
	var (
		a = hllValue(0)
		b = hllValue(1)
		c = hllValue(2)
		d = hllValue(3)
		e = hllValue(4)
		f = hllValue(5)
	)

	t.Run("UnionAcrossRegions", func(t *testing.T) {
		// {a,b,c} ∪ {b,c,d} ∪ {c,e,f} = {a,b,c,d,e,f} = 6 distinct.
		ndvSketches := []*HLL{
			newHLLFromHashValues(a, b, c),
			newHLLFromHashValues(b, c, d),
			newHLLFromHashValues(c, e, f),
		}
		require.Equal(t, uint64(6), EstimateGlobalNDVBySketches(ndvSketches))
	})

	t.Run("SingleRegion", func(t *testing.T) {
		require.Equal(t, uint64(3), EstimateGlobalNDVBySketches([]*HLL{newHLLFromHashValues(a, b, c)}))
	})

	t.Run("DoesNotMutateInputs", func(t *testing.T) {
		// The union must clone the first sketch rather than merge into it: the same
		// slice is passed on to the singleton estimator, so mutation would corrupt f1.
		s0 := newHLLFromHashValues(a, b)
		s1 := newHLLFromHashValues(c, d)
		require.Equal(t, uint64(4), EstimateGlobalNDVBySketches([]*HLL{s0, s1}))
		require.Equal(t, uint64(2), s0.Count())
		require.Equal(t, uint64(2), s1.Count())
	})
}

func TestEstimateGlobalSingletonBySketches(t *testing.T) {
	// Distinct ids → distinct registers, so tiny-set counts are exact.
	var (
		a = hllValue(0)
		b = hllValue(1)
		c = hllValue(2)
		d = hllValue(3)
		e = hllValue(4)
		f = hllValue(5)
	)

	t.Run("DocCommentExample", func(t *testing.T) {
		// Region 0: all distinct = {a, b, c}, local singletons = {a, b, c}
		// Region 1: all distinct = {b, c, d}, local singletons = {b, d}
		// Region 2: all distinct = {c, e, f}, local singletons = {e, f}
		// Global singletons = {a, d, e, f} = 4
		ndvSketches := []*HLL{
			newHLLFromHashValues(a, b, c),
			newHLLFromHashValues(b, c, d),
			newHLLFromHashValues(c, e, f),
		}
		singletonSketches := []*HLL{
			newHLLFromHashValues(a, b, c),
			newHLLFromHashValues(b, d),
			newHLLFromHashValues(e, f),
		}
		got := EstimateGlobalSingletonBySketches(ndvSketches, singletonSketches)
		require.Equal(t, uint64(4), got)
	})

	t.Run("SingleRegion", func(t *testing.T) {
		// With one region, all local singletons are global singletons.
		ndvSketches := []*HLL{newHLLFromHashValues(a, b, c)}
		singletonSketches := []*HLL{newHLLFromHashValues(a, b, c)}
		got := EstimateGlobalSingletonBySketches(ndvSketches, singletonSketches)
		require.Equal(t, uint64(3), got)
	})

	t.Run("NoOverlap", func(t *testing.T) {
		// Regions have disjoint values. All local singletons are global singletons.
		ndvSketches := []*HLL{
			newHLLFromHashValues(a, b),
			newHLLFromHashValues(c, d),
			newHLLFromHashValues(e, f),
		}
		singletonSketches := []*HLL{
			newHLLFromHashValues(a, b),
			newHLLFromHashValues(c, d),
			newHLLFromHashValues(e, f),
		}
		got := EstimateGlobalSingletonBySketches(ndvSketches, singletonSketches)
		require.Equal(t, uint64(6), got)
	})

	t.Run("FullOverlap", func(t *testing.T) {
		// Every local singleton also appears in another region's NDV, so no value is
		// unique to a single region → 0 global singletons.
		ndvSketches := []*HLL{
			newHLLFromHashValues(a, b),
			newHLLFromHashValues(a, b),
		}
		singletonSketches := []*HLL{
			newHLLFromHashValues(a, b),
			newHLLFromHashValues(a, b),
		}
		got := EstimateGlobalSingletonBySketches(ndvSketches, singletonSketches)
		require.Equal(t, uint64(0), got)
	})

	t.Run("NegativeContributionIsClamped", func(t *testing.T) {
		// Region 0 distinct = {a}, singletons = {a}.
		// Region 1 distinct = {a, b, e, f}, singletons = {e, f} (a, b repeat locally).
		// `a` appears in both regions' NDV, so it is not a global singleton; the only
		// global singletons are {e, f} → 2. Region 0's contribution must clamp to >= 0.
		ndvSketches := []*HLL{
			newHLLFromHashValues(a),
			newHLLFromHashValues(a, b, e, f),
		}
		singletonSketches := []*HLL{
			newHLLFromHashValues(a),
			newHLLFromHashValues(e, f),
		}
		got := EstimateGlobalSingletonBySketches(ndvSketches, singletonSketches)
		require.Equal(t, uint64(2), got)
	})

	t.Run("NilEntry", func(t *testing.T) {
		require.PanicsWithValue(t, "assert failed, ndvSketches must not contain nil entries", func() {
			EstimateGlobalSingletonBySketches(
				[]*HLL{nil, newHLLFromHashValues(c, d)},
				[]*HLL{newHLLFromHashValues(a, b), newHLLFromHashValues(c, d)},
			)
		})
		require.PanicsWithValue(t, "assert failed, singletonSketches must not contain nil entries", func() {
			EstimateGlobalSingletonBySketches(
				[]*HLL{newHLLFromHashValues(a, b), newHLLFromHashValues(c, d)},
				[]*HLL{nil, newHLLFromHashValues(c, d)},
			)
		})
	})

	t.Run("EmptyInput", func(t *testing.T) {
		require.PanicsWithValue(t, "assert failed, ndvSketches shouldn't be empty", func() {
			EstimateGlobalSingletonBySketches(nil, nil)
		})
	})

	t.Run("MismatchedLengths", func(t *testing.T) {
		ndvSketches := []*HLL{newHLLFromHashValues(a)}
		singletonSketches := []*HLL{newHLLFromHashValues(a), newHLLFromHashValues(b)}
		require.PanicsWithValue(t, "assert failed, ndvSketches and singletonSketches should have the same length", func() {
			EstimateGlobalSingletonBySketches(ndvSketches, singletonSketches)
		})
	})
}
