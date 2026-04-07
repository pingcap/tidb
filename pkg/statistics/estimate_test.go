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

// newFMSketchFromHashValues builds an FM sketch by directly inserting hash values.
// With a large maxSize and few values, the sketch gives exact NDV (mask stays 0).
func newFMSketchFromHashValues(vals ...uint64) *FMSketch {
	s := NewFMSketch(1000)
	for _, v := range vals {
		s.insertHashValue(v)
	}
	return s
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
		// Region 0: all distinct = {a, b, c}, local singletons = {a, b, c}
		// Region 1: all distinct = {b, c, d}, local singletons = {b, d}
		// Region 2: all distinct = {c, e, f}, local singletons = {e, f}
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

	t.Run("SingleRegion", func(t *testing.T) {
		// With one region, all local singletons are global singletons.
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
		// Regions have disjoint values. All local singletons are global singletons.
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
		// Every local singleton also appears in another region's NDV.
		// Region 0: all = {a, b}, singletons = {a, b}
		// Region 1: all = {a, b}, singletons = {a, b}
		// No value is unique to a single region → 0 global singletons.
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

	t.Run("NilSingletonSketches", func(t *testing.T) {
		// A region with nil singleton sketch contributes 0.
		ndvSketches := []*FMSketch{
			newFMSketchFromHashValues(a, b),
			newFMSketchFromHashValues(c, d),
		}
		singletonSketches := []*FMSketch{
			nil,
			newFMSketchFromHashValues(c, d),
		}
		// Region 0 contributes 0 (nil singleton).
		// Region 1: others' NDV = {a, b}, union with {c, d} = {a, b, c, d} → contribution = 2.
		got := EstimateGlobalSingletonBySketches(ndvSketches, singletonSketches)
		require.Equal(t, uint64(2), got)
	})

	t.Run("NilNDVSketches", func(t *testing.T) {
		// A region with nil NDV sketch is skipped when building "others".
		ndvSketches := []*FMSketch{
			nil,
			newFMSketchFromHashValues(c, d),
		}
		singletonSketches := []*FMSketch{
			newFMSketchFromHashValues(a, b),
			newFMSketchFromHashValues(c, d),
		}
		// Region 0: others' NDV = {c, d}, union with {a, b} = {a, b, c, d} → contribution = 2.
		// Region 1: others' NDV = {} (nil), union with {c, d} = {c, d} → contribution = 2.
		got := EstimateGlobalSingletonBySketches(ndvSketches, singletonSketches)
		require.Equal(t, uint64(4), got)
	})

	t.Run("AllNilSketches", func(t *testing.T) {
		ndvSketches := []*FMSketch{nil, nil}
		singletonSketches := []*FMSketch{nil, nil}
		got := EstimateGlobalSingletonBySketches(ndvSketches, singletonSketches)
		require.Equal(t, uint64(0), got)
	})

	t.Run("EmptyInput", func(t *testing.T) {
		got := EstimateGlobalSingletonBySketches(nil, nil)
		require.Equal(t, uint64(0), got)
	})

	t.Run("MismatchedLengths", func(t *testing.T) {
		ndvSketches := []*FMSketch{newFMSketchFromHashValues(a)}
		singletonSketches := []*FMSketch{newFMSketchFromHashValues(a), newFMSketchFromHashValues(b)}
		got := EstimateGlobalSingletonBySketches(ndvSketches, singletonSketches)
		require.Equal(t, uint64(0), got)
	})
}
