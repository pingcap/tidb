// Copyright 2019 PingCAP, Inc.
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
	"math"

	"github.com/pingcap/tidb/pkg/util/intest"
)

// calculateEstimateNDV calculates the estimate ndv of a sampled data from a multisize with size total.
func calculateEstimateNDV(h *topNHelper, rowCount uint64) (ndv uint64, scaleRatio uint64) {
	sampleSize, sampleNDV, singletonItems := h.sampleSize, uint64(len(h.sorted)), h.singletonItems
	scaleRatio = rowCount / sampleSize

	if singletonItems == sampleSize {
		// Assume this is a unique column, so do not scale up the count of elements
		return rowCount, 1
	} else if singletonItems == 0 {
		// Assume data only consists of sampled data
		// Nothing to do, no change with scale ratio
		return sampleNDV, scaleRatio
	}
	ndv = EstimateNDVByGEE(sampleNDV, singletonItems, sampleSize, rowCount)
	return ndv, scaleRatio
}

// EstimateNDVByGEE estimates NDV using the GEE estimator from:
// "Towards estimation error guarantees for distinct values." (Charikar et al., 2000).
//
// D_hat = sqrt(N/n) * f1 + d - f1
// d: sample NDV; f1: number of singleton values in the sample.
// n: sample size; N: row count.
func EstimateNDVByGEE(sampleNDV, singletonItems, sampleSize, rowCount uint64) uint64 {
	intest.Assert(sampleSize > 0, "sampleSize should be greater than 0")
	intest.Assert(sampleNDV > 0, "sampleNDV should be greater than 0")
	// Defensive code, in case of wrong input, return 0 to avoid overestimation.
	if sampleSize == 0 || sampleNDV == 0 {
		return 0
	}
	intest.Assert(rowCount >= sampleNDV, "rowCount should be greater than or equal to sampleNDV")

	f1 := float64(singletonItems)
	n := float64(sampleSize)
	rowCountN := float64(rowCount)
	d := float64(sampleNDV)

	est := d + (math.Sqrt(rowCountN/n)-1.0)*f1
	ndv := uint64(est + 0.5)
	ndv = max(ndv, sampleNDV)
	if rowCount > 0 {
		ndv = min(ndv, rowCount)
	}
	return ndv
}

// EstimateGlobalSingletonBySketches estimates the global singleton count using NDV and singleton sketches.
// For each node i, we ask: how many of node i's local singletons
// never appeared in any other node? Those are the values that are
// truly unique across the entire dataset, contributed by node i.
//
// We compute this by merging all *other* nodes' NDV sketches (their full
// distinct-value sets), then checking how much node i's local singletons
// grow that union. The growth is approximately node i's singleton's
// FMSketch that no other node has seen.
//
// Summing these per-node contributions gives the global singleton estimate.
//
// The implementation splits the nodes into two halves, precomputes one NDV
// union per half, and then rebuilds only the suffix within each half while
// keeping a rolling in-half prefix. That keeps the O(k²) time complexity
// but cuts repeated merge work to roughly one quarter of the naive
// rebuild-from-scratch loop while preserving O(1) extra sketches. A full
// prefix-suffix cache could reduce the runtime to O(k), but it would require
// O(k) extra sketches (~80KB each), which risks significant memory pressure
// for tables with many nodes.
//
// Example with three nodes:
//
//	Node 0 all distinct values: {a, b, c}    local singletons: {a, b, c}
//	Node 1 all distinct values: {b, c, d}    local singletons: {b, d}
//	Node 2 all distinct values: {c, e, f}    local singletons: {e, f}
//
// True global frequencies: a×1, b×2, c×3, d×1, e×1, f×1
// True singletons = 4  (the values {a, d, e, f} appear exactly once globally)
//
//	Node 0: others' NDV = {b,c,d,e,f} (size 5)
//	        + node 0 singletons {a,b,c} = {a,b,c,d,e,f} (size 6)
//	        contribution = 1  (only `a` is new)
//
//	Node 1: others' NDV = {a,b,c,e,f} (size 5)
//	        + node 1 singletons {b,d} = {a,b,c,d,e,f} (size 6)
//	        contribution = 1  (only `d` is new)
//
//	Node 2: others' NDV = {a,b,c,d} (size 4)
//	        + node 2 singletons {e,f} = {a,b,c,d,e,f} (size 6)
//	        contribution = 2  (`e` and `f` are new)
//
// Estimated singletons = 1 + 1 + 2 = 4
func EstimateGlobalSingletonBySketches(ndvSketches, singletonSketches []*FMSketch) uint64 {
	// Defensive checks.
	intest.Assert(len(ndvSketches) > 0, "ndvSketches shouldn't be empty")
	intest.Assert(len(ndvSketches) == len(singletonSketches), "ndvSketches and singletonSketches should have the same length")
	intest.AssertFunc(func() bool {
		for _, ndvSketch := range ndvSketches {
			if ndvSketch == nil {
				return false
			}
		}
		return true
	}, "ndvSketches must not contain nil entries")
	intest.AssertFunc(func() bool {
		for _, singletonSketch := range singletonSketches {
			if singletonSketch == nil {
				return false
			}
		}
		return true
	}, "singletonSketches must not contain nil entries")
	if len(ndvSketches) == 0 || len(ndvSketches) != len(singletonSketches) {
		return 0
	}

	mid := len(ndvSketches) - len(ndvSketches)/2
	var leftHalfNDV *FMSketch
	for _, sketch := range ndvSketches[:mid] {
		leftHalfNDV = mergeCopiedFMSketch(leftHalfNDV, sketch)
	}
	var rightHalfNDV *FMSketch
	for _, sketch := range ndvSketches[mid:] {
		rightHalfNDV = mergeCopiedFMSketch(rightHalfNDV, sketch)
	}

	// NOTE: For each node, we still merge every other node's NDV sketch.
	globalSingleton := estimateGlobalSingletonInRange(ndvSketches[:mid], singletonSketches[:mid], rightHalfNDV)
	globalSingleton += estimateGlobalSingletonInRange(ndvSketches[mid:], singletonSketches[mid:], leftHalfNDV)
	// SAFETY: Each per-node contribution is clamped to >= 0 before accumulation.
	intest.Assert(globalSingleton >= 0, "globalSingleton must be positive")
	return uint64(globalSingleton)
}

func estimateGlobalSingletonInRange(ndvSketches, singletonSketches []*FMSketch, outOfRangeNDVSketch *FMSketch) int64 {
	var globalSingleton int64
	// prefixNDVSketch accumulates ndvSketches[0..i-1] as i advances, so
	// each iteration only rebuilds the suffix (ndvSketches[i+1..]) from
	// scratch instead of the full "all-except-i" set.
	var prefixNDVSketch *FMSketch
	for i := range ndvSketches {
		other := mergeCopiedFMSketch(nil, prefixNDVSketch)
		for _, sketch := range ndvSketches[i+1:] {
			other = mergeCopiedFMSketch(other, sketch)
		}
		other = mergeCopiedFMSketch(other, outOfRangeNDVSketch)

		// NDV of the union of all other nodes before merging this node's singletons.
		ndvOther := other.NDV()
		other = mergeCopiedFMSketch(other, singletonSketches[i])

		// NDV of the union after merging this node's singleton sketch.
		ndvUnion := other.NDV()
		// FM sketch NDV estimates are not monotone under merge, so the estimated
		// union can be smaller than ndvOther. Clamp the per-node contribution to 0.
		// In practice, this appears to be fairly rare.
		globalSingleton += max(0, ndvUnion-ndvOther)
		prefixNDVSketch = mergeCopiedFMSketch(prefixNDVSketch, ndvSketches[i])
	}
	return globalSingleton
}

func mergeCopiedFMSketch(dst, src *FMSketch) *FMSketch {
	if src == nil {
		return dst
	}
	if dst == nil {
		return src.Copy()
	}
	dst.MergeFMSketch(src)
	return dst
}
