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
// For each region i, we ask: how many of region i's local singletons
// never appeared in any other region? Those are the values that are
// truly unique across the entire dataset, contributed by region i.
//
// We compute this by merging all *other* regions' NDV sketches (their full
// distinct-value sets), then checking how much region i's local singletons
// grow that union. The growth is approximately region i's singletons
// that no other region has seen.
//
// Summing these per-region contributions gives the global singleton estimate.
//
// To avoid rebuilding "all other regions" from scratch for every region
// (O(k^2) merges), we precompute suffix unions and keep a rolling prefix
// union, so each region's complement is a single prefix-suffix combine. This
// runs in O(k) merges using O(k) cached union sketches.
//
// Example with three regions:
//
//	Region 0 all distinct values: {a, b, c}    local singletons: {a, b, c}
//	Region 1 all distinct values: {b, c, d}    local singletons: {b, d}
//	Region 2 all distinct values: {c, e, f}    local singletons: {e, f}
//
// True global frequencies: a×1, b×2, c×3, d×1, e×1, f×1
// True singletons = 4  (the values {a, d, e, f} appear exactly once globally)
//
//	Region 0: others' NDV = {b,c,d,e,f} (size 5)
//	        + region 0 singletons {a,b,c} = {a,b,c,d,e,f} (size 6)
//	        contribution = 1  (only `a` is new)
//
//	Region 1: others' NDV = {a,b,c,e,f} (size 5)
//	        + region 1 singletons {b,d} = {a,b,c,d,e,f} (size 6)
//	        contribution = 1  (only `d` is new)
//
//	Region 2: others' NDV = {a,b,c,d} (size 4)
//	        + region 2 singletons {e,f} = {a,b,c,d,e,f} (size 6)
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

	// suffixNDV[i] = union of ndvSketches[i:] (suffixNDV[n] is empty). Precomputing
	// these makes each region's "union of all others" a prefix∪suffix combine, so the
	// pass is O(n) merges instead of O(n^2), at the cost of O(n) cached union sketches.
	n := len(ndvSketches)
	suffixNDV := make([]*FMSketch, n+1)
	for i := n - 1; i >= 1; i-- {
		suffixNDV[i] = mergeCopiedFMSketch(mergeCopiedFMSketch(nil, suffixNDV[i+1]), ndvSketches[i])
	}

	var globalSingleton int64
	// prefixNDV accumulates the union of ndvSketches[0:i] as i advances.
	var prefixNDV *FMSketch
	for i := range ndvSketches {
		// Union of every region except i = prefix(0:i) ∪ suffix(i+1:).
		other := mergeCopiedFMSketch(mergeCopiedFMSketch(nil, prefixNDV), suffixNDV[i+1])
		ndvOther := other.NDV()
		other = mergeCopiedFMSketch(other, singletonSketches[i])
		// FM sketch NDV estimates are not monotone under merge, so the union can come
		// out smaller than ndvOther; clamp the per-region contribution to >= 0.
		globalSingleton += max(0, other.NDV()-ndvOther)
		prefixNDV = mergeCopiedFMSketch(prefixNDV, ndvSketches[i])
		suffixNDV[i+1] = nil // consumed; release for GC
	}
	// SAFETY: Each per-region contribution is clamped to >= 0 before accumulation.
	intest.Assert(globalSingleton >= 0, "globalSingleton must be >= 0")
	return uint64(globalSingleton)
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
