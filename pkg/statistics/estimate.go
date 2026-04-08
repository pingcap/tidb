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
// grow that union. The growth is exactly the count of node i's singletons
// that no other node has seen.
//
// Summing these per-node contributions gives the global singleton estimate.
//
// The current implementation rebuilds "all except i" from scratch for each
// node, which is O(k²) in the number of nodes. A prefix-suffix approach
// could reduce this to O(k) time, but would require O(k) extra sketches
// (~160KB each), which risks significant memory pressure for tables with
// many nodes. We keep the O(k²) approach
// for its O(1) memory overhead.
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
		for i := range ndvSketches {
			if ndvSketches[i] == nil {
				return false
			}
			if singletonSketches[i] == nil {
				return false
			}
		}
		return true
	}, "ndvSketches and singletonSketches should never contains nil values")
	if len(ndvSketches) == 0 || len(ndvSketches) != len(singletonSketches) {
		return 0
	}

	var globalSingleton int64
	for i := range ndvSketches {
		// Merge NDV sketches from all nodes except node i.
		var other *FMSketch
		for j, ns := range ndvSketches {
			if j == i || ns == nil {
				continue
			}

			if other == nil {
				other = ns.Copy()
				continue
			}
			other.MergeFMSketch(ns)
		}

		ndvOther := int64(0)
		if other != nil {
			ndvOther = other.NDV()
		}

		// Merge the other-nodes sketch with node i's singleton sketch.
		// ndvUnion - ndvOther gives the count of node i's singletons
		// that don't appear in any other node (i.e. globally unique values).
		// We already captured ndvOther, so we can safely reuse other here.
		if other != nil {
			if singletonSketches[i] != nil {
				other.MergeFMSketch(singletonSketches[i])
			}
		} else if singletonSketches[i] != nil {
			other = singletonSketches[i].Copy()
		}

		ndvUnion := int64(0)
		if other != nil {
			ndvUnion = other.NDV()
		}
		// FM sketch NDV estimates are not monotone under merge, so the estimated
		// union can be smaller than ndvOther. Clamp the per-node contribution to 0.
		// In practice, this appears to be fairly rare.
		delta := max(0, ndvUnion-ndvOther)
		globalSingleton += delta
	}
	intest.Assert(globalSingleton >= 0, "globalSingleton must be positive")
	return uint64(globalSingleton)
}
