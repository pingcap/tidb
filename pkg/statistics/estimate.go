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
)

// calculateEstimateNDV calculates the estimate ndv of a sampled data from a multisize with size total.
func calculateEstimateNDV(h *topNHelper, rowCount uint64) (ndv uint64, scaleRatio uint64) {
	sampleSize, sampleNDV, onlyOnceItems := h.sampleSize, uint64(len(h.sorted)), h.onlyOnceItems
	scaleRatio = rowCount / sampleSize

	if onlyOnceItems == sampleSize {
		// Assume this is a unique column, so do not scale up the count of elements
		return rowCount, 1
	} else if onlyOnceItems == 0 {
		// Assume data only consists of sampled data
		// Nothing to do, no change with scale ratio
		return sampleNDV, scaleRatio
	}
	// Charikar, Moses, et al. "Towards estimation error guarantees for distinct values."
	// Proceedings of the nineteenth ACM SIGMOD-SIGACT-SIGART symposium on Principles of database systems. ACM, 2000.
	// This is GEE in that paper.
	// estimateNDV = sqrt(rowCountN/n) f_1 + sum_2..inf f_i
	// f_i = number of elements occurred i times in sample

	f1 := float64(onlyOnceItems)
	n := float64(sampleSize)
	rowCountN := float64(rowCount)
	d := float64(sampleNDV)

	ndv = uint64(math.Sqrt(rowCountN/n)*f1 + d - f1 + 0.5)
	ndv = max(ndv, sampleNDV)
	ndv = min(ndv, rowCount)
	return ndv, scaleRatio
}

// EstimateNDVByChao3 estimates NDV using the Chao3 estimator (Eq. 3) from:
// "Sampling-based Estimation of the Number of Distinct Values in Distributed Environment" (arXiv:2206.05476).
//
// D_hat = d + 1/2 * f1^2 / (d - f1)
// d: sample NDV; f1: number of values that appear once in the sample.
func EstimateNDVByChao3(sampleNDV, onlyOnceItems, sampleSize, rowCount uint64) uint64 {
	if sampleSize == 0 || sampleNDV == 0 {
		return 0
	}
	if rowCount != 0 && rowCount < sampleNDV {
		rowCount = sampleNDV
	}
	if onlyOnceItems == sampleSize {
		if rowCount > 0 {
			return rowCount
		}
		return sampleNDV
	}
	if onlyOnceItems == 0 {
		if rowCount > 0 {
			return min(sampleNDV, rowCount)
		}
		return sampleNDV
	}
	denom := float64(sampleNDV - onlyOnceItems)
	if denom <= 0 {
		if rowCount > 0 {
			return min(sampleNDV, rowCount)
		}
		return sampleNDV
	}
	est := float64(sampleNDV) + 0.5*float64(onlyOnceItems*onlyOnceItems)/denom
	ndv := uint64(est + 0.5)
	ndv = max(ndv, sampleNDV)
	if rowCount > 0 {
		ndv = min(ndv, rowCount)
	}
	return ndv
}

// EstimateNDVByGEE estimates NDV using the GEE estimator from:
// "Towards estimation error guarantees for distinct values." (Charikar et al., 2000).
//
// D_hat = sqrt(N/n) * f1 + d - f1
// d: sample NDV; f1: number of values that appear once in the sample.
// n: sample size; N: row count.
func EstimateNDVByGEE(sampleNDV, onlyOnceItems, sampleSize, rowCount uint64) uint64 {
	if sampleSize == 0 || sampleNDV == 0 {
		return 0
	}
	if rowCount != 0 && rowCount < sampleNDV {
		rowCount = sampleNDV
	}
	if onlyOnceItems == sampleSize {
		if rowCount > 0 {
			return rowCount
		}
		return sampleNDV
	}
	if onlyOnceItems == 0 {
		if rowCount > 0 {
			return min(sampleNDV, rowCount)
		}
		return sampleNDV
	}
	f1 := float64(onlyOnceItems)
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

// EstimateF1BySketches estimates f1 (the number of values that appear once in the sample)
// using NDVSketch and F1Sketch from multiple nodes. It follows Eq. 7-9 and Algorithm 2 in:
// "Sampling-based Estimation of the Number of Distinct Values in Distributed Environment"
// (arXiv:2206.05476).
//
// For each node i, it estimates |U_-i ∪ F1_i| - |U_-i|, where U_-i is the union of NDV sketches
// from all nodes except i. The sum of these differences is the estimated f1.
func EstimateF1BySketches(ndvSketches, f1Sketches []*FMSketch) uint64 {
	if len(ndvSketches) == 0 || len(ndvSketches) != len(f1Sketches) {
		return 0
	}
	var f1 int64
	for i := range ndvSketches {
		var other *FMSketch
		for j, sk := range ndvSketches {
			if j == i || sk == nil {
				continue
			}
			if other == nil {
				other = sk.Copy()
				continue
			}
			other.MergeFMSketch(sk)
		}
		ndvOther := uint64(0)
		if other != nil {
			ndvOther = uint64(other.NDV())
		}
		var union *FMSketch
		if other != nil {
			union = other.Copy()
			if f1Sketches[i] != nil {
				union.MergeFMSketch(f1Sketches[i])
			}
		} else if f1Sketches[i] != nil {
			union = f1Sketches[i].Copy()
		}
		ndvUnion := uint64(0)
		if union != nil {
			ndvUnion = uint64(union.NDV())
		}
		f1 += int64(ndvUnion) - int64(ndvOther)
		if other != nil {
			other.DestroyAndPutToPool()
		}
		if union != nil && union != other {
			union.DestroyAndPutToPool()
		}
	}
	if f1 < 0 {
		return 0
	}
	return uint64(f1)
}
