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
