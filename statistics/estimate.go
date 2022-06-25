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

	"github.com/pingcap/tidb/util/mathutil"
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
	// estimateNDV = sqrt(N/n) f_1 + sum_2..inf f_i
	// f_i = number of elements occurred i times in sample

	f1 := float64(onlyOnceItems)
	n := float64(sampleSize)
	N := float64(rowCount)
	d := float64(sampleNDV)

	ndv = uint64(math.Sqrt(N/n)*f1 + d - f1 + 0.5)
	ndv = mathutil.Max(ndv, sampleNDV)
	ndv = mathutil.Min(ndv, rowCount)
	return ndv, scaleRatio
}
