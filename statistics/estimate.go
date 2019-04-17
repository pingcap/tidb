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
// See the License for the specific language governing permissions and
// limitations under the License.

package statistics

import "math"

// calculateEstimateNDV calculates the estimate ndv of a sampled data from a multisize with size total.
// count[i] stores the count of the i-th element.
// onlyOnceItems is the number of elements that occurred only once.
func calculateEstimateNDV(count []uint64, total uint64) (ndv uint64, ratio uint64, onlyOnceItems uint64) {
	sampleSize := uint64(0)
	sampleNDV := uint64(len(count))
	for _, v := range count {
		if v == 1 {
			onlyOnceItems++
		}
		sampleSize += v
	}

	ratio = total / sampleSize
	if total < sampleSize {
		ratio = 1
	}

	if onlyOnceItems == uint64(len(count)) {
		// Assume this is a unique column
		ratio = 1
		ndv = total
	} else if onlyOnceItems == 0 {
		// Assume data only consists of sampled data
		// Nothing to do, no change with ratio
		ndv = sampleNDV
	} else {
		// Charikar, Moses, et al. "Towards estimation error guarantees for distinct values."
		// Proceedings of the nineteenth ACM SIGMOD-SIGACT-SIGART symposium on Principles of database systems. ACM, 2000.
		// estimateNDV = sqrt(N/n) f_1 + sum_2..inf f_i
		// f_i = number of elements occurred i times in sample

		f1 := float64(onlyOnceItems)
		n := float64(sampleSize)
		N := float64(total)
		d := float64(sampleNDV)

		ndv = uint64(math.Sqrt(N/n)*f1 + d - f1 + 0.5)

		if ndv < sampleNDV {
			ndv = sampleNDV
		}
		if ndv > total {
			ndv = total
		}
	}
	return ndv, ratio, onlyOnceItems
}
