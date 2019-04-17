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

func calculateEstimateNDV(dist []uint64, total uint64) (ndv uint64, ratio uint64, f1 uint64) {
	sampleSize := uint64(0)
	sampleNDV := uint64(len(dist))
	for _, v := range dist {
		if v == 1 {
			f1++
		}
		sampleSize += v
	}

	ratio = total / sampleSize
	if total < sampleSize {
		ratio = 1
	}

	if f1 == uint64(len(dist)) {
		// Assume this is a unique column
		ratio = 1
		ndv = total
	} else if f1 == 0 {
		// Assume data only consists of sampled data
		// Nothing to do, no change with ratio
		ndv = sampleNDV
	} else {
		// Charikar, Moses, et al. "Towards estimation error guarantees for distinct values."
		// Proceedings of the nineteenth ACM SIGMOD-SIGACT-SIGART symposium on Principles of database systems. ACM, 2000.
		// estimateNDV = sqrt(N/n) f_1 + sum_2..inf f_i
		// f_i = number of elements occurred i times in sample

		ff1 := float64(f1)
		n := float64(sampleSize)
		N := float64(total)
		d := float64(sampleNDV)

		ndv = uint64(math.Sqrt(N/n)*ff1 + d - ff1 + 0.5)

		if ndv < sampleNDV {
			ndv = sampleNDV
		}
		if ndv > total {
			ndv = total
		}
	}
	return ndv, ratio, f1
}
