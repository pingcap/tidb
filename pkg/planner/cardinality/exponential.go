// Copyright 2025 PingCAP, Inc.
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

package cardinality

import "math"

// MaxExponentialBackoffCols is the maximum number of columns to consider in exponential backoff.
// Beyond 4 columns, the exponential backoff weights (1/2^i) become small that additional
// columns have limited impact on the result.
const MaxExponentialBackoffCols = 4

// ApplyExponentialBackoff applies exponential backoff to pre-sorted values with bounds.
// Formula: val[0] * val[1]^(1/2) * val[2]^(1/4) * val[3]^(1/8) * ...
// Each column i gets weight 1/2^i, making later columns progressively less impactful.
// This function handles both selectivity and NDV estimation with appropriate bounds.
func ApplyExponentialBackoff(sortedValues []float64, lowerBound, upperBound float64) float64 {
	l := len(sortedValues)
	if l == 0 {
		return lowerBound
	}

	// For single value, just apply bounds
	if l == 1 {
		return math.Max(lowerBound, math.Min(sortedValues[0], upperBound))
	}

	// Apply exponential backoff formula: val[i]^(1/2^i)
	// Only consider up to MaxExponentialBackoffCols columns
	result := sortedValues[0]
	maxCols := min(MaxExponentialBackoffCols, l)

	for i := 1; i < maxCols; i++ {
		val := sortedValues[i]
		for range i {
			val = math.Sqrt(val)
		}
		result *= val
	}

	// Apply bounds
	return math.Max(lowerBound, math.Min(result, upperBound))
}
