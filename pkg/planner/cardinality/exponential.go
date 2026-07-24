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

// OrderingRatioForProbeLevel mutes the find-first-row optimism ratio (the session
// variable tidb_opt_ordering_index_selectivity_ratio) for a correlated scan that
// sits at the given probe level in a nested-loop hierarchy.
//
// The base ratio r expresses how much of the surplus scan range we assume to read
// before the first qualifying row is found (small r = optimistic, "found early").
// In a nest of correlated probes that optimism compounds multiplicatively across
// levels, so a constant r collapses to r^depth and becomes wildly over-optimistic
// the deeper we go. To counter that, the per-level effective ratio backs off toward
// 1 (full scan, no optimism) using the same exponential-backoff weighting that
// ApplyExponentialBackoff uses for stacked uncertain factors:
//
//	r_eff(level) = r ^ (1 / 2^(level-1))
//	level 1 -> r        (original optimism, single-table behavior preserved)
//	level 2 -> sqrt(r)
//	level 3 -> r^(1/4)
//	... -> 1 (full scan)
//
// level <= 1 returns r unchanged. r must be in (0, 1]; callers gate on r > 0.
func OrderingRatioForProbeLevel(r float64, level int) float64 {
	if level <= 1 || r <= 0 {
		return r
	}
	return math.Pow(r, 1/math.Exp2(float64(level-1)))
}
