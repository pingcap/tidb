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

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// testExponentialBackoffHelper tests exponential backoff with given values and bounds
func testExponentialBackoffHelper(t *testing.T, name string, values []float64, lowerBound, upperBound float64, expectedResult float64, tolerance float64) {
	t.Helper()
	result := ApplyExponentialBackoff(values, lowerBound, upperBound)
	require.InDelta(t, expectedResult, result, tolerance, "Test case: %s", name)
	require.GreaterOrEqual(t, result, lowerBound, "Result should respect lower bound for: %s", name)
	require.LessOrEqual(t, result, upperBound, "Result should respect upper bound for: %s", name)
}

func TestApplyExponentialBackoff(t *testing.T) {
	// Test NDV cases (values > 1)
	t.Run("NDV Cases", func(t *testing.T) {
		// Single value
		testExponentialBackoffHelper(t, "Single NDV", []float64{100}, 10, 10000, 100.0, 0.1)

		// Two values: 1000 * sqrt(500) ≈ 22360.68
		expected2 := 1000 * math.Sqrt(500)
		testExponentialBackoffHelper(t, "Two NDVs", []float64{1000, 500}, 100, 100000, expected2, 0.1)

		// Three values: 1000 * sqrt(500) * sqrt(sqrt(100)) ≈ 70710.68
		expected3 := 1000 * math.Sqrt(500) * math.Sqrt(math.Sqrt(100))
		testExponentialBackoffHelper(t, "Three NDVs", []float64{1000, 500, 100}, 100, 100000, expected3, 0.1)

		// Four values (max limit): 1000 * sqrt(500) * sqrt(sqrt(100)) * sqrt(sqrt(sqrt(10)))
		expected4 := 1000 * math.Sqrt(500) * math.Sqrt(math.Sqrt(100)) * math.Sqrt(math.Sqrt(math.Sqrt(10)))
		testExponentialBackoffHelper(t, "Four NDVs", []float64{1000, 500, 100, 10}, 10, 100000, expected4, 0.1)

		// Five values (should ignore 5th): same as four values
		testExponentialBackoffHelper(t, "Five NDVs (cap at 4)", []float64{1000, 500, 100, 10, 5}, 5, 100000, expected4, 0.1)
	})

	// Test selectivity cases (values < 1)
	t.Run("Selectivity Cases", func(t *testing.T) {
		// Single selectivity
		testExponentialBackoffHelper(t, "Single selectivity", []float64{0.1}, 0.001, 1.0, 0.1, 0.001)

		// Two selectivities: 0.01 * sqrt(0.02)
		expected2 := 0.01 * math.Sqrt(0.02)
		testExponentialBackoffHelper(t, "Two selectivities", []float64{0.01, 0.02}, 0.001, 1.0, expected2, 0.001)

		// Three selectivities: 0.01 * sqrt(0.02) * sqrt(sqrt(0.05))
		expected3 := 0.01 * math.Sqrt(0.02) * math.Sqrt(math.Sqrt(0.05))
		testExponentialBackoffHelper(t, "Three selectivities", []float64{0.01, 0.02, 0.05}, 0.001, 1.0, expected3, 0.001)

		// Four selectivities
		expected4 := 0.01 * math.Sqrt(0.02) * math.Sqrt(math.Sqrt(0.05)) * math.Sqrt(math.Sqrt(math.Sqrt(0.1)))
		testExponentialBackoffHelper(t, "Four selectivities", []float64{0.01, 0.02, 0.05, 0.1}, 0.001, 1.0, expected4, 0.001)
	})

	// Test bounds enforcement
	t.Run("Bounds Enforcement", func(t *testing.T) {
		// Result below lower bound should be clamped
		testExponentialBackoffHelper(t, "Below lower bound", []float64{0.001, 0.0005}, 0.01, 1.0, 0.01, 0.001)

		// Result above upper bound should be clamped
		testExponentialBackoffHelper(t, "Above upper bound", []float64{100, 50}, 1, 10, 10.0, 0.1)

		// Empty input should return lower bound
		testExponentialBackoffHelper(t, "Empty input", []float64{}, 5, 100, 5.0, 0.1)
	})
}
