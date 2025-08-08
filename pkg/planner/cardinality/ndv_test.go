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

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/stretchr/testify/require"
)

func TestEstimateColsNDVWithExponentialBackoff(t *testing.T) {
	// Create test schema with columns a, b, c
	schema := expression.NewSchema()
	colA := &expression.Column{UniqueID: 1}
	colB := &expression.Column{UniqueID: 2}
	colC := &expression.Column{UniqueID: 3}
	schema.Append(colA, colB, colC)

	// Create stats info with GroupNDVs and individual column NDVs
	statsInfo := &property.StatsInfo{
		RowCount: 100000,
		ColNDVs: map[int64]float64{
			1: 1000, // NDV(a) = 1000
			2: 500,  // NDV(b) = 500
			3: 10,   // NDV(c) = 10
		},
		GroupNDVs: []property.GroupNDV{
			{
				Cols: []int64{1, 2, 3}, // Index on (a,b,c)
				NDV:  5000,             // NDV(a,b,c) = 5000
			},
		},
	}

	// Test 1: Individual columns should return their own NDV
	ndv, matchedLen := EstimateColsNDVWithMatchedLen([]*expression.Column{colA}, schema, statsInfo)
	require.Equal(t, 1000.0, ndv)
	require.Equal(t, 1, matchedLen)

	ndv, matchedLen = EstimateColsNDVWithMatchedLen([]*expression.Column{colB}, schema, statsInfo)
	require.Equal(t, 500.0, ndv)
	require.Equal(t, 1, matchedLen)

	ndv, matchedLen = EstimateColsNDVWithMatchedLen([]*expression.Column{colC}, schema, statsInfo)
	require.Equal(t, 10.0, ndv)
	require.Equal(t, 1, matchedLen)

	// Test 2: Exact GroupNDV match should return exact NDV
	targetCols := []*expression.Column{colA, colB, colC}
	ndv, matchedLen = EstimateColsNDVWithMatchedLen(targetCols, schema, statsInfo)
	require.Equal(t, 5000.0, ndv)
	require.Equal(t, 3, matchedLen)

	// Test 3: Two-column combinations should use exponential backoff
	targetCols = []*expression.Column{colA, colB}
	ndv, matchedLen = EstimateColsNDVWithMatchedLen(targetCols, schema, statsInfo)
	expectedAB := 1000 * math.Sqrt(500)
	require.InDelta(t, expectedAB, ndv, 0.1)
	require.Equal(t, 2, matchedLen)

	targetCols = []*expression.Column{colA, colC}
	ndv, matchedLen = EstimateColsNDVWithMatchedLen(targetCols, schema, statsInfo)
	expectedAC := 1000 * math.Sqrt(10)
	require.InDelta(t, expectedAC, ndv, 0.1)
	require.Equal(t, 2, matchedLen)

	targetCols = []*expression.Column{colB, colC}
	ndv, matchedLen = EstimateColsNDVWithMatchedLen(targetCols, schema, statsInfo)
	expectedBC := 500 * math.Sqrt(10)
	require.InDelta(t, expectedBC, ndv, 0.1)
	require.Equal(t, 2, matchedLen)

	// Test 4: Without GroupNDVs
	statsInfoNoGroup := &property.StatsInfo{
		RowCount: 100000,
		ColNDVs: map[int64]float64{
			1: 1000,
			2: 500,
			3: 10,
		},
		GroupNDVs: []property.GroupNDV{},
	}

	// Test different 2-column combinations without GroupNDVs
	targetCols = []*expression.Column{colA, colB}
	ndv, matchedLen = EstimateColsNDVWithMatchedLen(targetCols, schema, statsInfoNoGroup)
	expectedABNoGroup := 1000 * math.Sqrt(500) // Same as with GroupNDVs since no exact match
	require.InDelta(t, expectedABNoGroup, ndv, 0.1)
	require.Equal(t, 2, matchedLen)

	targetCols = []*expression.Column{colA, colC}
	ndv, matchedLen = EstimateColsNDVWithMatchedLen(targetCols, schema, statsInfoNoGroup)
	expectedACNoGroup := 1000 * math.Sqrt(10)
	require.InDelta(t, expectedACNoGroup, ndv, 0.1)
	require.Equal(t, 2, matchedLen)

	// Test 3-column combination without GroupNDVs
	targetCols = []*expression.Column{colA, colB, colC}
	ndv, matchedLen = EstimateColsNDVWithMatchedLen(targetCols, schema, statsInfoNoGroup)
	// NDVs sorted descending: [1000, 500, 10]
	// Expected: 1000 * sqrt(500) * sqrt(sqrt(10)) ≈ 39783.22
	expectedABCNoGroup := 1000 * math.Sqrt(500) * math.Sqrt(math.Sqrt(10))
	require.InDelta(t, expectedABCNoGroup, ndv, 0.1)
	require.Equal(t, 3, matchedLen)
}
