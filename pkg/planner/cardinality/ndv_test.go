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
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/stretchr/testify/require"
)

// mockSessionVars provides a simple mock for testing
type mockSessionVars struct {
	riskGroupNDVSkewRatio float64
}

func (m *mockSessionVars) GetSessionVars() *variable.SessionVars {
	return &variable.SessionVars{
		RiskGroupNDVSkewRatio: m.riskGroupNDVSkewRatio,
	}
}

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

	// Test 1: Individual columns should return their own NDV (context doesn't matter for single columns)
	ndv, matchedLen := EstimateColsNDVWithMatchedLen(nil, []*expression.Column{colA}, schema, statsInfo)
	require.Equal(t, 1000.0, ndv)
	require.Equal(t, 1, matchedLen)

	ndv, matchedLen = EstimateColsNDVWithMatchedLen(nil, []*expression.Column{colB}, schema, statsInfo)
	require.Equal(t, 500.0, ndv)
	require.Equal(t, 1, matchedLen)

	ndv, matchedLen = EstimateColsNDVWithMatchedLen(nil, []*expression.Column{colC}, schema, statsInfo)
	require.Equal(t, 10.0, ndv)
	require.Equal(t, 1, matchedLen)

	// Test 2: Exact GroupNDV match should return exact NDV (context doesn't matter for exact matches)
	targetCols := []*expression.Column{colA, colB, colC}
	ndv, matchedLen = EstimateColsNDVWithMatchedLen(nil, targetCols, schema, statsInfo)
	require.Equal(t, 5000.0, ndv)
	require.Equal(t, 3, matchedLen)

	// Test 3: Two-column combinations with system variable disabled (default)
	targetCols = []*expression.Column{colA, colB}

	// Test with variable disabled (skewRatio = 0) - should use conservative estimate
	mockCtxDisabled := &mockSessionVars{riskGroupNDVSkewRatio: 0.0}
	ndvDisabled, matchedLen := EstimateColsNDVWithMatchedLen(mockCtxDisabled, targetCols, schema, statsInfo)
	expectedConservative := 1000.0 // max(1000, 500) - conservative approach
	require.InDelta(t, expectedConservative, ndvDisabled, 0.1)
	require.Equal(t, 1, matchedLen)

	// Test with variable enabled (skewRatio = 1.0) - should use exponential backoff
	mockCtxEnabled := &mockSessionVars{riskGroupNDVSkewRatio: 1.0}
	ndvEnabled, matchedLen := EstimateColsNDVWithMatchedLen(mockCtxEnabled, targetCols, schema, statsInfo)
	expectedExponential := 1000 * math.Sqrt(500) // ~22360.7 - exponential backoff
	require.InDelta(t, expectedExponential, ndvEnabled, 0.1)
	require.Equal(t, 1, matchedLen)

	// Verify they produce different results
	require.NotEqual(t, ndvDisabled, ndvEnabled)
	require.Greater(t, ndvEnabled, ndvDisabled) // Exponential should be higher

	// Test with variable partially enabled (skewRatio = 0.5) - should blend
	mockCtxBlended := &mockSessionVars{riskGroupNDVSkewRatio: 0.5}
	ndvBlended, _ := EstimateColsNDVWithMatchedLen(mockCtxBlended, targetCols, schema, statsInfo)
	expectedBlended := expectedConservative + (expectedExponential-expectedConservative)*0.5
	require.InDelta(t, expectedBlended, ndvBlended, 0.1)
	require.Greater(t, ndvBlended, ndvDisabled)
	require.Less(t, ndvBlended, ndvEnabled)

	// Test additional column combinations with exponential backoff enabled
	targetCols = []*expression.Column{colA, colC}
	ndv, matchedLen = EstimateColsNDVWithMatchedLen(mockCtxEnabled, targetCols, schema, statsInfo)
	expectedAC := 1000 * math.Sqrt(10)
	require.InDelta(t, expectedAC, ndv, 0.1)
	require.Equal(t, 1, matchedLen)

	targetCols = []*expression.Column{colB, colC}
	ndv, matchedLen = EstimateColsNDVWithMatchedLen(mockCtxEnabled, targetCols, schema, statsInfo)
	expectedBC := 500 * math.Sqrt(10)
	require.InDelta(t, expectedBC, ndv, 0.1)
	require.Equal(t, 1, matchedLen)

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

	// Test different 2-column combinations without GroupNDVs (with exponential backoff enabled)
	targetCols = []*expression.Column{colA, colB}
	ndv, matchedLen = EstimateColsNDVWithMatchedLen(mockCtxEnabled, targetCols, schema, statsInfoNoGroup)
	expectedABNoGroup := 1000 * math.Sqrt(500) // Same as with GroupNDVs since no exact match
	require.InDelta(t, expectedABNoGroup, ndv, 0.1)
	require.Equal(t, 1, matchedLen)

	targetCols = []*expression.Column{colA, colC}
	ndv, matchedLen = EstimateColsNDVWithMatchedLen(mockCtxEnabled, targetCols, schema, statsInfoNoGroup)
	expectedACNoGroup := 1000 * math.Sqrt(10)
	require.InDelta(t, expectedACNoGroup, ndv, 0.1)
	require.Equal(t, 1, matchedLen)

	// Test 3-column combination without GroupNDVs
	targetCols = []*expression.Column{colA, colB, colC}
	ndv, matchedLen = EstimateColsNDVWithMatchedLen(mockCtxEnabled, targetCols, schema, statsInfoNoGroup)
	// NDVs sorted descending: [1000, 500, 10]
	expectedABCNoGroup := 1000 * math.Sqrt(500) * math.Sqrt(math.Sqrt(10))
	require.InDelta(t, expectedABCNoGroup, ndv, 0.1)
	require.Equal(t, 1, matchedLen)
}
