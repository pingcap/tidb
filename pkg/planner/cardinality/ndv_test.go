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

package cardinality_test

import (
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestScaleNDV(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set @@tidb_opt_scale_ndv_skew_ratio = 0`)
	type TestCase struct {
		OriginalNDV  float64
		OriginalRows float64
		SelectedRows float64
		NewNDV       float64
	}
	cases := []TestCase{
		{0, 0, 0, 0},
		{10, 0, 100, 0},
		{10, 100, 100, 10},
		{10, 100, 1, 1},
		{10, 100, 2, 1.83},
		{10, 100, 10, 6.51},
		{10, 100, 50, 9.99},
		{10, 100, 80, 10.00},
		{10, 100, 90, 10.00},
	}
	for _, tc := range cases {
		newNDV := cardinality.ScaleNDV(tk.Session().GetSessionVars(), tc.OriginalNDV, tc.OriginalRows, tc.SelectedRows)
		require.Equal(t, fmt.Sprintf("%.2f", tc.NewNDV), fmt.Sprintf("%.2f", newNDV), tc)
	}
}

func TestOptScaleNDVSkewRatioSetVar(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, key(a), key(b));`)
	vals := make([]string, 0, 100)
	for i := 0; i < 100; i++ {
		vals = append(vals, fmt.Sprintf("(%d, %d)", i%20, i))
	}
	tk.MustExec(`insert into t values ` + strings.Join(vals, ","))
	tk.MustExec("analyze table t")
	tk.MustExec(`set @@tidb_stats_load_sync_wait=100`)

	aggEstRows := tk.MustQuery(`explain select /*+ set_var(tidb_opt_scale_ndv_skew_ratio=0) */ distinct(a) from t where b<50`).Rows()[0][1].(string)
	require.Equal(t, aggEstRows, "19.44")
	aggEstRows = tk.MustQuery(`explain select /*+ set_var(tidb_opt_scale_ndv_skew_ratio="0.5") */ distinct(a) from t where b<50`).Rows()[0][1].(string)
	require.Equal(t, aggEstRows, "14.82") // less than the prior one
	aggEstRows = tk.MustQuery(`explain select /*+ set_var(tidb_opt_scale_ndv_skew_ratio=1) */ distinct(a) from t where b<50`).Rows()[0][1].(string)
	require.Equal(t, aggEstRows, "10.20") // less than the prior one
}

func TestIssue54812(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set @@tidb_opt_scale_ndv_skew_ratio = 0`)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`create table t (a int, b int, key(a), key(b));`)
	vals := make([]string, 0, 100)
	for i := 0; i < 100; i++ {
		vals = append(vals, fmt.Sprintf("(%d, 1)", i))
	}
	tk.MustExec(`insert into t values ` + strings.Join(vals, ","))
	for i := 0; i < 10; i++ {
		tk.MustExec(`insert into t values ` + strings.Repeat("(100, 2), ", 99) + "(100, 2)")
	}
	tk.MustExec("analyze table t")
	tk.MustExec(`set @@tidb_stats_load_sync_wait=100`)
	tk.MustQuery(`explain format='brief' select distinct(a) from t where b=1`).Check(testkit.Rows(
		`HashAgg 65.23 root  group by:test.t.a, funcs:firstrow(test.t.a)->test.t.a`,
		`└─TableReader 65.23 root  data:HashAgg`,
		`  └─HashAgg 65.23 cop[tikv]  group by:test.t.a, `,
		`    └─Selection 100.00 cop[tikv]  eq(test.t.b, 1)`,
		`      └─TableFullScan 1100.00 cop[tikv] table:t keep order:false`))
	//aggEstRows := tk.MustQuery(`explain select distinct(a) from t where b=1`).Rows()[0][1].(string)
	//require.Equal(t, "65.23", aggEstRows)
}

// createMockPlanContext creates a mock plan context with specified skew ratio
func createMockPlanContext(riskGroupNDVSkewRatio float64) *mock.Context {
	ctx := mock.NewContext()
	ctx.GetSessionVars().RiskGroupNDVSkewRatio = riskGroupNDVSkewRatio
	return ctx
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
	ndv, matchedLen := cardinality.EstimateColsNDVWithMatchedLen(nil, []*expression.Column{colA}, schema, statsInfo)
	require.Equal(t, 1000.0, ndv)
	require.Equal(t, 1, matchedLen)

	ndv, matchedLen = cardinality.EstimateColsNDVWithMatchedLen(nil, []*expression.Column{colB}, schema, statsInfo)
	require.Equal(t, 500.0, ndv)
	require.Equal(t, 1, matchedLen)

	ndv, matchedLen = cardinality.EstimateColsNDVWithMatchedLen(nil, []*expression.Column{colC}, schema, statsInfo)
	require.Equal(t, 10.0, ndv)
	require.Equal(t, 1, matchedLen)

	// Test 2: Exact GroupNDV match should return exact NDV (context doesn't matter for exact matches)
	targetCols := []*expression.Column{colA, colB, colC}
	ndv, matchedLen = cardinality.EstimateColsNDVWithMatchedLen(nil, targetCols, schema, statsInfo)
	require.Equal(t, 5000.0, ndv)
	require.Equal(t, 3, matchedLen)

	// Test 3: Two-column combinations with system variable disabled (default)
	targetCols = []*expression.Column{colA, colB}

	// Test with variable disabled (skewRatio = 0) - should use conservative estimate
	mockCtxDisabled := createMockPlanContext(0.0)
	ndvDisabled, matchedLen := cardinality.EstimateColsNDVWithMatchedLen(mockCtxDisabled, targetCols, schema, statsInfo)
	expectedConservative := 1000.0 // max(1000, 500) - conservative approach
	require.InDelta(t, expectedConservative, ndvDisabled, 0.1)
	require.Equal(t, 1, matchedLen)

	// Test with variable enabled (skewRatio = 1.0) - should use exponential backoff
	mockCtxEnabled := createMockPlanContext(1.0)
	ndvEnabled, matchedLen := cardinality.EstimateColsNDVWithMatchedLen(mockCtxEnabled, targetCols, schema, statsInfo)
	expectedExponential := 1000 * math.Sqrt(500) // ~22360.7 - exponential backoff
	require.InDelta(t, expectedExponential, ndvEnabled, 0.1)
	require.Equal(t, 1, matchedLen)

	// Verify they produce different results
	require.NotEqual(t, ndvDisabled, ndvEnabled)
	require.Greater(t, ndvEnabled, ndvDisabled) // Exponential should be higher

	// Test with variable partially enabled (skewRatio = 0.5) - should blend
	mockCtxBlended := createMockPlanContext(0.5)
	ndvBlended, _ := cardinality.EstimateColsNDVWithMatchedLen(mockCtxBlended, targetCols, schema, statsInfo)
	expectedBlended := expectedConservative + (expectedExponential-expectedConservative)*0.5
	require.InDelta(t, expectedBlended, ndvBlended, 0.1)
	require.Greater(t, ndvBlended, ndvDisabled)
	require.Less(t, ndvBlended, ndvEnabled)

	// Test additional column combinations with exponential backoff enabled
	targetCols = []*expression.Column{colA, colC}
	ndv, matchedLen = cardinality.EstimateColsNDVWithMatchedLen(mockCtxEnabled, targetCols, schema, statsInfo)
	expectedAC := 1000 * math.Sqrt(10)
	require.InDelta(t, expectedAC, ndv, 0.1)
	require.Equal(t, 1, matchedLen)

	targetCols = []*expression.Column{colB, colC}
	ndv, matchedLen = cardinality.EstimateColsNDVWithMatchedLen(mockCtxEnabled, targetCols, schema, statsInfo)
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
	ndv, matchedLen = cardinality.EstimateColsNDVWithMatchedLen(mockCtxEnabled, targetCols, schema, statsInfoNoGroup)
	expectedABNoGroup := 1000 * math.Sqrt(500) // Same as with GroupNDVs since no exact match
	require.InDelta(t, expectedABNoGroup, ndv, 0.1)
	require.Equal(t, 1, matchedLen)

	targetCols = []*expression.Column{colA, colC}
	ndv, matchedLen = cardinality.EstimateColsNDVWithMatchedLen(mockCtxEnabled, targetCols, schema, statsInfoNoGroup)
	expectedACNoGroup := 1000 * math.Sqrt(10)
	require.InDelta(t, expectedACNoGroup, ndv, 0.1)
	require.Equal(t, 1, matchedLen)

	// Test 3-column combination without GroupNDVs
	targetCols = []*expression.Column{colA, colB, colC}
	ndv, matchedLen = cardinality.EstimateColsNDVWithMatchedLen(mockCtxEnabled, targetCols, schema, statsInfoNoGroup)
	// NDVs sorted descending: [1000, 500, 10]
	expectedABCNoGroup := 1000 * math.Sqrt(500) * math.Sqrt(math.Sqrt(10))
	require.InDelta(t, expectedABCNoGroup, ndv, 0.1)
	require.Equal(t, 1, matchedLen)

	// Test empty columns - should return 1.0 and not record the opt variable
	var emptyTargetCols []*expression.Column
	ndv, matchedLen = cardinality.EstimateColsNDVWithMatchedLen(mockCtxEnabled, emptyTargetCols, schema, statsInfoNoGroup)
	require.Equal(t, 1.0, ndv)
	require.Equal(t, 1, matchedLen)

	// Test single column - should use conservative estimate only (no exponential backoff)
	singleTargetCol := []*expression.Column{colA}
	ndv, matchedLen = cardinality.EstimateColsNDVWithMatchedLen(mockCtxEnabled, singleTargetCol, schema, statsInfoNoGroup)
	require.Equal(t, 1000.0, ndv) // Should be exactly colA's NDV
	require.Equal(t, 1, matchedLen)
}
