// Copyright 2026 PingCAP, Inc.
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
	"testing"

	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestProgressiveFMSketchBasic(t *testing.T) {
	sketch := NewProgressiveFMSketch(MaxSketchSize)
	require.NotNil(t, sketch)
	require.NotNil(t, sketch.FMSketch)
	require.Equal(t, uint64(0), sketch.RowsProcessed())
	require.Equal(t, uint64(0), sketch.RowsSampled())
	require.Equal(t, float64(1.0), sketch.CurrentPhaseRate())
}

func TestProgressiveFMSketchSamplingPhases(t *testing.T) {
	sketch := NewProgressiveFMSketch(MaxSketchSize)

	// Test that first 500K rows are always sampled (100% rate)
	for i := 0; i < 1000; i++ {
		sampled := sketch.ShouldSample()
		require.True(t, sampled, "First 500K rows should always be sampled")
	}

	require.Equal(t, uint64(1000), sketch.RowsProcessed())
	require.Equal(t, uint64(1000), sketch.RowsSampled())
	require.Equal(t, float64(1.0), sketch.SampleRate())
}

func TestProgressiveFMSketchPhaseTransition(t *testing.T) {
	// Create a sketch with custom schedule for easier testing
	// Note: Phase transition occurs when rowsProcessed >= threshold
	schedule := []SamplingPhase{
		{Threshold: 0, Rate: 1.0},    // 0-100: 100% (rows 1-100)
		{Threshold: 101, Rate: 0.5},  // 101-200: 50% (rows 101-200)
		{Threshold: 201, Rate: 0.25}, // 201+: 25%
	}
	sketch := NewProgressiveFMSketchWithSchedule(MaxSketchSize, schedule)

	// First 100 rows should all be sampled at 100% rate
	for i := 0; i < 100; i++ {
		sketch.ShouldSample()
	}
	require.Equal(t, uint64(100), sketch.RowsProcessed())
	require.Equal(t, uint64(100), sketch.RowsSampled())
	require.Equal(t, float64(1.0), sketch.CurrentPhaseRate())

	// After 100 rows (row 101), rate should drop to 50%
	sketch.ShouldSample() // row 101
	require.Equal(t, float64(0.5), sketch.CurrentPhaseRate())

	// Process more rows - after 200 (row 201), rate should be 25%
	for i := 0; i < 100; i++ {
		sketch.ShouldSample()
	}
	require.Equal(t, float64(0.25), sketch.CurrentPhaseRate())
}

func TestProgressiveFMSketchInsertValue(t *testing.T) {
	sketch := NewProgressiveFMSketch(MaxSketchSize)
	sc := stmtctx.NewStmtCtx()

	// Insert some values
	for i := 0; i < 100; i++ {
		datum := types.NewIntDatum(int64(i))
		sampled, err := sketch.InsertValue(sc, datum)
		require.NoError(t, err)
		require.True(t, sampled, "First values should be sampled")
	}

	// Check NDV
	ndv := sketch.EstimateNDV()
	require.Equal(t, int64(100), ndv, "NDV should be 100 for 100 unique values")
}

func TestProgressiveFMSketchNDVExtrapolation(t *testing.T) {
	// Create a sketch with custom schedule for easier testing
	schedule := []SamplingPhase{
		{Threshold: 0, Rate: 1.0},     // 0-1000: 100%
		{Threshold: 1000, Rate: 0.10}, // 1000+: 10%
	}
	sketch := NewProgressiveFMSketchWithSchedule(MaxSketchSize, schedule)
	sc := stmtctx.NewStmtCtx()

	// Insert unique values
	uniqueValues := 2000
	for i := 0; i < uniqueValues; i++ {
		datum := types.NewIntDatum(int64(i))
		_, err := sketch.InsertValue(sc, datum)
		require.NoError(t, err)
	}

	// After inserting 2000 unique values with partial sampling,
	// the estimated NDV should be reasonably close to 2000
	ndv := sketch.EstimateNDV()

	// With 10% sampling after first 1000, we expect some extrapolation
	// Allow a generous error margin for probabilistic sampling
	require.Greater(t, ndv, int64(1500), "NDV should be at least 1500")
	require.Less(t, ndv, int64(3000), "NDV should be at most 3000")
}

func TestProgressiveFMSketchConfidenceInterval(t *testing.T) {
	sketch := NewProgressiveFMSketch(MaxSketchSize)
	sc := stmtctx.NewStmtCtx()

	// Insert some values
	for i := 0; i < 100; i++ {
		datum := types.NewIntDatum(int64(i))
		_, err := sketch.InsertValue(sc, datum)
		require.NoError(t, err)
	}

	// When we sample 100%, confidence interval should be exact
	lower, upper := sketch.ConfidenceInterval(0.95)
	ndv := sketch.EstimateNDV()
	require.Equal(t, ndv, lower)
	require.Equal(t, ndv, upper)
}

func TestProgressiveFMSketchCopy(t *testing.T) {
	sketch := NewProgressiveFMSketch(MaxSketchSize)
	sc := stmtctx.NewStmtCtx()

	// Insert some values
	for i := 0; i < 100; i++ {
		datum := types.NewIntDatum(int64(i))
		_, err := sketch.InsertValue(sc, datum)
		require.NoError(t, err)
	}

	// Copy the sketch
	copied := sketch.Copy()
	require.NotNil(t, copied)
	require.Equal(t, sketch.RowsProcessed(), copied.RowsProcessed())
	require.Equal(t, sketch.RowsSampled(), copied.RowsSampled())
	require.Equal(t, sketch.EstimateNDV(), copied.EstimateNDV())

	// Modify original shouldn't affect copy
	for i := 100; i < 200; i++ {
		datum := types.NewIntDatum(int64(i))
		_, err := sketch.InsertValue(sc, datum)
		require.NoError(t, err)
	}
	require.NotEqual(t, sketch.RowsProcessed(), copied.RowsProcessed())
}

func TestProgressiveFMSketchMerge(t *testing.T) {
	sketch1 := NewProgressiveFMSketch(MaxSketchSize)
	sketch2 := NewProgressiveFMSketch(MaxSketchSize)
	sc := stmtctx.NewStmtCtx()

	// Insert different values into each sketch
	for i := 0; i < 50; i++ {
		datum := types.NewIntDatum(int64(i))
		_, err := sketch1.InsertValue(sc, datum)
		require.NoError(t, err)
	}
	for i := 50; i < 100; i++ {
		datum := types.NewIntDatum(int64(i))
		_, err := sketch2.InsertValue(sc, datum)
		require.NoError(t, err)
	}

	// Merge sketch2 into sketch1
	sketch1.MergeProgressiveFMSketch(sketch2)

	// Combined stats
	require.Equal(t, uint64(100), sketch1.RowsProcessed())
	require.Equal(t, uint64(100), sketch1.RowsSampled())
	// NDV should be close to 100 (all unique values)
	ndv := sketch1.EstimateNDV()
	require.GreaterOrEqual(t, ndv, int64(90))
	require.LessOrEqual(t, ndv, int64(110))
}

func TestProgressiveFMSketchReset(t *testing.T) {
	sketch := NewProgressiveFMSketch(MaxSketchSize)
	sc := stmtctx.NewStmtCtx()

	// Insert some values
	for i := 0; i < 100; i++ {
		datum := types.NewIntDatum(int64(i))
		_, err := sketch.InsertValue(sc, datum)
		require.NoError(t, err)
	}

	require.Greater(t, sketch.RowsProcessed(), uint64(0))

	// Reset
	sketch.Reset()

	require.Equal(t, uint64(0), sketch.RowsProcessed())
	require.Equal(t, uint64(0), sketch.RowsSampled())
	require.Equal(t, float64(1.0), sketch.CurrentPhaseRate())
	require.Equal(t, int64(0), sketch.EstimateNDV())
}

func TestProgressiveFMSketchMemoryUsage(t *testing.T) {
	sketch := NewProgressiveFMSketch(MaxSketchSize)
	sc := stmtctx.NewStmtCtx()

	memBefore := sketch.MemoryUsage()
	require.Greater(t, memBefore, int64(0))

	// Insert values
	for i := 0; i < 100; i++ {
		datum := types.NewIntDatum(int64(i))
		_, err := sketch.InsertValue(sc, datum)
		require.NoError(t, err)
	}

	memAfter := sketch.MemoryUsage()
	require.Greater(t, memAfter, memBefore)
}

func TestDefaultProgressiveSchedule(t *testing.T) {
	// Verify the default schedule is properly defined
	require.Equal(t, 7, len(DefaultProgressiveSchedule))

	// Check thresholds are monotonically increasing
	for i := 1; i < len(DefaultProgressiveSchedule); i++ {
		require.Greater(t, DefaultProgressiveSchedule[i].Threshold,
			DefaultProgressiveSchedule[i-1].Threshold)
	}

	// Check rates are monotonically decreasing
	for i := 1; i < len(DefaultProgressiveSchedule); i++ {
		require.Less(t, DefaultProgressiveSchedule[i].Rate,
			DefaultProgressiveSchedule[i-1].Rate)
	}

	// First rate should be 1.0 (100%)
	require.Equal(t, float64(1.0), DefaultProgressiveSchedule[0].Rate)

	// Last rate should be 0.01 (1%)
	require.Equal(t, float64(0.01), DefaultProgressiveSchedule[len(DefaultProgressiveSchedule)-1].Rate)
}

func TestNormalQuantile(t *testing.T) {
	// Test common confidence levels
	require.InDelta(t, 1.96, normalQuantile(0.95), 0.01)
	require.InDelta(t, 2.576, normalQuantile(0.99), 0.01)
	require.InDelta(t, 1.645, normalQuantile(0.90), 0.01)
	require.InDelta(t, 1.282, normalQuantile(0.80), 0.01)
}

func TestProgressiveFMSketchWithDuplicates(t *testing.T) {
	sketch := NewProgressiveFMSketch(MaxSketchSize)
	sc := stmtctx.NewStmtCtx()

	// Insert many duplicates - only 10 unique values
	uniqueValues := 10
	duplicatesPerValue := 100
	for i := 0; i < uniqueValues; i++ {
		for j := 0; j < duplicatesPerValue; j++ {
			datum := types.NewIntDatum(int64(i))
			_, err := sketch.InsertValue(sc, datum)
			require.NoError(t, err)
		}
	}

	// NDV should be close to 10 despite 1000 rows
	ndv := sketch.EstimateNDV()
	require.GreaterOrEqual(t, ndv, int64(8))
	require.LessOrEqual(t, ndv, int64(15))
}
