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

package globalstats_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/globalstats"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

// TestBuildGlobalStatsFromSamplesWithFilteredColumns verifies that
// BuildGlobalStatsFromSamples must receive the filtered colsInfo (after
// filterSkipColumnTypes) rather than the raw table.Meta().Columns.
//
// When a table has columns that are skipped during ANALYZE (e.g., JSON),
// the sample collector's arrays (Samples[i].Columns, FMSketches, NullCount,
// TotalSizes) are indexed by the filtered column positions. Passing the
// unfiltered table metadata causes an index-out-of-range panic because the
// unfiltered column count exceeds the sample array length.
func TestBuildGlobalStatsFromSamplesWithFilteredColumns(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	sctx := tk.Session().(sessionctx.Context)

	// Simulate a table: (a INT, j JSON, b INT)
	// After filterSkipColumnTypes, JSON is removed → filtered: [a, b]
	// Sample collectors are built with 2 columns, not 3.
	colA := &model.ColumnInfo{
		ID:     1,
		Name:   ast.NewCIStr("a"),
		Offset: 0,
	}
	colA.FieldType = *types.NewFieldType(mysql.TypeLong)

	colJSON := &model.ColumnInfo{
		ID:     2,
		Name:   ast.NewCIStr("j"),
		Offset: 1,
	}
	colJSON.FieldType = *types.NewFieldType(mysql.TypeJSON)

	colB := &model.ColumnInfo{
		ID:     3,
		Name:   ast.NewCIStr("b"),
		Offset: 2,
	}
	colB.FieldType = *types.NewFieldType(mysql.TypeLong)

	unfilteredCols := []*model.ColumnInfo{colA, colJSON, colB} // 3 columns
	filteredCols := []*model.ColumnInfo{colA, colB}            // 2 columns (what samples use)

	// Build a ReservoirRowSampleCollector with 2 columns (the filtered layout).
	numFilteredCols := len(filteredCols)
	collector := statistics.NewReservoirRowSampleCollector(100, numFilteredCols)
	for i := range numFilteredCols {
		collector.Base().FMSketches = append(collector.Base().FMSketches, statistics.NewFMSketch(statistics.MaxSketchSize))
		collector.Base().NullCount[i] = 0
		collector.Base().TotalSizes[i] = 100
	}
	// Add sample rows with 2 datums each (matching filtered column count).
	for i := 1; i <= 10; i++ {
		row := &statistics.ReservoirRowSampleItem{
			Columns: []types.Datum{
				types.NewIntDatum(int64(i)),      // column a
				types.NewIntDatum(int64(i * 10)), // column b
			},
			Weight: int64(i),
		}
		collector.Base().Samples = append(collector.Base().Samples, row)
	}
	collector.Base().Count = 10

	opts := map[ast.AnalyzeOptionType]uint64{
		ast.AnalyzeOptNumBuckets: 256,
		ast.AnalyzeOptNumTopN:    20,
	}
	// histIDs for columns a and b.
	histIDs := []int64{colA.ID, colB.ID}

	// With unfiltered colsInfo (3 columns, but samples only have 2), the code
	// tries to access sample.Columns[2] which is out of bounds → panic.
	require.Panics(t, func() {
		_, _ = globalstats.BuildGlobalStatsFromSamples(
			sctx, collector, opts,
			unfilteredCols, nil, histIDs, false,
		)
	}, "BuildGlobalStatsFromSamples should panic with unfiltered colsInfo due to index out of range")

	// With filtered colsInfo (2 columns, matching sample layout), it succeeds.
	result, err := globalstats.BuildGlobalStatsFromSamples(
		sctx, collector, opts,
		filteredCols, nil, histIDs, false,
	)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, int64(10), result.Count)
	// Both columns should have histograms built.
	require.NotNil(t, result.Hg[0], "histogram for column a should be built")
	require.NotNil(t, result.Hg[1], "histogram for column b should be built")
	// Both columns should have FMSketches.
	require.NotNil(t, result.Fms[0], "FMSketch for column a should be present")
	require.NotNil(t, result.Fms[1], "FMSketch for column b should be present")
}
