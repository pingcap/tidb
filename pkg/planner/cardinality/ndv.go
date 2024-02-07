// Copyright 2023 PingCAP, Inc.
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

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const distinctFactor = 0.8

// EstimateColumnNDV computes estimated NDV of specified column using the original
// histogram of `DataSource` which is retrieved from storage(not the derived one).
func EstimateColumnNDV(tbl *statistics.Table, colID int64) (ndv float64) {
	hist, ok := tbl.Columns[colID]
	if ok && hist.IsStatsInitialized() {
		ndv = float64(hist.Histogram.NDV)
		// TODO: a better way to get the total row count derived from the last analyze.
		analyzeCount := getTotalRowCount(tbl, hist)
		if analyzeCount > 0 {
			factor := float64(tbl.RealtimeCount) / float64(analyzeCount)
			ndv *= factor
		}
	} else {
		ndv = float64(tbl.RealtimeCount) * distinctFactor
	}
	return ndv
}

// getTotalRowCount returns the total row count, which is obtained when collecting colHist.
func getTotalRowCount(statsTbl *statistics.Table, colHist *statistics.Column) int64 {
	if colHist.IsFullLoad() {
		return int64(colHist.TotalRowCount())
	}
	// If colHist is not fully loaded, we may still get its total row count from other index/column stats.
	for _, idx := range statsTbl.Indices {
		if idx.IsFullLoad() && idx.LastUpdateVersion == colHist.LastUpdateVersion {
			return int64(idx.TotalRowCount())
		}
	}
	for _, col := range statsTbl.Columns {
		if col.IsFullLoad() && col.LastUpdateVersion == colHist.LastUpdateVersion {
			return int64(col.TotalRowCount())
		}
	}
	return 0
}

// EstimateColsNDVWithMatchedLen returns the NDV of a couple of columns.
// If the columns match any GroupNDV maintained by child operator, we can get an accurate NDV.
// Otherwise, we simply return the max NDV among the columns, which is a lower bound.
func EstimateColsNDVWithMatchedLen(cols []*expression.Column, schema *expression.Schema, profile *property.StatsInfo) (float64, int) {
	ndv := 1.0
	if groupNDV := profile.GetGroupNDV4Cols(cols); groupNDV != nil {
		return math.Max(groupNDV.NDV, ndv), len(groupNDV.Cols)
	}
	indices := schema.ColumnsIndices(cols)
	if indices == nil {
		logutil.BgLogger().Error("column not found in schema", zap.Any("columns", cols), zap.String("schema", schema.String()))
		return ndv, 1
	}
	for _, idx := range indices {
		// It is a very naive estimation.
		col := schema.Columns[idx]
		ndv = math.Max(ndv, profile.ColNDVs[col.UniqueID])
	}
	return ndv, 1
}

// EstimateColsDNVWithMatchedLenFromUniqueIDs is similar to EstimateColsDNVWithMatchedLen, but it receives UniqueIDs instead of Columns.
func EstimateColsDNVWithMatchedLenFromUniqueIDs(ids []int64, schema *expression.Schema, profile *property.StatsInfo) (float64, int) {
	cols := make([]*expression.Column, 0, len(ids))
	for _, id := range ids {
		cols = append(cols, &expression.Column{
			UniqueID: id,
		})
	}
	return EstimateColsNDVWithMatchedLen(cols, schema, profile)
}
