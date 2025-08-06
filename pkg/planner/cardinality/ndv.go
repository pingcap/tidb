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
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const (
	distinctFactor = 0.8
	riskThreshold  = 0.3
	// maxNDVRowCountRatio is the maximum ratio of NDV to total row count.
	// NDV estimates should not exceed this percentage of total rows.
	maxNDVRowCountRatio = 0.1
)

// EstimateColumnNDV computes estimated NDV of specified column using the original
// histogram of `DataSource` which is retrieved from storage(not the derived one).
func EstimateColumnNDV(tbl *statistics.Table, colID int64) (ndv float64) {
	hist := tbl.GetCol(colID)
	if hist != nil && hist.IsStatsInitialized() {
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
	totCount := int64(0)
	stop := false
	statsTbl.ForEachIndexImmutable(func(_ int64, idx *statistics.Index) bool {
		if idx.IsFullLoad() && idx.LastUpdateVersion == colHist.LastUpdateVersion {
			totCount = int64(idx.TotalRowCount())
			stop = true
			return true
		}
		return false
	})
	if stop {
		return totCount
	}
	statsTbl.ForEachColumnImmutable(func(_ int64, col *statistics.Column) bool {
		if col.IsFullLoad() && col.LastUpdateVersion == colHist.LastUpdateVersion {
			totCount = int64(col.TotalRowCount())
			return true
		}
		return false
	})
	return totCount
}

// EstimateColsNDVWithMatchedLen returns the NDV of a couple of columns.
// If the columns match any GroupNDV maintained by child operator, we can get an accurate NDV.
// This method is primarily used by join operations.
func EstimateColsNDVWithMatchedLen(cols []*expression.Column, schema *expression.Schema, profile *property.StatsInfo) (float64, int) {
	ndv := 1.0
	if groupNDV := profile.GetGroupNDV4Cols(cols); groupNDV != nil {
		return math.Max(groupNDV.NDV, ndv), len(groupNDV.Cols)
	}
	return estimateNaiveNDV(cols, schema, profile), 1
}

// EstimateGroupNDV returns NDV estimation specifically for GROUP BY operations.
// It uses risk-aware selection between exponential backoff and naive estimates.
func EstimateGroupNDV(cols []*expression.Column, schema *expression.Schema, profile *property.StatsInfo) float64 {
	// First try exact match from existing GroupNDVs
	if groupNDV := profile.GetGroupNDV4Cols(cols); groupNDV != nil {
		return math.Max(groupNDV.NDV, 1.0)
	}

	// Calculate both estimates
	expoNDV := estimateNDVWithExponentialBackoff(cols, schema, profile)
	naiveNDV := estimateNaiveNDV(cols, schema, profile)

	// Calculate risk factor and choose NDV strategy
	riskFactor := calculateNDVRiskFactor(expoNDV, naiveNDV)
	finalNDV := chooseNDVBasedOnRisk(expoNDV, naiveNDV, riskFactor)

	return finalNDV
}

// calculateNDVRiskFactor assesses the risk of using exponential backoff vs naive estimates.
// Higher risk factor means less confidence in exponential backoff estimate.
func calculateNDVRiskFactor(expoNDV, naiveNDV float64) float64 {
	// Calculate divergence between exponential backoff and naive estimates
	maxNDV := math.Max(expoNDV, naiveNDV)
	if maxNDV == 0 {
		return 0.0
	}
	divergence := math.Abs(expoNDV-naiveNDV) / maxNDV
	return divergence
}

// chooseNDVBasedOnRisk selects between exponential backoff and naive estimates based on risk.
func chooseNDVBasedOnRisk(expoNDV, naiveNDV, riskFactor float64) float64 {
	if riskFactor < riskThreshold {
		return expoNDV
	} else {
		return naiveNDV
	}
}

// estimateNaiveNDV implements the original max NDV approach.
func estimateNaiveNDV(cols []*expression.Column, schema *expression.Schema, profile *property.StatsInfo) float64 {
	if profile == nil || len(cols) == 0 {
		return 1.0
	}

	maxNDV := 1.0
	indices := schema.ColumnsIndices(cols)
	if indices == nil {
		return 1.0
	}

	for _, idx := range indices {
		col := schema.Columns[idx]
		if colNDV, exists := profile.ColNDVs[col.UniqueID]; exists && colNDV > 0 {
			maxNDV = math.Max(maxNDV, colNDV)
		}
	}

	return maxNDV
}

// estimateNDVWithExponentialBackoff applies exponential backoff estimation to NDV calculation.
func estimateNDVWithExponentialBackoff(
	cols []*expression.Column, schema *expression.Schema, profile *property.StatsInfo) float64 {
	defaultNdv := 1.0
	if profile == nil || len(cols) == 0 {
		return defaultNdv
	}

	// Collect individual column NDVs
	singleColumnNDVs := make([]float64, 0, len(cols))
	indices := schema.ColumnsIndices(cols)
	if indices == nil {
		logutil.BgLogger().Error("column not found in schema", zap.Any("columns", cols), zap.String("schema", schema.String()))
		return defaultNdv
	}

	for _, idx := range indices {
		col := schema.Columns[idx]
		if colNDV, exists := profile.ColNDVs[col.UniqueID]; exists && colNDV > 0 {
			singleColumnNDVs = append(singleColumnNDVs, colNDV)
		}
	}

	if len(singleColumnNDVs) == 0 {
		return defaultNdv
	}

	// Sort NDVs in descending order (highest NDV first for exponential backoff)
	slices.Sort(singleColumnNDVs)
	slices.Reverse(singleColumnNDVs)

	// Calculate bounds
	lowerBound := max(singleColumnNDVs[0], defaultNdv) // At least max individual column NDV
	upperBound := profile.RowCount * maxNDVRowCountRatio
	// In case RowCount is not accurate, we fall back to naive approach
	if upperBound <= lowerBound {
		return lowerBound
	}

	// Apply exponential backoff directly to NDV values
	resultNDV := ApplyExponentialBackoff(singleColumnNDVs, lowerBound, upperBound)

	return resultNDV
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
