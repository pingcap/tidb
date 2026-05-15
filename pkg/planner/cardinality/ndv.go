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
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

func init() {
	property.ScaleNDVFunc = ScaleNDV
}

const distinctFactor = 0.8

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
func EstimateColsNDVWithMatchedLen(sctx planctx.PlanContext, cols []*expression.Column, schema *expression.Schema,
	profile *property.StatsInfo) (float64, int) {
	// Early return for empty columns - no NDV estimation needed
	if len(cols) == 0 {
		return 1.0, 1
	}

	// First try exact match from existing GroupNDVs
	if groupNDV := profile.GetGroupNDV4Cols(cols); groupNDV != nil {
		exact := math.Max(groupNDV.NDV, 1.0)
		return exact, len(groupNDV.Cols)
	}

	conservativeNDV := estimateNaiveNDV(cols, schema, profile)

	// For single column, conservative and exponential are the same - return early
	if len(cols) == 1 {
		return conservativeNDV, 1
	}

	// Multi-column case: calculate exponential estimate
	exponentialNDV := estimateNDVWithExponentialBackoff(cols, schema, profile)

	// Check if risk-based estimation is enabled
	if sctx != nil {
		skewRatio := sctx.GetSessionVars().RiskGroupNDVSkewRatio
		sctx.GetSessionVars().RecordRelevantOptVar(vardef.TiDBOptRiskGroupNDVSkewRatio)
		if skewRatio > 0 {
			// Use risk-based blending between conservative and exponential
			blendedNDV := calculateGroupNDVWithSkewRatio(conservativeNDV, exponentialNDV, skewRatio)
			return blendedNDV, 1
		}
	}

	// Default behavior: return conservative estimate only (production mode)
	return conservativeNDV, 1
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
	upperBound := profile.RowCount
	// In case RowCount is not accurate, we fall back to naive approach
	if upperBound <= lowerBound {
		return lowerBound
	}

	// Apply exponential backoff directly to NDV values
	resultNDV := ApplyExponentialBackoff(singleColumnNDVs, lowerBound, upperBound)

	return resultNDV
}

// calculateGroupNDVWithSkewRatio calculates group NDV estimate using skew ratio.
// The ratio controls how much to trust exponential backoff vs. conservative estimate:
// 0.0 = fully conservative estimate
// 0.1 = mostly conservative with 10% exponential influence
// 0.5 = balanced between conservative and exponential
// 1.0 = fully trust exponential backoff
func calculateGroupNDVWithSkewRatio(conservativeNDV, exponentialNDV, skewRatio float64) float64 {
	return conservativeNDV + (exponentialNDV-conservativeNDV)*skewRatio
}

// EstimateColsDNVWithMatchedLenFromUniqueIDs is similar to EstimateColsDNVWithMatchedLen, but it receives UniqueIDs instead of Columns.
func EstimateColsDNVWithMatchedLenFromUniqueIDs(sctx planctx.PlanContext, ids []int64, schema *expression.Schema,
	profile *property.StatsInfo) (float64, int) {
	cols := make([]*expression.Column, 0, len(ids))
	for _, id := range ids {
		cols = append(cols, &expression.Column{
			UniqueID: id,
		})
	}
	return EstimateColsNDVWithMatchedLen(sctx, cols, schema, profile)
}

// ScaleNDV scales the original NDV based on the selectivity of the rows.
func ScaleNDV(vars *variable.SessionVars, originalNDV, originalRows, selectedRows float64) (newNDV float64) {
	skewRatio := vardef.DefOptRiskScaleNDVSkewRatio
	if vars != nil { // for safety
		skewRatio = vars.RiskScaleNDVSkewRatio
	}
	uniformNDV := estimateUniformNDV(originalNDV, originalRows, selectedRows)
	skewedNDV := estimateSkewedNDV(originalNDV, originalRows, selectedRows)
	return skewedNDV*skewRatio + uniformNDV*(1-skewRatio)
}

// estimateUniformNDV scales the original NDV based on the selectivity of the rows.
// This function is based on the uniform assumption:
// 1. each value appears the same number of times in total rows.
// 2. each row has the same possibility to be selected.
// For example, if originalNDV is 5, selectedRows is 6 and originalRows is 10.
// Then we assume that each value appears 10/5 = 2 times (assumption 1).
// For each row, the possibility of being selected is 6/10 = 0.6 (assumption 2).
// Then for each value, the possibility of not being selected is (1-0.6)^2 = 0.16.
// Finally, the new NDV should be 5 * (1-0.16) = 4.2.
func estimateUniformNDV(originalNDV, originalRows, selectedRows float64) (newNDV float64) {
	if originalRows <= 0 || selectedRows <= 0 || originalNDV <= 0 {
		return 0
	}
	newNDV = originalNDV
	if selectedRows >= originalRows {
		return
	}
	selectivity := selectedRows / originalRows
	// uniform assumption that each value appears the same number of times
	rowsPerValue := originalRows / originalNDV
	// the possibility that a value is not selected
	notSelectedPossPerRow := 1 - selectivity
	notSelectedPossPerValue := math.Pow(notSelectedPossPerRow, rowsPerValue)
	newNDV = originalNDV * (1 - notSelectedPossPerValue)

	// revise newNDV
	newNDV = max(newNDV, 1.0)          // at least 1 value
	newNDV = min(newNDV, selectedRows) // at most selectedRows values
	return
}

// estimateSkewedNDV estimates the new NDV based on skewed possibility.
func estimateSkewedNDV(originalNDV, originalRows, selectedRows float64) (newNDV float64) {
	if originalRows <= 0 {
		return 0 // for safety
	}
	return originalNDV * selectedRows / originalRows
}
