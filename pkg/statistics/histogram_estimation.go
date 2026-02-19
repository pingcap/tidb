// Copyright 2017 PingCAP, Inc.
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
	"math"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"go.uber.org/zap"
)

func calculateLeftOverlapPercent(l, r, boundL, histL, histWidth float64) float64 {
	if histWidth <= 0 {
		return 0
	}
	// bound the left/right ranges of the predicates to the "left triangle" of the histogram.
	l = max(l, boundL)
	r = min(r, histL)
	// If there's no overlap after bounding, return 0.
	if l >= r {
		return 0
	}
	// NOTE: Ranges are squared to determine a triangular (linear) distribution rather than uniform.
	// Width of the histogram - squared to normalize to a percentage
	histWidthSq := math.Pow(histWidth, 2)
	// Right side of the predicate as distance from the left edge of the histogram
	rightRange := math.Pow(r-boundL, 2)
	// Left side of the predicate as distance from the left edge of the histogram
	leftRange := math.Pow(l-boundL, 2)
	return (rightRange - leftRange) / histWidthSq
}

// calculateRightOverlapPercent calculates the percentage for an out-of-range overlap
// on the right side of the histogram. The predicate range [l, r] overlaps with
// the region (histR, boundR), and we calculate the percentage of the shaded area.
func calculateRightOverlapPercent(l, r, histR, boundR, histWidth float64) float64 {
	if histWidth <= 0 {
		return 0
	}
	// bound the left/right ranges of the predicates to the "right triangle" of the histogram.
	l = max(l, histR)
	r = min(r, boundR)
	// If there's no overlap after bounding, return 0.
	if l >= r {
		return 0
	}
	// NOTE: Ranges are squared to determine a triangular (linear) distribution rather than uniform.
	// Width of the histogram - squared to normalize to a percentage
	histWidthSq := math.Pow(histWidth, 2)
	// Left side of the predicate as distance from the right edge of the histogram.
	leftRange := math.Pow(boundR-l, 2)
	// Right side of the predicate as distance from the right edge of the histogram.
	rightRange := math.Pow(boundR-r, 2)
	return (leftRange - rightRange) / histWidthSq
}

// OutOfRangeRowCount estimate the row count of part of [lDatum, rDatum] which is out of range of the histogram.
// Here we assume the density of data is decreasing from the lower/upper bound of the histogram toward outside.
// The maximum row count it can get is the modifyCount. It reaches the maximum when out-of-range width reaches histogram range width.
// As it shows below. To calculate the out-of-range row count, we need to calculate the percentage of the shaded area.
// Note that we assume histL-boundL == histR-histL == boundR-histR here.
/*
               /│             │\
             /  │             │  \
           /x│  │◄─histogram─►│    \
         / xx│  │    range    │      \
       / │xxx│  │             │        \
     /   │xxx│  │             │          \
────┴────┴───┴──┴─────────────┴───────────┴─────
    ▲    ▲   ▲  ▲             ▲           ▲
    │    │   │  │             │           │
 boundL  │   │histL         histR       boundR
         │   │
    lDatum  rDatum
*/
// The percentage of shaded area on the left side calculation formula is:
// leftPercent = (math.Pow(actualR-boundL, 2) - math.Pow(actualL-boundL, 2)) / math.Pow(histWidth, 2)
// You can find more details at https://github.com/pingcap/tidb/pull/47966#issuecomment-1778866876
func (hg *Histogram) OutOfRangeRowCount(
	sctx planctx.PlanContext,
	lDatum, rDatum *types.Datum,
	realtimeRowCount, modifyCount, histNDV int64,
) (result RowEstimate) {
	if hg.Len() == 0 {
		return DefaultRowEst(0)
	}

	// Step 1: Calculate a default of "one value"
	// oneValue assumes "one value qualifies", and is used as a lower bound.
	histNDV = max(histNDV, 1)
	oneValue := hg.NotNullCount() / float64(histNDV)

	// Step 2: If modifications are not allowed, return the one value.
	// In OptObjectiveDeterminate mode, we can't rely on real time statistics, so default to assuming
	// one value qualifies.
	allowUseModifyCount := sctx.GetSessionVars().GetOptObjective() != vardef.OptObjectiveDeterminate
	if !allowUseModifyCount {
		return RowEstimate{Est: oneValue, MinEst: oneValue, MaxEst: oneValue}
	}

	// Step 3: Adjust oneValue if the NDV is low
	// If NDV is low, it may no longer be representative of the data since ANALYZE
	// was last run. Use a default value against realtimeRowCount.
	// If NDV is not representitative, then hg.NotNullCount may not be either.
	if float64(histNDV) < outOfRangeBetweenRate {
		oneValue = max(min(oneValue, float64(realtimeRowCount)/outOfRangeBetweenRate), 1.0)
	}
	// Step 4: Calculate how much of the statistics share a common prefix.
	// For bytes and string type, we need to cut the common prefix when converting them to scalar value.
	// Here we calculate the length of common prefix.
	// TODO: If the common prefix is large, we may underestimate the out-of-range
	// portion because we can't distinguish the values with the same prefix.
	commonPrefix := 0
	if hg.GetLower(0).Kind() == types.KindBytes || hg.GetLower(0).Kind() == types.KindString {
		// Calculate the common prefix length among the lower and upper bound of histogram and the range we want to estimate.
		commonPrefix = commonPrefixLength(hg.GetLower(0).GetBytes(),
			hg.GetUpper(hg.Len()-1).GetBytes(),
			lDatum.GetBytes(),
			rDatum.GetBytes())
	}

	// Step 5:Convert the range we want to estimate to scalar value(float64)
	l := convertDatumToScalar(lDatum, commonPrefix)
	r := convertDatumToScalar(rDatum, commonPrefix)
	unsigned := mysql.HasUnsignedFlag(hg.Tp.GetFlag())
	// If this is an unsigned column, we need to make sure values are not negative.
	// Normal negative value should have become 0. But this still might happen when met MinNotNull here.
	// Maybe it's better to do this transformation in the ranger like the normal negative value.
	// Track whether negative values were clamped to 0, to detect impossible ranges.
	var leftClamped, rightClamped bool
	if unsigned {
		if l < 0 {
			l = 0
			leftClamped = true
		}
		if r < 0 {
			r = 0
			rightClamped = true
		}
		// If both bounds collapsed to 0 due to clamping negative values, this is
		// an impossible range (e.g., unsigned_col < 0). Return 0 estimate.
		if l == 0 && r == 0 && (leftClamped || rightClamped) {
			return DefaultRowEst(0)
		}
	}

	// Step 6: Convert the lower and upper bound of the histogram to scalar value(float64)
	histL := convertDatumToScalar(hg.GetLower(0), commonPrefix)
	histR := convertDatumToScalar(hg.GetUpper(hg.Len()-1), commonPrefix)
	histWidth := histR - histL
	// If we find that the histogram width is too small or too large - we still may need to consider
	// the impact of modifications to the table. Reset the histogram width to 0.
	if histWidth < 0 {
		histWidth = 0
	}
	if math.IsInf(histWidth, 1) {
		histWidth = 0
	}
	boundL := histL - histWidth
	boundR := histR + histWidth

	// Step 7: Calculate the width of the predicate
	// TODO: If predWidth == 0, it may be because the common prefix is too large.
	// In future - out of range for equal predicates should also use this logic
	// for consistency. We need to handle both "equal" and "large common prefix".
	predWidth := r - l
	if predWidth < 0 {
		// This should never happen.
		intest.Assert(false, "Right bound should not be less than left bound")

		return DefaultRowEst(0)
	} else if predWidth == 0 {
		// Set histWidth to 0 so that we can still return a minimum of oneValue,
		// and return the max as worst case.
		histWidth = 0
	}

	// Step 8: Calculate the out of range percentages
	// Calculate left overlap percentage if the range overlaps with (boundL, histL)
	leftPercent := calculateLeftOverlapPercent(l, r, boundL, histL, histWidth)
	// Calculate right overlap percentage if the range overlaps with (histR, boundR)
	rightPercent := calculateRightOverlapPercent(l, r, histR, boundR, histWidth)

	totalPercent := min(leftPercent*0.5+rightPercent*0.5, 1.0)
	maxTotalPercent := min(leftPercent+rightPercent, 1.0)

	// Step 9: Calculate the added rows
	// Use absolute value to account for the case where rows may have been added on one side,
	// but deleted from the other, resulting in qualifying out of range rows even though
	// realtimeRowCount is less than histogram count
	addedRows := hg.AbsRowCountDifference(realtimeRowCount)
	maxAddedRows := addedRows

	// Step 10: Calculate the estimated rows
	estRows := oneValue
	skewRatio := sctx.GetSessionVars().RiskRangeSkewRatio
	sctx.GetSessionVars().RecordRelevantOptVar(vardef.TiDBOptRiskRangeSkewRatio)
	if totalPercent > 0 {
		// Multiplying addedRows by 0.5 provides the assumption that 50% "addedRows" are inside
		// the histogram range, and 50% (0.5) are out-of-range. Users can adjust this
		// magic number by setting the session variable `tidb_opt_risk_range_skew_ratio`.
		// When skewRatio > 0, estRows sets the starting point for the skew ratio calculation.
		addedRowMultiplier := 0.5
		// NOTE: Skew ratio is used twice in this function.
		// This first usage allows a user to specify a ratio that is smaller or
		// larger than the default 0.5.
		if skewRatio > 0 {
			addedRowMultiplier = skewRatio
		}
		estRows = (addedRows * addedRowMultiplier) * totalPercent
	}

	// Step 11: Calculate a potential worst case for use in final MaxEst
	// We may have missed the true lowest/highest values due to sampling OR there could be a delay in
	// updates to modifyCount (meaning modifyCount is incorrectly set to 0). So ensure we always
	// account for at least 1% of the total row count as a worst case for "addedRows".
	// We inflate this here so ONLY to impact the MaxEst value.
	if modifyCount == 0 || addedRows == 0 {
		if realtimeRowCount <= 0 {
			realtimeRowCount = int64(hg.TotalRowCount())
		}
		// Use outOfRangeBetweenRate as a divisor to get a small percentage of the approximate
		// modifyCount (since outOfRangeBetweenRate has a default value of 100).
		maxAddedRows = max(maxAddedRows, float64(realtimeRowCount)/outOfRangeBetweenRate)
	}
	if maxTotalPercent > 0 {
		// Always apply maxTotalPercent to maxAddedRows to limit scaling when the predicate has an upper bound
		maxAddedRows *= maxTotalPercent
	}

	// Step 12: Calculate the final min/max/est rows including the skew ratio adjustment
	result.MinEst = min(estRows, oneValue)
	// NOTE: Skew ratio is used twice in this function.
	// This second usage scales the estimate from the base estimate to the max estimate.
	if skewRatio > 0 {
		result = CalculateSkewRatioCounts(estRows, maxAddedRows, skewRatio)
	} else {
		// Do not scale the estimate if skew ratio is not set.
		result.Est = estRows
	}
	result.Est = max(result.Est, oneValue)
	result.MaxEst = max(result.Est, maxAddedRows)

	return result
}
