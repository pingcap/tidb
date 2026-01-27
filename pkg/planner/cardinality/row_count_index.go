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
	"bytes"
	"slices"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

// GetRowCountByIndexRanges estimates the row count by a slice of Range.
// idxCols used when index statistics are invalid, because coll may not have index info, can be nil whenever index statistics are valid.
func GetRowCountByIndexRanges(sctx planctx.PlanContext, coll *statistics.HistColl, idxID int64, indexRanges []*ranger.Range, idxCols []*expression.Column) (result statistics.RowEstimate, err error) {
	var count, maxCount float64
	sc := sctx.GetSessionVars().StmtCtx
	idx := coll.GetIdx(idxID)
	recordUsedItemStatsStatus(sctx, idx, coll.PhysicalID, idxID)
	if statistics.IndexStatsIsInvalid(sctx, idx, coll, idxID) {
		if hasColumnStats(sctx, coll, idxCols) {
			count, maxCount, err = getPseudoRowCountWithPartialStats(sctx, coll, indexRanges, float64(coll.RealtimeCount), idxCols)
			result = statistics.RowEstimate{Est: count, MinEst: count, MaxEst: maxCount}
		} else {
			colsLen := -1
			if idx != nil && idx.Info.Unique {
				colsLen = len(idx.Info.Columns)
			}
			count, err = getPseudoRowCountByIndexRanges(sc.TypeCtx(), indexRanges, float64(coll.RealtimeCount), colsLen)
			result = statistics.DefaultRowEst(count)
		}
		return result, err
	}
	realtimeCnt, modifyCount := coll.GetScaledRealtimeAndModifyCnt(idx)
	if idx.CMSketch != nil && idx.StatsVer == statistics.Version1 {
		count, err = getIndexRowCountForStatsV1(sctx, coll, idxID, indexRanges)
		result = statistics.DefaultRowEst(count)
	} else {
		result, err = getIndexRowCountForStatsV2(sctx, idx, coll, indexRanges, realtimeCnt, modifyCount)
	}
	return result, errors.Trace(err)
}

func getIndexRowCountForStatsV1(sctx planctx.PlanContext, coll *statistics.HistColl, idxID int64, indexRanges []*ranger.Range) (float64, error) {
	sc := sctx.GetSessionVars().StmtCtx
	idx := coll.GetIdx(idxID)
	totalCount := float64(0)
	for _, ran := range indexRanges {
		rangePosition := getOrdinalOfRangeCond(sc, ran)
		var rangeVals []types.Datum
		// Try to enum the last range values.
		if rangePosition != len(ran.LowVal) {
			rangeVals = statistics.EnumRangeValues(ran.LowVal[rangePosition], ran.HighVal[rangePosition], ran.LowExclude, ran.HighExclude)
			if rangeVals != nil {
				rangePosition++
			}
		}
		// If first one is range, just use the previous way to estimate; if it is [NULL, NULL] range
		// on single-column index, use previous way as well, because CMSketch does not contain null
		// values in this case.
		if rangePosition == 0 || isSingleColIdxNullRange(idx, ran) {
			realtimeCnt, modifyCount := coll.GetScaledRealtimeAndModifyCnt(idx)
			rowEstimate, err := getIndexRowCountForStatsV2(sctx, idx, nil, []*ranger.Range{ran}, realtimeCnt, modifyCount)
			count := rowEstimate.Est
			if err != nil {
				return 0, errors.Trace(err)
			}
			totalCount += count
			continue
		}
		var selectivity float64
		// use CM Sketch to estimate the equal conditions
		if rangeVals == nil {
			bytes, err := codec.EncodeKey(sc.TimeZone(), nil, ran.LowVal[:rangePosition]...)
			err = sc.HandleError(err)
			if err != nil {
				return 0, errors.Trace(err)
			}
			selectivity, err = getEqualCondSelectivity(sctx, coll, idx, bytes, rangePosition, ran)
			if err != nil {
				return 0, errors.Trace(err)
			}
		} else {
			bytes, err := codec.EncodeKey(sc.TimeZone(), nil, ran.LowVal[:rangePosition-1]...)
			err = sc.HandleError(err)
			if err != nil {
				return 0, errors.Trace(err)
			}
			prefixLen := len(bytes)
			for _, val := range rangeVals {
				bytes = bytes[:prefixLen]
				bytes, err = codec.EncodeKey(sc.TimeZone(), bytes, val)
				err = sc.HandleError(err)
				if err != nil {
					return 0, err
				}
				res, err := getEqualCondSelectivity(sctx, coll, idx, bytes, rangePosition, ran)
				if err != nil {
					return 0, errors.Trace(err)
				}
				selectivity += res
			}
		}
		// use histogram to estimate the range condition
		if rangePosition != len(ran.LowVal) {
			rang := ranger.Range{
				LowVal:      []types.Datum{ran.LowVal[rangePosition]},
				LowExclude:  ran.LowExclude,
				HighVal:     []types.Datum{ran.HighVal[rangePosition]},
				HighExclude: ran.HighExclude,
				Collators:   []collate.Collator{ran.Collators[rangePosition]},
			}
			var count float64
			var err error
			colUniqueIDs := coll.Idx2ColUniqueIDs[idxID]
			var colUniqueID int64
			if rangePosition >= len(colUniqueIDs) {
				colUniqueID = -1
			} else {
				colUniqueID = colUniqueIDs[rangePosition]
			}
			// prefer index stats over column stats
			if idxIDs, ok := coll.ColUniqueID2IdxIDs[colUniqueID]; ok && len(idxIDs) > 0 {
				idxID := idxIDs[0]
				var tempResult statistics.RowEstimate
				tempResult, err = GetRowCountByIndexRanges(sctx, coll, idxID, []*ranger.Range{&rang}, nil)
				count = tempResult.Est
			} else {
				var countEst statistics.RowEstimate
				countEst, err = GetRowCountByColumnRanges(sctx, coll, colUniqueID, []*ranger.Range{&rang}, false)
				count = countEst.Est
			}
			if err != nil {
				return 0, errors.Trace(err)
			}
			selectivity = selectivity * count / idx.TotalRowCount()
		}
		count := selectivity * idx.TotalRowCount()
		totalCount += count
	}
	if totalCount > idx.TotalRowCount() {
		totalCount = idx.TotalRowCount()
	}
	return totalCount, nil
}

// isSingleColIdxNullRange checks if a range is [NULL, NULL] on a single-column index.
func isSingleColIdxNullRange(idx *statistics.Index, ran *ranger.Range) bool {
	if len(idx.Info.Columns) > 1 {
		return false
	}
	l, h := ran.LowVal[0], ran.HighVal[0]
	if l.IsNull() && h.IsNull() {
		return true
	}
	return false
}

// It uses the modifyCount to validate, and realtimeRowCount to adjust the influence of modifications on the table.
func getIndexRowCountForStatsV2(sctx planctx.PlanContext, idx *statistics.Index, coll *statistics.HistColl, indexRanges []*ranger.Range, realtimeRowCount, modifyCount int64) (totalCount statistics.RowEstimate, err error) {
	sc := sctx.GetSessionVars().StmtCtx
	isSingleColIdx := len(idx.Info.Columns) == 1
	for _, indexRange := range indexRanges {
		var count statistics.RowEstimate
		var lb, rb []byte
		lb, err = codec.EncodeKey(sc.TimeZone(), nil, indexRange.LowVal...)
		err = sc.HandleError(err)
		if err != nil {
			return statistics.DefaultRowEst(0), err
		}
		rb, err = codec.EncodeKey(sc.TimeZone(), nil, indexRange.HighVal...)
		err = sc.HandleError(err)
		if err != nil {
			return statistics.DefaultRowEst(0), err
		}
		fullLen := len(indexRange.LowVal) == len(indexRange.HighVal) && len(indexRange.LowVal) == len(idx.Info.Columns)
		if bytes.Equal(lb, rb) {
			// case 1: it's a point
			if indexRange.LowExclude || indexRange.HighExclude {
				continue
			}
			if fullLen {
				// At most 1 in this case.
				if idx.Info.Unique {
					if !indexRange.IsOnlyNull() {
						totalCount.AddAll(1)
						continue
					}
					totalCount = statistics.DefaultRowEst(float64(idx.NullCount))
					continue
				}
				count = equalRowCountOnIndex(sctx, idx, lb, realtimeRowCount, modifyCount)
				// If the current table row count has changed, we should scale the row count accordingly.
				count.MultiplyAll(idx.GetIncreaseFactor(realtimeRowCount))
				totalCount.Add(count)
				continue
			}
		}

		// case 2: it's an interval
		// The final interval is [low, high)
		if indexRange.LowExclude {
			lb = kv.Key(lb).PrefixNext()
		}
		if !indexRange.HighExclude {
			rb = kv.Key(rb).PrefixNext()
		}
		l := types.NewBytesDatum(lb)
		r := types.NewBytesDatum(rb)
		lowIsNull := bytes.Equal(lb, nullKeyBytes)
		if isSingleColIdx && lowIsNull {
			count.AddAll(float64(idx.Histogram.NullCount))
		}
		expBackoffSuccess := false
		// Due to the limitation of calcFraction and convertDatumToScalar, the histogram actually won't estimate anything.
		// If the first column's range is point.
		if rangePosition := getOrdinalOfRangeCond(sc, indexRange); rangePosition > 0 && idx.StatsVer >= statistics.Version2 && coll != nil {
			var expBackoffSel, minSel, maxSel float64
			expBackoffSel, minSel, maxSel, expBackoffSuccess, err = expBackoffEstimation(sctx, idx, coll, indexRange)
			if err != nil {
				return statistics.DefaultRowEst(0), err
			}
			if expBackoffSuccess {
				expBackoffResult := statistics.RowEstimate{Est: expBackoffSel, MinEst: minSel, MaxEst: maxSel}
				expBackoffResult.MultiplyAll(idx.TotalRowCount())

				upperLimit := expBackoffResult.Est
				// Use the multi-column stats to calculate the max possible row count of [l, r)
				if idx.Histogram.Len() > 0 {
					_, lowerBkt, _, _ := idx.Histogram.LocateBucket(sctx, l)
					_, upperBkt, _, _ := idx.Histogram.LocateBucket(sctx, r)
					// Use Count of the Bucket before l as the lower bound.
					preCount := float64(0)
					if lowerBkt > 0 {
						preCount = float64(idx.Histogram.Buckets[lowerBkt-1].Count)
					}
					// Use Count of the Bucket where r exists as the upper bound.
					upperCnt := float64(idx.Histogram.Buckets[upperBkt].Count)
					upperLimit = upperCnt - preCount
					upperLimit += float64(idx.TopN.BetweenCount(sctx, lb, rb))
				}

				// If the result of exponential backoff strategy is larger than the result from multi-column stats,
				// 	use the upper limit from multi-column histogram instead.
				if expBackoffResult.Est > upperLimit {
					expBackoffResult.Est = upperLimit
				}
				count.Add(expBackoffResult)
			}
		}
		if !expBackoffSuccess {
			count.Add(betweenRowCountOnIndex(sctx, idx, l, r))
		}

		// If the current table row count has changed, we should scale the row count accordingly.
		increaseFactor := idx.GetIncreaseFactor(realtimeRowCount)
		count.MultiplyAll(increaseFactor)

		// Calculate if the estimate already covers the full range of realtimeRowCount.
		// Use a tolerance factor to avoid precision issues.
		atFullRange := count.Est >= float64(realtimeRowCount)*(1-cost.ToleranceFactor)
		// handling the out-of-range part if the estimate does not cover the full range.
		if !atFullRange && ((outOfRangeOnIndex(idx, l) && !(isSingleColIdx && lowIsNull)) || outOfRangeOnIndex(idx, r)) {
			histNDV := idx.NDV
			// Exclude the TopN in Stats Version 2
			if idx.StatsVer == statistics.Version2 {
				colIDs := coll.Idx2ColUniqueIDs[idx.Histogram.ID]
				// Retrieve column statistics for the 1st index column
				c := coll.GetCol(colIDs[0])
				// If this is single column predicate - use the column's information rather than index.
				// Index histograms are converted to string. Column uses original type - which can be more accurate for out of range
				isSingleColRange := len(indexRange.LowVal) == len(indexRange.HighVal) && len(indexRange.LowVal) == 1
				if isSingleColRange && c != nil && c.Histogram.NDV > 0 && c.Histogram.Len() > 0 {
					histNDV = c.Histogram.NDV - int64(c.TopN.Num())
					count.Add(c.Histogram.OutOfRangeRowCount(sctx, &indexRange.LowVal[0], &indexRange.HighVal[0], realtimeRowCount, modifyCount, histNDV))
				} else {
					// TODO: Extend original datatype out-of-range estimation to multi-column
					histNDV -= int64(idx.TopN.Num())
					count.Add(idx.Histogram.OutOfRangeRowCount(sctx, &l, &r, realtimeRowCount, modifyCount, histNDV))
				}
			} else {
				count.Add(idx.Histogram.OutOfRangeRowCount(sctx, &l, &r, realtimeRowCount, modifyCount, histNDV))
			}
		}

		totalCount.Add(count)
	}
	totalCount.Clamp(1.0, float64(realtimeRowCount))
	return totalCount, nil
}

var nullKeyBytes, _ = codec.EncodeKey(time.UTC, nil, types.NewDatum(nil))

// StatsProvider defines the interface for statistics that can provide the necessary
// information for row count estimation with uniform distribution.
type StatsProvider interface {
	// GetHistogram returns the histogram for this stats object
	GetHistogram() *statistics.Histogram
	// GetTopN returns the TopN for this stats object
	GetTopN() *statistics.TopN
	// TotalRowCount returns the total row count
	TotalRowCount() float64
	// GetIncreaseFactor returns the increase factor for the given realtime row count
	GetIncreaseFactor(realtimeRowCount int64) float64
}

// estimateRowCountWithUniformDistribution estimates row count using uniform distribution assumption
// for values not covered by TopN or histograms. This function handles the common logic used by
// both equalRowCountOnIndex and equalRowCountOnColumn.
func estimateRowCountWithUniformDistribution(
	sctx planctx.PlanContext,
	stats StatsProvider,
	realtimeRowCount int64,
	modifyCount int64,
) statistics.RowEstimate {
	if stats == nil {
		// Return a default estimate when stats are nil
		return statistics.DefaultRowEst(1)
	}
	histogram := stats.GetHistogram()
	topN := stats.GetTopN()
	// Calculate histNDV excluding TopN from NDV
	histNDV := float64(histogram.NDV - int64(topN.Num()))
	totalRowCount := stats.TotalRowCount()
	increaseFactor := stats.GetIncreaseFactor(realtimeRowCount)
	notNullCount := histogram.NotNullCount()

	var avgRowEstimate float64
	if histNDV <= 0 || notNullCount == 0 { // Branch 1: all NDV's are in TopN, and no histograms.
		// We have no histograms, but c.Histogram.NDV > c.TopN.Num().
		// This can happen when sampling collects fewer than all NDV.
		if histNDV > 0 && modifyCount == 0 {
			return statistics.DefaultRowEst(max(float64(topN.MinCount()-1), 1))
		}
		// All values are in TopN (and TopN NDV is accurate).
		// We need to derive a RowCount because the histogram is empty.
		if notNullCount <= 0 {
			notNullCount = totalRowCount - float64(histogram.NullCount)
		}
		avgRowEstimate = outOfRangeFullNDV(float64(histogram.NDV), totalRowCount, notNullCount, float64(realtimeRowCount), increaseFactor, modifyCount)
	} else { // Branch 2: some NDV's are in histograms
		// Calculate the average histogram rows (which excludes topN) and NDV that excluded topN
		avgRowEstimate = notNullCount / histNDV
	}

	// skewRatio determines how much of the potential skew should be considered
	skewRatio := sctx.GetSessionVars().RiskEqSkewRatio
	sctx.GetSessionVars().RecordRelevantOptVar(vardef.TiDBOptRiskEqSkewRatio)
	if skewRatio > 0 {
		// Calculate the worst case selectivity assuming the value is skewed within the remaining values not in TopN.
		skewEstimate := notNullCount - (histNDV - 1)
		minTopN := topN.MinCount()
		if minTopN > 0 {
			// The skewEstimate should not be larger than the minimum TopN value.
			skewEstimate = min(skewEstimate, float64(minTopN))
		}
		return statistics.CalculateSkewRatioCounts(avgRowEstimate, skewEstimate, skewRatio)
	}

	return statistics.DefaultRowEst(avgRowEstimate)
}

// equalRowCountOnIndex estimates the row count by a slice of Range and a Datum.
func equalRowCountOnIndex(sctx planctx.PlanContext, idx *statistics.Index, b []byte, realtimeRowCount, modifyCount int64) (result statistics.RowEstimate) {
	if len(idx.Info.Columns) == 1 {
		if bytes.Equal(b, nullKeyBytes) {
			return statistics.DefaultRowEst(float64(idx.Histogram.NullCount))
		}
	}
	val := types.NewBytesDatum(b)
	if idx.StatsVer < statistics.Version2 {
		if idx.Histogram.NDV > 0 && outOfRangeOnIndex(idx, val) {
			outOfRangeCnt := outOfRangeEQSelectivity(sctx, idx.Histogram.NDV, realtimeRowCount, int64(idx.TotalRowCount())) * idx.TotalRowCount()
			return statistics.DefaultRowEst(outOfRangeCnt)
		}
		if idx.CMSketch != nil {
			return statistics.DefaultRowEst(float64(idx.QueryBytes(sctx, b)))
		}
		histRowCount, _ := idx.Histogram.EqualRowCount(sctx, val, false)
		return statistics.DefaultRowEst(histRowCount)
	}
	// stats version == 2
	// 1. try to find this value in TopN
	if idx.TopN != nil {
		count, found := idx.TopN.QueryTopN(sctx, b)
		if found {
			return statistics.DefaultRowEst(float64(count))
		}
	}
	// 2. try to find this value in bucket.Repeat(the last value in every bucket)
	histCnt, matched := idx.Histogram.EqualRowCount(sctx, val, true)
	// Calculate histNDV here as it's needed for both the underrepresented check and later calculations
	histNDV := float64(idx.Histogram.NDV - int64(idx.TopN.Num()))
	// also check if this last bucket end value is underrepresented
	if matched && !IsLastBucketEndValueUnderrepresented(sctx,
		&idx.Histogram, val, histCnt, histNDV, realtimeRowCount, modifyCount) {
		return statistics.DefaultRowEst(histCnt)
	}
	// 3. use uniform distribution assumption for the rest (even when this value is not covered by the range of stats)
	// branch1: histDNV <= 0 means that all NDV's are in TopN, and no histograms.
	// branch2: histDNA > 0 basically means while there is still a case, c.Histogram.NDV >
	// c.TopN.Num() a little bit, but the histogram is still empty. In this case, we should use the branch1 and for the diff
	// in NDV, it's mainly comes from the NDV is conducted and calculated ahead of sampling.
	return estimateRowCountWithUniformDistribution(sctx, idx, realtimeRowCount, modifyCount)
}

// expBackoffEstimation estimate the multi-col cases following the Exponential Backoff. See comment below for details.
func expBackoffEstimation(sctx planctx.PlanContext, idx *statistics.Index, coll *statistics.HistColl, indexRange *ranger.Range) (sel float64, minSel float64, maxSel float64, success bool, err error) {
	tmpRan := []*ranger.Range{
		{
			LowVal:    make([]types.Datum, 1),
			HighVal:   make([]types.Datum, 1),
			Collators: make([]collate.Collator, 1),
		},
	}
	colsIDs := coll.Idx2ColUniqueIDs[idx.Histogram.ID]
	singleColumnEstResults := make([]float64, 0, len(indexRange.LowVal))
	minSel, maxSel = 1.0, 1.0
	// The following codes uses Exponential Backoff to reduce the impact of independent assumption. It works like:
	//   1. Calc the selectivity of each column.
	//   2. Sort them and choose the first 4 most selective filter and the corresponding selectivity is sel_1, sel_2, sel_3, sel_4 where i < j => sel_i < sel_j.
	//   3. The final selectivity would be sel_1 * sel_2^{1/2} * sel_3^{1/4} * sel_4^{1/8}.
	// This calculation reduced the independence assumption and can work well better than it.
	for i := range indexRange.LowVal {
		tmpRan[0].LowVal[0] = indexRange.LowVal[i]
		tmpRan[0].HighVal[0] = indexRange.HighVal[i]
		tmpRan[0].Collators[0] = indexRange.Collators[0]
		if i == len(indexRange.LowVal)-1 {
			tmpRan[0].LowExclude = indexRange.LowExclude
			tmpRan[0].HighExclude = indexRange.HighExclude
		}
		// Safety check to prevent panic when accessing colsIDs[i]
		if colsIDs == nil || i >= len(colsIDs) {
			continue
		}
		colID := colsIDs[i]
		var (
			count       float64
			selectivity float64
			foundStats  bool
		)
		if !statistics.ColumnStatsIsInvalid(coll.GetCol(colID), sctx, coll, colID) {
			foundStats = true
			var countEst statistics.RowEstimate
			countEst, err = GetRowCountByColumnRanges(sctx, coll, colID, tmpRan, false)
			if err != nil {
				return 0, 0, 0, false, err
			}
			count = countEst.Est
			selectivity = count / float64(coll.RealtimeCount)
			maxSel = min(maxSel, countEst.MaxEst/float64(coll.RealtimeCount))
		}
		if idxIDs, ok := coll.ColUniqueID2IdxIDs[colID]; ok && !foundStats && len(indexRange.LowVal) > 1 {
			// Note the `len(indexRange.LowVal) > 1` condition here, it means we only recursively call
			// `GetRowCountByIndexRanges()` when the input `indexRange` is a multi-column range. This
			// check avoids infinite recursion.
			for _, idxID := range idxIDs {
				idxStats := coll.GetIdx(idxID)
				if idxStats == nil || statistics.IndexStatsIsInvalid(sctx, idxStats, coll, idxID) {
					continue
				}
				foundStats = true
				countResult, err := GetRowCountByIndexRanges(sctx, coll, idxID, tmpRan, nil)
				if err == nil {
					break
				}
				realtimeCnt, _ := coll.GetScaledRealtimeAndModifyCnt(idxStats)
				selectivity = countResult.Est / float64(realtimeCnt)
				maxSel = min(maxSel, countResult.MaxEst/float64(coll.RealtimeCount))
			}
		}
		if !foundStats {
			continue
		}
		singleColumnEstResults = append(singleColumnEstResults, selectivity)
		minSel *= selectivity
	}
	// Sort selectivities ascending (most selective first) for exponential backoff
	slices.Sort(singleColumnEstResults)
	l := len(singleColumnEstResults)
	failpoint.Inject("cleanEstResults", func() {
		singleColumnEstResults = singleColumnEstResults[:0]
		l = 0
	})
	if l == 1 {
		return singleColumnEstResults[0], singleColumnEstResults[0], singleColumnEstResults[0], true, nil
	} else if l == 0 {
		return 0, 0, 0, false, nil
	}
	// Do not allow the exponential backoff to go below the available index bound. If the number of predicates
	// is less than the number of index columns - use 90% of the bound to differentiate a subset from full index match.
	// If there is an individual column selectivity that goes below this bound, use that selectivity only.
	histNDV := coll.RealtimeCount
	if idx.NDV > 0 {
		histNDV = idx.NDV
	}
	idxLowBound := 1 / float64(min(histNDV, coll.RealtimeCount))
	minBound := idxLowBound
	// Adjust idxLowBound upwards if we have not used all index columns.
	if l < len(idx.Info.Columns) {
		idxLowBound /= 0.9
	}
	// maxSel is the "best" selectivity from all maximum of single column selectivities.
	maxSel = max(idxLowBound, maxSel)
	// minSel assumes independence between columns, so is the product of all single column selectivities.
	minSel = max(minBound, minSel)

	// Calculate minimum bound: take minimum of all selectivities (up to limit) and index bound
	maxCols := min(MaxExponentialBackoffCols, l)
	for i := range maxCols {
		minBound = min(minBound, singleColumnEstResults[i])
	}

	// Apply exponential backoff to pre-sorted selectivities
	multResult := ApplyExponentialBackoff(singleColumnEstResults, minBound, 1.0)

	return multResult, minSel, maxSel, true, nil
}

// outOfRangeOnIndex checks if the datum is out of the range.
func outOfRangeOnIndex(idx *statistics.Index, val types.Datum) bool {
	if !idx.Histogram.OutOfRange(val) {
		return false
	}
	if idx.Histogram.Len() > 0 && matchPrefix(idx.Histogram.Bounds.GetRow(0), 0, &val) {
		return false
	}
	return true
}

// matchPrefix checks whether ad is the prefix of value
func matchPrefix(row chunk.Row, colIdx int, ad *types.Datum) bool {
	switch ad.Kind() {
	case types.KindString, types.KindBytes, types.KindBinaryLiteral, types.KindMysqlBit:
		return strings.HasPrefix(row.GetString(colIdx), ad.GetString())
	}
	return false
}

// betweenRowCountOnIndex estimates the row count for interval [l, r).
// The input sctx is required for stats version 2. For version 1, it is just for debug trace, you can pass nil safely.
func betweenRowCountOnIndex(sctx planctx.PlanContext, idx *statistics.Index, l, r types.Datum) statistics.RowEstimate {
	histBetweenResult := idx.Histogram.BetweenRowCount(sctx, l, r)
	if idx.StatsVer == statistics.Version1 {
		return histBetweenResult
	}
	topNCnt := float64(idx.TopN.BetweenCount(sctx, l.GetBytes(), r.GetBytes()))
	histBetweenResult.AddAll(topNCnt)
	return histBetweenResult
}

// getOrdinalOfRangeCond gets the ordinal of the position range condition,
// if not exist, it returns the end position.
func getOrdinalOfRangeCond(sc *stmtctx.StatementContext, ran *ranger.Range) int {
	for i := range ran.LowVal {
		a, b := ran.LowVal[i], ran.HighVal[i]
		cmp, err := a.Compare(sc.TypeCtx(), &b, ran.Collators[0])
		if err != nil {
			return 0
		}
		if cmp != 0 {
			return i
		}
	}
	return len(ran.LowVal)
}

// hasColumnStats checks if we have collected stats on any of the given columns.
func hasColumnStats(sctx planctx.PlanContext, coll *statistics.HistColl, idxCols []*expression.Column) bool {
	if idxCols == nil {
		return false
	}
	for i := range idxCols {
		if !statistics.ColumnStatsIsInvalid(coll.GetCol(idxCols[i].UniqueID), sctx, coll, idxCols[i].UniqueID) {
			return true
		}
	}
	return false
}
