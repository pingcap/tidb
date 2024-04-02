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
	"math"
	"slices"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/context"
	"github.com/pingcap/tidb/pkg/planner/util/debugtrace"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

// GetRowCountByIndexRanges estimates the row count by a slice of Range.
func GetRowCountByIndexRanges(sctx context.PlanContext, coll *statistics.HistColl, idxID int64, indexRanges []*ranger.Range) (result float64, err error) {
	var name string
	if sctx.GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(sctx)
		debugTraceGetRowCountInput(sctx, idxID, indexRanges)
		defer func() {
			debugtrace.RecordAnyValuesWithNames(sctx, "Name", name, "Result", result)
			debugtrace.LeaveContextCommon(sctx)
		}()
	}
	sc := sctx.GetSessionVars().StmtCtx
	idx, ok := coll.Indices[idxID]
	colNames := make([]string, 0, 8)
	if ok {
		if idx.Info != nil {
			name = idx.Info.Name.O
			for _, col := range idx.Info.Columns {
				colNames = append(colNames, col.Name.O)
			}
		}
	}
	recordUsedItemStatsStatus(sctx, idx, coll.PhysicalID, idxID)
	if statistics.IndexStatsIsInvalid(sctx, idx, coll, idxID) {
		colsLen := -1
		if idx != nil && idx.Info.Unique {
			colsLen = len(idx.Info.Columns)
		}
		result, err = getPseudoRowCountByIndexRanges(sc.TypeCtx(), indexRanges, float64(coll.RealtimeCount), colsLen)
		if err == nil && sc.EnableOptimizerCETrace && ok {
			ceTraceRange(sctx, coll.PhysicalID, colNames, indexRanges, "Index Stats-Pseudo", uint64(result))
		}
		return result, err
	}
	realtimeCnt, modifyCount := coll.GetScaledRealtimeAndModifyCnt(idx)
	if sctx.GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.RecordAnyValuesWithNames(sctx,
			"Histogram NotNull Count", idx.Histogram.NotNullCount(),
			"TopN total count", idx.TopN.TotalCount(),
			"Increase Factor", idx.GetIncreaseFactor(realtimeCnt),
		)
	}
	if idx.CMSketch != nil && idx.StatsVer == statistics.Version1 {
		result, err = getIndexRowCountForStatsV1(sctx, coll, idxID, indexRanges)
	} else {
		result, err = getIndexRowCountForStatsV2(sctx, idx, coll, indexRanges, realtimeCnt, modifyCount)
	}
	if sc.EnableOptimizerCETrace {
		ceTraceRange(sctx, coll.PhysicalID, colNames, indexRanges, "Index Stats", uint64(result))
	}
	return result, errors.Trace(err)
}

func getIndexRowCountForStatsV1(sctx context.PlanContext, coll *statistics.HistColl, idxID int64, indexRanges []*ranger.Range) (float64, error) {
	sc := sctx.GetSessionVars().StmtCtx
	debugTrace := sc.EnableOptimizerDebugTrace
	if debugTrace {
		debugtrace.EnterContextCommon(sctx)
		defer debugtrace.LeaveContextCommon(sctx)
	}
	idx := coll.Indices[idxID]
	totalCount := float64(0)
	for _, ran := range indexRanges {
		if debugTrace {
			debugTraceStartEstimateRange(sctx, ran, nil, nil, totalCount)
		}
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
			count, err := getIndexRowCountForStatsV2(sctx, idx, nil, []*ranger.Range{ran}, realtimeCnt, modifyCount)
			if err != nil {
				return 0, errors.Trace(err)
			}
			if debugTrace {
				debugTraceEndEstimateRange(sctx, count, debugTraceRange)
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
				count, err = GetRowCountByIndexRanges(sctx, coll, idxID, []*ranger.Range{&rang})
			} else {
				count, err = GetRowCountByColumnRanges(sctx, coll, colUniqueID, []*ranger.Range{&rang})
			}
			if err != nil {
				return 0, errors.Trace(err)
			}
			selectivity = selectivity * count / idx.TotalRowCount()
		}
		count := selectivity * idx.TotalRowCount()
		if debugTrace {
			debugTraceEndEstimateRange(sctx, count, debugTraceRange)
		}
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

// It uses the modifyCount to adjust the influence of modifications on the table.
func getIndexRowCountForStatsV2(sctx context.PlanContext, idx *statistics.Index, coll *statistics.HistColl, indexRanges []*ranger.Range, realtimeRowCount, modifyCount int64) (float64, error) {
	sc := sctx.GetSessionVars().StmtCtx
	debugTrace := sc.EnableOptimizerDebugTrace
	if debugTrace {
		debugtrace.EnterContextCommon(sctx)
		defer debugtrace.LeaveContextCommon(sctx)
	}
	totalCount := float64(0)
	isSingleColIdx := len(idx.Info.Columns) == 1
	for _, indexRange := range indexRanges {
		var count float64
		lb, err := codec.EncodeKey(sc.TimeZone(), nil, indexRange.LowVal...)
		err = sc.HandleError(err)
		if err != nil {
			return 0, err
		}
		rb, err := codec.EncodeKey(sc.TimeZone(), nil, indexRange.HighVal...)
		err = sc.HandleError(err)
		if err != nil {
			return 0, err
		}
		if debugTrace {
			debugTraceStartEstimateRange(sctx, indexRange, lb, rb, totalCount)
		}
		fullLen := len(indexRange.LowVal) == len(indexRange.HighVal) && len(indexRange.LowVal) == len(idx.Info.Columns)
		if bytes.Equal(lb, rb) {
			// case 1: it's a point
			if indexRange.LowExclude || indexRange.HighExclude {
				if debugTrace {
					debugTraceEndEstimateRange(sctx, 0, debugTraceImpossible)
				}
				continue
			}
			if fullLen {
				// At most 1 in this case.
				if idx.Info.Unique {
					totalCount++
					if debugTrace {
						debugTraceEndEstimateRange(sctx, 1, debugTraceUniquePoint)
					}
					continue
				}
				count = equalRowCountOnIndex(sctx, idx, lb, realtimeRowCount)
				// If the current table row count has changed, we should scale the row count accordingly.
				count *= idx.GetIncreaseFactor(realtimeRowCount)
				if debugTrace {
					debugTraceEndEstimateRange(sctx, count, debugTracePoint)
				}
				totalCount += count
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
			count += float64(idx.Histogram.NullCount)
		}
		expBackoffSuccess := false
		// Due to the limitation of calcFraction and convertDatumToScalar, the histogram actually won't estimate anything.
		// If the first column's range is point.
		if rangePosition := getOrdinalOfRangeCond(sc, indexRange); rangePosition > 0 && idx.StatsVer >= statistics.Version2 && coll != nil {
			var expBackoffSel float64
			expBackoffSel, expBackoffSuccess, err = expBackoffEstimation(sctx, idx, coll, indexRange)
			if err != nil {
				return 0, err
			}
			if expBackoffSuccess {
				expBackoffCnt := expBackoffSel * idx.TotalRowCount()

				upperLimit := expBackoffCnt
				// Use the multi-column stats to calculate the max possible row count of [l, r)
				if idx.Histogram.Len() > 0 {
					_, lowerBkt, _, _ := idx.Histogram.LocateBucket(sctx, l)
					_, upperBkt, _, _ := idx.Histogram.LocateBucket(sctx, r)
					if debugTrace {
						statistics.DebugTraceBuckets(sctx, &idx.Histogram, []int{lowerBkt - 1, upperBkt})
					}
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
				if expBackoffCnt > upperLimit {
					expBackoffCnt = upperLimit
				}
				count += expBackoffCnt
			}
		}
		if !expBackoffSuccess {
			count += betweenRowCountOnIndex(sctx, idx, l, r)
		}

		// If the current table row count has changed, we should scale the row count accordingly.
		increaseFactor := idx.GetIncreaseFactor(realtimeRowCount)
		count *= increaseFactor

		// handling the out-of-range part
		if (outOfRangeOnIndex(idx, l) && !(isSingleColIdx && lowIsNull)) || outOfRangeOnIndex(idx, r) {
			histNDV := idx.NDV
			// Exclude the TopN in Stats Version 2
			if idx.StatsVer == statistics.Version2 {
				c, ok := coll.Columns[idx.Histogram.ID]
				// If this is single column of a multi-column index - use the column's NDV rather than index NDV
				isSingleColRange := len(indexRange.LowVal) == len(indexRange.HighVal) && len(indexRange.LowVal) == 1
				if isSingleColRange && !isSingleColIdx && ok && c != nil && c.Histogram.NDV > 0 {
					histNDV = c.Histogram.NDV - int64(c.TopN.Num())
				} else {
					histNDV -= int64(idx.TopN.Num())
				}
			}
			count += idx.Histogram.OutOfRangeRowCount(sctx, &l, &r, modifyCount, histNDV, increaseFactor)
		}

		if debugTrace {
			debugTraceEndEstimateRange(sctx, count, debugTraceRange)
		}
		totalCount += count
	}
	totalCount = mathutil.Clamp(totalCount, 0, float64(realtimeRowCount))
	return totalCount, nil
}

var nullKeyBytes, _ = codec.EncodeKey(time.UTC, nil, types.NewDatum(nil))

func equalRowCountOnIndex(sctx context.PlanContext, idx *statistics.Index, b []byte, realtimeRowCount int64) (result float64) {
	if sctx.GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(sctx)
		debugtrace.RecordAnyValuesWithNames(sctx, "Encoded Value", b)
		defer func() {
			debugtrace.RecordAnyValuesWithNames(sctx, "Result", result)
			debugtrace.LeaveContextCommon(sctx)
		}()
	}
	if len(idx.Info.Columns) == 1 {
		if bytes.Equal(b, nullKeyBytes) {
			return float64(idx.Histogram.NullCount)
		}
	}
	val := types.NewBytesDatum(b)
	if idx.StatsVer < statistics.Version2 {
		if idx.Histogram.NDV > 0 && outOfRangeOnIndex(idx, val) {
			return outOfRangeEQSelectivity(sctx, idx.Histogram.NDV, realtimeRowCount, int64(idx.TotalRowCount())) * idx.TotalRowCount()
		}
		if idx.CMSketch != nil {
			return float64(idx.QueryBytes(sctx, b))
		}
		histRowCount, _ := idx.Histogram.EqualRowCount(sctx, val, false)
		return histRowCount
	}
	// stats version == 2
	// 1. try to find this value in TopN
	if idx.TopN != nil {
		count, found := idx.TopN.QueryTopN(sctx, b)
		if found {
			return float64(count)
		}
	}
	// 2. try to find this value in bucket.Repeat(the last value in every bucket)
	histCnt, matched := idx.Histogram.EqualRowCount(sctx, val, true)
	if matched {
		return histCnt
	}
	// 3. use uniform distribution assumption for the rest (even when this value is not covered by the range of stats)
	histNDV := float64(idx.Histogram.NDV - int64(idx.TopN.Num()))
	if histNDV <= 0 {
		return 0
	}
	return idx.Histogram.NotNullCount() / histNDV
}

// expBackoffEstimation estimate the multi-col cases following the Exponential Backoff. See comment below for details.
func expBackoffEstimation(sctx context.PlanContext, idx *statistics.Index, coll *statistics.HistColl, indexRange *ranger.Range) (sel float64, success bool, err error) {
	if sctx.GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(sctx)
		defer func() {
			debugtrace.RecordAnyValuesWithNames(sctx,
				"Result", sel,
				"Success", success,
				"error", err,
			)
			debugtrace.LeaveContextCommon(sctx)
		}()
	}
	tmpRan := []*ranger.Range{
		{
			LowVal:    make([]types.Datum, 1),
			HighVal:   make([]types.Datum, 1),
			Collators: make([]collate.Collator, 1),
		},
	}
	colsIDs := coll.Idx2ColUniqueIDs[idx.Histogram.ID]
	singleColumnEstResults := make([]float64, 0, len(indexRange.LowVal))
	// The following codes uses Exponential Backoff to reduce the impact of independent assumption. It works like:
	//   1. Calc the selectivity of each column.
	//   2. Sort them and choose the first 4 most selective filter and the corresponding selectivity is sel_1, sel_2, sel_3, sel_4 where i < j => sel_i < sel_j.
	//   3. The final selectivity would be sel_1 * sel_2^{1/2} * sel_3^{1/4} * sel_4^{1/8}.
	// This calculation reduced the independence assumption and can work well better than it.
	for i := 0; i < len(indexRange.LowVal); i++ {
		tmpRan[0].LowVal[0] = indexRange.LowVal[i]
		tmpRan[0].HighVal[0] = indexRange.HighVal[i]
		tmpRan[0].Collators[0] = indexRange.Collators[0]
		if i == len(indexRange.LowVal)-1 {
			tmpRan[0].LowExclude = indexRange.LowExclude
			tmpRan[0].HighExclude = indexRange.HighExclude
		}
		colID := colsIDs[i]
		var (
			count       float64
			selectivity float64
			err         error
			foundStats  bool
		)
		if !statistics.ColumnStatsIsInvalid(coll.Columns[colID], sctx, coll, colID) {
			foundStats = true
			count, err = GetRowCountByColumnRanges(sctx, coll, colID, tmpRan)
			selectivity = count / float64(coll.RealtimeCount)
		}
		if idxIDs, ok := coll.ColUniqueID2IdxIDs[colID]; ok && !foundStats && len(indexRange.LowVal) > 1 {
			// Note the `len(indexRange.LowVal) > 1` condition here, it means we only recursively call
			// `GetRowCountByIndexRanges()` when the input `indexRange` is a multi-column range. This
			// check avoids infinite recursion.
			for _, idxID := range idxIDs {
				if idxID == idx.Histogram.ID {
					continue
				}
				idxStats, ok := coll.Indices[idxID]
				if !ok || statistics.IndexStatsIsInvalid(sctx, idxStats, coll, idxID) {
					continue
				}
				foundStats = true
				count, err = GetRowCountByIndexRanges(sctx, coll, idxID, tmpRan)
				if err == nil {
					break
				}
				realtimeCnt, _ := coll.GetScaledRealtimeAndModifyCnt(idxStats)
				selectivity = count / float64(realtimeCnt)
			}
		}
		if !foundStats {
			continue
		}
		if err != nil {
			return 0, false, err
		}
		singleColumnEstResults = append(singleColumnEstResults, selectivity)
	}
	// Sort them.
	slices.Sort(singleColumnEstResults)
	l := len(singleColumnEstResults)
	failpoint.Inject("cleanEstResults", func() {
		singleColumnEstResults = singleColumnEstResults[:0]
		l = 0
	})
	if l == 1 {
		return singleColumnEstResults[0], true, nil
	} else if l == 0 {
		return 0, false, nil
	}
	// Do not allow the exponential backoff to go below the available index bound. If the number of predicates
	// is less than the number of index columns - use 90% of the bound to differentiate a subset from full index match.
	// If there is an individual column selectivity that goes below this bound, use that selectivity only.
	histNDV := coll.RealtimeCount
	if idx.NDV > 0 {
		histNDV = idx.NDV
	}
	idxLowBound := 1 / float64(min(histNDV, coll.RealtimeCount))
	if l < len(idx.Info.Columns) {
		idxLowBound /= 0.9
	}
	minTwoCol := min(singleColumnEstResults[0], singleColumnEstResults[1], idxLowBound)
	multTwoCol := singleColumnEstResults[0] * math.Sqrt(singleColumnEstResults[1])
	if l == 2 {
		return max(minTwoCol, multTwoCol), true, nil
	}
	minThreeCol := min(minTwoCol, singleColumnEstResults[2])
	multThreeCol := multTwoCol * math.Sqrt(math.Sqrt(singleColumnEstResults[2]))
	if l == 3 {
		return max(minThreeCol, multThreeCol), true, nil
	}
	minFourCol := min(minThreeCol, singleColumnEstResults[3])
	multFourCol := multThreeCol * math.Sqrt(math.Sqrt(math.Sqrt(singleColumnEstResults[3])))
	return max(minFourCol, multFourCol), true, nil
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
// The input sctx is just for debug trace, you can pass nil safely if that's not needed.
func betweenRowCountOnIndex(sctx context.PlanContext, idx *statistics.Index, l, r types.Datum) float64 {
	histBetweenCnt := idx.Histogram.BetweenRowCount(sctx, l, r)
	if idx.StatsVer == statistics.Version1 {
		return histBetweenCnt
	}
	return float64(idx.TopN.BetweenCount(sctx, l.GetBytes(), r.GetBytes())) + histBetweenCnt
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
