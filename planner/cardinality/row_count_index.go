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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/planner/util/debugtrace"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/ranger"
)

// GetRowCountByIndexRanges estimates the row count by a slice of Range.
func GetRowCountByIndexRanges(sctx sessionctx.Context, coll *statistics.HistColl, idxID int64, indexRanges []*ranger.Range) (result float64, err error) {
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
	if !ok || idx.IsInvalid(sctx, coll.Pseudo) {
		colsLen := -1
		if idx != nil && idx.Info.Unique {
			colsLen = len(idx.Info.Columns)
		}
		result, err = getPseudoRowCountByIndexRanges(sc, indexRanges, float64(coll.RealtimeCount), colsLen)
		if err == nil && sc.EnableOptimizerCETrace && ok {
			ceTraceRange(sctx, coll.PhysicalID, colNames, indexRanges, "Index Stats-Pseudo", uint64(result))
		}
		return result, err
	}
	if sctx.GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.RecordAnyValuesWithNames(sctx,
			"Histogram NotNull Count", idx.Histogram.NotNullCount(),
			"TopN total count", idx.TopN.TotalCount(),
			"Increase Factor", idx.GetIncreaseFactor(coll.RealtimeCount),
		)
	}
	if idx.CMSketch != nil && idx.StatsVer == statistics.Version1 {
		result, err = getIndexRowCount(sctx, coll, idxID, indexRanges)
	} else {
		result, err = GetIndexRowCount(sctx, idx, coll, indexRanges, coll.RealtimeCount, coll.ModifyCount)
	}
	if sc.EnableOptimizerCETrace {
		ceTraceRange(sctx, coll.PhysicalID, colNames, indexRanges, "Index Stats", uint64(result))
	}
	return result, errors.Trace(err)
}

func getIndexRowCount(sctx sessionctx.Context, coll *statistics.HistColl, idxID int64, indexRanges []*ranger.Range) (float64, error) {
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
			rangeVals = enumRangeValues(ran.LowVal[rangePosition], ran.HighVal[rangePosition], ran.LowExclude, ran.HighExclude)
			if rangeVals != nil {
				rangePosition++
			}
		}
		// If first one is range, just use the previous way to estimate; if it is [NULL, NULL] range
		// on single-column index, use previous way as well, because CMSketch does not contain null
		// values in this case.
		if rangePosition == 0 || isSingleColIdxNullRange(idx, ran) {
			count, err := GetIndexRowCount(sctx, idx, nil, []*ranger.Range{ran}, coll.RealtimeCount, coll.ModifyCount)
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
			bytes, err := codec.EncodeKey(sc, nil, ran.LowVal[:rangePosition]...)
			if err != nil {
				return 0, errors.Trace(err)
			}
			selectivity, err = getEqualCondSelectivity(sctx, coll, idx, bytes, rangePosition, ran)
			if err != nil {
				return 0, errors.Trace(err)
			}
		} else {
			bytes, err := codec.EncodeKey(sc, nil, ran.LowVal[:rangePosition-1]...)
			if err != nil {
				return 0, errors.Trace(err)
			}
			prefixLen := len(bytes)
			for _, val := range rangeVals {
				bytes = bytes[:prefixLen]
				bytes, err = codec.EncodeKey(sc, bytes, val)
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
			colIDs := coll.Idx2ColumnIDs[idxID]
			var colID int64
			if rangePosition >= len(colIDs) {
				colID = -1
			} else {
				colID = colIDs[rangePosition]
			}
			// prefer index stats over column stats
			if idxIDs, ok := coll.ColID2IdxIDs[colID]; ok && len(idxIDs) > 0 {
				idxID := idxIDs[0]
				count, err = GetRowCountByIndexRanges(sctx, coll, idxID, []*ranger.Range{&rang})
			} else {
				count, err = GetRowCountByColumnRanges(sctx, coll, colID, []*ranger.Range{&rang})
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

// getOrdinalOfRangeCond gets the ordinal of the position range condition,
// if not exist, it returns the end position.
func getOrdinalOfRangeCond(sc *stmtctx.StatementContext, ran *ranger.Range) int {
	for i := range ran.LowVal {
		a, b := ran.LowVal[i], ran.HighVal[i]
		cmp, err := a.Compare(sc, &b, ran.Collators[0])
		if err != nil {
			return 0
		}
		if cmp != 0 {
			return i
		}
	}
	return len(ran.LowVal)
}

const maxNumStep = 10

func enumRangeValues(low, high types.Datum, lowExclude, highExclude bool) []types.Datum {
	if low.Kind() != high.Kind() {
		return nil
	}
	exclude := 0
	if lowExclude {
		exclude++
	}
	if highExclude {
		exclude++
	}
	switch low.Kind() {
	case types.KindInt64:
		// Overflow check.
		lowVal, highVal := low.GetInt64(), high.GetInt64()
		if lowVal <= 0 && highVal >= 0 {
			if lowVal < -maxNumStep || highVal > maxNumStep {
				return nil
			}
		}
		remaining := highVal - lowVal
		if remaining >= maxNumStep+1 {
			return nil
		}
		remaining = remaining + 1 - int64(exclude)
		if remaining >= maxNumStep || remaining < 0 {
			return nil
		}
		values := make([]types.Datum, 0, remaining)
		startValue := lowVal
		if lowExclude {
			startValue++
		}
		for i := int64(0); i < remaining; i++ {
			values = append(values, types.NewIntDatum(startValue+i))
		}
		return values
	case types.KindUint64:
		remaining := high.GetUint64() - low.GetUint64()
		if remaining >= maxNumStep+1 {
			return nil
		}
		remaining = remaining + 1 - uint64(exclude)
		if remaining >= maxNumStep || remaining < 0 {
			return nil
		}
		values := make([]types.Datum, 0, remaining)
		startValue := low.GetUint64()
		if lowExclude {
			startValue++
		}
		for i := uint64(0); i < remaining; i++ {
			values = append(values, types.NewUintDatum(startValue+i))
		}
		return values
	case types.KindMysqlDuration:
		lowDur, highDur := low.GetMysqlDuration(), high.GetMysqlDuration()
		fsp := max(lowDur.Fsp, highDur.Fsp)
		stepSize := int64(math.Pow10(types.MaxFsp-fsp)) * int64(time.Microsecond)
		lowDur.Duration = lowDur.Duration.Round(time.Duration(stepSize))
		remaining := int64(highDur.Duration-lowDur.Duration)/stepSize + 1 - int64(exclude)
		if remaining <= 0 || remaining >= maxNumStep {
			return nil
		}
		startValue := int64(lowDur.Duration)
		if lowExclude {
			startValue += stepSize
		}
		values := make([]types.Datum, 0, remaining)
		for i := int64(0); i < remaining; i++ {
			values = append(values, types.NewDurationDatum(types.Duration{Duration: time.Duration(startValue + i*stepSize), Fsp: fsp}))
		}
		return values
	case types.KindMysqlTime:
		lowTime, highTime := low.GetMysqlTime(), high.GetMysqlTime()
		if lowTime.Type() != highTime.Type() {
			return nil
		}
		fsp := max(lowTime.Fsp(), highTime.Fsp())
		var stepSize int64
		sc := &stmtctx.StatementContext{TimeZone: time.UTC}
		if lowTime.Type() == mysql.TypeDate {
			stepSize = 24 * int64(time.Hour)
			lowTime.SetCoreTime(types.FromDate(lowTime.Year(), lowTime.Month(), lowTime.Day(), 0, 0, 0, 0))
		} else {
			var err error
			lowTime, err = lowTime.RoundFrac(sc, fsp)
			if err != nil {
				return nil
			}
			stepSize = int64(math.Pow10(types.MaxFsp-fsp)) * int64(time.Microsecond)
		}
		remaining := int64(highTime.Sub(sc, &lowTime).Duration)/stepSize + 1 - int64(exclude)
		// When `highTime` is much larger than `lowTime`, `remaining` may be overflowed to a negative value.
		if remaining <= 0 || remaining >= maxNumStep {
			return nil
		}
		startValue := lowTime
		var err error
		if lowExclude {
			startValue, err = lowTime.Add(sc, types.Duration{Duration: time.Duration(stepSize), Fsp: fsp})
			if err != nil {
				return nil
			}
		}
		values := make([]types.Datum, 0, remaining)
		for i := int64(0); i < remaining; i++ {
			value, err := startValue.Add(sc, types.Duration{Duration: time.Duration(i * stepSize), Fsp: fsp})
			if err != nil {
				return nil
			}
			values = append(values, types.NewTimeDatum(value))
		}
		return values
	}
	return nil
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

// GetIndexRowCount returns the row count of the given ranges.
// It uses the modifyCount to adjust the influence of modifications on the table.
func GetIndexRowCount(sctx sessionctx.Context, idx *statistics.Index, coll *statistics.HistColl, indexRanges []*ranger.Range, realtimeRowCount, modifyCount int64) (float64, error) {
	idx.CheckStats()
	sc := sctx.GetSessionVars().StmtCtx
	debugTrace := sc.EnableOptimizerDebugTrace
	if debugTrace {
		debugtrace.EnterContextCommon(sctx)
		defer debugtrace.LeaveContextCommon(sctx)
	}
	totalCount := float64(0)
	isSingleCol := len(idx.Info.Columns) == 1
	for _, indexRange := range indexRanges {
		var count float64
		lb, err := codec.EncodeKey(sc, nil, indexRange.LowVal...)
		if err != nil {
			return 0, err
		}
		rb, err := codec.EncodeKey(sc, nil, indexRange.HighVal...)
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
				count = indexEqualRowCount(sctx, idx, lb, realtimeRowCount)
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
		if isSingleCol && lowIsNull {
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
			count += idx.BetweenRowCount(sctx, l, r)
		}

		// If the current table row count has changed, we should scale the row count accordingly.
		count *= idx.GetIncreaseFactor(realtimeRowCount)

		// handling the out-of-range part
		if (idx.OutOfRange(l) && !(isSingleCol && lowIsNull)) || idx.OutOfRange(r) {
			count += idx.Histogram.OutOfRangeRowCount(sctx, &l, &r, modifyCount)
		}

		if debugTrace {
			debugTraceEndEstimateRange(sctx, count, debugTraceRange)
		}
		totalCount += count
	}
	totalCount = mathutil.Clamp(totalCount, 0, float64(realtimeRowCount))
	return totalCount, nil
}

// expBackoffEstimation estimate the multi-col cases following the Exponential Backoff. See comment below for details.
func expBackoffEstimation(sctx sessionctx.Context, idx *statistics.Index, coll *statistics.HistColl, indexRange *ranger.Range) (sel float64, success bool, err error) {
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
	colsIDs := coll.Idx2ColumnIDs[idx.Histogram.ID]
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
			count      float64
			err        error
			foundStats bool
		)
		if col, ok := coll.Columns[colID]; ok && !col.IsInvalid(sctx, coll.Pseudo) {
			foundStats = true
			count, err = GetRowCountByColumnRanges(sctx, coll, colID, tmpRan)
		}
		if idxIDs, ok := coll.ColID2IdxIDs[colID]; ok && !foundStats && len(indexRange.LowVal) > 1 {
			// Note the `len(indexRange.LowVal) > 1` condition here, it means we only recursively call
			// `GetRowCountByIndexRanges()` when the input `indexRange` is a multi-column range. This
			// check avoids infinite recursion.
			for _, idxID := range idxIDs {
				if idxID == idx.Histogram.ID {
					continue
				}
				foundStats = true
				count, err = GetRowCountByIndexRanges(sctx, coll, idxID, tmpRan)
				if err == nil {
					break
				}
			}
		}
		if !foundStats {
			continue
		}
		if err != nil {
			return 0, false, err
		}
		singleColumnEstResults = append(singleColumnEstResults, count)
	}
	// Sort them.
	slices.Sort(singleColumnEstResults)
	l := len(singleColumnEstResults)
	// Convert the first 4 to selectivity results.
	for i := 0; i < l && i < 4; i++ {
		singleColumnEstResults[i] = singleColumnEstResults[i] / float64(coll.RealtimeCount)
	}
	failpoint.Inject("cleanEstResults", func() {
		singleColumnEstResults = singleColumnEstResults[:0]
		l = 0
	})
	if l == 1 {
		return singleColumnEstResults[0], true, nil
	} else if l == 2 {
		return singleColumnEstResults[0] * math.Sqrt(singleColumnEstResults[1]), true, nil
	} else if l == 3 {
		return singleColumnEstResults[0] * math.Sqrt(singleColumnEstResults[1]) * math.Sqrt(math.Sqrt(singleColumnEstResults[2])), true, nil
	} else if l == 0 {
		return 0, false, nil
	}
	return singleColumnEstResults[0] * math.Sqrt(singleColumnEstResults[1]) * math.Sqrt(math.Sqrt(singleColumnEstResults[2])) * math.Sqrt(math.Sqrt(math.Sqrt(singleColumnEstResults[3]))), true, nil
}

var nullKeyBytes, _ = codec.EncodeKey(nil, nil, types.NewDatum(nil))

func indexEqualRowCount(sctx sessionctx.Context, idx *statistics.Index, b []byte, realtimeRowCount int64) (result float64) {
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
		if idx.Histogram.NDV > 0 && idx.OutOfRange(val) {
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
