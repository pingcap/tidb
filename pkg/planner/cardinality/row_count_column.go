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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/planner/util/debugtrace"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

func init() {
	statistics.GetRowCountByColumnRanges = GetRowCountByColumnRanges
	statistics.GetRowCountByIntColumnRanges = GetRowCountByIntColumnRanges
	statistics.GetRowCountByIndexRanges = GetRowCountByIndexRanges
}

// GetRowCountByColumnRanges estimates the row count by a slice of Range.
func GetRowCountByColumnRanges(sctx planctx.PlanContext, coll *statistics.HistColl, colUniqueID int64, colRanges []*ranger.Range) (result statistics.RowEstimate, err error) {
	var name string
	if sctx.GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(sctx)
		debugTraceGetRowCountInput(sctx, colUniqueID, colRanges)
		defer func() {
			debugtrace.RecordAnyValuesWithNames(sctx, "Name", name, "Result", result)
			debugtrace.LeaveContextCommon(sctx)
		}()
	}
	sc := sctx.GetSessionVars().StmtCtx
	c := coll.GetCol(colUniqueID)
	colInfoID := colUniqueID
	if len(coll.UniqueID2colInfoID) > 0 {
		colInfoID = coll.UniqueID2colInfoID[colUniqueID]
	}
	recordUsedItemStatsStatus(sctx, c, coll.PhysicalID, colInfoID)
	if c != nil && c.Info != nil {
		name = c.Info.Name.O
	}
	if statistics.ColumnStatsIsInvalid(c, sctx, coll, colUniqueID) {
		var pseudoResult float64
		pseudoResult, err = getPseudoRowCountByColumnRanges(sc.TypeCtx(), float64(coll.RealtimeCount), colRanges, 0)
		if err != nil {
			return statistics.DefaultRowEst(0), err
		}
		result = statistics.DefaultRowEst(pseudoResult)
		if sc.EnableOptimizerCETrace && c != nil {
			ceTraceRange(sctx, coll.PhysicalID, []string{c.Info.Name.O}, colRanges, "Column Stats-Pseudo", uint64(result.Est))
		}
		return result, nil
	}
	if sctx.GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.RecordAnyValuesWithNames(sctx,
			"Histogram NotNull Count", c.Histogram.NotNullCount(),
			"TopN total count", c.TopN.TotalCount(),
			"Increase Factor", c.GetIncreaseFactor(coll.RealtimeCount),
		)
	}
	result, err = GetColumnRowCount(sctx, c, colRanges, coll.RealtimeCount, coll.ModifyCount, false)
	if err != nil {
		return statistics.DefaultRowEst(0), errors.Trace(err)
	}
	if sc.EnableOptimizerCETrace {
		ceTraceRange(sctx, coll.PhysicalID, []string{c.Info.Name.O}, colRanges, "Column Stats", uint64(result.Est))
	}
	return result, nil
}

// GetRowCountByIntColumnRanges estimates the row count by a slice of IntColumnRange.
func GetRowCountByIntColumnRanges(sctx planctx.PlanContext, coll *statistics.HistColl, colUniqueID int64, intRanges []*ranger.Range) (result statistics.RowEstimate, err error) {
	var name string
	if sctx.GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(sctx)
		debugTraceGetRowCountInput(sctx, colUniqueID, intRanges)
		defer func() {
			debugtrace.RecordAnyValuesWithNames(sctx, "Name", name, "Result", result)
			debugtrace.LeaveContextCommon(sctx)
		}()
	}
	sc := sctx.GetSessionVars().StmtCtx
	c := coll.GetCol(colUniqueID)
	colInfoID := colUniqueID
	if len(coll.UniqueID2colInfoID) > 0 {
		colInfoID = coll.UniqueID2colInfoID[colUniqueID]
	}
	recordUsedItemStatsStatus(sctx, c, coll.PhysicalID, colInfoID)
	if c != nil && c.Info != nil {
		name = c.Info.Name.O
	}
	if statistics.ColumnStatsIsInvalid(c, sctx, coll, colUniqueID) {
		if len(intRanges) == 0 {
			return statistics.DefaultRowEst(0), nil
		}
		var pseudoResult float64
		if intRanges[0].LowVal[0].Kind() == types.KindInt64 {
			pseudoResult = getPseudoRowCountBySignedIntRanges(intRanges, float64(coll.RealtimeCount))
		} else {
			pseudoResult = getPseudoRowCountByUnsignedIntRanges(intRanges, float64(coll.RealtimeCount))
		}
		result = statistics.DefaultRowEst(pseudoResult)
		if sc.EnableOptimizerCETrace && c != nil {
			ceTraceRange(sctx, coll.PhysicalID, []string{c.Info.Name.O}, intRanges, "Column Stats-Pseudo", uint64(result.Est))
		}
		return result, nil
	}
	if sctx.GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.RecordAnyValuesWithNames(sctx,
			"Histogram NotNull Count", c.Histogram.NotNullCount(),
			"TopN total count", c.TopN.TotalCount(),
			"Increase Factor", c.GetIncreaseFactor(coll.RealtimeCount),
		)
	}
	result, err = GetColumnRowCount(sctx, c, intRanges, coll.RealtimeCount, coll.ModifyCount, true)
	if err != nil {
		return statistics.DefaultRowEst(0), errors.Trace(err)
	}
	if sc.EnableOptimizerCETrace {
		ceTraceRange(sctx, coll.PhysicalID, []string{c.Info.Name.O}, intRanges, "Column Stats", uint64(result.Est))
	}
	return result, nil
}

// equalRowCountOnColumn estimates the row count by a slice of Range and a Datum.
func equalRowCountOnColumn(sctx planctx.PlanContext, c *statistics.Column, val types.Datum, encodedVal []byte, realtimeRowCount, modifyCount int64) (result statistics.RowEstimate, err error) {
	if sctx.GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(sctx)
		debugtrace.RecordAnyValuesWithNames(sctx, "Value", val.String(), "Encoded", encodedVal)
		defer func() {
			debugtrace.RecordAnyValuesWithNames(sctx, "Result", result, "Error", err)
			debugtrace.LeaveContextCommon(sctx)
		}()
	}
	if val.IsNull() {
		return statistics.DefaultRowEst(float64(c.NullCount)), nil
	}
	if c.StatsVer < statistics.Version2 {
		// All the values are null.
		if c.Histogram.Bounds.NumRows() == 0 {
			return statistics.DefaultRowEst(0.0), nil
		}
		if c.Histogram.NDV > 0 && c.OutOfRange(val) {
			outOfRangeCnt := outOfRangeEQSelectivity(sctx, c.Histogram.NDV, realtimeRowCount, int64(c.TotalRowCount())) * c.TotalRowCount()
			return statistics.DefaultRowEst(outOfRangeCnt), nil
		}
		if c.CMSketch != nil {
			count, err := statistics.QueryValue(sctx, c.CMSketch, c.TopN, val)
			return statistics.DefaultRowEst(float64(count)), errors.Trace(err)
		}
		histRowCount, _ := c.Histogram.EqualRowCount(sctx, val, false)
		return statistics.DefaultRowEst(histRowCount), nil
	}

	// Stats version == 2
	// All the values are null.
	if c.Histogram.Bounds.NumRows() == 0 && c.TopN.Num() == 0 {
		return statistics.DefaultRowEst(0), nil
	}
	// 1. try to find this value in TopN
	if c.TopN != nil {
		rowcount, ok := c.TopN.QueryTopN(sctx, encodedVal)
		if ok {
			return statistics.DefaultRowEst(float64(rowcount)), nil
		}
	}
	// 2. try to find this value in bucket.Repeat(the last value in every bucket)
	histCnt, matched := c.Histogram.EqualRowCount(sctx, val, true)
	// Calculate histNDV here as it's needed for both the underrepresented check and later calculations
	histNDV := float64(c.Histogram.NDV - int64(c.TopN.Num()))
	// also check if this last bucket end value is underrepresented
	if matched && !IsLastBucketEndValueUnderrepresented(sctx,
		&c.Histogram, val, histCnt, histNDV, realtimeRowCount, modifyCount) {
		return statistics.DefaultRowEst(histCnt), nil
	}
	// 3. use uniform distribution assumption for the rest, and address special cases for out of range
	// or all values assumed to be contained within TopN.
	rowEstimate := estimateRowCountWithUniformDistribution(sctx, c, realtimeRowCount, modifyCount)
	return rowEstimate, nil
}

// GetColumnRowCount estimates the row count by a slice of Range.
func GetColumnRowCount(sctx planctx.PlanContext, c *statistics.Column, ranges []*ranger.Range, realtimeRowCount, modifyCount int64, pkIsHandle bool) (statistics.RowEstimate, error) {
	sc := sctx.GetSessionVars().StmtCtx
	debugTrace := sc.EnableOptimizerDebugTrace
	if debugTrace {
		debugtrace.EnterContextCommon(sctx)
		defer debugtrace.LeaveContextCommon(sctx)
	}
	var totalCount statistics.RowEstimate
	for _, rg := range ranges {
		highVal := *rg.HighVal[0].Clone()
		lowVal := *rg.LowVal[0].Clone()
		if highVal.Kind() == types.KindString {
			highVal.SetBytes(collate.GetCollator(highVal.Collation()).Key(highVal.GetString()))
		}
		if lowVal.Kind() == types.KindString {
			lowVal.SetBytes(collate.GetCollator(lowVal.Collation()).Key(lowVal.GetString()))
		}
		cmp, err := lowVal.Compare(sc.TypeCtx(), &highVal, collate.GetBinaryCollator())
		if err != nil {
			return statistics.DefaultRowEst(0), errors.Trace(err)
		}
		lowEncoded, err := codec.EncodeKey(sc.TimeZone(), nil, lowVal)
		err = sc.HandleError(err)
		if err != nil {
			return statistics.DefaultRowEst(0), err
		}
		highEncoded, err := codec.EncodeKey(sc.TimeZone(), nil, highVal)
		err = sc.HandleError(err)
		if err != nil {
			return statistics.DefaultRowEst(0), err
		}
		if debugTrace {
			debugTraceStartEstimateRange(sctx, rg, lowEncoded, highEncoded, totalCount.Est)
		}
		if cmp == 0 {
			// case 1: it's a point
			if !rg.LowExclude && !rg.HighExclude {
				// In this case, the row count is at most 1.
				if pkIsHandle {
					totalCount.AddAll(1)
					if debugTrace {
						debugTraceEndEstimateRange(sctx, 1, debugTraceUniquePoint)
					}
					continue
				}
				var cnt statistics.RowEstimate
				cnt, err = equalRowCountOnColumn(sctx, c, lowVal, lowEncoded, realtimeRowCount, modifyCount)
				if err != nil {
					return statistics.DefaultRowEst(0), errors.Trace(err)
				}
				// If the current table row count has changed, we should scale the row count accordingly.
				cnt.MultiplyAll(c.GetIncreaseFactor(realtimeRowCount))
				totalCount.Add(cnt)
				if debugTrace {
					debugTraceEndEstimateRange(sctx, cnt.Est, debugTracePoint)
				}
			}
			continue
		}
		// In stats ver 1, we use CM Sketch to estimate row count for point condition, which is more accurate.
		// So for the small range, we convert it to points.
		if c.StatsVer < 2 {
			rangeVals := statistics.EnumRangeValues(lowVal, highVal, rg.LowExclude, rg.HighExclude)

			// case 2: it's a small range && using ver1 stats
			if rangeVals != nil {
				for _, val := range rangeVals {
					cnt, err := equalRowCountOnColumn(sctx, c, val, lowEncoded, realtimeRowCount, modifyCount)
					if err != nil {
						return statistics.DefaultRowEst(0), err
					}
					// If the current table row count has changed, we should scale the row count accordingly.
					cnt.MultiplyAll(c.GetIncreaseFactor(realtimeRowCount))
					if debugTrace {
						debugTraceEndEstimateRange(sctx, cnt.Est, debugTraceVer1SmallRange)
					}
					totalCount.Add(cnt)
				}

				continue
			}
		}

		// case 3: it's an interval
		cnt := betweenRowCountOnColumn(sctx, c, lowVal, highVal, lowEncoded, highEncoded)
		// `betweenRowCount` returns count for [l, h) range, we adjust cnt for boundaries here.
		// Note that, `cnt` does not include null values, we need specially handle cases
		//   where null is the lower bound.
		// And because we use (2, MaxValue] to represent expressions like a > 2 and use [MinNotNull, 3) to represent
		//   expressions like b < 3, we need to exclude the special values.
		if rg.LowExclude && !lowVal.IsNull() && lowVal.Kind() != types.KindMaxValue && lowVal.Kind() != types.KindMinNotNull {
			lowCnt, err := equalRowCountOnColumn(sctx, c, lowVal, lowEncoded, realtimeRowCount, modifyCount)
			if err != nil {
				return statistics.DefaultRowEst(0), errors.Trace(err)
			}
			cnt.Subtract(lowCnt)
			cnt.Clamp(0, c.NotNullCount())
		}
		if !rg.LowExclude && lowVal.IsNull() {
			cnt.AddAll(float64(c.NullCount))
		}
		if !rg.HighExclude && highVal.Kind() != types.KindMaxValue && highVal.Kind() != types.KindMinNotNull {
			highCnt, err := equalRowCountOnColumn(sctx, c, highVal, highEncoded, realtimeRowCount, modifyCount)
			if err != nil {
				return statistics.DefaultRowEst(0), errors.Trace(err)
			}
			cnt.Add(highCnt)
		}
		// Clamp all 3 fields of RowEstimate to [0, realtimeRowCount]
		cnt.Clamp(0, float64(realtimeRowCount))

		// If the current table row count has changed, we should scale the row count accordingly.
		increaseFactor := c.GetIncreaseFactor(realtimeRowCount)
		cnt.MultiplyAll(increaseFactor)

		// handling the out-of-range part
		if (c.OutOfRange(lowVal) && !lowVal.IsNull()) || c.OutOfRange(highVal) {
			histNDV := c.NDV
			// Exclude the TopN
			if c.StatsVer == statistics.Version2 {
				histNDV -= int64(c.TopN.Num())
			}
<<<<<<< HEAD
			cnt += c.Histogram.OutOfRangeRowCount(sctx, &lowVal, &highVal, realtimeRowCount, modifyCount, histNDV)
=======
			var count statistics.RowEstimate
			count.Add(c.Histogram.OutOfRangeRowCount(sctx, &lowVal, &highVal, realtimeRowCount, modifyCount, histNDV))
			cnt.Add(count)
>>>>>>> 72a540b8042 (Planner: Add min/max for out of range (#63077))
		}

		if debugTrace {
			debugTraceEndEstimateRange(sctx, cnt.Est, debugTraceRange)
		}
		totalCount.Add(cnt)
	}
	allowZeroEst := fixcontrol.GetBoolWithDefault(
		sctx.GetSessionVars().GetOptimizerFixControlMap(),
		fixcontrol.Fix47400,
		false,
	)
	minCount := float64(1)
	if allowZeroEst {
		minCount = 0
	}
	totalCount.Clamp(minCount, float64(realtimeRowCount))
	return totalCount, nil
}

// betweenRowCountOnColumn estimates the row count for interval [l, r).
<<<<<<< HEAD
func betweenRowCountOnColumn(sctx planctx.PlanContext, c *statistics.Column, l, r types.Datum, lowEncoded, highEncoded []byte) float64 {
=======
func betweenRowCountOnColumn(sctx planctx.PlanContext, c *statistics.Column, l, r types.Datum, lowEncoded, highEncoded []byte) statistics.RowEstimate {
	// TODO: Track min/max range for column estimates, currently only used for indexes.
>>>>>>> 72a540b8042 (Planner: Add min/max for out of range (#63077))
	histBetweenCnt := c.Histogram.BetweenRowCount(sctx, l, r)
	if c.StatsVer <= statistics.Version1 {
		return histBetweenCnt
	}
	topNCnt := float64(c.TopN.BetweenCount(sctx, lowEncoded, highEncoded))
	// Only add TopN count to the main estimate, keep min/max estimates from histogram
	histBetweenCnt.Est += topNCnt
	return histBetweenCnt
}

// functions below are mainly for testing.

// ColumnGreaterRowCount estimates the row count where the column greater than value.
func ColumnGreaterRowCount(sctx planctx.PlanContext, t *statistics.Table, value types.Datum, colID int64) float64 {
	c := t.GetCol(colID)
	if statistics.ColumnStatsIsInvalid(c, sctx, &t.HistColl, colID) {
		return float64(t.RealtimeCount) / pseudoLessRate
	}
	return c.GreaterRowCount(value) * c.GetIncreaseFactor(t.RealtimeCount)
}

// columnLessRowCount estimates the row count where the column less than value. Note that null values are not counted.
func columnLessRowCount(sctx planctx.PlanContext, t *statistics.Table, value types.Datum, colID int64) float64 {
	c := t.GetCol(colID)
	if statistics.ColumnStatsIsInvalid(c, sctx, &t.HistColl, colID) {
		return float64(t.RealtimeCount) / pseudoLessRate
	}
	return c.LessRowCount(sctx, value) * c.GetIncreaseFactor(t.RealtimeCount)
}

// columnBetweenRowCount estimates the row count where column greater or equal to a and less than b.
func columnBetweenRowCount(sctx planctx.PlanContext, t *statistics.Table, a, b types.Datum, colID int64) (float64, error) {
	sc := sctx.GetSessionVars().StmtCtx
	c := t.GetCol(colID)
	if statistics.ColumnStatsIsInvalid(c, sctx, &t.HistColl, colID) {
		return float64(t.RealtimeCount) / pseudoBetweenRate, nil
	}
	aEncoded, err := codec.EncodeKey(sc.TimeZone(), nil, a)
	err = sc.HandleError(err)
	if err != nil {
		return 0, err
	}
	bEncoded, err := codec.EncodeKey(sc.TimeZone(), nil, b)
	err = sc.HandleError(err)
	if err != nil {
		return 0, err
	}
	count := betweenRowCountOnColumn(sctx, c, a, b, aEncoded, bEncoded)
	if a.IsNull() {
		count.AddAll(float64(c.NullCount))
	}
	count.MultiplyAll(c.GetIncreaseFactor(t.RealtimeCount))
	return count.Est, nil
}

// ColumnEqualRowCount estimates the row count where the column equals to value.
func ColumnEqualRowCount(sctx planctx.PlanContext, t *statistics.Table, value types.Datum, colID int64) (float64, error) {
	c := t.GetCol(colID)
	if statistics.ColumnStatsIsInvalid(c, sctx, &t.HistColl, colID) {
		return float64(t.RealtimeCount) / pseudoEqualRate, nil
	}
	encodedVal, err := codec.EncodeKey(sctx.GetSessionVars().StmtCtx.TimeZone(), nil, value)
	err = sctx.GetSessionVars().StmtCtx.HandleError(err)
	if err != nil {
		return 0, err
	}
	result, err := equalRowCountOnColumn(sctx, c, value, encodedVal, t.RealtimeCount, t.ModifyCount)
	if err != nil {
		return 0, errors.Trace(err)
	}
	result.MultiplyAll(c.GetIncreaseFactor(t.RealtimeCount))
	return result.Est, nil
}
<<<<<<< HEAD
=======

// getPseudoRowCountWithPartialStats calculates the row count if there are no statistics on the index, but there are column stats available.
func getPseudoRowCountWithPartialStats(sctx planctx.PlanContext, coll *statistics.HistColl, indexRanges []*ranger.Range,
	tableRowCount float64, idxCols []*expression.Column) (totalCount float64, maxCount float64, err error) {
	if tableRowCount == 0 {
		return 0, 0, nil
	}
	// If it is a single column index, directly use column estimation instead.
	if len(idxCols) == 1 {
		var countEst statistics.RowEstimate
		countEst, err = GetRowCountByColumnRanges(sctx, coll, idxCols[0].UniqueID, indexRanges)
		if err != nil {
			return 0, 0, err
		}
		return countEst.Est, 0, nil
	}
	tmpRan := []*ranger.Range{
		{
			LowVal:    make([]types.Datum, 1),
			HighVal:   make([]types.Datum, 1),
			Collators: make([]collate.Collator, 1),
		},
	}
	var (
		count float64
		colID int64
	)
	totalCount = float64(0)
	maxCount = float64(0)
	for _, indexRange := range indexRanges {
		selectivity := float64(1.0)
		corrSelectivity := float64(1.0)
		for i := range indexRange.LowVal {
			tmpRan[0].LowVal[0] = indexRange.LowVal[i]
			tmpRan[0].HighVal[0] = indexRange.HighVal[i]
			tmpRan[0].Collators[0] = indexRange.Collators[0]
			if i == len(indexRange.LowVal)-1 {
				tmpRan[0].LowExclude = indexRange.LowExclude
				tmpRan[0].HighExclude = indexRange.HighExclude
			}
			colID = idxCols[i].UniqueID
			// GetRowCountByColumnRanges handles invalid stats internally by using pseudo estimation
			var countEst statistics.RowEstimate
			countEst, err = GetRowCountByColumnRanges(sctx, coll, colID, tmpRan)
			if err != nil {
				return 0, 0, errors.Trace(err)
			}
			count = countEst.Est
			tempSelectivity := count / tableRowCount
			selectivity *= tempSelectivity
			corrSelectivity = min(corrSelectivity, tempSelectivity)
		}
		totalCount += selectivity * tableRowCount
		maxCount += corrSelectivity * tableRowCount
	}
	totalCount = mathutil.Clamp(totalCount, 1, tableRowCount)
	return totalCount, maxCount, nil
}
>>>>>>> 72a540b8042 (Planner: Add min/max for out of range (#63077))
