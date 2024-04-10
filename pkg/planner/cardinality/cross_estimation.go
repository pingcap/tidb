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
	"github.com/pingcap/tidb/pkg/planner/context"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/pingcap/tidb/pkg/util/set"
)

// SelectionFactor is the factor which is used to estimate the row count of selection.
const SelectionFactor = 0.8

// AdjustRowCountForTableScanByLimit will adjust the row count for table scan by limit.
// For a query like `select pk from t using index(primary) where pk > 10 limit 1`, the row count of the table scan
// should be adjusted by the limit number 1, because only one row is returned.
func AdjustRowCountForTableScanByLimit(sctx context.PlanContext,
	dsStatsInfo, dsTableStats *property.StatsInfo, dsStatisticTable *statistics.Table,
	path *util.AccessPath, expectedCnt float64, desc bool) float64 {
	rowCount := path.CountAfterAccess
	if expectedCnt < dsStatsInfo.RowCount {
		selectivity := dsStatsInfo.RowCount / path.CountAfterAccess
		uniformEst := min(path.CountAfterAccess, expectedCnt/selectivity)

		corrEst, ok, corr := crossEstimateTableRowCount(sctx,
			dsStatsInfo, dsTableStats, dsStatisticTable, path, expectedCnt, desc)
		if ok {
			// TODO: actually, before using this count as the estimated row count of table scan, we need additionally
			// check if count < row_count(first_region | last_region), and use the larger one since we build one copTask
			// for one region now, so even if it is `limit 1`, we have to scan at least one region in table scan.
			// Currently, we can use `tikvrpc.CmdDebugGetRegionProperties` interface as `getSampRegionsRowCount()` does
			// to get the row count in a region, but that result contains MVCC old version rows, so it is not that accurate.
			// Considering that when this scenario happens, the execution time is close between IndexScan and TableScan,
			// we do not add this check temporarily.

			// to reduce risks of correlation adjustment, use the maximum between uniformEst and corrEst
			rowCount = max(uniformEst, corrEst)
		} else if abs := math.Abs(corr); abs < 1 {
			correlationFactor := math.Pow(1-abs, float64(sctx.GetSessionVars().CorrelationExpFactor))
			rowCount = min(path.CountAfterAccess, uniformEst/correlationFactor)
		}
	}
	return rowCount
}

// crossEstimateTableRowCount estimates row count of table scan using histogram of another column which is in TableFilters
// and has high order correlation with handle column. For example, if the query is like:
// `select * from tbl where a = 1 order by pk limit 1`
// if order of column `a` is strictly correlated with column `pk`, the row count of table scan should be:
// `1 + row_count(a < 1 or a is null)`
func crossEstimateTableRowCount(sctx context.PlanContext,
	dsStatsInfo, dsTableStats *property.StatsInfo, dsStatisticTable *statistics.Table,
	path *util.AccessPath, expectedCnt float64, desc bool) (float64, bool, float64) {
	if dsStatisticTable.Pseudo || len(path.TableFilters) == 0 || !sctx.GetSessionVars().EnableCorrelationAdjustment {
		return 0, false, 0
	}
	col, corr := getMostCorrCol4Handle(path.TableFilters, dsStatisticTable, sctx.GetSessionVars().CorrelationThreshold)
	return crossEstimateRowCount(sctx, dsStatsInfo, dsTableStats, path, path.TableFilters, col, corr, expectedCnt, desc)
}

// AdjustRowCountForIndexScanByLimit will adjust the row count for table scan by limit.
// For a query like `select k from t using index(k) where k > 10 limit 1`, the row count of the index scan
// should be adjusted by the limit number 1, because only one row is returned.
func AdjustRowCountForIndexScanByLimit(sctx context.PlanContext,
	dsStatsInfo, dsTableStats *property.StatsInfo, dsStatisticTable *statistics.Table,
	path *util.AccessPath, expectedCnt float64, desc bool) float64 {
	rowCount := path.CountAfterAccess
	count, ok, corr := crossEstimateIndexRowCount(sctx,
		dsStatsInfo, dsTableStats, dsStatisticTable, path, expectedCnt, desc)
	if ok {
		rowCount = count
	} else if abs := math.Abs(corr); abs < 1 {
		// If OptOrderingIdxSelRatio is enabled - estimate the difference between index and table filtering, as this represents
		// the possible scan range when LIMIT rows will be found. orderRatio is the estimated percentage of that range when the first
		// row is expected to be found. Index filtering applies orderRatio twice. Once found - rows are estimated to be clustered (expectedCnt).
		// This formula is to bias away from non-filtering (or poorly filtering) indexes that provide order due, where filtering exists
		// outside of that index. Such plans have high risk since we cannot estimate when rows will be found.
		orderRatio := sctx.GetSessionVars().OptOrderingIdxSelRatio
		if dsStatsInfo.RowCount < path.CountAfterAccess && orderRatio >= 0 {
			rowsToMeetFirst := (((path.CountAfterAccess - path.CountAfterIndex) * orderRatio) + (path.CountAfterIndex - dsStatsInfo.RowCount)) * orderRatio
			rowCount = rowsToMeetFirst + expectedCnt
		} else {
			// Assume rows are linearly distributed throughout the range - for example: selectivity 0.1 assumes that a
			// qualified row is found every 10th row.
			correlationFactor := math.Pow(1-abs, float64(sctx.GetSessionVars().CorrelationExpFactor))
			selectivity := dsStatsInfo.RowCount / rowCount
			rowCount = min(expectedCnt/selectivity/correlationFactor, rowCount)
		}
	}
	return rowCount
}

// crossEstimateIndexRowCount estimates row count of index scan using histogram of another column which is in TableFilters/IndexFilters
// and has high order correlation with the first index column. For example, if the query is like:
// `select * from tbl where a = 1 order by b limit 1`
// if order of column `a` is strictly correlated with column `b`, the row count of IndexScan(b) should be:
// `1 + row_count(a < 1 or a is null)`
func crossEstimateIndexRowCount(sctx context.PlanContext,
	dsStatsInfo, dsTableStats *property.StatsInfo, dsStatisticTable *statistics.Table,
	path *util.AccessPath, expectedCnt float64, desc bool) (float64, bool, float64) {
	filtersLen := len(path.TableFilters) + len(path.IndexFilters)
	sessVars := sctx.GetSessionVars()
	if dsStatisticTable.Pseudo || filtersLen == 0 || !sessVars.EnableExtendedStats || !sctx.GetSessionVars().EnableCorrelationAdjustment {
		return 0, false, 0
	}
	col, corr := getMostCorrCol4Index(path, dsStatisticTable, sessVars.CorrelationThreshold)
	filters := make([]expression.Expression, 0, filtersLen)
	filters = append(filters, path.TableFilters...)
	filters = append(filters, path.IndexFilters...)
	return crossEstimateRowCount(sctx, dsStatsInfo, dsTableStats, path, filters, col, corr, expectedCnt, desc)
}

// crossEstimateRowCount is the common logic of crossEstimateTableRowCount and crossEstimateIndexRowCount.
func crossEstimateRowCount(sctx context.PlanContext,
	dsStatsInfo, dsTableStats *property.StatsInfo,
	path *util.AccessPath, conds []expression.Expression, col *expression.Column,
	corr, expectedCnt float64, desc bool) (float64, bool, float64) {
	// If the scan is not full range scan, we cannot use histogram of other columns for estimation, because
	// the histogram reflects value distribution in the whole table level.
	if col == nil || len(path.AccessConds) > 0 {
		return 0, false, corr
	}
	colUniqueID := col.UniqueID
	if corr < 0 {
		desc = !desc
	}
	accessConds, remained := ranger.DetachCondsForColumn(sctx.GetRangerCtx(), conds, col)
	if len(accessConds) == 0 {
		return 0, false, corr
	}
	ranges, accessConds, _, err := ranger.BuildColumnRange(accessConds, sctx.GetRangerCtx(), col.RetType, types.UnspecifiedLength, sctx.GetSessionVars().RangeMaxSize)
	if len(ranges) == 0 || len(accessConds) == 0 || err != nil {
		return 0, err == nil, corr
	}
	idxID := int64(-1)
	idxIDs, idxExists := dsStatsInfo.HistColl.ColUniqueID2IdxIDs[colUniqueID]
	if idxExists && len(idxIDs) > 0 {
		idxID = idxIDs[0]
	}
	rangeCounts, ok := getColumnRangeCounts(sctx, colUniqueID, ranges, dsTableStats.HistColl, idxID)
	if !ok {
		return 0, false, corr
	}
	convertedRanges, count, isFull := convertRangeFromExpectedCnt(ranges, rangeCounts, expectedCnt, desc)
	if isFull {
		return path.CountAfterAccess, true, 0
	}
	var rangeCount float64
	if idxExists {
		rangeCount, err = GetRowCountByIndexRanges(sctx, dsTableStats.HistColl, idxID, convertedRanges)
	} else {
		rangeCount, err = GetRowCountByColumnRanges(sctx, dsTableStats.HistColl, colUniqueID, convertedRanges)
	}
	if err != nil {
		return 0, false, corr
	}
	scanCount := rangeCount + expectedCnt - count
	if len(remained) > 0 {
		scanCount = scanCount / SelectionFactor
	}
	scanCount = min(scanCount, path.CountAfterAccess)
	return scanCount, true, 0
}

// getColumnRangeCounts estimates row count for each range respectively.
func getColumnRangeCounts(sctx context.PlanContext, colID int64, ranges []*ranger.Range, histColl *statistics.HistColl, idxID int64) ([]float64, bool) {
	var err error
	var count float64
	rangeCounts := make([]float64, len(ranges))
	for i, ran := range ranges {
		if idxID >= 0 {
			idxHist := histColl.Indices[idxID]
			if statistics.IndexStatsIsInvalid(sctx, idxHist, histColl, idxID) {
				return nil, false
			}
			count, err = GetRowCountByIndexRanges(sctx, histColl, idxID, []*ranger.Range{ran})
		} else {
			colHist := histColl.Columns[colID]
			if statistics.ColumnStatsIsInvalid(colHist, sctx, histColl, colID) {
				return nil, false
			}
			count, err = GetRowCountByColumnRanges(sctx, histColl, colID, []*ranger.Range{ran})
		}
		if err != nil {
			return nil, false
		}
		rangeCounts[i] = count
	}
	return rangeCounts, true
}

// convertRangeFromExpectedCnt builds new ranges used to estimate row count we need to scan in table scan before finding specified
// number of tuples which fall into input ranges.
func convertRangeFromExpectedCnt(ranges []*ranger.Range, rangeCounts []float64, expectedCnt float64, desc bool) ([]*ranger.Range, float64, bool) {
	var i int
	var count float64
	var convertedRanges []*ranger.Range
	if desc {
		for i = len(ranges) - 1; i >= 0; i-- {
			if count+rangeCounts[i] >= expectedCnt {
				break
			}
			count += rangeCounts[i]
		}
		if i < 0 {
			return nil, 0, true
		}
		convertedRanges = []*ranger.Range{{LowVal: ranges[i].HighVal, HighVal: []types.Datum{types.MaxValueDatum()}, LowExclude: !ranges[i].HighExclude, Collators: ranges[i].Collators}}
	} else {
		for i = 0; i < len(ranges); i++ {
			if count+rangeCounts[i] >= expectedCnt {
				break
			}
			count += rangeCounts[i]
		}
		if i == len(ranges) {
			return nil, 0, true
		}
		convertedRanges = []*ranger.Range{{LowVal: []types.Datum{{}}, HighVal: ranges[i].LowVal, HighExclude: !ranges[i].LowExclude, Collators: ranges[i].Collators}}
	}
	return convertedRanges, count, false
}

// getMostCorrCol4Index checks if column in the condition is correlated enough with the first index column. If the condition
// contains multiple columns, return nil and get the max correlation, which would be used in the heuristic estimation.
func getMostCorrCol4Index(path *util.AccessPath, histColl *statistics.Table, threshold float64) (*expression.Column, float64) {
	if histColl.ExtendedStats == nil || len(histColl.ExtendedStats.Stats) == 0 {
		return nil, 0
	}
	var cols []*expression.Column
	cols = expression.ExtractColumnsFromExpressions(cols, path.TableFilters, nil)
	cols = expression.ExtractColumnsFromExpressions(cols, path.IndexFilters, nil)
	if len(cols) == 0 {
		return nil, 0
	}
	colSet := set.NewInt64Set()
	var corr float64
	var corrCol *expression.Column
	for _, col := range cols {
		if colSet.Exist(col.UniqueID) {
			continue
		}
		colSet.Insert(col.UniqueID)
		curCorr := float64(0)
		for _, item := range histColl.ExtendedStats.Stats {
			if (col.ID == item.ColIDs[0] && path.FullIdxCols[0].ID == item.ColIDs[1]) ||
				(col.ID == item.ColIDs[1] && path.FullIdxCols[0].ID == item.ColIDs[0]) {
				curCorr = item.ScalarVals
				break
			}
		}
		if corrCol == nil || math.Abs(corr) < math.Abs(curCorr) {
			corrCol = col
			corr = curCorr
		}
	}
	if len(colSet) == 1 && math.Abs(corr) >= threshold {
		return corrCol, corr
	}
	return nil, corr
}

// getMostCorrCol4Handle checks if column in the condition is correlated enough with handle. If the condition
// contains multiple columns, return nil and get the max correlation, which would be used in the heuristic estimation.
func getMostCorrCol4Handle(exprs []expression.Expression, histColl *statistics.Table, threshold float64) (*expression.Column, float64) {
	var cols []*expression.Column
	cols = expression.ExtractColumnsFromExpressions(cols, exprs, nil)
	if len(cols) == 0 {
		return nil, 0
	}
	colSet := set.NewInt64Set()
	var corr float64
	var corrCol *expression.Column
	for _, col := range cols {
		if colSet.Exist(col.UniqueID) {
			continue
		}
		colSet.Insert(col.UniqueID)
		hist, ok := histColl.Columns[col.ID]
		if !ok {
			continue
		}
		curCorr := hist.Correlation
		if corrCol == nil || math.Abs(corr) < math.Abs(curCorr) {
			corrCol = col
			corr = curCorr
		}
	}
	if len(colSet) == 1 && math.Abs(corr) >= threshold {
		return corrCol, corr
	}
	return nil, corr
}
