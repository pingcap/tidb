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

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/set"
)

// SelectionFactor is the factor which is used to estimate the row count of selection.
const SelectionFactor = 0.8

// CrossEstimateTableRowCount estimates row count of table scan using histogram of another column which is in TableFilters
// and has high order correlation with handle column. For example, if the query is like:
// `select * from tbl where a = 1 order by pk limit 1`
// if order of column `a` is strictly correlated with column `pk`, the row count of table scan should be:
// `1 + row_count(a < 1 or a is null)`
func CrossEstimateTableRowCount(sctx sessionctx.Context,
	dsStatsInfo, dsTableStats *property.StatsInfo, dsStatisticTable *statistics.Table,
	path *util.AccessPath, expectedCnt float64, desc bool) (float64, bool, float64) {
	if dsStatisticTable.Pseudo || len(path.TableFilters) == 0 || !sctx.GetSessionVars().EnableCorrelationAdjustment {
		return 0, false, 0
	}
	col, corr := getMostCorrCol4Handle(path.TableFilters, dsStatisticTable, sctx.GetSessionVars().CorrelationThreshold)
	return crossEstimateRowCount(sctx, dsStatsInfo, dsTableStats, path, path.TableFilters, col, corr, expectedCnt, desc)
}

// CrossEstimateIndexRowCount estimates row count of index scan using histogram of another column which is in TableFilters/IndexFilters
// and has high order correlation with the first index column. For example, if the query is like:
// `select * from tbl where a = 1 order by b limit 1`
// if order of column `a` is strictly correlated with column `b`, the row count of IndexScan(b) should be:
// `1 + row_count(a < 1 or a is null)`
func CrossEstimateIndexRowCount(sctx sessionctx.Context,
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
func crossEstimateRowCount(sctx sessionctx.Context,
	dsStatsInfo, dsTableStats *property.StatsInfo,
	path *util.AccessPath, conds []expression.Expression, col *expression.Column,
	corr, expectedCnt float64, desc bool) (float64, bool, float64) {
	// If the scan is not full range scan, we cannot use histogram of other columns for estimation, because
	// the histogram reflects value distribution in the whole table level.
	if col == nil || len(path.AccessConds) > 0 {
		return 0, false, corr
	}
	colID := col.UniqueID
	if corr < 0 {
		desc = !desc
	}
	accessConds, remained := ranger.DetachCondsForColumn(sctx, conds, col)
	if len(accessConds) == 0 {
		return 0, false, corr
	}
	ranges, accessConds, _, err := ranger.BuildColumnRange(accessConds, sctx, col.RetType, types.UnspecifiedLength, sctx.GetSessionVars().RangeMaxSize)
	if len(ranges) == 0 || len(accessConds) == 0 || err != nil {
		return 0, err == nil, corr
	}
	idxID := int64(-1)
	idxIDs, idxExists := dsStatsInfo.HistColl.ColID2IdxIDs[colID]
	if idxExists && len(idxIDs) > 0 {
		idxID = idxIDs[0]
	}
	rangeCounts, ok := getColumnRangeCounts(sctx, colID, ranges, dsTableStats.HistColl, idxID)
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
		rangeCount, err = GetRowCountByColumnRanges(sctx, dsTableStats.HistColl, colID, convertedRanges)
	}
	if err != nil {
		return 0, false, corr
	}
	scanCount := rangeCount + expectedCnt - count
	if len(remained) > 0 {
		scanCount = scanCount / SelectionFactor
	}
	scanCount = math.Min(scanCount, path.CountAfterAccess)
	return scanCount, true, 0
}

// getColumnRangeCounts estimates row count for each range respectively.
func getColumnRangeCounts(sctx sessionctx.Context, colID int64, ranges []*ranger.Range, histColl *statistics.HistColl, idxID int64) ([]float64, bool) {
	var err error
	var count float64
	rangeCounts := make([]float64, len(ranges))
	for i, ran := range ranges {
		if idxID >= 0 {
			idxHist := histColl.Indices[idxID]
			if idxHist == nil || idxHist.IsInvalid(sctx, false) {
				return nil, false
			}
			count, err = GetRowCountByIndexRanges(sctx, histColl, idxID, []*ranger.Range{ran})
		} else {
			colHist, ok := histColl.Columns[colID]
			if !ok || colHist.IsInvalid(sctx, false) {
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
