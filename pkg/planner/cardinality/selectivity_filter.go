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
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	planutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/ranger"
)
// GetSelectivityByFilter try to estimate selectivity of expressions by evaluate the expressions using TopN, Histogram buckets boundaries and NULL.
// Currently, this method can only handle expressions involving a single column.
func GetSelectivityByFilter(sctx planctx.PlanContext, coll *statistics.HistColl, filters expression.Expression) (ok bool, selectivity float64, err error) {
	// 1. Make sure the expressions
	//   (1) are safe to be evaluated here,
	//   (2) involve only one column,
	//   (3) and this column is not a "new collation" string column so that we're able to restore values from the stats.
	if expression.IsMutableEffectsExpr(filters) {
		return false, 0, nil
	}
	if expression.ContainCorrelatedColumn(filters) {
		return false, 0, nil
	}
	cols := expression.ExtractColumnsMapFromExpressions(nil, filters)
	if len(cols) != 1 {
		return false, 0, nil
	}
	var col *expression.Column
	for _, c := range cols {
		col = c
		break
	}
	tp := col.RetType
	if types.IsString(tp.GetType()) && collate.NewCollationEnabled() && !collate.IsBinCollation(tp.GetCollate()) {
		return false, 0, nil
	}

	// 2. Get the available stats, make sure it's a ver2 stats and get the needed data structure from it.
	isIndex, i := findAvailableStatsForCol(sctx, coll, col.UniqueID)
	if i < 0 {
		return false, 0, nil
	}
	var statsVer, nullCnt int64
	var histTotalCnt, totalCnt float64
	var topnTotalCnt uint64
	var hist *statistics.Histogram
	var topn *statistics.TopN
	if isIndex {
		stats := coll.GetIdx(i)
		statsVer = stats.StatsVer
		hist = &stats.Histogram
		nullCnt = hist.NullCount
		topn = stats.TopN
	} else {
		stats := coll.GetCol(i)
		statsVer = stats.StatsVer
		hist = &stats.Histogram
		nullCnt = hist.NullCount
		topn = stats.TopN
	}
	// Only in stats ver2, we can assume that: TopN + Histogram + NULL == All data
	if statsVer != statistics.Version2 {
		return false, 0, nil
	}
	topnTotalCnt = topn.TotalCount()
	histTotalCnt = hist.NotNullCount()
	totalCnt = float64(topnTotalCnt) + histTotalCnt + float64(nullCnt)

	var topNSel, histSel, nullSel float64

	// Prepare for evaluation.

	// For execution, we use Column.Index instead of Column.UniqueID to locate a column.
	// We have only one column here, so we set it to 0.
	originalIndex := col.Index
	col.Index = 0
	defer func() {
		// Restore the original Index to avoid unexpected situation.
		col.Index = originalIndex
	}()
	topNLen := 0
	histBucketsLen := hist.Len()
	if topn != nil {
		topNLen = len(topn.TopN)
	}
	c := chunk.NewChunkWithCapacity([]*types.FieldType{tp}, max(1, topNLen))
	selected := make([]bool, 0, max(histBucketsLen, topNLen))
	vecEnabled := sctx.GetSessionVars().EnableVectorizedExpression

	// 3. Calculate the TopN part selectivity.
	// This stage is considered as the core functionality of this method, errors in this stage would make this entire method fail.
	var topNSelectedCnt uint64
	if topn != nil {
		for _, item := range topn.TopN {
			_, val, err := codec.DecodeOne(item.Encoded)
			if err != nil {
				return false, 0, err
			}
			c.AppendDatum(0, &val)
		}
		selected, err = expression.VectorizedFilter(sctx.GetExprCtx().GetEvalCtx(), vecEnabled, []expression.Expression{filters}, chunk.NewIterator4Chunk(c), selected)
		if err != nil {
			return false, 0, err
		}
		for i, isTrue := range selected {
			if isTrue {
				topNSelectedCnt += topn.TopN[i].Count
			}
		}
	}
	topNSel = float64(topNSelectedCnt) / totalCnt

	// 4. Calculate the Histogram part selectivity.
	// The buckets upper bounds and the Bucket.Repeat are used like the TopN above.
	// The buckets lower bounds are used as random samples and are regarded equally.
	if hist != nil && histTotalCnt > 0 {
		selected = selected[:0]
		selected, err = expression.VectorizedFilter(sctx.GetExprCtx().GetEvalCtx(), vecEnabled, []expression.Expression{filters}, chunk.NewIterator4Chunk(hist.Bounds), selected)
		if err != nil {
			return false, 0, err
		}
		var bucketRepeatTotalCnt, bucketRepeatSelectedCnt, lowerBoundMatchCnt int64
		for i := range hist.Buckets {
			bucketRepeatTotalCnt += hist.Buckets[i].Repeat
			if len(selected) < 2*i {
				// This should not happen, but we add this check for safety.
				break
			}
			if selected[2*i] {
				lowerBoundMatchCnt++
			}
			if selected[2*i+1] {
				bucketRepeatSelectedCnt += hist.Buckets[i].Repeat
			}
		}
		var lowerBoundsRatio, upperBoundsRatio, lowerBoundsSel, upperBoundsSel float64
		upperBoundsRatio = min(float64(bucketRepeatTotalCnt)/histTotalCnt, 1)
		lowerBoundsRatio = 1 - upperBoundsRatio
		if bucketRepeatTotalCnt > 0 {
			upperBoundsSel = float64(bucketRepeatSelectedCnt) / float64(bucketRepeatTotalCnt)
		}
		lowerBoundsSel = float64(lowerBoundMatchCnt) / float64(histBucketsLen)
		histSel = lowerBoundsSel*lowerBoundsRatio + upperBoundsSel*upperBoundsRatio
		histSel *= histTotalCnt / totalCnt
	}

	// 5. Calculate the NULL part selectivity.
	// Errors in this staged would be returned, but would not make this entire method fail.
	c.Reset()
	c.AppendNull(0)
	selected = selected[:0]
	selected, err = expression.VectorizedFilter(sctx.GetExprCtx().GetEvalCtx(), vecEnabled, []expression.Expression{filters}, chunk.NewIterator4Chunk(c), selected)
	if err != nil || len(selected) != 1 || !selected[0] {
		nullSel = 0
	} else {
		nullSel = float64(nullCnt) / totalCnt
	}

	// 6. Get the final result.
	res := topNSel + histSel + nullSel
	return true, res, err
}

func findAvailableStatsForCol(sctx planctx.PlanContext, coll *statistics.HistColl, uniqueID int64) (isIndex bool, idx int64) {
	// try to find available stats in column stats
	if colStats := coll.GetCol(uniqueID); !statistics.ColumnStatsIsInvalid(colStats, sctx, coll, uniqueID) && colStats.IsFullLoad() {
		return false, uniqueID
	}
	// try to find available stats in single column index stats (except for prefix index)
	for idxStatsIdx, cols := range coll.Idx2ColUniqueIDs {
		if len(cols) == 1 && cols[0] == uniqueID {
			idxStats := coll.GetIdx(idxStatsIdx)
			if !statistics.IndexStatsIsInvalid(sctx, idxStats, coll, idxStatsIdx) &&
				idxStats.Info.Columns[0].Length == types.UnspecifiedLength &&
				idxStats.IsFullLoad() {
				return true, idxStatsIdx
			}
		}
	}
	return false, -1
}

// getEqualCondSelectivity gets the selectivity of the equal conditions.
func getEqualCondSelectivity(sctx planctx.PlanContext, coll *statistics.HistColl, idx *statistics.Index, bytes []byte,
	usedColsLen int, idxPointRange *ranger.Range) (result float64, err error) {
	coverAll := len(idx.Info.Columns) == usedColsLen
	// In this case, the row count is at most 1.
	if idx.Info.Unique && coverAll {
		return 1.0 / idx.TotalRowCount(), nil
	}
	val := types.NewBytesDatum(bytes)
	if outOfRangeOnIndex(idx, val) {
		realtimeCnt, _ := coll.GetScaledRealtimeAndModifyCnt(idx)
		// When the value is out of range, we could not found this value in the CM Sketch,
		// so we use heuristic methods to estimate the selectivity.
		if idx.NDV > 0 && coverAll {
			return outOfRangeEQSelectivity(sctx, idx.NDV, realtimeCnt, int64(idx.TotalRowCount())), nil
		}
		// The equal condition only uses prefix columns of the index.
		colIDs := coll.Idx2ColUniqueIDs[idx.ID]
		var ndv int64
		for i, colID := range colIDs {
			if i >= usedColsLen {
				break
			}
			if col := coll.GetCol(colID); col != nil {
				ndv = max(ndv, col.Histogram.NDV)
			}
		}
		return outOfRangeEQSelectivity(sctx, ndv, realtimeCnt, int64(idx.TotalRowCount())), nil
	}

	minRowCount, crossValidSelectivity, err := crossValidationSelectivity(sctx, coll, idx, usedColsLen, idxPointRange)
	if err != nil {
		return 0, err
	}

	idxCount := float64(idx.QueryBytes(sctx, bytes))
	if minRowCount < idxCount {
		return crossValidSelectivity, nil
	}
	return idxCount / idx.TotalRowCount(), nil
}

// outOfRangeEQSelectivity estimates selectivities for out-of-range values.
// It assumes all modifications are insertions and all new-inserted rows are uniformly distributed
// and has the same distribution with analyzed rows, which means each unique value should have the
// same number of rows(Tot/NDV) of it.
// The input sctx is just for debug trace, you can pass nil safely if that's not needed.
func outOfRangeEQSelectivity(_ planctx.PlanContext, ndv, realtimeRowCount, columnRowCount int64) (result float64) {
	increaseRowCount := realtimeRowCount - columnRowCount
	if increaseRowCount <= 0 {
		return 0 // it must be 0 since the histogram contains the whole data
	}
	if ndv < outOfRangeBetweenRate {
		ndv = outOfRangeBetweenRate // avoid inaccurate selectivity caused by small NDV
	}
	selectivity := 1 / float64(ndv)
	if selectivity*float64(columnRowCount) > float64(increaseRowCount) {
		selectivity = float64(increaseRowCount) / float64(columnRowCount)
	}
	return selectivity
}

// outOfRangeFullNDV estimates the number of qualified rows when the topN represents all NDV values
// and the searched value does not appear in the topN
func outOfRangeFullNDV(ndv, origRowCount, notNullCount, realtimeRowCount, increaseFactor float64, modifyCount int64) (result float64) {
	// TODO: align or merge this out-of-range-est methods with `Histogram.OutOfRangeRowCount`.
	// If the table hasn't been modified, it's safe to return 0.
	if modifyCount == 0 {
		return 0
	}
	// Calculate "newly added rows" using original row count. We do NOT use notNullCount here
	// because that can always be less than realtimeRowCount if NULLs exist
	newRows := realtimeRowCount - origRowCount
	// If the original row count is zero - take the min of original row count and realtimeRowCount
	if notNullCount <= 0 {
		notNullCount = min(origRowCount, realtimeRowCount)
	}
	// If realtimeRowCount has reduced below the original, we can't determine if there has been a
	// combination of inserts/updates/deletes or only deletes - any out of range estimate is unreliable
	if newRows < 0 {
		newRows = min(notNullCount, realtimeRowCount)
	}
	// if no NDV - derive an NDV using sqrt, this could happen for unanalyzed tables
	if ndv <= 0 {
		ndv = math.Sqrt(max(notNullCount, realtimeRowCount))
	} else {
		// We need to increase the ndv by increaseFactor because the estimate will be increased by
		// the caller of the function
		ndv *= increaseFactor
	}
	// If topN represents all NDV values, the NDV should be relatively small.
	// Small NDV could cause extremely inaccurate result, use `outOfRangeBetweenRate` to smooth the result.
	// For example, TopN = {(value:1, rows: 10000), (2, 10000), (3, 10000)} and newRows = 15000, we should assume most
	// newly added rows are 1, 2 or 3. Then for an out-of-range estimation like `where col=9999`, the result should be
	// close to 0, but if we still use the original NDV, the result could be extremely large: 15000/3 = 5000.
	// See #64137 for a concrete example.
	ndv = max(ndv, float64(outOfRangeBetweenRate)) // avoid inaccurate estimate caused by small NDV
	return max(1, newRows/ndv)
}

// crossValidationSelectivity gets the selectivity of multi-column equal conditions by cross validation.
func crossValidationSelectivity(
	sctx planctx.PlanContext,
	coll *statistics.HistColl,
	idx *statistics.Index,
	usedColsLen int,
	idxPointRange *ranger.Range,
) (
	minRowCount float64,
	crossValidationSelectivity float64,
	err error,
) {
	minRowCount = math.MaxFloat64
	cols := coll.Idx2ColUniqueIDs[idx.ID]
	crossValidationSelectivity = 1.0
	totalRowCount := idx.TotalRowCount()
	for i, colID := range cols {
		if i >= usedColsLen {
			break
		}
		col := coll.GetCol(colID)
		if statistics.ColumnStatsIsInvalid(col, sctx, coll, colID) {
			continue
		}
		// Since the column range is point range(LowVal is equal to HighVal), we need to set both LowExclude and HighExclude to false.
		// Otherwise we would get 0.0 estRow from GetColumnRowCount.
		rang := ranger.Range{
			LowVal:      []types.Datum{idxPointRange.LowVal[i]},
			LowExclude:  false,
			HighVal:     []types.Datum{idxPointRange.HighVal[i]},
			HighExclude: false,
			Collators:   []collate.Collator{idxPointRange.Collators[i]},
		}

		rowCountEst, err := getColumnRowCount(sctx, col, []*ranger.Range{&rang}, coll.RealtimeCount, coll.ModifyCount, col.IsHandle)
		if err != nil {
			return 0, 0, err
		}
		rowCount := rowCountEst.Est
		crossValidationSelectivity = crossValidationSelectivity * (rowCount / totalRowCount)

		if rowCount < minRowCount {
			minRowCount = rowCount
		}
	}
	return minRowCount, crossValidationSelectivity, nil
}

// CollectFilters4MVIndex and BuildPartialPaths4MVIndex are for matching JSON expressions against mv index.
// This logic is shared between the estimation logic and the access path generation logic. But the two functions are
// defined in planner/core package and hard to move here. So we use this trick to avoid the import cycle.
var (
	CollectFilters4MVIndex func(
		sctx planctx.PlanContext,
		filters []expression.Expression,
		idxCols []*expression.Column,
	) (
		accessFilters,
		remainingFilters []expression.Expression,
		accessTp int,
	)
	BuildPartialPaths4MVIndex func(
		sctx planctx.PlanContext,
		accessFilters []expression.Expression,
		idxCols []*expression.Column,
		mvIndex *model.IndexInfo,
		histColl *statistics.HistColl,
	) (
		partialPaths []*planutil.AccessPath,
		isIntersection bool,
		ok bool,
		err error,
	)
)
