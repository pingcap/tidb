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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/context"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

const (
	pseudoEqualRate   = 1000
	pseudoLessRate    = 3
	pseudoBetweenRate = 40
)

// If one condition can't be calculated, we will assume that the selectivity of this condition is 0.8.
const selectionFactor = 0.8

// PseudoAvgCountPerValue gets a pseudo average count if histogram not exists.
func PseudoAvgCountPerValue(t *statistics.Table) float64 {
	return float64(t.RealtimeCount) / pseudoEqualRate
}

func pseudoSelectivity(sctx context.PlanContext, coll *statistics.HistColl, exprs []expression.Expression) float64 {
	minFactor := selectionFactor
	colExists := make(map[string]bool)
	for _, expr := range exprs {
		fun, ok := expr.(*expression.ScalarFunction)
		if !ok {
			continue
		}
		colID := getConstantColumnID(fun.GetArgs())
		if colID == unknownColumnID {
			continue
		}
		statistics.ColumnStatsIsInvalid((*statistics.Column)(nil), sctx, coll, colID)
		switch fun.FuncName.L {
		case ast.EQ, ast.NullEQ, ast.In:
			minFactor = math.Min(minFactor, 1.0/pseudoEqualRate)
			col, ok := coll.Columns[colID]
			if !ok {
				continue
			}
			colExists[col.Info.Name.L] = true
			if mysql.HasUniKeyFlag(col.Info.GetFlag()) {
				return 1.0 / float64(coll.RealtimeCount)
			}
		case ast.GE, ast.GT, ast.LE, ast.LT:
			minFactor = math.Min(minFactor, 1.0/pseudoLessRate)
			// FIXME: To resolve the between case.
		}
	}
	if len(colExists) == 0 {
		return minFactor
	}
	// use the unique key info
	for _, idx := range coll.Indices {
		unique := true
		firstMatch := false
		for _, col := range idx.Info.Columns {
			if !colExists[col.Name.L] {
				unique = false
				break
			}
			firstMatch = true
		}
		if firstMatch {
			statistics.IndexStatsIsInvalid(sctx, (*statistics.Index)(nil), coll, idx.ID)
		}
		if idx.Info.Unique && unique {
			return 1.0 / float64(coll.RealtimeCount)
		}
	}
	return minFactor
}

func getPseudoRowCountBySignedIntRanges(intRanges []*ranger.Range, tableRowCount float64) float64 {
	var rowCount float64
	for _, rg := range intRanges {
		var cnt float64
		low := rg.LowVal[0].GetInt64()
		if rg.LowVal[0].Kind() == types.KindNull || rg.LowVal[0].Kind() == types.KindMinNotNull {
			low = math.MinInt64
		}
		high := rg.HighVal[0].GetInt64()
		if rg.HighVal[0].Kind() == types.KindMaxValue {
			high = math.MaxInt64
		}
		if low == math.MinInt64 && high == math.MaxInt64 {
			cnt = tableRowCount
		} else if low == math.MinInt64 {
			cnt = tableRowCount / pseudoLessRate
		} else if high == math.MaxInt64 {
			cnt = tableRowCount / pseudoLessRate
		} else {
			if low == high {
				cnt = 1 // When primary key is handle, the equal row count is at most one.
			} else {
				cnt = tableRowCount / pseudoBetweenRate
			}
		}
		if high-low > 0 && cnt > float64(high-low) {
			cnt = float64(high - low)
		}
		rowCount += cnt
	}
	if rowCount > tableRowCount {
		rowCount = tableRowCount
	}
	return rowCount
}

func getPseudoRowCountByUnsignedIntRanges(intRanges []*ranger.Range, tableRowCount float64) float64 {
	var rowCount float64
	for _, rg := range intRanges {
		var cnt float64
		low := rg.LowVal[0].GetUint64()
		if rg.LowVal[0].Kind() == types.KindNull || rg.LowVal[0].Kind() == types.KindMinNotNull {
			low = 0
		}
		high := rg.HighVal[0].GetUint64()
		if rg.HighVal[0].Kind() == types.KindMaxValue {
			high = math.MaxUint64
		}
		if low == 0 && high == math.MaxUint64 {
			cnt = tableRowCount
		} else if low == 0 {
			cnt = tableRowCount / pseudoLessRate
		} else if high == math.MaxUint64 {
			cnt = tableRowCount / pseudoLessRate
		} else {
			if low == high {
				cnt = 1 // When primary key is handle, the equal row count is at most one.
			} else {
				cnt = tableRowCount / pseudoBetweenRate
			}
		}
		if high > low && cnt > float64(high-low) {
			cnt = float64(high - low)
		}
		rowCount += cnt
	}
	if rowCount > tableRowCount {
		rowCount = tableRowCount
	}
	return rowCount
}

func getPseudoRowCountByIndexRanges(tc types.Context, indexRanges []*ranger.Range,
	tableRowCount float64, colsLen int) (float64, error) {
	if tableRowCount == 0 {
		return 0, nil
	}
	var totalCount float64
	for _, indexRange := range indexRanges {
		count := tableRowCount
		i, err := indexRange.PrefixEqualLen(tc)
		if err != nil {
			return 0, errors.Trace(err)
		}
		if i == colsLen && !indexRange.LowExclude && !indexRange.HighExclude {
			totalCount += 1.0
			continue
		}
		if i >= len(indexRange.LowVal) {
			i = len(indexRange.LowVal) - 1
		}
		rowCount, err := getPseudoRowCountByColumnRanges(tc, tableRowCount, []*ranger.Range{indexRange}, i)
		if err != nil {
			return 0, errors.Trace(err)
		}
		count = count / tableRowCount * rowCount
		// If the condition is a = 1, b = 1, c = 1, d = 1, we think every a=1, b=1, c=1 only filtrate 1/100 data,
		// so as to avoid collapsing too fast.
		for j := 0; j < i; j++ {
			count = count / float64(100)
		}
		totalCount += count
	}
	if totalCount > tableRowCount {
		totalCount = tableRowCount / 3.0
	}
	return totalCount, nil
}

// getPseudoRowCountByColumnRanges calculate the row count by the ranges if there's no statistics information for this column.
func getPseudoRowCountByColumnRanges(tc types.Context, tableRowCount float64, columnRanges []*ranger.Range, colIdx int) (float64, error) {
	var rowCount float64
	for _, ran := range columnRanges {
		if ran.LowVal[colIdx].Kind() == types.KindNull && ran.HighVal[colIdx].Kind() == types.KindMaxValue {
			rowCount += tableRowCount
		} else if ran.LowVal[colIdx].Kind() == types.KindMinNotNull {
			nullCount := tableRowCount / pseudoEqualRate
			if ran.HighVal[colIdx].Kind() == types.KindMaxValue {
				rowCount += tableRowCount - nullCount
			} else {
				lessCount := tableRowCount / pseudoLessRate
				rowCount += lessCount - nullCount
			}
		} else if ran.HighVal[colIdx].Kind() == types.KindMaxValue {
			rowCount += tableRowCount / pseudoLessRate
		} else {
			compare, err := ran.LowVal[colIdx].Compare(tc, &ran.HighVal[colIdx], ran.Collators[colIdx])
			if err != nil {
				return 0, errors.Trace(err)
			}
			if compare == 0 {
				rowCount += tableRowCount / pseudoEqualRate
			} else {
				rowCount += tableRowCount / pseudoBetweenRate
			}
		}
	}
	if rowCount > tableRowCount {
		rowCount = tableRowCount
	}
	return rowCount, nil
}
