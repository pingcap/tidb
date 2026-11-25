// Copyright 2026 PingCAP, Inc.
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
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/stretchr/testify/require"
)

// The tests in this file check that a worst case (MaxEst) computed for a non-exact estimate
// survives the code that combines estimates, rather than being dropped or shrunk on the way
// to path.MaxCountAfterAccess.

const (
	maxEstBktLower  = 0
	maxEstBktUpper  = 100
	maxEstBktCount  = 1000
	maxEstBktRepeat = 10
	maxEstBktNDV    = 50
)

// newMaxEstColumn builds a V2 integer column whose histogram is a single wide bucket
// [0, 100] holding 1000 rows, so any range inside it is interpolated and gets the whole
// bucket as its worst case. topNCnt > 0 adds the TopN value topNVal.
func newMaxEstColumn(id int64, topNVal int64, topNCnt uint64) *statistics.Column {
	tp := types.NewFieldType(mysql.TypeLonglong)
	hist := statistics.NewHistogram(id, maxEstBktNDV, 0, 0, tp, 1, 0)
	lower, upper := types.NewIntDatum(maxEstBktLower), types.NewIntDatum(maxEstBktUpper)
	hist.AppendBucket(&lower, &upper, maxEstBktCount, maxEstBktRepeat)
	topN := statistics.NewTopN(1)
	if topNCnt > 0 {
		key, err := codec.EncodeKey(time.UTC, nil, types.NewIntDatum(topNVal))
		if err != nil {
			panic(err)
		}
		topN.AppendTopN(key, topNCnt)
	}
	return &statistics.Column{
		Histogram:         *hist,
		TopN:              topN,
		Info:              &model.ColumnInfo{ID: id, FieldType: *tp, State: model.StatePublic},
		StatsLoadedStatus: statistics.NewStatsFullLoadStatus(),
		StatsVer:          statistics.Version2,
	}
}

func newMaxEstColl(realtimeCount int64, cols ...*statistics.Column) *statistics.HistColl {
	coll := statistics.NewHistColl(1, realtimeCount, 0, len(cols), 0)
	coll.CanNotTriggerLoad = true
	for _, c := range cols {
		coll.SetCol(c.Info.ID, c)
	}
	return coll
}

func intRange(low, high int64, lowExclude bool) []*ranger.Range {
	return []*ranger.Range{{
		LowVal:     []types.Datum{types.NewIntDatum(low)},
		HighVal:    []types.Datum{types.NewIntDatum(high)},
		LowExclude: lowExclude,
		Collators:  collate.GetBinaryCollatorSlice(1),
	}}
}

// TestColumnRangeTopNKeepsMaxEst: the TopN rows inside a column range are exact and must
// shift MaxEst together with Est. Adding them to Est alone left MaxEst below Est, and the
// later Clamp then threw away the bucket's worst case.
func TestColumnRangeTopNKeepsMaxEst(t *testing.T) {
	sctx := mock.NewContext()
	// [20, 30) lies inside the single bucket and has no upper point estimate, so the result
	// is exactly the interpolated bucket fraction plus the TopN rows in the range.
	ran := intRange(20, 30, false)
	ran[0].HighExclude = true

	noTopN := newMaxEstColumn(1, 0, 0)
	histOnly, err := getColumnRowCount(sctx, noTopN, ran, int64(noTopN.TotalRowCount()), 0, false)
	require.NoError(t, err)
	require.Greater(t, histOnly.MaxEst, histOnly.Est, "in-bucket range must report a worst case")

	// The TopN value 25 adds 2000 exact rows, which must move all three bounds.
	col := newMaxEstColumn(1, 25, 2000)
	est, err := getColumnRowCount(sctx, col, ran, int64(col.TotalRowCount()), 0, false)
	require.NoError(t, err)
	require.InDelta(t, histOnly.Est+2000, est.Est, 1e-6)
	require.InDelta(t, histOnly.MinEst+2000, est.MinEst, 1e-6)
	require.InDelta(t, histOnly.MaxEst+2000, est.MaxEst, 1e-6)
}

// TestColumnRangeLowExcludeKeepsMaxEst: (range) - (low point) must subtract the point's
// MinEst from MaxEst. Subtracting the point's MaxEst shrank the worst case exactly when the
// point estimate was uncertain.
func TestColumnRangeLowExcludeKeepsMaxEst(t *testing.T) {
	sctx := mock.NewContext()
	// Make the uniform point estimate carry a spread, so MinEst < Est < MaxEst for the low point.
	// The TopN value 90 (outside the range) caps that spread at 60 rows, which keeps every
	// bound below the table size so no clamp hides the arithmetic.
	sctx.GetSessionVars().RiskEqSkewRatio = 0.5
	col := newMaxEstColumn(1, 90, 60)
	realtime := int64(col.TotalRowCount())

	closed, err := getColumnRowCount(sctx, col, intRange(20, 30, false), realtime, 0, false)
	require.NoError(t, err)
	point, err := getColumnRowCount(sctx, col, intRange(20, 20, false), realtime, 0, false)
	require.NoError(t, err)
	require.Less(t, point.MinEst, point.MaxEst, "the low point must carry a spread for this test")
	open, err := getColumnRowCount(sctx, col, intRange(20, 30, true), realtime, 0, false)
	require.NoError(t, err)

	require.InDelta(t, closed.Est-point.Est, open.Est, 1e-6)
	require.InDelta(t, closed.MaxEst-point.MinEst, open.MaxEst, 1e-6)
	require.InDelta(t, max(closed.MinEst-point.MaxEst, 1), open.MinEst, 1e-6)
}

// TestExpBackoffSingleColumnKeepsMaxEst: when only one column of a multi-column range has
// statistics there is nothing to back off, but that column's worst case still applies.
func TestExpBackoffSingleColumnKeepsMaxEst(t *testing.T) {
	sctx := mock.NewContext()
	// Column 1 has no statistics, column 2 has the single wide bucket.
	col2 := newMaxEstColumn(2, 0, 0)
	coll := newMaxEstColl(maxEstBktCount, col2)

	single, err := getColumnRowCount(sctx, col2, intRange(20, 30, false), maxEstBktCount, 0, false)
	require.NoError(t, err)
	require.Greater(t, single.MaxEst, single.Est)

	indexRange := &ranger.Range{
		LowVal:    []types.Datum{types.NewIntDatum(1), types.NewIntDatum(20)},
		HighVal:   []types.Datum{types.NewIntDatum(1), types.NewIntDatum(30)},
		Collators: collate.GetBinaryCollatorSlice(2),
	}
	sel, _, maxSel, ok, err := expBackoffEstimationByColIDs(sctx, coll, []int64{1, 2}, 0, 2, false, indexRange, nil)
	require.NoError(t, err)
	require.True(t, ok)
	require.InDelta(t, single.Est/maxEstBktCount, sel, 1e-9)
	require.InDelta(t, single.MaxEst/maxEstBktCount, maxSel, 1e-9)
}

// TestPartialStatsKeepsMaxEst covers the pseudo estimate used when the index has no
// statistics but its columns do.
func TestPartialStatsKeepsMaxEst(t *testing.T) {
	sctx := mock.NewContext()
	col2 := newMaxEstColumn(2, 0, 0)
	coll := newMaxEstColl(maxEstBktCount, col2)
	intTp := types.NewFieldType(mysql.TypeLonglong)
	exprCol1 := &expression.Column{ID: 1, UniqueID: 1, RetType: intTp}
	exprCol2 := &expression.Column{ID: 2, UniqueID: 2, RetType: intTp}

	single, err := getColumnRowCount(sctx, col2, intRange(20, 30, false), maxEstBktCount, 0, false)
	require.NoError(t, err)
	require.Greater(t, single.MaxEst, single.Est)

	t.Run("single column index", func(t *testing.T) {
		// This used to return a max of 0, below the estimate.
		count, maxCount, err := getPseudoRowCountWithPartialStats(sctx, coll, intRange(20, 30, false), maxEstBktCount,
			[]*expression.Column{exprCol2})
		require.NoError(t, err)
		require.InDelta(t, single.Est, count, 1e-6)
		require.InDelta(t, single.MaxEst, maxCount, 1e-6)
	})

	t.Run("independence branch", func(t *testing.T) {
		// Column 1 has no statistics, so exp backoff is not used. Its full range qualifies
		// every row, so column 2's worst case bounds the conjunction.
		indexRange := &ranger.Range{
			LowVal:    []types.Datum{types.NewDatum(nil), types.NewIntDatum(20)},
			HighVal:   []types.Datum{types.MaxValueDatum(), types.NewIntDatum(30)},
			Collators: collate.GetBinaryCollatorSlice(2),
		}
		count, maxCount, err := getPseudoRowCountWithPartialStats(sctx, coll, []*ranger.Range{indexRange}, maxEstBktCount,
			[]*expression.Column{exprCol1, exprCol2})
		require.NoError(t, err)
		require.GreaterOrEqual(t, maxCount, count)
		require.InDelta(t, single.MaxEst, maxCount, 1e-6)
	})
}
