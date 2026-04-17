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

package globalstats

import (
	"bytes"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/stretchr/testify/require"
)

// findTopNCountByEncoded returns the count for the given encoded value in the TopN, or 0 if absent.
func findTopNCountByEncoded(topN *statistics.TopN, encoded []byte) uint64 {
	if topN == nil {
		return 0
	}
	for _, meta := range topN.TopN {
		if bytes.Equal(meta.Encoded, encoded) {
			return meta.Count
		}
	}
	return 0
}

// TestMergePartTopNAndHistToGlobalSpreadValue verifies that the combined merge
// correctly picks up histogram upper-bound Repeat counts for values that are
// in some partitions' TopN and at histogram upper bounds in others.
//
// This is the key "spread value" scenario that the combined merge addresses.
// It should capture exact counts for upper-bound values while avoiding
// the separate merge's inflation for non-upper-bound values.
//
// Setup (20 partitions, perTopN=2, globalTopN=2):
//
//	Partitions 0-1:  TopN = {val_spread(=50): 500, val_common(=200): 100}
//	                 Histogram: [1,20] [21,49] [51,100] [101,199]
//	Partitions 2-19: TopN = {val_X(=300): 90, val_common(=200): 85}
//	                 Histogram: [1,20] [21,50] [51,80] [81,100]
//	                 val_spread(=50) IS at upper bound with Repeat=80
//
// True:     val_spread=2440, val_common=1730, val_X=1620
// Separate: val_spread=2440 (exact), val_common=1730
// Combined: val_spread=2440 (extracts Repeat from upper bounds), val_common=1730
func TestMergePartTopNAndHistToGlobalSpreadValue(t *testing.T) {
	sc := stmtctx.NewStmtCtxWithTimeZone(time.UTC)
	killer := sqlkiller.SQLKiller{}

	encodeInt := func(v int64) []byte {
		key, err := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(v))
		require.NoError(t, err)
		return key
	}
	keySpread := encodeInt(50)
	keyCommon := encodeInt(200)
	keyX := encodeInt(300)

	const nPartitions = 20
	const globalTopNSize = 2

	d := func(v int64) types.Datum { return types.NewIntDatum(v) }
	tp := types.NewFieldType(mysql.TypeLong)

	topNs := make([]*statistics.TopN, nPartitions)
	hists := make([]*statistics.Histogram, nPartitions)
	for i := range nPartitions {
		if i < 2 {
			topN := statistics.NewTopN(2)
			topN.AppendTopN(keySpread, 500)
			topN.AppendTopN(keyCommon, 100)
			topNs[i] = topN
			h := statistics.NewHistogram(1, 20, 0, 0, tp, chunk.InitialCapacity, 0)
			d1, d20 := d(1), d(20)
			h.AppendBucket(&d1, &d20, 60, 8)
			d21, d49 := d(21), d(49)
			h.AppendBucket(&d21, &d49, 120, 6)
			d51, d100 := d(51), d(100)
			h.AppendBucket(&d51, &d100, 200, 10)
			d101, d199 := d(101), d(199)
			h.AppendBucket(&d101, &d199, 280, 12)
			hists[i] = h
		} else {
			topN := statistics.NewTopN(2)
			topN.AppendTopN(keyX, 90)
			topN.AppendTopN(keyCommon, 85)
			topNs[i] = topN
			h := statistics.NewHistogram(1, 20, 0, 0, tp, chunk.InitialCapacity, 0)
			d1, d20 := d(1), d(20)
			h.AppendBucket(&d1, &d20, 80, 10)
			d21, d50 := d(21), d(50)
			h.AppendBucket(&d21, &d50, 200, 80) // val_spread at upper bound!
			d51, d80 := d(51), d(80)
			h.AppendBucket(&d51, &d80, 280, 15)
			d81, d100 := d(81), d(100)
			h.AppendBucket(&d81, &d100, 350, 12)
			hists[i] = h
		}
	}

	hybridTopN, hybridHist, err := statistics.MergePartTopNAndHistToGlobal(
		topNs, hists, globalTopNSize, 100, false, &killer, sc,
	)
	require.NoError(t, err)
	require.NotNil(t, hybridTopN)
	require.NotNil(t, hybridHist)
	require.Len(t, hybridTopN.TopN, globalTopNSize)

	// Combined merge should pick up val_spread via Repeat extraction:
	// val_spread = 500*2 (TopN) + 80*18 (Repeat from histogram upper bounds) = 2440
	hybridSpread := findTopNCountByEncoded(hybridTopN, keySpread)
	require.Equal(t, uint64(2440), hybridSpread,
		"combined merge should find exact count for val_spread via Repeat extraction")

	// val_common = 100*2 + 85*18 = 1730
	hybridCommon := findTopNCountByEncoded(hybridTopN, keyCommon)
	require.Equal(t, uint64(1730), hybridCommon,
		"combined merge should have exact count for val_common")

	// val_X should not be in global TopN (1620 < 1730)
	require.Equal(t, uint64(0), findTopNCountByEncoded(hybridTopN, keyX))
}

// TestMergePartTopNAndHistToGlobalNoInflation verifies that the combined merge
// does NOT inflate values that fall inside histogram buckets (not at upper
// bound), avoiding the separate merge's over-estimation.
func TestMergePartTopNAndHistToGlobalNoInflation(t *testing.T) {
	sc := stmtctx.NewStmtCtxWithTimeZone(time.UTC)
	killer := sqlkiller.SQLKiller{}

	encodeInt := func(v int64) []byte {
		key, err := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(v))
		require.NoError(t, err)
		return key
	}
	keyRare := encodeInt(50)
	keyCommon := encodeInt(200)
	keyFiller := encodeInt(300)

	const nPartitions = 20
	const globalTopNSize = 2

	d := func(v int64) types.Datum { return types.NewIntDatum(v) }
	tp := types.NewFieldType(mysql.TypeLong)

	topNs := make([]*statistics.TopN, nPartitions)
	hists := make([]*statistics.Histogram, nPartitions)
	for i := range nPartitions {
		if i < 2 {
			topN := statistics.NewTopN(2)
			topN.AppendTopN(keyRare, 60)
			topN.AppendTopN(keyCommon, 40)
			topNs[i] = topN
			h := statistics.NewHistogram(1, 20, 0, 0, tp, chunk.InitialCapacity, 0)
			d1, d25 := d(1), d(25)
			h.AppendBucket(&d1, &d25, 50, 5)
			d26, d49 := d(26), d(49)
			h.AppendBucket(&d26, &d49, 100, 6)
			d51, d100 := d(51), d(100)
			h.AppendBucket(&d51, &d100, 180, 10)
			d101, d199 := d(101), d(199)
			h.AppendBucket(&d101, &d199, 250, 12)
			hists[i] = h
		} else {
			topN := statistics.NewTopN(2)
			topN.AppendTopN(keyCommon, 40)
			topN.AppendTopN(keyFiller, 35)
			topNs[i] = topN
			// val_rare(=50) is INSIDE [26,55] but NOT at upper bound (55).
			// Combined merge should NOT pick up any inflated count for val_rare.
			h := statistics.NewHistogram(1, 5, 0, 0, tp, chunk.InitialCapacity, 0)
			d1, d25 := d(1), d(25)
			h.AppendBucket(&d1, &d25, 100, 10)
			d26, d55 := d(26), d(55)
			h.AppendBucket(&d26, &d55, 250, 20)
			d56, d80 := d(56), d(80)
			h.AppendBucket(&d56, &d80, 380, 15)
			d81, d100 := d(81), d(100)
			h.AppendBucket(&d81, &d100, 500, 12)
			hists[i] = h
		}
	}

	hybridTopN, hybridHist, err := statistics.MergePartTopNAndHistToGlobal(
		topNs, hists, globalTopNSize, 100, false, &killer, sc,
	)
	require.NoError(t, err)
	require.NotNil(t, hybridTopN)
	require.NotNil(t, hybridHist)
	require.Len(t, hybridTopN.TopN, globalTopNSize)

	// val_common = 40*20 = 800, val_filler = 35*18 = 630.
	// val_rare = 60*2 = 120 (TopN only, no histogram upper-bound inflation).
	// The upper-bound Repeat at [26,55] is 20 per partition (for value 55, not 50).
	require.Equal(t, uint64(800), findTopNCountByEncoded(hybridTopN, keyCommon),
		"combined merge should have exact count for val_common")
	require.Equal(t, uint64(630), findTopNCountByEncoded(hybridTopN, keyFiller),
		"combined merge should have exact count for val_filler")
	require.Equal(t, uint64(0), findTopNCountByEncoded(hybridTopN, keyRare),
		"combined merge should NOT inflate val_rare (not at upper bound)")
}

// TestMergePartTopNAndHistToGlobalCountPreservation verifies that the combined
// merge preserves total count: globalTopN + histogram total = sum of all
// input TopN counts + input histogram counts.
func TestMergePartTopNAndHistToGlobalCountPreservation(t *testing.T) {
	sc := stmtctx.NewStmtCtxWithTimeZone(time.UTC)
	killer := sqlkiller.SQLKiller{}

	encodeInt := func(v int64) []byte {
		key, err := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(v))
		require.NoError(t, err)
		return key
	}

	d := func(v int64) types.Datum { return types.NewIntDatum(v) }
	tp := types.NewFieldType(mysql.TypeLong)

	const nPartitions = 4
	topNs := make([]*statistics.TopN, nPartitions)
	hists := make([]*statistics.Histogram, nPartitions)

	// Build 4 partitions with overlapping TopN and histograms.
	for i := range nPartitions {
		topN := statistics.NewTopN(2)
		topN.AppendTopN(encodeInt(1), uint64(100+i*10))
		topN.AppendTopN(encodeInt(2), uint64(80+i*5))
		topNs[i] = topN

		h := statistics.NewHistogram(1, 10, 0, 0, tp, chunk.InitialCapacity, 0)
		d10, d20 := d(10), d(20)
		h.AppendBucket(&d10, &d20, 50, 10)
		d21, d30 := d(21), d(30)
		h.AppendBucket(&d21, &d30, 100, 15)
		hists[i] = h
	}

	// Calculate input totals.
	var inputTopNTotal uint64
	for _, topN := range topNs {
		inputTopNTotal += topN.TotalCount()
	}
	var inputHistTotal int64
	for _, h := range hists {
		if h.Len() > 0 {
			inputHistTotal += h.Buckets[h.Len()-1].Count
		}
	}

	hybridTopN, hybridHist, err := statistics.MergePartTopNAndHistToGlobal(
		topNs, hists, 2, 10, false, &killer, sc,
	)
	require.NoError(t, err)

	outputTotal := hybridHist.NotNullCount()
	if hybridTopN != nil {
		outputTotal += float64(hybridTopN.TotalCount())
	}
	inputTotal := float64(inputTopNTotal) + float64(inputHistTotal)
	require.InDelta(t, inputTotal, outputTotal, inputTotal*0.01,
		"total count should be preserved: input=%.0f, output=%.0f", inputTotal, outputTotal)
}
