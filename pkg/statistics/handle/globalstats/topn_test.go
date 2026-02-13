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

func TestMergePartTopN2GlobalTopNWithoutHists(t *testing.T) {
	loc := time.UTC
	sc := stmtctx.NewStmtCtxWithTimeZone(loc)
	version := 1
	killer := sqlkiller.SQLKiller{}

	// Prepare TopNs.
	topNs := make([]*statistics.TopN, 0, 10)
	for range 10 {
		// Construct TopN, should be key(1, 1) -> 2, key(1, 2) -> 2, key(1, 3) -> 3.
		topN := statistics.NewTopN(3)
		{
			key1, err := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(1), types.NewIntDatum(1))
			require.NoError(t, err)
			topN.AppendTopN(key1, 2)
			key2, err := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(1), types.NewIntDatum(2))
			require.NoError(t, err)
			topN.AppendTopN(key2, 2)
			key3, err := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(1), types.NewIntDatum(3))
			require.NoError(t, err)
			topN.AppendTopN(key3, 3)
		}
		topNs = append(topNs, topN)
	}

	// Test merge 2 topN with nil hists.
	globalTopN, leftTopN, _, err := MergePartTopN2GlobalTopN(loc, version, topNs, 2, nil, false, &killer)
	require.NoError(t, err)
	require.Len(t, globalTopN.TopN, 2, "should only have 2 topN")
	require.Equal(t, uint64(50), globalTopN.TotalCount(), "should have 50 rows")
	require.Len(t, leftTopN, 1, "should have 1 left topN")
}

func TestMergePartTopN2GlobalTopNWithHists(t *testing.T) {
	loc := time.UTC
	sc := stmtctx.NewStmtCtxWithTimeZone(loc)
	version := 1
	killer := sqlkiller.SQLKiller{}

	// Prepare TopNs.
	topNs := make([]*statistics.TopN, 0, 10)
	for i := range 10 {
		// Construct TopN, should be key1 -> 2, key2 -> 2, key3 -> 3.
		topN := statistics.NewTopN(3)
		{
			key1, err := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(1))
			require.NoError(t, err)
			topN.AppendTopN(key1, 2)
			key2, err := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(2))
			require.NoError(t, err)
			topN.AppendTopN(key2, 2)
			if i%2 == 0 {
				key3, err := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(3))
				require.NoError(t, err)
				topN.AppendTopN(key3, 3)
			}
		}
		topNs = append(topNs, topN)
	}

	// Prepare Hists.
	hists := make([]*statistics.Histogram, 0, 10)
	for range 10 {
		// Construct Hist
		h := statistics.NewHistogram(1, 10, 0, 0, types.NewFieldType(mysql.TypeTiny), chunk.InitialCapacity, 0)
		h.Bounds.AppendInt64(0, 1)
		h.Buckets = append(h.Buckets, statistics.Bucket{Repeat: 10, Count: 20})
		h.Bounds.AppendInt64(0, 2)
		h.Buckets = append(h.Buckets, statistics.Bucket{Repeat: 10, Count: 30})
		h.Bounds.AppendInt64(0, 3)
		h.Buckets = append(h.Buckets, statistics.Bucket{Repeat: 10, Count: 30})
		h.Bounds.AppendInt64(0, 4)
		h.Buckets = append(h.Buckets, statistics.Bucket{Repeat: 10, Count: 40})
		hists = append(hists, h)
	}

	// Test merge 2 topN.
	globalTopN, leftTopN, _, err := MergePartTopN2GlobalTopN(loc, version, topNs, 2, hists, false, &killer)
	require.NoError(t, err)
	require.Len(t, globalTopN.TopN, 2, "should only have 2 topN")
	require.Equal(t, uint64(55), globalTopN.TotalCount(), "should have 55")
	require.Len(t, leftTopN, 1, "should have 1 left topN")
}

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

// TestMergeTopNV1InflationHistValueNotAtUpperBound shows how V1 inflates a
// TopN value's count using the uniform histogram estimate (NotNullCount/NDV)
// when the value falls inside a histogram bucket but is NOT at the upper bound.
//
// Setup (4 partitions, perTopN=2, 4 histogram buckets, globalTopN=2):
//
//	Partitions 0-1: TopN = {val50: 50, val200: 30}
//	                Histogram (4 buckets, excludes val50 and val200):
//	                  [1,25] cnt=60 rep=8, [26,49] cnt=120 rep=6,
//	                  [51,100] cnt=200 rep=10, [101,199] cnt=280 rep=12
//	Partitions 2-3: TopN = {val200: 30, val300: 28}
//	                Histogram (4 buckets, NDV=20, NotNullCount=500):
//	                  [1,25] cnt=100 rep=10, [26,55] cnt=250 rep=20,
//	                  [56,80] cnt=380 rep=15, [81,100] cnt=500 rep=12
//	                val50 is INSIDE [26,55] but NOT at upper bound (55)
//	                → EqualRowCount(val50) = NotNullCount/NDV = 500/20 = 25
//
// V1: val50 = 50*2 + 25*2 = 150 (inflated), val200 = 30*4 = 120
//
//	→ V1 TopN = {val50: 150, val200: 120} — val50 inflated to #1
func TestMergeTopNV1InflationHistValueNotAtUpperBound(t *testing.T) {
	loc := time.UTC
	sc := stmtctx.NewStmtCtxWithTimeZone(loc)
	killer := sqlkiller.SQLKiller{}

	encodeInt := func(v int64) []byte {
		key, err := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(v))
		require.NoError(t, err)
		return key
	}
	keyVal50 := encodeInt(50)
	keyVal200 := encodeInt(200)
	keyVal300 := encodeInt(300)

	d := func(v int64) types.Datum { return types.NewIntDatum(v) }

	topNs := make([]*statistics.TopN, 4)
	hists := make([]*statistics.Histogram, 4)
	tp := types.NewFieldType(mysql.TypeLong)
	for i := range 4 {
		if i < 2 {
			topN := statistics.NewTopN(2)
			topN.AppendTopN(keyVal50, 50)
			topN.AppendTopN(keyVal200, 30)
			topNs[i] = topN
			h := statistics.NewHistogram(1, 20, 0, 0, tp, chunk.InitialCapacity, 0)
			d1, d25 := d(1), d(25)
			h.AppendBucket(&d1, &d25, 60, 8)
			d26, d49 := d(26), d(49)
			h.AppendBucket(&d26, &d49, 120, 6)
			d51, d100 := d(51), d(100)
			h.AppendBucket(&d51, &d100, 200, 10)
			d101, d199 := d(101), d(199)
			h.AppendBucket(&d101, &d199, 280, 12)
			hists[i] = h
		} else {
			topN := statistics.NewTopN(2)
			topN.AppendTopN(keyVal200, 30)
			topN.AppendTopN(keyVal300, 28)
			topNs[i] = topN
			h := statistics.NewHistogram(1, 20, 0, 0, tp, chunk.InitialCapacity, 0)
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

	v1TopN, _, _, err := MergePartTopN2GlobalTopN(loc, 1, topNs, 2, hists, false, &killer)
	require.NoError(t, err)
	require.Len(t, v1TopN.TopN, 2)
	// V1 inflates val50: 50*2 + 25*2 = 150, making it #1.
	require.Equal(t, uint64(150), findTopNCountByEncoded(v1TopN, keyVal50))
	require.Equal(t, uint64(120), findTopNCountByEncoded(v1TopN, keyVal200))
	_ = sc // used by encodeInt
}

// TestMergePartTopNAndHistToGlobalSpreadValue verifies that the hybrid merge
// correctly picks up histogram upper-bound Repeat counts for values that are
// in some partitions' TopN and at histogram upper bounds in others.
//
// This is the key "spread value" scenario where V2 underestimates.
// The hybrid should match V1's accuracy for upper-bound values while avoiding
// V1's inflation for non-upper-bound values.
//
// Setup (20 partitions, perTopN=2, globalTopN=2):
//
//	Partitions 0-1:  TopN = {val_spread(=50): 500, val_common(=200): 100}
//	                 Histogram: [1,20] [21,49] [51,100] [101,199]
//	Partitions 2-19: TopN = {val_X(=300): 90, val_common(=200): 85}
//	                 Histogram: [1,20] [21,50] [51,80] [81,100]
//	                 val_spread(=50) IS at upper bound with Repeat=80
//
// True: val_spread=2440, val_common=1730, val_X=1620
// V1:   val_spread=2440 (exact), val_common=1730
// V2:   val_spread=1000 (misses 80*18=1440 from histograms)
// Hybrid: val_spread=2440 (extracts Repeat from upper bounds), val_common=1730
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
		topNs, hists, globalTopNSize, 100, false, &killer, sc, 1,
	)
	require.NoError(t, err)
	require.NotNil(t, hybridTopN)
	require.NotNil(t, hybridHist)
	require.Len(t, hybridTopN.TopN, globalTopNSize)

	// Hybrid should pick up val_spread via Repeat extraction:
	// val_spread = 500*2 (TopN) + 80*18 (Repeat from histogram upper bounds) = 2440
	hybridSpread := findTopNCountByEncoded(hybridTopN, keySpread)
	require.Equal(t, uint64(2440), hybridSpread,
		"hybrid should find exact count for val_spread via Repeat extraction")

	// val_common = 100*2 + 85*18 = 1730
	hybridCommon := findTopNCountByEncoded(hybridTopN, keyCommon)
	require.Equal(t, uint64(1730), hybridCommon,
		"hybrid should have exact count for val_common")

	// val_X should not be in global TopN (1620 < 1730)
	require.Equal(t, uint64(0), findTopNCountByEncoded(hybridTopN, keyX))
}

// TestMergePartTopNAndHistToGlobalNoInflation verifies that the hybrid merge
// does NOT inflate values that fall inside histogram buckets (not at upper
// bound), matching V2's behavior and avoiding V1's over-estimation.
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
			// Hybrid should NOT pick up any inflated count for val_rare.
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
		topNs, hists, globalTopNSize, 100, false, &killer, sc, 1,
	)
	require.NoError(t, err)
	require.NotNil(t, hybridTopN)
	require.NotNil(t, hybridHist)
	require.Len(t, hybridTopN.TopN, globalTopNSize)

	// val_common = 40*20 = 800, val_filler = 35*18 = 630.
	// val_rare = 60*2 = 120 (TopN only, no histogram upper-bound inflation).
	// The upper-bound Repeat at [26,55] is 20 per partition (for value 55, not 50).
	require.Equal(t, uint64(800), findTopNCountByEncoded(hybridTopN, keyCommon),
		"hybrid should have exact count for val_common")
	require.Equal(t, uint64(630), findTopNCountByEncoded(hybridTopN, keyFiller),
		"hybrid should have exact count for val_filler")
	require.Equal(t, uint64(0), findTopNCountByEncoded(hybridTopN, keyRare),
		"hybrid should NOT inflate val_rare (not at upper bound)")
}

// TestMergePartTopNAndHistToGlobalCountPreservation verifies that the hybrid
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
		topNs, hists, 2, 10, false, &killer, sc, 1,
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
