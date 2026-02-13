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
	"math"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/mock"
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

func TestMergePartTopN2GlobalTopNV2WithoutHists(t *testing.T) {
	sc := stmtctx.NewStmtCtxWithTimeZone(time.UTC)
	killer := sqlkiller.SQLKiller{}

	// Prepare TopNs: 10 partitions, each with 3 keys.
	topNs := make([]*statistics.TopN, 0, 10)
	for range 10 {
		topN := statistics.NewTopN(3)
		key1, err := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(1), types.NewIntDatum(1))
		require.NoError(t, err)
		topN.AppendTopN(key1, 2)
		key2, err := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(1), types.NewIntDatum(2))
		require.NoError(t, err)
		topN.AppendTopN(key2, 2)
		key3, err := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(1), types.NewIntDatum(3))
		require.NoError(t, err)
		topN.AppendTopN(key3, 3)
		topNs = append(topNs, topN)
	}

	globalTopN, leftTopN, err := MergePartTopN2GlobalTopNV2(topNs, 2, &killer)
	require.NoError(t, err)
	require.Len(t, globalTopN.TopN, 2, "should only have 2 topN")
	require.Equal(t, uint64(50), globalTopN.TotalCount(), "should have 50 rows")
	require.Len(t, leftTopN, 1, "should have 1 left topN")
}

func TestMergePartTopN2GlobalTopNV2WithHists(t *testing.T) {
	sc := stmtctx.NewStmtCtxWithTimeZone(time.UTC)
	killer := sqlkiller.SQLKiller{}

	// Prepare TopNs: key3 only appears in even partitions.
	topNs := make([]*statistics.TopN, 0, 10)
	for i := range 10 {
		topN := statistics.NewTopN(3)
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
		topNs = append(topNs, topN)
	}

	// Prepare Hists (should not be modified by V2).
	hists := make([]*statistics.Histogram, 0, 10)
	for range 10 {
		h := statistics.NewHistogram(1, 10, 0, 0, types.NewFieldType(mysql.TypeTiny), chunk.InitialCapacity, 0)
		h.Bounds.AppendInt64(0, 1)
		h.Buckets = append(h.Buckets, statistics.Bucket{Repeat: 10, Count: 20})
		h.Bounds.AppendInt64(0, 2)
		h.Buckets = append(h.Buckets, statistics.Bucket{Repeat: 10, Count: 30})
		hists = append(hists, h)
	}

	// Snapshot histogram state before V2 merge.
	histsBefore := make([][]statistics.Bucket, len(hists))
	for i, h := range hists {
		histsBefore[i] = make([]statistics.Bucket, len(h.Buckets))
		copy(histsBefore[i], h.Buckets)
	}

	globalTopN, leftTopN, err := MergePartTopN2GlobalTopNV2(topNs, 2, &killer)
	require.NoError(t, err)
	require.Len(t, globalTopN.TopN, 2)
	require.Len(t, leftTopN, 1)
	// V2 counts from TopN only: key1=2*10=20, key2=2*10=20, key3=3*5=15. Top-2=40.
	require.Equal(t, uint64(40), globalTopN.TotalCount())

	// Histograms must be unchanged.
	for i, h := range hists {
		require.Equal(t, histsBefore[i], h.Buckets, "histogram %d should not be modified", i)
	}
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

// findTopNMetaCountByEncoded returns the count for the given encoded value in left-over TopNMeta, or 0.
func findTopNMetaCountByEncoded(metas []statistics.TopNMeta, encoded []byte) uint64 {
	for _, meta := range metas {
		if bytes.Equal(meta.Encoded, encoded) {
			return meta.Count
		}
	}
	return 0
}

// TestMergeTopNV1V2ComparisonHistValueNotAtUpperBound shows how V1 inflates a
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
// V1: val50 = 50*2 + 25*2 = 150, val200 = 30*4 = 120
//
//	→ V1 TopN = {val50: 150, val200: 120} — val50 inflated to #1
//
// V2: val50 = 50*2 = 100, val200 = 30*4 = 120
//
//	→ V2 TopN = {val200: 120, val50: 100} — val200 correctly ranked #1
func TestMergeTopNV1V2ComparisonHistValueNotAtUpperBound(t *testing.T) {
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

	makeInputs := func() ([]*statistics.TopN, []*statistics.Histogram) {
		topNs := make([]*statistics.TopN, 4)
		hists := make([]*statistics.Histogram, 4)
		tp := types.NewFieldType(mysql.TypeLong)
		for i := range 4 {
			if i < 2 {
				topN := statistics.NewTopN(2)
				topN.AppendTopN(keyVal50, 50)
				topN.AppendTopN(keyVal200, 30)
				topNs[i] = topN
				// 4 buckets (excludes val50 and val200).
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
				// 4 buckets, NDV=20, NotNullCount=500.
				// val50 INSIDE [26,55], NOT at upper bound → EqualRowCount = 500/20 = 25.
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
		return topNs, hists
	}

	// --- V1 merge ---
	v1TopNs, v1Hists := makeInputs()
	v1TopN, v1Left, v1Hists, err := MergePartTopN2GlobalTopN(loc, 1, v1TopNs, 2, v1Hists, false, &killer)
	require.NoError(t, err)
	require.Len(t, v1TopN.TopN, 2)
	// V1 inflates val50: 50*2 + 25*2 = 150, making it #1.
	require.Equal(t, uint64(150), findTopNCountByEncoded(v1TopN, keyVal50))
	require.Equal(t, uint64(120), findTopNCountByEncoded(v1TopN, keyVal200))

	v1GlobalHist, err := statistics.MergePartitionHist2GlobalHist(sc, v1Hists, v1Left, 100, false, 1)
	require.NoError(t, err)
	require.NotNil(t, v1GlobalHist)

	// --- V2 merge ---
	v2TopNs, v2Hists := makeInputs()
	v2TopN, v2Left, err := MergePartTopN2GlobalTopNV2(v2TopNs, 2, &killer)
	require.NoError(t, err)
	require.Len(t, v2TopN.TopN, 2)
	// V2: val200 = 120 is #1 (no inflation), val50 = 100 is #2.
	require.Equal(t, uint64(120), findTopNCountByEncoded(v2TopN, keyVal200))
	require.Equal(t, uint64(100), findTopNCountByEncoded(v2TopN, keyVal50))

	v2GlobalHist, err := statistics.MergePartitionHist2GlobalHist(sc, v2Hists, v2Left, 100, false, 1)
	require.NoError(t, err)
	require.NotNil(t, v2GlobalHist)

	// V1 subtracted estimated counts from histograms via BinarySearchRemoveVal,
	// so its histogram totals are lower.
	require.Greater(t, v2GlobalHist.NotNullCount(), v1GlobalHist.NotNullCount(),
		"V2 histogram should have higher total count because V1 subtracted histogram estimates")
}

// TestMergeTopNV1V2ComparisonHistValueAtUpperBound shows how V1 picks up the
// accurate Repeat count when a TopN value happens to be at a histogram bucket's
// upper bound. This is the one scenario where V1's histogram lookup is precise.
//
// Setup (4 partitions, perTopN=2, 4 histogram buckets, globalTopN=2):
//
//	Partitions 0-1: TopN = {val50: 50, val200: 35}
//	                Histogram (4 buckets, excludes val50 and val200):
//	                  [1,25] cnt=60 rep=8, [26,49] cnt=120 rep=6,
//	                  [51,100] cnt=200 rep=10, [101,199] cnt=280 rep=12
//	Partitions 2-3: TopN = {val200: 35, val300: 30}
//	                Histogram (4 buckets):
//	                  [1,25] cnt=80 rep=10, [26,50] cnt=200 rep=80,
//	                  [51,80] cnt=280 rep=15, [81,100] cnt=350 rep=12
//	                val50 IS at upper bound of [26,50] → EqualRowCount = Repeat = 80
//
// V1: val50 = 50*2 + 80*2 = 260, val200 = 35*4 = 140
//
//	→ V1 TopN = {val50: 260, val200: 140} — val50 boosted with exact Repeat
//
// V2: val50 = 50*2 = 100, val200 = 35*4 = 140
//
//	→ V2 TopN = {val200: 140, val50: 100} — val200 ranked #1
func TestMergeTopNV1V2ComparisonHistValueAtUpperBound(t *testing.T) {
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

	makeInputs := func() ([]*statistics.TopN, []*statistics.Histogram) {
		topNs := make([]*statistics.TopN, 4)
		hists := make([]*statistics.Histogram, 4)
		tp := types.NewFieldType(mysql.TypeLong)
		for i := range 4 {
			if i < 2 {
				topN := statistics.NewTopN(2)
				topN.AppendTopN(keyVal50, 50)
				topN.AppendTopN(keyVal200, 35)
				topNs[i] = topN
				// 4 buckets (excludes val50 and val200).
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
				topN.AppendTopN(keyVal200, 35)
				topN.AppendTopN(keyVal300, 30)
				topNs[i] = topN
				// 4 buckets; val50 IS at upper bound of [26,50] → Repeat=80.
				h := statistics.NewHistogram(1, 20, 0, 0, tp, chunk.InitialCapacity, 0)
				d1, d25 := d(1), d(25)
				h.AppendBucket(&d1, &d25, 80, 10)
				d26, d50 := d(26), d(50)
				h.AppendBucket(&d26, &d50, 200, 80) // val50 at upper bound!
				d51, d80 := d(51), d(80)
				h.AppendBucket(&d51, &d80, 280, 15)
				d81, d100 := d(81), d(100)
				h.AppendBucket(&d81, &d100, 350, 12)
				hists[i] = h
			}
		}
		return topNs, hists
	}

	// --- V1 merge ---
	v1TopNs, v1Hists := makeInputs()
	v1TopN, v1Left, v1Hists, err := MergePartTopN2GlobalTopN(loc, 1, v1TopNs, 2, v1Hists, false, &killer)
	require.NoError(t, err)
	require.Len(t, v1TopN.TopN, 2)
	// V1 picks val50 as #1: 50*2 + 80*2 (exact Repeat) = 260.
	require.Equal(t, uint64(260), findTopNCountByEncoded(v1TopN, keyVal50))
	require.Equal(t, uint64(140), findTopNCountByEncoded(v1TopN, keyVal200))

	v1GlobalHist, err := statistics.MergePartitionHist2GlobalHist(sc, v1Hists, v1Left, 100, false, 1)
	require.NoError(t, err)
	require.NotNil(t, v1GlobalHist)

	// --- V2 merge ---
	v2TopNs, v2Hists := makeInputs()
	v2TopN, v2Left, err := MergePartTopN2GlobalTopNV2(v2TopNs, 2, &killer)
	require.NoError(t, err)
	require.Len(t, v2TopN.TopN, 2)
	// V2: val200 = 140 is #1, val50 = 100 is #2 (misses Repeat from p2-3).
	require.Equal(t, uint64(140), findTopNCountByEncoded(v2TopN, keyVal200))
	require.Equal(t, uint64(100), findTopNCountByEncoded(v2TopN, keyVal50))

	v2GlobalHist, err := statistics.MergePartitionHist2GlobalHist(sc, v2Hists, v2Left, 100, false, 1)
	require.NoError(t, err)
	require.NotNil(t, v2GlobalHist)

	// V1 removed exact repeat counts (80 per partition for val50) from histograms
	// via BinarySearchRemoveVal. V2 leaves histograms intact.
	require.Greater(t, v2GlobalHist.NotNullCount(), v1GlobalHist.NotNullCount(),
		"V2 histogram should have higher total count because V1 subtracted repeat counts from histograms")
}

// TestMergeTopNV2BetterThanV1ManyPartitionsInflation demonstrates V1's key
// weakness: when a value is in TopN of a few partitions but falls inside
// histogram buckets (not at upper bound) of many others, V1 accumulates the
// uniform estimate NotNullCount/NDV from each partition. With many partitions
// this inflates the count so much that V1 picks the wrong global TopN.
//
// Setup (20 partitions, perTopN=2, 4 histogram buckets, globalTopN=2):
//
//	Partitions 0-1:  TopN = {val_rare(=50): 60, val_common(=200): 40}
//	                 Histogram (4 buckets, excludes TopN values):
//	                   [1,25] cnt=50 rep=5, [26,49] cnt=100 rep=6,
//	                   [51,100] cnt=180 rep=10, [101,199] cnt=250 rep=12
//
//	Partitions 2-19: TopN = {val_common(=200): 40, val_filler(=300): 35}
//	                 Histogram (4 buckets, NDV=5, NotNullCount=500):
//	                   [1,25] cnt=100 rep=10, [26,55] cnt=250 rep=20,
//	                   [56,80] cnt=380 rep=15, [81,100] cnt=500 rep=12
//	                 val_rare(=50) is INSIDE [26,55] but NOT at upper bound
//	                 → EqualRowCount(val_rare) = NotNullCount/NDV = 500/5 = 100
//
// True global counts:
//
//	val_rare:   60*2              = 120
//	val_common: 40*20             = 800
//	val_filler: 35*18             = 630
//
// V1: val_rare  = 60*2 + 100*18 = 1920 (16x overestimate!)
//
//	val_common = 40*20           = 800
//	val_filler = 35*18           = 630
//	→ V1 TopN = {val_rare: 1920, val_common: 800} — WRONG, val_rare inflated
//
// V2: val_rare  = 60*2           = 120
//
//	val_common = 40*20           = 800
//	val_filler = 35*18           = 630
//	→ V2 TopN = {val_common: 800, val_filler: 630} — correct top 2
func TestMergeTopNV2BetterThanV1ManyPartitionsInflation(t *testing.T) {
	loc := time.UTC
	sc := stmtctx.NewStmtCtxWithTimeZone(loc)
	killer := sqlkiller.SQLKiller{}

	encodeInt := func(v int64) []byte {
		key, err := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(v))
		require.NoError(t, err)
		return key
	}
	keyRare := encodeInt(50)    // in TopN of p0-1, inside histogram of p2-19
	keyCommon := encodeInt(200) // in TopN of all 20 partitions
	keyFiller := encodeInt(300) // in TopN of p2-19 only

	const nPartitions = 20
	const globalTopNSize = 2

	makeInputs := func() ([]*statistics.TopN, []*statistics.Histogram) {
		topNs := make([]*statistics.TopN, nPartitions)
		hists := make([]*statistics.Histogram, nPartitions)

		d := func(v int64) types.Datum { return types.NewIntDatum(v) }
		for i := range nPartitions {
			tp := types.NewFieldType(mysql.TypeLong)
			if i < 2 {
				// P0-1: val_rare and val_common in TopN.
				topN := statistics.NewTopN(2)
				topN.AppendTopN(keyRare, 60)
				topN.AppendTopN(keyCommon, 40)
				topNs[i] = topN
				// 4 buckets (excludes val_rare=50 and val_common=200).
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
				// P2-19: val_common and val_filler in TopN.
				topN := statistics.NewTopN(2)
				topN.AppendTopN(keyCommon, 40)
				topN.AppendTopN(keyFiller, 35)
				topNs[i] = topN
				// 4 buckets, NDV=5, NotNullCount=500.
				// val_rare(=50) falls INSIDE [26,55] but is NOT at upper bound (55).
				// EqualRowCount(val_rare) = 500/5 = 100.
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
		return topNs, hists
	}

	// --- V1 merge ---
	v1TopNs, v1Hists := makeInputs()
	v1TopN, v1Left, v1Hists, err := MergePartTopN2GlobalTopN(
		loc, 1, v1TopNs, globalTopNSize, v1Hists, false, &killer,
	)
	require.NoError(t, err)
	require.Len(t, v1TopN.TopN, globalTopNSize)

	// V1 inflates val_rare: 60*2 + 100*18 = 1920. Picks it over val_filler (630).
	v1RareCount := findTopNCountByEncoded(v1TopN, keyRare)
	require.Equal(t, uint64(1920), v1RareCount,
		"V1 should inflate val_rare via uniform histogram estimates")
	require.Equal(t, uint64(800), findTopNCountByEncoded(v1TopN, keyCommon))

	v1GlobalHist, err := statistics.MergePartitionHist2GlobalHist(
		sc, v1Hists, v1Left, 100, false, 1,
	)
	require.NoError(t, err)

	// --- V2 merge ---
	v2TopNs, v2Hists := makeInputs()
	v2TopN, v2Left, err := MergePartTopN2GlobalTopNV2(v2TopNs, globalTopNSize, &killer)
	require.NoError(t, err)
	require.Len(t, v2TopN.TopN, globalTopNSize)

	// V2 correctly picks {val_common: 800, val_filler: 630}.
	require.Equal(t, uint64(800), findTopNCountByEncoded(v2TopN, keyCommon))
	require.Equal(t, uint64(630), findTopNCountByEncoded(v2TopN, keyFiller))
	require.Equal(t, uint64(0), findTopNCountByEncoded(v2TopN, keyRare),
		"V2 correctly excluded val_rare from global TopN")

	v2GlobalHist, err := statistics.MergePartitionHist2GlobalHist(
		sc, v2Hists, v2Left, 100, false, 1,
	)
	require.NoError(t, err)

	// --- Accuracy comparison ---
	const trueRare = 120   // 60*2
	const trueCommon = 800 // 40*20
	const trueFiller = 630 // 35*18

	// V1 TopN error: val_rare claimed 1920, true=120 → +1800 (16x overestimate).
	v1TopNError := math.Abs(float64(v1RareCount)-trueRare) +
		math.Abs(float64(findTopNCountByEncoded(v1TopN, keyCommon))-trueCommon)
	require.Equal(t, float64(1800), v1TopNError)

	// V2 TopN error: val_common=800 (exact), val_filler=630 (exact) → 0.
	v2TopNError := math.Abs(float64(findTopNCountByEncoded(v2TopN, keyCommon))-trueCommon) +
		math.Abs(float64(findTopNCountByEncoded(v2TopN, keyFiller))-trueFiller)
	require.Equal(t, float64(0), v2TopNError)

	// V2 is strictly more accurate.
	require.Less(t, v2TopNError, v1TopNError,
		"V2 should have lower TopN error than V1")

	// V2 also preserves higher histogram totals because it doesn't subtract
	// phantom counts from partition histograms.
	require.Greater(t, v2GlobalHist.NotNullCount(), v1GlobalHist.NotNullCount(),
		"V2 histogram should retain more rows because V1 subtracted inflated estimates")
}

// TestMergeTopNV1BetterThanV2SpreadValue shows the scenario where V1 is much
// more accurate than V2: a value that is globally #1 but only appears in two
// partitions' TopN, while being at a histogram bucket upper bound in 18 others.
// V1 picks up the exact Repeat count from each histogram; V2 only sees the two
// TopN entries and badly underestimates the value.
//
// Setup (20 partitions, perTopN=2, globalTopN=2):
//
//	All partitions: 4 histogram buckets, 2 TopN entries
//
//	Partitions 0-1:  TopN = {val_spread(=50): 500, val_common(=200): 100}
//	                 Histogram (excludes TopN values):
//	                   [1,20] cnt=60 rep=8, [21,49] cnt=120 rep=6,
//	                   [51,100] cnt=200 rep=10, [101,199] cnt=280 rep=12
//
//	Partitions 2-19: TopN = {val_X(=300): 90, val_common(=200): 85}
//	                 Histogram (excludes TopN values):
//	                   [1,20] cnt=80 rep=10, [21,50] cnt=200 rep=80,
//	                   [51,80] cnt=280 rep=15, [81,100] cnt=350 rep=12
//	                 val_spread(=50) IS at bucket upper bound → Repeat=80
//
// True global counts:
//
//	val_spread: 500*2 + 80*18      = 2440  (true #1)
//	val_common: 100*2 + 85*18      = 1730  (true #2)
//	val_X:      90*18              = 1620  (true #3)
//
// V1: val_spread = 500*2 + 80*18 = 2440, val_common = 100*2 + 85*18 = 1730
//
//	→ V1 TopN = {val_spread: 2440, val_common: 1730} — correct top 2
//
// V2: val_spread = 500*2 = 1000, val_common = 1730, val_X = 1620
//
//	→ V2 TopN = {val_common: 1730, val_X: 1620} — misses true #1
func TestMergeTopNV1BetterThanV2SpreadValue(t *testing.T) {
	loc := time.UTC
	sc := stmtctx.NewStmtCtxWithTimeZone(loc)
	killer := sqlkiller.SQLKiller{}

	encodeInt := func(v int64) []byte {
		key, err := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(v))
		require.NoError(t, err)
		return key
	}
	keySpread := encodeInt(50)  // in TopN of p0-1, at histogram upper bound in p2-19
	keyCommon := encodeInt(200) // in TopN of all 20 partitions
	keyX := encodeInt(300)      // in TopN of p2-19 only

	const nPartitions = 20
	const globalTopNSize = 2

	makeInputs := func() ([]*statistics.TopN, []*statistics.Histogram) {
		topNs := make([]*statistics.TopN, nPartitions)
		hists := make([]*statistics.Histogram, nPartitions)

		d := func(v int64) types.Datum { return types.NewIntDatum(v) }
		for i := range nPartitions {
			tp := types.NewFieldType(mysql.TypeLong)
			if i < 2 {
				// P0-1: val_spread and val_common in TopN.
				topN := statistics.NewTopN(2)
				topN.AppendTopN(keySpread, 500)
				topN.AppendTopN(keyCommon, 100)
				topNs[i] = topN
				// 4 buckets covering ranges that exclude val_spread(50) and val_common(200).
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
				// P2-19: val_X and val_common in TopN.
				topN := statistics.NewTopN(2)
				topN.AppendTopN(keyX, 90)
				topN.AppendTopN(keyCommon, 85)
				topNs[i] = topN
				// 4 buckets; val_spread(50) at upper bound of [21,50] with Repeat=80.
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
		return topNs, hists
	}

	// --- V1 merge ---
	v1TopNs, v1Hists := makeInputs()
	v1TopN, v1Left, v1Hists, err := MergePartTopN2GlobalTopN(
		loc, 1, v1TopNs, globalTopNSize, v1Hists, false, &killer,
	)
	require.NoError(t, err)
	require.Len(t, v1TopN.TopN, globalTopNSize)

	// V1 correctly identifies the top 2: val_spread=2440, val_common=1730.
	v1SpreadCount := findTopNCountByEncoded(v1TopN, keySpread)
	require.Equal(t, uint64(2440), v1SpreadCount,
		"V1 should find exact count for val_spread via histogram Repeat lookups")
	require.Equal(t, uint64(1730), findTopNCountByEncoded(v1TopN, keyCommon),
		"V1 should have exact count for val_common (in all partitions' TopN)")

	v1GlobalHist, err := statistics.MergePartitionHist2GlobalHist(
		sc, v1Hists, v1Left, 100, false, 1,
	)
	require.NoError(t, err)

	// --- V2 merge ---
	v2TopNs, v2Hists := makeInputs()
	v2TopN, v2Left, err := MergePartTopN2GlobalTopNV2(v2TopNs, globalTopNSize, &killer)
	require.NoError(t, err)
	require.Len(t, v2TopN.TopN, globalTopNSize)

	// V2 sees: val_spread=1000, val_common=1730, val_X=1620.
	// Picks {val_common: 1730, val_X: 1620} — misses the true #1.
	require.Equal(t, uint64(1730), findTopNCountByEncoded(v2TopN, keyCommon))
	require.Equal(t, uint64(1620), findTopNCountByEncoded(v2TopN, keyX))
	require.Equal(t, uint64(0), findTopNCountByEncoded(v2TopN, keySpread),
		"V2 misses val_spread from global TopN")

	// val_spread falls into V2's leftovers with only 1000 (true: 2440).
	v2SpreadLeftover := findTopNMetaCountByEncoded(v2Left, keySpread)
	require.Equal(t, uint64(1000), v2SpreadLeftover)

	v2GlobalHist, err := statistics.MergePartitionHist2GlobalHist(
		sc, v2Hists, v2Left, 100, false, 1,
	)
	require.NoError(t, err)

	// --- Accuracy comparison ---
	const trueSpread = 2440 // 500*2 + 80*18

	// V1 picks the correct #1 with exact count.
	require.Equal(t, uint64(trueSpread), v1SpreadCount,
		"V1 TopN count for val_spread should be exact")

	// V2's TopN-only count for val_spread (1000) underestimates by 1440 —
	// the 80*18 hidden in partition histograms that V2 never consults.
	v2SpreadError := float64(trueSpread) - float64(v2SpreadLeftover)
	require.Equal(t, float64(1440), v2SpreadError,
		"V2 underestimates val_spread by 80*18 = 1440")

	// V2's histogram retains higher totals because histograms are unmodified.
	require.Greater(t, v2GlobalHist.NotNullCount(), v1GlobalHist.NotNullCount(),
		"V2 histogram retains more rows since V1 subtracted from partitions")
}

func TestMergePartTopN2GlobalTopNV2EmptyTopNs(t *testing.T) {
	killer := sqlkiller.SQLKiller{}
	topNs := make([]*statistics.TopN, 5)
	for i := range topNs {
		topNs[i] = statistics.NewTopN(0)
	}
	globalTopN, leftTopN, err := MergePartTopN2GlobalTopNV2(topNs, 10, &killer)
	require.NoError(t, err)
	require.Nil(t, globalTopN)
	require.Nil(t, leftTopN)
}

func TestMergePartTopN2GlobalTopNV2SinglePartitionValues(t *testing.T) {
	sc := stmtctx.NewStmtCtxWithTimeZone(time.UTC)
	killer := sqlkiller.SQLKiller{}

	// Each partition has a unique value.
	topNs := make([]*statistics.TopN, 5)
	for i := range 5 {
		topN := statistics.NewTopN(1)
		key, err := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(int64(i)))
		require.NoError(t, err)
		topN.AppendTopN(key, 100)
		topNs[i] = topN
	}

	globalTopN, leftTopN, err := MergePartTopN2GlobalTopNV2(topNs, 3, &killer)
	require.NoError(t, err)
	require.Len(t, globalTopN.TopN, 3)
	require.Len(t, leftTopN, 2)
	require.Equal(t, uint64(300), globalTopN.TotalCount())

	// Verify total count is preserved: globalTopN + leftTopN == input total.
	var leftTotal uint64
	for _, m := range leftTopN {
		leftTotal += m.Count
	}
	require.Equal(t, uint64(500), globalTopN.TotalCount()+leftTotal)
}

// TestMergePartTopN2GlobalTopNV2PreservesTotalCount verifies that
// MergePartTopN2GlobalTopNV2 never loses or creates counts: the sum of
// globalTopN counts + leftover TopN counts must equal the sum of all
// input TopN counts.
func TestMergePartTopN2GlobalTopNV2PreservesTotalCount(t *testing.T) {
	sc := stmtctx.NewStmtCtxWithTimeZone(time.UTC)
	killer := sqlkiller.SQLKiller{}

	encodeInt := func(v int64) []byte {
		key, err := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(v))
		require.NoError(t, err)
		return key
	}

	// 6 partitions with overlapping TopN values and varying counts.
	// 5 distinct values across partitions, some shared, some unique.
	topNs := make([]*statistics.TopN, 6)

	// P0: val1=200, val2=150
	topNs[0] = statistics.NewTopN(2)
	topNs[0].AppendTopN(encodeInt(1), 200)
	topNs[0].AppendTopN(encodeInt(2), 150)

	// P1: val1=180, val3=120
	topNs[1] = statistics.NewTopN(2)
	topNs[1].AppendTopN(encodeInt(1), 180)
	topNs[1].AppendTopN(encodeInt(3), 120)

	// P2: val2=90, val4=80
	topNs[2] = statistics.NewTopN(2)
	topNs[2].AppendTopN(encodeInt(2), 90)
	topNs[2].AppendTopN(encodeInt(4), 80)

	// P3: val3=110, val5=70
	topNs[3] = statistics.NewTopN(2)
	topNs[3].AppendTopN(encodeInt(3), 110)
	topNs[3].AppendTopN(encodeInt(5), 70)

	// P4: val1=50, val4=60
	topNs[4] = statistics.NewTopN(2)
	topNs[4].AppendTopN(encodeInt(1), 50)
	topNs[4].AppendTopN(encodeInt(4), 60)

	// P5: val2=40, val5=30
	topNs[5] = statistics.NewTopN(2)
	topNs[5].AppendTopN(encodeInt(2), 40)
	topNs[5].AppendTopN(encodeInt(5), 30)

	// Input total: 200+150 + 180+120 + 90+80 + 110+70 + 50+60 + 40+30 = 1180
	var inputTotal uint64
	for _, topN := range topNs {
		inputTotal += topN.TotalCount()
	}
	require.Equal(t, uint64(1180), inputTotal)

	// globalTopN=2: only top 2 values kept, rest go to leftover.
	globalTopN, leftTopN, err := MergePartTopN2GlobalTopNV2(topNs, 2, &killer)
	require.NoError(t, err)

	var outputTotal uint64
	outputTotal += globalTopN.TotalCount()
	for _, m := range leftTopN {
		outputTotal += m.Count
	}
	require.Equal(t, inputTotal, outputTotal,
		"total count must be preserved: globalTopN(%d) + leftover(%d) = %d, want %d",
		globalTopN.TotalCount(), outputTotal-globalTopN.TotalCount(), outputTotal, inputTotal)
}

// TestMergeTopNV1V2AccuracyWithBuildStats builds partition-level statistics
// using the real BuildHistAndTopN pipeline (the same code path ANALYZE uses),
// then merges them with V1 and V2 and compares estimated counts against the
// known true counts.
//
// Data layout (4 partitions, per-partition TopN=2):
//
//	Partition 0: val1=80, val2=40, val3..10=10 each (200 rows)
//	Partition 1: val1=70, val2=45, val3..10=10 each (195 rows)
//	Partition 2: val1=8, val2=65, val3=50, val4..10=10 each (193 rows)
//	Partition 3: val1=5, val2=60, val3=55, val4..10=10 each (195 rows)
//
// Per-partition TopN (numTopN=2):
//
//	P0: {val1: 80, val2: 40}  → val1 dominant
//	P1: {val1: 70, val2: 45}  → val1 dominant
//	P2: {val2: 65, val3: 50}  → val1 rare, falls into histogram
//	P3: {val2: 60, val3: 55}  → val1 rare, falls into histogram
//
// True global counts: val1=163, val2=210, val3=125, val4..10=40 each
//
// Key differences:
//   - val1: V2 sees 80+70=150 (TopN only). V1 sees 150 + histogram estimates from P2/P3.
//   - val2: both see 40+45+65+60=210 (in all partitions' TopN).
//   - val3: V2 sees 50+55=105 (TopN only). V1 sees 105 + histogram estimates from P0/P1.
func TestMergeTopNV1V2AccuracyWithBuildStats(t *testing.T) {
	ctx := mock.NewContext()
	sc := ctx.GetSessionVars().StmtCtx
	loc := sc.TimeZone()
	killer := sqlkiller.SQLKiller{}
	tp := types.NewFieldType(mysql.TypeLonglong)

	// Define known data distribution for 4 partitions.
	partitionData := []map[int64]int64{
		{1: 80, 2: 40, 3: 10, 4: 10, 5: 10, 6: 10, 7: 10, 8: 10, 9: 10, 10: 10},
		{1: 70, 2: 45, 3: 10, 4: 10, 5: 10, 6: 10, 7: 10, 8: 10, 9: 10, 10: 10},
		{1: 8, 2: 65, 3: 50, 4: 10, 5: 10, 6: 10, 7: 10, 8: 10, 9: 10, 10: 10},
		{1: 5, 2: 60, 3: 55, 4: 10, 5: 10, 6: 10, 7: 10, 8: 10, 9: 10, 10: 10},
	}

	// Compute true global counts.
	trueCounts := make(map[int64]int64)
	var trueTotal int64
	for _, pd := range partitionData {
		for val, cnt := range pd {
			trueCounts[val] += cnt
			trueTotal += cnt
		}
	}

	const perPartTopN = 2 // TopN size used during per-partition stats building
	const globalTopN = 2  // TopN size for global merge
	const numBuckets = 10 // histogram buckets

	// Build partition stats using the real BuildHistAndTopN pipeline.
	buildPartitionStats := func() ([]*statistics.TopN, []*statistics.Histogram) {
		topNs := make([]*statistics.TopN, len(partitionData))
		hists := make([]*statistics.Histogram, len(partitionData))
		for p, pd := range partitionData {
			sketch := statistics.NewFMSketch(1000)
			var samples []*statistics.SampleItem
			for val, cnt := range pd {
				for range cnt {
					d := types.NewIntDatum(val)
					err := sketch.InsertValue(sc, d)
					require.NoError(t, err)
					samples = append(samples, &statistics.SampleItem{Value: &d})
				}
			}
			collector := &statistics.SampleCollector{
				Samples:   samples,
				NullCount: 0,
				Count:     int64(len(samples)),
				FMSketch:  sketch,
				TotalSize: int64(len(samples)) * 8,
			}
			hist, topN, err := statistics.BuildHistAndTopN(
				ctx, numBuckets, perPartTopN, 1, collector, tp, true, nil, false,
			)
			require.NoError(t, err)
			topNs[p] = topN
			hists[p] = hist
		}
		return topNs, hists
	}

	// Helper: estimate a value's count from global stats (TopN + histogram).
	estimateCount := func(globalTopN *statistics.TopN, globalHist *statistics.Histogram, val int64) float64 {
		encoded, err := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(val))
		require.NoError(t, err)
		topNCount := findTopNCountByEncoded(globalTopN, encoded)
		if topNCount > 0 {
			return float64(topNCount)
		}
		count, _ := globalHist.EqualRowCount(nil, types.NewIntDatum(val), false)
		return count
	}

	// --- V1 merge ---
	v1TopNs, v1Hists := buildPartitionStats()
	v1TopN, v1Left, v1Hists, err := MergePartTopN2GlobalTopN(
		loc, 1, v1TopNs, globalTopN, v1Hists, false, &killer,
	)
	require.NoError(t, err)
	v1GlobalHist, err := statistics.MergePartitionHist2GlobalHist(
		sc, v1Hists, v1Left, numBuckets, false, 1,
	)
	require.NoError(t, err)
	// MergePartitionHist2GlobalHist leaves NDV=0; in the real code path NDV is
	// set from the merged FMSketch (global_stats.go:356). Set it here from the
	// known true NDV so EqualRowCount doesn't return +Inf.
	globalNDV := int64(len(trueCounts))
	v1GlobalHist.NDV = globalNDV

	// --- V2 merge ---
	v2TopNs, v2Hists := buildPartitionStats()
	v2TopN, v2Left, err := MergePartTopN2GlobalTopNV2(v2TopNs, globalTopN, &killer)
	require.NoError(t, err)
	v2GlobalHist, err := statistics.MergePartitionHist2GlobalHist(
		sc, v2Hists, v2Left, numBuckets, false, 1,
	)
	require.NoError(t, err)
	v2GlobalHist.NDV = globalNDV

	// Compare per-value accuracy.
	t.Log("Value | TrueCount | V1 Estimate | V1 Error | V2 Estimate | V2 Error")
	t.Log("------|-----------|-------------|----------|-------------|--------")
	var v1SumAbsErr, v2SumAbsErr float64
	for val := int64(1); val <= 10; val++ {
		trueCount := float64(trueCounts[val])
		v1Est := estimateCount(v1TopN, v1GlobalHist, val)
		v2Est := estimateCount(v2TopN, v2GlobalHist, val)
		v1Err := v1Est - trueCount
		v2Err := v2Est - trueCount
		v1SumAbsErr += math.Abs(v1Err)
		v2SumAbsErr += math.Abs(v2Err)
		t.Logf("val=%-2d | %5.0f | %11.1f | %+8.1f | %11.1f | %+8.1f",
			val, trueCount, v1Est, v1Err, v2Est, v2Err)
	}
	t.Logf("Sum of absolute errors: V1=%.1f, V2=%.1f", v1SumAbsErr, v2SumAbsErr)

	// Total row count: TopN total + histogram total.
	v1Total := v1GlobalHist.NotNullCount()
	if v1TopN != nil {
		v1Total += float64(v1TopN.TotalCount())
	}
	v2Total := v2GlobalHist.NotNullCount()
	if v2TopN != nil {
		v2Total += float64(v2TopN.TotalCount())
	}
	t.Logf("Total rows: true=%d, V1=%.0f, V2=%.0f", trueTotal, v1Total, v2Total)

	// Both totals should be within 5% of the true total.
	require.InDelta(t, float64(trueTotal), v1Total, float64(trueTotal)*0.05,
		"V1 total should be within 5%% of true total")
	require.InDelta(t, float64(trueTotal), v2Total, float64(trueTotal)*0.05,
		"V2 total should be within 5%% of true total")

	// val2 (the globally most frequent) should be in both global TopNs.
	encoded2, err := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(2))
	require.NoError(t, err)
	require.Greater(t, findTopNCountByEncoded(v1TopN, encoded2), uint64(0),
		"val2 should be in V1 global TopN")
	require.Greater(t, findTopNCountByEncoded(v2TopN, encoded2), uint64(0),
		"val2 should be in V2 global TopN")

	// val2 appears in all partitions' TopN, so both V1 and V2 should have
	// the same (exact) count for it.
	require.Equal(t,
		findTopNCountByEncoded(v1TopN, encoded2),
		findTopNCountByEncoded(v2TopN, encoded2),
		"val2 count should match between V1 and V2")
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
