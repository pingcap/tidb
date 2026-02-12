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
// Setup (4 partitions, column histogram, isIndex=false):
//
//	Partitions 0-1: TopN = {val10: 50, val20: 30}
//	Partitions 2-3: TopN = {val20: 30}; val10 is NOT in TopN
//	                Histogram: buckets [1,5] cnt=200 rep=40, [6,15] cnt=500 rep=60
//	                NDV=20, so EqualRowCount(val10) = NotNullCount/NDV = 500/20 = 25
//
//	Global TopN size = 1
//
// V1: val10 = 50*2(TopN) + 25*2(hist estimate from p2,p3) = 150
//
//	val20 = 30*4(TopN, present in all partitions) = 120
//	→ V1 picks val10 (150)
//
// V2: val10 = 50*2 = 100 (TopN only)
//
//	val20 = 30*4 = 120 (TopN only)
//	→ V2 picks val20 (120)
func TestMergeTopNV1V2ComparisonHistValueNotAtUpperBound(t *testing.T) {
	loc := time.UTC
	sc := stmtctx.NewStmtCtxWithTimeZone(loc)
	killer := sqlkiller.SQLKiller{}

	encodeInt := func(v int64) []byte {
		key, err := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(v))
		require.NoError(t, err)
		return key
	}
	keyVal10 := encodeInt(10)
	keyVal20 := encodeInt(20)

	makeHist := func() *statistics.Histogram {
		d1 := types.NewIntDatum(1)
		d5 := types.NewIntDatum(5)
		d6 := types.NewIntDatum(6)
		d15 := types.NewIntDatum(15)
		h := statistics.NewHistogram(1, 20, 0, 0, types.NewFieldType(mysql.TypeLong), chunk.InitialCapacity, 0)
		h.AppendBucket(&d1, &d5, 200, 40)
		h.AppendBucket(&d6, &d15, 500, 60)
		return h
	}

	makeInputs := func() ([]*statistics.TopN, []*statistics.Histogram) {
		topNs := make([]*statistics.TopN, 4)
		hists := make([]*statistics.Histogram, 4)
		for i := range 4 {
			topN := statistics.NewTopN(2)
			if i < 2 {
				topN.AppendTopN(keyVal10, 50)
			}
			topN.AppendTopN(keyVal20, 30)
			topNs[i] = topN
			hists[i] = makeHist()
		}
		return topNs, hists
	}

	// --- V1 merge ---
	v1TopNs, v1Hists := makeInputs()
	// V1 mutates histograms (BinarySearchRemoveVal), so it needs its own copy.
	v1TopN, v1Left, v1Hists, err := MergePartTopN2GlobalTopN(loc, 1, v1TopNs, 1, v1Hists, false, &killer)
	require.NoError(t, err)
	require.Len(t, v1TopN.TopN, 1)
	// V1 picks val10: 50*2 (TopN) + 25*2 (hist estimate) = 150
	require.Equal(t, uint64(150), findTopNCountByEncoded(v1TopN, keyVal10))
	require.Equal(t, uint64(0), findTopNCountByEncoded(v1TopN, keyVal20))
	require.Equal(t, uint64(120), findTopNMetaCountByEncoded(v1Left, keyVal20))

	// Merge histograms with V1 leftovers.
	v1GlobalHist, err := statistics.MergePartitionHist2GlobalHist(sc, v1Hists, v1Left, 100, false, 1)
	require.NoError(t, err)
	require.NotNil(t, v1GlobalHist)

	// --- V2 merge ---
	v2TopNs, v2Hists := makeInputs()
	v2TopN, v2Left, err := MergePartTopN2GlobalTopNV2(v2TopNs, 1, &killer)
	require.NoError(t, err)
	require.Len(t, v2TopN.TopN, 1)
	// V2 picks val20: 30*4 = 120 > val10: 50*2 = 100
	require.Equal(t, uint64(120), findTopNCountByEncoded(v2TopN, keyVal20))
	require.Equal(t, uint64(0), findTopNCountByEncoded(v2TopN, keyVal10))
	require.Equal(t, uint64(100), findTopNMetaCountByEncoded(v2Left, keyVal10))

	// Merge histograms with V2 leftovers. Histograms are unmodified by V2.
	v2GlobalHist, err := statistics.MergePartitionHist2GlobalHist(sc, v2Hists, v2Left, 100, false, 1)
	require.NoError(t, err)
	require.NotNil(t, v2GlobalHist)

	// V2 folds val10 (count=100) back into the histogram as an extra bucket,
	// preserving accurate counts. V1 already subtracted estimated counts from
	// histograms via BinarySearchRemoveVal, so its histogram totals are lower.
	require.Greater(t, v2GlobalHist.NotNullCount(), v1GlobalHist.NotNullCount(),
		"V2 histogram should have higher total count because V1 subtracted histogram estimates")
}

// TestMergeTopNV1V2ComparisonHistValueAtUpperBound shows how V1 picks up the
// accurate Repeat count when a TopN value happens to be at a histogram bucket's
// upper bound. This is the one scenario where V1's histogram lookup is precise.
//
// Setup (4 partitions, column histogram, isIndex=false):
//
//	Partitions 0-1: TopN = {val15: 50, val20: 35}
//	Partitions 2-3: TopN = {val20: 35}; val15 is NOT in TopN
//	                Histogram: buckets [1,5] cnt=100 rep=10, [6,15] cnt=300 rep=80
//	                val15 IS at upper bound → EqualRowCount(val15) = Repeat = 80
//
//	Global TopN size = 1
//
// V1: val15 = 50*2(TopN) + 80*2(exact repeat from p2,p3) = 260
//
//	val20 = 35*4(TopN) = 140
//	→ V1 picks val15 (260)
//
// V2: val15 = 50*2 = 100 (TopN only)
//
//	val20 = 35*4 = 140 (TopN only)
//	→ V2 picks val20 (140)
func TestMergeTopNV1V2ComparisonHistValueAtUpperBound(t *testing.T) {
	loc := time.UTC
	sc := stmtctx.NewStmtCtxWithTimeZone(loc)
	killer := sqlkiller.SQLKiller{}

	encodeInt := func(v int64) []byte {
		key, err := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(v))
		require.NoError(t, err)
		return key
	}
	keyVal15 := encodeInt(15)
	keyVal20 := encodeInt(20)

	makeHist := func() *statistics.Histogram {
		d1 := types.NewIntDatum(1)
		d5 := types.NewIntDatum(5)
		d6 := types.NewIntDatum(6)
		d15 := types.NewIntDatum(15)
		h := statistics.NewHistogram(1, 20, 0, 0, types.NewFieldType(mysql.TypeLong), chunk.InitialCapacity, 0)
		h.AppendBucket(&d1, &d5, 100, 10)
		h.AppendBucket(&d6, &d15, 300, 80)
		return h
	}

	makeInputs := func() ([]*statistics.TopN, []*statistics.Histogram) {
		topNs := make([]*statistics.TopN, 4)
		hists := make([]*statistics.Histogram, 4)
		for i := range 4 {
			topN := statistics.NewTopN(2)
			if i < 2 {
				topN.AppendTopN(keyVal15, 50)
			}
			topN.AppendTopN(keyVal20, 35)
			topNs[i] = topN
			hists[i] = makeHist()
		}
		return topNs, hists
	}

	// --- V1 merge ---
	v1TopNs, v1Hists := makeInputs()
	v1TopN, v1Left, v1Hists, err := MergePartTopN2GlobalTopN(loc, 1, v1TopNs, 1, v1Hists, false, &killer)
	require.NoError(t, err)
	require.Len(t, v1TopN.TopN, 1)
	// V1 picks val15: 50*2 (TopN) + 80*2 (exact repeat) = 260
	require.Equal(t, uint64(260), findTopNCountByEncoded(v1TopN, keyVal15))
	require.Equal(t, uint64(140), findTopNMetaCountByEncoded(v1Left, keyVal20))

	// Merge histograms with V1 leftovers.
	v1GlobalHist, err := statistics.MergePartitionHist2GlobalHist(sc, v1Hists, v1Left, 100, false, 1)
	require.NoError(t, err)
	require.NotNil(t, v1GlobalHist)

	// --- V2 merge ---
	v2TopNs, v2Hists := makeInputs()
	v2TopN, v2Left, err := MergePartTopN2GlobalTopNV2(v2TopNs, 1, &killer)
	require.NoError(t, err)
	require.Len(t, v2TopN.TopN, 1)
	// V2 picks val20: 35*4 = 140 > val15: 50*2 = 100
	require.Equal(t, uint64(140), findTopNCountByEncoded(v2TopN, keyVal20))
	require.Equal(t, uint64(100), findTopNMetaCountByEncoded(v2Left, keyVal15))

	// Merge histograms with V2 leftovers. Histograms are unmodified by V2.
	v2GlobalHist, err := statistics.MergePartitionHist2GlobalHist(sc, v2Hists, v2Left, 100, false, 1)
	require.NoError(t, err)
	require.NotNil(t, v2GlobalHist)

	// V1 removed exact repeat counts (80 per partition for val15) from histograms
	// via BinarySearchRemoveVal before the histogram merge. V2 leaves histograms
	// intact and folds val15 (count=100) back as a leftover bucket instead.
	require.Greater(t, v2GlobalHist.NotNullCount(), v1GlobalHist.NotNullCount(),
		"V2 histogram should have higher total count because V1 subtracted repeat counts from histograms")
}

// TestMergeTopNV1V2ManyPartitionsInflation demonstrates V1's key weakness:
// when a value is in TopN of a few partitions but falls inside histogram
// buckets of many others, V1 accumulates NotNullCount/NDV uniform estimates
// from each partition, producing a massively inflated count.
//
// Setup (20 partitions, column histogram, isIndex=false, globalTopN=1):
//
//	Partitions 0-1: TopN = {val_rare: 60, val_common: 10}
//	                val_rare is dominant in these two partitions only.
//	Partitions 2-19: TopN = {val_common: 10}
//	                val_rare is NOT in TopN but falls inside histogram range.
//	                Histogram: NDV=5, NotNullCount=500
//	                → EqualRowCount(val_rare) = 500/5 = 100 per partition
//
// True global counts:
//
//	val_rare:   60*2           = 120
//	val_common: 10*20          = 200
//
// V1: val_rare  = 60*2 + 100*18 = 1920 (16x overestimate!)
//
//	val_common = 10*20         = 200
//	→ V1 picks val_rare (1920) — WRONG choice, massively inflated
//
// V2: val_rare  = 60*2          = 120
//
//	val_common = 10*20         = 200
//	→ V2 picks val_common (200) — correct choice, exact count
func TestMergeTopNV1V2ManyPartitionsInflation(t *testing.T) {
	loc := time.UTC
	sc := stmtctx.NewStmtCtxWithTimeZone(loc)
	killer := sqlkiller.SQLKiller{}

	encodeInt := func(v int64) []byte {
		key, err := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(v))
		require.NoError(t, err)
		return key
	}
	keyRare := encodeInt(50)   // val_rare = 50, falls inside histogram range [1,100]
	keyCommon := encodeInt(200) // val_common = 200, always in TopN

	const nPartitions = 20

	// Histogram for partitions 2-19: val_rare falls inside a bucket but is not
	// at the upper bound, so EqualRowCount returns NotNullCount/NDV = 500/5 = 100.
	makeHist := func() *statistics.Histogram {
		d1 := types.NewIntDatum(1)
		d100 := types.NewIntDatum(100)
		h := statistics.NewHistogram(1, 5, 0, 0,
			types.NewFieldType(mysql.TypeLong), chunk.InitialCapacity, 0)
		h.AppendBucket(&d1, &d100, 500, 10)
		return h
	}

	makeInputs := func() ([]*statistics.TopN, []*statistics.Histogram) {
		topNs := make([]*statistics.TopN, nPartitions)
		hists := make([]*statistics.Histogram, nPartitions)
		for i := range nPartitions {
			topN := statistics.NewTopN(2)
			if i < 2 {
				topN.AppendTopN(keyRare, 60)
			}
			topN.AppendTopN(keyCommon, 10)
			topNs[i] = topN
			hists[i] = makeHist()
		}
		return topNs, hists
	}

	// --- V1 merge ---
	v1TopNs, v1Hists := makeInputs()
	v1TopN, v1Left, v1Hists, err := MergePartTopN2GlobalTopN(
		loc, 1, v1TopNs, 1, v1Hists, false, &killer,
	)
	require.NoError(t, err)
	require.Len(t, v1TopN.TopN, 1)

	// V1 picks val_rare with massively inflated count: 60*2 + 100*18 = 1920.
	v1RareCount := findTopNCountByEncoded(v1TopN, keyRare)
	require.Equal(t, uint64(1920), v1RareCount)
	require.Equal(t, uint64(0), findTopNCountByEncoded(v1TopN, keyCommon),
		"V1 wrongly excluded val_common from global TopN")

	// V1 histogram merge: histograms were mutated by BinarySearchRemoveVal.
	v1GlobalHist, err := statistics.MergePartitionHist2GlobalHist(
		sc, v1Hists, v1Left, 100, false, 1,
	)
	require.NoError(t, err)

	// --- V2 merge ---
	v2TopNs, v2Hists := makeInputs()
	v2TopN, v2Left, err := MergePartTopN2GlobalTopNV2(v2TopNs, 1, &killer)
	require.NoError(t, err)
	require.Len(t, v2TopN.TopN, 1)

	// V2 correctly picks val_common with exact count: 10*20 = 200.
	v2CommonCount := findTopNCountByEncoded(v2TopN, keyCommon)
	require.Equal(t, uint64(200), v2CommonCount)
	require.Equal(t, uint64(0), findTopNCountByEncoded(v2TopN, keyRare),
		"V2 correctly excluded val_rare from global TopN")

	// V2 histogram merge: histograms are pristine (unmodified).
	v2GlobalHist, err := statistics.MergePartitionHist2GlobalHist(
		sc, v2Hists, v2Left, 100, false, 1,
	)
	require.NoError(t, err)

	// --- Accuracy comparison ---
	const trueRare = 120  // 60*2
	const trueCommon = 200 // 10*20

	// V1 TopN error: claims val_rare=1920, true=120 → error=+1800 (16x overestimate).
	v1TopNError := math.Abs(float64(v1RareCount) - trueRare)
	require.Equal(t, float64(1800), v1TopNError)

	// V2 TopN error: claims val_common=200, true=200 → error=0 (exact).
	v2TopNError := math.Abs(float64(v2CommonCount) - trueCommon)
	require.Equal(t, float64(0), v2TopNError)

	// V2 is strictly more accurate for the global TopN value.
	require.Less(t, v2TopNError, v1TopNError,
		"V2 should have lower TopN error than V1")

	// V2 also preserves higher histogram totals because it doesn't subtract
	// phantom counts from partition histograms.
	require.Greater(t, v2GlobalHist.NotNullCount(), v1GlobalHist.NotNullCount(),
		"V2 histogram should retain more rows because V1 subtracted inflated estimates")
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

	const perPartTopN = 2  // TopN size used during per-partition stats building
	const globalTopN = 2   // TopN size for global merge
	const numBuckets = 10  // histogram buckets

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
		encoded, _ := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(val))
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
	encoded2, _ := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(2))
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
