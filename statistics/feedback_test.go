// Copyright 2018 PingCAP, Inc.
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

package statistics

import (
	"bytes"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func newFeedback(lower, upper, count, ndv int64) Feedback {
	low, upp := types.NewIntDatum(lower), types.NewIntDatum(upper)
	return Feedback{&low, &upp, count, 0, ndv}
}

func genFeedbacks(lower, upper int64) []Feedback {
	var feedbacks []Feedback
	for i := lower; i < upper; i++ {
		feedbacks = append(feedbacks, newFeedback(i, upper, upper-i+1, upper-i+1))
	}
	return feedbacks
}

func appendBucket(h *Histogram, l, r int64) {
	lower, upper := types.NewIntDatum(l), types.NewIntDatum(r)
	h.AppendBucketWithNDV(&lower, &upper, 0, 0, 0)
}

func genHistogram() *Histogram {
	h := NewHistogram(0, 0, 0, 0, types.NewFieldType(mysql.TypeLong), 5, 0)
	appendBucket(h, 1, 1)
	appendBucket(h, 2, 3)
	appendBucket(h, 5, 7)
	appendBucket(h, 10, 20)
	appendBucket(h, 30, 50)
	return h
}

func TestUpdateHistogram(t *testing.T) {
	feedbacks := []Feedback{
		newFeedback(0, 1, 10000, 1),
		newFeedback(1, 2, 1, 1),
		newFeedback(2, 3, 3, 1),
		newFeedback(4, 5, 2, 1),
		newFeedback(5, 7, 4, 1),
	}
	feedbacks = append(feedbacks, genFeedbacks(8, 20)...)
	feedbacks = append(feedbacks, genFeedbacks(21, 60)...)

	q := NewQueryFeedback(0, genHistogram(), 0, false)
	q.Feedback = feedbacks
	require.Equal(t,
		"column:0 ndv:10053 totColSize:0\n"+
			"num: 10001 lower_bound: 0 upper_bound: 2 repeats: 0 ndv: 2\n"+
			"num: 7 lower_bound: 2 upper_bound: 5 repeats: 0 ndv: 2\n"+
			"num: 4 lower_bound: 5 upper_bound: 7 repeats: 0 ndv: 1\n"+
			"num: 11 lower_bound: 10 upper_bound: 20 repeats: 0 ndv: 11\n"+
			"num: 19 lower_bound: 30 upper_bound: 49 repeats: 0 ndv: 19\n"+
			"num: 11 lower_bound: 50 upper_bound: 60 repeats: 0 ndv: 11",
		UpdateHistogramWithBucketCount(q.Hist, q, Version2, 7).ToString(0))
}

func TestSplitBuckets(t *testing.T) {
	// test bucket split
	feedbacks := []Feedback{newFeedback(0, 1, 1, 1)}
	for i := 0; i < 100; i++ {
		feedbacks = append(feedbacks, newFeedback(10, 15, 5, 5))
	}
	q := NewQueryFeedback(0, genHistogram(), 0, false)
	q.Feedback = feedbacks
	oldCnts := make([]int64, q.Hist.Len())
	for i := range q.Hist.Buckets {
		oldCnts[i] = q.Hist.bucketCount(i)
	}
	oldNdvs := make([]int64, q.Hist.Len())
	for i := range q.Hist.Buckets {
		oldNdvs[i] = q.Hist.Buckets[i].NDV
	}
	log.Warn("in test", zap.Int64s("ndvs", oldNdvs), zap.Int64s("cnts", oldCnts))
	buckets, isNewBuckets, totalCount := splitBuckets(q.Hist, q, defaultBucketCount)
	ndvs := make([]int64, len(buckets))
	for i := range buckets {
		ndvs[i] = buckets[i].Ndv
	}
	log.Warn("in test", zap.Int64s("ndvs", ndvs))
	require.Equal(t,
		"column:0 ndv:0 totColSize:0\n"+
			"num: 1 lower_bound: 0 upper_bound: 1 repeats: 0 ndv: 1\n"+
			"num: 0 lower_bound: 2 upper_bound: 3 repeats: 0 ndv: 0\n"+
			"num: 0 lower_bound: 5 upper_bound: 7 repeats: 0 ndv: 0\n"+
			"num: 5 lower_bound: 10 upper_bound: 15 repeats: 0 ndv: 5\n"+
			"num: 0 lower_bound: 16 upper_bound: 20 repeats: 0 ndv: 0\n"+
			"num: 0 lower_bound: 30 upper_bound: 50 repeats: 0 ndv: 0",
		buildNewHistogram(q.Hist, buckets).ToString(0))
	require.Equal(t, []bool{false, false, false, true, true, false}, isNewBuckets)
	require.Equal(t, int64(6), totalCount)

	// test do not split if the bucket count is too small
	feedbacks = []Feedback{newFeedback(0, 1, 100000, 1)}
	for i := 0; i < 100; i++ {
		feedbacks = append(feedbacks, newFeedback(10, 15, 1, 1))
	}
	q = NewQueryFeedback(0, genHistogram(), 0, false)
	q.Feedback = feedbacks
	buckets, isNewBuckets, totalCount = splitBuckets(q.Hist, q, defaultBucketCount)
	require.Equal(t,
		"column:0 ndv:0 totColSize:0\n"+
			"num: 100000 lower_bound: 0 upper_bound: 1 repeats: 0 ndv: 1\n"+
			"num: 0 lower_bound: 2 upper_bound: 3 repeats: 0 ndv: 0\n"+
			"num: 0 lower_bound: 5 upper_bound: 7 repeats: 0 ndv: 0\n"+
			"num: 1 lower_bound: 10 upper_bound: 15 repeats: 0 ndv: 1\n"+
			"num: 0 lower_bound: 16 upper_bound: 20 repeats: 0 ndv: 0\n"+
			"num: 0 lower_bound: 30 upper_bound: 50 repeats: 0 ndv: 0",
		buildNewHistogram(q.Hist, buckets).ToString(0))
	require.Equal(t, []bool{false, false, false, true, true, false}, isNewBuckets)
	require.Equal(t, int64(100001), totalCount)

	// test do not split if the result bucket count is too small
	h := NewHistogram(0, 0, 0, 0, types.NewFieldType(mysql.TypeLong), 5, 0)
	appendBucket(h, 0, 1000000)
	h.Buckets[0].Count = 1000000
	h.Buckets[0].NDV = 1000000
	feedbacks = feedbacks[:0]
	for i := 0; i < 100; i++ {
		feedbacks = append(feedbacks, newFeedback(0, 10, 1, 1))
	}
	q = NewQueryFeedback(0, h, 0, false)
	q.Feedback = feedbacks
	buckets, isNewBuckets, totalCount = splitBuckets(q.Hist, q, defaultBucketCount)
	require.Equal(t,
		"column:0 ndv:0 totColSize:0\n"+
			"num: 1000000 lower_bound: 0 upper_bound: 1000000 repeats: 0 ndv: 1000000",
		buildNewHistogram(q.Hist, buckets).ToString(0))
	require.Equal(t, []bool{false}, isNewBuckets)
	require.Equal(t, int64(1000000), totalCount)

	// test split even if the feedback range is too small
	h = NewHistogram(0, 0, 0, 0, types.NewFieldType(mysql.TypeLong), 5, 0)
	appendBucket(h, 0, 1000000)
	feedbacks = feedbacks[:0]
	for i := 0; i < 100; i++ {
		feedbacks = append(feedbacks, newFeedback(0, 10, 1, 1))
	}
	q = NewQueryFeedback(0, h, 0, false)
	q.Feedback = feedbacks
	buckets, isNewBuckets, totalCount = splitBuckets(q.Hist, q, defaultBucketCount)
	require.Equal(t,
		"column:0 ndv:0 totColSize:0\n"+
			"num: 1 lower_bound: 0 upper_bound: 10 repeats: 0 ndv: 1\n"+
			"num: 0 lower_bound: 11 upper_bound: 1000000 repeats: 0 ndv: 0",
		buildNewHistogram(q.Hist, buckets).ToString(0))
	require.Equal(t, []bool{true, true}, isNewBuckets)
	require.Equal(t, int64(1), totalCount)

	// test merge the non-overlapped feedbacks.
	h = NewHistogram(0, 0, 0, 0, types.NewFieldType(mysql.TypeLong), 5, 0)
	appendBucket(h, 0, 10000)
	feedbacks = feedbacks[:0]
	feedbacks = append(feedbacks, newFeedback(0, 4000, 4000, 4000))
	feedbacks = append(feedbacks, newFeedback(4001, 9999, 1000, 1000))
	q = NewQueryFeedback(0, h, 0, false)
	q.Feedback = feedbacks
	buckets, isNewBuckets, totalCount = splitBuckets(q.Hist, q, defaultBucketCount)
	require.Equal(t,
		"column:0 ndv:0 totColSize:0\n"+
			"num: 5001 lower_bound: 0 upper_bound: 10000 repeats: 0 ndv: 5001",
		buildNewHistogram(q.Hist, buckets).ToString(0))
	require.Equal(t, []bool{false}, isNewBuckets)
	require.Equal(t, int64(5001), totalCount)
}

func TestMergeBuckets(t *testing.T) {
	tests := []struct {
		points       []int64
		counts       []int64
		ndvs         []int64
		isNewBuckets []bool
		bucketCount  int
		result       string
	}{
		{
			points:       []int64{1, 2},
			counts:       []int64{1},
			ndvs:         []int64{1},
			isNewBuckets: []bool{false},
			bucketCount:  1,
			result:       "column:0 ndv:0 totColSize:0\nnum: 1 lower_bound: 1 upper_bound: 2 repeats: 0 ndv: 1",
		},
		{
			points:       []int64{1, 2, 2, 3, 3, 4},
			counts:       []int64{100000, 1, 1},
			ndvs:         []int64{1, 1, 1},
			isNewBuckets: []bool{false, false, false},
			bucketCount:  2,
			result: "column:0 ndv:0 totColSize:0\n" +
				"num: 100000 lower_bound: 1 upper_bound: 2 repeats: 0 ndv: 1\n" +
				"num: 2 lower_bound: 2 upper_bound: 4 repeats: 0 ndv: 2",
		},
		// test do not Merge if the result bucket count is too large
		{
			points:       []int64{1, 2, 2, 3, 3, 4, 4, 5},
			counts:       []int64{1, 1, 100000, 100000},
			ndvs:         []int64{1, 1, 1, 1},
			isNewBuckets: []bool{false, false, false, false},
			bucketCount:  3,
			result: "column:0 ndv:0 totColSize:0\n" +
				"num: 2 lower_bound: 1 upper_bound: 3 repeats: 0 ndv: 2\n" +
				"num: 100000 lower_bound: 3 upper_bound: 4 repeats: 0 ndv: 1\n" +
				"num: 100000 lower_bound: 4 upper_bound: 5 repeats: 0 ndv: 1",
		},
	}
	for _, tt := range tests {
		require.Equal(t, len(tt.ndvs), len(tt.counts))
		bkts := make([]bucket, 0, len(tt.counts))
		totalCount := int64(0)
		for i := 0; i < len(tt.counts); i++ {
			lower, upper := types.NewIntDatum(tt.points[2*i]), types.NewIntDatum(tt.points[2*i+1])
			bkts = append(bkts, bucket{&lower, &upper, tt.counts[i], 0, tt.ndvs[i]})
			totalCount += tt.counts[i]
		}
		bkts = mergeBuckets(bkts, tt.isNewBuckets, tt.bucketCount, float64(totalCount))
		result := buildNewHistogram(&Histogram{Tp: types.NewFieldType(mysql.TypeLong)}, bkts).ToString(0)
		require.Equal(t, tt.result, result)
	}
}

func encodeInt(v int64) *types.Datum {
	val := codec.EncodeInt(nil, v)
	d := types.NewBytesDatum(val)
	return &d
}

func TestFeedbackEncoding(t *testing.T) {
	hist := NewHistogram(0, 0, 0, 0, types.NewFieldType(mysql.TypeLong), 0, 0)
	q := &QueryFeedback{Hist: hist, Tp: PkType}
	q.Feedback = append(q.Feedback, Feedback{encodeInt(0), encodeInt(3), 1, 0, 1})
	q.Feedback = append(q.Feedback, Feedback{encodeInt(0), encodeInt(5), 1, 0, 1})
	val, err := EncodeFeedback(q)
	require.NoError(t, err)
	rq := &QueryFeedback{}
	require.NoError(t, DecodeFeedback(val, rq, nil, nil, hist.Tp))
	for _, fb := range rq.Feedback {
		fb.Lower.SetBytes(codec.EncodeInt(nil, fb.Lower.GetInt64()))
		fb.Upper.SetBytes(codec.EncodeInt(nil, fb.Upper.GetInt64()))
	}
	require.True(t, q.Equal(rq))

	hist.Tp = types.NewFieldType(mysql.TypeBlob)
	q = &QueryFeedback{Hist: hist}
	q.Feedback = append(q.Feedback, Feedback{encodeInt(0), encodeInt(3), 1, 0, 1})
	q.Feedback = append(q.Feedback, Feedback{encodeInt(0), encodeInt(1), 1, 0, 1})
	val, err = EncodeFeedback(q)
	require.NoError(t, err)
	rq = &QueryFeedback{}
	cms := NewCMSketch(4, 4)
	require.NoError(t, DecodeFeedback(val, rq, cms, nil, hist.Tp))
	require.Equal(t, uint64(1), cms.QueryBytes(codec.EncodeInt(nil, 0)))
	q.Feedback = q.Feedback[:1]
	require.True(t, q.Equal(rq))
}

// Equal tests if two query feedback equal, it is only used in test.
func (q *QueryFeedback) Equal(rq *QueryFeedback) bool {
	if len(q.Feedback) != len(rq.Feedback) {
		return false
	}
	for i, fb := range q.Feedback {
		rfb := rq.Feedback[i]
		if fb.Count != rfb.Count {
			return false
		}
		if fb.Ndv != rfb.Ndv {
			return false
		}
		if fb.Lower.Kind() == types.KindInt64 {
			if fb.Lower.GetInt64() != rfb.Lower.GetInt64() {
				return false
			}
			if fb.Upper.GetInt64() != rfb.Upper.GetInt64() {
				return false
			}
		} else {
			if !bytes.Equal(fb.Lower.GetBytes(), rfb.Lower.GetBytes()) {
				return false
			}
			if !bytes.Equal(fb.Upper.GetBytes(), rfb.Upper.GetBytes()) {
				return false
			}
		}
	}
	return true
}
