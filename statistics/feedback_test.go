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
// See the License for the specific language governing permissions and
// limitations under the License.

package statistics

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
)

var _ = Suite(&testFeedbackSuite{})

type testFeedbackSuite struct {
}

func newFeedback(lower, upper, count int64) feedback {
	low, upp := types.NewIntDatum(lower), types.NewIntDatum(upper)
	return feedback{&low, &upp, count, 0}
}

func genFeedbacks(lower, upper int64) []feedback {
	var feedbacks []feedback
	for i := lower; i < upper; i++ {
		feedbacks = append(feedbacks, newFeedback(i, upper, upper-i+1))
	}
	return feedbacks
}

func appendBucket(h *Histogram, l, r int64) {
	lower, upper := types.NewIntDatum(l), types.NewIntDatum(r)
	h.AppendBucket(&lower, &upper, 0, 0)
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

func (s *testFeedbackSuite) TestUpdateHistogram(c *C) {
	feedbacks := []feedback{
		newFeedback(0, 1, 10000),
		newFeedback(1, 2, 1),
		newFeedback(2, 3, 3),
		newFeedback(4, 5, 2),
		newFeedback(5, 7, 4),
	}
	feedbacks = append(feedbacks, genFeedbacks(8, 20)...)
	feedbacks = append(feedbacks, genFeedbacks(21, 60)...)

	q := NewQueryFeedback(0, genHistogram(), 0, false)
	q.feedback = feedbacks
	originBucketCount := defaultBucketCount
	defaultBucketCount = 5
	defer func() { defaultBucketCount = originBucketCount }()
	c.Assert(UpdateHistogram(q.Hist(), []*QueryFeedback{q}).ToString(0), Equals,
		"column:0 ndv:0 totColSize:0\n"+
			"num: 10000\tlower_bound: 0\tupper_bound: 1\trepeats: 0\n"+
			"num: 10003\tlower_bound: 2\tupper_bound: 3\trepeats: 0\n"+
			"num: 10021\tlower_bound: 4\tupper_bound: 20\trepeats: 0\n"+
			"num: 10046\tlower_bound: 21\tupper_bound: 46\trepeats: 0\n"+
			"num: 10060\tlower_bound: 46\tupper_bound: 60\trepeats: 0")
}

func (s *testFeedbackSuite) TestSplitBuckets(c *C) {
	// test bucket split
	feedbacks := []feedback{newFeedback(0, 1, 1)}
	for i := 0; i < 100; i++ {
		feedbacks = append(feedbacks, newFeedback(10, 15, 1))
	}
	q := NewQueryFeedback(0, genHistogram(), 0, false)
	q.feedback = feedbacks
	buckets, isNewBuckets, totalCount := splitBuckets(q.Hist(), []*QueryFeedback{q})
	c.Assert(buildNewHistogram(q.Hist(), buckets).ToString(0), Equals,
		"column:0 ndv:0 totColSize:0\n"+
			"num: 1\tlower_bound: 0\tupper_bound: 1\trepeats: 0\n"+
			"num: 1\tlower_bound: 2\tupper_bound: 3\trepeats: 0\n"+
			"num: 1\tlower_bound: 5\tupper_bound: 7\trepeats: 0\n"+
			"num: 2\tlower_bound: 10\tupper_bound: 15\trepeats: 0\n"+
			"num: 3\tlower_bound: 15\tupper_bound: 20\trepeats: 0\n"+
			"num: 3\tlower_bound: 30\tupper_bound: 50\trepeats: 0")
	c.Assert(isNewBuckets, DeepEquals, []bool{false, false, false, true, true, false})
	c.Assert(totalCount, Equals, int64(3))

	// test do not split if the bucket count is too small
	feedbacks = []feedback{newFeedback(0, 1, 100000)}
	for i := 0; i < 100; i++ {
		feedbacks = append(feedbacks, newFeedback(10, 15, 1))
	}
	q = NewQueryFeedback(0, genHistogram(), 0, false)
	q.feedback = feedbacks
	buckets, isNewBuckets, totalCount = splitBuckets(q.Hist(), []*QueryFeedback{q})
	c.Assert(buildNewHistogram(q.Hist(), buckets).ToString(0), Equals,
		"column:0 ndv:0 totColSize:0\n"+
			"num: 100000\tlower_bound: 0\tupper_bound: 1\trepeats: 0\n"+
			"num: 100000\tlower_bound: 2\tupper_bound: 3\trepeats: 0\n"+
			"num: 100000\tlower_bound: 5\tupper_bound: 7\trepeats: 0\n"+
			"num: 100002\tlower_bound: 10\tupper_bound: 20\trepeats: 0\n"+
			"num: 100002\tlower_bound: 30\tupper_bound: 50\trepeats: 0")
	c.Assert(isNewBuckets, DeepEquals, []bool{false, false, false, false, false})
	c.Assert(totalCount, Equals, int64(100002))

	// test do not split if the result bucket count is too small
	h := NewHistogram(0, 0, 0, 0, types.NewFieldType(mysql.TypeLong), 5, 0)
	appendBucket(h, 0, 1000000)
	feedbacks = feedbacks[:0]
	for i := 0; i < 100; i++ {
		feedbacks = append(feedbacks, newFeedback(0, 1, 1))
	}
	q = NewQueryFeedback(0, h, 0, false)
	q.feedback = feedbacks
	buckets, isNewBuckets, totalCount = splitBuckets(q.Hist(), []*QueryFeedback{q})
	c.Assert(buildNewHistogram(q.Hist(), buckets).ToString(0), Equals,
		"column:0 ndv:0 totColSize:0\n"+
			"num: 1000000\tlower_bound: 0\tupper_bound: 1000000\trepeats: 0")
	c.Assert(isNewBuckets, DeepEquals, []bool{false})
	c.Assert(totalCount, Equals, int64(1000000))
}

func (s *testFeedbackSuite) TestMergeBuckets(c *C) {
	originBucketCount := defaultBucketCount
	defer func() { defaultBucketCount = originBucketCount }()
	tests := []struct {
		points       []int64
		counts       []int64
		isNewBuckets []bool
		bucketCount  int
		result       string
	}{
		{
			points:       []int64{1, 2},
			counts:       []int64{1},
			isNewBuckets: []bool{false},
			bucketCount:  1,
			result:       "column:0 ndv:0 totColSize:0\nnum: 1\tlower_bound: 1\tupper_bound: 2\trepeats: 0",
		},
		{
			points:       []int64{1, 2, 2, 3, 3, 4},
			counts:       []int64{100000, 1, 1},
			isNewBuckets: []bool{false, false, false},
			bucketCount:  2,
			result: "column:0 ndv:0 totColSize:0\n" +
				"num: 100000\tlower_bound: 1\tupper_bound: 2\trepeats: 0\n" +
				"num: 100002\tlower_bound: 2\tupper_bound: 4\trepeats: 0",
		},
		// test do not merge if the result bucket count is too large
		{
			points:       []int64{1, 2, 2, 3, 3, 4, 4, 5},
			counts:       []int64{1, 1, 100000, 100000},
			isNewBuckets: []bool{false, false, false, false},
			bucketCount:  3,
			result: "column:0 ndv:0 totColSize:0\n" +
				"num: 2\tlower_bound: 1\tupper_bound: 3\trepeats: 0\n" +
				"num: 100002\tlower_bound: 3\tupper_bound: 4\trepeats: 0\n" +
				"num: 200002\tlower_bound: 4\tupper_bound: 5\trepeats: 0",
		},
	}
	for _, t := range tests {
		bkts := make([]bucket, 0, len(t.counts))
		totalCount := int64(0)
		for i := 0; i < len(t.counts); i++ {
			lower, upper := types.NewIntDatum(t.points[2*i]), types.NewIntDatum(t.points[2*i+1])
			bkts = append(bkts, bucket{&lower, &upper, t.counts[i], 0})
			totalCount += t.counts[i]
		}
		defaultBucketCount = t.bucketCount
		bkts = mergeBuckets(bkts, t.isNewBuckets, float64(totalCount))
		result := buildNewHistogram(&Histogram{tp: types.NewFieldType(mysql.TypeLong)}, bkts).ToString(0)
		c.Assert(result, Equals, t.result)
	}
}
