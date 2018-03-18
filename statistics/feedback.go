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
	"bytes"
	"math"
	"sort"

	"github.com/cznic/mathutil"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/ranger"
)

// `feedback` represents the total scan count in range [lower, upper).
type feedback struct {
	lower *types.Datum
	upper *types.Datum
	count int64
}

// QueryFeedback is used to represent the query feedback info. It contains the query's scan ranges and number of rows
// in each range.
type QueryFeedback struct {
	tableID  int64
	hist     *Histogram
	feedback []feedback
	expected int64 // expected is the expected scan count of corresponding query.
	actual   int64 // actual is the actual scan count of corresponding query.
	valid    bool  // valid represents the whether this query feedback is still valid.
	desc     bool  // desc represents the corresponding query is desc scan.
}

// NewQueryFeedback returns a new query feedback.
func NewQueryFeedback(tableID int64, hist *Histogram, expected int64, desc bool) *QueryFeedback {
	if hist != nil && hist.Len() == 0 {
		hist = nil
	}
	return &QueryFeedback{
		tableID:  tableID,
		valid:    true,
		hist:     hist,
		expected: expected,
		desc:     desc,
	}
}

// StoreRanges stores the ranges for update.
func (q *QueryFeedback) StoreRanges(ranges []*ranger.NewRange) {
	q.feedback = make([]feedback, 0, len(ranges))
	for _, ran := range ranges {
		q.feedback = append(q.feedback, feedback{&ran.LowVal[0], &ran.HighVal[0], 0})
	}
}

// Invalidate is used to invalidate the query feedback.
func (q *QueryFeedback) Invalidate() {
	q.feedback = nil
	q.hist = nil
	q.valid = false
	q.actual = -1
}

// Actual gets the actual row count.
func (q *QueryFeedback) Actual() int64 {
	if !q.valid {
		return -1
	}
	return q.actual
}

// Hist gets the histogram.
func (q *QueryFeedback) Hist() *Histogram {
	return q.hist
}

// Counts returns the counts info for each range. It is only used in test.
func (q *QueryFeedback) Counts() []int64 {
	counts := make([]int64, 0, len(q.feedback))
	for _, fb := range q.feedback {
		counts = append(counts, fb.count)
	}
	return counts
}

// Update updates the query feedback. `startKey` is the start scan key of the partial result, used to find
// the range for update. `counts` is the scan counts of each range, used to update the feedback count info.
func (q *QueryFeedback) Update(startKey kv.Key, counts []int64) {
	// Older version do not have the counts info.
	if len(counts) == 0 {
		q.Invalidate()
		return
	}
	length := len(counts)
	// The `counts` was the output count of each push down executor.
	if counts[length-1] != -1 {
		metrics.DistSQLScanKeysPartialHistogram.Observe(float64(counts[0]))
		q.actual += counts[0]
		return
	}
	// The counts is the scan count of each range now.
	sum := int64(0)
	rangeCounts := counts[:length-1]
	for _, count := range rangeCounts {
		sum += count
	}
	metrics.DistSQLScanKeysPartialHistogram.Observe(float64(sum))
	q.actual += sum
	if !q.valid || q.hist == nil {
		return
	}

	if q.hist.tp.Tp == mysql.TypeLong {
		startKey = tablecodec.CutRowKeyPrefix(startKey)
	} else {
		startKey = tablecodec.CutIndexPrefix(startKey)
	}
	// Find the range that startKey falls in.
	idx := sort.Search(len(q.feedback), func(i int) bool {
		return bytes.Compare(q.feedback[i].lower.GetBytes(), startKey) > 0
	})
	idx--
	if idx < 0 {
		return
	}
	// If the desc is true, the counts is reversed, so here we need to reverse it back.
	if q.desc {
		for i := 0; i < len(rangeCounts)/2; i++ {
			j := len(rangeCounts) - i - 1
			rangeCounts[i], rangeCounts[j] = rangeCounts[j], rangeCounts[i]
		}
	}
	// Update the feedback count info.
	for i, count := range rangeCounts {
		if i+idx >= len(q.feedback) {
			q.Invalidate()
			break
		}
		q.feedback[i+idx].count += count
	}
	return
}

// DecodeInt decodes the int value stored in the feedback, only used for test.
func (q *QueryFeedback) DecodeInt() error {
	for _, fb := range q.feedback {
		_, v, err := codec.DecodeInt(fb.lower.GetBytes())
		if err != nil {
			return errors.Trace(err)
		}
		fb.lower.SetInt64(v)
		_, v, err = codec.DecodeInt(fb.upper.GetBytes())
		if err != nil {
			return errors.Trace(err)
		}
		fb.upper.SetInt64(v)
	}
	return nil
}

// BucketFeedback stands for all the feedback for a bucket.
type BucketFeedback struct {
	feedback []feedback   // All the feedback info in the same bucket.
	lower    *types.Datum // The lower bound of the new bucket.
	upper    *types.Datum // The upper bound of the new bucket.
	scalar   scalar       // The scalar info for the boundary.
}

// buildBucketFeedback build the feedback for each bucket from the histogram feedback.
func buildBucketFeedback(h *Histogram, feedbacks []*QueryFeedback) (map[int]*BucketFeedback, int) {
	bkts := make(map[int]*BucketFeedback)
	total := 0
	for _, feedback := range feedbacks {
		for _, ran := range feedback.feedback {
			idx, _ := h.Bounds.LowerBound(0, ran.lower)
			bktIdx := 0
			// The last bucket also stores the feedback that falls outside the upper bound.
			if idx >= h.Bounds.NumRows()-2 {
				bktIdx = h.Len() - 1
			} else {
				bktIdx = idx / 2
				// Make sure that this feedback lies within the bucket.
				if chunk.Compare(h.Bounds.GetRow(2*bktIdx+1), 0, ran.upper) <= 0 {
					continue
				}
			}
			total++
			bkt := bkts[bktIdx]
			if bkt == nil {
				bkt = &BucketFeedback{lower: h.GetLower(bktIdx), upper: h.GetUpper(bktIdx)}
				bkts[bktIdx] = bkt
			}
			bkt.feedback = append(bkt.feedback, ran)
			// Update the bound if necessary.
			res, err := bkt.lower.CompareDatum(nil, ran.lower)
			if err != nil {
				continue
			}
			if res > 0 {
				bkt.lower = ran.lower
			}
			res, err = bkt.upper.CompareDatum(nil, ran.upper)
			if err != nil {
				continue
			}
			if res < 0 {
				bkt.upper = ran.upper
			}
		}
	}
	return bkts, total
}

// getBoundaries gets the new boundaries after split.
func (b *BucketFeedback) getBoundaries(num int) []types.Datum {
	// Get all the possible new boundaries.
	vals := make([]types.Datum, 0, len(b.feedback)*2+2)
	for _, fb := range b.feedback {
		vals = append(vals, *fb.lower, *fb.upper)
	}
	vals = append(vals, *b.lower)
	err := types.SortDatums(nil, vals)
	if err != nil {
		vals = vals[:0]
		vals = append(vals, *b.lower, *b.upper)
		return vals
	}
	total, interval := len(vals)/num, 0
	// Pick values per `interval`.
	for i := 0; i < len(vals); i, total = i+interval, total+1 {
		vals[total] = vals[i]
	}
	// Append the upper bound.
	vals[total] = *b.upper
	vals = vals[:total+1]
	total = 1
	// Erase the repeat values.
	for i := 1; i < len(vals); i++ {
		cmp, err := vals[total-1].CompareDatum(nil, &vals[i])
		if err != nil {
			continue
		}
		if cmp == 0 {
			continue
		}
		vals[total] = vals[i]
		total++
	}
	return vals[:total]
}

type bucket = feedback

// Get the fraction of the [lowerVal, upperVal] that intersect with the bucket boundary.
func (b *BucketFeedback) getFraction(lowerVal, upperVal *types.Datum) float64 {
	var lower, upper float64
	if b.lower.Kind() == types.KindBytes {
		bytes := lowerVal.GetBytes()
		lower = convertBytesToScalar(bytes[b.scalar.commonPfxLen:])
		bytes = upperVal.GetBytes()
		upper = convertBytesToScalar(bytes[b.scalar.commonPfxLen:])
	} else {
		lower = float64(lowerVal.GetInt64())
		upper = float64(upperVal.GetInt64())
	}
	return calcFraction(b.scalar.lower, b.scalar.upper, upper) - calcFraction(b.scalar.lower, b.scalar.upper, lower)
}

// updateBucket updates and split the bucket according to feedback.
func (b *BucketFeedback) updateBucket(newBktCount int) []bucket {
	// Get the scalar info for boundary.
	prefixLen := commonPrefixLength(b.lower.GetBytes(), b.upper.GetBytes())
	if b.lower.Kind() == types.KindBytes {
		b.scalar.commonPfxLen = commonPrefixLength(b.lower.GetBytes(), b.upper.GetBytes())
		b.scalar.lower = convertBytesToScalar(b.lower.GetBytes()[prefixLen:])
		b.scalar.upper = convertBytesToScalar(b.upper.GetBytes()[prefixLen:])
	} else {
		b.scalar.lower = float64(b.lower.GetInt64())
		b.scalar.upper = float64(b.upper.GetInt64())
	}
	// Use the feedback that covers most to update this bucket's count.
	var maxFraction, count float64
	for _, fb := range b.feedback {
		fraction := b.getFraction(fb.lower, fb.upper)
		if fraction > maxFraction {
			maxFraction = fraction
			count = float64(fb.count) / fraction
		}
	}
	if newBktCount == 1 {
		bkt := bucket{lower: b.lower, upper: b.upper, count: int64(count)}
		return []bucket{bkt}
	}
	// Split the bucket.
	bounds := b.getBoundaries(newBktCount)
	bkts := make([]bucket, len(bounds)-1)
	for i := 1; i < len(bounds); i++ {
		bkt := bucket{lower: &bounds[i-1], upper: &bounds[i]}
		bkt.count = int64(count * b.getFraction(bkt.lower, bkt.upper))
		if bkt.count == 0 {
			bounds[i] = bounds[i-1]
			continue
		}
		bkts = append(bkts, bkt)
	}
	return bkts
}

// Get the split count for the histogram.
func getSplitCount(count int) int {
	return mathutil.Min(10, count/10)
}

type bucketScore struct {
	id    int
	score float64
}

type bucketScores []bucketScore

func (bs bucketScores) Len() int           { return len(bs) }
func (bs bucketScores) Swap(i, j int)      { bs[i], bs[j] = bs[j], bs[i] }
func (bs bucketScores) Less(i, j int) bool { return bs[i].score < bs[j].score }

// getBucketScore gets the score for merge this bucket with previous one.
// TODO: We also need to consider the bucket hit count.
func getBucketScore(h *Histogram, id int) bucketScore {
	if id == 0 {
		return bucketScore{id, math.MaxFloat64} // do not merge the first one
	}
	low, mid, high := h.GetLower(id-1), h.GetUpper(id-1), h.GetUpper(id)
	var lowVal, midVal, highVal float64
	if low.Kind() == types.KindBytes {
		common := commonPrefixLength(low.GetBytes(), high.GetBytes())
		lowVal = convertBytesToScalar(low.GetBytes()[common:])
		midVal = convertBytesToScalar(mid.GetBytes()[common:])
		highVal = convertBytesToScalar(high.GetBytes()[common:])
	} else {
		lowVal, midVal, highVal = float64(low.GetInt64()), float64(mid.GetInt64()), float64(high.GetInt64())
	}
	preCount, count := float64(h.bucketCount(id-1)), float64(h.bucketCount(id))
	// If we choose to merge, err is the absolute estimate error for the previous bucket.
	err := calcFraction(lowVal, highVal, midVal)*(preCount+count) - preCount
	return bucketScore{id, math.Abs(err / (preCount + count))}
}

func mergeBuckets(h *Histogram, bs bucketScores) *Histogram {
	mergeCount := h.Len() - defaultBucketCount
	if mergeCount <= 0 {
		return h
	}
	sort.Sort(bs)
	ids := make([]int, mergeCount)
	for i := 0; i < mergeCount; i++ {
		ids = append(ids, bs[i].id)
	}
	sort.Ints(ids)
	newHist := NewHistogram(h.ID, h.NDV, h.NullCount, h.LastUpdateVersion, h.tp, h.Len())
	for i, cur := 0, 0; i < h.Len(); i++ {
		// Merge this bucket with last one.
		if cur < mergeCount && ids[cur] == i {
			newHist.updateLastBucket(h.GetUpper(i), h.Buckets[i].Count, h.Buckets[i].Repeat)
			cur++
			continue
		}
		newHist.AppendBucket(h.GetLower(i), h.GetUpper(i), h.Buckets[i].Count, h.Buckets[i].Repeat)
	}
	return newHist
}

func splitBuckets(h *Histogram, feedbacks []*QueryFeedback) (*Histogram, bucketScores) {
	bktFB, count := buildBucketFeedback(h, feedbacks)
	newHist := NewHistogram(h.ID, h.NDV, h.NullCount, h.LastUpdateVersion, h.tp, h.Len())
	preCount, splitCount := int64(0), getSplitCount(count)
	bucketScores := make([]bucketScore, 0, h.Len())
	for i := 0; i < h.Len(); i++ {
		bkt, ok := bktFB[i]
		// No feedback, just use the origin bucket.
		if !ok {
			newHist.AppendBucket(h.GetLower(i), h.GetUpper(i), preCount+h.bucketCount(i), h.Buckets[i].Repeat)
			bucketScores = append(bucketScores, getBucketScore(newHist, newHist.Len()-1))
			preCount += h.bucketCount(i)
			continue
		}
		// Split this bucket.
		newBuckets := bkt.updateBucket(mathutil.Max(1, splitCount*len(bkt.feedback)/count))
		for _, newBucket := range newBuckets {
			newHist.AppendBucket(newBucket.lower, newBucket.upper, preCount+newBucket.count, 0)
			preCount += newBucket.count
		}
		// Do not merge the newly created buckets.
		if len(newBuckets) == 1 {
			bucketScores = append(bucketScores, getBucketScore(newHist, newHist.Len()-1))
		}
	}
	return newHist, bucketScores
}

// UpdateHistogram updates the histogram according buckets.
func UpdateHistogram(h *Histogram, feedbacks []*QueryFeedback) *Histogram {
	newHist, bucketScores := splitBuckets(h, feedbacks)
	return mergeBuckets(newHist, bucketScores)
}
