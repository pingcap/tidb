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
	"encoding/gob"
	"math"
	"math/rand"
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
	log "github.com/sirupsen/logrus"
	"github.com/spaolacci/murmur3"
)

// `feedback` represents the total scan count in range [lower, upper).
type feedback struct {
	lower  *types.Datum
	upper  *types.Datum
	count  int64
	repeat int64
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

var (
	// MaxNumberOfRanges is the max number of ranges before split to collect feedback.
	MaxNumberOfRanges = 20
	// FeedbackProbability is the probability to collect the feedback.
	FeedbackProbability = 0.0
)

// CollectFeedback decides whether to collect the feedback. It returns false when:
// 1: the histogram is nil or has no buckets;
// 2: the number of scan ranges exceeds the limit because it may affect the performance;
// 3: it does not pass the probabilistic sampler.
func (q *QueryFeedback) CollectFeedback(numOfRanges int) bool {
	if q.hist == nil || q.hist.Len() == 0 {
		q.Invalidate()
		return false
	}
	if numOfRanges > MaxNumberOfRanges || rand.Float64() > FeedbackProbability {
		q.Invalidate()
		return false
	}
	return true
}

// StoreRanges stores the ranges for update.
func (q *QueryFeedback) StoreRanges(ranges []*ranger.Range) {
	q.feedback = make([]feedback, 0, len(ranges))
	for _, ran := range ranges {
		q.feedback = append(q.feedback, feedback{&ran.LowVal[0], &ran.HighVal[0], 0, 0})
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

// Update updates the query feedback. `startKey` is the start scan key of the partial result, used to find
// the range for update. `counts` is the scan counts of each range, used to update the feedback count info.
func (q *QueryFeedback) Update(startKey kv.Key, counts []int64) {
	// Older version do not have the counts info.
	if len(counts) == 0 {
		q.Invalidate()
		return
	}
	sum := int64(0)
	for _, count := range counts {
		sum += count
	}
	metrics.DistSQLScanKeysPartialHistogram.Observe(float64(sum))
	q.actual += sum
	if !q.valid || q.hist == nil {
		return
	}

	if q.hist.tp.Tp == mysql.TypeBlob {
		startKey = tablecodec.CutIndexPrefix(startKey)
	} else {
		startKey = tablecodec.CutRowKeyPrefix(startKey)
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
		for i := 0; i < len(counts)/2; i++ {
			j := len(counts) - i - 1
			counts[i], counts[j] = counts[j], counts[i]
		}
	}
	// Update the feedback count info.
	for i, count := range counts {
		if i+idx >= len(q.feedback) {
			q.Invalidate()
			break
		}
		q.feedback[i+idx].count += count
	}
	return
}

// BucketFeedback stands for all the feedback for a bucket.
type BucketFeedback struct {
	feedback []feedback   // All the feedback info in the same bucket.
	lower    *types.Datum // The lower bound of the new bucket.
	upper    *types.Datum // The upper bound of the new bucket.
	scalar   scalar       // The scalar info for the boundary.
}

// buildBucketFeedback build the feedback for each bucket from the histogram feedback.
func buildBucketFeedback(h *Histogram, feedback *QueryFeedback) (map[int]*BucketFeedback, int) {
	bktID2FB := make(map[int]*BucketFeedback)
	total := 0
	for _, ran := range feedback.feedback {
		idx, _ := h.Bounds.LowerBound(0, ran.lower)
		bktIdx := 0
		// The last bucket also stores the feedback that falls outside the upper bound.
		if idx >= h.Bounds.NumRows()-2 {
			bktIdx = h.Len() - 1
		} else {
			bktIdx = idx / 2
			// Make sure that this feedback lies within the bucket.
			if chunk.Compare(h.Bounds.GetRow(2*bktIdx+1), 0, ran.upper) < 0 {
				continue
			}
		}
		total++
		bkt := bktID2FB[bktIdx]
		if bkt == nil {
			bkt = &BucketFeedback{lower: h.GetLower(bktIdx), upper: h.GetUpper(bktIdx)}
			bktID2FB[bktIdx] = bkt
		}
		bkt.feedback = append(bkt.feedback, ran)
		// Update the bound if necessary.
		res, err := bkt.lower.CompareDatum(nil, ran.lower)
		if err != nil {
			log.Debugf("compare datum %v with %v failed, err: %v", bkt.lower, ran.lower, errors.ErrorStack(err))
			continue
		}
		if res > 0 {
			bkt.lower = ran.lower
		}
		res, err = bkt.upper.CompareDatum(nil, ran.upper)
		if err != nil {
			log.Debugf("compare datum %v with %v failed, err: %v", bkt.upper, ran.upper, errors.ErrorStack(err))
			continue
		}
		if res < 0 {
			bkt.upper = ran.upper
		}
	}
	return bktID2FB, total
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
		log.Debugf("sort datums failed, err: %v", errors.ErrorStack(err))
		vals = vals[:0]
		vals = append(vals, *b.lower, *b.upper)
		return vals
	}
	total, interval := 0, len(vals)/num
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
			log.Debugf("compare datum %v with %v failed, err: %v", vals[total-1], vals[i], errors.ErrorStack(err))
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
		value := lowerVal.GetBytes()
		lower = convertBytesToScalar(value[b.scalar.commonPfxLen:])
		value = upperVal.GetBytes()
		upper = convertBytesToScalar(value[b.scalar.commonPfxLen:])
	} else {
		lower = float64(lowerVal.GetInt64())
		upper = float64(upperVal.GetInt64())
	}
	return calcFraction(b.scalar.lower, b.scalar.upper, upper) - calcFraction(b.scalar.lower, b.scalar.upper, lower)
}

func (b *BucketFeedback) getBucketCount(count float64) int64 {
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
	// Use the feedback that covers most to update this bucket's count. We only consider feedback that covers at
	// least minBucketFraction.
	maxFraction := minBucketFraction
	for _, fb := range b.feedback {
		fraction := b.getFraction(fb.lower, fb.upper)
		if fraction >= maxFraction {
			maxFraction = fraction
			count = float64(fb.count) / fraction
		}
	}
	return int64(count)
}

// updateBucket split the bucket according to feedback.
func (b *BucketFeedback) splitBucket(newBktNum int, totalCount float64, count float64) []bucket {
	// do not split if the count is already too small.
	if newBktNum <= 1 || count < minBucketFraction*totalCount {
		bkt := bucket{lower: b.lower, upper: b.upper, count: int64(count)}
		return []bucket{bkt}
	}
	// Split the bucket.
	bounds := b.getBoundaries(newBktNum)
	bkts := make([]bucket, 0, len(bounds)-1)
	for i := 1; i < len(bounds); i++ {
		newCount := int64(count * b.getFraction(&bounds[i-1], &bounds[i]))
		// do not split if the count of result bucket is too small.
		if float64(newCount) < minBucketFraction*totalCount {
			bounds[i] = bounds[i-1]
			continue
		}
		bkts = append(bkts, bucket{lower: &bounds[i-1], upper: bounds[i].Copy(), count: newCount, repeat: 0})
		// To guarantee that each bucket's range will not overlap.
		if bounds[i].Kind() == types.KindBytes {
			bounds[i].SetBytes(kv.Key(bounds[i].GetBytes()).PrefixNext())
		} else if bounds[i].Kind() == types.KindInt64 {
			bounds[i].SetInt64(bounds[i].GetInt64() + 1)
		} else if bounds[i].Kind() == types.KindUint64 {
			bounds[i].SetUint64(bounds[i].GetUint64() + 1)
		}
	}
	return bkts
}

// Get the split count for the histogram.
func getSplitCount(count, remainBuckets int) int {
	remainBuckets = mathutil.Max(remainBuckets, 10)
	// Split more if have more buckets available.
	return mathutil.Min(remainBuckets, count/10)
}

type bucketScore struct {
	id    int
	score float64
}

type bucketScores []bucketScore

func (bs bucketScores) Len() int           { return len(bs) }
func (bs bucketScores) Swap(i, j int)      { bs[i], bs[j] = bs[j], bs[i] }
func (bs bucketScores) Less(i, j int) bool { return bs[i].score < bs[j].score }

const (
	// To avoid the histogram been too imbalanced, we constrain the count of a bucket in range
	// [minBucketFraction * totalCount, maxBucketFraction * totalCount].
	minBucketFraction = 1 / 10000.0
	maxBucketFraction = 1 / 10.0
)

// getBucketScore gets the score for merge this bucket with previous one.
// TODO: We also need to consider the bucket hit count.
func getBucketScore(bkts []bucket, totalCount float64, id int) bucketScore {
	preCount, count := float64(bkts[id-1].count), float64(bkts[id].count)
	// do not merge if the result bucket is too large
	if (preCount + count) > maxBucketFraction*totalCount {
		return bucketScore{id, math.MaxFloat64}
	}
	// merge them if the result bucket is already too small.
	if (preCount + count) < minBucketFraction*totalCount {
		return bucketScore{id, 0}
	}
	low, mid, high := bkts[id-1].lower, bkts[id-1].upper, bkts[id].upper
	var lowVal, midVal, highVal float64
	if low.Kind() == types.KindBytes {
		common := commonPrefixLength(low.GetBytes(), high.GetBytes())
		lowVal = convertBytesToScalar(low.GetBytes()[common:])
		midVal = convertBytesToScalar(mid.GetBytes()[common:])
		highVal = convertBytesToScalar(high.GetBytes()[common:])
	} else {
		lowVal, midVal, highVal = float64(low.GetInt64()), float64(mid.GetInt64()), float64(high.GetInt64())
	}
	// If we choose to merge, err is the absolute estimate error for the previous bucket.
	err := calcFraction(lowVal, highVal, midVal)*(preCount+count) - preCount
	return bucketScore{id, math.Abs(err / (preCount + count))}
}

// defaultBucketCount is the number of buckets a column histogram has.
var defaultBucketCount = 256

func mergeBuckets(bkts []bucket, isNewBuckets []bool, totalCount float64) []bucket {
	mergeCount := len(bkts) - defaultBucketCount
	if mergeCount <= 0 {
		return bkts
	}
	bs := make(bucketScores, 0, len(bkts))
	for i := 1; i < len(bkts); i++ {
		// Do not merge the newly created buckets.
		if !isNewBuckets[i] && !isNewBuckets[i-1] {
			bs = append(bs, getBucketScore(bkts, totalCount, i))
		}
	}
	sort.Sort(bs)
	ids := make([]int, 0, mergeCount)
	for i := 0; i < mergeCount; i++ {
		ids = append(ids, bs[i].id)
	}
	sort.Ints(ids)
	idCursor, bktCursor := 0, 0
	for i := range bkts {
		// Merge this bucket with last one.
		if idCursor < mergeCount && ids[idCursor] == i {
			bkts[bktCursor-1].upper = bkts[i].upper
			bkts[bktCursor-1].count += bkts[i].count
			bkts[bktCursor-1].repeat = bkts[i].repeat
			idCursor++
		} else {
			bkts[bktCursor] = bkts[i]
			bktCursor++
		}
	}
	bkts = bkts[:bktCursor]
	return bkts
}

func splitBuckets(h *Histogram, feedback *QueryFeedback) ([]bucket, []bool, int64) {
	bktID2FB, fbNum := buildBucketFeedback(h, feedback)
	counts := make([]int64, 0, h.Len())
	for i := 0; i < h.Len(); i++ {
		bkt, ok := bktID2FB[i]
		if !ok {
			counts = append(counts, h.bucketCount(i))
		} else {
			counts = append(counts, bkt.getBucketCount(float64(h.bucketCount(i))))
		}
	}
	totCount := int64(0)
	for _, count := range counts {
		totCount += count
	}
	buckets := make([]bucket, 0, h.Len())
	isNewBuckets := make([]bool, 0, h.Len())
	splitCount := getSplitCount(fbNum, defaultBucketCount-h.Len())
	for i := 0; i < h.Len(); i++ {
		bkt, ok := bktID2FB[i]
		// No feedback, just use the original one.
		if !ok {
			buckets = append(buckets, bucket{h.GetLower(i), h.GetUpper(i), counts[i], h.Buckets[i].Repeat})
			isNewBuckets = append(isNewBuckets, false)
			continue
		}
		bkts := bkt.splitBucket(splitCount*len(bkt.feedback)/fbNum, float64(totCount), float64(counts[i]))
		buckets = append(buckets, bkts...)
		if len(bkts) == 1 {
			isNewBuckets = append(isNewBuckets, false)
		} else {
			for i := 0; i < len(bkts); i++ {
				isNewBuckets = append(isNewBuckets, true)
			}
		}
	}
	return buckets, isNewBuckets, totCount
}

// UpdateHistogram updates the histogram according buckets.
func UpdateHistogram(h *Histogram, feedback *QueryFeedback) *Histogram {
	buckets, isNewBuckets, totalCount := splitBuckets(h, feedback)
	buckets = mergeBuckets(buckets, isNewBuckets, float64(totalCount))
	return buildNewHistogram(h, buckets)
}

func buildNewHistogram(h *Histogram, buckets []bucket) *Histogram {
	hist := NewHistogram(h.ID, h.NDV, h.NullCount, h.LastUpdateVersion, h.tp, len(buckets), h.TotColSize)
	preCount := int64(0)
	for _, bkt := range buckets {
		hist.AppendBucket(bkt.lower, bkt.upper, bkt.count+preCount, bkt.repeat)
		preCount += bkt.count
	}
	return hist
}

// queryFeedback is used to serialize the QueryFeedback.
type queryFeedback struct {
	IntRanges   []int64
	HashValues  []uint64 // HashValues is the murmur hash values for each index point.
	IndexRanges [][]byte
	Counts      []int64 // Counts is the number of scan keys in each range.
}

func encodePKFeedback(q *QueryFeedback) (*queryFeedback, error) {
	pb := &queryFeedback{}
	for _, fb := range q.feedback {
		// There is no need to update the point queries.
		if bytes.Compare(kv.Key(fb.lower.GetBytes()).PrefixNext(), fb.upper.GetBytes()) >= 0 {
			continue
		}
		_, low, err := codec.DecodeInt(fb.lower.GetBytes())
		if err != nil {
			return nil, errors.Trace(err)
		}
		_, high, err := codec.DecodeInt(fb.upper.GetBytes())
		if err != nil {
			return nil, errors.Trace(err)
		}
		pb.IntRanges = append(pb.IntRanges, low, high)
		pb.Counts = append(pb.Counts, fb.count)
	}
	return pb, nil
}

func encodeIndexFeedback(q *QueryFeedback) *queryFeedback {
	pb := &queryFeedback{}
	var pointCounts []int64
	for _, fb := range q.feedback {
		if bytes.Equal(kv.Key(fb.lower.GetBytes()).PrefixNext(), fb.upper.GetBytes()) {
			h1, h2 := murmur3.Sum128(fb.lower.GetBytes())
			pb.HashValues = append(pb.HashValues, h1, h2)
			pointCounts = append(pointCounts, fb.count)
		} else {
			pb.IndexRanges = append(pb.IndexRanges, fb.lower.GetBytes(), fb.upper.GetBytes())
			pb.Counts = append(pb.Counts, fb.count)
		}
	}
	pb.Counts = append(pb.Counts, pointCounts...)
	return pb
}

func encodeFeedback(q *QueryFeedback) ([]byte, error) {
	var pb *queryFeedback
	var err error
	if q.hist.tp.Tp == mysql.TypeBlob {
		pb = encodeIndexFeedback(q)
	} else {
		pb, err = encodePKFeedback(q)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err = enc.Encode(pb)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return buf.Bytes(), nil
}

func decodeFeedback(val []byte, q *QueryFeedback, c *CMSketch) error {
	buf := bytes.NewBuffer(val)
	dec := gob.NewDecoder(buf)
	pb := &queryFeedback{}
	err := dec.Decode(pb)
	if err != nil {
		return errors.Trace(err)
	}
	// decode feedback for index
	if len(pb.IndexRanges) > 0 {
		// decode the index range feedback
		for i := 0; i < len(pb.IndexRanges); i += 2 {
			lower, upper := types.NewBytesDatum(pb.IndexRanges[i]), types.NewBytesDatum(pb.IndexRanges[i+1])
			q.feedback = append(q.feedback, feedback{&lower, &upper, pb.Counts[i/2], 0})
		}
		if c == nil {
			return nil
		}
		// decode the index point feedback, just set value count in CM Sketch
		start := len(pb.IndexRanges) / 2
		for i := 0; i < len(pb.HashValues); i += 2 {
			c.setValue(pb.HashValues[i], pb.HashValues[i+1], uint32(pb.Counts[start+i/2]))
		}
		return nil
	}
	// decode feedback for primary key
	for i := 0; i < len(pb.IntRanges); i += 2 {
		lower, upper := types.NewIntDatum(pb.IntRanges[i]), types.NewIntDatum(pb.IntRanges[i+1])
		q.feedback = append(q.feedback, feedback{&lower, &upper, pb.Counts[i/2], 0})
	}
	return nil
}

// Equal tests if two query feedback equal, it is only used in test.
func (q *QueryFeedback) Equal(rq *QueryFeedback) bool {
	if len(q.feedback) != len(rq.feedback) {
		return false
	}
	for i, fb := range q.feedback {
		rfb := rq.feedback[i]
		if fb.count != rfb.count {
			return false
		}
		if fb.lower.Kind() == types.KindInt64 {
			if fb.lower.GetInt64() != rfb.lower.GetInt64() {
				return false
			}
			if fb.upper.GetInt64() != rfb.upper.GetInt64() {
				return false
			}
		} else {
			if bytes.Compare(fb.lower.GetBytes(), rfb.lower.GetBytes()) != 0 {
				return false
			}
			if bytes.Compare(fb.upper.GetBytes(), rfb.upper.GetBytes()) != 0 {
				return false
			}
		}
	}
	return true
}
