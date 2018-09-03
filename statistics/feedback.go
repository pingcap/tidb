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
	"fmt"
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/cznic/mathutil"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
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

// DecodeToRanges decode the feedback to ranges.
func (q *QueryFeedback) DecodeToRanges(isIndex bool) ([]*ranger.Range, error) {
	ranges := make([]*ranger.Range, 0, len(q.feedback))
	for _, val := range q.feedback {
		low, high := *val.lower, *val.upper
		var lowVal, highVal []types.Datum
		if isIndex {
			var err error
			// As we do not know the origin length, just use a custom value here.
			lowVal, err = codec.DecodeRange(low.GetBytes(), 4)
			if err != nil {
				return nil, errors.Trace(err)
			}
			highVal, err = codec.DecodeRange(high.GetBytes(), 4)
			if err != nil {
				return nil, errors.Trace(err)
			}
		} else {
			_, lowInt, err := codec.DecodeInt(val.lower.GetBytes())
			if err != nil {
				return nil, errors.Trace(err)
			}
			_, highInt, err := codec.DecodeInt(val.upper.GetBytes())
			if err != nil {
				return nil, errors.Trace(err)
			}
			lowVal = []types.Datum{types.NewIntDatum(lowInt)}
			highVal = []types.Datum{types.NewIntDatum(highInt)}
		}
		ranges = append(ranges, &(ranger.Range{
			LowVal:      lowVal,
			HighVal:     highVal,
			HighExclude: true,
		}))
	}
	return ranges, nil
}

func (q *QueryFeedback) decodeIntValues() *QueryFeedback {
	nq := &QueryFeedback{}
	nq.feedback = make([]feedback, 0, len(q.feedback))
	for _, fb := range q.feedback {
		_, lowInt, err := codec.DecodeInt(fb.lower.GetBytes())
		if err != nil {
			log.Debugf("decode feedback lower bound \"%v\" to integer failed: %v", fb.lower.GetBytes(), err)
			continue
		}
		_, highInt, err := codec.DecodeInt(fb.upper.GetBytes())
		if err != nil {
			log.Debugf("decode feedback upper bound \"%v\" to integer failed: %v", fb.upper.GetBytes(), err)
			continue
		}
		low, high := types.NewIntDatum(lowInt), types.NewIntDatum(highInt)
		nq.feedback = append(nq.feedback, feedback{lower: &low, upper: &high, count: fb.count})
	}
	return nq
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

	if q.hist.isIndexHist() {
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

// There are only two types of datum in bucket: one is `Blob`, which is for index; the other one
// is `Int`, which is for primary key.
type bucket = feedback

// splitBucket firstly splits this "BucketFeedback" to "newNumBkts" new buckets,
// calculates the count for each new bucket, merge the new bucket whose count
// is smaller than "minBucketFraction*totalCount" with the next new bucket
// until the last new bucket.
func (b *BucketFeedback) splitBucket(newNumBkts int, totalCount float64, originBucketCount float64) []bucket {
	// Split the bucket.
	bounds := b.getBoundaries(newNumBkts + 1)
	bkts := make([]bucket, 0, len(bounds)-1)
	for i := 1; i < len(bounds); i++ {
		newBkt := bucket{&bounds[i-1], bounds[i].Copy(), 0, 0}
		// get bucket count
		_, ratio := getOverlapFraction(feedback{b.lower, b.upper, int64(originBucketCount), 0}, newBkt)
		countInNewBkt := originBucketCount * ratio
		countInNewBkt = b.refineBucketCount(newBkt, countInNewBkt)
		// do not split if the count of result bucket is too small.
		if countInNewBkt < minBucketFraction*totalCount {
			bounds[i] = bounds[i-1]
			continue
		}
		newBkt.count = int64(countInNewBkt)
		bkts = append(bkts, newBkt)
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

func getFraction4PK(minValue, maxValue, lower, upper *types.Datum) (float64, float64) {
	if minValue.Kind() == types.KindInt64 {
		l, r := float64(minValue.GetInt64()), float64(maxValue.GetInt64())
		return calcFraction(l, r, float64(lower.GetInt64())), calcFraction(l, r, float64(upper.GetInt64()))
	}
	l, r := float64(minValue.GetUint64()), float64(maxValue.GetUint64())
	return calcFraction(l, r, float64(lower.GetUint64())), calcFraction(l, r, float64(upper.GetUint64()))
}

func getFraction4Index(minValue, maxValue, lower, upper *types.Datum, prefixLen int) (float64, float64) {
	l, r := convertBytesToScalar(minValue.GetBytes()[prefixLen:]), convertBytesToScalar(maxValue.GetBytes()[prefixLen:])
	return calcFraction(l, r, convertBytesToScalar(lower.GetBytes()[prefixLen:])),
		calcFraction(l, r, convertBytesToScalar(upper.GetBytes()[prefixLen:]))
}

// getOverlapFraction gets the overlap fraction of feedback and bucket range. In order to get the bucket count, it also
// returns the ratio between bucket fraction and feedback fraction.
func getOverlapFraction(fb feedback, bkt bucket) (float64, float64) {
	datums := make([]types.Datum, 0, 4)
	datums = append(datums, *fb.lower, *fb.upper)
	datums = append(datums, *bkt.lower, *bkt.upper)
	err := types.SortDatums(nil, datums)
	if err != nil {
		return 0, 0
	}
	var fbLower, fbUpper, bktLower, bktUpper float64
	minValue, maxValue := &datums[0], &datums[3]
	if datums[0].Kind() == types.KindBytes {
		prefixLen := commonPrefixLength(minValue.GetBytes(), maxValue.GetBytes())
		fbLower, fbUpper = getFraction4Index(minValue, maxValue, fb.lower, fb.upper, prefixLen)
		bktLower, bktUpper = getFraction4Index(minValue, maxValue, bkt.lower, bkt.upper, prefixLen)
	} else {
		fbLower, fbUpper = getFraction4PK(minValue, maxValue, fb.lower, fb.upper)
		bktLower, bktUpper = getFraction4PK(minValue, maxValue, bkt.lower, bkt.upper)
	}
	ratio := (bktUpper - bktLower) / (fbUpper - fbLower)
	// full overlap
	if fbLower <= bktLower && bktUpper <= fbUpper {
		return bktUpper - bktLower, ratio
	}
	if bktLower <= fbLower && fbUpper <= bktUpper {
		return fbUpper - fbLower, ratio
	}
	// partial overlap
	overlap := math.Min(bktUpper-fbLower, fbUpper-bktLower)
	return overlap, ratio
}

// refineBucketCount refine the newly split bucket count. It uses the feedback that overlaps most
// with the bucket to get the bucket count.
func (b *BucketFeedback) refineBucketCount(bkt bucket, defaultCount float64) float64 {
	bestFraction := minBucketFraction
	count := defaultCount
	for _, fb := range b.feedback {
		fraction, ratio := getOverlapFraction(fb, bkt)
		// choose the max overlap fraction
		if fraction > bestFraction {
			bestFraction = fraction
			count = float64(fb.count) * ratio
		}
	}
	return count
}

const (
	defaultSplitCount = 10
	splitPerFeedback  = 10
)

// getSplitCount gets the split count for the histogram. It is based on the intuition that:
// 1: If we have more remaining unused buckets, we can split more.
// 2: We cannot split too aggressive, thus we make it split every `splitPerFeedback`.
func getSplitCount(numFeedbacks, remainBuckets int) int {
	// Split more if have more buckets available.
	splitCount := mathutil.Max(remainBuckets, defaultSplitCount)
	return mathutil.Min(splitCount, numFeedbacks/splitPerFeedback)
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

// splitBuckets split the histogram buckets according to the feedback.
func splitBuckets(h *Histogram, feedback *QueryFeedback) ([]bucket, []bool, int64) {
	bktID2FB, numTotalFBs := buildBucketFeedback(h, feedback)
	buckets := make([]bucket, 0, h.Len())
	isNewBuckets := make([]bool, 0, h.Len())
	splitCount := getSplitCount(numTotalFBs, defaultBucketCount-h.Len())
	for i := 0; i < h.Len(); i++ {
		bktFB, ok := bktID2FB[i]
		// No feedback, just use the original one.
		if !ok {
			buckets = append(buckets, bucket{h.GetLower(i), h.GetUpper(i), h.bucketCount(i), h.Buckets[i].Repeat})
			isNewBuckets = append(isNewBuckets, false)
			continue
		}
		// Distribute the total split count to bucket based on number of bucket feedback.
		newBktNums := splitCount * len(bktFB.feedback) / numTotalFBs
		bkts := bktFB.splitBucket(newBktNums, h.totalRowCount(), float64(h.bucketCount(i)))
		buckets = append(buckets, bkts...)
		if len(bkts) == 1 {
			isNewBuckets = append(isNewBuckets, false)
		} else {
			for i := 0; i < len(bkts); i++ {
				isNewBuckets = append(isNewBuckets, true)
			}
		}
	}
	totCount := int64(0)
	for _, bkt := range buckets {
		totCount += bkt.count
	}
	return buckets, isNewBuckets, totCount
}

// UpdateHistogram updates the histogram according buckets.
func UpdateHistogram(h *Histogram, feedback *QueryFeedback) *Histogram {
	buckets, isNewBuckets, totalCount := splitBuckets(h, feedback)
	buckets = mergeBuckets(buckets, isNewBuckets, float64(totalCount))
	return buildNewHistogram(h, buckets)
}

// UpdateCMSketch updates the CMSketch by feedback.
func UpdateCMSketch(c *CMSketch, eqFeedbacks []feedback) *CMSketch {
	if c == nil || len(eqFeedbacks) == 0 {
		return c
	}
	newCMSketch := c.copy()
	for _, fb := range eqFeedbacks {
		h1, h2 := murmur3.Sum128(fb.lower.GetBytes())
		newCMSketch.setValue(h1, h2, uint32(fb.count))
	}
	return newCMSketch
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
	if q.hist.isIndexHist() {
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
	// decode the index range feedback
	for i := 0; i < len(pb.IndexRanges); i += 2 {
		lower, upper := types.NewBytesDatum(pb.IndexRanges[i]), types.NewBytesDatum(pb.IndexRanges[i+1])
		q.feedback = append(q.feedback, feedback{&lower, &upper, pb.Counts[i/2], 0})
	}
	if c != nil {
		// decode the index point feedback, just set value count in CM Sketch
		start := len(pb.IndexRanges) / 2
		for i := 0; i < len(pb.HashValues); i += 2 {
			c.setValue(pb.HashValues[i], pb.HashValues[i+1], uint32(pb.Counts[start+i/2]))
		}
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

// recalculateExpectCount recalculates the expect row count if the origin row count is estimated by pseudo.
func (q *QueryFeedback) recalculateExpectCount(h *Handle) error {
	t, ok := h.statsCache.Load().(statsCache)[q.tableID]
	if !ok {
		return nil
	}
	tablePseudo := t.Pseudo || t.IsOutdated()
	if tablePseudo == false {
		return nil
	}
	isIndex := q.hist.tp.Tp == mysql.TypeBlob
	id := q.hist.ID
	if isIndex && (t.Indices[id] == nil || t.Indices[id].NotAccurate() == false) {
		return nil
	}
	if !isIndex && (t.Columns[id] == nil || t.Columns[id].NotAccurate() == false) {
		return nil
	}

	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	ranges, err := q.DecodeToRanges(isIndex)
	if err != nil {
		return errors.Trace(err)
	}
	expected := 0.0
	if isIndex {
		idx := t.Indices[id]
		expected, err = idx.getRowCount(sc, ranges, t.ModifyCount)
		expected *= idx.getIncreaseFactor(t.Count)
	} else {
		c := t.Columns[id]
		expected, err = c.getColumnRowCount(sc, ranges, t.ModifyCount)
		expected *= c.getIncreaseFactor(t.Count)
	}
	if err != nil {
		return errors.Trace(err)
	}
	q.expected = int64(expected)
	return nil
}

// splitFeedback splits the feedbacks into equality feedbacks and range feedbacks.
func splitFeedbackByQueryType(feedbacks []feedback) ([]feedback, []feedback) {
	var eqFB, ranFB []feedback
	for _, fb := range feedbacks {
		// Use `>=` here because sometimes the lower is equal to upper.
		if bytes.Compare(kv.Key(fb.lower.GetBytes()).PrefixNext(), fb.upper.GetBytes()) >= 0 {
			eqFB = append(eqFB, fb)
		} else {
			ranFB = append(ranFB, fb)
		}
	}
	return eqFB, ranFB
}

// formatBuckets formats bucket from lowBkt to highBkt.
func formatBuckets(hg *Histogram, lowBkt, highBkt, idxCols int) string {
	if lowBkt == highBkt {
		return hg.bucketToString(lowBkt, idxCols)
	}
	if lowBkt+1 == highBkt {
		return fmt.Sprintf("%s, %s", hg.bucketToString(lowBkt, 0), hg.bucketToString(highBkt, 0))
	}
	// do not care the middle buckets
	return fmt.Sprintf("%s, (%d buckets, total count %d), %s", hg.bucketToString(lowBkt, 0),
		highBkt-lowBkt-1, hg.Buckets[highBkt-1].Count-hg.Buckets[lowBkt].Count, hg.bucketToString(highBkt, 0))
}

func colRangeToStr(c *Column, ran *ranger.Range, actual int64, factor float64) string {
	lowCount, lowBkt := c.lessRowCountWithBktIdx(ran.LowVal[0])
	highCount, highBkt := c.lessRowCountWithBktIdx(ran.HighVal[0])
	return fmt.Sprintf("range: %s, actual: %d, expected: %d, buckets: {%s}", ran.String(), actual,
		int64((highCount-lowCount)*factor), formatBuckets(&c.Histogram, lowBkt, highBkt, 0))
}

func logForPK(prefix string, c *Column, ranges []*ranger.Range, actual []int64, factor float64) {
	for i, ran := range ranges {
		if ran.LowVal[0].GetInt64()+1 >= ran.HighVal[0].GetInt64() {
			continue
		}
		log.Debugf("%s column: %s, %s", prefix, c.Info.Name, colRangeToStr(c, ran, actual[i], factor))
	}
}

func logForIndexRange(idx *Index, ran *ranger.Range, actual int64, factor float64) string {
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	lb, err := codec.EncodeKey(sc, nil, ran.LowVal...)
	if err != nil {
		return ""
	}
	rb, err := codec.EncodeKey(sc, nil, ran.HighVal...)
	if err != nil {
		return ""
	}
	if idx.CMSketch != nil && bytes.Compare(kv.Key(lb).PrefixNext(), rb) >= 0 {
		str, err := types.DatumsToString(ran.LowVal, true)
		if err != nil {
			return ""
		}
		return fmt.Sprintf("value: %s, actual: %d, expected: %d", str, actual, int64(float64(idx.QueryBytes(lb))*factor))
	}
	l, r := types.NewBytesDatum(lb), types.NewBytesDatum(rb)
	lowCount, lowBkt := idx.lessRowCountWithBktIdx(l)
	highCount, highBkt := idx.lessRowCountWithBktIdx(r)
	return fmt.Sprintf("range: %s, actual: %d, expected: %d, histogram: {%s}", ran.String(), actual,
		int64((highCount-lowCount)*factor), formatBuckets(&idx.Histogram, lowBkt, highBkt, len(idx.Info.Columns)))
}

func logForIndex(prefix string, t *Table, idx *Index, ranges []*ranger.Range, actual []int64, factor float64) {
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	if idx.CMSketch == nil || idx.statsVer != version1 {
		for i, ran := range ranges {
			log.Debugf("%s index: %s, %s", prefix, idx.Info.Name.O, logForIndexRange(idx, ran, actual[i], factor))
		}
		return
	}
	for i, ran := range ranges {
		rangePosition := getOrdinalOfRangeCond(sc, ran)
		// only contains range or equality query
		if rangePosition == 0 || rangePosition == len(ran.LowVal) {
			log.Debugf("%s index: %s, %s", prefix, idx.Info.Name.O, logForIndexRange(idx, ran, actual[i], factor))
			continue
		}
		equalityString, err := types.DatumsToString(ran.LowVal[:rangePosition], true)
		if err != nil {
			continue
		}
		bytes, err := codec.EncodeKey(sc, nil, ran.LowVal[:rangePosition]...)
		if err != nil {
			continue
		}
		equalityCount := idx.CMSketch.QueryBytes(bytes)
		rang := ranger.Range{
			LowVal:  []types.Datum{ran.LowVal[rangePosition]},
			HighVal: []types.Datum{ran.HighVal[rangePosition]},
		}
		colName := idx.Info.Columns[rangePosition].Name.L
		// prefer index stats over column stats
		if idxHist := t.indexStartWithColumnForDebugLog(colName); idxHist != nil && idxHist.Histogram.Len() > 0 {
			rangeString := logForIndexRange(idxHist, &rang, -1, factor)
			log.Debugf("%s index: %s, actual: %d, equality: %s, expected equality: %d, %s", prefix, idx.Info.Name.O,
				actual[i], equalityString, equalityCount, rangeString)
		} else if colHist := t.columnByNameForDebugLog(colName); colHist != nil && colHist.Histogram.Len() > 0 {
			rangeString := colRangeToStr(colHist, &rang, -1, factor)
			log.Debugf("%s index: %s, actual: %d, equality: %s, expected equality: %d, %s", prefix, idx.Info.Name.O,
				actual[i], equalityString, equalityCount, rangeString)
		} else {
			count, err := getPseudoRowCountByColumnRanges(sc, float64(t.Count), []*ranger.Range{&rang}, 0)
			if err == nil {
				log.Debugf("%s index: %s, actual: %d, equality: %s, expected equality: %d, range: %s, pseudo count: %.0f", prefix, idx.Info.Name.O,
					actual[i], equalityString, equalityCount, rang.String(), count)
			}
		}
	}
}

func (q *QueryFeedback) logDetailedInfo(h *Handle) {
	t, ok := h.statsCache.Load().(statsCache)[q.tableID]
	if !ok {
		return
	}
	isIndex := q.hist.isIndexHist()
	ranges, err := q.DecodeToRanges(isIndex)
	if err != nil {
		log.Debug(err)
		return
	}
	actual := make([]int64, 0, len(q.feedback))
	for _, fb := range q.feedback {
		actual = append(actual, fb.count)
	}
	logPrefix := fmt.Sprintf("[stats-feedback] %s,", t.name)
	if isIndex {
		idx := t.Indices[q.hist.ID]
		if idx == nil {
			return
		}
		logForIndex(logPrefix, t, idx, ranges, actual, idx.getIncreaseFactor(t.Count))
	} else {
		c := t.Columns[q.hist.ID]
		if c == nil {
			return
		}
		logForPK(logPrefix, c, ranges, actual, c.getIncreaseFactor(t.Count))
	}
}
