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
	"encoding/gob"
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/ranger"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// Feedback represents the total scan count in range [lower, upper).
type Feedback struct {
	Lower  *types.Datum
	Upper  *types.Datum
	Count  int64
	Repeat int64
	Ndv    int64
}

// QueryFeedback is used to represent the query feedback info. It contains the query's scan ranges and number of rows
// in each range.
type QueryFeedback struct {
	PhysicalID int64
	Hist       *Histogram
	Tp         int
	Feedback   []Feedback
	Expected   int64 // Expected is the Expected scan count of corresponding query.
	actual     int64 // actual is the actual scan count of corresponding query.
	Valid      bool  // Valid represents the whether this query feedback is still Valid.
	desc       bool  // desc represents the corresponding query is desc scan.
}

// NewQueryFeedback returns a new query feedback.
func NewQueryFeedback(physicalID int64, hist *Histogram, expected int64, desc bool) *QueryFeedback {
	if hist != nil && hist.Len() == 0 {
		hist = nil
	}
	tp := PkType
	if hist != nil && hist.IsIndexHist() {
		tp = IndexType
	}
	return &QueryFeedback{
		PhysicalID: physicalID,
		Valid:      true,
		Tp:         tp,
		Hist:       hist,
		Expected:   expected,
		desc:       desc,
	}
}

// QueryFeedbackKey is the key for a group of feedbacks on the same index/column.
type QueryFeedbackKey struct {
	PhysicalID int64
	HistID     int64
	Tp         int
}

// QueryFeedbackMap is the collection of feedbacks.
type QueryFeedbackMap struct {
	Size      int
	Feedbacks map[QueryFeedbackKey][]*QueryFeedback
}

// NewQueryFeedbackMap builds a feedback collection.
func NewQueryFeedbackMap() *QueryFeedbackMap {
	return &QueryFeedbackMap{Feedbacks: make(map[QueryFeedbackKey][]*QueryFeedback)}
}

// Append adds a feedback into map.
func (m *QueryFeedbackMap) Append(q *QueryFeedback) {
	k := QueryFeedbackKey{
		PhysicalID: q.PhysicalID,
		HistID:     q.Hist.ID,
		Tp:         q.Tp,
	}
	m.append(k, []*QueryFeedback{q})
}

// MaxQueryFeedbackCount is the max number of feedbacks that are cached in memory.
var MaxQueryFeedbackCount = atomic.NewInt64(1 << 9)

func (m *QueryFeedbackMap) append(k QueryFeedbackKey, qs []*QueryFeedback) bool {
	remained := MaxQueryFeedbackCount.Load() - int64(m.Size)
	if remained <= 0 {
		return false
	}
	s, ok := m.Feedbacks[k]
	if !ok || s == nil {
		s = make([]*QueryFeedback, 0, 8)
	}
	l := mathutil.Min(int64(len(qs)), remained)
	s = append(s, qs[:l]...)
	m.Feedbacks[k] = s
	m.Size = m.Size + int(l)
	return true
}

// SiftFeedbacks eliminates feedbacks which are overlapped with others. It is a tradeoff between
// feedback accuracy and its overhead.
func (m *QueryFeedbackMap) SiftFeedbacks() {
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	for k, qs := range m.Feedbacks {
		fbs := make([]Feedback, 0, len(qs)*2)
		for _, q := range qs {
			fbs = append(fbs, q.Feedback...)
		}
		if len(fbs) == 0 {
			delete(m.Feedbacks, k)
			continue
		}
		m.Feedbacks[k] = m.Feedbacks[k][:1]
		m.Feedbacks[k][0].Feedback, _ = NonOverlappedFeedbacks(sc, fbs)
	}
	m.Size = len(m.Feedbacks)
}

// Merge combines 2 collections of feedbacks.
func (m *QueryFeedbackMap) Merge(r *QueryFeedbackMap) {
	for k, qs := range r.Feedbacks {
		if !m.append(k, qs) {
			break
		}
	}
}

var (
	// MaxNumberOfRanges is the max number of ranges before split to collect feedback.
	MaxNumberOfRanges = 20
	// FeedbackProbability is the probability to collect the feedback.
	FeedbackProbability = atomic.NewFloat64(0)
)

func init() {
	// This is for solving import cycle.
	// We need to read the value of FeedbackProbability when setting the variable tidb_analyze_version in sessionctx/variable package
	// but we have imported sessionctx/variable in statistics package here.
	variable.FeedbackProbability = FeedbackProbability
}

// CalcErrorRate calculates the error rate the current QueryFeedback.
func (q *QueryFeedback) CalcErrorRate() float64 {
	expected := float64(q.Expected)
	if q.actual == 0 {
		if expected == 0 {
			return 0
		}
		return 1
	}
	return math.Abs(expected-float64(q.actual)) / float64(q.actual)
}

// CollectFeedback decides whether to collect the feedback. It returns false when:
// 1: the feedback is not generated by select query;
// 2: the histogram is nil or has no buckets;
// 3: the number of scan ranges exceeds the limit because it may affect the performance;
// 4: it does not pass the probabilistic sampler.
func CollectFeedback(sc *stmtctx.StatementContext, q *QueryFeedback, numOfRanges int) bool {
	if !sc.InSelectStmt {
		return false
	}
	if q.Hist == nil || q.Hist.Len() == 0 {
		return false
	}
	// #nosec G404
	if numOfRanges > MaxNumberOfRanges || rand.Float64() > FeedbackProbability.Load() {
		return false
	}
	return true
}

// DecodeToRanges decode the feedback to ranges.
func (q *QueryFeedback) DecodeToRanges(isIndex bool) ([]*ranger.Range, error) {
	ranges := make([]*ranger.Range, 0, len(q.Feedback))
	for _, val := range q.Feedback {
		low, high := *val.Lower, *val.Upper
		var lowVal, highVal []types.Datum
		if isIndex {
			var err error
			// As we do not know the origin length, just use a custom value here.
			lowVal, _, err = codec.DecodeRange(low.GetBytes(), 4, nil, nil)
			if err != nil {
				return nil, errors.Trace(err)
			}
			highVal, _, err = codec.DecodeRange(high.GetBytes(), 4, nil, nil)
			if err != nil {
				return nil, errors.Trace(err)
			}
		} else {
			_, lowInt, err := codec.DecodeInt(val.Lower.GetBytes())
			if err != nil {
				return nil, errors.Trace(err)
			}
			_, highInt, err := codec.DecodeInt(val.Upper.GetBytes())
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
			Collators:   collate.GetBinaryCollatorSlice(len(lowVal)),
		}))
	}
	return ranges, nil
}

// DecodeIntValues is called when the current Feedback stores encoded int values.
func (q *QueryFeedback) DecodeIntValues() *QueryFeedback {
	nq := &QueryFeedback{}
	nq.Feedback = make([]Feedback, 0, len(q.Feedback))
	for _, fb := range q.Feedback {
		_, lowInt, err := codec.DecodeInt(fb.Lower.GetBytes())
		if err != nil {
			logutil.BgLogger().Debug("decode feedback lower bound value to integer failed", zap.Binary("value", fb.Lower.GetBytes()), zap.Error(err))
			continue
		}
		_, highInt, err := codec.DecodeInt(fb.Upper.GetBytes())
		if err != nil {
			logutil.BgLogger().Debug("decode feedback upper bound value to integer failed", zap.Binary("value", fb.Upper.GetBytes()), zap.Error(err))
			continue
		}
		low, high := types.NewIntDatum(lowInt), types.NewIntDatum(highInt)
		nq.Feedback = append(nq.Feedback, Feedback{Lower: &low, Upper: &high, Count: fb.Count})
	}
	return nq
}

// StoreRanges stores the ranges for update.
func (q *QueryFeedback) StoreRanges(ranges []*ranger.Range) {
	q.Feedback = make([]Feedback, 0, len(ranges))
	for _, ran := range ranges {
		q.Feedback = append(q.Feedback, Feedback{&ran.LowVal[0], &ran.HighVal[0], 0, 0, 0})
	}
}

// Invalidate is used to invalidate the query feedback.
func (q *QueryFeedback) Invalidate() {
	q.Feedback = nil
	q.Hist = nil
	q.Valid = false
	q.actual = -1
}

// Actual gets the actual row count.
func (q *QueryFeedback) Actual() int64 {
	if !q.Valid {
		return -1
	}
	return q.actual
}

// Update updates the query feedback. `startKey` is the start scan key of the partial result, used to find
// the range for update. `counts` is the scan counts of each range, used to update the feedback count info.
func (q *QueryFeedback) Update(startKey kv.Key, counts, ndvs []int64) {
	failpoint.Inject("feedbackNoNDVCollect", func() {
		ndvs = nil
	})
	// Older versions do not have the counts info.
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
	if !q.Valid || q.Hist == nil {
		return
	}

	if q.Tp == IndexType {
		startKey = tablecodec.CutIndexPrefix(startKey)
	} else {
		startKey = tablecodec.CutRowKeyPrefix(startKey)
	}
	// Find the range that startKey falls in.
	idx := sort.Search(len(q.Feedback), func(i int) bool {
		return bytes.Compare(q.Feedback[i].Lower.GetBytes(), startKey) > 0
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
		for i := 0; i < len(ndvs)/2; i++ {
			j := len(ndvs) - i - 1
			ndvs[i], ndvs[j] = ndvs[j], ndvs[i]
		}
	}
	// Update the feedback count info.
	for i, count := range counts {
		if i+idx >= len(q.Feedback) {
			q.Invalidate()
			break
		}
		q.Feedback[i+idx].Count += count
	}
	for i, ndv := range ndvs {
		q.Feedback[i+idx].Ndv += ndv
	}
}

// NonOverlappedFeedbacks extracts a set of feedbacks which are not overlapped with each other.
func NonOverlappedFeedbacks(sc *stmtctx.StatementContext, fbs []Feedback) ([]Feedback, bool) {
	// Sort feedbacks by end point and start point incrementally, then pick every feedback that is not overlapped
	// with the previous chosen feedbacks.
	var existsErr bool
	sort.Slice(fbs, func(i, j int) bool {
		res, err := fbs[i].Upper.Compare(sc, fbs[j].Upper, collate.GetBinaryCollator())
		if err != nil {
			existsErr = true
		}
		if existsErr || res != 0 {
			return res < 0
		}
		res, err = fbs[i].Lower.Compare(sc, fbs[j].Lower, collate.GetBinaryCollator())
		if err != nil {
			existsErr = true
		}
		return res < 0
	})
	if existsErr {
		return fbs, false
	}
	resFBs := make([]Feedback, 0, len(fbs))
	previousEnd := &types.Datum{}
	for _, fb := range fbs {
		res, err := previousEnd.Compare(sc, fb.Lower, collate.GetBinaryCollator())
		if err != nil {
			return fbs, false
		}
		if res <= 0 {
			resFBs = append(resFBs, fb)
			previousEnd = fb.Upper
		}
	}
	return resFBs, true
}

// BucketFeedback stands for all the feedback for a bucket.
type BucketFeedback struct {
	feedback []Feedback   // All the feedback info in the same bucket.
	lower    *types.Datum // The lower bound of the new bucket.
	upper    *types.Datum // The upper bound of the new bucket.
}

// outOfRange checks if the `val` is between `min` and `max`.
func outOfRange(sc *stmtctx.StatementContext, min, max, val *types.Datum) (int, error) {
	result, err := val.Compare(sc, min, collate.GetBinaryCollator())
	if err != nil {
		return 0, err
	}
	if result < 0 {
		return result, nil
	}
	result, err = val.Compare(sc, max, collate.GetBinaryCollator())
	if err != nil {
		return 0, err
	}
	if result > 0 {
		return result, nil
	}
	return 0, nil
}

// adjustFeedbackBoundaries adjust the feedback boundaries according to the `min` and `max`.
// If the feedback has no intersection with `min` and `max`, we could just skip this feedback.
func (f *Feedback) adjustFeedbackBoundaries(sc *stmtctx.StatementContext, min, max *types.Datum) (bool, error) {
	result, err := outOfRange(sc, min, max, f.Lower)
	if err != nil {
		return false, err
	}
	if result > 0 {
		return true, nil
	}
	if result < 0 {
		f.Lower = min
	}
	result, err = outOfRange(sc, min, max, f.Upper)
	if err != nil {
		return false, err
	}
	if result < 0 {
		return true, nil
	}
	if result > 0 {
		f.Upper = max
	}
	return false, nil
}

// buildBucketFeedback build the feedback for each bucket from the histogram feedback.
func buildBucketFeedback(h *Histogram, feedback *QueryFeedback) (map[int]*BucketFeedback, int) {
	bktID2FB := make(map[int]*BucketFeedback)
	if len(feedback.Feedback) == 0 {
		return bktID2FB, 0
	}
	total := 0
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	min, max := types.GetMinValue(h.Tp), types.GetMaxValue(h.Tp)
	for _, fb := range feedback.Feedback {
		skip, err := fb.adjustFeedbackBoundaries(sc, &min, &max)
		if err != nil {
			logutil.BgLogger().Debug("adjust feedback boundaries failed", zap.Error(err))
			continue
		}
		if skip {
			continue
		}
		idx := h.Bounds.UpperBound(0, fb.Lower)
		bktIdx := 0
		// The last bucket also stores the feedback that falls outside the upper bound.
		if idx >= h.Bounds.NumRows()-1 {
			bktIdx = h.Len() - 1
		} else if h.Len() == 1 {
			bktIdx = 0
		} else {
			if idx == 0 {
				bktIdx = 0
			} else {
				bktIdx = (idx - 1) / 2
			}
			// Make sure that this feedback lies within the bucket.
			if chunk.Compare(h.Bounds.GetRow(2*(bktIdx+1)), 0, fb.Upper) < 0 {
				continue
			}
		}
		total++
		bkt := bktID2FB[bktIdx]
		if bkt == nil {
			bkt = &BucketFeedback{lower: h.GetLower(bktIdx), upper: h.GetUpper(bktIdx)}
			bktID2FB[bktIdx] = bkt
		}
		bkt.feedback = append(bkt.feedback, fb)
		// Update the bound if necessary.
		res, err := bkt.lower.Compare(nil, fb.Lower, collate.GetBinaryCollator())
		if err != nil {
			logutil.BgLogger().Debug("compare datum failed", zap.Any("value1", bkt.lower), zap.Any("value2", fb.Lower), zap.Error(err))
			continue
		}
		if res > 0 {
			bkt.lower = fb.Lower
		}
		res, err = bkt.upper.Compare(nil, fb.Upper, collate.GetBinaryCollator())
		if err != nil {
			logutil.BgLogger().Debug("compare datum failed", zap.Any("value1", bkt.upper), zap.Any("value2", fb.Upper), zap.Error(err))
			continue
		}
		if res < 0 {
			bkt.upper = fb.Upper
		}
	}
	return bktID2FB, total
}

// getBoundaries gets the new boundaries after split.
func (b *BucketFeedback) getBoundaries(num int) []types.Datum {
	// Get all the possible new boundaries.
	vals := make([]types.Datum, 0, len(b.feedback)*2+2)
	for _, fb := range b.feedback {
		vals = append(vals, *fb.Lower, *fb.Upper)
	}
	vals = append(vals, *b.lower)
	err := types.SortDatums(nil, vals)
	if err != nil {
		logutil.BgLogger().Debug("sort datums failed", zap.Error(err))
		return []types.Datum{*b.lower, *b.upper}
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
		cmp, err := vals[total-1].Compare(nil, &vals[i], collate.GetBinaryCollator())
		if err != nil {
			logutil.BgLogger().Debug("compare datum failed", zap.Any("value1", vals[total-1]), zap.Any("value2", vals[i]), zap.Error(err))
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
type bucket = Feedback

// splitBucket firstly splits this "BucketFeedback" to "newNumBkts" new buckets,
// calculates the count for each new bucket, merge the new bucket whose count
// is smaller than "minBucketFraction*totalCount" with the next new bucket
// until the last new bucket.
func (b *BucketFeedback) splitBucket(newNumBkts int, totalCount float64, originBucketCount float64, originalNdv int64) []bucket {
	// Split the bucket.
	bounds := b.getBoundaries(newNumBkts + 1)
	bkts := make([]bucket, 0, len(bounds)-1)
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	for i := 1; i < len(bounds); i++ {
		newBkt := bucket{&bounds[i-1], bounds[i].Clone(), 0, 0, 0}
		// get bucket count
		_, ratio := getOverlapFraction(Feedback{b.lower, b.upper, int64(originBucketCount), 0, 0}, newBkt)
		countInNewBkt := originBucketCount * ratio
		ndvInNewBkt := int64(float64(originalNdv) * ratio)
		countInNewBkt, ndvInNewBkt = b.refineBucketCount(sc, newBkt, countInNewBkt, ndvInNewBkt)
		// do not split if the count of result bucket is too small.
		if countInNewBkt < minBucketFraction*totalCount {
			bounds[i] = bounds[i-1]
			continue
		}
		newBkt.Count = int64(countInNewBkt)
		newBkt.Ndv = ndvInNewBkt
		bkts = append(bkts, newBkt)
		// To guarantee that each bucket's range will not overlap.
		setNextValue(&bounds[i])
	}
	return bkts
}

// getOverlapFraction gets the overlap fraction of feedback and bucket range. In order to get the bucket count, it also
// returns the ratio between bucket fraction and feedback fraction.
func getOverlapFraction(fb Feedback, bkt bucket) (float64, float64) {
	datums := make([]types.Datum, 0, 4)
	datums = append(datums, *fb.Lower, *fb.Upper)
	datums = append(datums, *bkt.Lower, *bkt.Upper)
	err := types.SortDatums(nil, datums)
	if err != nil {
		return 0, 0
	}
	minValue, maxValue := &datums[0], &datums[3]
	fbLower := calcFraction4Datums(minValue, maxValue, fb.Lower)
	fbUpper := calcFraction4Datums(minValue, maxValue, fb.Upper)
	bktLower := calcFraction4Datums(minValue, maxValue, bkt.Lower)
	bktUpper := calcFraction4Datums(minValue, maxValue, bkt.Upper)
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

// mergeFullyContainedFeedback merges the max fraction of non-overlapped feedbacks that are fully contained in the bucket.
func (b *BucketFeedback) mergeFullyContainedFeedback(sc *stmtctx.StatementContext, bkt bucket) (float64, float64, int64, bool) {
	feedbacks := make([]Feedback, 0, len(b.feedback))
	// Get all the fully contained feedbacks.
	for _, fb := range b.feedback {
		res, err := outOfRange(sc, bkt.Lower, bkt.Upper, fb.Lower)
		if res != 0 || err != nil {
			return 0, 0, 0, false
		}
		res, err = outOfRange(sc, bkt.Lower, bkt.Upper, fb.Upper)
		if res != 0 || err != nil {
			return 0, 0, 0, false
		}
		feedbacks = append(feedbacks, fb)
	}
	if len(feedbacks) == 0 {
		return 0, 0, 0, false
	}
	sortedFBs, ok := NonOverlappedFeedbacks(sc, feedbacks)
	if !ok {
		return 0, 0, 0, false
	}
	var (
		sumFraction, sumCount float64
		ndv                   int64
	)
	for _, fb := range sortedFBs {
		fraction, _ := getOverlapFraction(fb, bkt)
		sumFraction += fraction
		sumCount += float64(fb.Count)
		ndv += fb.Ndv
	}
	return sumFraction, sumCount, ndv, true
}

// refineBucketCount refine the newly split bucket count. It uses the feedback that overlaps most
// with the bucket to get the bucket count.
func (b *BucketFeedback) refineBucketCount(sc *stmtctx.StatementContext, bkt bucket, defaultCount float64, defaultNdv int64) (float64, int64) {
	bestFraction := minBucketFraction
	count := defaultCount
	ndv := defaultNdv
	sumFraction, sumCount, sumNdv, ok := b.mergeFullyContainedFeedback(sc, bkt)
	if ok && sumFraction > bestFraction {
		bestFraction = sumFraction
		count = sumCount / sumFraction
		ndv = int64(float64(sumNdv) / sumFraction)
	}
	for _, fb := range b.feedback {
		fraction, ratio := getOverlapFraction(fb, bkt)
		// choose the max overlap fraction
		if fraction > bestFraction {
			bestFraction = fraction
			count = float64(fb.Count) * ratio
			ndv = int64(float64(fb.Ndv) * ratio)
		}
	}
	return count, ndv
}

const (
	defaultSplitCount = 10
	splitPerFeedback  = 10
	// defaultBucketCount is the number of buckets a column histogram has.
	defaultBucketCount = 256
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
	preCount, count := float64(bkts[id-1].Count), float64(bkts[id].Count)
	// do not merge if the result bucket is too large
	if (preCount + count) > maxBucketFraction*totalCount {
		return bucketScore{id, math.MaxFloat64}
	}
	// Merge them if the result bucket is already too small.
	if (preCount + count) < minBucketFraction*totalCount {
		return bucketScore{id, 0}
	}
	low, mid, high := bkts[id-1].Lower, bkts[id-1].Upper, bkts[id].Upper
	// If we choose to merge, err is the absolute estimate error for the previous bucket.
	err := calcFraction4Datums(low, high, mid)*(preCount+count) - preCount
	return bucketScore{id, math.Abs(err / (preCount + count))}
}

func mergeBuckets(bkts []bucket, isNewBuckets []bool, bucketCount int, totalCount float64) []bucket {
	mergeCount := len(bkts) - bucketCount
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
			bkts[bktCursor-1].Upper = bkts[i].Upper
			bkts[bktCursor-1].Count += bkts[i].Count
			bkts[bktCursor-1].Repeat = bkts[i].Repeat
			bkts[bktCursor-1].Ndv += bkts[i].Ndv
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
func splitBuckets(h *Histogram, feedback *QueryFeedback, bucketCount int) ([]bucket, []bool, int64) {
	bktID2FB, numTotalFBs := buildBucketFeedback(h, feedback)
	buckets := make([]bucket, 0, h.Len())
	isNewBuckets := make([]bool, 0, h.Len())
	splitCount := getSplitCount(numTotalFBs, bucketCount-h.Len())
	for i := 0; i < h.Len(); i++ {
		bktFB, ok := bktID2FB[i]
		// No feedback, just use the original one.
		if !ok {
			buckets = append(buckets, bucket{h.GetLower(i), h.GetUpper(i), h.bucketCount(i), h.Buckets[i].Repeat, h.Buckets[i].NDV})
			isNewBuckets = append(isNewBuckets, false)
			continue
		}
		// Distribute the total split count to bucket based on number of bucket feedback.
		newBktNums := splitCount * len(bktFB.feedback) / numTotalFBs
		bkts := bktFB.splitBucket(newBktNums, h.TotalRowCount(), float64(h.bucketCount(i)), h.Buckets[i].NDV)
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
		totCount += bkt.Count
	}
	return buckets, isNewBuckets, totCount
}

// UpdateHistogram updates the histogram according buckets.
func UpdateHistogram(h *Histogram, feedback *QueryFeedback, statsVer int) *Histogram {
	return UpdateHistogramWithBucketCount(h, feedback, statsVer, defaultBucketCount)
}

// UpdateHistogramWithBucketCount updates the histogram according buckets with customized
// bucketCount for testing.
func UpdateHistogramWithBucketCount(h *Histogram, feedback *QueryFeedback, statsVer int, bucketCount int) *Histogram {
	if statsVer < Version2 {
		// If it's the stats we haven't maintained the bucket NDV yet. Reset the ndv.
		for i := range feedback.Feedback {
			feedback.Feedback[i].Ndv = 0
		}
	}
	buckets, isNewBuckets, totalCount := splitBuckets(h, feedback, bucketCount)
	buckets = mergeBuckets(buckets, isNewBuckets, bucketCount, float64(totalCount))
	hist := buildNewHistogram(h, buckets)
	// Update the NDV of primary key column.
	if feedback.Tp == PkType {
		hist.NDV = int64(hist.TotalRowCount())
		// If we maintained the NDV of bucket. We can also update the total ndv.
	} else if feedback.Tp == IndexType && statsVer == 2 {
		totNdv := int64(0)
		for _, bkt := range buckets {
			totNdv += bkt.Ndv
		}
		hist.NDV = totNdv
	}
	return hist
}

// UpdateCMSketchAndTopN updates the CMSketch and TopN by feedback.
func UpdateCMSketchAndTopN(c *CMSketch, t *TopN, eqFeedbacks []Feedback) (*CMSketch, *TopN) {
	if c == nil || len(eqFeedbacks) == 0 {
		return c, t
	}
	newCMSketch := c.Copy()
	newTopN := t.Copy()
	for _, fb := range eqFeedbacks {
		updateValueBytes(newCMSketch, newTopN, fb.Lower.GetBytes(), uint64(fb.Count))
	}
	return newCMSketch, newTopN
}

func buildNewHistogram(h *Histogram, buckets []bucket) *Histogram {
	hist := NewHistogram(h.ID, h.NDV, h.NullCount, h.LastUpdateVersion, h.Tp, len(buckets), h.TotColSize)
	preCount := int64(0)
	for _, bkt := range buckets {
		hist.AppendBucketWithNDV(bkt.Lower, bkt.Upper, bkt.Count+preCount, bkt.Repeat, bkt.Ndv)
		preCount += bkt.Count
	}
	return hist
}

// queryFeedback is used to serialize the QueryFeedback.
type queryFeedback struct {
	IntRanges []int64
	// HashValues is the murmur hash values for each index point.
	// Note that index points will be stored in `IndexPoints`, we keep it here only for compatibility.
	HashValues  []uint64
	IndexRanges [][]byte
	// IndexPoints stores the value of each equal condition.
	IndexPoints [][]byte
	// Counts is the number of scan keys in each range. It first stores the count for `IntRanges`, `IndexRanges` or `ColumnRanges`.
	// After that, it stores the Ranges for `HashValues`.
	Counts       []int64
	ColumnRanges [][]byte

	Ndvs []int64
}

func encodePKFeedback(q *QueryFeedback) (*queryFeedback, error) {
	pb := &queryFeedback{}
	for _, fb := range q.Feedback {
		// There is no need to update the point queries.
		if bytes.Compare(kv.Key(fb.Lower.GetBytes()).PrefixNext(), fb.Upper.GetBytes()) >= 0 {
			continue
		}
		_, low, err := codec.DecodeInt(fb.Lower.GetBytes())
		if err != nil {
			return nil, errors.Trace(err)
		}
		_, high, err := codec.DecodeInt(fb.Upper.GetBytes())
		if err != nil {
			return nil, errors.Trace(err)
		}
		pb.IntRanges = append(pb.IntRanges, low, high)
		pb.Counts = append(pb.Counts, fb.Count)
		pb.Ndvs = append(pb.Ndvs, fb.Ndv)
	}
	return pb, nil
}

func encodeIndexFeedback(q *QueryFeedback) *queryFeedback {
	pb := &queryFeedback{}
	var pointCounts []int64
	for _, fb := range q.Feedback {
		if bytes.Compare(kv.Key(fb.Lower.GetBytes()).PrefixNext(), fb.Upper.GetBytes()) >= 0 {
			pb.IndexPoints = append(pb.IndexPoints, fb.Lower.GetBytes())
			pointCounts = append(pointCounts, fb.Count)
			pb.Ndvs = append(pb.Ndvs, fb.Ndv)
		} else {
			pb.IndexRanges = append(pb.IndexRanges, fb.Lower.GetBytes(), fb.Upper.GetBytes())
			pb.Counts = append(pb.Counts, fb.Count)
			pb.Ndvs = append(pb.Ndvs, fb.Ndv)
		}
	}
	pb.Counts = append(pb.Counts, pointCounts...)
	return pb
}

func encodeColumnFeedback(q *QueryFeedback) (*queryFeedback, error) {
	pb := &queryFeedback{}
	sc := stmtctx.StatementContext{TimeZone: time.UTC}
	for _, fb := range q.Feedback {
		lowerBytes, err := codec.EncodeKey(&sc, nil, *fb.Lower)
		if err != nil {
			return nil, errors.Trace(err)
		}
		upperBytes, err := codec.EncodeKey(&sc, nil, *fb.Upper)
		if err != nil {
			return nil, errors.Trace(err)
		}
		pb.ColumnRanges = append(pb.ColumnRanges, lowerBytes, upperBytes)
		pb.Counts = append(pb.Counts, fb.Count)
	}
	return pb, nil
}

// EncodeFeedback encodes the given feedback to byte slice.
func EncodeFeedback(q *QueryFeedback) ([]byte, error) {
	var pb *queryFeedback
	var err error
	switch q.Tp {
	case PkType:
		pb, err = encodePKFeedback(q)
	case IndexType:
		pb = encodeIndexFeedback(q)
	case ColType:
		pb, err = encodeColumnFeedback(q)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err = enc.Encode(pb)
	return buf.Bytes(), errors.Trace(err)
}

func decodeFeedbackForIndex(q *QueryFeedback, pb *queryFeedback, c *CMSketch, t *TopN) {
	q.Tp = IndexType
	// decode the index range feedback
	for i := 0; i < len(pb.IndexRanges); i += 2 {
		lower, upper := types.NewBytesDatum(pb.IndexRanges[i]), types.NewBytesDatum(pb.IndexRanges[i+1])
		q.Feedback = append(q.Feedback, Feedback{&lower, &upper, pb.Counts[i/2], 0, pb.Ndvs[i/2]})
	}
	if c != nil {
		// decode the index point feedback, just set value count in CM Sketch
		start := len(pb.IndexRanges) / 2
		if len(pb.HashValues) > 0 {
			for i := 0; i < len(pb.HashValues); i += 2 {
				c.setValue(pb.HashValues[i], pb.HashValues[i+1], uint64(pb.Counts[start+i/2]))
			}
			return
		}
		for i := 0; i < len(pb.IndexPoints); i++ {
			updateValueBytes(c, t, pb.IndexPoints[i], uint64(pb.Counts[start+i]))
		}
	}
}

func decodeFeedbackForPK(q *QueryFeedback, pb *queryFeedback, isUnsigned bool) {
	q.Tp = PkType
	// decode feedback for primary key
	for i := 0; i < len(pb.IntRanges); i += 2 {
		var lower, upper types.Datum
		if isUnsigned {
			lower.SetUint64(uint64(pb.IntRanges[i]))
			upper.SetUint64(uint64(pb.IntRanges[i+1]))
		} else {
			lower.SetInt64(pb.IntRanges[i])
			upper.SetInt64(pb.IntRanges[i+1])
		}
		q.Feedback = append(q.Feedback, Feedback{&lower, &upper, pb.Counts[i/2], 0, pb.Ndvs[i/2]})
	}
}

// ConvertDatumsType converts the datums type to `ft`.
func ConvertDatumsType(vals []types.Datum, ft *types.FieldType, loc *time.Location) error {
	for i, val := range vals {
		if val.Kind() == types.KindMinNotNull || val.Kind() == types.KindMaxValue {
			continue
		}
		newVal, err := tablecodec.UnflattenDatums([]types.Datum{val}, []*types.FieldType{ft}, loc)
		if err != nil {
			return err
		}
		vals[i] = newVal[0]
	}
	return nil
}

func decodeColumnBounds(data []byte, ft *types.FieldType) ([]types.Datum, error) {
	vals, _, err := codec.DecodeRange(data, 1, nil, nil)
	if err != nil {
		return nil, err
	}
	err = ConvertDatumsType(vals, ft, time.UTC)
	return vals, err
}

func decodeFeedbackForColumn(q *QueryFeedback, pb *queryFeedback, ft *types.FieldType) error {
	q.Tp = ColType
	for i := 0; i < len(pb.ColumnRanges); i += 2 {
		low, err := decodeColumnBounds(pb.ColumnRanges[i], ft)
		if err != nil {
			return err
		}
		high, err := decodeColumnBounds(pb.ColumnRanges[i+1], ft)
		if err != nil {
			return err
		}
		q.Feedback = append(q.Feedback, Feedback{&low[0], &high[0], pb.Counts[i/2], 0, 0})
	}
	return nil
}

// DecodeFeedback decodes a byte slice to feedback.
func DecodeFeedback(val []byte, q *QueryFeedback, c *CMSketch, t *TopN, ft *types.FieldType) error {
	buf := bytes.NewBuffer(val)
	dec := gob.NewDecoder(buf)
	pb := &queryFeedback{}
	err := dec.Decode(pb)
	if err != nil {
		return errors.Trace(err)
	}
	if len(pb.IndexRanges) > 0 || len(pb.HashValues) > 0 || len(pb.IndexPoints) > 0 {
		decodeFeedbackForIndex(q, pb, c, t)
	} else if len(pb.IntRanges) > 0 {
		decodeFeedbackForPK(q, pb, mysql.HasUnsignedFlag(ft.GetFlag()))
	} else {
		err = decodeFeedbackForColumn(q, pb, ft)
	}
	return err
}

// SplitFeedbackByQueryType splits the feedbacks into equality feedbacks and range feedbacks.
func SplitFeedbackByQueryType(feedbacks []Feedback) ([]Feedback, []Feedback) {
	var eqFB, ranFB []Feedback
	for _, fb := range feedbacks {
		// Use `>=` here because sometimes the lower is equal to upper.
		if bytes.Compare(kv.Key(fb.Lower.GetBytes()).PrefixNext(), fb.Upper.GetBytes()) >= 0 {
			eqFB = append(eqFB, fb)
		} else {
			ranFB = append(ranFB, fb)
		}
	}
	return eqFB, ranFB
}

// CleanRangeFeedbackByTopN will not update the part containing the TopN.
func CleanRangeFeedbackByTopN(feedbacks []Feedback, topN *TopN) []Feedback {
	for i := len(feedbacks) - 1; i >= 0; i-- {
		lIdx, lMatch := topN.LowerBound(feedbacks[i].Lower.GetBytes())
		rIdx, _ := topN.LowerBound(feedbacks[i].Upper.GetBytes())
		// If the LowerBound return the same result for the range's upper bound and lower bound and the lower one isn't matched,
		// we can indicate that no top-n overlaps the feedback's ranges.
		if lIdx == rIdx && !lMatch {
			continue
		}
		feedbacks = append(feedbacks[:i], feedbacks[i+1:]...)
	}
	return feedbacks
}

// setNextValue sets the next value for the given datum. For types like float,
// we do not set because it is not discrete and does not matter too much when estimating the scalar info.
func setNextValue(d *types.Datum) {
	switch d.Kind() {
	case types.KindBytes, types.KindString:
		// Here is the encoded value instead of string value, so SetBytes is enough.
		d.SetBytes(kv.Key(d.GetBytes()).PrefixNext())
	case types.KindInt64:
		d.SetInt64(d.GetInt64() + 1)
	case types.KindUint64:
		d.SetUint64(d.GetUint64() + 1)
	case types.KindMysqlDuration:
		duration := d.GetMysqlDuration()
		duration.Duration = duration.Duration + 1
		d.SetMysqlDuration(duration)
	case types.KindMysqlTime:
		t := d.GetMysqlTime()
		sc := &stmtctx.StatementContext{TimeZone: types.BoundTimezone}
		if _, err := t.Add(sc, types.Duration{Duration: 1, Fsp: 0}); err != nil {
			log.Error(errors.ErrorStack(err))
		}
		d.SetMysqlTime(t)
	}
}

// SupportColumnType checks if the type of the column can be updated by feedback.
func SupportColumnType(ft *types.FieldType) bool {
	switch ft.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeFloat,
		mysql.TypeDouble, mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob,
		mysql.TypeNewDecimal, mysql.TypeDuration, mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		return true
	}
	return false
}
