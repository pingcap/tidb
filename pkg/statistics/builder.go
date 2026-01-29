// Copyright 2017 PingCAP, Inc.
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
	"cmp"
	"math"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/generic"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/memory"
)

// local builder-only tuning knobs
const (
	// topNPruningThreshold represents 10% threshold for TopN pruning
	topNPruningThreshold = 10
	// bucketNDVDivisor is used to calculate bucket count based on remaining NDV
	bucketNDVDivisor = 2
)

// TopNWithRange wraps TopNMeta with index range information for efficient histogram building.
type TopNWithRange struct {
	TopNMeta       // embedded TopNMeta
	startIdx int64 // inclusive start index in samples array
	endIdx   int64 // inclusive end index in samples array
}

// SequentialRangeChecker efficiently checks if an index is within any TopN range.
// It takes advantage of the fact that we iterate indices sequentially (0,1,2,...)
// and ranges are sorted, so we can advance through ranges rather than search.
type SequentialRangeChecker struct {
	ranges          []TopNWithRange
	currentRangeIdx int // index of the range we're currently checking
}

// NewSequentialRangeChecker creates a new range checker with sorted ranges.
func NewSequentialRangeChecker(ranges []TopNWithRange) *SequentialRangeChecker {
	// Sort ranges by start index to enable sequential checking
	sort.Slice(ranges, func(i, j int) bool {
		return ranges[i].startIdx < ranges[j].startIdx
	})

	return &SequentialRangeChecker{
		ranges:          ranges,
		currentRangeIdx: 0,
	}
}

// IsIndexInTopNRange checks if the given index is within any TopN range.
// This is optimized for sequential index access patterns.
func (s *SequentialRangeChecker) IsIndexInTopNRange(idx int64) bool {
	// Advance past completed ranges
	for s.currentRangeIdx < len(s.ranges) && idx > s.ranges[s.currentRangeIdx].endIdx {
		s.currentRangeIdx++
	}

	// Check if current index is in the current range
	if s.currentRangeIdx < len(s.ranges) {
		currentRange := s.ranges[s.currentRangeIdx]
		return idx >= currentRange.startIdx && idx <= currentRange.endIdx
	}

	return false
}

// processTopNValue handles the logic for a complete TopN value count using bounded min-heap with range tracking.
func processTopNValue(boundedMinHeap *generic.BoundedMinHeap[TopNWithRange], encoded []byte, curCnt float64,
	startIdx, endIdx int64, numTopN int, allowPruning bool, sampleFactor float64, lastValue bool) {
	// case 1: do not add a count of 1 if we're sampling or if we've already collected 10% of the topN
	// Note: adding lastValue corner case handling just to make consistent behavior with previous code version,
	// it is not necessary to special handle last value but a lot of current tests hardcoded the output of the last
	// version and making change will involve a lot of test changes.
	if !lastValue && curCnt == 1 && allowPruning &&
		(boundedMinHeap.Len() >= (numTopN/topNPruningThreshold) || sampleFactor > 1) {
		return
	}

	// case 2: add to bounded min-heap (heap handles all optimization internally)
	newItem := TopNWithRange{
		TopNMeta: TopNMeta{Encoded: encoded, Count: uint64(curCnt)},
		startIdx: startIdx,
		endIdx:   endIdx,
	}
	boundedMinHeap.Add(newItem)
}

// SortedBuilder is used to build histograms for PK and index.
type SortedBuilder struct {
	sc              *stmtctx.StatementContext
	hist            *Histogram
	numBuckets      int64
	valuesPerBucket int64
	lastNumber      int64
	bucketIdx       int64
	Count           int64
	needBucketNDV   bool
}

// NewSortedBuilder creates a new SortedBuilder.
func NewSortedBuilder(sc *stmtctx.StatementContext, numBuckets, id int64, tp *types.FieldType, statsVer int) *SortedBuilder {
	return &SortedBuilder{
		sc:              sc,
		numBuckets:      numBuckets,
		valuesPerBucket: 1,
		hist:            NewHistogram(id, 0, 0, 0, tp, int(numBuckets), 0),
		needBucketNDV:   statsVer >= Version2,
	}
}

// Hist returns the histogram built by SortedBuilder.
func (b *SortedBuilder) Hist() *Histogram {
	return b.hist
}

// Iterate updates the histogram incrementally.
func (b *SortedBuilder) Iterate(data types.Datum) error {
	b.Count++
	appendBucket := b.hist.AppendBucket
	if b.needBucketNDV {
		appendBucket = func(lower, upper *types.Datum, count, repeat int64) {
			b.hist.AppendBucketWithNDV(lower, upper, count, repeat, 1)
		}
	}
	if b.Count == 1 {
		appendBucket(&data, &data, 1, 1)
		b.hist.NDV = 1
		return nil
	}
	cmp, err := b.hist.GetUpper(int(b.bucketIdx)).Compare(b.sc.TypeCtx(), &data, collate.GetBinaryCollator())
	if err != nil {
		return errors.Trace(err)
	}
	if cmp == 0 {
		// The new item has the same value as current bucket value, to ensure that
		// a same value only stored in a single bucket, we do not increase bucketIdx even if it exceeds
		// valuesPerBucket.
		b.hist.Buckets[b.bucketIdx].Count++
		b.hist.Buckets[b.bucketIdx].Repeat++
	} else if b.hist.Buckets[b.bucketIdx].Count+1-b.lastNumber <= b.valuesPerBucket {
		// The bucket still have room to store a new item, update the bucket.
		b.hist.updateLastBucket(&data, b.hist.Buckets[b.bucketIdx].Count+1, 1, b.needBucketNDV)
		b.hist.NDV++
	} else {
		// All buckets are full, we should merge buckets.
		if b.bucketIdx+1 == b.numBuckets {
			b.hist.mergeBuckets(int(b.bucketIdx))
			b.valuesPerBucket *= 2
			b.bucketIdx = b.bucketIdx / 2
			if b.bucketIdx == 0 {
				b.lastNumber = 0
			} else {
				b.lastNumber = b.hist.Buckets[b.bucketIdx-1].Count
			}
		}
		// We may merge buckets, so we should check it again.
		if b.hist.Buckets[b.bucketIdx].Count+1-b.lastNumber <= b.valuesPerBucket {
			b.hist.updateLastBucket(&data, b.hist.Buckets[b.bucketIdx].Count+1, 1, b.needBucketNDV)
		} else {
			b.lastNumber = b.hist.Buckets[b.bucketIdx].Count
			b.bucketIdx++
			appendBucket(&data, &data, b.lastNumber+1, 1)
		}
		b.hist.NDV++
	}
	return nil
}

// BuildColumnHist build a histogram for a column.
// numBuckets: number of buckets for the histogram.
// id: the id of the table.
// collector: the collector of samples.
// tp: the FieldType for the column.
// count: represents the row count for the column.
// ndv: represents the number of distinct values for the column.
// nullCount: represents the number of null values for the column.
func BuildColumnHist(ctx sessionctx.Context, numBuckets, id int64, collector *SampleCollector, tp *types.FieldType, count int64, ndv int64, nullCount int64) (*Histogram, error) {
	if ndv > count {
		ndv = count
	}
	if count == 0 || len(collector.Samples) == 0 {
		return NewHistogram(id, ndv, nullCount, 0, tp, 0, collector.TotalSize), nil
	}
	sc := ctx.GetSessionVars().StmtCtx
	samples := collector.Samples
	err := sortSampleItems(sc, samples)
	if err != nil {
		return nil, err
	}
	hg := NewHistogram(id, ndv, nullCount, 0, tp, int(numBuckets), collector.TotalSize)

	corrXYSum, err := buildHist(sc, hg, samples, count, ndv, numBuckets, nil, int64(len(samples)), nil)
	if err != nil {
		return nil, err
	}
	hg.Correlation = calcCorrelation(int64(len(samples)), corrXYSum)
	return hg, nil
}

// buildHist builds histogram from samples and other information.
// It stores the built histogram in hg and returns corrXYSum used for calculating the correlation.
// If rangeChecker is provided, it will skip indices that are within TopN ranges for efficient building.
func buildHist(
	sc *stmtctx.StatementContext,
	hg *Histogram,
	samples []*SampleItem,
	count, ndv, numBuckets int64,
	memTracker *memory.Tracker,
	sampleCountExcludeTopN int64,
	rangeChecker *SequentialRangeChecker, // optional range checker for skipping TopN indices
) (corrXYSum float64, err error) {
	sampleNum := int64(len(samples))
	// As we use samples to build the histogram, the bucket number and repeat should multiply a factor.
	sampleFactor := float64(count) / float64(sampleCountExcludeTopN)
	// ndvFactor is a ratio that represents the average number of times each distinct value (NDV) should appear in the dataset.
	// It is calculated as the total number of rows divided by the number of distinct values.
	ndvFactor := min(float64(count)/float64(ndv), sampleFactor)
	// Since bucket count is increased by sampleFactor, so the actual max values per bucket are
	// floor(valuesPerBucket/sampleFactor)*sampleFactor, which may less than valuesPerBucket,
	// thus we need to add a sampleFactor to avoid building too many buckets.
	valuesPerBucket := float64(count)/float64(numBuckets) + sampleFactor

	bucketIdx := 0
	var lastCount int64
	corrXYSum = float64(0)

	// find the first non-skipped sample to initialize the histogram
	firstSampleIdx := int64(-1)
	if rangeChecker != nil {
		for i := range sampleNum {
			if !rangeChecker.IsIndexInTopNRange(i) {
				firstSampleIdx = i
				break
			}
		}
		if firstSampleIdx == -1 {
			// all samples are in TopN ranges, return empty histogram
			return 0, nil
		}
	} else {
		firstSampleIdx = 0
	}

	// The underlying idea is that when a value is sampled,
	// it does not necessarily mean that the actual row count of this value reaches the sample factor.
	// In extreme cases, it could be that this value only appears once, and that one row happens to be sampled.
	// Therefore, if the sample count of this value is only once, we use a more conservative ndvFactor.
	// However, if the calculated ndvFactor is larger than the sampleFactor, we still use the sampleFactor.
	hg.AppendBucket(samples[firstSampleIdx].Value, samples[firstSampleIdx].Value, int64(sampleFactor), int64(ndvFactor))
	bufferedMemSize := int64(0)
	bufferedReleaseSize := int64(0)
	defer func() {
		if memTracker != nil {
			memTracker.Consume(bufferedMemSize)
			memTracker.Release(bufferedReleaseSize)
		}
	}()

	var upper = new(types.Datum)
	processedCount := int64(1) // we've processed the first sample

	// Start from firstSampleIdx + 1 since the first non-skipped sample has already been processed
	// when the range checker is not null.
	for i := firstSampleIdx + 1; i < sampleNum; i++ {
		// Skip if this index is in a TopN range
		if rangeChecker != nil && rangeChecker.IsIndexInTopNRange(i) {
			continue
		}

		processedCount++
		corrXYSum += float64(i) * float64(samples[i].Ordinal)
		hg.UpperToDatum(bucketIdx, upper)
		if memTracker != nil {
			// tmp memory usage
			deltaSize := upper.MemUsage()
			memTracker.BufferedConsume(&bufferedMemSize, deltaSize)
			memTracker.BufferedRelease(&bufferedReleaseSize, deltaSize)
		}
		cmp, err := upper.Compare(sc.TypeCtx(), samples[i].Value, collate.GetBinaryCollator())
		if err != nil {
			return 0, errors.Trace(err)
		}
		totalCount := float64(processedCount) * sampleFactor
		if cmp == 0 {
			// The new item has the same value as the current bucket value, to ensure that
			// a same value only stored in a single bucket, we do not increase bucketIdx even if it exceeds
			// valuesPerBucket.
			hg.Buckets[bucketIdx].Count = int64(totalCount)
			// This means the value appears more than once in the sample, so we need to update the repeat count.
			// Because we initialize the repeat count as ndvFactor, so we need to directly reset it to 2*sampleFactor.
			// Refer to the comments for the first bucket for the reason why we use ndvFactor here.
			if hg.Buckets[bucketIdx].Repeat == int64(ndvFactor) {
				// This is a special case, the value appears twice in the sample.
				// repeat = 2 * sampleFactor
				hg.Buckets[bucketIdx].Repeat = int64(2 * sampleFactor)
			} else {
				// repeat =  3 * sampleFactor
				// repeat =  4 * sampleFactor
				// ...
				hg.Buckets[bucketIdx].Repeat += int64(sampleFactor)
			}
		} else if totalCount-float64(lastCount) <= valuesPerBucket {
			// The bucket still has room to store a new item, update the bucket.
			hg.updateLastBucket(samples[i].Value, int64(totalCount), int64(ndvFactor), false)
		} else {
			lastCount = hg.Buckets[bucketIdx].Count
			// The bucket is full, store the item in the next bucket.
			bucketIdx++
			// Refer to the comments for the first bucket for the reason why we use ndvFactor here.
			hg.AppendBucket(samples[i].Value, samples[i].Value, int64(totalCount), int64(ndvFactor))
		}
	}
	return corrXYSum, nil
}

// calcCorrelation computes column order correlation with the handle.
func calcCorrelation(sampleNum int64, corrXYSum float64) float64 {
	if sampleNum == 1 {
		return 1
	}
	// X means the ordinal of the item in original sequence, Y means the ordinal of the item in the
	// sorted sequence, we know that X and Y value sets are both:
	// 0, 1, ..., sampleNum-1
	// we can simply compute sum(X) = sum(Y) =
	//    (sampleNum-1)*sampleNum / 2
	// and sum(X^2) = sum(Y^2) =
	//    (sampleNum-1)*sampleNum*(2*sampleNum-1) / 6
	// We use "Pearson correlation coefficient" to compute the order correlation of columns,
	// the formula is based on https://en.wikipedia.org/wiki/Pearson_correlation_coefficient.
	// Note that (itemsCount*corrX2Sum - corrXSum*corrXSum) would never be zero when sampleNum is larger than 1.
	itemsCount := float64(sampleNum)
	corrXSum := (itemsCount - 1) * itemsCount / 2.0
	corrX2Sum := (itemsCount - 1) * itemsCount * (2*itemsCount - 1) / 6.0
	return (itemsCount*corrXYSum - corrXSum*corrXSum) / (itemsCount*corrX2Sum - corrXSum*corrXSum)
}

// BuildColumn builds histogram from samples for column.
func BuildColumn(ctx sessionctx.Context, numBuckets, id int64, collector *SampleCollector, tp *types.FieldType) (*Histogram, error) {
	return BuildColumnHist(ctx, numBuckets, id, collector, tp, collector.Count, collector.FMSketch.NDV(), collector.NullCount)
}

// BuildHistAndTopN build a histogram and TopN for a column or an index from samples.
func BuildHistAndTopN(
	ctx sessionctx.Context,
	numBuckets, numTopN int,
	id int64,
	collector *SampleCollector,
	tp *types.FieldType,
	isColumn bool,
	memTracker *memory.Tracker,
	needExtStats bool,
) (*Histogram, *TopN, error) {
	bufferedMemSize := int64(0)
	bufferedReleaseSize := int64(0)
	defer func() {
		if memTracker != nil {
			memTracker.Consume(bufferedMemSize)
			memTracker.Release(bufferedReleaseSize)
		}
	}()
	var getComparedBytes func(datum types.Datum) ([]byte, error)
	if isColumn {
		timeZone := ctx.GetSessionVars().StmtCtx.TimeZone()
		getComparedBytes = func(datum types.Datum) ([]byte, error) {
			encoded, err := codec.EncodeKey(timeZone, nil, datum)
			err = ctx.GetSessionVars().StmtCtx.HandleError(err)
			if memTracker != nil {
				// tmp memory usage
				deltaSize := int64(cap(encoded))
				memTracker.BufferedConsume(&bufferedMemSize, deltaSize)
				memTracker.BufferedRelease(&bufferedReleaseSize, deltaSize)
			}
			return encoded, err
		}
	} else {
		getComparedBytes = func(datum types.Datum) ([]byte, error) {
			return datum.GetBytes(), nil
		}
	}
	count := collector.Count
	ndv := collector.FMSketch.NDV()
	nullCount := collector.NullCount
	if ndv > count {
		ndv = count
	}
	if count == 0 || len(collector.Samples) == 0 || ndv == 0 {
		return NewHistogram(id, ndv, nullCount, 0, tp, 0, collector.TotalSize), nil, nil
	}
	sc := ctx.GetSessionVars().StmtCtx
	var samples []*SampleItem
	// if we need to build extended stats, we need to copy the samples to avoid modifying the original samples.
	if needExtStats {
		samples = make([]*SampleItem, len(collector.Samples))
		copy(samples, collector.Samples)
	} else {
		samples = collector.Samples
	}
	err := sortSampleItems(sc, samples)
	if err != nil {
		return nil, nil, err
	}
	hg := NewHistogram(id, ndv, nullCount, 0, tp, numBuckets, collector.TotalSize)

	sampleNum := int64(len(samples))
	// As we use samples to build the histogram, the bucket number and repeat should multiply a factor.
	sampleFactor := float64(count) / float64(sampleNum)
	// If a numTopn value other than default is passed in, we assume it's a value that the user wants us to honor
	allowPruning := true
	if numTopN != DefaultTopNValue {
		allowPruning = false
	}

	// Step1: collect topn from samples using bounded min-heap and track their index ranges
	boundedMinHeap := generic.NewBoundedMinHeap(numTopN, func(a, b TopNWithRange) int {
		return cmp.Compare(a.Count, b.Count) // min-heap: smaller counts at root
	})

	intest.Assert(samples[0].Value != nil, "sample item value should not be nil")
	if samples[0].Value == nil {
		return nil, nil, errors.Errorf("sample item value is nil")
	}
	cur, err := getComparedBytes(*samples[0].Value)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	curCnt := float64(0)
	curStartIdx := int64(0) // track start index of current value group
	// sampleNDV is the number of distinct values in the samples, which may differ from the real NDV due to sampling.
	// Initialize to 1 because the first time in the loop we don't increment the sampleNDV - we increment upon change
	// of value, and the first value is always new.
	sampleNDV := int64(1)
	var corrXYSum float64

	// Iterate through the samples
	for i := range sampleNum {
		if isColumn {
			corrXYSum += float64(i) * float64(samples[i].Ordinal)
		}
		if numTopN == 0 {
			continue
		}
		intest.Assert(samples[i].Value != nil, "sample item value should not be nil")
		if samples[i].Value == nil {
			return nil, nil, errors.Errorf("sample item value is nil")
		}
		sampleBytes, err := getComparedBytes(*samples[i].Value)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

		// case 1, this value is equal to the last one: current count++
		if bytes.Equal(cur, sampleBytes) {
			curCnt++
			continue
		}
		// case 2, meet a different value: counting for the "current" is complete
		sampleNDV++
		// process the completed value using bounded min-heap with range tracking
		processTopNValue(boundedMinHeap, cur, curCnt, curStartIdx, i-1, numTopN, allowPruning, sampleFactor, false)

		cur, curCnt = sampleBytes, 1
		curStartIdx = i // new value group starts at current index
	}

	// Calc the correlation of the column between the handle column.
	if isColumn {
		hg.Correlation = calcCorrelation(sampleNum, corrXYSum)
	}

	// handle the counting for the last value
	// Note: not necessary to add the condition (!allowPruning || (sampleFactor <= 1 || curCnt > 1)), it can be handled
	// inside processTopNValue but just to make it consistent with previous behavior...
	if numTopN != 0 && (!allowPruning || (sampleFactor <= 1 || curCnt > 1)) {
		processTopNValue(boundedMinHeap, cur, curCnt, curStartIdx, sampleNum-1, numTopN, allowPruning, sampleFactor,
			true)
	}

	// convert to sorted slice
	sortedTopNItems := boundedMinHeap.ToSortedSlice()

	prunedTopNItems := sortedTopNItems
	if allowPruning {
		// Prune out any TopN values that have the same count as the remaining average.
		prunedTopNItems = pruneTopNItem(sortedTopNItems, ndv, nullCount, sampleNum, count)
		if sampleNDV > 1 && sampleFactor > 1 && ndv > sampleNDV && len(prunedTopNItems) >= int(sampleNDV) {
			// If we're sampling, and TopN contains everything in the sample - trim TopN so
			// that buckets will be built. This can help address issues in optimizer
			// cardinality estimation if TopN contains all values in the sample, but the
			// length of the TopN is less than the true column/index NDV. Ensure that we keep
			// at least one item in the topN list. If the sampleNDV is small, all remaining
			// values are likely to added as the last value of a bucket such that skew will
			// still be recognized.
			keepTopN := max(1, sampleNDV-1)
			prunedTopNItems = prunedTopNItems[:keepTopN]
		}
	}

	// extract TopNMeta for result from final pruned items
	topNList := make([]TopNMeta, len(prunedTopNItems))
	for i, item := range prunedTopNItems {
		topNList[i] = item.TopNMeta
	}

	topn := &TopN{TopN: topNList}
	lenTopN := int64(len(topn.TopN))

	haveAllNDV := sampleNDV == lenTopN && lenTopN > 0

	// Step2: calculate adjusted parameters for histogram
	// The histogram will be built with reduced count and NDV to account for TopN values
	var topNTotalCount uint64
	var topNSampleCount int64
	for i := range topn.TopN {
		topNSampleCount += int64(topn.TopN[i].Count)
		topn.TopN[i].Count = uint64(float64(topn.TopN[i].Count) * sampleFactor)
		topNTotalCount += topn.TopN[i].Count
	}

	if haveAllNDV || numBuckets <= 0 {
		// If we've collected everything or numBuckets == 0 - don't create any buckets
		return hg, topn, nil
	}

	// Step3: build histogram excluding TopN values
	samplesExcludingTopN := sampleNum - topNSampleCount
	if samplesExcludingTopN > 0 {
		remainingNDV := ndv - lenTopN
		// if we pruned the topN, it means that there are no remaining skewed values in the samples
		if lenTopN < int64(numTopN) && numBuckets == DefaultHistogramBuckets {
			// set the number of buckets to be the number of remaining distinct values divided by bucketNDVDivisor
			// but no less than 1 and no more than the original number of buckets
			numBuckets = int(min(max(1, remainingNDV/bucketNDVDivisor), int64(numBuckets)))
		}

		// create range checker for efficient TopN index skipping using final TopN items only
		rangeChecker := NewSequentialRangeChecker(prunedTopNItems)

		_, err = buildHist(sc, hg, samples, count-int64(topNTotalCount), remainingNDV,
			int64(numBuckets), memTracker, samplesExcludingTopN, rangeChecker)
		if err != nil {
			return nil, nil, err
		}
	}

	return hg, topn, nil
}

// pruneTopNItem tries to prune the least common values in the top-n list if it is not significantly more common than the values not in the list.
//
//	We assume that the ones not in the top-n list's selectivity is 1/remained_ndv which is the internal implementation of EqualRowCount
func pruneTopNItem(topns []TopNWithRange, ndv, nullCount, sampleRows, totalRows int64) []TopNWithRange {
	if totalRows <= 1 || int64(len(topns)) >= ndv || len(topns) <= 1 {
		return topns
	}
	// Sum the occurrence except the least common one from the top-n list. To check whether the lest common one is worth
	// storing later.
	sumCount := uint64(0)
	for i := range len(topns) - 1 {
		sumCount += topns[i].TopNMeta.Count
	}
	topNNum := len(topns)
	for topNNum > 0 {
		// Selectivity for the ones not in the top-n list.
		// (1 - things in top-n list - null) / remained ndv.
		selectivity := 1.0 - float64(sumCount)/float64(sampleRows) - float64(nullCount)/float64(totalRows)
		if selectivity < 0.0 {
			selectivity = 0
		}
		if selectivity > 1 {
			selectivity = 1
		}
		otherNDV := float64(ndv) - (float64(topNNum) - 1)
		if otherNDV > 1 {
			selectivity /= otherNDV
		}
		totalRowsN := float64(totalRows)
		n := float64(sampleRows)
		k := totalRowsN * float64(topns[topNNum-1].TopNMeta.Count) / n
		// Since we are sampling without replacement. The distribution would be a hypergeometric distribution.
		// Thus the variance is the following formula.
		variance := n * k * (totalRowsN - k) * (totalRowsN - n) / (totalRowsN * totalRowsN * (totalRowsN - 1))
		stddev := math.Sqrt(variance)
		// We choose the bound that plus two stddev of the sample frequency, plus an additional 0.5 for the continuity correction.
		//   Note:
		//  	The mean + 2 * stddev is known as Wald confidence interval, plus 0.5 would be continuity-corrected Wald interval
		if float64(topns[topNNum-1].TopNMeta.Count) > selectivity*n+2*stddev+0.5 {
			// Estimated selectivity of this item in the TopN is significantly higher than values not in TopN.
			// So this value, and all other values in the TopN (selectivity of which is higher than this value) are
			// worth being remained in the TopN list, and we stop pruning now.
			break
		}
		// Current one is not worth storing, remove it and subtract it from sumCount, go to next one.
		topNNum--
		if topNNum == 0 {
			break
		}
		sumCount -= topns[topNNum-1].TopNMeta.Count
	}
	return topns[:topNNum]
}
