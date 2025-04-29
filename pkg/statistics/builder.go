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
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/memory"
	"go.uber.org/zap"
)

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

	corrXYSum, err := buildHist(sc, hg, samples, count, ndv, numBuckets, nil)
	if err != nil {
		return nil, err
	}
	hg.Correlation = calcCorrelation(int64(len(samples)), corrXYSum)
	return hg, nil
}

// buildHist builds histogram from samples and other information.
// It stores the built histogram in hg and returns corrXYSum used for calculating the correlation.
func buildHist(
	sc *stmtctx.StatementContext,
	hg *Histogram,
	samples []*SampleItem,
	count, ndv, numBuckets int64,
	memTracker *memory.Tracker,
) (corrXYSum float64, err error) {
	sampleNum := int64(len(samples))
	// As we use samples to build the histogram, the bucket number and repeat should multiply a factor.
	sampleFactor := float64(count) / float64(sampleNum)
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
	// The underlying idea is that when a value is sampled,
	// it does not necessarily mean that the actual row count of this value reaches the sample factor.
	// In extreme cases, it could be that this value only appears once, and that one row happens to be sampled.
	// Therefore, if the sample count of this value is only once, we use a more conservative ndvFactor.
	// However, if the calculated ndvFactor is larger than the sampleFactor, we still use the sampleFactor.
	hg.AppendBucket(&samples[0].Value, &samples[0].Value, int64(sampleFactor), int64(ndvFactor))
	bufferedMemSize := int64(0)
	bufferedReleaseSize := int64(0)
	defer func() {
		if memTracker != nil {
			memTracker.Consume(bufferedMemSize)
			memTracker.Release(bufferedReleaseSize)
		}
	}()

	var upper = new(types.Datum)
	// Note: Start from 1 because we have already processed the first sample.
	for i := int64(1); i < sampleNum; i++ {
		corrXYSum += float64(i) * float64(samples[i].Ordinal)
		hg.UpperToDatum(bucketIdx, upper)
		if memTracker != nil {
			// tmp memory usage
			deltaSize := upper.MemUsage()
			memTracker.BufferedConsume(&bufferedMemSize, deltaSize)
			memTracker.BufferedRelease(&bufferedReleaseSize, deltaSize)
		}
		cmp, err := upper.Compare(sc.TypeCtx(), &samples[i].Value, collate.GetBinaryCollator())
		if err != nil {
			return 0, errors.Trace(err)
		}
		totalCount := float64(i+1) * sampleFactor
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
			hg.updateLastBucket(&samples[i].Value, int64(totalCount), int64(ndvFactor), false)
		} else {
			lastCount = hg.Buckets[bucketIdx].Count
			// The bucket is full, store the item in the next bucket.
			bucketIdx++
			// Refer to the comments for the first bucket for the reason why we use ndvFactor here.
			hg.AppendBucket(&samples[i].Value, &samples[i].Value, int64(totalCount), int64(ndvFactor))
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
		getComparedBytes = func(datum types.Datum) ([]byte, error) {
			encoded, err := codec.EncodeKey(ctx.GetSessionVars().StmtCtx.TimeZone(), nil, datum)
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
	sampleFactor := float64(count) / float64(len(samples))
	// If a numTopn value other than 100 is passed in, we assume it's a value that the user wants us to honor
	allowPruning := true
	if numTopN != 100 {
		allowPruning = false
	}

	// Step1: collect topn from samples

	// the topNList is always sorted by count from more to less
	topNList := make([]TopNMeta, 0, numTopN)
	cur, err := getComparedBytes(samples[0].Value)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	curCnt := float64(0)
	var corrXYSum float64

	// Iterate through the samples
	for i := range sampleNum {
		if isColumn {
			corrXYSum += float64(i) * float64(samples[i].Ordinal)
		}
		if numTopN == 0 {
			continue
		}
		sampleBytes, err := getComparedBytes(samples[i].Value)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		// case 1, this value is equal to the last one: current count++
		if bytes.Equal(cur, sampleBytes) {
			curCnt++
			continue
		}
		// case 2, meet a different value: counting for the "current" is complete
		// case 2-1, do not add a count of 1 if we're sampling or if we've already collected 10% of the topN
		if curCnt == 1 && allowPruning && (len(topNList) >= (numTopN/10) || sampleFactor > 1) {
			cur, curCnt = sampleBytes, 1
			continue
		}
		// case 2-2, now topn is empty: append the "current" count directly
		if len(topNList) == 0 {
			topNList = append(topNList, TopNMeta{Encoded: cur, Count: uint64(curCnt)})
			cur, curCnt = sampleBytes, 1
			continue
		}
		// case 2-3, now topn is full, and the "current" count is less than the least count in the topn: no need to insert the "current"
		if len(topNList) >= numTopN && (curCnt == 1 || uint64(curCnt) <= topNList[len(topNList)-1].Count) {
			cur, curCnt = sampleBytes, 1
			continue
		}
		// case 2-4, now topn is not full, or the "current" count is larger than the least count in the topn: need to find a slot to insert the "current"
		j := len(topNList)
		for ; j > 0; j-- {
			if uint64(curCnt) < topNList[j-1].Count {
				break
			}
		}
		topNList = append(topNList, TopNMeta{})
		copy(topNList[j+1:], topNList[j:])
		topNList[j] = TopNMeta{Encoded: cur, Count: uint64(curCnt)}
		if len(topNList) > numTopN {
			topNList = topNList[:numTopN]
		}
		cur, curCnt = sampleBytes, 1
	}

	// Calc the correlation of the column between the handle column.
	if isColumn {
		hg.Correlation = calcCorrelation(sampleNum, corrXYSum)
	}

	// Handle the counting for the last value. Basically equal to the case 2 above - including
	// limiting addition of a value with a count of 1 (since it will be pruned anyway).
	if numTopN != 0 && (!allowPruning || (allowPruning && (sampleFactor <= 1 || curCnt > 1))) {
		// now topn is empty: append the "current" count directly
		if len(topNList) == 0 {
			topNList = append(topNList, TopNMeta{Encoded: cur, Count: uint64(curCnt)})
		} else if len(topNList) < numTopN || uint64(curCnt) > topNList[len(topNList)-1].Count {
			// now topn is not full, or the "current" count is larger than the least count in the topn: need to find a slot to insert the "current"
			j := len(topNList)
			for ; j > 0; j-- {
				if uint64(curCnt) < topNList[j-1].Count {
					break
				}
			}
			topNList = append(topNList, TopNMeta{})
			copy(topNList[j+1:], topNList[j:])
			topNList[j] = TopNMeta{Encoded: cur, Count: uint64(curCnt)}
			if len(topNList) > numTopN {
				topNList = topNList[:numTopN]
			}
		}
	}

	if allowPruning {
		topNList = pruneTopNItem(topNList, ndv, nullCount, sampleNum, count)
	}

	// Step2: exclude topn from samples
	if numTopN != 0 {
		for i := int64(0); i < int64(len(samples)); i++ {
			sampleBytes, err := getComparedBytes(samples[i].Value)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			// For debugging invalid sample data.
			var (
				foundTwice      bool
				firstTimeSample types.Datum
			)
			for j := range topNList {
				if bytes.Equal(sampleBytes, topNList[j].Encoded) {
					// This should never happen, but we met this panic before, so we add this check here.
					// See: https://github.com/pingcap/tidb/issues/35948
					if foundTwice {
						datumString, err := firstTimeSample.ToString()
						if err != nil {
							statslogutil.StatsLogger().Error("try to convert datum to string failed", zap.Error(err))
						}

						statslogutil.StatsLogger().Warn(
							"invalid sample data",
							zap.Bool("isColumn", isColumn),
							zap.Int64("columnID", id),
							zap.String("datum", datumString),
							zap.Binary("sampleBytes", sampleBytes),
							zap.Binary("topNBytes", topNList[j].Encoded),
						)
						// NOTE: if we don't return here, we may meet panic in the following code.
						// The i may decrease to a negative value.
						// We haven't fix the issue here, because we don't know how to
						// remove the invalid sample data from the samples.
						break
					}
					// First time to find the same value in topN: need to record the sample data for debugging.
					firstTimeSample = samples[i].Value
					// Found the same value in topn: need to skip over this value in samples.
					copy(samples[i:], samples[uint64(i)+topNList[j].Count:])
					samples = samples[:uint64(len(samples))-topNList[j].Count]
					i--
					foundTwice = true
					continue
				}
			}
		}
	}

	topn := &TopN{TopN: topNList}
	topn.Scale(sampleFactor)

	if uint64(count) <= topn.TotalCount() || int(hg.NDV) <= len(topn.TopN) {
		// If we've collected everything  - don't create any buckets
		return hg, topn, nil
	}

	// Step3: build histogram with the rest samples
	if len(samples) > 0 {
		// if we pruned the topN, it means that there are no remaining skewed values in the samples
		if len(topn.TopN) < numTopN && numBuckets == 256 {
			remainingNDV := ndv - int64(len(topn.TopN))
			// set the number of buckets to be the number of remaining distinct values divided by 2
			// but no less than 1 and no more than the original number of buckets
			numBuckets = int(min(max(1, remainingNDV/2), int64(numBuckets)))
		}
		_, err = buildHist(sc, hg, samples, count-int64(topn.TotalCount()), ndv-int64(len(topn.TopN)), int64(numBuckets), memTracker)
		if err != nil {
			return nil, nil, err
		}
	}

	return hg, topn, nil
}

// pruneTopNItem tries to prune the least common values in the top-n list if it is not significantly more common than the values not in the list.
//
//	We assume that the ones not in the top-n list's selectivity is 1/remained_ndv which is the internal implementation of EqualRowCount
func pruneTopNItem(topns []TopNMeta, ndv, nullCount, sampleRows, totalRows int64) []TopNMeta {
	if totalRows <= 1 || int64(len(topns)) >= ndv || len(topns) <= 1 {
		return topns
	}
	// Sum the occurrence except the least common one from the top-n list. To check whether the lest common one is worth
	// storing later.
	sumCount := uint64(0)
	for i := range len(topns) - 1 {
		sumCount += topns[i].Count
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
		k := totalRowsN * float64(topns[topNNum-1].Count) / n
		// Since we are sampling without replacement. The distribution would be a hypergeometric distribution.
		// Thus the variance is the following formula.
		variance := n * k * (totalRowsN - k) * (totalRowsN - n) / (totalRowsN * totalRowsN * (totalRowsN - 1))
		stddev := math.Sqrt(variance)
		// We choose the bound that plus two stddev of the sample frequency, plus an additional 0.5 for the continuity correction.
		//   Note:
		//  	The mean + 2 * stddev is known as Wald confidence interval, plus 0.5 would be continuity-corrected Wald interval
		if float64(topns[topNNum-1].Count) > selectivity*n+2*stddev+0.5 {
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
		sumCount -= topns[topNNum-1].Count
	}
	return topns[:topNNum]
}
