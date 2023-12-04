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
	"math"

	"github.com/google/btree"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/memory"
	"golang.org/x/exp/maps"
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
// It stores the built histogram in hg and return corrXYSum used for calculating the correlation.
func buildHist(sc *stmtctx.StatementContext, hg *Histogram, samples []*SampleItem, count, ndv, numBuckets int64, memTracker *memory.Tracker) (corrXYSum float64, err error) {
	sampleNum := int64(len(samples))
	// As we use samples to build the histogram, the bucket number and repeat should multiply a factor.
	sampleFactor := float64(count) / float64(sampleNum)
	ndvFactor := float64(count) / float64(ndv)
	if ndvFactor > sampleFactor {
		ndvFactor = sampleFactor
	}
	// Since bucket count is increased by sampleFactor, so the actual max values per bucket is
	// floor(valuesPerBucket/sampleFactor)*sampleFactor, which may less than valuesPerBucket,
	// thus we need to add a sampleFactor to avoid building too many buckets.
	valuesPerBucket := float64(count)/float64(numBuckets) + sampleFactor

	bucketIdx := 0
	var lastCount int64
	corrXYSum = float64(0)
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
			// The new item has the same value as current bucket value, to ensure that
			// a same value only stored in a single bucket, we do not increase bucketIdx even if it exceeds
			// valuesPerBucket.
			hg.Buckets[bucketIdx].Count = int64(totalCount)
			if hg.Buckets[bucketIdx].Repeat == int64(ndvFactor) {
				hg.Buckets[bucketIdx].Repeat = int64(2 * sampleFactor)
			} else {
				hg.Buckets[bucketIdx].Repeat += int64(sampleFactor)
			}
		} else if totalCount-float64(lastCount) <= valuesPerBucket {
			// The bucket still have room to store a new item, update the bucket.
			hg.updateLastBucket(&samples[i].Value, int64(totalCount), int64(ndvFactor), false)
		} else {
			lastCount = hg.Buckets[bucketIdx].Count
			// The bucket is full, store the item in the next bucket.
			bucketIdx++
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

type sortItem struct {
	Value       *types.Datum
	SampleBytes []byte
	Sample      []*SampleItem
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

	hg := NewHistogram(id, ndv, nullCount, 0, tp, numBuckets, collector.TotalSize)

	sampleNum := int64(len(collector.Samples))
	// As we use samples to build the histogram, the bucket number and repeat should multiply a factor.
	sampleFactor := float64(count) / float64(len(collector.Samples))
	var sMap = make(map[hack.MutableString]*sortItem, ndv)
	var corrXYSum float64
	for _, s := range samples {
		sampleBytes, err := getComparedBytes(s.Value)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		key := hack.String(sampleBytes)
		if value, ok := sMap[key]; ok {
			value.Sample = append(value.Sample, s)
		} else {
			sMap[key] = &sortItem{
				SampleBytes: sampleBytes,
				Sample:      []*SampleItem{s},
				Value:       &s.Value,
			}
		}
	}

	// Step1: collect topn from samples
	// the topNList is always sorted by count from more to less
	var err error
	topNbtree := btree.NewG(16, func(a, b *sortItem) bool {
		if len(a.Sample) < len(b.Sample) {
			return true
		}
		if len(a.Sample) > len(b.Sample) {
			return false
		}
		var cmp int
		cmp, err = a.Value.Compare(sc.TypeCtx(), b.Value, collate.GetBinaryCollator())
		if err != nil {
			return true
		}
		return cmp > 0
	})
	sampleTree := btree.NewG(20, func(a, b *sortItem) bool {
		var cmp int
		cmp, err = a.Value.Compare(sc.TypeCtx(), b.Value, collate.GetBinaryCollator())
		if err != nil {
			return true
		}
		return cmp < 0
	})
	for _, value := range sMap {
		topNbtree.ReplaceOrInsert(value)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		for topNbtree.Len() > numTopN {
			topNbtree.DeleteMin()
		}
		sampleTree.ReplaceOrInsert(value)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
	}
	maps.Clear(sMap)
	if isColumn {
		// Calc the correlation of the column between the handle column.
		idx := 0
		sampleTree.Ascend(func(i *sortItem) bool {
			for _, s := range i.Sample {
				corrXYSum += float64(idx) * float64(s.Ordinal)
				idx++
			}
			return true
		})
		hg.Correlation = calcCorrelation(sampleNum, corrXYSum)
	}
	// Handle the counting for the last value. Basically equal to the case 2 above.
	// now topn is empty: append the "current" count directly
	topNList, err := pruneTopNItem(topNbtree, sampleTree, ndv, nullCount, sampleNum, count)
	if err != nil {
		return nil, nil, err
	}
	ss := make([]*SampleItem, 0, sampleTree.Len())
	for sampleTree.Len() != 0 {
		item, _ := sampleTree.DeleteMin()
		ss = append(ss, item.Sample...)
	}

	for i := 0; i < len(topNList); i++ {
		topNList[i].Count *= uint64(sampleFactor)
	}
	topn := &TopN{TopN: topNList}

	if uint64(count) <= topn.TotalCount() || int(hg.NDV) <= len(topn.TopN) {
		// TopN includes all sample data
		return hg, topn, nil
	}

	// Step3: build histogram with the rest samples
	if len(ss) > 0 {
		_, err = buildHist(sc, hg, ss, count-int64(topn.TotalCount()), ndv-int64(len(topn.TopN)), int64(numBuckets), memTracker)
		if err != nil {
			return nil, nil, err
		}
	}

	return hg, topn, nil
}

func toTopNMeta(topns *btree.BTreeG[*sortItem], n int) ([]TopNMeta, error) {
	topNList := make([]TopNMeta, 0, n)
	var err error
	topns.Descend(func(i *sortItem) bool {
		topNList = append(topNList, TopNMeta{Count: uint64(len(i.Sample)), Encoded: i.SampleBytes})
		return len(topNList) != n
	})
	return topNList, err
}

// pruneTopNItem tries to prune the least common values in the top-n list if it is not significantly more common than the values not in the list.
//
//	We assume that the ones not in the top-n list's selectivity is 1/remained_ndv which is the internal implementation of EqualRowCount
func pruneTopNItem(topns *btree.BTreeG[*sortItem], sampleTree *btree.BTreeG[*sortItem], ndv, nullCount, sampleRows, totalRows int64) ([]TopNMeta, error) {
	// If the sampleRows holds all rows, or NDV of samples equals to actual NDV, we just return the TopN directly.
	if sampleRows == totalRows || totalRows <= 1 || int64(topns.Len()) >= ndv {
		topns.Descend(func(item *sortItem) bool {
			sampleTree.Delete(item)
			return true
		})
		result, err := toTopNMeta(topns, topns.Len())
		return result, err
	}
	// Sum the occurrence except the least common one from the top-n list. To check whether the lest common one is worth
	// storing later.
	sumCount := uint64(0)
	topnCountList := make([]uint64, 0, topns.Len())
	topns.Descend(func(item *sortItem) bool {
		sampleTree.Delete(item)
		sumCount += uint64(len(item.Sample))
		topnCountList = append(topnCountList, uint64(len(item.Sample)))
		return true
	})
	topNNum := topns.Len()
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
		k := totalRowsN * float64(topnCountList[topNNum-1]) / n
		// Since we are sampling without replacement. The distribution would be a hypergeometric distribution.
		// Thus the variance is the following formula.
		variance := n * k * (totalRowsN - k) * (totalRowsN - n) / (totalRowsN * totalRowsN * (totalRowsN - 1))
		stddev := math.Sqrt(variance)
		// We choose the bound that plus two stddev of the sample frequency, plus an additional 0.5 for the continuity correction.
		//   Note:
		//  	The mean + 2 * stddev is known as Wald confidence interval, plus 0.5 would be continuity-corrected Wald interval
		if float64(topnCountList[topNNum-1]) > selectivity*n+2*stddev+0.5 {
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
		sumCount -= topnCountList[topNNum-1]
	}
	result := make([]TopNMeta, 0, topNNum)
	topns.Descend(func(item *sortItem) bool {
		if len(result) < topNNum {
			result = append(result, TopNMeta{Count: uint64(len(item.Sample)), Encoded: item.SampleBytes})
		} else {
			sampleTree.ReplaceOrInsert(item)
		}
		return true
	})
	return result, nil
}
