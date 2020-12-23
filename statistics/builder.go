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
// See the License for the specific language governing permissions and
// limitations under the License.

package statistics

import (
	"bytes"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
)

// SortedBuilder is used to build histograms for PK and index.
type SortedBuilder struct {
	sc              *stmtctx.StatementContext
	numBuckets      int64
	valuesPerBucket int64
	lastNumber      int64
	bucketIdx       int64
	Count           int64
	hist            *Histogram
}

// NewSortedBuilder creates a new SortedBuilder.
func NewSortedBuilder(sc *stmtctx.StatementContext, numBuckets, id int64, tp *types.FieldType) *SortedBuilder {
	return &SortedBuilder{
		sc:              sc,
		numBuckets:      numBuckets,
		valuesPerBucket: 1,
		hist:            NewHistogram(id, 0, 0, 0, tp, int(numBuckets), 0),
	}
}

// Hist returns the histogram built by SortedBuilder.
func (b *SortedBuilder) Hist() *Histogram {
	return b.hist
}

// Iterate updates the histogram incrementally.
func (b *SortedBuilder) Iterate(data types.Datum) error {
	b.Count++
	if b.Count == 1 {
		b.hist.AppendBucket(&data, &data, 1, 1)
		b.hist.NDV = 1
		return nil
	}
	cmp, err := b.hist.GetUpper(int(b.bucketIdx)).CompareDatum(b.sc, &data)
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
		b.hist.updateLastBucket(&data, b.hist.Buckets[b.bucketIdx].Count+1, 1)
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
			b.hist.updateLastBucket(&data, b.hist.Buckets[b.bucketIdx].Count+1, 1)
		} else {
			b.lastNumber = b.hist.Buckets[b.bucketIdx].Count
			b.bucketIdx++
			b.hist.AppendBucket(&data, &data, b.lastNumber+1, 1)
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
	samples, err := SortSampleItems(sc, samples)
	if err != nil {
		return nil, err
	}
	hg := NewHistogram(id, ndv, nullCount, 0, tp, int(numBuckets), collector.TotalSize)

	corrXYSum, err := buildHist(sc, hg, samples, count, ndv, numBuckets)
	if err != nil {
		return nil, err
	}
	// Compute column order correlation with handle.
	if len(samples) == 1 {
		hg.Correlation = 1
		return hg, nil
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
	itemsCount := float64(len(samples))
	corrXSum := (itemsCount - 1) * itemsCount / 2.0
	corrX2Sum := (itemsCount - 1) * itemsCount * (2*itemsCount - 1) / 6.0
	hg.Correlation = (itemsCount*corrXYSum - corrXSum*corrXSum) / (itemsCount*corrX2Sum - corrXSum*corrXSum)
	return hg, nil
}

func buildHist(sc *stmtctx.StatementContext, hg *Histogram, samples []*SampleItem, count, ndv, numBuckets int64) (float64, error) {
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
	var corrXYSum float64
	hg.AppendBucket(&samples[0].Value, &samples[0].Value, int64(sampleFactor), int64(ndvFactor))
	for i := int64(1); i < sampleNum; i++ {
		corrXYSum += float64(i) * float64(samples[i].Ordinal)
		cmp, err := hg.GetUpper(bucketIdx).CompareDatum(sc, &samples[i].Value)
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
			hg.updateLastBucket(&samples[i].Value, int64(totalCount), int64(ndvFactor))
		} else {
			lastCount = hg.Buckets[bucketIdx].Count
			// The bucket is full, store the item in the next bucket.
			bucketIdx++
			hg.AppendBucket(&samples[i].Value, &samples[i].Value, int64(totalCount), int64(ndvFactor))
		}
	}
	return corrXYSum, nil
}

// BuildColumn builds histogram from samples for column.
func BuildColumn(ctx sessionctx.Context, numBuckets, id int64, collector *SampleCollector, tp *types.FieldType) (*Histogram, error) {
	return BuildColumnHist(ctx, numBuckets, id, collector, tp, collector.Count, collector.FMSketch.NDV(), collector.NullCount)
}

// BuildColumnHistAndTopN build a histogram and TopN for a column from samples.
func BuildColumnHistAndTopN(ctx sessionctx.Context, numBuckets, numTopN int, id int64, collector *SampleCollector, tp *types.FieldType) (*Histogram, *TopN, error) {
	count := collector.Count
	ndv := collector.FMSketch.NDV()
	nullCount := collector.NullCount
	if ndv > count {
		ndv = count
	}
	if count == 0 || len(collector.Samples) == 0 {
		return NewHistogram(id, ndv, nullCount, 0, tp, 0, collector.TotalSize), nil, nil
	}
	sc := ctx.GetSessionVars().StmtCtx
	samples := collector.Samples
	samples, err := SortSampleItems(sc, samples)
	if err != nil {
		return nil, nil, err
	}
	hg := NewHistogram(id, ndv, nullCount, 0, tp, numBuckets, collector.TotalSize)

	sampleNum := int64(len(samples))
	// As we use samples to build the histogram, the bucket number and repeat should multiply a factor.
	sampleFactor := float64(count) / float64(len(samples))

	// Step1: collect topn from samples

	// the topNList is always sorted by count from more to less
	topNList := make([]TopNMeta, 0, numTopN)
	cur, err := codec.EncodeKey(ctx.GetSessionVars().StmtCtx, nil, samples[0].Value)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	curCnt := float64(0)
	var corrXYSum float64

	// Iterate through the samples
	for i := int64(0); i < sampleNum; i++ {
		corrXYSum += float64(i) * float64(samples[i].Ordinal)

		sampleBytes, err := codec.EncodeKey(ctx.GetSessionVars().StmtCtx, nil, samples[i].Value)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		// case 1, this value is equal to the last one: current count++
		if bytes.Equal(cur, sampleBytes) {
			curCnt += 1
			continue
		}
		// case 2, meet a different value: counting for the "current" is complete
		// case 2-1, now topn is empty: append the "current" count directly
		if len(topNList) == 0 {
			topNList = append(topNList, TopNMeta{Encoded: cur, Count: uint64(curCnt)})
			cur, curCnt = sampleBytes, 1
			continue
		}
		// case 2-2, now topn is full, and the "current" count is less than the least count in the topn: no need to insert the "current"
		if len(topNList) >= numTopN && uint64(curCnt) <= topNList[len(topNList)-1].Count {
			cur, curCnt = sampleBytes, 1
			continue
		}
		// case 2-3, now topn is not full, or the "current" count is larger than the least count in the topn: need to find a slot to insert the "current"
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

	// Handle the counting for the last value. Basically equal to the case 2 above.
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

	// Step2: exclude topn from samples
	for i := int64(0); i < int64(len(samples)); i++ {
		sampleBytes, err := codec.EncodeKey(ctx.GetSessionVars().StmtCtx, nil, samples[i].Value)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		for j := 0; j < len(topNList); j++ {
			if bytes.Equal(sampleBytes, topNList[j].Encoded) {
				// find the same value in topn: need to skip over this value in samples
				copy(samples[i:], samples[uint64(i)+topNList[j].Count:])
				samples = samples[:uint64(len(samples))-topNList[j].Count]
				i--
				continue
			}
		}
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
	if len(samples) > 0 {
		_, err = buildHist(sc, hg, samples, count-int64(topn.TotalCount()), ndv-int64(len(topn.TopN)), int64(numBuckets))
		if err != nil {
			return nil, nil, err
		}
	}

	// Compute column order correlation with handle.
	if sampleNum == 1 {
		hg.Correlation = 1
		return hg, topn, nil
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
	hg.Correlation = (itemsCount*corrXYSum - corrXSum*corrXSum) / (itemsCount*corrX2Sum - corrXSum*corrXSum)
	return hg, topn, nil
}
