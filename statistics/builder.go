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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
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
func NewSortedBuilder(sc *stmtctx.StatementContext, numBuckets, id int64) *SortedBuilder {
	return &SortedBuilder{
		sc:              sc,
		numBuckets:      numBuckets,
		valuesPerBucket: 1,
		hist: &Histogram{
			ID:      id,
			Buckets: make([]Bucket, 1, numBuckets),
		},
	}
}

// Hist returns the histogram built by SortedBuilder.
func (b *SortedBuilder) Hist() *Histogram {
	if b.Count == 0 {
		return &Histogram{ID: b.hist.ID}
	}
	return b.hist
}

// Iterate updates the histogram incrementally.
func (b *SortedBuilder) Iterate(data types.Datum) error {
	cmp, err := b.hist.Buckets[b.bucketIdx].UpperBound.CompareDatum(b.sc, &data)
	if err != nil {
		return errors.Trace(err)
	}
	b.Count++
	if cmp == 0 {
		// The new item has the same value as current bucket value, to ensure that
		// a same value only stored in a single bucket, we do not increase bucketIdx even if it exceeds
		// valuesPerBucket.
		b.hist.Buckets[b.bucketIdx].Count++
		b.hist.Buckets[b.bucketIdx].Repeats++
	} else if b.hist.Buckets[b.bucketIdx].Count+1-b.lastNumber <= b.valuesPerBucket {
		// The bucket still have room to store a new item, update the bucket.
		b.hist.Buckets[b.bucketIdx].Count++
		b.hist.Buckets[b.bucketIdx].UpperBound = data
		b.hist.Buckets[b.bucketIdx].Repeats = 1
		if b.bucketIdx == 0 && b.hist.Buckets[0].Count == 1 {
			b.hist.Buckets[0].LowerBound = data
		}
		b.hist.NDV++
	} else {
		// All buckets are full, we should merge buckets.
		if b.bucketIdx+1 == b.numBuckets {
			b.hist.mergeBuckets(b.bucketIdx)
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
			b.hist.Buckets[b.bucketIdx].Count++
			b.hist.Buckets[b.bucketIdx].UpperBound = data
			b.hist.Buckets[b.bucketIdx].Repeats = 1
		} else {
			b.lastNumber = b.hist.Buckets[b.bucketIdx].Count
			b.bucketIdx++
			b.hist.Buckets = append(b.hist.Buckets, Bucket{
				Count:      b.lastNumber + 1,
				UpperBound: data,
				LowerBound: data,
				Repeats:    1,
			})
		}
		b.hist.NDV++
	}
	return nil
}

// BuildColumn builds histogram from samples for column.
func BuildColumn(ctx context.Context, numBuckets, id int64, collector *SampleCollector) (*Histogram, error) {
	count := collector.Count
	if count == 0 {
		return &Histogram{ID: id, NullCount: collector.NullCount}, nil
	}
	sc := ctx.GetSessionVars().StmtCtx
	samples := collector.Samples
	err := types.SortDatums(sc, samples)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ndv := collector.FMSketch.NDV()
	if ndv > count {
		ndv = count
	}
	hg := &Histogram{
		ID:        id,
		NDV:       ndv,
		NullCount: collector.NullCount,
		Buckets:   make([]Bucket, 1, numBuckets),
	}
	valuesPerBucket := float64(count)/float64(numBuckets) + 1

	// As we use samples to build the histogram, the bucket number and repeat should multiply a factor.
	sampleFactor := float64(count) / float64(len(samples))
	ndvFactor := float64(count) / float64(hg.NDV)
	if ndvFactor > sampleFactor {
		ndvFactor = sampleFactor
	}
	bucketIdx := 0
	var lastCount int64
	hg.Buckets[0].LowerBound = samples[0]
	for i := int64(0); i < int64(len(samples)); i++ {
		cmp, err := hg.Buckets[bucketIdx].UpperBound.CompareDatum(sc, &samples[i])
		if err != nil {
			return nil, errors.Trace(err)
		}
		totalCount := float64(i+1) * sampleFactor
		if cmp == 0 {
			// The new item has the same value as current bucket value, to ensure that
			// a same value only stored in a single bucket, we do not increase bucketIdx even if it exceeds
			// valuesPerBucket.
			hg.Buckets[bucketIdx].Count = int64(totalCount)
			if float64(hg.Buckets[bucketIdx].Repeats) == ndvFactor {
				hg.Buckets[bucketIdx].Repeats = int64(2 * sampleFactor)
			} else {
				hg.Buckets[bucketIdx].Repeats += int64(sampleFactor)
			}
		} else if totalCount-float64(lastCount) <= valuesPerBucket {
			// The bucket still have room to store a new item, update the bucket.
			hg.Buckets[bucketIdx].Count = int64(totalCount)
			hg.Buckets[bucketIdx].UpperBound = samples[i]
			hg.Buckets[bucketIdx].Repeats = int64(ndvFactor)
		} else {
			lastCount = hg.Buckets[bucketIdx].Count
			// The bucket is full, store the item in the next bucket.
			bucketIdx++
			hg.Buckets = append(hg.Buckets, Bucket{
				Count:      int64(totalCount),
				UpperBound: samples[i],
				LowerBound: samples[i],
				Repeats:    int64(ndvFactor),
			})
		}
	}
	return hg, nil
}

// AnalyzeResult is used to represent analyze result.
type AnalyzeResult struct {
	TableID int64
	Hist    []*Histogram
	Cms     []*CMSketch
	Count   int64
	IsIndex int
	Err     error
}
