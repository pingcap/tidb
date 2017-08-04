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
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
)

// SortedBuilder is used to build histograms for PK and index.
type SortedBuilder struct {
	sc              *variable.StatementContext
	numBuckets      int64
	valuesPerBucket int64
	lastNumber      int64
	bucketIdx       int64
	Count           int64
	isPK            bool
	Hist            *Histogram
}

// NewSortedBuilder creates a new SortedBuilder.
func NewSortedBuilder(ctx context.Context, numBuckets, id int64, isPK bool) *SortedBuilder {
	return &SortedBuilder{
		sc:              ctx.GetSessionVars().StmtCtx,
		numBuckets:      numBuckets,
		valuesPerBucket: 1,
		isPK:            isPK,
		Hist: &Histogram{
			ID:      id,
			Buckets: make([]Bucket, 1, numBuckets),
		},
	}
}

// Iterate updates the histogram incrementally.
func (b *SortedBuilder) Iterate(datums []types.Datum) error {
	var data types.Datum
	if b.isPK {
		data = datums[0]
	} else {
		bytes, err := codec.EncodeKey(nil, datums...)
		if err != nil {
			return errors.Trace(err)
		}
		data = types.NewBytesDatum(bytes)
	}
	cmp, err := b.Hist.Buckets[b.bucketIdx].UpperBound.CompareDatum(b.sc, data)
	if err != nil {
		return errors.Trace(err)
	}
	b.Count++
	if cmp == 0 {
		// The new item has the same value as current bucket value, to ensure that
		// a same value only stored in a single bucket, we do not increase bucketIdx even if it exceeds
		// valuesPerBucket.
		b.Hist.Buckets[b.bucketIdx].Count++
		b.Hist.Buckets[b.bucketIdx].Repeats++
	} else if b.Hist.Buckets[b.bucketIdx].Count+1-b.lastNumber <= b.valuesPerBucket {
		// The bucket still have room to store a new item, update the bucket.
		b.Hist.Buckets[b.bucketIdx].Count++
		b.Hist.Buckets[b.bucketIdx].UpperBound = data
		b.Hist.Buckets[b.bucketIdx].Repeats = 1
		if b.bucketIdx == 0 && b.Hist.Buckets[0].Count == 1 {
			b.Hist.Buckets[0].LowerBound = data
		}
		b.Hist.NDV++
	} else {
		// All buckets are full, we should merge buckets.
		if b.bucketIdx+1 == b.numBuckets {
			b.Hist.mergeBuckets(b.bucketIdx)
			b.valuesPerBucket *= 2
			b.bucketIdx = b.bucketIdx / 2
			if b.bucketIdx == 0 {
				b.lastNumber = 0
			} else {
				b.lastNumber = b.Hist.Buckets[b.bucketIdx-1].Count
			}
		}
		// We may merge buckets, so we should check it again.
		if b.Hist.Buckets[b.bucketIdx].Count+1-b.lastNumber <= b.valuesPerBucket {
			b.Hist.Buckets[b.bucketIdx].Count++
			b.Hist.Buckets[b.bucketIdx].UpperBound = data
			b.Hist.Buckets[b.bucketIdx].Repeats = 1
		} else {
			b.lastNumber = b.Hist.Buckets[b.bucketIdx].Count
			b.bucketIdx++
			b.Hist.Buckets = append(b.Hist.Buckets, Bucket{
				Count:      b.lastNumber + 1,
				UpperBound: data,
				LowerBound: data,
				Repeats:    1,
			})
		}
		b.Hist.NDV++
	}
	return nil
}

// BuildIndex builds histogram for index.
func BuildIndex(ctx context.Context, numBuckets, id int64, records ast.RecordSet) (int64, *Histogram, error) {
	b := NewSortedBuilder(ctx, numBuckets, id, false)
	for {
		row, err := records.Next()
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		if row == nil {
			break
		}
		err = b.Iterate(row.Data)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
	}
	return int64(b.Hist.totalRowCount()), b.Hist, nil
}

// BuildColumn builds histogram from samples for column.
func BuildColumn(ctx context.Context, numBuckets, id int64, ndv int64, count int64, nullCount int64, samples []types.Datum) (*Histogram, error) {
	if count == 0 {
		return &Histogram{ID: id, NullCount: nullCount}, nil
	}
	sc := ctx.GetSessionVars().StmtCtx
	err := types.SortDatums(sc, samples)
	if err != nil {
		return nil, errors.Trace(err)
	}
	hg := &Histogram{
		ID:        id,
		NDV:       ndv,
		NullCount: nullCount,
		Buckets:   make([]Bucket, 1, numBuckets),
	}
	valuesPerBucket := float64(count)/float64(numBuckets) + 1

	// As we use samples to build the histogram, the bucket number and repeat should multiply a factor.
	sampleFactor := float64(count) / float64(len(samples))
	ndvFactor := float64(count) / float64(ndv)
	if ndvFactor > sampleFactor {
		ndvFactor = sampleFactor
	}
	bucketIdx := 0
	var lastCount int64
	hg.Buckets[0].LowerBound = samples[0]
	for i := int64(0); i < int64(len(samples)); i++ {
		cmp, err := hg.Buckets[bucketIdx].UpperBound.CompareDatum(sc, samples[i])
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
	Count   int64
	IsIndex int
	Ctx     context.Context
	Err     error
}
