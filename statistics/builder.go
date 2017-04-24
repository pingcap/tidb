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
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
)

// BuildPK builds histogram for pk.
func BuildPK(ctx context.Context, numBuckets, id int64, records ast.RecordSet) (int64, *Histogram, error) {
	return build4SortedColumn(ctx, numBuckets, id, records, true)
}

// BuildIndex builds histogram for index.
func BuildIndex(ctx context.Context, numBuckets, id int64, records ast.RecordSet) (int64, *Histogram, error) {
	return build4SortedColumn(ctx, numBuckets, id, records, false)
}

func build4SortedColumn(ctx context.Context, numBuckets, id int64, records ast.RecordSet, isPK bool) (int64, *Histogram, error) {
	hg := &Histogram{
		ID:      id,
		NDV:     0,
		Buckets: make([]Bucket, 1, numBuckets),
	}
	var valuesPerBucket, lastNumber, bucketIdx int64 = 1, 0, 0
	count := int64(0)
	sc := ctx.GetSessionVars().StmtCtx
	for {
		row, err := records.Next()
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		if row == nil {
			break
		}
		var data types.Datum
		if isPK {
			data = row.Data[0]
		} else {
			bytes, err := codec.EncodeKey(nil, row.Data...)
			if err != nil {
				return 0, nil, errors.Trace(err)
			}
			data = types.NewBytesDatum(bytes)
		}
		cmp, err := hg.Buckets[bucketIdx].Value.CompareDatum(sc, data)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		count++
		if cmp == 0 {
			// The new item has the same value as current bucket value, to ensure that
			// a same value only stored in a single bucket, we do not increase bucketIdx even if it exceeds
			// valuesPerBucket.
			hg.Buckets[bucketIdx].Count++
			hg.Buckets[bucketIdx].Repeats++
		} else if hg.Buckets[bucketIdx].Count+1-lastNumber <= valuesPerBucket {
			// The bucket still have room to store a new item, update the bucket.
			hg.Buckets[bucketIdx].Count++
			hg.Buckets[bucketIdx].Value = data
			hg.Buckets[bucketIdx].Repeats = 1
			hg.NDV++
		} else {
			// All buckets are full, we should merge buckets.
			if bucketIdx+1 == numBuckets {
				hg.mergeBuckets(bucketIdx)
				valuesPerBucket *= 2
				bucketIdx = bucketIdx / 2
				if bucketIdx == 0 {
					lastNumber = 0
				} else {
					lastNumber = hg.Buckets[bucketIdx-1].Count
				}
			}
			// We may merge buckets, so we should check it again.
			if hg.Buckets[bucketIdx].Count+1-lastNumber <= valuesPerBucket {
				hg.Buckets[bucketIdx].Count++
				hg.Buckets[bucketIdx].Value = data
				hg.Buckets[bucketIdx].Repeats = 1
			} else {
				lastNumber = hg.Buckets[bucketIdx].Count
				bucketIdx++
				hg.Buckets = append(hg.Buckets, Bucket{
					Count:   lastNumber + 1,
					Value:   data,
					Repeats: 1,
				})
			}
			hg.NDV++
		}
	}
	if count == 0 {
		hg = &Histogram{ID: id}
	}
	return count, hg, nil
}

// BuildColumn builds histogram from samples for column.
func BuildColumn(ctx context.Context, numBuckets, id int64, ndv int64, count int64, samples []types.Datum) (*Histogram, error) {
	if count == 0 {
		return &Histogram{ID: id}, nil
	}
	sc := ctx.GetSessionVars().StmtCtx
	err := types.SortDatums(sc, samples)
	if err != nil {
		return nil, errors.Trace(err)
	}
	hg := &Histogram{
		ID:      id,
		NDV:     ndv,
		Buckets: make([]Bucket, 1, numBuckets),
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
	for i := int64(0); i < int64(len(samples)); i++ {
		cmp, err := hg.Buckets[bucketIdx].Value.CompareDatum(sc, samples[i])
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
			hg.Buckets[bucketIdx].Value = samples[i]
			hg.Buckets[bucketIdx].Repeats = int64(ndvFactor)
		} else {
			lastCount = hg.Buckets[bucketIdx].Count
			// The bucket is full, store the item in the next bucket.
			bucketIdx++
			hg.Buckets = append(hg.Buckets, Bucket{
				Count:   int64(totalCount),
				Value:   samples[i],
				Repeats: int64(ndvFactor),
			})
		}
	}
	return hg, nil
}
