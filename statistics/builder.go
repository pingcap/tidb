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
	"strconv"
	"sync/atomic"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessionctx/varsutil"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
)

// Builder describes information needed by NewTable
type Builder struct {
	Ctx           context.Context  // Ctx is the context.
	TblInfo       *model.TableInfo // TblInfo is the table info of the table.
	StartTS       int64            // StartTS is the start timestamp of the statistics table builder.
	Count         int64            // Count is the total rows in the table.
	NumBuckets    int64            // NumBuckets is the number of buckets a column histogram has.
	ColumnSamples [][]types.Datum  // ColumnSamples is the sample of columns.
	ColOffsets    []int            // ColOffsets is the offset of columns in the table.
	IdxRecords    []ast.RecordSet  // IdxRecords is the record set of index columns.
	IdxOffsets    []int            // IdxOffsets is the offset of indices in the table.
	PkRecords     ast.RecordSet    // PkRecords is the record set of primary key of integer type.
	PkOffset      int              // PkOffset is the offset of primary key of integer type in the table.
}

func (b *Builder) buildMultiColumns(t *Table, offsets []int, baseOffset int, isSorted bool, done chan error) {
	for i, offset := range offsets {
		var err error
		if isSorted {
			err = t.build4SortedColumn(b.Ctx.GetSessionVars().StmtCtx, offset, b.IdxRecords[i+baseOffset], b.NumBuckets, false)
		} else {
			err = t.buildColumn(b.Ctx.GetSessionVars().StmtCtx, offset, b.ColumnSamples[i+baseOffset], b.NumBuckets)
		}
		if err != nil {
			done <- err
			return
		}
	}
	done <- nil
}

func (b *Builder) getBuildStatsConcurrency() (int, error) {
	sessionVars := b.Ctx.GetSessionVars()
	concurrency, err := varsutil.GetSessionSystemVar(sessionVars, variable.TiDBBuildStatsConcurrency)
	if err != nil {
		return 0, errors.Trace(err)
	}
	c, err := strconv.ParseInt(concurrency, 10, 64)
	return int(c), errors.Trace(err)
}

func (b *Builder) splitAndConcurrentBuild(t *Table, offsets []int, isSorted bool) error {
	offsetCnt := len(offsets)
	concurrency, err := b.getBuildStatsConcurrency()
	if err != nil {
		return errors.Trace(err)
	}
	groupSize := (offsetCnt + concurrency - 1) / concurrency
	splittedOffsets := make([][]int, 0, concurrency)
	for i := 0; i < offsetCnt; i += groupSize {
		end := i + groupSize
		if end > offsetCnt {
			end = offsetCnt
		}
		splittedOffsets = append(splittedOffsets, offsets[i:end])
	}
	doneCh := make(chan error, len(splittedOffsets))
	for i, offsets := range splittedOffsets {
		go b.buildMultiColumns(t, offsets, i*groupSize, isSorted, doneCh)
	}
	for range splittedOffsets {
		errc := <-doneCh
		if errc != nil && err == nil {
			err = errc
		}
	}
	return errors.Trace(err)
}

// NewTable creates a table statistics.
func (b *Builder) NewTable() (*Table, error) {
	if b.Count == 0 {
		return PseudoTable(b.TblInfo), nil
	}
	t := &Table{
		Info:    b.TblInfo,
		Count:   b.Count,
		Columns: make([]*Column, len(b.TblInfo.Columns)),
		Indices: make([]*Column, len(b.TblInfo.Indices)),
	}
	err := b.splitAndConcurrentBuild(t, b.ColOffsets, false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if b.PkOffset != -1 {
		err := t.build4SortedColumn(b.Ctx.GetSessionVars().StmtCtx, b.PkOffset, b.PkRecords, b.NumBuckets, true)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	err = b.splitAndConcurrentBuild(t, b.IdxOffsets, true)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, offset := range b.IdxOffsets {
		if len(b.TblInfo.Indices[offset].Columns) == 1 {
			for j, col := range b.TblInfo.Columns {
				if col.Name.L == b.TblInfo.Indices[offset].Columns[0].Name.L && t.Columns[j] == nil {
					t.Columns[j], err = copyFromIndexColumns(t.Indices[offset], col.ID, b.NumBuckets)
					if err != nil {
						return nil, errors.Trace(err)
					}
					break
				}
			}
		}
	}
	// There may be cases that we have no columnSamples, and only after we build the index columns that we can know it is 0,
	// so we should also checked it here.
	if t.Count == 0 {
		return PseudoTable(b.TblInfo), nil
	}
	// Some Indices may not need to have histograms, here we give them pseudo one to remove edge cases in pb.
	// However, it should never be used.
	for i, idx := range b.TblInfo.Indices {
		if t.Indices[i] == nil {
			t.Indices[i] = &Column{
				ID:  idx.ID,
				NDV: pseudoRowCount / 2,
			}
		}
	}
	return t, nil
}

// build4SortedColumn builds column statistics for sorted columns.
func (t *Table) build4SortedColumn(sc *variable.StatementContext, offset int, records ast.RecordSet, bucketCount int64, isPK bool) error {
	var id int64
	if isPK {
		id = t.Info.Columns[offset].ID
	} else {
		id = t.Info.Indices[offset].ID
	}
	col := &Column{
		ID:      id,
		NDV:     0,
		Numbers: make([]int64, 1, bucketCount),
		Values:  make([]types.Datum, 1, bucketCount),
		Repeats: make([]int64, 1, bucketCount),
	}
	var valuesPerBucket, lastNumber, bucketIdx int64 = 1, 0, 0
	count := int64(0)
	for {
		row, err := records.Next()
		if err != nil {
			return errors.Trace(err)
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
				return errors.Trace(err)
			}
			data = types.NewBytesDatum(bytes)
		}
		cmp, err := col.Values[bucketIdx].CompareDatum(sc, data)
		if err != nil {
			return errors.Trace(err)
		}
		count++
		if cmp == 0 {
			// The new item has the same value as current bucket value, to ensure that
			// a same value only stored in a single bucket, we do not increase bucketIdx even if it exceeds
			// valuesPerBucket.
			col.Numbers[bucketIdx]++
			col.Repeats[bucketIdx]++
		} else if col.Numbers[bucketIdx]+1-lastNumber <= valuesPerBucket {
			// The bucket still have room to store a new item, update the bucket.
			col.Numbers[bucketIdx]++
			col.Values[bucketIdx] = data
			col.Repeats[bucketIdx] = 0
			col.NDV++
		} else {
			// All buckets are full, we should merge buckets.
			if bucketIdx+1 == bucketCount {
				col.mergeBuckets(bucketIdx)
				valuesPerBucket *= 2
				bucketIdx = bucketIdx / 2
				if bucketIdx == 0 {
					lastNumber = 0
				} else {
					lastNumber = col.Numbers[bucketIdx-1]
				}
			}
			// We may merge buckets, so we should check it again.
			if col.Numbers[bucketIdx]+1-lastNumber <= valuesPerBucket {
				col.Numbers[bucketIdx]++
				col.Values[bucketIdx] = data
				col.Repeats[bucketIdx] = 0
			} else {
				lastNumber = col.Numbers[bucketIdx]
				bucketIdx++
				col.Numbers = append(col.Numbers, lastNumber+1)
				col.Values = append(col.Values, data)
				col.Repeats = append(col.Repeats, 0)
			}
			col.NDV++
		}
	}
	atomic.StoreInt64(&t.Count, count)
	if isPK {
		t.Columns[offset] = col
	} else {
		t.Indices[offset] = col
	}
	return nil
}

// buildColumn builds column statistics from samples.
func (t *Table) buildColumn(sc *variable.StatementContext, offset int, samples []types.Datum, bucketCount int64) error {
	err := types.SortDatums(sc, samples)
	if err != nil {
		return errors.Trace(err)
	}
	estimatedNDV, err := estimateNDV(sc, t.Count, samples)
	if err != nil {
		return errors.Trace(err)
	}
	ci := t.Info.Columns[offset]
	col := &Column{
		ID:      ci.ID,
		NDV:     estimatedNDV,
		Numbers: make([]int64, 1, bucketCount),
		Values:  make([]types.Datum, 1, bucketCount),
		Repeats: make([]int64, 1, bucketCount),
	}
	valuesPerBucket := t.Count/bucketCount + 1

	// As we use samples to build the histogram, the bucket number and repeat should multiply a factor.
	sampleFactor := t.Count / int64(len(samples))
	bucketIdx := 0
	var lastNumber int64
	for i := int64(0); i < int64(len(samples)); i++ {
		cmp, err := col.Values[bucketIdx].CompareDatum(sc, samples[i])
		if err != nil {
			return errors.Trace(err)
		}
		if cmp == 0 {
			// The new item has the same value as current bucket value, to ensure that
			// a same value only stored in a single bucket, we do not increase bucketIdx even if it exceeds
			// valuesPerBucket.
			col.Numbers[bucketIdx] = i * sampleFactor
			col.Repeats[bucketIdx] += sampleFactor
		} else if i*sampleFactor-lastNumber <= valuesPerBucket {
			// The bucket still have room to store a new item, update the bucket.
			col.Numbers[bucketIdx] = i * sampleFactor
			col.Values[bucketIdx] = samples[i]
			col.Repeats[bucketIdx] = 0
		} else {
			// The bucket is full, store the item in the next bucket.
			lastNumber = col.Numbers[bucketIdx]
			bucketIdx++
			col.Numbers = append(col.Numbers, i*sampleFactor)
			col.Values = append(col.Values, samples[i])
			col.Repeats = append(col.Repeats, 0)
		}
	}
	t.Columns[offset] = col
	return nil
}

// estimateNDV estimates the number of distinct value given a count and samples.
// It implements a simplified Good–Turing frequency estimation algorithm.
// See https://en.wikipedia.org/wiki/Good%E2%80%93Turing_frequency_estimation
func estimateNDV(sc *variable.StatementContext, count int64, samples []types.Datum) (int64, error) {
	lastValue := samples[0]
	occurrence := 1
	sampleDistinct := 1
	occurredOnceCount := 0
	for i := 1; i < len(samples); i++ {
		cmp, err := lastValue.CompareDatum(sc, samples[i])
		if err != nil {
			return 0, errors.Trace(err)
		}
		if cmp == 0 {
			occurrence++
		} else {
			if occurrence == 1 {
				occurredOnceCount++
			}
			sampleDistinct++
			occurrence = 1
		}
		lastValue = samples[i]
	}
	newValueProbability := float64(occurredOnceCount) / float64(len(samples))
	unsampledCount := float64(count) - float64(len(samples))
	estimatedDistinct := float64(sampleDistinct) + unsampledCount*newValueProbability
	return int64(estimatedDistinct), nil
}

func copyFromIndexColumns(ind *Column, id, numBuckets int64) (*Column, error) {
	col := &Column{
		ID:      id,
		NDV:     ind.NDV,
		Numbers: ind.Numbers,
		Values:  make([]types.Datum, 0, numBuckets),
		Repeats: ind.Repeats,
	}
	for _, val := range ind.Values {
		if val.GetBytes() == nil {
			break
		}
		data, err := codec.Decode(val.GetBytes(), 1)
		if err != nil {
			return nil, errors.Trace(err)
		}
		col.Values = append(col.Values, data[0])
	}
	return col, nil
}
