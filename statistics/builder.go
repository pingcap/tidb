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
	Ctx           context.Context      // Ctx is the context.
	TblInfo       *model.TableInfo     // TblInfo is the table info of the table.
	StartTS       int64                // StartTS is the start timestamp of the statistics table builder.
	Count         int64                // Count is the total rows in the table.
	NumBuckets    int64                // NumBuckets is the number of buckets a column histogram has.
	ColumnSamples [][]types.Datum      // ColumnSamples is the sample of columns.
	ColIDs        []int64              // ColIDs is the id of columns in the table.
	ColNDVs       []int64              // ColNDVs is the NDV of columns.
	IdxRecords    []ast.RecordSet      // IdxRecords is the record set of index columns.
	IdxIDs        []int64              // IdxIDs is the id of indices in the table.
	PkRecords     ast.RecordSet        // PkRecords is the record set of primary key of integer type.
	PkID          int64                // PkID is the id of primary key with integer type in the table.
	doneCh        chan *buildStatsTask // doneCh is the channel that stores stats building task.
}

type buildStatsTask struct {
	err   error
	index bool
	t     *Table
	col   *Column
}

func (b *Builder) buildMultiColumns(t *Table, IDs []int64, baseOffset int, isSorted bool) {
	for i, id := range IDs {
		var err error
		var col *Column
		if isSorted {
			col, err = b.build4SortedColumn(t, b.Ctx.GetSessionVars().StmtCtx, id, b.IdxRecords[i+baseOffset], b.NumBuckets, false)
			b.doneCh <- &buildStatsTask{err: err, index: true, t: t, col: col}
		} else {
			col, err = b.buildColumn(t, b.Ctx.GetSessionVars().StmtCtx, id, b.ColNDVs[i+baseOffset], b.ColumnSamples[i+baseOffset], b.NumBuckets)
			b.doneCh <- &buildStatsTask{err: err, t: t, col: col}
		}
	}
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

func (b *Builder) splitAndConcurrentBuild(t *Table, IDs []int64, isSorted bool) error {
	IDCnt := len(IDs)
	concurrency, err := b.getBuildStatsConcurrency()
	if err != nil {
		return errors.Trace(err)
	}
	groupSize := (IDCnt + concurrency - 1) / concurrency
	splitIDs := make([][]int64, 0, concurrency)
	for i := 0; i < IDCnt; i += groupSize {
		end := i + groupSize
		if end > IDCnt {
			end = IDCnt
		}
		splitIDs = append(splitIDs, IDs[i:end])
	}
	for i, IDs := range splitIDs {
		go b.buildMultiColumns(t, IDs, i*groupSize, isSorted)
	}
	for range IDs {
		task := <-b.doneCh
		if task.err != nil {
			return errors.Trace(task.err)
		}
		if task.index {
			task.t.Indices[task.col.ID] = task.col
		} else {
			task.t.Columns[task.col.ID] = task.col
		}
	}
	return nil
}

// NewTable creates a table statistics.
func (b *Builder) NewTable() (*Table, error) {
	t := &Table{
		Info:    b.TblInfo,
		Count:   b.Count,
		Columns: make(map[int64]*Column, len(b.TblInfo.Columns)),
		Indices: make(map[int64]*Column, len(b.TblInfo.Indices)),
	}
	if b.Count == 0 {
		for i := range t.Columns {
			t.Columns[i] = &Column{ID: b.TblInfo.Columns[i].ID}
		}
		for i := range t.Indices {
			t.Indices[i] = &Column{ID: b.TblInfo.Indices[i].ID}
		}
		return t, nil
	}
	b.doneCh = make(chan *buildStatsTask, len(b.ColIDs)+len(b.IdxIDs))
	err := b.splitAndConcurrentBuild(t, b.ColIDs, false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if b.PkID != 0 {
		col, err := b.build4SortedColumn(t, b.Ctx.GetSessionVars().StmtCtx, b.PkID, b.PkRecords, b.NumBuckets, true)
		if err != nil {
			return nil, errors.Trace(err)
		}
		t.Columns[col.ID] = col
	}
	err = b.splitAndConcurrentBuild(t, b.IdxIDs, true)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, idx := range b.TblInfo.Indices {
		if len(idx.Columns) == 1 {
			for _, col := range b.TblInfo.Columns {
				if col.Name.L == idx.Columns[0].Name.L && t.Columns[col.ID] == nil {
					t.Columns[col.ID], err = copyFromIndexColumns(t.Indices[idx.ID], col.ID)
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
		for i := range t.Columns {
			t.Columns[i] = &Column{ID: i}
		}
		for i := range t.Indices {
			t.Indices[i] = &Column{ID: i}
		}
	}
	return t, nil
}

// build4SortedColumn builds column statistics for sorted columns.
func (b *Builder) build4SortedColumn(t *Table, sc *variable.StatementContext, id int64, records ast.RecordSet, bucketCount int64, isPK bool) (*Column, error) {
	col := &Column{
		ID:      id,
		NDV:     0,
		Buckets: make([]bucket, 1, bucketCount),
	}
	var valuesPerBucket, lastNumber, bucketIdx int64 = 1, 0, 0
	count := int64(0)
	for {
		row, err := records.Next()
		if err != nil {
			return nil, errors.Trace(err)
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
				return nil, errors.Trace(err)
			}
			data = types.NewBytesDatum(bytes)
		}
		cmp, err := col.Buckets[bucketIdx].Value.CompareDatum(sc, data)
		if err != nil {
			return nil, errors.Trace(err)
		}
		count++
		if cmp == 0 {
			// The new item has the same value as current bucket value, to ensure that
			// a same value only stored in a single bucket, we do not increase bucketIdx even if it exceeds
			// valuesPerBucket.
			col.Buckets[bucketIdx].Count++
			col.Buckets[bucketIdx].Repeats++
		} else if col.Buckets[bucketIdx].Count+1-lastNumber <= valuesPerBucket {
			// The bucket still have room to store a new item, update the bucket.
			col.Buckets[bucketIdx].Count++
			col.Buckets[bucketIdx].Value = data
			col.Buckets[bucketIdx].Repeats = 1
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
					lastNumber = col.Buckets[bucketIdx-1].Count
				}
			}
			// We may merge buckets, so we should check it again.
			if col.Buckets[bucketIdx].Count+1-lastNumber <= valuesPerBucket {
				col.Buckets[bucketIdx].Count++
				col.Buckets[bucketIdx].Value = data
				col.Buckets[bucketIdx].Repeats = 1
			} else {
				lastNumber = col.Buckets[bucketIdx].Count
				bucketIdx++
				col.Buckets = append(col.Buckets, bucket{
					Count:   lastNumber + 1,
					Value:   data,
					Repeats: 1,
				})
			}
			col.NDV++
		}
	}
	atomic.StoreInt64(&t.Count, count)
	return col, nil
}

// buildColumn builds column statistics from samples.
func (b *Builder) buildColumn(t *Table, sc *variable.StatementContext, id int64, ndv int64, samples []types.Datum, bucketCount int64) (*Column, error) {
	err := types.SortDatums(sc, samples)
	if err != nil {
		return nil, errors.Trace(err)
	}
	col := &Column{
		ID:      id,
		NDV:     ndv,
		Buckets: make([]bucket, 1, bucketCount),
	}
	valuesPerBucket := t.Count/bucketCount + 1

	// As we use samples to build the histogram, the bucket number and repeat should multiply a factor.
	sampleFactor := t.Count / int64(len(samples))
	ndvFactor := t.Count / ndv
	if ndvFactor > sampleFactor {
		ndvFactor = sampleFactor
	}
	bucketIdx := 0
	var lastCount int64
	for i := int64(0); i < int64(len(samples)); i++ {
		cmp, err := col.Buckets[bucketIdx].Value.CompareDatum(sc, samples[i])
		if err != nil {
			return nil, errors.Trace(err)
		}
		totalCount := (i + 1) * sampleFactor
		if cmp == 0 {
			// The new item has the same value as current bucket value, to ensure that
			// a same value only stored in a single bucket, we do not increase bucketIdx even if it exceeds
			// valuesPerBucket.
			col.Buckets[bucketIdx].Count = totalCount
			if col.Buckets[bucketIdx].Repeats == ndvFactor {
				col.Buckets[bucketIdx].Repeats = 2 * sampleFactor
			} else {
				col.Buckets[bucketIdx].Repeats += sampleFactor
			}
		} else if totalCount-lastCount <= valuesPerBucket {
			// TODO: Making sampleFactor as float may be better.
			// The bucket still have room to store a new item, update the bucket.
			col.Buckets[bucketIdx].Count = totalCount
			col.Buckets[bucketIdx].Value = samples[i]
			col.Buckets[bucketIdx].Repeats = ndvFactor
		} else {
			lastCount = col.Buckets[bucketIdx].Count
			// The bucket is full, store the item in the next bucket.
			bucketIdx++
			col.Buckets = append(col.Buckets, bucket{
				Count:   totalCount,
				Value:   samples[i],
				Repeats: ndvFactor,
			})
		}
	}
	return col, nil
}

func copyFromIndexColumns(ind *Column, id int64) (*Column, error) {
	col := &Column{
		ID:      id,
		NDV:     ind.NDV,
		Buckets: make([]bucket, 0, len(ind.Buckets)),
	}
	for _, b := range ind.Buckets {
		val := b.Value
		if val.GetBytes() == nil {
			break
		}
		data, err := codec.Decode(val.GetBytes(), 1)
		if err != nil {
			return nil, errors.Trace(err)
		}
		col.Buckets = append(col.Buckets, bucket{
			Count:   b.Count,
			Value:   data[0],
			Repeats: b.Repeats,
		})
	}
	return col, nil
}
