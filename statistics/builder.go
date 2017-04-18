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
	hg    *Histogram
}

func (b *Builder) buildMultiHistograms(t *Table, IDs []int64, baseOffset int, isSorted bool) {
	for i, id := range IDs {
		if isSorted {
			hg, err := b.buildIndex(t, b.Ctx.GetSessionVars().StmtCtx, id, b.IdxRecords[i+baseOffset], b.NumBuckets, false)
			b.doneCh <- &buildStatsTask{err: err, index: true, hg: hg}
		} else {
			hg, err := b.buildColumn(t, b.Ctx.GetSessionVars().StmtCtx, id, b.ColNDVs[i+baseOffset], b.ColumnSamples[i+baseOffset], b.NumBuckets)
			b.doneCh <- &buildStatsTask{err: err, hg: hg}
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
		go b.buildMultiHistograms(t, IDs, i*groupSize, isSorted)
	}
	for range IDs {
		task := <-b.doneCh
		if task.err != nil {
			return errors.Trace(task.err)
		}
		if task.index {
			t.Indices[task.hg.ID] = &Index{Histogram: *task.hg, NumColumns: indexNumColumnsByID(b.TblInfo, task.hg.ID)}
		} else {
			t.Columns[task.hg.ID] = &Column{Histogram: *task.hg}
		}
	}
	return nil
}

// NewTable creates a table statistics.
func (b *Builder) NewTable() (*Table, error) {
	t := &Table{
		tableID: b.TblInfo.ID,
		Count:   b.Count,
		Columns: make(map[int64]*Column, len(b.TblInfo.Columns)),
		Indices: make(map[int64]*Index, len(b.TblInfo.Indices)),
	}
	if b.Count == 0 {
		for _, col := range b.TblInfo.Columns {
			t.Columns[col.ID] = &Column{Histogram{ID: col.ID}}
		}
		for _, idx := range b.TblInfo.Indices {
			t.Indices[idx.ID] = &Index{
				Histogram:  Histogram{ID: idx.ID},
				NumColumns: len(idx.Columns),
			}
		}
		return t, nil
	}
	b.doneCh = make(chan *buildStatsTask, len(b.ColIDs)+len(b.IdxIDs))
	err := b.splitAndConcurrentBuild(t, b.ColIDs, false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if b.PkID != 0 {
		hg, err := b.buildIndex(t, b.Ctx.GetSessionVars().StmtCtx, b.PkID, b.PkRecords, b.NumBuckets, true)
		if err != nil {
			return nil, errors.Trace(err)
		}
		t.Columns[hg.ID] = &Column{Histogram: *hg}
	}
	err = b.splitAndConcurrentBuild(t, b.IdxIDs, true)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// The sorted bucket is more accurate, we replace the sampled column histogram with index histogram if the
	// index is single column index.
	for _, idxInfo := range b.TblInfo.Indices {
		if idxInfo != nil && len(idxInfo.Columns) == 1 {
			columnOffset := idxInfo.Columns[0].Offset
			columnID := b.TblInfo.Columns[columnOffset].ID
			t.Columns[columnID], err = copyFromIndexColumns(t.Indices[idxInfo.ID], columnID)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}
	// There may be cases that we have no columnSamples, and only after we build the index columns that we can know it is 0,
	// so we should also checked it here.
	if t.Count == 0 {
		for _, col := range b.TblInfo.Columns {
			t.Columns[col.ID] = &Column{Histogram{ID: col.ID}}
		}
		for _, idx := range b.TblInfo.Indices {
			t.Indices[idx.ID] = &Index{
				Histogram:  Histogram{ID: idx.ID},
				NumColumns: len(idx.Columns),
			}
		}
	}
	return t, nil
}

func indexNumColumnsByID(tblInfo *model.TableInfo, idxID int64) int {
	for _, idx := range tblInfo.Indices {
		if idx.ID == idxID {
			return len(idx.Columns)
		}
	}
	return 0
}

// buildIndex builds histogram for index.
func (b *Builder) buildIndex(t *Table, sc *variable.StatementContext, id int64, records ast.RecordSet, bucketCount int64, isPK bool) (*Histogram, error) {
	hg := &Histogram{
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
		cmp, err := hg.Buckets[bucketIdx].Value.CompareDatum(sc, data)
		if err != nil {
			return nil, errors.Trace(err)
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
			if bucketIdx+1 == bucketCount {
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
				hg.Buckets = append(hg.Buckets, bucket{
					Count:   lastNumber + 1,
					Value:   data,
					Repeats: 1,
				})
			}
			hg.NDV++
		}
	}
	atomic.StoreInt64(&t.Count, count)
	return hg, nil
}

// buildColumn builds histogram from samples for column.
func (b *Builder) buildColumn(t *Table, sc *variable.StatementContext, id int64, ndv int64, samples []types.Datum, bucketCount int64) (*Histogram, error) {
	err := types.SortDatums(sc, samples)
	if err != nil {
		return nil, errors.Trace(err)
	}
	hg := &Histogram{
		ID:      id,
		NDV:     ndv,
		Buckets: make([]bucket, 1, bucketCount),
	}
	valuesPerBucket := float64(t.Count)/float64(bucketCount) + 1

	// As we use samples to build the histogram, the bucket number and repeat should multiply a factor.
	sampleFactor := float64(t.Count) / float64(len(samples))
	ndvFactor := float64(t.Count) / float64(ndv)
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
			hg.Buckets = append(hg.Buckets, bucket{
				Count:   int64(totalCount),
				Value:   samples[i],
				Repeats: int64(ndvFactor),
			})
		}
	}
	return hg, nil
}

// Index histogram is encoded, it need to be decoded to be used as column histogram.
// TODO: use field type to decode the value.
func copyFromIndexColumns(ind *Index, id int64) (*Column, error) {
	hg := Histogram{
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
		hg.Buckets = append(hg.Buckets, bucket{
			Count:   b.Count,
			Value:   data[0],
			Repeats: b.Repeats,
		})
	}
	return &Column{Histogram: hg}, nil
}
