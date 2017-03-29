// Copyright 2016 PingCAP, Inc.
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
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessionctx/varsutil"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/types"
)

const (
	// Default number of buckets a column histogram has.
	defaultBucketCount = 256

	// When we haven't analyzed a table, we use pseudo statistics to estimate costs.
	// It has row count 10000000, equal condition selects 1/1000 of total rows, less condition selects 1/3 of total rows,
	// between condition selects 1/40 of total rows.
	pseudoRowCount    = 10000000
	pseudoEqualRate   = 1000
	pseudoLessRate    = 3
	pseudoBetweenRate = 40
)

// Column represents statistics for a column.
type Column struct {
	ID  int64 // Column ID.
	NDV int64 // Number of distinct values.

	// Histogram elements.
	//
	// A bucket number is the number of items stored in all previous buckets and the current bucket.
	// bucket numbers are always in increasing order.
	//
	// A bucket value is the greatest item value stored in the bucket.
	//
	// Repeat is the number of repeats of the bucket value, it can be used to find popular values.
	//
	// TODO: We could have make a bucket struct contains number, value and repeats.
	Numbers []int64
	Values  []types.Datum
	Repeats []int64
}

func (c *Column) saveToStorage(ctx context.Context, tableID int64, isIndex int) error {
	insertSQL := fmt.Sprintf("insert into mysql.stats_histograms (table_id, is_index, hist_id, distinct_count) values (%d, %d, %d, %d)", tableID, isIndex, c.ID, c.NDV)
	_, err := ctx.(sqlexec.SQLExecutor).Execute(insertSQL)
	if err != nil {
		return errors.Trace(err)
	}
	for i := 0; i < len(c.Numbers); i++ {
		var count int64
		if i == 0 {
			count = c.Numbers[i]
		} else {
			count = c.Numbers[i] - c.Numbers[i-1]
		}
		val, err := c.Values[i].ConvertTo(ctx.GetSessionVars().StmtCtx, types.NewFieldType(mysql.TypeBlob))
		if err != nil {
			return errors.Trace(err)
		}
		insertSQL = fmt.Sprintf("insert into mysql.stats_buckets values(%d, %d, %d, %d, %d, %d, X'%X')", tableID, isIndex, c.ID, i, count, c.Repeats[i], val.GetBytes())
		_, err = ctx.(sqlexec.SQLExecutor).Execute(insertSQL)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func colStatsFromStorage(ctx context.Context, tableID int64, colID int64, tp *types.FieldType, distinct int64, isIndex int) (*Column, error) {
	selSQL := fmt.Sprintf("select bucket_id, count, repeats, value from mysql.stats_buckets where table_id = %d and is_index = %d and hist_id = %d", tableID, isIndex, colID)
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, selSQL)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bucketSize := len(rows)
	if bucketSize == 0 {
		return nil, nil
	}
	colStats := &Column{
		ID:      colID,
		NDV:     distinct,
		Numbers: make([]int64, bucketSize),
		Repeats: make([]int64, bucketSize),
		Values:  make([]types.Datum, bucketSize),
	}
	for i := 0; i < bucketSize; i++ {
		bucketID := rows[i].Data[0].GetInt64()
		count := rows[i].Data[1].GetInt64()
		repeats := rows[i].Data[2].GetInt64()
		var value types.Datum
		if isIndex == 1 {
			value = rows[i].Data[3]
		} else {
			value, err = rows[i].Data[3].ConvertTo(ctx.GetSessionVars().StmtCtx, tp)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		colStats.Numbers[bucketID] = count
		colStats.Repeats[bucketID] = repeats
		colStats.Values[bucketID] = value
	}
	for i := 1; i < bucketSize; i++ {
		colStats.Numbers[i] += colStats.Numbers[i-1]
	}
	return colStats, nil
}

func (c *Column) String() string {
	strs := make([]string, 0, len(c.Numbers)+1)
	strs = append(strs, fmt.Sprintf("column:%d ndv:%d", c.ID, c.NDV))
	for i := range c.Numbers {
		strVal, _ := c.Values[i].ToString()
		strs = append(strs, fmt.Sprintf("num: %d\tvalue: %s\trepeats: %d", c.Numbers[i], strVal, c.Repeats[i]))
	}
	return strings.Join(strs, "\n")
}

// EqualRowCount estimates the row count where the column equals to value.
func (c *Column) EqualRowCount(sc *variable.StatementContext, value types.Datum) (int64, error) {
	if len(c.Numbers) == 0 {
		return pseudoRowCount / pseudoEqualRate, nil
	}
	index, match, err := c.search(sc, value)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if index == len(c.Numbers) {
		return c.Numbers[index-1] + 1, nil
	}
	if match {
		return c.Repeats[index] + 1, nil
	}
	totalCount := c.Numbers[len(c.Numbers)-1] + 1
	return totalCount / c.NDV, nil
}

// GreaterRowCount estimates the row count where the column greater than value.
func (c *Column) GreaterRowCount(sc *variable.StatementContext, value types.Datum) (int64, error) {
	if len(c.Numbers) == 0 {
		return pseudoRowCount / pseudoLessRate, nil
	}
	index, match, err := c.search(sc, value)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if index == 0 {
		return c.totalRowCount(), nil
	}
	number := c.Numbers[index]
	nextNumber := int64(0)
	if index < len(c.Numbers)-1 {
		nextNumber = c.Numbers[index+1]
	}
	greaterThanBucketValueCount := number - c.Repeats[index]
	if match {
		return greaterThanBucketValueCount, nil
	}
	return (nextNumber + greaterThanBucketValueCount) / 2, nil
}

// LessRowCount estimates the row count where the column less than value.
func (c *Column) LessRowCount(sc *variable.StatementContext, value types.Datum) (int64, error) {
	if len(c.Numbers) == 0 {
		return pseudoRowCount / pseudoLessRate, nil
	}
	index, match, err := c.search(sc, value)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if index == len(c.Numbers) {
		return c.totalRowCount(), nil
	}
	number := c.Numbers[index]
	prevNumber := int64(0)
	if index > 0 {
		prevNumber = c.Numbers[index-1]
	}
	lessThanBucketValueCount := number - c.Repeats[index]
	if match {
		return lessThanBucketValueCount, nil
	}
	return (prevNumber + lessThanBucketValueCount) / 2, nil
}

// BetweenRowCount estimates the row count where column greater or equal to a and less than b.
func (c *Column) BetweenRowCount(sc *variable.StatementContext, a, b types.Datum) (int64, error) {
	if len(c.Numbers) == 0 {
		return pseudoRowCount / pseudoBetweenRate, nil
	}
	lessCountA, err := c.LessRowCount(sc, a)
	if err != nil {
		return 0, errors.Trace(err)
	}
	lessCountB, err := c.LessRowCount(sc, b)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if lessCountA >= lessCountB {
		return c.inBucketBetweenCount(), nil
	}
	return lessCountB - lessCountA, nil
}

func (c *Column) totalRowCount() int64 {
	return c.Numbers[len(c.Numbers)-1] + 1
}

func (c *Column) bucketRowCount() int64 {
	return c.totalRowCount() / int64(len(c.Numbers))
}

func (c *Column) inBucketBetweenCount() int64 {
	return c.bucketRowCount()/3 + 1
}

func (c *Column) search(sc *variable.StatementContext, target types.Datum) (index int, match bool, err error) {
	index = sort.Search(len(c.Values), func(i int) bool {
		cmp, err1 := c.Values[i].CompareDatum(sc, target)
		if err1 != nil {
			err = errors.Trace(err1)
			return false
		}
		if cmp == 0 {
			match = true
		}
		return cmp >= 0
	})
	return
}

// mergeBuckets is used to merge every two neighbor buckets.
func (c *Column) mergeBuckets(bucketIdx int64) {
	curBuck := 0
	for i := int64(0); i+1 <= bucketIdx; i += 2 {
		c.Numbers[curBuck] = c.Numbers[i+1]
		c.Values[curBuck] = c.Values[i+1]
		c.Repeats[curBuck] = c.Repeats[i+1]
		curBuck++
	}
	if bucketIdx%2 == 0 {
		c.Numbers[curBuck] = c.Numbers[bucketIdx]
		c.Values[curBuck] = c.Values[bucketIdx]
		c.Repeats[curBuck] = c.Repeats[bucketIdx]
		curBuck++
	}
	c.Numbers = c.Numbers[:curBuck]
	c.Values = c.Values[:curBuck]
	c.Repeats = c.Repeats[:curBuck]
	return
}

// Table represents statistics for a table.
type Table struct {
	Info    *model.TableInfo
	Columns []*Column
	Indices []*Column
	Count   int64 // Total row count in a table.
	Pseudo  bool
}

// SaveToStorage saves stats table to storage.
func (t *Table) SaveToStorage(ctx context.Context) error {
	_, err := ctx.(sqlexec.SQLExecutor).Execute("begin")
	if err != nil {
		return errors.Trace(err)
	}
	txn := ctx.Txn()
	version := txn.StartTS()
	SetStatisticsTableCache(t.Info.ID, t, version)
	deleteSQL := fmt.Sprintf("delete from mysql.stats_meta where table_id = %d", t.Info.ID)
	_, err = ctx.(sqlexec.SQLExecutor).Execute(deleteSQL)
	if err != nil {
		return errors.Trace(err)
	}
	insertSQL := fmt.Sprintf("insert into mysql.stats_meta (version, table_id, count) values (%d, %d, %d)", version, t.Info.ID, t.Count)
	_, err = ctx.(sqlexec.SQLExecutor).Execute(insertSQL)
	if err != nil {
		return errors.Trace(err)
	}
	deleteSQL = fmt.Sprintf("delete from mysql.stats_histograms where table_id = %d", t.Info.ID)
	_, err = ctx.(sqlexec.SQLExecutor).Execute(deleteSQL)
	if err != nil {
		return errors.Trace(err)
	}
	deleteSQL = fmt.Sprintf("delete from mysql.stats_buckets where table_id = %d", t.Info.ID)
	_, err = ctx.(sqlexec.SQLExecutor).Execute(deleteSQL)
	if err != nil {
		return errors.Trace(err)
	}
	for _, col := range t.Columns {
		err = col.saveToStorage(ctx, t.Info.ID, 0)
		if err != nil {
			return errors.Trace(err)
		}
	}
	for _, idx := range t.Indices {
		err = idx.saveToStorage(ctx, t.Info.ID, 1)
		if err != nil {
			return errors.Trace(err)
		}
	}
	_, err = ctx.(sqlexec.SQLExecutor).Execute("commit")
	return errors.Trace(err)
}

// TableStatsFromStorage loads table stats info from storage.
func TableStatsFromStorage(ctx context.Context, info *model.TableInfo, count int64) (*Table, error) {
	table := &Table{
		Info:  info,
		Count: count,
	}
	selSQL := fmt.Sprintf("select table_id, is_index, hist_id, distinct_count from mysql.stats_histograms where table_id = %d", info.ID)
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, selSQL)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// indexCount and columnCount record the number of indices and columns in table stats. If the number don't match with
	// tableInfo, we will return pseudo table.
	// TODO: In fact, we can return pseudo column.
	indexCount, columnCount := 0, 0
	for _, row := range rows {
		distinct := row.Data[3].GetInt64()
		histID := row.Data[2].GetInt64()
		if row.Data[1].GetInt64() > 0 {
			// process index
			var col *Column
			for _, idxInfo := range info.Indices {
				if histID == idxInfo.ID {
					col, err = colStatsFromStorage(ctx, info.ID, histID, nil, distinct, 1)
					if err != nil {
						return nil, errors.Trace(err)
					}
					break
				}
			}
			if col != nil {
				table.Indices = append(table.Indices, col)
				indexCount++
			} else {
				log.Warnf("We cannot find index id %d in table %s now. It may be deleted.", histID, info.Name)
			}
		} else {
			// process column
			var col *Column
			for _, colInfo := range info.Columns {
				if histID == colInfo.ID {
					col, err = colStatsFromStorage(ctx, info.ID, histID, &colInfo.FieldType, distinct, 0)
					if err != nil {
						return nil, errors.Trace(err)
					}
					break
				}
			}
			if col != nil {
				table.Columns = append(table.Columns, col)
				columnCount++
			} else {
				log.Warnf("We cannot find column id %d in table %s now. It may be deleted.", histID, info.Name)
			}
		}
	}
	if indexCount != len(info.Indices) {
		return nil, errors.New("The number of indices doesn't match with the schema")
	}
	if columnCount != len(info.Columns) {
		return nil, errors.New("The number of columns doesn't match with the schema")
	}
	return table, nil
}

// String implements Stringer interface.
func (t *Table) String() string {
	strs := make([]string, 0, len(t.Columns)+1)
	strs = append(strs, fmt.Sprintf("Table:%d count:%d", t.Info.ID, t.Count))
	for _, col := range t.Columns {
		strs = append(strs, col.String())
	}
	return strings.Join(strs, "\n")
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

// PseudoTable creates a pseudo table statistics when statistic can not be found in KV store.
func PseudoTable(ti *model.TableInfo) *Table {
	t := &Table{Info: ti, Pseudo: true}
	t.Count = pseudoRowCount
	t.Columns = make([]*Column, len(ti.Columns))
	t.Indices = make([]*Column, len(ti.Indices))
	for i, v := range ti.Columns {
		c := &Column{
			ID:  v.ID,
			NDV: pseudoRowCount / 2,
		}
		t.Columns[i] = c
	}
	for i, v := range ti.Indices {
		c := &Column{
			ID:  v.ID,
			NDV: pseudoRowCount / 2,
		}
		t.Indices[i] = c
	}
	return t
}
