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
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
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
	pseudoTimestamp   = 1
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
	// We could have make a bucket struct contains number, value and repeat, but that would be harder to
	// serialize, so we store every bucket field in its own slice instead. those slices all have
	// the bucket count length.
	Numbers []int64
	Values  []types.Datum
	Repeats []int64
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

// columnToPB converts Column to ColumnPB.
func columnToPB(col *Column) (*ColumnPB, error) {
	data, err := codec.EncodeValue(nil, col.Values...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	cpb := &ColumnPB{
		Id:      proto.Int64(col.ID),
		Ndv:     proto.Int64(col.NDV),
		Numbers: col.Numbers,
		Value:   data,
		Repeats: col.Repeats,
	}
	return cpb, nil
}

// columnFromPB gets Column from ColumnPB.
func columnFromPB(cpb *ColumnPB, ft *types.FieldType) (*Column, error) {
	var values []types.Datum
	var err error
	if len(cpb.GetValue()) > 0 {
		values, err = codec.Decode(cpb.GetValue(), 1)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	c := &Column{
		ID:      cpb.GetId(),
		NDV:     cpb.GetNdv(),
		Numbers: cpb.GetNumbers(),
		Values:  make([]types.Datum, len(values)),
		Repeats: cpb.GetRepeats(),
	}
	for i, val := range values {
		c.Values[i], err = tablecodec.Unflatten(val, ft, false)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return c, nil
}

// Table represents statistics for a table.
type Table struct {
	Info    *model.TableInfo
	TS      int64 // build timestamp.
	Columns []*Column
	Indices []*Column
	Count   int64 // Total row count in a table.
	Pseudo  bool
}

// String implements Stringer interface.
func (t *Table) String() string {
	strs := make([]string, 0, len(t.Columns)+1)
	strs = append(strs, fmt.Sprintf("Table:%d ts:%d count:%d", t.Info.ID, t.TS, t.Count))
	for _, col := range t.Columns {
		strs = append(strs, col.String())
	}
	return strings.Join(strs, "\n")
}

// ToPB converts Table to TablePB.
func (t *Table) ToPB() (*TablePB, error) {
	tblPB := &TablePB{
		Id:      proto.Int64(t.Info.ID),
		Ts:      proto.Int64(t.TS),
		Count:   proto.Int64(t.Count),
		Columns: make([]*ColumnPB, len(t.Columns)),
		Indices: make([]*ColumnPB, len(t.Indices)),
	}
	for i, col := range t.Columns {
		cpb, err := columnToPB(col)
		if err != nil {
			return nil, errors.Trace(err)
		}
		tblPB.Columns[i] = cpb
	}
	for i, col := range t.Indices {
		cpb, err := columnToPB(col)
		if err != nil {
			return nil, errors.Trace(err)
		}
		tblPB.Indices[i] = cpb
	}
	return tblPB, nil
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
	knowCount := true
	if t.Count < 0 {
		t.Count = 0
		knowCount = false
	}
	if knowCount {
		valuesPerBucket = t.Count/bucketCount + 1
	}
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
		if !knowCount {
			t.Count++
		}
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
			if !knowCount && bucketIdx+1 == bucketCount {
				col.mergeBuckets(bucketIdx)
				valuesPerBucket *= 2
				bucketIdx = (bucketIdx + 1) / 2
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
	Sc            *variable.StatementContext // Sc is the statement context.
	TblInfo       *model.TableInfo           // TblInfo is the table info of the table.
	StartTS       int64                      // StartTS is the start timestamp of the statistics table builder.
	Count         int64                      // Count is the total rows in the table.
	NumBuckets    int64                      // NumBuckets is the number of buckets a column histogram has.
	ColumnSamples [][]types.Datum            // ColumnSamples is the sample of columns.
	ColOffsets    []int                      // ColOffsets is the offset of columns in the table.
	IdxRecords    []ast.RecordSet            // IdxRecords is the record set of index columns.
	IdxOffsets    []int                      // IdxOffsets is the offset of indices in the table.
	PkRecords     ast.RecordSet              // PkRecords is the record set of primary key of integer type.
	PkOffset      int                        // PkOffset is the offset of primary key of integer type in the table.
}

// NewTable creates a table statistics.
func (b *Builder) NewTable() (*Table, error) {
	if b.Count == 0 {
		return PseudoTable(b.TblInfo), nil
	}
	t := &Table{
		Info:    b.TblInfo,
		TS:      b.StartTS,
		Count:   b.Count,
		Columns: make([]*Column, len(b.TblInfo.Columns)),
		Indices: make([]*Column, len(b.TblInfo.Indices)),
	}
	for i, offset := range b.ColOffsets {
		err := t.buildColumn(b.Sc, offset, b.ColumnSamples[i], b.NumBuckets)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if b.PkOffset != -1 {
		err := t.build4SortedColumn(b.Sc, b.PkOffset, b.PkRecords, b.NumBuckets, true)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	for i, offset := range b.IdxOffsets {
		err := t.build4SortedColumn(b.Sc, offset, b.IdxRecords[i], b.NumBuckets, false)
		if err != nil {
			return nil, errors.Trace(err)
		}
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

// TableFromPB creates a table statistics from protobuffer.
func TableFromPB(ti *model.TableInfo, tpb *TablePB) (*Table, error) {
	// TODO: The following error may mean that there is a ddl change on this table. Currently, The caller simply drop the statistics table. Maybe we can have better solution.
	if tpb.GetId() != ti.ID {
		return nil, errors.Errorf("table id not match, expected %d, got %d", ti.ID, tpb.GetId())
	}
	if len(tpb.Columns) != len(ti.Columns) {
		return nil, errors.Errorf("column count not match, expected %d, got %d", len(ti.Columns), len(tpb.Columns))
	}
	if len(tpb.Indices) != len(ti.Indices) {
		return nil, errors.Errorf("indices count not match, expected %d, got %d", len(ti.Indices), len(tpb.Indices))
	}
	for i := range ti.Columns {
		if ti.Columns[i].ID != tpb.Columns[i].GetId() {
			return nil, errors.Errorf("column ID not match, expected %d, got %d", ti.Columns[i].ID, tpb.Columns[i].GetId())
		}
	}
	for i := range ti.Indices {
		if ti.Indices[i].ID != tpb.Indices[i].GetId() {
			return nil, errors.Errorf("index column ID not match, expected %d, got %d", ti.Indices[i].ID, tpb.Indices[i].GetId())
		}
	}
	t := &Table{Info: ti}
	t.TS = tpb.GetTs()
	t.Count = tpb.GetCount()
	t.Columns = make([]*Column, len(tpb.GetColumns()))
	t.Indices = make([]*Column, len(tpb.GetIndices()))
	for i, cInfo := range t.Info.Columns {
		c, err := columnFromPB(tpb.Columns[i], &cInfo.FieldType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		t.Columns[i] = c
	}
	for i := range t.Info.Indices {
		c, err := columnFromPB(tpb.Indices[i], types.NewFieldType(types.KindBytes))
		if err != nil {
			return nil, errors.Trace(err)
		}
		t.Indices[i] = c
	}
	return t, nil
}

// PseudoTable creates a pseudo table statistics when statistic can not be found in KV store.
func PseudoTable(ti *model.TableInfo) *Table {
	t := &Table{Info: ti, Pseudo: true}
	t.TS = pseudoTimestamp
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
