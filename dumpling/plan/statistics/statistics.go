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
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
)

const (
	// Default number of buckets a column histogram has.
	defaultBucketCount = 256

	// When we haven't analyzed a table, we use pseudo statistics to estimate costs.
	// It has row count 10000, equal condition selects 1/10 of total rows, less condition selects 1/3 of total rows,
	// between condition selects 1/4 of total rows.
	pseudoRowCount    = 10000
	pseudoEqualRate   = 200
	pseudoLessRate    = 3
	pseudoBetweenRate = 4
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
func (c *Column) EqualRowCount(value types.Datum) (int64, error) {
	if len(c.Numbers) == 0 {
		return pseudoRowCount / pseudoEqualRate, nil
	}
	index, match, err := c.search(value)
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
func (c *Column) GreaterRowCount(value types.Datum) (int64, error) {
	if len(c.Numbers) == 0 {
		return pseudoRowCount / pseudoLessRate, nil
	}
	index, match, err := c.search(value)
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
func (c *Column) LessRowCount(value types.Datum) (int64, error) {
	if len(c.Numbers) == 0 {
		return pseudoRowCount / pseudoLessRate, nil
	}
	index, match, err := c.search(value)
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
func (c *Column) BetweenRowCount(a, b types.Datum) (int64, error) {
	if len(c.Numbers) == 0 {
		return pseudoRowCount / pseudoBetweenRate, nil
	}
	lessCountA, err := c.LessRowCount(a)
	if err != nil {
		return 0, errors.Trace(err)
	}
	lessCountB, err := c.LessRowCount(b)
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

func (c *Column) search(target types.Datum) (index int, match bool, err error) {
	index = sort.Search(len(c.Values), func(i int) bool {
		cmp, err1 := c.Values[i].CompareDatum(target)
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

// Table represents statistics for a table.
type Table struct {
	info    *model.TableInfo
	TS      int64 // build timestamp.
	Columns []*Column
	Count   int64 // Total row count in a table.
}

// String implements Stringer interface.
func (t *Table) String() string {
	strs := make([]string, 0, len(t.Columns)+1)
	strs = append(strs, fmt.Sprintf("Table:%d ts:%d count:%d", t.info.ID, t.TS, t.Count))
	for _, col := range t.Columns {
		strs = append(strs, col.String())
	}
	return strings.Join(strs, "\n")
}

// ToPB converts Table to TablePB.
func (t *Table) ToPB() (*TablePB, error) {
	tblPB := &TablePB{
		Id:      proto.Int64(t.info.ID),
		Ts:      proto.Int64(t.TS),
		Count:   proto.Int64(t.Count),
		Columns: make([]*ColumnPB, len(t.Columns)),
	}
	for i, col := range t.Columns {
		data, err := codec.EncodeValue(nil, col.Values...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		tblPB.Columns[i] = &ColumnPB{
			Id:      proto.Int64(col.ID),
			Ndv:     proto.Int64(col.NDV),
			Numbers: col.Numbers,
			Value:   data,
			Repeats: col.Repeats,
		}
	}
	return tblPB, nil
}

// buildColumn builds column statistics from samples.
func (t *Table) buildColumn(offset int, samples []types.Datum, bucketCount int64) error {
	err := types.SortDatums(samples)
	if err != nil {
		return errors.Trace(err)
	}
	estimatedNDV, err := estimateNDV(t.Count, samples)
	if err != nil {
		return errors.Trace(err)
	}
	ci := t.info.Columns[offset]
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
		cmp, err := col.Values[bucketIdx].CompareDatum(samples[i])
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
func estimateNDV(count int64, samples []types.Datum) (int64, error) {
	lastValue := samples[0]
	occurrence := 1
	sampleDistinct := 1
	occurredOnceCount := 0
	for i := 1; i < len(samples); i++ {
		cmp, err := lastValue.CompareDatum(samples[i])
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

// NewTable creates a table statistics.
func NewTable(ti *model.TableInfo, ts, count, numBuckets int64, columnSamples [][]types.Datum) (*Table, error) {
	t := &Table{
		info:    ti,
		TS:      ts,
		Count:   count,
		Columns: make([]*Column, len(columnSamples)),
	}
	for i, sample := range columnSamples {
		err := t.buildColumn(i, sample, defaultBucketCount)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return t, nil
}

// TableFromPB creates a table statistics from protobuffer.
func TableFromPB(ti *model.TableInfo, tpb *TablePB) (*Table, error) {
	if tpb.GetId() != ti.ID {
		return nil, errors.Errorf("table id not match, expected %d, got %d", ti.ID, tpb.GetId())
	}
	if len(tpb.Columns) != len(ti.Columns) {
		return nil, errors.Errorf("column count not match, expected %d, got %d", len(ti.Columns), len(tpb.Columns))
	}
	for i := range ti.Columns {
		if ti.Columns[i].ID != tpb.Columns[i].GetId() {
			return nil, errors.Errorf("column ID not match, expected %d, got %d", ti.Columns[i].ID, tpb.Columns[i].GetId())
		}
	}
	t := &Table{info: ti}
	t.TS = tpb.GetTs()
	t.Count = tpb.GetCount()
	t.Columns = make([]*Column, len(tpb.GetColumns()))
	for i, cInfo := range t.info.Columns {
		cpb := tpb.Columns[i]
		values, err := codec.Decode(cpb.GetValue(), 1)
		if err != nil {
			return nil, errors.Trace(err)
		}
		c := &Column{
			ID:      cpb.GetId(),
			NDV:     cpb.GetNdv(),
			Numbers: cpb.GetNumbers(),
			Values:  make([]types.Datum, len(values)),
			Repeats: cpb.GetRepeats(),
		}
		for i, val := range values {
			c.Values[i], err = tablecodec.Unflatten(val, &cInfo.FieldType, false)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		t.Columns[i] = c
	}
	return t, nil
}

// PseudoTable creates a pseudo table statistics when statistic can not be found in KV store.
func PseudoTable(ti *model.TableInfo) *Table {
	t := &Table{info: ti}
	t.TS = pseudoTimestamp
	t.Count = pseudoRowCount
	t.Columns = make([]*Column, len(ti.Columns))
	for i, v := range ti.Columns {
		c := &Column{
			ID:  v.ID,
			NDV: pseudoRowCount / 2,
		}
		t.Columns[i] = c
	}
	return t
}
