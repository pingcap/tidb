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
	"fmt"
	"sort"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/types"
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
	if index >= len(c.Numbers) {
		return 0, nil
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
