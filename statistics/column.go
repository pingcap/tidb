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

	Buckets []bucket
}

// bucket is an element of histogram.
//
// A bucket count is the number of items stored in all previous buckets and the current bucket.
// bucket numbers are always in increasing order.
//
// A bucket value is the greatest item value stored in the bucket.
//
// Repeat is the number of repeats of the bucket value, it can be used to find popular values.
//
type bucket struct {
	Count   int64
	Value   types.Datum
	Repeats int64
}

func (c *Column) saveToStorage(ctx context.Context, tableID int64, isIndex int) error {
	insertSQL := fmt.Sprintf("insert into mysql.stats_histograms (table_id, is_index, hist_id, distinct_count) values (%d, %d, %d, %d)", tableID, isIndex, c.ID, c.NDV)
	_, err := ctx.(sqlexec.SQLExecutor).Execute(insertSQL)
	if err != nil {
		return errors.Trace(err)
	}
	for i, bucket := range c.Buckets {
		var count int64
		if i == 0 {
			count = bucket.Count
		} else {
			count = bucket.Count - c.Buckets[i-1].Count
		}
		val, err := bucket.Value.ConvertTo(ctx.GetSessionVars().StmtCtx, types.NewFieldType(mysql.TypeBlob))
		if err != nil {
			return errors.Trace(err)
		}
		insertSQL = fmt.Sprintf("insert into mysql.stats_buckets values(%d, %d, %d, %d, %d, %d, X'%X')", tableID, isIndex, c.ID, i, count, bucket.Repeats, val.GetBytes())
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
		Buckets: make([]bucket, bucketSize),
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
		colStats.Buckets[bucketID] = bucket{
			Count:   count,
			Value:   value,
			Repeats: repeats,
		}
	}
	for i := 1; i < bucketSize; i++ {
		colStats.Buckets[i].Count += colStats.Buckets[i-1].Count
	}
	return colStats, nil
}

func (c *Column) String() string {
	strs := make([]string, 0, len(c.Buckets)+1)
	strs = append(strs, fmt.Sprintf("column:%d ndv:%d", c.ID, c.NDV))
	for _, bucket := range c.Buckets {
		strVal, _ := bucket.Value.ToString()
		strs = append(strs, fmt.Sprintf("num: %d\tvalue: %s\trepeats: %d", bucket.Count, strVal, bucket.Repeats))
	}
	return strings.Join(strs, "\n")
}

// EqualRowCount estimates the row count where the column equals to value.
func (c *Column) EqualRowCount(sc *variable.StatementContext, value types.Datum) (int64, error) {
	if len(c.Buckets) == 0 {
		return pseudoRowCount / pseudoEqualRate, nil
	}
	index, match, err := c.lowerBound(sc, value)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if index == len(c.Buckets) {
		return 0, nil
	}
	if match {
		return c.Buckets[index].Repeats, nil
	}
	return c.totalRowCount() / c.NDV, nil
}

// GreaterRowCount estimates the row count where the column greater than value.
func (c *Column) GreaterRowCount(sc *variable.StatementContext, value types.Datum) (int64, error) {
	if len(c.Buckets) == 0 {
		return pseudoRowCount / pseudoLessRate, nil
	}
	lessCount, err := c.LessRowCount(sc, value)
	if err != nil {
		return 0, errors.Trace(err)
	}
	eqCount, err := c.EqualRowCount(sc, value)
	if err != nil {
		return 0, errors.Trace(err)
	}
	gtCount := c.totalRowCount() - lessCount - eqCount
	if gtCount < 0 {
		gtCount = 0
	}
	return gtCount, nil
}

// LessRowCount estimates the row count where the column less than value.
func (c *Column) LessRowCount(sc *variable.StatementContext, value types.Datum) (int64, error) {
	if len(c.Buckets) == 0 {
		return pseudoRowCount / pseudoLessRate, nil
	}
	index, match, err := c.lowerBound(sc, value)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if index == len(c.Buckets) {
		return c.totalRowCount(), nil
	}
	curCount := c.Buckets[index].Count
	prevCount := int64(0)
	if index > 0 {
		prevCount = c.Buckets[index-1].Count
	}
	lessThanBucketValueCount := curCount - c.Buckets[index].Repeats
	if match {
		return lessThanBucketValueCount, nil
	}
	return (prevCount + lessThanBucketValueCount) / 2, nil
}

// BetweenRowCount estimates the row count where column greater or equal to a and less than b.
func (c *Column) BetweenRowCount(sc *variable.StatementContext, a, b types.Datum) (int64, error) {
	if len(c.Buckets) == 0 {
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
	return c.Buckets[len(c.Buckets)-1].Count
}

func (c *Column) bucketRowCount() int64 {
	return c.totalRowCount() / int64(len(c.Buckets))
}

func (c *Column) inBucketBetweenCount() int64 {
	// TODO: Make this estimation more accurate using uniform spread assumption.
	return c.bucketRowCount()/3 + 1
}

func (c *Column) lowerBound(sc *variable.StatementContext, target types.Datum) (index int, match bool, err error) {
	index = sort.Search(len(c.Buckets), func(i int) bool {
		cmp, err1 := c.Buckets[i].Value.CompareDatum(sc, target)
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
		c.Buckets[curBuck] = bucket{
			Count:   c.Buckets[i+1].Count,
			Value:   c.Buckets[i+1].Value,
			Repeats: c.Buckets[i+1].Repeats,
		}
		curBuck++
	}
	if bucketIdx%2 == 0 {
		c.Buckets[curBuck] = c.Buckets[bucketIdx]
		curBuck++
	}
	c.Buckets = c.Buckets[:curBuck]
	return
}
