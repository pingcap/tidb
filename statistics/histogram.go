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

// Histogram represents statistics for a column or index.
type Histogram struct {
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

func (hg *Histogram) saveToStorage(ctx context.Context, tableID int64, isIndex int) error {
	insertSQL := fmt.Sprintf("insert into mysql.stats_histograms (table_id, is_index, hist_id, distinct_count) values (%d, %d, %d, %d)", tableID, isIndex, hg.ID, hg.NDV)
	_, err := ctx.(sqlexec.SQLExecutor).Execute(insertSQL)
	if err != nil {
		return errors.Trace(err)
	}
	for i, bucket := range hg.Buckets {
		var count int64
		if i == 0 {
			count = bucket.Count
		} else {
			count = bucket.Count - hg.Buckets[i-1].Count
		}
		val, err := bucket.Value.ConvertTo(ctx.GetSessionVars().StmtCtx, types.NewFieldType(mysql.TypeBlob))
		if err != nil {
			return errors.Trace(err)
		}
		insertSQL = fmt.Sprintf("insert into mysql.stats_buckets values(%d, %d, %d, %d, %d, %d, X'%X')", tableID, isIndex, hg.ID, i, count, bucket.Repeats, val.GetBytes())
		_, err = ctx.(sqlexec.SQLExecutor).Execute(insertSQL)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func histogramFromStorage(ctx context.Context, tableID int64, colID int64, tp *types.FieldType, distinct int64, isIndex int) (*Histogram, error) {
	selSQL := fmt.Sprintf("select bucket_id, count, repeats, value from mysql.stats_buckets where table_id = %d and is_index = %d and hist_id = %d", tableID, isIndex, colID)
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, selSQL)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bucketSize := len(rows)
	hg := &Histogram{
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
		hg.Buckets[bucketID] = bucket{
			Count:   count,
			Value:   value,
			Repeats: repeats,
		}
	}
	for i := 1; i < bucketSize; i++ {
		hg.Buckets[i].Count += hg.Buckets[i-1].Count
	}
	return hg, nil
}

func (hg *Histogram) toString(isIndex bool) string {
	strs := make([]string, 0, len(hg.Buckets)+1)
	if isIndex {
		strs = append(strs, fmt.Sprintf("index:%d ndv:%d", hg.ID, hg.NDV))
	} else {
		strs = append(strs, fmt.Sprintf("column:%d ndv:%d", hg.ID, hg.NDV))
	}
	for _, bucket := range hg.Buckets {
		strVal, _ := bucket.Value.ToString()
		strs = append(strs, fmt.Sprintf("num: %d\tvalue: %s\trepeats: %d", bucket.Count, strVal, bucket.Repeats))
	}
	return strings.Join(strs, "\n")
}

// EqualRowCount estimates the row count where the column equals to value.
func (hg *Histogram) EqualRowCount(sc *variable.StatementContext, value types.Datum) (int64, error) {
	if len(hg.Buckets) == 0 {
		return pseudoRowCount / pseudoEqualRate, nil
	}
	index, match, err := hg.lowerBound(sc, value)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if index == len(hg.Buckets) {
		return 0, nil
	}
	if match {
		return hg.Buckets[index].Repeats, nil
	}
	return hg.totalRowCount() / hg.NDV, nil
}

// GreaterRowCount estimates the row count where the column greater than value.
func (hg *Histogram) GreaterRowCount(sc *variable.StatementContext, value types.Datum) (int64, error) {
	if len(hg.Buckets) == 0 {
		return pseudoRowCount / pseudoLessRate, nil
	}
	lessCount, err := hg.LessRowCount(sc, value)
	if err != nil {
		return 0, errors.Trace(err)
	}
	eqCount, err := hg.EqualRowCount(sc, value)
	if err != nil {
		return 0, errors.Trace(err)
	}
	gtCount := hg.totalRowCount() - lessCount - eqCount
	if gtCount < 0 {
		gtCount = 0
	}
	return gtCount, nil
}

// LessRowCount estimates the row count where the column less than value.
func (hg *Histogram) LessRowCount(sc *variable.StatementContext, value types.Datum) (int64, error) {
	if len(hg.Buckets) == 0 {
		return pseudoRowCount / pseudoLessRate, nil
	}
	index, match, err := hg.lowerBound(sc, value)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if index == len(hg.Buckets) {
		return hg.totalRowCount(), nil
	}
	curCount := hg.Buckets[index].Count
	prevCount := int64(0)
	if index > 0 {
		prevCount = hg.Buckets[index-1].Count
	}
	lessThanBucketValueCount := curCount - hg.Buckets[index].Repeats
	if match {
		return lessThanBucketValueCount, nil
	}
	return (prevCount + lessThanBucketValueCount) / 2, nil
}

// BetweenRowCount estimates the row count where column greater or equal to a and less than b.
func (hg *Histogram) BetweenRowCount(sc *variable.StatementContext, a, b types.Datum) (int64, error) {
	if len(hg.Buckets) == 0 {
		return pseudoRowCount / pseudoBetweenRate, nil
	}
	lessCountA, err := hg.LessRowCount(sc, a)
	if err != nil {
		return 0, errors.Trace(err)
	}
	lessCountB, err := hg.LessRowCount(sc, b)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if lessCountA >= lessCountB {
		return hg.inBucketBetweenCount(), nil
	}
	return lessCountB - lessCountA, nil
}

func (hg *Histogram) totalRowCount() int64 {
	return hg.Buckets[len(hg.Buckets)-1].Count
}

func (hg *Histogram) bucketRowCount() int64 {
	return hg.totalRowCount() / int64(len(hg.Buckets))
}

func (hg *Histogram) inBucketBetweenCount() int64 {
	// TODO: Make this estimation more accurate using uniform spread assumption.
	return hg.bucketRowCount()/3 + 1
}

func (hg *Histogram) lowerBound(sc *variable.StatementContext, target types.Datum) (index int, match bool, err error) {
	index = sort.Search(len(hg.Buckets), func(i int) bool {
		cmp, err1 := hg.Buckets[i].Value.CompareDatum(sc, target)
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
func (hg *Histogram) mergeBuckets(bucketIdx int64) {
	curBuck := 0
	for i := int64(0); i+1 <= bucketIdx; i += 2 {
		hg.Buckets[curBuck] = bucket{
			Count:   hg.Buckets[i+1].Count,
			Value:   hg.Buckets[i+1].Value,
			Repeats: hg.Buckets[i+1].Repeats,
		}
		curBuck++
	}
	if bucketIdx%2 == 0 {
		hg.Buckets[curBuck] = hg.Buckets[bucketIdx]
		curBuck++
	}
	hg.Buckets = hg.Buckets[:curBuck]
	return
}

// Column represents a column histogram.
type Column struct {
	Histogram
}

func (c *Column) String() string {
	return c.Histogram.toString(false)
}

// Index represents an index histogram.
type Index struct {
	Histogram
	NumColumns int
}

func (idx *Index) String() string {
	return idx.Histogram.toString(true)
}
