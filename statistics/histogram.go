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
	"math"
	"sort"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/types"
)

// Histogram represents statistics for a column or index.
type Histogram struct {
	ID        int64 // Column ID.
	NDV       int64 // Number of distinct values.
	NullCount int64 // Number of null values.
	// LastUpdateVersion is the version that this histogram updated last time.
	LastUpdateVersion uint64

	Buckets []Bucket
}

// Bucket is an element of histogram.
//
// A bucket count is the number of items stored in all previous buckets and the current bucket.
// bucket numbers are always in increasing order.
//
// A bucket value is the greatest item value stored in the bucket.
//
// Repeat is the number of repeats of the bucket value, it can be used to find popular values.
//
type Bucket struct {
	Count      int64
	UpperBound types.Datum
	LowerBound types.Datum
	Repeats    int64
}

// SaveToStorage saves the histogram to storage.
func (hg *Histogram) SaveToStorage(ctx context.Context, tableID int64, count int64, isIndex int) error {
	exec := ctx.(sqlexec.SQLExecutor)
	_, err := exec.Execute("begin")
	if err != nil {
		return errors.Trace(err)
	}
	txn := ctx.Txn()
	version := txn.StartTS()
	replaceSQL := fmt.Sprintf("replace into mysql.stats_meta (version, table_id, count) values (%d, %d, %d)", version, tableID, count)
	_, err = exec.Execute(replaceSQL)
	if err != nil {
		return errors.Trace(err)
	}
	replaceSQL = fmt.Sprintf("replace into mysql.stats_histograms (table_id, is_index, hist_id, distinct_count, version, null_count) values (%d, %d, %d, %d, %d, %d)", tableID, isIndex, hg.ID, hg.NDV, version, hg.NullCount)
	_, err = exec.Execute(replaceSQL)
	if err != nil {
		return errors.Trace(err)
	}
	deleteSQL := fmt.Sprintf("delete from mysql.stats_buckets where table_id = %d and is_index = %d and hist_id = %d", tableID, isIndex, hg.ID)
	_, err = exec.Execute(deleteSQL)
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
		var upperBound types.Datum
		upperBound, err = bucket.UpperBound.ConvertTo(ctx.GetSessionVars().StmtCtx, types.NewFieldType(mysql.TypeBlob))
		if err != nil {
			return errors.Trace(err)
		}
		var lowerBound types.Datum
		lowerBound, err = bucket.LowerBound.ConvertTo(ctx.GetSessionVars().StmtCtx, types.NewFieldType(mysql.TypeBlob))
		if err != nil {
			return errors.Trace(err)
		}
		insertSQL := fmt.Sprintf("insert into mysql.stats_buckets(table_id, is_index, hist_id, bucket_id, count, repeats, lower_bound, upper_bound) values(%d, %d, %d, %d, %d, %d, X'%X', X'%X')", tableID, isIndex, hg.ID, i, count, bucket.Repeats, lowerBound.GetBytes(), upperBound.GetBytes())
		_, err = exec.Execute(insertSQL)
		if err != nil {
			return errors.Trace(err)
		}
	}
	_, err = exec.Execute("commit")
	return errors.Trace(err)
}

func histogramFromStorage(ctx context.Context, tableID int64, colID int64, tp *types.FieldType, distinct int64, isIndex int, ver uint64, nullCount int64) (*Histogram, error) {
	selSQL := fmt.Sprintf("select bucket_id, count, repeats, lower_bound, upper_bound from mysql.stats_buckets where table_id = %d and is_index = %d and hist_id = %d", tableID, isIndex, colID)
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, selSQL)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bucketSize := len(rows)
	hg := &Histogram{
		ID:                colID,
		NDV:               distinct,
		LastUpdateVersion: ver,
		Buckets:           make([]Bucket, bucketSize),
		NullCount:         nullCount,
	}
	for i := 0; i < bucketSize; i++ {
		bucketID := rows[i].Data[0].GetInt64()
		count := rows[i].Data[1].GetInt64()
		repeats := rows[i].Data[2].GetInt64()
		var upperBound, lowerBound types.Datum
		if isIndex == 1 {
			lowerBound, upperBound = rows[i].Data[3], rows[i].Data[4]
		} else {
			lowerBound, err = rows[i].Data[3].ConvertTo(ctx.GetSessionVars().StmtCtx, tp)
			if err != nil {
				return nil, errors.Trace(err)
			}
			upperBound, err = rows[i].Data[4].ConvertTo(ctx.GetSessionVars().StmtCtx, tp)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		hg.Buckets[bucketID] = Bucket{
			Count:      count,
			UpperBound: upperBound,
			LowerBound: lowerBound,
			Repeats:    repeats,
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
		upperVal, _ := bucket.UpperBound.ToString()
		lowerVal, _ := bucket.LowerBound.ToString()
		strs = append(strs, fmt.Sprintf("num: %d\tlower_bound: %s\tupper_bound: %s\trepeats: %d", bucket.Count, lowerVal, upperVal, bucket.Repeats))
	}
	return strings.Join(strs, "\n")
}

// equalRowCount estimates the row count where the column equals to value.
func (hg *Histogram) equalRowCount(sc *variable.StatementContext, value types.Datum) (float64, error) {
	index, match, err := hg.lowerBound(sc, value)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if index == len(hg.Buckets) {
		return 0, nil
	}
	if match {
		return float64(hg.Buckets[index].Repeats), nil
	}
	c, err := value.CompareDatum(sc, hg.Buckets[index].LowerBound)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if c < 0 {
		return 0, nil
	}
	return hg.totalRowCount() / float64(hg.NDV), nil
}

// greaterRowCount estimates the row count where the column greater than value.
func (hg *Histogram) greaterRowCount(sc *variable.StatementContext, value types.Datum) (float64, error) {
	lessCount, err := hg.lessRowCount(sc, value)
	if err != nil {
		return 0, errors.Trace(err)
	}
	eqCount, err := hg.equalRowCount(sc, value)
	if err != nil {
		return 0, errors.Trace(err)
	}
	gtCount := hg.totalRowCount() - lessCount - eqCount
	if gtCount < 0 {
		gtCount = 0
	}
	return gtCount, nil
}

// greaterAndEqRowCount estimates the row count where the column less than or equal to value.
func (hg *Histogram) greaterAndEqRowCount(sc *variable.StatementContext, value types.Datum) (float64, error) {
	greaterCount, err := hg.greaterRowCount(sc, value)
	if err != nil {
		return 0, errors.Trace(err)
	}
	eqCount, err := hg.equalRowCount(sc, value)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return greaterCount + eqCount, nil
}

// lessRowCount estimates the row count where the column less than value.
func (hg *Histogram) lessRowCount(sc *variable.StatementContext, value types.Datum) (float64, error) {
	index, match, err := hg.lowerBound(sc, value)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if index == len(hg.Buckets) {
		return hg.totalRowCount(), nil
	}
	curCount := float64(hg.Buckets[index].Count)
	prevCount := float64(0)
	if index > 0 {
		prevCount = float64(hg.Buckets[index-1].Count)
	}
	lessThanBucketValueCount := curCount - float64(hg.Buckets[index].Repeats)
	if match {
		return lessThanBucketValueCount, nil
	}
	c, err := value.CompareDatum(sc, hg.Buckets[index].LowerBound)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if c <= 0 {
		return prevCount, nil
	}
	return (prevCount + lessThanBucketValueCount) / 2, nil
}

// lessAndEqRowCount estimates the row count where the column less than or equal to value.
func (hg *Histogram) lessAndEqRowCount(sc *variable.StatementContext, value types.Datum) (float64, error) {
	lessCount, err := hg.lessRowCount(sc, value)
	if err != nil {
		return 0, errors.Trace(err)
	}
	eqCount, err := hg.equalRowCount(sc, value)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return lessCount + eqCount, nil
}

// betweenRowCount estimates the row count where column greater or equal to a and less than b.
func (hg *Histogram) betweenRowCount(sc *variable.StatementContext, a, b types.Datum) (float64, error) {
	lessCountA, err := hg.lessRowCount(sc, a)
	if err != nil {
		return 0, errors.Trace(err)
	}
	lessCountB, err := hg.lessRowCount(sc, b)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if lessCountA >= lessCountB {
		return hg.inBucketBetweenCount(), nil
	}
	return lessCountB - lessCountA, nil
}

func (hg *Histogram) totalRowCount() float64 {
	if len(hg.Buckets) == 0 {
		return 0
	}
	return float64(hg.Buckets[len(hg.Buckets)-1].Count)
}

func (hg *Histogram) bucketRowCount() float64 {
	return hg.totalRowCount() / float64(len(hg.Buckets))
}

func (hg *Histogram) inBucketBetweenCount() float64 {
	// TODO: Make this estimation more accurate using uniform spread assumption.
	return hg.bucketRowCount()/3 + 1
}

func (hg *Histogram) lowerBound(sc *variable.StatementContext, target types.Datum) (index int, match bool, err error) {
	index = sort.Search(len(hg.Buckets), func(i int) bool {
		cmp, err1 := hg.Buckets[i].UpperBound.CompareDatum(sc, target)
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
		hg.Buckets[curBuck] = Bucket{
			Count:      hg.Buckets[i+1].Count,
			UpperBound: hg.Buckets[i+1].UpperBound,
			LowerBound: hg.Buckets[i].LowerBound,
			Repeats:    hg.Buckets[i+1].Repeats,
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

// getIncreaseFactor will return a factor of data increasing after the last analysis.
func (hg *Histogram) getIncreaseFactor(totalCount int64) float64 {
	columnCount := hg.Buckets[len(hg.Buckets)-1].Count + hg.NullCount
	if columnCount == 0 {
		// avoid dividing by 0
		return 1.0
	}
	return float64(totalCount) / float64(columnCount)
}

// Column represents a column histogram.
type Column struct {
	Histogram
	Info *model.ColumnInfo
}

func (c *Column) String() string {
	return c.Histogram.toString(false)
}

// getIntColumnRowCount estimates the row count by a slice of IntColumnRange.
func (c *Column) getIntColumnRowCount(sc *variable.StatementContext, intRanges []types.IntColumnRange,
	totalRowCount float64) (float64, error) {
	var rowCount float64
	for _, rg := range intRanges {
		var cnt float64
		var err error
		if rg.LowVal == math.MinInt64 && rg.HighVal == math.MaxInt64 {
			cnt = totalRowCount
		} else if rg.LowVal == math.MinInt64 {
			cnt, err = c.lessAndEqRowCount(sc, types.NewIntDatum(rg.HighVal))
		} else if rg.HighVal == math.MaxInt64 {
			cnt, err = c.greaterAndEqRowCount(sc, types.NewIntDatum(rg.LowVal))
		} else {
			if rg.LowVal == rg.HighVal {
				cnt, err = c.equalRowCount(sc, types.NewIntDatum(rg.LowVal))
			} else {
				cnt, err = c.betweenRowCount(sc, types.NewIntDatum(rg.LowVal), types.NewIntDatum(rg.HighVal+1))
			}
		}
		if err != nil {
			return 0, errors.Trace(err)
		}
		if rg.HighVal-rg.LowVal > 0 && cnt > float64(rg.HighVal-rg.LowVal) {
			cnt = float64(rg.HighVal - rg.LowVal)
		}
		rowCount += cnt
	}
	if rowCount > totalRowCount {
		rowCount = totalRowCount
	}
	return rowCount, nil
}

// getColumnRowCount estimates the row count by a slice of ColumnRange.
func (c *Column) getColumnRowCount(sc *variable.StatementContext, ranges []*types.ColumnRange) (float64, error) {
	var rowCount float64
	for _, rg := range ranges {
		cmp, err := rg.Low.CompareDatum(sc, rg.High)
		if err != nil {
			return 0, errors.Trace(err)
		}
		if cmp == 0 {
			// the point case.
			if !rg.LowExcl && !rg.HighExcl {
				var cnt float64
				cnt, err = c.equalRowCount(sc, rg.Low)
				if err != nil {
					return 0, errors.Trace(err)
				}
				rowCount += cnt
			}
			continue
		}
		// the interval case.
		cnt, err := c.betweenRowCount(sc, rg.Low, rg.High)
		if err != nil {
			return 0, errors.Trace(err)
		}
		if rg.LowExcl {
			lowCnt, err := c.equalRowCount(sc, rg.Low)
			if err != nil {
				return 0, errors.Trace(err)
			}
			cnt -= lowCnt
		}
		if !rg.HighExcl {
			highCnt, err := c.equalRowCount(sc, rg.High)
			if err != nil {
				return 0, errors.Trace(err)
			}
			cnt += highCnt
		}
		rowCount += cnt
	}
	if rowCount > c.totalRowCount() {
		rowCount = c.totalRowCount()
	} else if rowCount < 0 {
		rowCount = 0
	}
	return rowCount, nil
}

// Index represents an index histogram.
type Index struct {
	Histogram
	Info *model.IndexInfo
}

func (idx *Index) String() string {
	return idx.Histogram.toString(true)
}

func (idx *Index) getRowCount(sc *variable.StatementContext, indexRanges []*types.IndexRange) (float64, error) {
	totalCount := float64(0)
	for _, indexRange := range indexRanges {
		indexRange.Align(len(idx.Info.Columns))
		lb, err := codec.EncodeKey(nil, indexRange.LowVal...)
		if err != nil {
			return 0, errors.Trace(err)
		}
		if indexRange.LowExclude {
			lb = append(lb, 0)
		}
		rb, err := codec.EncodeKey(nil, indexRange.HighVal...)
		if err != nil {
			return 0, errors.Trace(err)
		}
		if !indexRange.HighExclude {
			rb = append(rb, 0)
		}
		l := types.NewBytesDatum(lb)
		r := types.NewBytesDatum(rb)
		rowCount, err := idx.betweenRowCount(sc, l, r)
		if err != nil {
			return 0, errors.Trace(err)
		}
		totalCount += rowCount
	}
	if totalCount > idx.totalRowCount() {
		totalCount = idx.totalRowCount()
	}
	return totalCount, nil
}
