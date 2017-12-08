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
	"bytes"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tipb/go-tipb"
	goctx "golang.org/x/net/context"
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
	Count        int64
	UpperBound   types.Datum
	LowerBound   types.Datum
	Repeats      int64
	lowerScalar  float64
	upperScalar  float64
	commonPfxLen int // when the bucket value type is KindString or KindBytes, commonPfxLen is the common prefix length of the lower bound and upper bound.
}

// SaveStatsToStorage saves the stats to storage.
func SaveStatsToStorage(ctx context.Context, tableID int64, count int64, isIndex int, hg *Histogram, cms *CMSketch) error {
	goCtx := goctx.TODO()
	exec := ctx.(sqlexec.SQLExecutor)
	_, err := exec.Execute(goCtx, "begin")
	if err != nil {
		return errors.Trace(err)
	}
	txn := ctx.Txn()
	version := txn.StartTS()
	replaceSQL := fmt.Sprintf("replace into mysql.stats_meta (version, table_id, count) values (%d, %d, %d)", version, tableID, count)
	_, err = exec.Execute(goCtx, replaceSQL)
	if err != nil {
		return errors.Trace(err)
	}
	data, err := encodeCMSketch(cms)
	if err != nil {
		return errors.Trace(err)
	}
	replaceSQL = fmt.Sprintf("replace into mysql.stats_histograms (table_id, is_index, hist_id, distinct_count, version, null_count, cm_sketch) values (%d, %d, %d, %d, %d, %d, X'%X')",
		tableID, isIndex, hg.ID, hg.NDV, version, hg.NullCount, data)
	_, err = exec.Execute(goCtx, replaceSQL)
	if err != nil {
		return errors.Trace(err)
	}
	deleteSQL := fmt.Sprintf("delete from mysql.stats_buckets where table_id = %d and is_index = %d and hist_id = %d", tableID, isIndex, hg.ID)
	_, err = exec.Execute(goCtx, deleteSQL)
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
		_, err = exec.Execute(goCtx, insertSQL)
		if err != nil {
			return errors.Trace(err)
		}
	}
	_, err = exec.Execute(goCtx, "commit")
	return errors.Trace(err)
}

func histogramFromStorage(ctx context.Context, tableID int64, colID int64, tp *types.FieldType, distinct int64, isIndex int, ver uint64, nullCount int64) (*Histogram, error) {
	selSQL := fmt.Sprintf("select bucket_id, count, repeats, lower_bound, upper_bound from mysql.stats_buckets where table_id = %d and is_index = %d and hist_id = %d", tableID, isIndex, colID)
	rows, fields, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, selSQL)
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
		bucketID := rows[i].GetInt64(0)
		count := rows[i].GetInt64(1)
		repeats := rows[i].GetInt64(2)
		var upperBound, lowerBound types.Datum
		if isIndex == 1 {
			lowerBound = rows[i].GetDatum(3, &fields[3].Column.FieldType)
			upperBound = rows[i].GetDatum(4, &fields[4].Column.FieldType)
		} else {
			d := rows[i].GetDatum(3, &fields[3].Column.FieldType)
			lowerBound, err = d.ConvertTo(ctx.GetSessionVars().StmtCtx, tp)
			if err != nil {
				return nil, errors.Trace(err)
			}
			d = rows[i].GetDatum(4, &fields[4].Column.FieldType)
			upperBound, err = d.ConvertTo(ctx.GetSessionVars().StmtCtx, tp)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		lowerScalar, upperScalar, commonLength := preCalculateDatumScalar(&lowerBound, &upperBound)
		hg.Buckets[bucketID] = Bucket{
			Count:        count,
			UpperBound:   upperBound,
			LowerBound:   lowerBound,
			Repeats:      repeats,
			lowerScalar:  lowerScalar,
			upperScalar:  upperScalar,
			commonPfxLen: commonLength,
		}
	}
	for i := 1; i < bucketSize; i++ {
		hg.Buckets[i].Count += hg.Buckets[i-1].Count
	}
	return hg, nil
}

func columnCountFromStorage(ctx context.Context, tableID, colID int64) (int64, error) {
	selSQL := fmt.Sprintf("select sum(count) from mysql.stats_buckets where table_id = %d and is_index = %d and hist_id = %d", tableID, 0, colID)
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, selSQL)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if rows[0].IsNull(0) {
		return 0, nil
	}
	return rows[0].GetMyDecimal(0).ToInt()
}

func (hg *Histogram) toString(isIndex bool) string {
	strs := make([]string, 0, len(hg.Buckets)+1)
	if isIndex {
		strs = append(strs, fmt.Sprintf("index:%d ndv:%d", hg.ID, hg.NDV))
	} else {
		strs = append(strs, fmt.Sprintf("column:%d ndv:%d", hg.ID, hg.NDV))
	}
	for _, bucket := range hg.Buckets {
		upperVal, err := bucket.UpperBound.ToString()
		terror.Log(errors.Trace(err))
		lowerVal, err := bucket.LowerBound.ToString()
		terror.Log(errors.Trace(err))
		strs = append(strs, fmt.Sprintf("num: %d\tlower_bound: %s\tupper_bound: %s\trepeats: %d", bucket.Count, lowerVal, upperVal, bucket.Repeats))
	}
	return strings.Join(strs, "\n")
}

// equalRowCount estimates the row count where the column equals to value.
func (hg *Histogram) equalRowCount(sc *stmtctx.StatementContext, value types.Datum) (float64, error) {
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
	c, err := value.CompareDatum(sc, &hg.Buckets[index].LowerBound)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if c < 0 {
		return 0, nil
	}
	return hg.totalRowCount() / float64(hg.NDV), nil
}

// greaterRowCount estimates the row count where the column greater than value.
func (hg *Histogram) greaterRowCount(sc *stmtctx.StatementContext, value types.Datum) (float64, error) {
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
func (hg *Histogram) greaterAndEqRowCount(sc *stmtctx.StatementContext, value types.Datum) (float64, error) {
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
func (hg *Histogram) lessRowCount(sc *stmtctx.StatementContext, value types.Datum) (float64, error) {
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
	c, err := value.CompareDatum(sc, &hg.Buckets[index].LowerBound)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if c <= 0 {
		return prevCount, nil
	}
	valueScalar := convertDatumToScalar(&value, hg.Buckets[index].commonPfxLen)
	frac := calcFraction(hg.Buckets[index].lowerScalar, hg.Buckets[index].upperScalar, valueScalar)
	return prevCount + (lessThanBucketValueCount-prevCount)*frac, nil
}

// lessAndEqRowCount estimates the row count where the column less than or equal to value.
func (hg *Histogram) lessAndEqRowCount(sc *stmtctx.StatementContext, value types.Datum) (float64, error) {
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
func (hg *Histogram) betweenRowCount(sc *stmtctx.StatementContext, a, b types.Datum) (float64, error) {
	lessCountA, err := hg.lessRowCount(sc, a)
	if err != nil {
		return 0, errors.Trace(err)
	}
	lessCountB, err := hg.lessRowCount(sc, b)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if lessCountA >= lessCountB {
		return hg.totalRowCount() / float64(hg.NDV), nil
	}
	return lessCountB - lessCountA, nil
}

func (hg *Histogram) totalRowCount() float64 {
	if len(hg.Buckets) == 0 {
		return 0
	}
	return float64(hg.Buckets[len(hg.Buckets)-1].Count)
}

func (hg *Histogram) lowerBound(sc *stmtctx.StatementContext, target types.Datum) (index int, match bool, err error) {
	index = sort.Search(len(hg.Buckets), func(i int) bool {
		cmp, err1 := hg.Buckets[i].UpperBound.CompareDatum(sc, &target)
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

// HistogramToProto converts Histogram to its protobuf representation.
// Note that when this is used, the lower/upper bound in the bucket must be BytesDatum.
func HistogramToProto(hg *Histogram) *tipb.Histogram {
	protoHg := &tipb.Histogram{
		Ndv: hg.NDV,
	}
	for _, bucket := range hg.Buckets {
		bkt := &tipb.Bucket{
			Count:      bucket.Count,
			LowerBound: bucket.LowerBound.GetBytes(),
			UpperBound: bucket.UpperBound.GetBytes(),
			Repeats:    bucket.Repeats,
		}
		protoHg.Buckets = append(protoHg.Buckets, bkt)
	}
	return protoHg
}

// HistogramFromProto converts Histogram from its protobuf representation.
// Note that we will set BytesDatum for the lower/upper bound in the bucket, the decode will
// be after all histograms merged.
func HistogramFromProto(protoHg *tipb.Histogram) *Histogram {
	hg := &Histogram{
		NDV: protoHg.Ndv,
	}
	for _, bucket := range protoHg.Buckets {
		bkt := Bucket{
			Count:      bucket.Count,
			LowerBound: types.NewBytesDatum(bucket.LowerBound),
			UpperBound: types.NewBytesDatum(bucket.UpperBound),
			Repeats:    bucket.Repeats,
		}
		hg.Buckets = append(hg.Buckets, bkt)
	}
	return hg
}

// MergeHistograms merges two histograms.
func MergeHistograms(sc *stmtctx.StatementContext, lh *Histogram, rh *Histogram, bucketSize int) (*Histogram, error) {
	if len(lh.Buckets) == 0 {
		return rh, nil
	}
	if len(rh.Buckets) == 0 {
		return lh, nil
	}
	lh.NDV += rh.NDV
	lLen := len(lh.Buckets)
	cmp, err := lh.Buckets[lLen-1].UpperBound.CompareDatum(sc, &rh.Buckets[0].LowerBound)
	if err != nil {
		return nil, errors.Trace(err)
	}
	offset := int64(0)
	if cmp == 0 {
		lh.NDV--
		lh.Buckets[lLen-1].UpperBound = rh.Buckets[0].UpperBound
		lh.Buckets[lLen-1].Repeats = rh.Buckets[0].Repeats
		lh.Buckets[lLen-1].Count += rh.Buckets[0].Count
		offset = rh.Buckets[0].Count
		rh.Buckets = rh.Buckets[1:]
	}
	for len(lh.Buckets) > bucketSize {
		lh.mergeBuckets(int64(len(lh.Buckets)) - 1)
	}
	if len(rh.Buckets) == 0 {
		return lh, nil
	}
	for len(rh.Buckets) > bucketSize {
		rh.mergeBuckets(int64(len(rh.Buckets)) - 1)
	}
	lCount := lh.Buckets[len(lh.Buckets)-1].Count
	rCount := rh.Buckets[len(rh.Buckets)-1].Count - offset
	lAvg := float64(lCount) / float64(len(lh.Buckets))
	rAvg := float64(rCount) / float64(len(rh.Buckets))
	for len(lh.Buckets) > 1 && lAvg*2 <= rAvg {
		lh.mergeBuckets(int64(len(lh.Buckets)) - 1)
		lAvg *= 2
	}
	for len(rh.Buckets) > 1 && rAvg*2 <= lAvg {
		rh.mergeBuckets(int64(len(rh.Buckets)) - 1)
		rAvg *= 2
	}
	for _, bkt := range rh.Buckets {
		bkt.Count = bkt.Count + lCount - offset
		lh.Buckets = append(lh.Buckets, bkt)
	}
	for len(lh.Buckets) > bucketSize {
		lh.mergeBuckets(int64(len(lh.Buckets)) - 1)
	}
	return lh, nil
}

// Column represents a column histogram.
type Column struct {
	Histogram
	*CMSketch
	Count int64
	Info  *model.ColumnInfo
}

func (c *Column) String() string {
	return c.Histogram.toString(false)
}

func (c *Column) equalRowCount(sc *stmtctx.StatementContext, val types.Datum) (float64, error) {
	if c.CMSketch != nil {
		count, err := c.CMSketch.queryValue(val)
		return float64(count), errors.Trace(err)
	}
	count, err := c.Histogram.equalRowCount(sc, val)
	return count, errors.Trace(err)
}

// getIntColumnRowCount estimates the row count by a slice of IntColumnRange.
func (c *Column) getIntColumnRowCount(sc *stmtctx.StatementContext, intRanges []ranger.IntColumnRange,
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
func (c *Column) getColumnRowCount(sc *stmtctx.StatementContext, ranges []*ranger.ColumnRange) (float64, error) {
	var rowCount float64
	for _, rg := range ranges {
		cmp, err := rg.Low.CompareDatum(sc, &rg.High)
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
	*CMSketch
	Info *model.IndexInfo
}

func (idx *Index) String() string {
	return idx.Histogram.toString(true)
}

func (idx *Index) equalRowCount(sc *stmtctx.StatementContext, b []byte) (float64, error) {
	if idx.CMSketch != nil {
		return float64(idx.CMSketch.queryBytes(b)), nil
	}
	count, err := idx.Histogram.equalRowCount(sc, types.NewBytesDatum(b))
	return count, errors.Trace(err)
}

func (idx *Index) getRowCount(sc *stmtctx.StatementContext, indexRanges []*ranger.IndexRange) (float64, error) {
	totalCount := float64(0)
	for _, indexRange := range indexRanges {
		lb, err := codec.EncodeKey(nil, indexRange.LowVal...)
		if err != nil {
			return 0, errors.Trace(err)
		}
		rb, err := codec.EncodeKey(nil, indexRange.HighVal...)
		if err != nil {
			return 0, errors.Trace(err)
		}
		fullLen := len(indexRange.LowVal) == len(indexRange.HighVal) && len(indexRange.LowVal) == len(idx.Info.Columns)
		if fullLen && bytes.Equal(lb, rb) {
			if !indexRange.LowExclude && !indexRange.HighExclude {
				rowCount, err1 := idx.equalRowCount(sc, lb)
				if err1 != nil {
					return 0, errors.Trace(err1)
				}
				totalCount += rowCount
			}
			continue
		}
		if indexRange.LowExclude {
			lb = kv.Key(lb).PrefixNext()
		}
		if !indexRange.HighExclude {
			rb = kv.Key(rb).PrefixNext()
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
