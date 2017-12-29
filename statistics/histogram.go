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
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
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

	tp *types.FieldType

	// Histogram elements.
	//
	// A bucket bound is the smallest and greatest values stored in the bucket.
	//
	// A bucket count is the number of items stored in all previous buckets and the current bucket.
	// Bucket counts are always in increasing order.
	//
	// A bucket value is the greatest item value stored in the bucket.
	//
	// Repeat is the number of repeats of the bucket value, it can be used to find popular values.
	Bounds  *chunk.Chunk
	Counts  []int64
	Repeats []int64

	// Used for estimating fraction of the interval [lower, upper] that lies within the [lower, value].
	// For some types like `Int`, we do not build it because we can get them directly from `Bounds`.
	lowerScalar  []float64
	upperScalar  []float64
	commonPfxLen []int // commonPfxLen is the common prefix length of the lower bound and upper bound when the value type is KindString or KindBytes.
}

// NewHistogram creates a new histogram.
func NewHistogram(id, ndv, nullCount int64, version uint64, tp *types.FieldType, bucketSize int) *Histogram {
	return &Histogram{
		ID:                id,
		NDV:               ndv,
		NullCount:         nullCount,
		LastUpdateVersion: version,
		tp:                tp,
		Bounds:            chunk.NewChunkWithCapacity([]*types.FieldType{tp, tp}, bucketSize),
		Counts:            make([]int64, 0, bucketSize),
		Repeats:           make([]int64, 0, bucketSize),
	}
}

// GetLower gets the lower bound of bucket `idx`.
func (hg *Histogram) GetLower(idx int) *types.Datum {
	d := hg.Bounds.GetRow(idx).GetDatum(0, hg.tp)
	return &d
}

// GetUpper gets the upper bound of bucket `idx`.
func (hg *Histogram) GetUpper(idx int) *types.Datum {
	d := hg.Bounds.GetRow(idx).GetDatum(1, hg.tp)
	return &d
}

// AddBucket adds a bucket into `hg`.
func (hg *Histogram) AddBucket(lower *types.Datum, upper *types.Datum, count, repeat int64) {
	hg.Counts = append(hg.Counts, count)
	hg.Repeats = append(hg.Repeats, repeat)
	hg.Bounds.AppendDatum(0, lower)
	hg.Bounds.AppendDatum(1, upper)
}

func (hg *Histogram) updateLastBucket(upper *types.Datum, count, repeat int64) {
	len := hg.NumBuckets()
	lower := hg.GetLower(len - 1)
	hg.Bounds.TruncateTo(len - 1)
	hg.Bounds.AppendDatum(0, lower)
	hg.Bounds.AppendDatum(1, upper)
	hg.Counts[len-1], hg.Repeats[len-1] = count, repeat
}

// DecodeTo decodes the histogram bucket values into `tp`.
func (hg *Histogram) DecodeTo(tp *types.FieldType, timeZone *time.Location) error {
	old := hg.Bounds
	hg.Bounds = chunk.NewChunk([]*types.FieldType{tp, tp})
	hg.tp = tp
	for row := old.Begin(); row != old.End(); row = row.Next() {
		lower, err := tablecodec.DecodeColumnValue(row.GetBytes(0), tp, timeZone)
		if err != nil {
			return errors.Trace(err)
		}
		upper, err := tablecodec.DecodeColumnValue(row.GetBytes(1), tp, timeZone)
		if err != nil {
			return errors.Trace(err)
		}
		hg.Bounds.AppendDatum(0, &lower)
		hg.Bounds.AppendDatum(1, &upper)
	}
	return nil
}

// NumBuckets is the number of buckets in the histogram.
func (hg *Histogram) NumBuckets() int {
	return len(hg.Counts)
}

// HistogramEqual tests if two histograms are equal.
func HistogramEqual(a, b *Histogram, ignoreID bool) bool {
	if ignoreID {
		old := b.ID
		b.ID = a.ID
		defer func() { b.ID = old }()
	}
	return bytes.Equal([]byte(a.toString(false)), []byte(b.toString(false)))
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
	sc := ctx.GetSessionVars().StmtCtx
	for i, count := range hg.Counts {
		if i > 0 {
			count -= hg.Counts[i-1]
		}
		var upperBound types.Datum
		upperBound, err = hg.GetUpper(i).ConvertTo(sc, types.NewFieldType(mysql.TypeBlob))
		if err != nil {
			return errors.Trace(err)
		}
		var lowerBound types.Datum
		lowerBound, err = hg.GetLower(i).ConvertTo(sc, types.NewFieldType(mysql.TypeBlob))
		if err != nil {
			return errors.Trace(err)
		}
		insertSQL := fmt.Sprintf("insert into mysql.stats_buckets(table_id, is_index, hist_id, bucket_id, count, repeats, lower_bound, upper_bound) values(%d, %d, %d, %d, %d, %d, X'%X', X'%X')", tableID, isIndex, hg.ID, i, count, hg.Repeats[i], lowerBound.GetBytes(), upperBound.GetBytes())
		_, err = exec.Execute(goCtx, insertSQL)
		if err != nil {
			return errors.Trace(err)
		}
	}
	_, err = exec.Execute(goCtx, "commit")
	return errors.Trace(err)
}

func histogramFromStorage(ctx context.Context, tableID int64, colID int64, tp *types.FieldType, distinct int64, isIndex int, ver uint64, nullCount int64) (*Histogram, error) {
	selSQL := fmt.Sprintf("select count, repeats, lower_bound, upper_bound from mysql.stats_buckets where table_id = %d and is_index = %d and hist_id = %d order by bucket_id", tableID, isIndex, colID)
	rows, fields, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, selSQL)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bucketSize := len(rows)
	hg := NewHistogram(colID, distinct, nullCount, ver, tp, bucketSize)
	preCount := int64(0)
	for i := 0; i < bucketSize; i++ {
		count := rows[i].GetInt64(0)
		repeats := rows[i].GetInt64(1)
		var upperBound, lowerBound types.Datum
		if isIndex == 1 {
			lowerBound = rows[i].GetDatum(2, &fields[2].Column.FieldType)
			upperBound = rows[i].GetDatum(3, &fields[3].Column.FieldType)
		} else {
			d := rows[i].GetDatum(2, &fields[2].Column.FieldType)
			lowerBound, err = d.ConvertTo(ctx.GetSessionVars().StmtCtx, tp)
			if err != nil {
				return nil, errors.Trace(err)
			}
			d = rows[i].GetDatum(3, &fields[3].Column.FieldType)
			upperBound, err = d.ConvertTo(ctx.GetSessionVars().StmtCtx, tp)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		preCount += count
		hg.AddBucket(&lowerBound, &upperBound, preCount, repeats)
	}
	hg.PreCalculateScalar()
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
	strs := make([]string, 0, hg.NumBuckets()+1)
	if isIndex {
		strs = append(strs, fmt.Sprintf("index:%d ndv:%d", hg.ID, hg.NDV))
	} else {
		strs = append(strs, fmt.Sprintf("column:%d ndv:%d", hg.ID, hg.NDV))
	}
	for i := 0; i < hg.NumBuckets(); i++ {
		upperVal, err := hg.GetUpper(i).ToString()
		terror.Log(errors.Trace(err))
		lowerVal, err := hg.GetLower(i).ToString()
		terror.Log(errors.Trace(err))
		strs = append(strs, fmt.Sprintf("num: %d\tlower_bound: %s\tupper_bound: %s\trepeats: %d", hg.Counts[i], lowerVal, upperVal, hg.Repeats[i]))
	}
	return strings.Join(strs, "\n")
}

// equalRowCount estimates the row count where the column equals to value.
func (hg *Histogram) equalRowCount(sc *stmtctx.StatementContext, value types.Datum) (float64, error) {
	index, match, err := hg.lowerBound(sc, value)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if index == hg.NumBuckets() {
		return 0, nil
	}
	if match {
		return float64(hg.Repeats[index]), nil
	}
	c, err := value.CompareDatum(sc, hg.GetLower(index))
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
	if index == hg.NumBuckets() {
		return hg.totalRowCount(), nil
	}
	curCount := float64(hg.Counts[index])
	prevCount := float64(0)
	if index > 0 {
		prevCount = float64(hg.Counts[index-1])
	}
	lessThanBucketValueCount := curCount - float64(hg.Repeats[index])
	if match {
		return lessThanBucketValueCount, nil
	}
	c, err := value.CompareDatum(sc, hg.GetLower(index))
	if err != nil {
		return 0, errors.Trace(err)
	}
	if c <= 0 {
		return prevCount, nil
	}
	frac := hg.calcFraction(index, &value)
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
	if hg.NumBuckets() == 0 {
		return 0
	}
	return float64(hg.Counts[hg.NumBuckets()-1])
}

func (hg *Histogram) lowerBound(sc *stmtctx.StatementContext, target types.Datum) (index int, match bool, err error) {
	index = sort.Search(hg.NumBuckets(), func(i int) bool {
		// TODO: We can just binary search on the chunk.
		cmp, err1 := hg.GetUpper(i).CompareDatum(sc, &target)
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
func (hg *Histogram) mergeBuckets(bucketIdx int) {
	curBuck := 0
	c := chunk.NewChunk([]*types.FieldType{hg.tp, hg.tp})
	for i := 0; i+1 <= bucketIdx; i += 2 {
		hg.Counts[curBuck] = hg.Counts[i+1]
		hg.Repeats[curBuck] = hg.Repeats[i+1]
		c.AppendDatum(0, hg.GetLower(i))
		c.AppendDatum(1, hg.GetUpper(i+1))
		curBuck++
	}
	if bucketIdx%2 == 0 {
		hg.Counts[curBuck] = hg.Counts[bucketIdx]
		hg.Repeats[curBuck] = hg.Repeats[bucketIdx]
		c.AppendDatum(0, hg.GetLower(bucketIdx))
		c.AppendDatum(1, hg.GetUpper(bucketIdx))
		curBuck++
	}
	hg.Bounds = c
	hg.Counts = hg.Counts[:curBuck]
	hg.Repeats = hg.Repeats[:curBuck]
	return
}

// getIncreaseFactor will return a factor of data increasing after the last analysis.
func (hg *Histogram) getIncreaseFactor(totalCount int64) float64 {
	columnCount := int64(hg.totalRowCount()) + hg.NullCount
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
	for i := 0; i < hg.NumBuckets(); i++ {
		bkt := &tipb.Bucket{
			Count:      hg.Counts[i],
			LowerBound: hg.GetLower(i).GetBytes(),
			UpperBound: hg.GetUpper(i).GetBytes(),
			Repeats:    hg.Repeats[i],
		}
		protoHg.Buckets = append(protoHg.Buckets, bkt)
	}
	return protoHg
}

// HistogramFromProto converts Histogram from its protobuf representation.
// Note that we will set BytesDatum for the lower/upper bound in the bucket, the decode will
// be after all histograms merged.
func HistogramFromProto(protoHg *tipb.Histogram) *Histogram {
	tp := types.NewFieldType(mysql.TypeBlob)
	hg := NewHistogram(0, protoHg.Ndv, 0, 0, tp, len(protoHg.Buckets))
	for _, bucket := range protoHg.Buckets {
		lower, upper := types.NewBytesDatum(bucket.LowerBound), types.NewBytesDatum(bucket.UpperBound)
		hg.AddBucket(&lower, &upper, bucket.Count, bucket.Repeats)
	}
	return hg
}

func (hg *Histogram) popFirstBucket() {
	hg.Counts = hg.Counts[1:]
	hg.Repeats = hg.Repeats[1:]
	c := chunk.NewChunk([]*types.FieldType{hg.tp, hg.tp})
	c.Append(hg.Bounds, 1, hg.Bounds.NumRows())
	hg.Bounds = c
}

// MergeHistograms merges two histograms.
func MergeHistograms(sc *stmtctx.StatementContext, lh *Histogram, rh *Histogram, bucketSize int) (*Histogram, error) {
	if lh.NumBuckets() == 0 {
		return rh, nil
	}
	if rh.NumBuckets() == 0 {
		return lh, nil
	}
	lh.NDV += rh.NDV
	lLen := lh.NumBuckets()
	cmp, err := lh.GetUpper(lLen-1).CompareDatum(sc, rh.GetLower(0))
	if err != nil {
		return nil, errors.Trace(err)
	}
	offset := int64(0)
	if cmp == 0 {
		lh.NDV--
		lh.updateLastBucket(rh.GetUpper(0), lh.Counts[lLen-1]+rh.Counts[0], rh.Repeats[0])
		offset = rh.Counts[0]
		rh.popFirstBucket()
	}
	for lh.NumBuckets() > bucketSize {
		lh.mergeBuckets(lh.NumBuckets() - 1)
	}
	if rh.NumBuckets() == 0 {
		return lh, nil
	}
	for rh.NumBuckets() > bucketSize {
		rh.mergeBuckets(rh.NumBuckets() - 1)
	}
	lCount := lh.Counts[lh.NumBuckets()-1]
	rCount := rh.Counts[rh.NumBuckets()-1] - offset
	lAvg := float64(lCount) / float64(lh.NumBuckets())
	rAvg := float64(rCount) / float64(rh.NumBuckets())
	for lh.NumBuckets() > 1 && lAvg*2 <= rAvg {
		lh.mergeBuckets(lh.NumBuckets() - 1)
		lAvg *= 2
	}
	for rh.NumBuckets() > 1 && rAvg*2 <= lAvg {
		rh.mergeBuckets(rh.NumBuckets() - 1)
		rAvg *= 2
	}
	for i := 0; i < rh.NumBuckets(); i++ {
		lh.AddBucket(rh.GetLower(i), rh.GetUpper(i), rh.Counts[i]+lCount-offset, rh.Repeats[i])
	}
	for lh.NumBuckets() > bucketSize {
		lh.mergeBuckets(lh.NumBuckets() - 1)
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

// getColumnRowCount estimates the row count by a slice of NewRange.
func (c *Column) getColumnRowCount(sc *stmtctx.StatementContext, ranges []*ranger.NewRange) (float64, error) {
	var rowCount float64
	for _, rg := range ranges {
		cmp, err := rg.LowVal[0].CompareDatum(sc, &rg.HighVal[0])
		if err != nil {
			return 0, errors.Trace(err)
		}
		if cmp == 0 {
			// the point case.
			if !rg.LowExclude && !rg.HighExclude {
				var cnt float64
				cnt, err = c.equalRowCount(sc, rg.LowVal[0])
				if err != nil {
					return 0, errors.Trace(err)
				}
				rowCount += cnt
			}
			continue
		}
		// the interval case.
		cnt, err := c.betweenRowCount(sc, rg.LowVal[0], rg.HighVal[0])
		if err != nil {
			return 0, errors.Trace(err)
		}
		if rg.LowExclude {
			lowCnt, err := c.equalRowCount(sc, rg.LowVal[0])
			if err != nil {
				return 0, errors.Trace(err)
			}
			cnt -= lowCnt
		}
		if !rg.HighExclude {
			highCnt, err := c.equalRowCount(sc, rg.HighVal[0])
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

func (idx *Index) getRowCount(sc *stmtctx.StatementContext, indexRanges []*ranger.NewRange) (float64, error) {
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
