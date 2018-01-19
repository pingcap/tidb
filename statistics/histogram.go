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
	// A bucket bound is the smallest and greatest values stored in the bucket. The lower and upper bound
	// are stored in one column.
	//
	// A bucket count is the number of items stored in all previous buckets and the current bucket.
	// Bucket counts are always in increasing order.
	//
	// A bucket repeat is the number of repeats of the bucket value, it can be used to find popular values.
	Bounds  *chunk.Chunk
	Buckets []Bucket

	// Used for estimating fraction of the interval [lower, upper] that lies within the [lower, value].
	// For some types like `Int`, we do not build it because we can get them directly from `Bounds`.
	scalars []scalar
}

// Bucket store the bucket count and repeat.
type Bucket struct {
	Count  int64
	Repeat int64
}

type scalar struct {
	lower        float64
	upper        float64
	commonPfxLen int // commonPfxLen is the common prefix length of the lower bound and upper bound when the value type is KindString or KindBytes.
}

// NewHistogram creates a new histogram.
func NewHistogram(id, ndv, nullCount int64, version uint64, tp *types.FieldType, bucketSize int) *Histogram {
	return &Histogram{
		ID:                id,
		NDV:               ndv,
		NullCount:         nullCount,
		LastUpdateVersion: version,
		tp:                tp,
		Bounds:            chunk.NewChunkWithCapacity([]*types.FieldType{tp}, 2*bucketSize),
		Buckets:           make([]Bucket, 0, bucketSize),
	}
}

// GetLower gets the lower bound of bucket `idx`.
func (hg *Histogram) GetLower(idx int) *types.Datum {
	d := hg.Bounds.GetRow(2*idx).GetDatum(0, hg.tp)
	return &d
}

// GetUpper gets the upper bound of bucket `idx`.
func (hg *Histogram) GetUpper(idx int) *types.Datum {
	d := hg.Bounds.GetRow(2*idx+1).GetDatum(0, hg.tp)
	return &d
}

// AppendBucket appends a bucket into `hg`.
func (hg *Histogram) AppendBucket(lower *types.Datum, upper *types.Datum, count, repeat int64) {
	hg.Buckets = append(hg.Buckets, Bucket{Count: count, Repeat: repeat})
	hg.Bounds.AppendDatum(0, lower)
	hg.Bounds.AppendDatum(0, upper)
}

func (hg *Histogram) updateLastBucket(upper *types.Datum, count, repeat int64) {
	len := hg.Len()
	hg.Bounds.TruncateTo(2*len - 1)
	hg.Bounds.AppendDatum(0, upper)
	hg.Buckets[len-1] = Bucket{Count: count, Repeat: repeat}
}

// DecodeTo decodes the histogram bucket values into `tp`.
func (hg *Histogram) DecodeTo(tp *types.FieldType, timeZone *time.Location) error {
	oldIter := chunk.NewIterator4Chunk(hg.Bounds)
	hg.Bounds = chunk.NewChunkWithCapacity([]*types.FieldType{tp}, oldIter.Len())
	hg.tp = tp
	for row := oldIter.Begin(); row != oldIter.End(); row = oldIter.Next() {
		datum, err := tablecodec.DecodeColumnValue(row.GetBytes(0), tp, timeZone)
		if err != nil {
			return errors.Trace(err)
		}
		hg.Bounds.AppendDatum(0, &datum)
	}
	return nil
}

// ConvertTo converts the histogram bucket values into `tp`.
func (hg *Histogram) ConvertTo(sc *stmtctx.StatementContext, tp *types.FieldType) (*Histogram, error) {
	hist := NewHistogram(hg.ID, hg.NDV, hg.NullCount, hg.LastUpdateVersion, tp, hg.Len())
	iter := chunk.NewIterator4Chunk(hg.Bounds)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		d := row.GetDatum(0, hg.tp)
		d, err := d.ConvertTo(sc, tp)
		if err != nil {
			return nil, errors.Trace(err)
		}
		hist.Bounds.AppendDatum(0, &d)
	}
	hist.Buckets = hg.Buckets
	return hist, nil
}

// Len is the number of buckets in the histogram.
func (hg *Histogram) Len() int {
	return len(hg.Buckets)
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
	for i := range hg.Buckets {
		count := hg.Buckets[i].Count
		if i > 0 {
			count -= hg.Buckets[i-1].Count
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
		insertSQL := fmt.Sprintf("insert into mysql.stats_buckets(table_id, is_index, hist_id, bucket_id, count, repeats, lower_bound, upper_bound) values(%d, %d, %d, %d, %d, %d, X'%X', X'%X')", tableID, isIndex, hg.ID, i, count, hg.Buckets[i].Repeat, lowerBound.GetBytes(), upperBound.GetBytes())
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
	totalCount := int64(0)
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
		totalCount += count
		hg.AppendBucket(&lowerBound, &upperBound, totalCount, repeats)
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
	strs := make([]string, 0, hg.Len()+1)
	if isIndex {
		strs = append(strs, fmt.Sprintf("index:%d ndv:%d", hg.ID, hg.NDV))
	} else {
		strs = append(strs, fmt.Sprintf("column:%d ndv:%d", hg.ID, hg.NDV))
	}
	for i := 0; i < hg.Len(); i++ {
		upperVal, err := hg.GetUpper(i).ToString()
		terror.Log(errors.Trace(err))
		lowerVal, err := hg.GetLower(i).ToString()
		terror.Log(errors.Trace(err))
		strs = append(strs, fmt.Sprintf("num: %d\tlower_bound: %s\tupper_bound: %s\trepeats: %d", hg.Buckets[i].Count, lowerVal, upperVal, hg.Buckets[i].Repeat))
	}
	return strings.Join(strs, "\n")
}

// equalRowCount estimates the row count where the column equals to value.
func (hg *Histogram) equalRowCount(value types.Datum) float64 {
	index, match := hg.Bounds.LowerBound(0, &value)
	// Since we store the lower and upper bound together, if the index is an odd number, then it points to a upper bound.
	if index%2 == 1 {
		if match {
			return float64(hg.Buckets[index/2].Repeat)
		}
		return hg.totalRowCount() / float64(hg.NDV)
	}
	if match {
		cmp := chunk.GetCompareFunc(hg.tp)
		if cmp(hg.Bounds.GetRow(index), 0, hg.Bounds.GetRow(index+1), 0) == 0 {
			return float64(hg.Buckets[index/2].Repeat)
		}
		return hg.totalRowCount() / float64(hg.NDV)
	}
	return 0
}

// greaterRowCount estimates the row count where the column greater than value.
func (hg *Histogram) greaterRowCount(value types.Datum) float64 {
	gtCount := hg.totalRowCount() - hg.lessRowCount(value) - hg.equalRowCount(value)
	if gtCount < 0 {
		gtCount = 0
	}
	return gtCount
}

// greaterAndEqRowCount estimates the row count where the column greater than or equal to value.
func (hg *Histogram) greaterAndEqRowCount(value types.Datum) float64 {
	return hg.totalRowCount() - hg.lessRowCount(value)
}

// lessRowCount estimates the row count where the column less than value.
func (hg *Histogram) lessRowCount(value types.Datum) float64 {
	index, match := hg.Bounds.LowerBound(0, &value)
	if index == hg.Bounds.NumRows() {
		return hg.totalRowCount()
	}
	// Since we store the lower and upper bound together, so dividing the index by 2 will get the bucket index.
	bucketIdx := index / 2
	curCount, curRepeat := float64(hg.Buckets[bucketIdx].Count), float64(hg.Buckets[bucketIdx].Repeat)
	preCount := float64(0)
	if bucketIdx > 0 {
		preCount = float64(hg.Buckets[bucketIdx-1].Count)
	}
	if index%2 == 1 {
		if match {
			return curCount - curRepeat
		}
		return preCount + hg.calcFraction(bucketIdx, &value)*(curCount-curRepeat-preCount)
	}
	return preCount
}

// lessAndEqRowCount estimates the row count where the column less than or equal to value.
func (hg *Histogram) lessAndEqRowCount(value types.Datum) float64 {
	return hg.lessRowCount(value) + hg.equalRowCount(value)
}

// betweenRowCount estimates the row count where column greater or equal to a and less than b.
func (hg *Histogram) betweenRowCount(a, b types.Datum) float64 {
	lessCountA := hg.lessRowCount(a)
	lessCountB := hg.lessRowCount(b)
	// If lessCountA is not less than lessCountB, it may be that they fall to the same bucket and we cannot estimate
	// the fraction, so we use `totalCount / NDV` to estimate the row count, but the result should not greater than lessCountB.
	if lessCountA >= lessCountB {
		return math.Min(lessCountB, hg.totalRowCount()/float64(hg.NDV))
	}
	return lessCountB - lessCountA
}

func (hg *Histogram) totalRowCount() float64 {
	if hg.Len() == 0 {
		return 0
	}
	return float64(hg.Buckets[hg.Len()-1].Count)
}

// mergeBuckets is used to merge every two neighbor buckets.
func (hg *Histogram) mergeBuckets(bucketIdx int) {
	curBuck := 0
	c := chunk.NewChunkWithCapacity([]*types.FieldType{hg.tp}, bucketIdx)
	for i := 0; i+1 <= bucketIdx; i += 2 {
		hg.Buckets[curBuck] = hg.Buckets[i+1]
		c.AppendDatum(0, hg.GetLower(i))
		c.AppendDatum(0, hg.GetUpper(i+1))
		curBuck++
	}
	if bucketIdx%2 == 0 {
		hg.Buckets[curBuck] = hg.Buckets[bucketIdx]
		c.AppendDatum(0, hg.GetLower(bucketIdx))
		c.AppendDatum(0, hg.GetUpper(bucketIdx))
		curBuck++
	}
	hg.Bounds = c
	hg.Buckets = hg.Buckets[:curBuck]
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
	for i := 0; i < hg.Len(); i++ {
		bkt := &tipb.Bucket{
			Count:      hg.Buckets[i].Count,
			LowerBound: hg.GetLower(i).GetBytes(),
			UpperBound: hg.GetUpper(i).GetBytes(),
			Repeats:    hg.Buckets[i].Repeat,
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
		hg.AppendBucket(&lower, &upper, bucket.Count, bucket.Repeats)
	}
	return hg
}

func (hg *Histogram) popFirstBucket() {
	hg.Buckets = hg.Buckets[1:]
	c := chunk.NewChunk([]*types.FieldType{hg.tp, hg.tp})
	c.Append(hg.Bounds, 2, hg.Bounds.NumRows())
	hg.Bounds = c
}

// MergeHistograms merges two histograms.
func MergeHistograms(sc *stmtctx.StatementContext, lh *Histogram, rh *Histogram, bucketSize int) (*Histogram, error) {
	if lh.Len() == 0 {
		return rh, nil
	}
	if rh.Len() == 0 {
		return lh, nil
	}
	lh.NDV += rh.NDV
	lLen := lh.Len()
	cmp, err := lh.GetUpper(lLen-1).CompareDatum(sc, rh.GetLower(0))
	if err != nil {
		return nil, errors.Trace(err)
	}
	offset := int64(0)
	if cmp == 0 {
		lh.NDV--
		lh.updateLastBucket(rh.GetUpper(0), lh.Buckets[lLen-1].Count+rh.Buckets[0].Count, rh.Buckets[0].Repeat)
		offset = rh.Buckets[0].Count
		rh.popFirstBucket()
	}
	for lh.Len() > bucketSize {
		lh.mergeBuckets(lh.Len() - 1)
	}
	if rh.Len() == 0 {
		return lh, nil
	}
	for rh.Len() > bucketSize {
		rh.mergeBuckets(rh.Len() - 1)
	}
	lCount := lh.Buckets[lh.Len()-1].Count
	rCount := rh.Buckets[rh.Len()-1].Count - offset
	lAvg := float64(lCount) / float64(lh.Len())
	rAvg := float64(rCount) / float64(rh.Len())
	for lh.Len() > 1 && lAvg*2 <= rAvg {
		lh.mergeBuckets(lh.Len() - 1)
		lAvg *= 2
	}
	for rh.Len() > 1 && rAvg*2 <= lAvg {
		rh.mergeBuckets(rh.Len() - 1)
		rAvg *= 2
	}
	for i := 0; i < rh.Len(); i++ {
		lh.AppendBucket(rh.GetLower(i), rh.GetUpper(i), rh.Buckets[i].Count+lCount-offset, rh.Buckets[i].Repeat)
	}
	for lh.Len() > bucketSize {
		lh.mergeBuckets(lh.Len() - 1)
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
		count, err := c.CMSketch.queryValue(sc, val)
		return float64(count), errors.Trace(err)
	}
	return c.Histogram.equalRowCount(val), nil
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
		cnt := c.betweenRowCount(rg.LowVal[0], rg.HighVal[0])
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

func (idx *Index) equalRowCount(sc *stmtctx.StatementContext, b []byte) float64 {
	if idx.CMSketch != nil {
		return float64(idx.CMSketch.queryBytes(b))
	}
	return idx.Histogram.equalRowCount(types.NewBytesDatum(b))
}

func (idx *Index) getRowCount(sc *stmtctx.StatementContext, indexRanges []*ranger.NewRange) (float64, error) {
	totalCount := float64(0)
	for _, indexRange := range indexRanges {
		lb, err := codec.EncodeKey(sc, nil, indexRange.LowVal...)
		if err != nil {
			return 0, errors.Trace(err)
		}
		rb, err := codec.EncodeKey(sc, nil, indexRange.HighVal...)
		if err != nil {
			return 0, errors.Trace(err)
		}
		fullLen := len(indexRange.LowVal) == len(indexRange.HighVal) && len(indexRange.LowVal) == len(idx.Info.Columns)
		if fullLen && bytes.Equal(lb, rb) {
			if !indexRange.LowExclude && !indexRange.HighExclude {
				totalCount += idx.equalRowCount(sc, lb)
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
		totalCount += idx.betweenRowCount(l, r)
	}
	if totalCount > idx.totalRowCount() {
		totalCount = idx.totalRowCount()
	}
	return totalCount, nil
}
