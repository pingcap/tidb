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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package statistics

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/twmb/murmur3"
	"go.uber.org/zap"
)

// Histogram represents statistics for a column or index.
type Histogram struct {
	ID        int64 // Column ID.
	NDV       int64 // Number of distinct values.
	NullCount int64 // Number of null values.
	// LastUpdateVersion is the version that this histogram updated last time.
	LastUpdateVersion uint64

	Tp *types.FieldType

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
	// TotColSize is the total column size for the histogram.
	// For unfixed-len types, it includes LEN and BYTE.
	TotColSize int64

	// Correlation is the statistical correlation between physical row ordering and logical ordering of
	// the column values. This ranges from -1 to +1, and it is only valid for Column histogram, not for
	// Index histogram.
	Correlation float64
}

// EmptyHistogramSize is the size of empty histogram, about 112 = 8*6 for int64 & float64, 24*2 for arrays, 8*2 for references.
const EmptyHistogramSize = int64(unsafe.Sizeof(Histogram{}))

// Bucket store the bucket count and repeat.
type Bucket struct {
	Count  int64
	Repeat int64
	NDV    int64
}

// EmptyBucketSize is the size of empty bucket, 3*8=24 now.
const EmptyBucketSize = int64(unsafe.Sizeof(Bucket{}))

type scalar struct {
	lower        float64
	upper        float64
	commonPfxLen int // commonPfxLen is the common prefix length of the lower bound and upper bound when the value type is KindString or KindBytes.
}

// EmptyScalarSize is the size of empty scalar.
const EmptyScalarSize = int64(unsafe.Sizeof(scalar{}))

// NewHistogram creates a new histogram.
func NewHistogram(id, ndv, nullCount int64, version uint64, tp *types.FieldType, bucketSize int, totColSize int64) *Histogram {
	if tp.EvalType() == types.ETString {
		// The histogram will store the string value's 'sort key' representation of its collation.
		// If we directly set the field type's collation to its original one. We would decode the Key representation using its collation.
		// This would cause panic. So we apply a little trick here to avoid decoding it by explicitly changing the collation to 'CollationBin'.
		tp = tp.Clone()
		tp.SetCollate(charset.CollationBin)
	}
	return &Histogram{
		ID:                id,
		NDV:               ndv,
		NullCount:         nullCount,
		LastUpdateVersion: version,
		Tp:                tp,
		Bounds:            chunk.NewChunkWithCapacity([]*types.FieldType{tp}, 2*bucketSize),
		Buckets:           make([]Bucket, 0, bucketSize),
		TotColSize:        totColSize,
	}
}

// GetLower gets the lower bound of bucket `idx`.
func (hg *Histogram) GetLower(idx int) *types.Datum {
	d := hg.Bounds.GetRow(2*idx).GetDatum(0, hg.Tp)
	return &d
}

// GetUpper gets the upper bound of bucket `idx`.
func (hg *Histogram) GetUpper(idx int) *types.Datum {
	d := hg.Bounds.GetRow(2*idx+1).GetDatum(0, hg.Tp)
	return &d
}

// MemoryUsage returns the total memory usage of this Histogram.
func (hg *Histogram) MemoryUsage() (sum int64) {
	if hg == nil {
		return
	}
	sum = EmptyHistogramSize + hg.Bounds.MemoryUsage() + int64(cap(hg.Buckets))*EmptyBucketSize + int64(cap(hg.scalars))*EmptyScalarSize
	return
}

// AvgColSize is the average column size of the histogram. These sizes are derived from function `encode`
// and `Datum::ConvertTo`, so we need to update them if those 2 functions are changed.
func (c *Column) AvgColSize(count int64, isKey bool) float64 {
	if count == 0 {
		return 0
	}
	// Note that, if the handle column is encoded as value, instead of key, i.e,
	// when the handle column is in a unique index, the real column size may be
	// smaller than 8 because it is encoded using `EncodeVarint`. Since we don't
	// know the exact value size now, use 8 as approximation.
	if c.IsHandle {
		return 8
	}
	histCount := c.TotalRowCount()
	notNullRatio := 1.0
	if histCount > 0 {
		notNullRatio = 1.0 - float64(c.NullCount)/histCount
	}
	switch c.Histogram.Tp.GetType() {
	case mysql.TypeFloat, mysql.TypeDouble, mysql.TypeDuration, mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		return 8 * notNullRatio
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear, mysql.TypeEnum, mysql.TypeBit, mysql.TypeSet:
		if isKey {
			return 8 * notNullRatio
		}
	}
	// Keep two decimal place.
	return math.Round(float64(c.TotColSize)/float64(count)*100) / 100
}

// AvgColSizeChunkFormat is the average column size of the histogram. These sizes are derived from function `Encode`
// and `DecodeToChunk`, so we need to update them if those 2 functions are changed.
func (c *Column) AvgColSizeChunkFormat(count int64) float64 {
	if count == 0 {
		return 0
	}
	fixedLen := chunk.GetFixedLen(c.Histogram.Tp)
	if fixedLen != -1 {
		return float64(fixedLen)
	}
	// Keep two decimal place.
	// Add 8 bytes for unfixed-len type's offsets.
	// Minus Log2(avgSize) for unfixed-len type LEN.
	avgSize := float64(c.TotColSize) / float64(count)
	if avgSize < 1 {
		return math.Round(avgSize*100)/100 + 8
	}
	return math.Round((avgSize-math.Log2(avgSize))*100)/100 + 8
}

// AvgColSizeListInDisk is the average column size of the histogram. These sizes are derived
// from `chunk.ListInDisk` so we need to update them if those 2 functions are changed.
func (c *Column) AvgColSizeListInDisk(count int64) float64 {
	if count == 0 {
		return 0
	}
	histCount := c.TotalRowCount()
	notNullRatio := 1.0
	if histCount > 0 {
		notNullRatio = 1.0 - float64(c.NullCount)/histCount
	}
	size := chunk.GetFixedLen(c.Histogram.Tp)
	if size != -1 {
		return float64(size) * notNullRatio
	}
	// Keep two decimal place.
	// Minus Log2(avgSize) for unfixed-len type LEN.
	avgSize := float64(c.TotColSize) / float64(count)
	if avgSize < 1 {
		return math.Round((avgSize)*100) / 100
	}
	return math.Round((avgSize-math.Log2(avgSize))*100) / 100
}

// AppendBucket appends a bucket into `hg`.
func (hg *Histogram) AppendBucket(lower *types.Datum, upper *types.Datum, count, repeat int64) {
	hg.AppendBucketWithNDV(lower, upper, count, repeat, 0)
}

// AppendBucketWithNDV appends a bucket into `hg` and set value for field `NDV`.
func (hg *Histogram) AppendBucketWithNDV(lower *types.Datum, upper *types.Datum, count, repeat, ndv int64) {
	hg.Buckets = append(hg.Buckets, Bucket{Count: count, Repeat: repeat, NDV: ndv})
	hg.Bounds.AppendDatum(0, lower)
	hg.Bounds.AppendDatum(0, upper)
}

func (hg *Histogram) updateLastBucket(upper *types.Datum, count, repeat int64, needBucketNDV bool) {
	l := hg.Len()
	hg.Bounds.TruncateTo(2*l - 1)
	hg.Bounds.AppendDatum(0, upper)
	// The sampling case doesn't hold NDV since the low sampling rate. So check the NDV here.
	if needBucketNDV && hg.Buckets[l-1].NDV > 0 {
		hg.Buckets[l-1].NDV++
	}
	hg.Buckets[l-1].Count = count
	hg.Buckets[l-1].Repeat = repeat
}

// DecodeTo decodes the histogram bucket values into `tp`.
func (hg *Histogram) DecodeTo(tp *types.FieldType, timeZone *time.Location) error {
	oldIter := chunk.NewIterator4Chunk(hg.Bounds)
	hg.Bounds = chunk.NewChunkWithCapacity([]*types.FieldType{tp}, oldIter.Len())
	hg.Tp = tp
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
	hist := NewHistogram(hg.ID, hg.NDV, hg.NullCount, hg.LastUpdateVersion, tp, hg.Len(), hg.TotColSize)
	hist.Correlation = hg.Correlation
	iter := chunk.NewIterator4Chunk(hg.Bounds)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		d := row.GetDatum(0, hg.Tp)
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
	return bytes.Equal([]byte(a.ToString(0)), []byte(b.ToString(0)))
}

// constants for stats version. These const can be used for solving compatibility issue.
const (
	// Version0 is the state that no statistics is actually collected, only the meta info.(the total count and the average col size)
	Version0 = 0
	// Version1 maintains the statistics in the following way.
	// Column stats: CM Sketch is built in TiKV using full data. Histogram is built from samples. TopN is extracted from CM Sketch.
	//    TopN + CM Sketch represent all data. Histogram also represents all data.
	// Index stats: CM Sketch and Histogram is built in TiKV using full data. TopN is extracted from histogram. Then values covered by TopN is removed from CM Sketch.
	//    TopN + CM Sketch represent all data. Histogram also represents all data.
	// Int PK column stats is always Version1 because it only has histogram built from full data.
	// Fast analyze is always Version1 currently.
	Version1 = 1
	// Version2 maintains the statistics in the following way.
	// Column stats: CM Sketch is not used. TopN and Histogram are built from samples. TopN + Histogram represent all data.
	// Index stats: CM SKetch is not used. TopN and Histograms are built from samples.
	//    Then values covered by TopN is removed from Histogram. TopN + Histogram represent all data.
	// Both Column and Index's NDVs are collected by full scan.
	Version2 = 2
)

// AnalyzeFlag is set when the statistics comes from analyze and has not been modified by feedback.
const AnalyzeFlag = 1

// IsAnalyzed checks whether this flag contains AnalyzeFlag.
func IsAnalyzed(flag int64) bool {
	return (flag & AnalyzeFlag) > 0
}

// ResetAnalyzeFlag resets the AnalyzeFlag because it has been modified by feedback.
func ResetAnalyzeFlag(flag int64) int64 {
	return flag &^ AnalyzeFlag
}

// ValueToString converts a possible encoded value to a formatted string. If the value is encoded, then
// idxCols equals to number of origin values, else idxCols is 0.
func ValueToString(vars *variable.SessionVars, value *types.Datum, idxCols int, idxColumnTypes []byte) (string, error) {
	if idxCols == 0 {
		return value.ToString()
	}
	var loc *time.Location
	if vars != nil {
		loc = vars.Location()
	}
	// Ignore the error and treat remaining part that cannot decode successfully as bytes.
	decodedVals, remained, err := codec.DecodeRange(value.GetBytes(), idxCols, idxColumnTypes, loc)
	// Ignore err explicit to pass errcheck.
	_ = err
	if len(remained) > 0 {
		decodedVals = append(decodedVals, types.NewBytesDatum(remained))
	}
	str, err := types.DatumsToString(decodedVals, true)
	return str, err
}

// BucketToString change the given bucket to string format.
func (hg *Histogram) BucketToString(bktID, idxCols int) string {
	upperVal, err := ValueToString(nil, hg.GetUpper(bktID), idxCols, nil)
	terror.Log(errors.Trace(err))
	lowerVal, err := ValueToString(nil, hg.GetLower(bktID), idxCols, nil)
	terror.Log(errors.Trace(err))
	return fmt.Sprintf("num: %d lower_bound: %s upper_bound: %s repeats: %d ndv: %d", hg.bucketCount(bktID), lowerVal, upperVal, hg.Buckets[bktID].Repeat, hg.Buckets[bktID].NDV)
}

// RemoveVals remove the given values from the histogram.
// This function contains an **ASSUMPTION**: valCntPairs is sorted in ascending order.
func (hg *Histogram) RemoveVals(valCntPairs []TopNMeta) {
	totalSubCnt := int64(0)
	var cmpResult int
	for bktIdx, pairIdx := 0, 0; bktIdx < hg.Len(); bktIdx++ {
		for pairIdx < len(valCntPairs) {
			// If the current val smaller than current bucket's lower bound, skip it.
			cmpResult = bytes.Compare(hg.Bounds.Column(0).GetRaw(bktIdx*2), valCntPairs[pairIdx].Encoded)
			if cmpResult > 0 {
				pairIdx++
				continue
			}
			// If the current val bigger than current bucket's upper bound, break.
			cmpResult = bytes.Compare(hg.Bounds.Column(0).GetRaw(bktIdx*2+1), valCntPairs[pairIdx].Encoded)
			if cmpResult < 0 {
				break
			}
			totalSubCnt += int64(valCntPairs[pairIdx].Count)
			if hg.Buckets[bktIdx].NDV > 0 {
				hg.Buckets[bktIdx].NDV--
			}
			pairIdx++
			if cmpResult == 0 {
				hg.Buckets[bktIdx].Repeat = 0
				break
			}
		}
		hg.Buckets[bktIdx].Count -= totalSubCnt
		if hg.Buckets[bktIdx].Count < 0 {
			hg.Buckets[bktIdx].Count = 0
		}
	}
}

// AddIdxVals adds the given values to the histogram.
func (hg *Histogram) AddIdxVals(idxValCntPairs []TopNMeta) {
	totalAddCnt := int64(0)
	sort.Slice(idxValCntPairs, func(i, j int) bool {
		return bytes.Compare(idxValCntPairs[i].Encoded, idxValCntPairs[j].Encoded) < 0
	})
	for bktIdx, pairIdx := 0, 0; bktIdx < hg.Len(); bktIdx++ {
		for pairIdx < len(idxValCntPairs) {
			// If the current val smaller than current bucket's lower bound, skip it.
			cmpResult := bytes.Compare(hg.Bounds.Column(0).GetBytes(bktIdx*2), idxValCntPairs[pairIdx].Encoded)
			if cmpResult > 0 {
				continue
			}
			// If the current val bigger than current bucket's upper bound, break.
			cmpResult = bytes.Compare(hg.Bounds.Column(0).GetBytes(bktIdx*2+1), idxValCntPairs[pairIdx].Encoded)
			if cmpResult < 0 {
				break
			}
			totalAddCnt += int64(idxValCntPairs[pairIdx].Count)
			hg.Buckets[bktIdx].NDV++
			if cmpResult == 0 {
				hg.Buckets[bktIdx].Repeat = int64(idxValCntPairs[pairIdx].Count)
				pairIdx++
				break
			}
			pairIdx++
		}
		hg.Buckets[bktIdx].Count += totalAddCnt
	}
}

// ToString gets the string representation for the histogram.
func (hg *Histogram) ToString(idxCols int) string {
	strs := make([]string, 0, hg.Len()+1)
	if idxCols > 0 {
		strs = append(strs, fmt.Sprintf("index:%d ndv:%d", hg.ID, hg.NDV))
	} else {
		strs = append(strs, fmt.Sprintf("column:%d ndv:%d totColSize:%d", hg.ID, hg.NDV, hg.TotColSize))
	}
	for i := 0; i < hg.Len(); i++ {
		strs = append(strs, hg.BucketToString(i, idxCols))
	}
	return strings.Join(strs, "\n")
}

// equalRowCount estimates the row count where the column equals to value.
// matched: return true if this returned row count is from Bucket.Repeat or bucket NDV, which is more accurate than if not.
func (hg *Histogram) equalRowCount(value types.Datum, hasBucketNDV bool) (count float64, matched bool) {
	_, bucketIdx, inBucket, match := hg.locateBucket(value)
	if !inBucket {
		return 0, false
	}
	if match {
		return float64(hg.Buckets[bucketIdx].Repeat), true
	}
	if hasBucketNDV && hg.Buckets[bucketIdx].NDV > 1 {
		return float64(hg.bucketCount(bucketIdx)-hg.Buckets[bucketIdx].Repeat) / float64(hg.Buckets[bucketIdx].NDV-1), true
	}
	return hg.notNullCount() / float64(hg.NDV), false
}

// greaterRowCount estimates the row count where the column greater than value.
// It's deprecated. Only used for test.
func (hg *Histogram) greaterRowCount(value types.Datum) float64 {
	histRowCount, _ := hg.equalRowCount(value, false)
	gtCount := hg.notNullCount() - hg.lessRowCount(value) - histRowCount
	return math.Max(0, gtCount)
}

// locateBucket locates where a value falls in the range of the Histogram.
// Return value:
// 	exceed: if the value is larger than the upper bound of the last Bucket of the Histogram
// 	bucketIdx: assuming exceed if false, which Bucket does this value fall in (note: the range before a Bucket is also
//		considered belong to this Bucket)
// 	inBucket: assuming exceed if false, whether this value falls in this Bucket, instead of falls between
//		this Bucket and the previous Bucket.
// 	matchLastValue: assuming inBucket is true, if this value is the last value in this Bucket, which has a counter (Bucket.Repeat)
// Examples:
// 	val0 |<-[bkt0]->| |<-[bkt1]->val1(last value)| val2 |<--val3--[bkt2]->| |<-[bkt3]->| val4
// 	locateBucket(val0): false, 0, false, false
// 	locateBucket(val1): false, 1, true, true
// 	locateBucket(val2): false, 2, false, false
// 	locateBucket(val3): false, 2, true, false
// 	locateBucket(val4): true, 3, false, false
func (hg *Histogram) locateBucket(value types.Datum) (exceed bool, bucketIdx int, inBucket, matchLastValue bool) {
	// Empty histogram
	if hg == nil || hg.Bounds.NumRows() == 0 {
		return true, 0, false, false
	}
	index, match := hg.Bounds.LowerBound(0, &value)
	// The value is larger than the max value in the histogram (exceed is true)
	if index >= hg.Bounds.NumRows() {
		return true, hg.Len() - 1, false, false
	}
	bucketIdx = index / 2
	// The value is before this bucket
	if index%2 == 0 && !match {
		return false, bucketIdx, false, false
	}
	// The value matches the last value in this bucket
	// case 1: The LowerBound()'s return value tells us the value matches an upper bound of a bucket
	// case 2: We compare and find that the value is equal to the upper bound of this bucket. This might happen when
	//           the bucket's lower bound is equal to its upper bound.
	if (index%2 == 1 && match) || chunk.Compare(hg.Bounds.GetRow(bucketIdx*2+1), 0, &value) == 0 {
		return false, bucketIdx, true, true
	}
	// The value is in the bucket and isn't the last value in this bucket
	return false, bucketIdx, true, false
}

// LessRowCountWithBktIdx estimates the row count where the column less than value.
func (hg *Histogram) LessRowCountWithBktIdx(value types.Datum) (float64, int) {
	// All the values are null.
	if hg.Bounds.NumRows() == 0 {
		return 0, 0
	}
	exceed, bucketIdx, inBucket, match := hg.locateBucket(value)
	if exceed {
		return hg.notNullCount(), hg.Len() - 1
	}
	preCount := float64(0)
	if bucketIdx > 0 {
		preCount = float64(hg.Buckets[bucketIdx-1].Count)
	}
	if !inBucket {
		return preCount, bucketIdx
	}
	curCount, curRepeat := float64(hg.Buckets[bucketIdx].Count), float64(hg.Buckets[bucketIdx].Repeat)
	if match {
		return curCount - curRepeat, bucketIdx
	}
	return preCount + hg.calcFraction(bucketIdx, &value)*(curCount-curRepeat-preCount), bucketIdx
}

func (hg *Histogram) lessRowCount(value types.Datum) float64 {
	result, _ := hg.LessRowCountWithBktIdx(value)
	return result
}

// BetweenRowCount estimates the row count where column greater or equal to a and less than b.
func (hg *Histogram) BetweenRowCount(a, b types.Datum) float64 {
	lessCountA := hg.lessRowCount(a)
	lessCountB := hg.lessRowCount(b)
	// If lessCountA is not less than lessCountB, it may be that they fall to the same bucket and we cannot estimate
	// the fraction, so we use `totalCount / NDV` to estimate the row count, but the result should not greater than
	// lessCountB or notNullCount-lessCountA.
	if lessCountA >= lessCountB && hg.NDV > 0 {
		result := math.Min(lessCountB, hg.notNullCount()-lessCountA)
		return math.Min(result, hg.notNullCount()/float64(hg.NDV))
	}
	return lessCountB - lessCountA
}

// BetweenRowCount estimates the row count for interval [l, r).
func (c *Column) BetweenRowCount(sctx sessionctx.Context, l, r types.Datum, lowEncoded, highEncoded []byte) float64 {
	histBetweenCnt := c.Histogram.BetweenRowCount(l, r)
	if c.StatsVer <= Version1 {
		return histBetweenCnt
	}
	return float64(c.TopN.BetweenCount(lowEncoded, highEncoded)) + histBetweenCnt
}

// TotalRowCount returns the total count of this histogram.
func (hg *Histogram) TotalRowCount() float64 {
	return hg.notNullCount() + float64(hg.NullCount)
}

// notNullCount indicates the count of non-null values in column histogram and single-column index histogram,
// for multi-column index histogram, since we cannot define null for the row, we treat all rows as non-null, that means,
// notNullCount would return same value as TotalRowCount for multi-column index histograms.
func (hg *Histogram) notNullCount() float64 {
	if hg.Len() == 0 {
		return 0
	}
	return float64(hg.Buckets[hg.Len()-1].Count)
}

// mergeBuckets is used to Merge every two neighbor buckets.
func (hg *Histogram) mergeBuckets(bucketIdx int) {
	curBuck := 0
	c := chunk.NewChunkWithCapacity([]*types.FieldType{hg.Tp}, bucketIdx)
	for i := 0; i+1 <= bucketIdx; i += 2 {
		hg.Buckets[curBuck].NDV = hg.Buckets[i+1].NDV + hg.Buckets[i].NDV
		hg.Buckets[curBuck].Count = hg.Buckets[i+1].Count
		hg.Buckets[curBuck].Repeat = hg.Buckets[i+1].Repeat
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
}

// GetIncreaseFactor get the increase factor to adjust the final estimated count when the table is modified.
func (idx *Index) GetIncreaseFactor(realtimeRowCount int64) float64 {
	columnCount := idx.TotalRowCount()
	if columnCount == 0 {
		return 1.0
	}
	return float64(realtimeRowCount) / columnCount
}

// BetweenRowCount estimates the row count for interval [l, r).
func (idx *Index) BetweenRowCount(l, r types.Datum) float64 {
	histBetweenCnt := idx.Histogram.BetweenRowCount(l, r)
	if idx.StatsVer == Version1 {
		return histBetweenCnt
	}
	return float64(idx.TopN.BetweenCount(l.GetBytes(), r.GetBytes())) + histBetweenCnt
}

// GetIncreaseFactor will return a factor of data increasing after the last analysis.
func (hg *Histogram) GetIncreaseFactor(totalCount int64) float64 {
	columnCount := hg.TotalRowCount()
	if columnCount == 0 {
		// avoid dividing by 0
		return 1.0
	}
	return float64(totalCount) / columnCount
}

// validRange checks if the range is Valid, it is used by `SplitRange` to remove the invalid range,
// the possible types of range are index key range and handle key range.
func validRange(sc *stmtctx.StatementContext, ran *ranger.Range, encoded bool) bool {
	var low, high []byte
	if encoded {
		low, high = ran.LowVal[0].GetBytes(), ran.HighVal[0].GetBytes()
	} else {
		var err error
		low, err = codec.EncodeKey(sc, nil, ran.LowVal[0])
		if err != nil {
			return false
		}
		high, err = codec.EncodeKey(sc, nil, ran.HighVal[0])
		if err != nil {
			return false
		}
	}
	if ran.LowExclude {
		low = kv.Key(low).PrefixNext()
	}
	if !ran.HighExclude {
		high = kv.Key(high).PrefixNext()
	}
	return bytes.Compare(low, high) < 0
}

func checkKind(vals []types.Datum, kind byte) bool {
	if kind == types.KindString {
		kind = types.KindBytes
	}
	for _, val := range vals {
		valKind := val.Kind()
		if valKind == types.KindNull || valKind == types.KindMinNotNull || valKind == types.KindMaxValue {
			continue
		}
		if valKind == types.KindString {
			valKind = types.KindBytes
		}
		if valKind != kind {
			return false
		}
		// Only check the first non-null value.
		break
	}
	return true
}

func (hg *Histogram) typeMatch(ranges []*ranger.Range) bool {
	kind := hg.GetLower(0).Kind()
	for _, ran := range ranges {
		if !checkKind(ran.LowVal, kind) || !checkKind(ran.HighVal, kind) {
			return false
		}
	}
	return true
}

// SplitRange splits the range according to the histogram lower bound. Note that we treat first bucket's lower bound
// as -inf and last bucket's upper bound as +inf, so all the split ranges will totally fall in one of the (-inf, l(1)),
// [l(1), l(2)),...[l(n-2), l(n-1)), [l(n-1), +inf), where n is the number of buckets, l(i) is the i-th bucket's lower bound.
func (hg *Histogram) SplitRange(sc *stmtctx.StatementContext, oldRanges []*ranger.Range, encoded bool) ([]*ranger.Range, bool) {
	if !hg.typeMatch(oldRanges) {
		return oldRanges, false
	}
	// Treat the only buckets as (-inf, +inf), so we do not need split it.
	if hg.Len() == 1 {
		return oldRanges, true
	}
	ranges := make([]*ranger.Range, 0, len(oldRanges))
	for _, ran := range oldRanges {
		ranges = append(ranges, ran.Clone())
	}
	split := make([]*ranger.Range, 0, len(ranges))
	for len(ranges) > 0 {
		// Find the first bound that greater than the LowVal.
		idx := hg.Bounds.UpperBound(0, &ranges[0].LowVal[0])
		// Treat last bucket's upper bound as +inf, so we do not need split any more.
		if idx >= hg.Bounds.NumRows()-1 {
			split = append(split, ranges...)
			break
		}
		// Treat first buckets's lower bound as -inf, just increase it to the next lower bound.
		if idx == 0 {
			idx = 2
		}
		// Get the next lower bound.
		if idx%2 == 1 {
			idx++
		}
		lowerBound := hg.Bounds.GetRow(idx)
		var i int
		// Find the first range that need to be split by the lower bound.
		for ; i < len(ranges); i++ {
			if chunk.Compare(lowerBound, 0, &ranges[i].HighVal[0]) <= 0 {
				break
			}
		}
		split = append(split, ranges[:i]...)
		ranges = ranges[i:]
		if len(ranges) == 0 {
			break
		}
		// Split according to the lower bound.
		cmp := chunk.Compare(lowerBound, 0, &ranges[0].LowVal[0])
		if cmp > 0 {
			lower := lowerBound.GetDatum(0, hg.Tp)
			newRange := &ranger.Range{
				LowExclude:  ranges[0].LowExclude,
				LowVal:      []types.Datum{ranges[0].LowVal[0]},
				HighVal:     []types.Datum{lower},
				HighExclude: true,
				Collators:   ranges[0].Collators,
			}
			if validRange(sc, newRange, encoded) {
				split = append(split, newRange)
			}
			ranges[0].LowVal[0] = lower
			ranges[0].LowExclude = false
			if !validRange(sc, ranges[0], encoded) {
				ranges = ranges[1:]
			}
		}
	}
	return split, true
}

func (hg *Histogram) bucketCount(idx int) int64 {
	if idx == 0 {
		return hg.Buckets[0].Count
	}
	return hg.Buckets[idx].Count - hg.Buckets[idx-1].Count
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
			Ndv:        &hg.Buckets[i].NDV,
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
	hg := NewHistogram(0, protoHg.Ndv, 0, 0, tp, len(protoHg.Buckets), 0)
	for _, bucket := range protoHg.Buckets {
		lower, upper := types.NewBytesDatum(bucket.LowerBound), types.NewBytesDatum(bucket.UpperBound)
		if bucket.Ndv != nil {
			hg.AppendBucketWithNDV(&lower, &upper, bucket.Count, bucket.Repeats, *bucket.Ndv)
		} else {
			hg.AppendBucket(&lower, &upper, bucket.Count, bucket.Repeats)
		}
	}
	return hg
}

func (hg *Histogram) popFirstBucket() {
	hg.Buckets = hg.Buckets[1:]
	c := chunk.NewChunkWithCapacity([]*types.FieldType{hg.Tp, hg.Tp}, hg.Bounds.NumRows()-2)
	c.Append(hg.Bounds, 2, hg.Bounds.NumRows())
	hg.Bounds = c
}

// IsIndexHist checks whether current histogram is one for index.
func (hg *Histogram) IsIndexHist() bool {
	return hg.Tp.GetType() == mysql.TypeBlob
}

// MergeHistograms merges two histograms.
func MergeHistograms(sc *stmtctx.StatementContext, lh *Histogram, rh *Histogram, bucketSize int, statsVer int) (*Histogram, error) {
	if lh.Len() == 0 {
		return rh, nil
	}
	if rh.Len() == 0 {
		return lh, nil
	}
	lh.NDV += rh.NDV
	lLen := lh.Len()
	cmp, err := lh.GetUpper(lLen-1).Compare(sc, rh.GetLower(0), collate.GetBinaryCollator())
	if err != nil {
		return nil, errors.Trace(err)
	}
	offset := int64(0)
	if cmp == 0 {
		lh.NDV--
		lh.Buckets[lLen-1].NDV += rh.Buckets[0].NDV
		// There's an overlapped one. So we need to subtract it if needed.
		if rh.Buckets[0].NDV > 0 && lh.Buckets[lLen-1].Repeat > 0 {
			lh.Buckets[lLen-1].NDV--
		}
		lh.updateLastBucket(rh.GetUpper(0), lh.Buckets[lLen-1].Count+rh.Buckets[0].Count, rh.Buckets[0].Repeat, false)
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
		if statsVer >= Version2 {
			lh.AppendBucketWithNDV(rh.GetLower(i), rh.GetUpper(i), rh.Buckets[i].Count+lCount-offset, rh.Buckets[i].Repeat, rh.Buckets[i].NDV)
			continue
		}
		lh.AppendBucket(rh.GetLower(i), rh.GetUpper(i), rh.Buckets[i].Count+lCount-offset, rh.Buckets[i].Repeat)
	}
	for lh.Len() > bucketSize {
		lh.mergeBuckets(lh.Len() - 1)
	}
	return lh, nil
}

// AvgCountPerNotNullValue gets the average row count per value by the data of histogram.
func (hg *Histogram) AvgCountPerNotNullValue(totalCount int64) float64 {
	factor := hg.GetIncreaseFactor(totalCount)
	totalNotNull := hg.notNullCount() * factor
	curNDV := float64(hg.NDV) * factor
	curNDV = math.Max(curNDV, 1)
	return totalNotNull / curNDV
}

func (hg *Histogram) outOfRange(val types.Datum) bool {
	if hg.Len() == 0 {
		return false
	}
	return chunk.Compare(hg.Bounds.GetRow(0), 0, &val) > 0 ||
		chunk.Compare(hg.Bounds.GetRow(hg.Bounds.NumRows()-1), 0, &val) < 0
}

// outOfRangeRowCount estimate the row count of part of [lDatum, rDatum] which is out of range of the histogram.
// Here we assume the density of data is decreasing from the lower/upper bound of the histogram toward outside.
// The maximum row count it can get is the increaseCount. It reaches the maximum when out-of-range width reaches histogram range width.
// As it shows below. To calculate the out-of-range row count, we need to calculate the percentage of the shaded area.
// Note that we assume histL-boundL == histR-histL == boundR-histR here.
//
//               /│             │\
//             /  │             │  \
//           /x│  │◄─histogram─►│    \
//         / xx│  │    range    │      \
//       / │xxx│  │             │        \
//     /   │xxx│  │             │          \
//────┴────┴───┴──┴─────────────┴───────────┴─────
//    ▲    ▲   ▲  ▲             ▲           ▲
//    │    │   │  │             │           │
// boundL  │   │histL         histR       boundR
//         │   │
//    lDatum  rDatum
func (hg *Histogram) outOfRangeRowCount(lDatum, rDatum *types.Datum, increaseCount int64) float64 {
	if hg.Len() == 0 {
		return 0
	}

	// For bytes and string type, we need to cut the common prefix when converting them to scalar value.
	// Here we calculate the length of common prefix.
	commonPrefix := 0
	if hg.GetLower(0).Kind() == types.KindBytes || hg.GetLower(0).Kind() == types.KindString {
		// Calculate the common prefix length among the lower and upper bound of histogram and the range we want to estimate.
		commonPrefix = commonPrefixLength(hg.GetLower(0).GetBytes(),
			hg.GetUpper(hg.Len()-1).GetBytes(),
			lDatum.GetBytes(),
			rDatum.GetBytes())
	}

	// Convert the range we want to estimate to scalar value(float64)
	l := convertDatumToScalar(lDatum, commonPrefix)
	r := convertDatumToScalar(rDatum, commonPrefix)
	// If this is an unsigned column, we need to make sure values are not negative.
	// Normal negative value should have become 0. But this still might happen when met MinNotNull here.
	// Maybe it's better to do this transformation in the ranger like the normal negative value.
	if mysql.HasUnsignedFlag(hg.Tp.GetFlag()) {
		if l < 0 {
			l = 0
		}
		if r < 0 {
			r = 0
		}
	}

	// make sure l < r
	if l >= r {
		return 0
	}
	// Convert the lower and upper bound of the histogram to scalar value(float64)
	histL := convertDatumToScalar(hg.GetLower(0), commonPrefix)
	histR := convertDatumToScalar(hg.GetUpper(hg.Len()-1), commonPrefix)
	histWidth := histR - histL
	if histWidth <= 0 {
		return 0
	}
	boundL := histL - histWidth
	boundR := histR + histWidth

	leftPercent := float64(0)
	rightPercent := float64(0)

	// keep l and r unchanged, use actualL and actualR to calculate.
	actualL := l
	actualR := r
	// If the range overlaps with (boundL,histL), we need to handle the out-of-range part on the left of the histogram range
	if actualL < histL && actualR > boundL {
		// make sure boundL <= actualL < actualR <= histL
		if actualL < boundL {
			actualL = boundL
		}
		if actualR > histL {
			actualR = histL
		}
		// Calculate the percentage of "the shaded area" on the left side.
		leftPercent = (math.Pow(actualR-boundL, 2) - math.Pow(actualL-boundL, 2)) / math.Pow(histWidth, 2)
	}

	actualL = l
	actualR = r
	// If the range overlaps with (histR,boundR), we need to handle the out-of-range part on the right of the histogram range
	if actualL < boundR && actualR > histR {
		// make sure histR <= actualL < actualR <= boundR
		if actualL < histR {
			actualL = histR
		}
		if actualR > boundR {
			actualR = boundR
		}
		// Calculate the percentage of "the shaded area" on the right side.
		rightPercent = (math.Pow(boundR-actualL, 2) - math.Pow(boundR-actualR, 2)) / math.Pow(histWidth, 2)
	}

	totalPercent := leftPercent*0.5 + rightPercent*0.5
	if totalPercent > 1 {
		totalPercent = 1
	}
	rowCount := totalPercent * hg.notNullCount()
	if rowCount > float64(increaseCount) {
		return float64(increaseCount)
	}
	return rowCount
}

// Copy deep copies the histogram.
func (hg *Histogram) Copy() *Histogram {
	newHist := *hg
	newHist.Bounds = hg.Bounds.CopyConstruct()
	newHist.Buckets = make([]Bucket, 0, len(hg.Buckets))
	newHist.Buckets = append(newHist.Buckets, hg.Buckets...)
	return &newHist
}

// RemoveUpperBound removes the upper bound from histogram.
// It is used when merge stats for incremental analyze.
func (hg *Histogram) RemoveUpperBound() *Histogram {
	hg.Buckets[hg.Len()-1].Count -= hg.Buckets[hg.Len()-1].Repeat
	hg.Buckets[hg.Len()-1].Repeat = 0
	if hg.NDV > 0 {
		hg.NDV--
	}
	return hg
}

// TruncateHistogram truncates the histogram to `numBkt` buckets.
func (hg *Histogram) TruncateHistogram(numBkt int) *Histogram {
	hist := hg.Copy()
	hist.Buckets = hist.Buckets[:numBkt]
	hist.Bounds.TruncateTo(numBkt * 2)
	return hist
}

// ErrorRate is the error rate of estimate row count by bucket and cm sketch.
type ErrorRate struct {
	ErrorTotal float64
	QueryTotal int64
}

// MaxErrorRate is the max error rate of estimate row count of a not pseudo column.
// If the table is pseudo, but the average error rate is less than MaxErrorRate,
// then the column is not pseudo.
const MaxErrorRate = 0.25

// NotAccurate is true when the total of query is zero or the average error
// rate is greater than MaxErrorRate.
func (e *ErrorRate) NotAccurate() bool {
	if e.QueryTotal == 0 {
		return true
	}
	return e.ErrorTotal/float64(e.QueryTotal) > MaxErrorRate
}

// Update updates the ErrorRate.
func (e *ErrorRate) Update(rate float64) {
	e.QueryTotal++
	e.ErrorTotal += rate
}

// Merge range merges two ErrorRate.
func (e *ErrorRate) Merge(rate *ErrorRate) {
	e.QueryTotal += rate.QueryTotal
	e.ErrorTotal += rate.ErrorTotal
}

// Column represents a column histogram.
type Column struct {
	Histogram
	*CMSketch
	*TopN
	*FMSketch
	PhysicalID int64
	Count      int64
	Info       *model.ColumnInfo
	IsHandle   bool
	ErrorRate
	Flag           int64
	LastAnalyzePos types.Datum
	StatsVer       int64 // StatsVer is the version of the current stats, used to maintain compatibility

	// Loaded means if the histogram, the topn and the cm sketch are loaded fully.
	// Those three parts of a Column is loaded lazily. It will only be loaded after trying to use them.
	// Note: Currently please use Column.IsLoaded() to check if it's loaded.
	Loaded bool
}

// IsLoaded is a wrap around c.Loaded.
// It's just for safe when we are switching from `c.notNullCount() > 0)` to `c.Loaded`.
func (c *Column) IsLoaded() bool {
	return c.Loaded || c.notNullCount() > 0
}

func (c *Column) String() string {
	return c.Histogram.ToString(0)
}

// TotalRowCount returns the total count of this column.
func (c *Column) TotalRowCount() float64 {
	if c.StatsVer >= Version2 {
		return c.Histogram.TotalRowCount() + float64(c.TopN.TotalCount())
	}
	return c.Histogram.TotalRowCount()
}

func (c *Column) notNullCount() float64 {
	if c.StatsVer >= Version2 {
		return c.Histogram.notNullCount() + float64(c.TopN.TotalCount())
	}
	return c.Histogram.notNullCount()
}

// GetIncreaseFactor get the increase factor to adjust the final estimated count when the table is modified.
func (c *Column) GetIncreaseFactor(realtimeRowCount int64) float64 {
	columnCount := c.TotalRowCount()
	if columnCount == 0 {
		// avoid dividing by 0
		return 1.0
	}
	return float64(realtimeRowCount) / columnCount
}

// MemoryUsage returns the total memory usage of Histogram, CMSketch, FMSketch in Column.
// We ignore the size of other metadata in Column
func (c *Column) MemoryUsage() CacheItemMemoryUsage {
	var sum int64
	columnMemUsage := &ColumnMemUsage{
		ColumnID: c.Info.ID,
	}
	histogramMemUsage := c.Histogram.MemoryUsage()
	columnMemUsage.HistogramMemUsage = histogramMemUsage
	sum = histogramMemUsage
	if c.CMSketch != nil {
		cmSketchMemUsage := c.CMSketch.MemoryUsage()
		columnMemUsage.CMSketchMemUsage = cmSketchMemUsage
		sum += cmSketchMemUsage
	}
	if c.FMSketch != nil {
		fmSketchMemUsage := c.FMSketch.MemoryUsage()
		columnMemUsage.FMSketchMemUsage = fmSketchMemUsage
		sum += fmSketchMemUsage
	}
	columnMemUsage.TotalMemUsage = sum
	return columnMemUsage
}

// HistogramNeededColumns stores the columns whose Histograms need to be loaded from physical kv layer.
// Currently, we only load index/pk's Histogram from kv automatically. Columns' are loaded by needs.
var HistogramNeededColumns = neededColumnMap{cols: map[tableColumnID]struct{}{}}

// IsInvalid checks if this column is invalid. If this column has histogram but not loaded yet, then we mark it
// as need histogram.
func (c *Column) IsInvalid(sctx sessionctx.Context, collPseudo bool) bool {
	if collPseudo && c.NotAccurate() {
		return true
	}
	if sctx != nil {
		stmtctx := sctx.GetSessionVars().StmtCtx
		if stmtctx != nil && stmtctx.StatsLoad.Fallback {
			return true
		}
		if !c.IsLoaded() && stmtctx != nil {
			if stmtctx.StatsLoad.Timeout > 0 {
				logutil.BgLogger().Warn("Hist for column should already be loaded as sync but not found.",
					zap.String(strconv.FormatInt(c.Info.ID, 10), c.Info.Name.O))
			}
			// In some tests, the c.Info is not set, so we add this check here.
			if c.Info != nil {
				HistogramNeededColumns.insert(tableColumnID{TableID: c.PhysicalID, ColumnID: c.Info.ID})
			}
		}
	}
	return c.TotalRowCount() == 0 || (!c.IsLoaded() && c.Histogram.NDV > 0)
}

// IsHistNeeded checks if this column needs histogram to be loaded
func (c *Column) IsHistNeeded(collPseudo bool) bool {
	return (!collPseudo || !c.NotAccurate()) && !c.IsLoaded()
}

func (c *Column) equalRowCount(sctx sessionctx.Context, val types.Datum, encodedVal []byte, realtimeRowCount int64) (float64, error) {
	if val.IsNull() {
		return float64(c.NullCount), nil
	}
	if c.StatsVer < Version2 {
		// All the values are null.
		if c.Histogram.Bounds.NumRows() == 0 {
			return 0.0, nil
		}
		if c.Histogram.NDV > 0 && c.outOfRange(val) {
			return outOfRangeEQSelectivity(c.Histogram.NDV, realtimeRowCount, int64(c.TotalRowCount())) * c.TotalRowCount(), nil
		}
		if c.CMSketch != nil {
			count, err := queryValue(sctx.GetSessionVars().StmtCtx, c.CMSketch, c.TopN, val)
			return float64(count), errors.Trace(err)
		}
		histRowCount, _ := c.Histogram.equalRowCount(val, false)
		return histRowCount, nil
	}

	// Stats version == 2
	// All the values are null.
	if c.Histogram.Bounds.NumRows() == 0 && c.TopN.Num() == 0 {
		return 0, nil
	}
	// 1. try to find this value in TopN
	if c.TopN != nil {
		rowcount, ok := c.QueryTopN(encodedVal)
		if ok {
			return float64(rowcount), nil
		}
	}
	// 2. try to find this value in bucket.Repeat(the last value in every bucket)
	histCnt, matched := c.Histogram.equalRowCount(val, true)
	if matched {
		return histCnt, nil
	}
	// 3. use uniform distribution assumption for the rest (even when this value is not covered by the range of stats)
	histNDV := float64(c.Histogram.NDV - int64(c.TopN.Num()))
	if histNDV <= 0 {
		return 0, nil
	}
	return c.Histogram.notNullCount() / histNDV, nil
}

// GetColumnRowCount estimates the row count by a slice of Range.
func (c *Column) GetColumnRowCount(sctx sessionctx.Context, ranges []*ranger.Range, realtimeRowCount int64, pkIsHandle bool) (float64, error) {
	sc := sctx.GetSessionVars().StmtCtx
	var rowCount float64
	for _, rg := range ranges {
		highVal := *rg.HighVal[0].Clone()
		lowVal := *rg.LowVal[0].Clone()
		if highVal.Kind() == types.KindString {
			highVal.SetBytes(collate.GetCollator(highVal.Collation()).Key(highVal.GetString()))
		}
		if lowVal.Kind() == types.KindString {
			lowVal.SetBytes(collate.GetCollator(lowVal.Collation()).Key(lowVal.GetString()))
		}
		cmp, err := lowVal.Compare(sc, &highVal, collate.GetBinaryCollator())
		if err != nil {
			return 0, errors.Trace(err)
		}
		lowEncoded, err := codec.EncodeKey(sc, nil, lowVal)
		if err != nil {
			return 0, err
		}
		highEncoded, err := codec.EncodeKey(sc, nil, highVal)
		if err != nil {
			return 0, err
		}
		if cmp == 0 {
			// case 1: it's a point
			if !rg.LowExclude && !rg.HighExclude {
				// In this case, the row count is at most 1.
				if pkIsHandle {
					rowCount += 1
					continue
				}
				var cnt float64
				cnt, err = c.equalRowCount(sctx, lowVal, lowEncoded, realtimeRowCount)
				if err != nil {
					return 0, errors.Trace(err)
				}
				// If the current table row count has changed, we should scale the row count accordingly.
				cnt *= c.GetIncreaseFactor(realtimeRowCount)
				rowCount += cnt
			}
			continue
		}
		// In stats ver 1, we use CM Sketch to estimate row count for point condition, which is more accurate.
		// So for the small range, we convert it to points.
		if c.StatsVer < 2 {
			rangeVals := enumRangeValues(lowVal, highVal, rg.LowExclude, rg.HighExclude)

			// case 2: it's a small range && using ver1 stats
			if rangeVals != nil {
				for _, val := range rangeVals {
					cnt, err := c.equalRowCount(sctx, val, lowEncoded, realtimeRowCount)
					if err != nil {
						return 0, err
					}
					// If the current table row count has changed, we should scale the row count accordingly.
					cnt *= c.GetIncreaseFactor(realtimeRowCount)
					rowCount += cnt
				}

				continue
			}
		}

		// case 3: it's an interval
		cnt := c.BetweenRowCount(sctx, lowVal, highVal, lowEncoded, highEncoded)
		// `betweenRowCount` returns count for [l, h) range, we adjust cnt for boundaries here.
		// Note that, `cnt` does not include null values, we need specially handle cases
		//   where null is the lower bound.
		// And because we use (2, MaxValue] to represent expressions like a > 2 and use [MinNotNull, 3) to represent
		//   expressions like b < 3, we need to exclude the special values.
		if rg.LowExclude && !lowVal.IsNull() && lowVal.Kind() != types.KindMaxValue && lowVal.Kind() != types.KindMinNotNull {
			lowCnt, err := c.equalRowCount(sctx, lowVal, lowEncoded, realtimeRowCount)
			if err != nil {
				return 0, errors.Trace(err)
			}
			cnt -= lowCnt
			cnt = mathutil.Clamp(cnt, 0, c.notNullCount())
		}
		if !rg.LowExclude && lowVal.IsNull() {
			cnt += float64(c.NullCount)
		}
		if !rg.HighExclude && highVal.Kind() != types.KindMaxValue && highVal.Kind() != types.KindMinNotNull {
			highCnt, err := c.equalRowCount(sctx, highVal, highEncoded, realtimeRowCount)
			if err != nil {
				return 0, errors.Trace(err)
			}
			cnt += highCnt
		}

		cnt = mathutil.Clamp(cnt, 0, c.TotalRowCount())

		// If the current table row count has changed, we should scale the row count accordingly.
		cnt *= c.GetIncreaseFactor(realtimeRowCount)

		// handling the out-of-range part
		if (c.outOfRange(lowVal) && !lowVal.IsNull()) || c.outOfRange(highVal) {
			increaseCount := realtimeRowCount - int64(c.TotalRowCount())
			if increaseCount < 0 {
				increaseCount = 0
			}
			cnt += c.Histogram.outOfRangeRowCount(&lowVal, &highVal, increaseCount)
		}

		rowCount += cnt
	}
	rowCount = mathutil.Clamp(rowCount, 0, float64(realtimeRowCount))
	return rowCount, nil
}

// Index represents an index histogram.
type Index struct {
	Histogram
	*CMSketch
	*TopN
	FMSketch *FMSketch
	ErrorRate
	StatsVer       int64 // StatsVer is the version of the current stats, used to maintain compatibility
	Info           *model.IndexInfo
	Flag           int64
	LastAnalyzePos types.Datum
}

// ItemID implements TableCacheItem
func (idx *Index) ItemID() int64 {
	return idx.Info.ID
}

// DropEvicted implements TableCacheItem
// DropEvicted drops evicted structures
func (idx *Index) DropEvicted() {
	idx.CMSketch = nil
}

// IsEvicted returns whether index statistics got evicted
func (idx *Index) IsEvicted() bool {
	return idx.CMSketch == nil
}

func (idx *Index) String() string {
	return idx.Histogram.ToString(len(idx.Info.Columns))
}

// TotalRowCount returns the total count of this index.
func (idx *Index) TotalRowCount() float64 {
	if idx.StatsVer >= Version2 {
		return idx.Histogram.TotalRowCount() + float64(idx.TopN.TotalCount())
	}
	return idx.Histogram.TotalRowCount()
}

// IsInvalid checks if this index is invalid.
func (idx *Index) IsInvalid(collPseudo bool) bool {
	return (collPseudo && idx.NotAccurate()) || idx.TotalRowCount() == 0
}

// MemoryUsage returns the total memory usage of a Histogram and CMSketch in Index.
// We ignore the size of other metadata in Index.
func (idx *Index) MemoryUsage() CacheItemMemoryUsage {
	var sum int64
	indexMemUsage := &IndexMemUsage{
		IndexID: idx.Info.ID,
	}
	histMemUsage := idx.Histogram.MemoryUsage()
	indexMemUsage.HistogramMemUsage = histMemUsage
	sum = histMemUsage
	if idx.CMSketch != nil {
		cmSketchMemUsage := idx.CMSketch.MemoryUsage()
		indexMemUsage.CMSketchMemUsage = cmSketchMemUsage
		sum += cmSketchMemUsage
	}
	indexMemUsage.TotalMemUsage = sum
	return indexMemUsage
}

var nullKeyBytes, _ = codec.EncodeKey(nil, nil, types.NewDatum(nil))

func (idx *Index) equalRowCount(b []byte, realtimeRowCount int64) float64 {
	if len(idx.Info.Columns) == 1 {
		if bytes.Equal(b, nullKeyBytes) {
			return float64(idx.NullCount)
		}
	}
	val := types.NewBytesDatum(b)
	if idx.StatsVer < Version2 {
		if idx.NDV > 0 && idx.outOfRange(val) {
			return outOfRangeEQSelectivity(idx.NDV, realtimeRowCount, int64(idx.TotalRowCount())) * idx.TotalRowCount()
		}
		if idx.CMSketch != nil {
			return float64(idx.QueryBytes(b))
		}
		histRowCount, _ := idx.Histogram.equalRowCount(val, false)
		return histRowCount
	}
	// stats version == 2
	// 1. try to find this value in TopN
	count, found := idx.TopN.QueryTopN(b)
	if found {
		return float64(count)
	}
	// 2. try to find this value in bucket.Repeat(the last value in every bucket)
	histCnt, matched := idx.Histogram.equalRowCount(val, true)
	if matched {
		return histCnt
	}
	// 3. use uniform distribution assumption for the rest (even when this value is not covered by the range of stats)
	histNDV := float64(idx.Histogram.NDV - int64(idx.TopN.Num()))
	if histNDV <= 0 {
		return 0
	}
	return idx.Histogram.notNullCount() / histNDV
}

// QueryBytes is used to query the count of specified bytes.
func (idx *Index) QueryBytes(d []byte) uint64 {
	h1, h2 := murmur3.Sum128(d)
	if count, ok := idx.TopN.QueryTopN(d); ok {
		return count
	}
	return idx.queryHashValue(h1, h2)
}

// GetRowCount returns the row count of the given ranges.
// It uses the modifyCount to adjust the influence of modifications on the table.
func (idx *Index) GetRowCount(sctx sessionctx.Context, coll *HistColl, indexRanges []*ranger.Range, realtimeRowCount int64) (float64, error) {
	sc := sctx.GetSessionVars().StmtCtx
	totalCount := float64(0)
	isSingleCol := len(idx.Info.Columns) == 1
	for _, indexRange := range indexRanges {
		lb, err := codec.EncodeKey(sc, nil, indexRange.LowVal...)
		if err != nil {
			return 0, err
		}
		rb, err := codec.EncodeKey(sc, nil, indexRange.HighVal...)
		if err != nil {
			return 0, err
		}
		fullLen := len(indexRange.LowVal) == len(indexRange.HighVal) && len(indexRange.LowVal) == len(idx.Info.Columns)
		if bytes.Equal(lb, rb) {
			// case 1: it's a point
			if indexRange.LowExclude || indexRange.HighExclude {
				continue
			}
			if fullLen {
				// At most 1 in this case.
				if idx.Info.Unique {
					totalCount += 1
					continue
				}
				count := idx.equalRowCount(lb, realtimeRowCount)
				// If the current table row count has changed, we should scale the row count accordingly.
				count *= idx.GetIncreaseFactor(realtimeRowCount)
				totalCount += count
				continue
			}
		}

		// case 2: it's an interval
		// The final interval is [low, high)
		if indexRange.LowExclude {
			lb = kv.Key(lb).PrefixNext()
		}
		if !indexRange.HighExclude {
			rb = kv.Key(rb).PrefixNext()
		}
		l := types.NewBytesDatum(lb)
		r := types.NewBytesDatum(rb)
		lowIsNull := bytes.Equal(lb, nullKeyBytes)
		if isSingleCol && lowIsNull {
			totalCount += float64(idx.NullCount)
		}
		expBackoffSuccess := false
		// Due to the limitation of calcFraction and convertDatumToScalar, the histogram actually won't estimate anything.
		// If the first column's range is point.
		if rangePosition := GetOrdinalOfRangeCond(sc, indexRange); rangePosition > 0 && idx.StatsVer >= Version2 && coll != nil {
			var expBackoffSel float64
			expBackoffSel, expBackoffSuccess, err = idx.expBackoffEstimation(sctx, coll, indexRange)
			if err != nil {
				return 0, err
			}
			if expBackoffSuccess {
				expBackoffCnt := expBackoffSel * idx.TotalRowCount()

				upperLimit := expBackoffCnt
				// Use the multi-column stats to calculate the max possible row count of [l, r)
				if idx.Len() > 0 {
					_, lowerBkt, _, _ := idx.locateBucket(l)
					_, upperBkt, _, _ := idx.locateBucket(r)
					// Use Count of the Bucket before l as the lower bound.
					preCount := float64(0)
					if lowerBkt > 0 {
						preCount = float64(idx.Buckets[lowerBkt-1].Count)
					}
					// Use Count of the Bucket where r exists as the upper bound.
					upperCnt := float64(idx.Buckets[upperBkt].Count)
					upperLimit = upperCnt - preCount
					upperLimit += float64(idx.TopN.BetweenCount(lb, rb))
				}

				// If the result of exponential backoff strategy is larger than the result from multi-column stats,
				// 	use the upper limit from multi-column histogram instead.
				if expBackoffCnt > upperLimit {
					expBackoffCnt = upperLimit
				}
				totalCount += expBackoffCnt
			}
		}
		if !expBackoffSuccess {
			totalCount += idx.BetweenRowCount(l, r)
		}

		// If the current table row count has changed, we should scale the row count accordingly.
		totalCount *= idx.GetIncreaseFactor(realtimeRowCount)

		// handling the out-of-range part
		if (idx.outOfRange(l) && !(isSingleCol && lowIsNull)) || idx.outOfRange(r) {
			increaseCount := realtimeRowCount - int64(idx.TotalRowCount())
			if increaseCount < 0 {
				increaseCount = 0
			}
			totalCount += idx.Histogram.outOfRangeRowCount(&l, &r, increaseCount)
		}
	}
	totalCount = mathutil.Clamp(totalCount, 0, float64(realtimeRowCount))
	return totalCount, nil
}

// expBackoffEstimation estimate the multi-col cases following the Exponential Backoff. See comment below for details.
func (idx *Index) expBackoffEstimation(sctx sessionctx.Context, coll *HistColl, indexRange *ranger.Range) (float64, bool, error) {
	tmpRan := []*ranger.Range{
		{
			LowVal:    make([]types.Datum, 1),
			HighVal:   make([]types.Datum, 1),
			Collators: make([]collate.Collator, 1),
		},
	}
	colsIDs := coll.Idx2ColumnIDs[idx.ID]
	singleColumnEstResults := make([]float64, 0, len(indexRange.LowVal))
	// The following codes uses Exponential Backoff to reduce the impact of independent assumption. It works like:
	//   1. Calc the selectivity of each column.
	//   2. Sort them and choose the first 4 most selective filter and the corresponding selectivity is sel_1, sel_2, sel_3, sel_4 where i < j => sel_i < sel_j.
	//   3. The final selectivity would be sel_1 * sel_2^{1/2} * sel_3^{1/4} * sel_4^{1/8}.
	// This calculation reduced the independence assumption and can work well better than it.
	for i := 0; i < len(indexRange.LowVal); i++ {
		tmpRan[0].LowVal[0] = indexRange.LowVal[i]
		tmpRan[0].HighVal[0] = indexRange.HighVal[i]
		tmpRan[0].Collators[0] = indexRange.Collators[0]
		if i == len(indexRange.LowVal)-1 {
			tmpRan[0].LowExclude = indexRange.LowExclude
			tmpRan[0].HighExclude = indexRange.HighExclude
		}
		colID := colsIDs[i]
		var (
			count float64
			err   error
		)
		if anotherIdxID, ok := coll.ColID2IdxID[colID]; ok && anotherIdxID != idx.ID {
			count, err = coll.GetRowCountByIndexRanges(sctx, anotherIdxID, tmpRan)
		} else if col, ok := coll.Columns[colID]; ok && !col.IsInvalid(sctx, coll.Pseudo) {
			count, err = coll.GetRowCountByColumnRanges(sctx, colID, tmpRan)
		} else {
			continue
		}
		if err != nil {
			return 0, false, err
		}
		singleColumnEstResults = append(singleColumnEstResults, count)
	}
	// Sort them.
	sort.Slice(singleColumnEstResults, func(i, j int) bool {
		return singleColumnEstResults[i] < singleColumnEstResults[j]
	})
	l := len(singleColumnEstResults)
	// Convert the first 4 to selectivity results.
	for i := 0; i < l && i < 4; i++ {
		singleColumnEstResults[i] = singleColumnEstResults[i] / float64(coll.Count)
	}
	failpoint.Inject("cleanEstResults", func() {
		singleColumnEstResults = singleColumnEstResults[:0]
		l = 0
	})
	if l == 1 {
		return singleColumnEstResults[0], true, nil
	} else if l == 2 {
		return singleColumnEstResults[0] * math.Sqrt(singleColumnEstResults[1]), true, nil
	} else if l == 3 {
		return singleColumnEstResults[0] * math.Sqrt(singleColumnEstResults[1]) * math.Sqrt(math.Sqrt(singleColumnEstResults[2])), true, nil
	} else if l == 0 {
		return 0, false, nil
	}
	return singleColumnEstResults[0] * math.Sqrt(singleColumnEstResults[1]) * math.Sqrt(math.Sqrt(singleColumnEstResults[2])) * math.Sqrt(math.Sqrt(math.Sqrt(singleColumnEstResults[3]))), true, nil
}

type countByRangeFunc = func(sessionctx.Context, int64, []*ranger.Range) (float64, error)

// newHistogramBySelectivity fulfills the content of new histogram by the given selectivity result.
// TODO: Datum is not efficient, try to avoid using it here.
//  Also, there're redundant calculation with Selectivity(). We need to reduce it too.
func newHistogramBySelectivity(sctx sessionctx.Context, histID int64, oldHist, newHist *Histogram, ranges []*ranger.Range, cntByRangeFunc countByRangeFunc) error {
	cntPerVal := int64(oldHist.AvgCountPerNotNullValue(int64(oldHist.TotalRowCount())))
	var totCnt int64
	for boundIdx, ranIdx, highRangeIdx := 0, 0, 0; boundIdx < oldHist.Bounds.NumRows() && ranIdx < len(ranges); boundIdx, ranIdx = boundIdx+2, highRangeIdx {
		for highRangeIdx < len(ranges) && chunk.Compare(oldHist.Bounds.GetRow(boundIdx+1), 0, &ranges[highRangeIdx].HighVal[0]) >= 0 {
			highRangeIdx++
		}
		if boundIdx+2 >= oldHist.Bounds.NumRows() && highRangeIdx < len(ranges) && ranges[highRangeIdx].HighVal[0].Kind() == types.KindMaxValue {
			highRangeIdx++
		}
		if ranIdx == highRangeIdx {
			continue
		}
		cnt, err := cntByRangeFunc(sctx, histID, ranges[ranIdx:highRangeIdx])
		// This should not happen.
		if err != nil {
			return err
		}
		if cnt == 0 {
			continue
		}
		if int64(cnt) > oldHist.bucketCount(boundIdx/2) {
			cnt = float64(oldHist.bucketCount(boundIdx / 2))
		}
		newHist.Bounds.AppendRow(oldHist.Bounds.GetRow(boundIdx))
		newHist.Bounds.AppendRow(oldHist.Bounds.GetRow(boundIdx + 1))
		totCnt += int64(cnt)
		bkt := Bucket{Count: totCnt}
		if chunk.Compare(oldHist.Bounds.GetRow(boundIdx+1), 0, &ranges[highRangeIdx-1].HighVal[0]) == 0 && !ranges[highRangeIdx-1].HighExclude {
			bkt.Repeat = cntPerVal
		}
		newHist.Buckets = append(newHist.Buckets, bkt)
		switch newHist.Tp.EvalType() {
		case types.ETString, types.ETDecimal, types.ETDatetime, types.ETTimestamp:
			newHist.scalars = append(newHist.scalars, oldHist.scalars[boundIdx/2])
		}
	}
	return nil
}

func (idx *Index) newIndexBySelectivity(sc *stmtctx.StatementContext, statsNode *StatsNode) (*Index, error) {
	var (
		ranLowEncode, ranHighEncode []byte
		err                         error
	)
	newIndexHist := &Index{Info: idx.Info, StatsVer: idx.StatsVer, CMSketch: idx.CMSketch}
	newIndexHist.Histogram = *NewHistogram(idx.ID, int64(float64(idx.NDV)*statsNode.Selectivity), 0, 0, types.NewFieldType(mysql.TypeBlob), chunk.InitialCapacity, 0)

	lowBucketIdx, highBucketIdx := 0, 0
	var totCnt int64

	// Bucket bound of index is encoded one, so we need to decode it if we want to calculate the fraction accurately.
	// TODO: enhance its calculation.
	// Now just remove the bucket that no range fell in.
	for _, ran := range statsNode.Ranges {
		lowBucketIdx = highBucketIdx
		ranLowEncode, ranHighEncode, err = ran.Encode(sc, ranLowEncode, ranHighEncode)
		if err != nil {
			return nil, err
		}
		for ; highBucketIdx < idx.Len(); highBucketIdx++ {
			// Encoded value can only go to its next quickly. So ranHighEncode is actually range.HighVal's PrefixNext value.
			// So the Bound should also go to its PrefixNext.
			bucketLowerEncoded := idx.Bounds.GetRow(highBucketIdx * 2).GetBytes(0)
			if bytes.Compare(ranHighEncode, kv.Key(bucketLowerEncoded).PrefixNext()) < 0 {
				break
			}
		}
		for ; lowBucketIdx < highBucketIdx; lowBucketIdx++ {
			bucketUpperEncoded := idx.Bounds.GetRow(lowBucketIdx*2 + 1).GetBytes(0)
			if bytes.Compare(ranLowEncode, bucketUpperEncoded) <= 0 {
				break
			}
		}
		if lowBucketIdx >= idx.Len() {
			break
		}
		for i := lowBucketIdx; i < highBucketIdx; i++ {
			newIndexHist.Bounds.AppendRow(idx.Bounds.GetRow(i * 2))
			newIndexHist.Bounds.AppendRow(idx.Bounds.GetRow(i*2 + 1))
			totCnt += idx.bucketCount(i)
			newIndexHist.Buckets = append(newIndexHist.Buckets, Bucket{Repeat: idx.Buckets[i].Repeat, Count: totCnt})
			newIndexHist.scalars = append(newIndexHist.scalars, idx.scalars[i])
		}
	}
	return newIndexHist, nil
}

// NewHistCollBySelectivity creates new HistColl by the given statsNodes.
func (coll *HistColl) NewHistCollBySelectivity(sctx sessionctx.Context, statsNodes []*StatsNode) *HistColl {
	newColl := &HistColl{
		Columns:       make(map[int64]*Column),
		Indices:       make(map[int64]*Index),
		Idx2ColumnIDs: coll.Idx2ColumnIDs,
		ColID2IdxID:   coll.ColID2IdxID,
		Count:         coll.Count,
	}
	for _, node := range statsNodes {
		if node.Tp == IndexType {
			idxHist, ok := coll.Indices[node.ID]
			if !ok {
				continue
			}
			newIdxHist, err := idxHist.newIndexBySelectivity(sctx.GetSessionVars().StmtCtx, node)
			if err != nil {
				logutil.BgLogger().Warn("[Histogram-in-plan]: something wrong happened when calculating row count, "+
					"failed to build histogram for index %v of table %v",
					zap.String("index", idxHist.Info.Name.O), zap.String("table", idxHist.Info.Table.O), zap.Error(err))
				continue
			}
			newColl.Indices[node.ID] = newIdxHist
			continue
		}
		oldCol, ok := coll.Columns[node.ID]
		if !ok {
			continue
		}
		newCol := &Column{
			PhysicalID: oldCol.PhysicalID,
			Info:       oldCol.Info,
			IsHandle:   oldCol.IsHandle,
			CMSketch:   oldCol.CMSketch,
		}
		newCol.Histogram = *NewHistogram(oldCol.ID, int64(float64(oldCol.Histogram.NDV)*node.Selectivity), 0, 0, oldCol.Tp, chunk.InitialCapacity, 0)
		var err error
		splitRanges, ok := oldCol.Histogram.SplitRange(sctx.GetSessionVars().StmtCtx, node.Ranges, false)
		if !ok {
			logutil.BgLogger().Warn("[Histogram-in-plan]: the type of histogram and ranges mismatch")
			continue
		}
		// Deal with some corner case.
		if len(splitRanges) > 0 {
			// Deal with NULL values.
			if splitRanges[0].LowVal[0].IsNull() {
				newCol.NullCount = oldCol.NullCount
				if splitRanges[0].HighVal[0].IsNull() {
					splitRanges = splitRanges[1:]
				} else {
					splitRanges[0].LowVal[0].SetMinNotNull()
				}
			}
		}
		if oldCol.IsHandle {
			err = newHistogramBySelectivity(sctx, node.ID, &oldCol.Histogram, &newCol.Histogram, splitRanges, coll.GetRowCountByIntColumnRanges)
		} else {
			err = newHistogramBySelectivity(sctx, node.ID, &oldCol.Histogram, &newCol.Histogram, splitRanges, coll.GetRowCountByColumnRanges)
		}
		if err != nil {
			logutil.BgLogger().Warn("[Histogram-in-plan]: something wrong happened when calculating row count",
				zap.Error(err))
			continue
		}
		newCol.Loaded = oldCol.Loaded
		newColl.Columns[node.ID] = newCol
	}
	for id, idx := range coll.Indices {
		_, ok := newColl.Indices[id]
		if !ok {
			newColl.Indices[id] = idx
		}
	}
	for id, col := range coll.Columns {
		_, ok := newColl.Columns[id]
		if !ok {
			newColl.Columns[id] = col
		}
	}
	return newColl
}

func (idx *Index) outOfRange(val types.Datum) bool {
	if !idx.Histogram.outOfRange(val) {
		return false
	}
	if idx.Histogram.Len() > 0 && matchPrefix(idx.Bounds.GetRow(0), 0, &val) {
		return false
	}
	return true
}

// matchPrefix checks whether ad is the prefix of value
func matchPrefix(row chunk.Row, colIdx int, ad *types.Datum) bool {
	switch ad.Kind() {
	case types.KindString, types.KindBytes, types.KindBinaryLiteral, types.KindMysqlBit:
		return strings.HasPrefix(row.GetString(colIdx), ad.GetString())
	}
	return false
}

type dataCnt struct {
	data []byte
	cnt  uint64
}

// GetIndexPrefixLens returns an array representing
func GetIndexPrefixLens(data []byte, numCols int) (prefixLens []int, err error) {
	prefixLens = make([]int, 0, numCols)
	var colData []byte
	prefixLen := 0
	for len(data) > 0 {
		colData, data, err = codec.CutOne(data)
		if err != nil {
			return nil, err
		}
		prefixLen += len(colData)
		prefixLens = append(prefixLens, prefixLen)
	}
	return prefixLens, nil
}

// ExtractTopN extracts topn from histogram.
func (hg *Histogram) ExtractTopN(cms *CMSketch, topN *TopN, numCols int, numTopN uint32) error {
	if hg.Len() == 0 || cms == nil || numTopN == 0 {
		return nil
	}
	dataSet := make(map[string]struct{}, hg.Bounds.NumRows())
	dataCnts := make([]dataCnt, 0, hg.Bounds.NumRows())
	hg.PreCalculateScalar()
	// Set a limit on the frequency of boundary values to avoid extract values with low frequency.
	limit := hg.notNullCount() / float64(hg.Len())
	// Since our histogram are equal depth, they must occurs on the boundaries of buckets.
	for i := 0; i < hg.Bounds.NumRows(); i++ {
		data := hg.Bounds.GetRow(i).GetBytes(0)
		prefixLens, err := GetIndexPrefixLens(data, numCols)
		if err != nil {
			return err
		}
		for _, prefixLen := range prefixLens {
			prefixColData := data[:prefixLen]
			_, ok := dataSet[string(prefixColData)]
			if ok {
				continue
			}
			dataSet[string(prefixColData)] = struct{}{}
			res := hg.BetweenRowCount(types.NewBytesDatum(prefixColData), types.NewBytesDatum(kv.Key(prefixColData).PrefixNext()))
			if res >= limit {
				dataCnts = append(dataCnts, dataCnt{prefixColData, uint64(res)})
			}
		}
	}
	sort.SliceStable(dataCnts, func(i, j int) bool { return dataCnts[i].cnt >= dataCnts[j].cnt })
	if len(dataCnts) > int(numTopN) {
		dataCnts = dataCnts[:numTopN]
	}
	topN.TopN = make([]TopNMeta, 0, len(dataCnts))
	for _, dataCnt := range dataCnts {
		h1, h2 := murmur3.Sum128(dataCnt.data)
		realCnt := cms.queryHashValue(h1, h2)
		cms.SubValue(h1, h2, realCnt)
		topN.AppendTopN(dataCnt.data, realCnt)
	}
	topN.Sort()
	return nil
}

// bucket4Merging is only used for merging partition hists to global hist.
type bucket4Merging struct {
	lower *types.Datum
	upper *types.Datum
	Bucket
	// disjointNDV is used for merging bucket NDV, see mergeBucketNDV for more details.
	disjointNDV int64
}

func newBucket4Meging() *bucket4Merging {
	return &bucket4Merging{
		lower: new(types.Datum),
		upper: new(types.Datum),
		Bucket: Bucket{
			Repeat: 0,
			NDV:    0,
			Count:  0,
		},
		disjointNDV: 0,
	}
}

// buildBucket4Merging builds bucket4Merging from Histogram
// Notice: Count in Histogram.Buckets is prefix sum but in bucket4Merging is not.
func (hg *Histogram) buildBucket4Merging() []*bucket4Merging {
	buckets := make([]*bucket4Merging, 0, hg.Len())
	for i := 0; i < hg.Len(); i++ {
		b := newBucket4Meging()
		hg.GetLower(i).Copy(b.lower)
		hg.GetUpper(i).Copy(b.upper)
		b.Repeat = hg.Buckets[i].Repeat
		b.NDV = hg.Buckets[i].NDV
		b.Count = hg.Buckets[i].Count
		if i != 0 {
			b.Count -= hg.Buckets[i-1].Count
		}
		buckets = append(buckets, b)
	}
	return buckets
}

func (b *bucket4Merging) Clone() bucket4Merging {
	return bucket4Merging{
		lower: b.lower.Clone(),
		upper: b.upper.Clone(),
		Bucket: Bucket{
			Repeat: b.Repeat,
			NDV:    b.NDV,
			Count:  b.Count,
		},
		disjointNDV: b.disjointNDV,
	}
}

// mergeBucketNDV merges bucket NDV from tow bucket `right` & `left`.
// Before merging, you need to make sure that when using (upper, lower) as the comparison key, `right` is greater than `left`
func mergeBucketNDV(sc *stmtctx.StatementContext, left *bucket4Merging, right *bucket4Merging) (*bucket4Merging, error) {
	res := right.Clone()
	if left.NDV == 0 {
		return &res, nil
	}
	if right.NDV == 0 {
		res.lower = left.lower.Clone()
		res.upper = left.upper.Clone()
		res.NDV = left.NDV
		return &res, nil
	}
	upperCompare, err := right.upper.Compare(sc, left.upper, collate.GetBinaryCollator())
	if err != nil {
		return nil, err
	}
	// __right__|
	// _______left____|
	// illegal order.
	if upperCompare < 0 {
		return nil, errors.Errorf("illegal bucket order")
	}
	//  ___right_|
	//  ___left__|
	// They have the same upper.
	if upperCompare == 0 {
		lowerCompare, err := right.lower.Compare(sc, left.lower, collate.GetBinaryCollator())
		if err != nil {
			return nil, err
		}
		//      |____right____|
		//         |__left____|
		// illegal order.
		if lowerCompare < 0 {
			return nil, errors.Errorf("illegal bucket order")
		}
		// |___right___|
		// |____left___|
		// ndv = max(right.ndv, left.ndv)
		if lowerCompare == 0 {
			if left.NDV > right.NDV {
				res.NDV = left.NDV
			}
			return &res, nil
		}
		//         |_right_|
		// |_____left______|
		// |-ratio-|
		// ndv = ratio * left.ndv + max((1-ratio) * left.ndv, right.ndv)
		ratio := calcFraction4Datums(left.lower, left.upper, right.lower)
		res.NDV = int64(ratio*float64(left.NDV) + math.Max((1-ratio)*float64(left.NDV), float64(right.NDV)))
		res.lower = left.lower.Clone()
		return &res, nil
	}
	// ____right___|
	// ____left__|
	// right.upper > left.upper
	lowerCompareUpper, err := right.lower.Compare(sc, left.upper, collate.GetBinaryCollator())
	if err != nil {
		return nil, err
	}
	//                  |_right_|
	//  |___left____|
	// `left` and `right` do not intersect
	// We add right.ndv in `disjointNDV`, and let `right.ndv = left.ndv` be used for subsequent merge.
	// This is because, for the merging of many buckets, we merge them from back to front.
	if lowerCompareUpper >= 0 {
		res.upper = left.upper.Clone()
		res.lower = left.lower.Clone()
		res.disjointNDV += right.NDV
		res.NDV = left.NDV
		return &res, nil
	}
	upperRatio := calcFraction4Datums(right.lower, right.upper, left.upper)
	lowerCompare, err := right.lower.Compare(sc, left.lower, collate.GetBinaryCollator())
	if err != nil {
		return nil, err
	}
	//              |-upperRatio-|
	//              |_______right_____|
	// |_______left______________|
	// |-lowerRatio-|
	// ndv = lowerRatio * left.ndv
	//		+ max((1-lowerRatio) * left.ndv, upperRatio * right.ndv)
	//		+ (1-upperRatio) * right.ndv
	if lowerCompare >= 0 {
		lowerRatio := calcFraction4Datums(left.lower, left.upper, right.lower)
		res.NDV = int64(lowerRatio*float64(left.NDV) +
			math.Max((1-lowerRatio)*float64(left.NDV), upperRatio*float64(right.NDV)) +
			(1-upperRatio)*float64(right.NDV))
		res.lower = left.lower.Clone()
		return &res, nil
	}
	// |------upperRatio--------|
	// |-lowerRatio-|
	// |____________right______________|
	//              |___left____|
	// ndv = lowerRatio * right.ndv
	//		+ max(left.ndv + (upperRatio - lowerRatio) * right.ndv)
	//		+ (1-upperRatio) * right.ndv
	lowerRatio := calcFraction4Datums(right.lower, right.upper, left.lower)
	res.NDV = int64(lowerRatio*float64(right.NDV) +
		math.Max(float64(left.NDV), (upperRatio-lowerRatio)*float64(right.NDV)) +
		(1-upperRatio)*float64(right.NDV))
	return &res, nil
}

// mergeParitionBuckets merges buckets[l...r) to one global bucket.
// global bucket:
//		upper = buckets[r-1].upper
//		count = sum of buckets[l...r).count
//		repeat = sum of buckets[i] (buckets[i].upper == global bucket.upper && i in [l...r))
//		ndv = merge bucket ndv from r-1 to l by mergeBucketNDV
// Notice: lower is not calculated here.
func mergePartitionBuckets(sc *stmtctx.StatementContext, buckets []*bucket4Merging) (*bucket4Merging, error) {
	if len(buckets) == 0 {
		return nil, errors.Errorf("not enough buckets to merge")
	}
	res := bucket4Merging{}
	res.upper = buckets[len(buckets)-1].upper.Clone()
	right := buckets[len(buckets)-1].Clone()

	totNDV := int64(0)
	for i := len(buckets) - 1; i >= 0; i-- {
		totNDV += buckets[i].NDV
		res.Count += buckets[i].Count
		compare, err := buckets[i].upper.Compare(sc, res.upper, collate.GetBinaryCollator())
		if err != nil {
			return nil, err
		}
		if compare == 0 {
			res.Repeat += buckets[i].Repeat
		}
		if i != len(buckets)-1 {
			tmp, err := mergeBucketNDV(sc, buckets[i], &right)
			if err != nil {
				return nil, err
			}
			right = *tmp
		}
	}
	res.NDV = right.NDV + right.disjointNDV

	// since `mergeBucketNDV` is based on uniform and inclusion assumptions, it has the trend to under-estimate,
	// and as the number of buckets increases, these assumptions become weak,
	// so to mitigate this problem, a damping factor based on the number of buckets is introduced.
	res.NDV = int64(float64(res.NDV) * math.Pow(1.15, float64(len(buckets)-1)))
	if res.NDV > totNDV {
		res.NDV = totNDV
	}
	return &res, nil
}

func (t *TopNMeta) buildBucket4Merging(d *types.Datum) *bucket4Merging {
	res := newBucket4Meging()
	res.lower = d.Clone()
	res.upper = d.Clone()
	res.Count = int64(t.Count)
	res.Repeat = int64(t.Count)
	res.NDV = int64(1)
	return res
}

// MergePartitionHist2GlobalHist merges hists (partition-level Histogram) to a global-level Histogram
func MergePartitionHist2GlobalHist(sc *stmtctx.StatementContext, hists []*Histogram, popedTopN []TopNMeta, expBucketNumber int64, isIndex bool) (*Histogram, error) {
	var totCount, totNull, bucketNumber, totColSize int64
	if expBucketNumber == 0 {
		return nil, errors.Errorf("expBucketNumber can not be zero")
	}
	// minValue is used to calc the bucket lower.
	var minValue *types.Datum
	for _, hist := range hists {
		totColSize += hist.TotColSize
		totNull += hist.NullCount
		bucketNumber += int64(hist.Len())
		if hist.Len() > 0 {
			totCount += hist.Buckets[hist.Len()-1].Count
			if minValue == nil {
				minValue = hist.GetLower(0).Clone()
				continue
			}
			res, err := hist.GetLower(0).Compare(sc, minValue, collate.GetBinaryCollator())
			if err != nil {
				return nil, err
			}
			if res < 0 {
				minValue = hist.GetLower(0).Clone()
			}
		}
	}

	bucketNumber += int64(len(popedTopN))
	buckets := make([]*bucket4Merging, 0, bucketNumber)
	globalBuckets := make([]*bucket4Merging, 0, expBucketNumber)

	// init `buckets`.
	for _, hist := range hists {
		buckets = append(buckets, hist.buildBucket4Merging()...)
	}

	for _, meta := range popedTopN {
		totCount += int64(meta.Count)
		var d types.Datum
		if isIndex {
			d.SetBytes(meta.Encoded)
		} else {
			var err error
			if types.IsTypeTime(hists[0].Tp.GetType()) {
				// handle datetime values specially since they are encoded to int and we'll get int values if using DecodeOne.
				_, d, err = codec.DecodeAsDateTime(meta.Encoded, hists[0].Tp.GetType(), sc.TimeZone)
			} else {
				_, d, err = codec.DecodeOne(meta.Encoded)
			}
			if err != nil {
				return nil, err
			}
		}
		if minValue == nil {
			minValue = d.Clone()
			continue
		}
		res, err := d.Compare(sc, minValue, collate.GetBinaryCollator())
		if err != nil {
			return nil, err
		}
		if res < 0 {
			minValue = d.Clone()
		}
		buckets = append(buckets, meta.buildBucket4Merging(&d))
	}

	// Remove empty buckets
	tail := 0
	for i := range buckets {
		if buckets[i].Count != 0 {
			buckets[tail] = buckets[i]
			tail++
		}
	}
	buckets = buckets[:tail]

	var sortError error
	sort.Slice(buckets, func(i, j int) bool {
		res, err := buckets[i].upper.Compare(sc, buckets[j].upper, collate.GetBinaryCollator())
		if err != nil {
			sortError = err
		}
		if res != 0 {
			return res < 0
		}
		res, err = buckets[i].lower.Compare(sc, buckets[j].lower, collate.GetBinaryCollator())
		if err != nil {
			sortError = err
		}
		return res < 0
	})
	if sortError != nil {
		return nil, sortError
	}

	var sum, prevSum int64
	r, prevR := len(buckets), 0
	bucketCount := int64(1)
	gBucketCountThreshold := (totCount / expBucketNumber) * 80 / 100 // expectedBucketSize * 0.8
	var bucketNDV int64
	for i := len(buckets) - 1; i >= 0; i-- {
		sum += buckets[i].Count
		bucketNDV += buckets[i].NDV
		if sum >= totCount*bucketCount/expBucketNumber && sum-prevSum >= gBucketCountThreshold {
			for ; i > 0; i-- { // if the buckets have the same upper, we merge them into the same new buckets.
				res, err := buckets[i-1].upper.Compare(sc, buckets[i].upper, collate.GetBinaryCollator())
				if err != nil {
					return nil, err
				}
				if res != 0 {
					break
				}
				sum += buckets[i-1].Count
				bucketNDV += buckets[i-1].NDV
			}
			merged, err := mergePartitionBuckets(sc, buckets[i:r])
			if err != nil {
				return nil, err
			}
			globalBuckets = append(globalBuckets, merged)
			prevR = r
			r = i
			bucketCount++
			prevSum = sum
			bucketNDV = 0
		}
	}
	if r > 0 {
		bucketSum := int64(0)
		for _, b := range buckets[:r] {
			bucketSum += b.Count
		}

		if len(globalBuckets) > 0 && bucketSum < gBucketCountThreshold { // merge them into the previous global bucket
			r = prevR
			globalBuckets = globalBuckets[:len(globalBuckets)-1]
		}

		merged, err := mergePartitionBuckets(sc, buckets[:r])
		if err != nil {
			return nil, err
		}
		globalBuckets = append(globalBuckets, merged)
	}
	// Because we merge backwards, we need to flip the slices.
	for i, j := 0, len(globalBuckets)-1; i < j; i, j = i+1, j-1 {
		globalBuckets[i], globalBuckets[j] = globalBuckets[j], globalBuckets[i]
	}

	// Calc the bucket lower.
	if minValue == nil || len(globalBuckets) == 0 { // both hists and popedTopN are empty, returns an empty hist in this case
		return NewHistogram(hists[0].ID, 0, totNull, hists[0].LastUpdateVersion, hists[0].Tp, len(globalBuckets), totColSize), nil
	}
	globalBuckets[0].lower = minValue.Clone()
	for i := 1; i < len(globalBuckets); i++ {
		if globalBuckets[i].NDV == 1 { // there is only 1 value so lower = upper
			globalBuckets[i].lower = globalBuckets[i].upper.Clone()
		} else {
			globalBuckets[i].lower = globalBuckets[i-1].upper.Clone()
		}
		globalBuckets[i].Count = globalBuckets[i].Count + globalBuckets[i-1].Count
	}

	// Recalculate repeats
	// TODO: optimize it later since it's a simple but not the fastest implementation whose complexity is O(nBkt * nHist * log(nBkt))
	for _, bucket := range globalBuckets {
		var repeat float64
		for _, hist := range hists {
			histRowCount, _ := hist.equalRowCount(*bucket.upper, isIndex)
			repeat += histRowCount // only hists of indexes have bucket.NDV
		}
		if int64(repeat) > bucket.Repeat {
			bucket.Repeat = int64(repeat)
		}
	}

	globalHist := NewHistogram(hists[0].ID, 0, totNull, hists[0].LastUpdateVersion, hists[0].Tp, len(globalBuckets), totColSize)
	for _, bucket := range globalBuckets {
		if !isIndex {
			bucket.NDV = 0 // bucket.NDV is not maintained for column histograms
		}
		globalHist.AppendBucketWithNDV(bucket.lower, bucket.upper, bucket.Count, bucket.Repeat, bucket.NDV)
	}
	return globalHist, nil
}
