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
	"cmp"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/planner/util/debugtrace"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/twmb/murmur3"
	"go.uber.org/zap"
)

// Histogram represents statistics for a column or index.
type Histogram struct {
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
	Scalars   []scalar
	ID        int64 // Column ID.
	NDV       int64 // Number of distinct values. Note that It contains the NDV of the TopN which is excluded from histogram.
	NullCount int64 // Number of null values.
	// LastUpdateVersion is the version that this histogram updated last time.
	LastUpdateVersion uint64

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
	// Count is the number of items till this bucket.
	Count int64
	// Repeat is the number of times the upper-bound value of the bucket appears in the data.
	// For example, in the range [x, y], Repeat indicates how many times y appears.
	// It is used to estimate the row count of values equal to the upper bound of the bucket, similar to TopN.
	Repeat int64
	// NDV is the number of distinct values in the bucket.
	NDV int64
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
		Bounds:            chunk.NewChunkFromPoolWithCapacity([]*types.FieldType{tp}, 2*bucketSize),
		Buckets:           make([]Bucket, 0, bucketSize),
		TotColSize:        totColSize,
	}
}

// GetLower gets the lower bound of bucket `idx`.
func (hg *Histogram) GetLower(idx int) *types.Datum {
	d := hg.Bounds.GetRow(2*idx).GetDatum(0, hg.Tp)
	return &d
}

// LowerToDatum gets the lower bound of bucket `idx` to datum.
func (hg *Histogram) LowerToDatum(idx int, d *types.Datum) {
	hg.Bounds.GetRow(2*idx).DatumWithBuffer(0, hg.Tp, d)
}

// GetUpper gets the upper bound of bucket `idx`.
func (hg *Histogram) GetUpper(idx int) *types.Datum {
	d := hg.Bounds.GetRow(2*idx+1).GetDatum(0, hg.Tp)
	return &d
}

// UpperToDatum gets the upper bound of bucket `idx` to datum.
func (hg *Histogram) UpperToDatum(idx int, d *types.Datum) {
	hg.Bounds.GetRow(2*idx+1).DatumWithBuffer(0, hg.Tp, d)
}

// MemoryUsage returns the total memory usage of this Histogram.
func (hg *Histogram) MemoryUsage() (sum int64) {
	if hg == nil {
		return
	}
	if len(hg.Buckets) == 0 && len(hg.Scalars) == 0 && hg.Bounds.Capacity() == 0 {
		return
	}
	sum = EmptyHistogramSize + hg.Bounds.MemoryUsage() + int64(cap(hg.Buckets))*EmptyBucketSize + int64(cap(hg.Scalars))*EmptyScalarSize
	return sum
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
	bucket := &hg.Buckets[l-1]
	if needBucketNDV && bucket.NDV > 0 {
		bucket.NDV++
	}
	bucket.Count = count
	bucket.Repeat = repeat
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
func (hg *Histogram) ConvertTo(tctx types.Context, tp *types.FieldType) (*Histogram, error) {
	hist := NewHistogram(hg.ID, hg.NDV, hg.NullCount, hg.LastUpdateVersion, tp, hg.Len(), hg.TotColSize)
	hist.Correlation = hg.Correlation
	iter := chunk.NewIterator4Chunk(hg.Bounds)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		d := row.GetDatum(0, hg.Tp)
		d, err := d.ConvertTo(tctx, tp)
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

// DestroyAndPutToPool resets the FMSketch and puts it to the pool.
func (hg *Histogram) DestroyAndPutToPool() {
	if hg == nil {
		return
	}
	hg.Bounds.Destroy(len(hg.Buckets), []*types.FieldType{hg.Tp})
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
	// Column stats: CM Sketch is not used. TopN and Histogram are built from samples. TopN + Histogram represent all data.(The values covered by TopN is removed from Histogram.)
	// Index stats: CM SKetch is not used. TopN and Histograms are built from samples. TopN + Histogram represent all data.(The values covered by TopN is removed from Histogram.)
	// Both Column and Index's NDVs are collected by full scan.
	Version2 = 2
)

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
	return fmt.Sprintf("num: %d lower_bound: %s upper_bound: %s repeats: %d ndv: %d", hg.BucketCount(bktID), lowerVal, upperVal, hg.Buckets[bktID].Repeat, hg.Buckets[bktID].NDV)
}

// BinarySearchRemoveVal removes the value from the TopN using binary search.
func (hg *Histogram) BinarySearchRemoveVal(valCntPairs TopNMeta) {
	lowIdx, highIdx := 0, hg.Len()-1
	column := hg.Bounds.Column(0)
	// if hg is too small, we don't need to check the branch. because the cost is more than binary search.
	if hg.Len() > 4 {
		if cmpResult := bytes.Compare(column.GetRaw(highIdx*2+1), valCntPairs.Encoded); cmpResult < 0 {
			return
		}
		if cmpResult := bytes.Compare(column.GetRaw(lowIdx), valCntPairs.Encoded); cmpResult > 0 {
			return
		}
	}
	var midIdx = 0
	var found bool
	for lowIdx <= highIdx {
		midIdx = (lowIdx + highIdx) / 2
		cmpResult := bytes.Compare(column.GetRaw(midIdx*2), valCntPairs.Encoded)
		if cmpResult > 0 {
			highIdx = midIdx - 1
			continue
		}
		cmpResult = bytes.Compare(column.GetRaw(midIdx*2+1), valCntPairs.Encoded)
		if cmpResult < 0 {
			lowIdx = midIdx + 1
			continue
		}
		midbucket := &hg.Buckets[midIdx]

		if midbucket.NDV > 0 {
			midbucket.NDV--
		}
		if cmpResult == 0 {
			midbucket.Repeat = 0
		}
		midbucket.Count -= int64(valCntPairs.Count)
		if midbucket.Count < 0 {
			midbucket.Count = 0
		}
		found = true
		break
	}
	if found {
		for midIdx++; midIdx <= hg.Len()-1; midIdx++ {
			hg.Buckets[midIdx].Count -= int64(valCntPairs.Count)
			if hg.Buckets[midIdx].Count < 0 {
				hg.Buckets[midIdx].Count = 0
			}
		}
	}
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

// StandardizeForV2AnalyzeIndex fixes some "irregular" places in the Histogram, which come from current implementation of
// analyze index task in v2.
// For now, it does two things: 1. Remove empty buckets. 2. Reset Bucket.NDV to 0.
func (hg *Histogram) StandardizeForV2AnalyzeIndex() {
	if hg == nil || len(hg.Buckets) == 0 {
		return
	}
	// Note that hg.Buckets is []Bucket instead of []*Bucket, so we avoid extra memory allocation for the struct Bucket
	// in the process below.

	// remainedBktIdxs are the positions of the eventually remained buckets in the original hg.Buckets slice.
	remainedBktIdxs := make([]int, 0, len(hg.Buckets))
	// We use two pointers here.
	// checkingIdx is the "fast" one, and it iterates the hg.Buckets and check if they are empty one by one.
	// When we find a non-empty bucket, we move it to the position where nextRemainedBktIdx, which is the "slow"
	// pointer, points to.
	nextRemainedBktIdx := 0
	for checkingIdx := range hg.Buckets {
		if hg.BucketCount(checkingIdx) <= 0 && hg.Buckets[checkingIdx].Repeat <= 0 {
			continue
		}
		remainedBktIdxs = append(remainedBktIdxs, checkingIdx)
		if nextRemainedBktIdx != checkingIdx {
			hg.Buckets[nextRemainedBktIdx] = hg.Buckets[checkingIdx]
		}
		hg.Buckets[nextRemainedBktIdx].NDV = 0
		nextRemainedBktIdx++
	}
	hg.Buckets = hg.Buckets[:nextRemainedBktIdx]

	// Get the new Bounds from the original Bounds according to the indexes we collect.
	c := chunk.NewChunkWithCapacity([]*types.FieldType{hg.Tp}, len(remainedBktIdxs))
	for _, i := range remainedBktIdxs {
		c.AppendDatum(0, hg.GetLower(i))
		c.AppendDatum(0, hg.GetUpper(i))
	}
	hg.Bounds = c
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

// EqualRowCount estimates the row count where the column equals to value.
// matched: return true if this returned row count is from Bucket.Repeat or bucket NDV, which is more accurate than if not.
// The input sctx is just for debug trace, you can pass nil safely if that's not needed.
func (hg *Histogram) EqualRowCount(sctx planctx.PlanContext, value types.Datum, hasBucketNDV bool) (count float64, matched bool) {
	if sctx != nil && sctx.GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(sctx)
		defer func() {
			debugtrace.RecordAnyValuesWithNames(sctx, "Count", count, "Matched", matched)
			debugtrace.LeaveContextCommon(sctx)
		}()
	}
	_, bucketIdx, inBucket, match := hg.LocateBucket(sctx, value)
	if !inBucket {
		return 0, false
	}
	if sctx != nil && sctx.GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		DebugTraceBuckets(sctx, hg, []int{bucketIdx})
	}
	if match {
		return float64(hg.Buckets[bucketIdx].Repeat), true
	}
	if hasBucketNDV && hg.Buckets[bucketIdx].NDV > 1 {
		return float64(hg.BucketCount(bucketIdx)-hg.Buckets[bucketIdx].Repeat) / float64(hg.Buckets[bucketIdx].NDV-1), true
	}
	return hg.NotNullCount() / float64(hg.NDV), false
}

// GreaterRowCount estimates the row count where the column greater than value.
// It's deprecated. Only used for test.
func (hg *Histogram) GreaterRowCount(value types.Datum) float64 {
	histRowCount, _ := hg.EqualRowCount(nil, value, false)
	gtCount := hg.NotNullCount() - hg.LessRowCount(nil, value) - histRowCount
	return math.Max(0, gtCount)
}

// LocateBucket locates where a value falls in the range of the Histogram.
// The input sctx is just for debug trace, you can pass nil safely if that's not needed.
//
// Return value:
// exceed: if the value is larger than the upper bound of the last Bucket of the Histogram.
// bucketIdx: assuming exceed if false, which Bucket does this value fall in (note: the range before a Bucket is also
// considered belong to this Bucket).
// inBucket: assuming exceed if false, whether this value falls in this Bucket, instead of falls between
// this Bucket and the previous Bucket.
// matchLastValue: assuming inBucket is true, if this value is the last value in this Bucket, which has a counter (Bucket.Repeat).
//
// Examples:
// val0 |<-[bkt0]->| |<-[bkt1]->val1(last value)| val2 |<--val3--[bkt2]->| |<-[bkt3]->| val4
// locateBucket(val0): false, 0, false, false
// locateBucket(val1): false, 1, true, true
// locateBucket(val2): false, 2, false, false
// locateBucket(val3): false, 2, true, false
// locateBucket(val4): true, 3, false, false
func (hg *Histogram) LocateBucket(sctx planctx.PlanContext, value types.Datum) (exceed bool, bucketIdx int, inBucket, matchLastValue bool) {
	if sctx != nil && sctx.GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		defer func() {
			debugTraceLocateBucket(sctx, &value, exceed, bucketIdx, inBucket, matchLastValue)
		}()
	}
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
// The input sctx is just for debug trace, you can pass nil safely if that's not needed.
func (hg *Histogram) LessRowCountWithBktIdx(sctx planctx.PlanContext, value types.Datum) (result float64, bucketIdx int) {
	if sctx != nil && sctx.GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(sctx)
		defer func() {
			debugtrace.RecordAnyValuesWithNames(sctx, "Result", result, "Bucket idx", bucketIdx)
			debugtrace.LeaveContextCommon(sctx)
		}()
	}
	// All the values are null.
	if hg.Bounds.NumRows() == 0 {
		return 0, 0
	}
	exceed, bucketIdx, inBucket, match := hg.LocateBucket(sctx, value)
	if exceed {
		return hg.NotNullCount(), hg.Len() - 1
	}
	if sctx != nil && sctx.GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		DebugTraceBuckets(sctx, hg, []int{bucketIdx - 1, bucketIdx})
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

// LessRowCount estimates the row count where the column less than value.
// The input sctx is just for debug trace, you can pass nil safely if that's not needed.
func (hg *Histogram) LessRowCount(sctx planctx.PlanContext, value types.Datum) float64 {
	result, _ := hg.LessRowCountWithBktIdx(sctx, value)
	return result
}

// BetweenRowCount estimates the row count where column greater or equal to a and less than b.
// The input sctx is just for debug trace, you can pass nil safely if that's not needed.
func (hg *Histogram) BetweenRowCount(sctx planctx.PlanContext, a, b types.Datum) float64 {
	lessCountA := hg.LessRowCount(sctx, a)
	lessCountB := hg.LessRowCount(sctx, b)
	rangeEst := lessCountB - lessCountA
	lowEqual, _ := hg.EqualRowCount(sctx, a, false)
	ndvAvg := hg.NotNullCount() / float64(hg.NDV)
	// If values fall in the same bucket, we may underestimate the fractional result. So estimate the low value (a) as an equals, and
	// estimate the high value as the default (because the input high value may be "larger" than the true high value). The range should
	// not be less than both the low+high - or the lesser of the estimate for the individual range of a or b is used as a bound.
	if rangeEst < math.Max(lowEqual, ndvAvg) && hg.NDV > 0 {
		result := math.Min(lessCountB, hg.NotNullCount()-lessCountA)
		return math.Min(result, lowEqual+ndvAvg)
	}
	return rangeEst
}

// TotalRowCount returns the total count of this histogram.
func (hg *Histogram) TotalRowCount() float64 {
	return hg.NotNullCount() + float64(hg.NullCount)
}

// NotNullCount indicates the count of non-null values in column histogram and single-column index histogram,
// for multi-column index histogram, since we cannot define null for the row, we treat all rows as non-null, that means,
// notNullCount would return same value as TotalRowCount for multi-column index histograms.
func (hg *Histogram) NotNullCount() float64 {
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
		low, err = codec.EncodeKey(sc.TimeZone(), nil, ran.LowVal[0])
		err = sc.HandleError(err)
		if err != nil {
			return false
		}
		high, err = codec.EncodeKey(sc.TimeZone(), nil, ran.HighVal[0])
		err = sc.HandleError(err)
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

// BucketCount returns the count of the bucket with index idx.
func (hg *Histogram) BucketCount(idx int) int64 {
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
			LowerBound: DeepSlice(hg.GetLower(i).GetBytes()),
			UpperBound: DeepSlice(hg.GetUpper(i).GetBytes()),
			Repeats:    hg.Buckets[i].Repeat,
			Ndv:        &hg.Buckets[i].NDV,
		}
		protoHg.Buckets = append(protoHg.Buckets, bkt)
	}
	return protoHg
}

// DeepSlice sallowly clones a slice.
func DeepSlice[T any](s []T) []T {
	r := make([]T, len(s))
	copy(r, s)
	return r
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
	cmp, err := lh.GetUpper(lLen-1).Compare(sc.TypeCtx(), rh.GetLower(0), collate.GetBinaryCollator())
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
	totalNotNull := hg.NotNullCount() * factor
	curNDV := float64(hg.NDV) * factor
	curNDV = math.Max(curNDV, 1)
	return totalNotNull / curNDV
}

// OutOfRange checks if the datum is out of range.
func (hg *Histogram) OutOfRange(val types.Datum) bool {
	if hg.Len() == 0 {
		return false
	}
	return chunk.Compare(hg.Bounds.GetRow(0), 0, &val) > 0 ||
		chunk.Compare(hg.Bounds.GetRow(hg.Bounds.NumRows()-1), 0, &val) < 0
}

// OutOfRangeRowCount estimate the row count of part of [lDatum, rDatum] which is out of range of the histogram.
// Here we assume the density of data is decreasing from the lower/upper bound of the histogram toward outside.
// The maximum row count it can get is the modifyCount. It reaches the maximum when out-of-range width reaches histogram range width.
// As it shows below. To calculate the out-of-range row count, we need to calculate the percentage of the shaded area.
// Note that we assume histL-boundL == histR-histL == boundR-histR here.
/*
               /│             │\
             /  │             │  \
           /x│  │◄─histogram─►│    \
         / xx│  │    range    │      \
       / │xxx│  │             │        \
     /   │xxx│  │             │          \
────┴────┴───┴──┴─────────────┴───────────┴─────
    ▲    ▲   ▲  ▲             ▲           ▲
    │    │   │  │             │           │
 boundL  │   │histL         histR       boundR
         │   │
    lDatum  rDatum
*/
// The percentage of shaded area on the left side calculation formula is:
// leftPercent = (math.Pow(actualR-boundL, 2) - math.Pow(actualL-boundL, 2)) / math.Pow(histWidth, 2)
// You can find more details at https://github.com/pingcap/tidb/pull/47966#issuecomment-1778866876
func (hg *Histogram) OutOfRangeRowCount(
	sctx planctx.PlanContext,
	lDatum, rDatum *types.Datum,
	realtimeRowCount, modifyCount, histNDV int64,
) (result float64) {
	debugTrace := sctx.GetSessionVars().StmtCtx.EnableOptimizerDebugTrace
	if debugTrace {
		debugtrace.EnterContextCommon(sctx)
		debugtrace.RecordAnyValuesWithNames(sctx,
			"lDatum", lDatum.String(),
			"rDatum", rDatum.String(),
			"modifyCount", modifyCount,
			"realtimeRowCount", realtimeRowCount,
		)
		defer func() {
			debugtrace.RecordAnyValuesWithNames(sctx, "Result", result)
			debugtrace.LeaveContextCommon(sctx)
		}()
	}
	if hg.Len() == 0 {
		return 0
	}

	// If there are no modifications to the table, return 0 - since all of this logic is
	// redundant if we get to the end and return the min - which includes zero,
	// TODO: The execution here is if we are out of range due to sampling of the histograms - which
	// may miss the lowest/highest values - and we are out of range without any modifications.
	if modifyCount == 0 {
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
	unsigned := mysql.HasUnsignedFlag(hg.Tp.GetFlag())
	// If this is an unsigned column, we need to make sure values are not negative.
	// Normal negative value should have become 0. But this still might happen when met MinNotNull here.
	// Maybe it's better to do this transformation in the ranger like the normal negative value.
	if unsigned {
		if l < 0 {
			l = 0
		}
		if r < 0 {
			r = 0
		}
	}

	if debugTrace {
		debugtrace.RecordAnyValuesWithNames(sctx,
			"commonPrefix", commonPrefix,
			"lScalar", l,
			"rScalar", r,
			"unsigned", unsigned,
		)
	}

	// make sure l < r
	if l >= r {
		return 0
	}

	allowUseModifyCount := sctx.GetSessionVars().GetOptObjective() != variable.OptObjectiveDeterminate

	// Convert the lower and upper bound of the histogram to scalar value(float64)
	histL := convertDatumToScalar(hg.GetLower(0), commonPrefix)
	histR := convertDatumToScalar(hg.GetUpper(hg.Len()-1), commonPrefix)
	histWidth := histR - histL
	// If we find that the histogram width is too small or too large - we still may need to consider
	// the impact of modifications to the table
	histInvalid := false
	if histWidth <= 0 {
		if !allowUseModifyCount {
			return 0
		}
		histInvalid = true
	}
	if math.IsInf(histWidth, 1) {
		if !allowUseModifyCount {
			// The histogram is too wide. As a quick fix, we return 0 to indicate that the overlap percentage is near 0.
			return 0
		}
		histInvalid = true
	}
	boundL := histL - histWidth
	boundR := histR + histWidth

	var leftPercent, rightPercent, rowCount float64
	if debugTrace {
		defer func() {
			debugtrace.RecordAnyValuesWithNames(sctx,
				"histL", histL,
				"histR", histR,
				"boundL", boundL,
				"boundR", boundR,
				"lPercent", leftPercent,
				"rPercent", rightPercent,
				"rowCount", rowCount,
			)
		}()
	}

	// keep l and r unchanged, use actualL and actualR to calculate.
	actualL := l
	actualR := r
	// Only attempt to calculate the ranges if the histogram is valid
	if !histInvalid {
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
	}

	totalPercent := min(leftPercent*0.5+rightPercent*0.5, 1.0)
	rowCount = totalPercent * hg.NotNullCount()

	// Upper & lower bound logic.
	upperBound := rowCount
	if histNDV > 0 {
		upperBound = hg.NotNullCount() / float64(histNDV)
	}

	if !allowUseModifyCount {
		// In OptObjectiveDeterminate mode, we can't rely on the modify count anymore.
		// An upper bound is necessary to make the estimation make sense for predicates with bound on only one end, like a > 1.
		// We use 1/NDV here (only the Histogram part is considered) and it seems reasonable and good enough for now.
		return min(rowCount, upperBound)
	}

	// If the realtimeRowCount is larger than the original table rows, then any out of range estimate is unreliable.
	// Assume at least 1/NDV is returned
	addedRows := float64(realtimeRowCount) - hg.TotalRowCount()
	if addedRows > 1 {
		// Conservatively - use the larger of the left or right percent - since we are working with
		// changes to the table since last Analyze - any out of range estimate is unreliable.
		// if the histogram range is invalid (too small/large - histInvalid) - totalPercent is zero
		// and we will set rowCount to min of upperbound and added rows
		totalPercent = min(0.5, max(leftPercent, rightPercent))
		rowCount += totalPercent * addedRows
		if rowCount < upperBound {
			rowCount = min(upperBound, addedRows)
		}
	}

	// Use modifyCount as a final bound
	return min(rowCount, float64(modifyCount))
}

// Copy deep copies the histogram.
func (hg *Histogram) Copy() *Histogram {
	if hg == nil {
		return nil
	}
	newHist := *hg
	if hg.Bounds != nil {
		newHist.Bounds = hg.Bounds.CopyConstruct()
	}
	newHist.Buckets = make([]Bucket, 0, len(hg.Buckets))
	newHist.Buckets = append(newHist.Buckets, hg.Buckets...)
	return &newHist
}

// TruncateHistogram truncates the histogram to `numBkt` buckets.
func (hg *Histogram) TruncateHistogram(numBkt int) *Histogram {
	hist := hg.Copy()
	hist.Buckets = hist.Buckets[:numBkt]
	hist.Bounds.TruncateTo(numBkt * 2)
	return hist
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
	limit := hg.NotNullCount() / float64(hg.Len())
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
			res := hg.BetweenRowCount(nil, types.NewBytesDatum(prefixColData), types.NewBytesDatum(kv.Key(prefixColData).PrefixNext()))
			if res >= limit {
				dataCnts = append(dataCnts, dataCnt{prefixColData, uint64(res)})
			}
		}
	}
	slices.SortStableFunc(dataCnts, func(a, b dataCnt) int { return -cmp.Compare(a.cnt, b.cnt) })
	if len(dataCnts) > int(numTopN) {
		dataCnts = dataCnts[:numTopN]
	}
	topN.TopN = make([]TopNMeta, 0, len(dataCnts))
	for _, dataCnt := range dataCnts {
		h1, h2 := murmur3.Sum128(dataCnt.data)
		realCnt := cms.queryHashValue(nil, h1, h2)
		cms.SubValue(h1, h2, realCnt)
		topN.AppendTopN(dataCnt.data, realCnt)
	}
	topN.Sort()
	return nil
}

var bucket4MergingPool = sync.Pool{
	New: func() any {
		return newBucket4Meging()
	},
}

func newbucket4MergingForRecycle() *bucket4Merging {
	return bucket4MergingPool.Get().(*bucket4Merging)
}

func releasebucket4MergingForRecycle(b *bucket4Merging) {
	b.disjointNDV = 0
	b.Repeat = 0
	b.NDV = 0
	b.Count = 0
	bucket4MergingPool.Put(b)
}

// bucket4Merging is only used for merging partition hists to global hist.
type bucket4Merging struct {
	lower *types.Datum
	upper *types.Datum
	Bucket
	// disjointNDV is used for merging bucket NDV, see mergeBucketNDV for more details.
	disjointNDV int64
}

// newBucket4Meging creates a new bucket4Merging.
// but we create it from bucket4MergingPool as soon as possible to reduce the cost of GC.
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
		b := newbucket4MergingForRecycle()
		hg.LowerToDatum(i, b.lower)
		hg.UpperToDatum(i, b.upper)
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
	result := newbucket4MergingForRecycle()
	result.Repeat = b.Repeat
	result.NDV = b.NDV
	b.upper.Copy(result.upper)
	b.lower.Copy(result.lower)
	result.Count = b.Count
	result.disjointNDV = b.disjointNDV
	return *result
}

// mergeBucketNDV merges bucket NDV from tow bucket `right` & `left`.
// Before merging, you need to make sure that when using (upper, lower) as the comparison key, `right` is greater than `left`
func mergeBucketNDV(sc *stmtctx.StatementContext, left *bucket4Merging, right *bucket4Merging) (*bucket4Merging, error) {
	res := right.Clone()
	if left.Count == 0 {
		return &res, nil
	}
	if right.Count == 0 {
		left.lower.Copy(res.lower)
		left.upper.Copy(res.upper)
		res.NDV = left.NDV
		return &res, nil
	}
	upperCompare, err := right.upper.Compare(sc.TypeCtx(), left.upper, collate.GetBinaryCollator())
	if err != nil {
		return nil, err
	}
	// __right__|
	// _______left____|
	// illegal order.
	if upperCompare < 0 {
		err := errors.Errorf("illegal bucket order")
		statslogutil.StatsLogger().Warn("fail to mergeBucketNDV", zap.Error(err))
		return nil, err
	}
	//  ___right_|
	//  ___left__|
	// They have the same upper.
	if upperCompare == 0 {
		lowerCompare, err := right.lower.Compare(sc.TypeCtx(), left.lower, collate.GetBinaryCollator())
		if err != nil {
			return nil, err
		}
		//      |____right____|
		//         |__left____|
		// illegal order.
		if lowerCompare < 0 {
			err := errors.Errorf("illegal bucket order")
			statslogutil.StatsLogger().Warn("fail to mergeBucketNDV", zap.Error(err))
			return nil, err
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
	lowerCompareUpper, err := right.lower.Compare(sc.TypeCtx(), left.upper, collate.GetBinaryCollator())
	if err != nil {
		return nil, err
	}
	//                  |_right_|
	//  |___left____|
	// `left` and `right` do not intersect
	// We add right.ndv in `disjointNDV`, and let `right.ndv = left.ndv` be used for subsequent merge.
	// This is because, for the merging of many buckets, we merge them from back to front.
	if lowerCompareUpper >= 0 {
		left.upper.Copy(res.upper)
		left.lower.Copy(res.lower)
		res.disjointNDV += right.NDV
		res.NDV = left.NDV
		return &res, nil
	}
	upperRatio := calcFraction4Datums(right.lower, right.upper, left.upper)
	lowerCompare, err := right.lower.Compare(sc.TypeCtx(), left.lower, collate.GetBinaryCollator())
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
//
//	upper = buckets[r-1].upper
//	count = sum of buckets[l...r).count
//	repeat = sum of buckets[i] (buckets[i].upper == global bucket.upper && i in [l...r))
//	ndv = merge bucket ndv from r-1 to l by mergeBucketNDV
//
// Notice: lower is not calculated here.
func mergePartitionBuckets(sc *stmtctx.StatementContext, buckets []*bucket4Merging) (*bucket4Merging, error) {
	if len(buckets) == 0 {
		return nil, errors.Errorf("not enough buckets to merge")
	}
	res := newbucket4MergingForRecycle()
	buckets[len(buckets)-1].upper.Copy(res.upper)
	right := buckets[len(buckets)-1].Clone()

	totNDV := int64(0)
	intest.Assert(res.Count == 0, "Count in the new bucket4Merging should be 0")
	intest.Assert(res.Repeat == 0, "Repeat in the new bucket4Merging should be 0")
	intest.Assert(res.NDV == 0, "NDV in the new bucket4Merging bucket4Merging should be 0")
	for i := len(buckets) - 1; i >= 0; i-- {
		totNDV += buckets[i].NDV
		res.Count += buckets[i].Count
		compare, err := buckets[i].upper.Compare(sc.TypeCtx(), res.upper, collate.GetBinaryCollator())
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
	return res, nil
}

func (t *TopNMeta) buildBucket4Merging(d *types.Datum, analyzeVer int) *bucket4Merging {
	res := newbucket4MergingForRecycle()
	d.Copy(res.lower)
	d.Copy(res.upper)
	res.Count = int64(t.Count)
	res.Repeat = int64(t.Count)
	if analyzeVer <= Version2 {
		res.NDV = 0
	}
	failpoint.Inject("github.com/pingcap/pkg/statistics/enableTopNNDV", func(_ failpoint.Value) {
		res.NDV = 1
	})
	intest.Assert(analyzeVer <= Version2)
	return res
}

// MergePartitionHist2GlobalHist merges hists (partition-level Histogram) to a global-level Histogram
func MergePartitionHist2GlobalHist(sc *stmtctx.StatementContext, hists []*Histogram, popedTopN []TopNMeta, expBucketNumber int64, isIndex bool, analyzeVer int) (*Histogram, error) {
	var totCount, totNull, totColSize int64
	var bucketNumber int
	if expBucketNumber == 0 {
		return nil, errors.Errorf("expBucketNumber can not be zero")
	}
	for _, hist := range hists {
		totColSize += hist.TotColSize
		totNull += hist.NullCount
		histLen := hist.Len()
		if histLen > 0 {
			bucketNumber += histLen
			totCount += hist.Buckets[hist.Len()-1].Count
		}
	}

	// If all the hist and the topn is empty, return a empty hist.
	if bucketNumber+len(popedTopN) == 0 {
		return NewHistogram(hists[0].ID, 0, totNull, hists[0].LastUpdateVersion, hists[0].Tp, 0, totColSize), nil
	}

	bucketNumber += len(popedTopN)
	buckets := make([]*bucket4Merging, 0, bucketNumber)
	globalBuckets := make([]*bucket4Merging, 0, expBucketNumber)

	// init `buckets`.
	for _, hist := range hists {
		buckets = append(buckets, hist.buildBucket4Merging()...)
	}

	for _, meta := range popedTopN {
		totCount += int64(meta.Count)
		d, err := topNMetaToDatum(meta, hists[0].Tp.GetType(), isIndex, sc.TimeZone())
		if err != nil {
			return nil, err
		}
		buckets = append(buckets, meta.buildBucket4Merging(&d, analyzeVer))
	}

	// Remove empty buckets
	tail := 0
	for i := range buckets {
		if buckets[i].Count != 0 {
			// Because we will reuse the tail of the slice in `releasebucket4MergingForRecycle`,
			// we need to shift the non-empty buckets to the front.
			buckets[tail], buckets[i] = buckets[i], buckets[tail]
			tail++
		}
	}
	for n := tail; n < len(buckets); n++ {
		releasebucket4MergingForRecycle(buckets[n])
	}
	buckets = buckets[:tail]

	err := sortBucketsByUpperBound(sc.TypeCtx(), buckets)
	if err != nil {
		return nil, err
	}

	var sum, prevSum int64
	r := len(buckets)
	bucketCount := int64(1)
	gBucketCountThreshold := (totCount / expBucketNumber) * 80 / 100 // expectedBucketSize * 0.8
	mergeBuffer := make([]*bucket4Merging, 0, (len(buckets)+int(expBucketNumber)-1)/int(expBucketNumber))
	cutAndFixBuffer := make([]*bucket4Merging, 0, (len(buckets)+int(expBucketNumber))/int(expBucketNumber))
	var currentLeftMost *types.Datum
	for i := len(buckets) - 1; i >= 0; i-- {
		if currentLeftMost == nil {
			currentLeftMost = buckets[i].lower
		} else {
			res, err := currentLeftMost.Compare(sc.TypeCtx(), buckets[i].lower, collate.GetBinaryCollator())
			if err != nil {
				return nil, err
			}
			if res > 0 {
				currentLeftMost = buckets[i].lower
			}
		}
		sum += buckets[i].Count
		if sum >= totCount*bucketCount/expBucketNumber && sum-prevSum >= gBucketCountThreshold {
			// If the buckets have the same upper, we merge them into the same new buckets.
			// We don't need to update the currentLeftMost in the for loop because the leftmost bucket's lower
			// will be the smallest when their upper is the same.
			// We just need to update it after the for loop.
			for ; i > 0; i-- {
				res, err := buckets[i-1].upper.Compare(sc.TypeCtx(), buckets[i].upper, collate.GetBinaryCollator())
				if err != nil {
					return nil, err
				}
				if res != 0 {
					break
				}
				sum += buckets[i-1].Count
			}
			res, err := currentLeftMost.Compare(sc.TypeCtx(), buckets[i].lower, collate.GetBinaryCollator())
			if err != nil {
				return nil, err
			}
			if res > 0 {
				currentLeftMost = buckets[i].lower
			}

			// Iterate possible overlapped ones.
			// We need to re-sort this part.
			mergeBuffer = mergeBuffer[:0]
			cutAndFixBuffer = cutAndFixBuffer[:0]
			leftMostValidPosForNonOverlapping := i
			for ; i > 0; i-- {
				res, err := buckets[i-1].upper.Compare(sc.TypeCtx(), currentLeftMost, collate.GetBinaryCollator())
				if err != nil {
					return nil, err
				}
				// If buckets[i-1].upper < currentLeftMost, this bucket has no overlap with current merging one. Break it.
				if res < 0 {
					break
				}
				// Now the bucket[i-1].upper >= currentLeftMost, they are overlapped.
				res, err = buckets[i-1].lower.Compare(sc.TypeCtx(), currentLeftMost, collate.GetBinaryCollator())
				if err != nil {
					return nil, err
				}
				// If buckets[i-1].lower >= currentLeftMost, this bucket is totally inside. So it can be totally merged.
				if res >= 0 {
					sum += buckets[i-1].Count
					mergeBuffer = append(mergeBuffer, buckets[i-1])
					continue
				}
				// Now buckets[i-1].lower < currentLeftMost < buckets[i-1].upper
				// calcFraction4Datums calc the value: (currentLeftMost - lower_bound) / (upper_bound - lower_bound)
				overlapping := 1 - calcFraction4Datums(buckets[i-1].lower, buckets[i-1].upper, currentLeftMost)
				overlappedCount := int64(float64(buckets[i-1].Count) * overlapping)
				overlappedNDV := int64(float64(buckets[i-1].NDV) * overlapping)
				sum += overlappedCount
				buckets[i-1].Count -= overlappedCount
				buckets[i-1].NDV -= overlappedNDV
				buckets[i-1].Repeat = 0
				if buckets[i-1].NDV < 0 {
					buckets[i-1].NDV = 0
				}
				if buckets[i-1].Count < 0 {
					buckets[i-1].Count = 0
				}

				// Cut it.
				cutBkt := newbucket4MergingForRecycle()
				buckets[i-1].upper.Copy(cutBkt.upper)
				currentLeftMost.Copy(cutBkt.lower)
				currentLeftMost.Copy(buckets[i-1].upper)
				cutBkt.Count = overlappedCount
				cutBkt.NDV = overlappedNDV
				mergeBuffer = append(mergeBuffer, cutBkt)
				cutAndFixBuffer = append(cutAndFixBuffer, cutBkt)
			}
			var merged *bucket4Merging
			if len(cutAndFixBuffer) == 0 {
				merged, err = mergePartitionBuckets(sc, buckets[i:r])
				if err != nil {
					return nil, err
				}
			} else {
				// The content in the merge buffer don't need a re-sort since we just fix some lower bound for them.
				mergeBuffer = append(mergeBuffer, buckets[leftMostValidPosForNonOverlapping:r]...)
				merged, err = mergePartitionBuckets(sc, mergeBuffer)
				if err != nil {
					return nil, err
				}
				for _, bkt := range cutAndFixBuffer {
					releasebucket4MergingForRecycle(bkt)
				}
				// The buckets in buckets[i:origI] needs a re-sort.
				err = sortBucketsByUpperBound(sc.TypeCtx(), buckets[i:leftMostValidPosForNonOverlapping])
				if err != nil {
					return nil, err
				}
				// After the operation, the buckets in buckets[i:origI] contains two kinds of buckets:
				// 1. The buckets that are totally inside the merged bucket. => lower_bound >= currentLeftMost
				//    It's not changed. [lower_bound_i, upper_bound_i] with lower_bound_i >= currentLeftMost
				// 2. The buckets that are overlapped with the merged bucket. lower_bound < currentLeftMost < upper_bound
				//    After cutting, the remained part is [lower_bound_i, currentLeftMost]
				// To do the next round of merging, we need to kick out the 1st kind of buckets.
				// And after the re-sort, the 2nd kind of buckets will be in the front.
				leftMostInvalidPosForNextRound := leftMostValidPosForNonOverlapping
				for ; leftMostInvalidPosForNextRound > i; leftMostInvalidPosForNextRound-- {
					res, err := buckets[leftMostInvalidPosForNextRound-1].lower.Compare(sc.TypeCtx(), currentLeftMost, collate.GetBinaryCollator())
					if err != nil {
						return nil, err
					}
					// Once the lower bound < currentLeftMost, we've skipped all the 1st kind of bucket.
					// We can break here.
					if res < 0 {
						break
					}
				}
				intest.AssertFunc(func() bool {
					for j := i; j < leftMostInvalidPosForNextRound; j++ {
						res, err := buckets[j].upper.Compare(sc.TypeCtx(), currentLeftMost, collate.GetBinaryCollator())
						if err != nil {
							return false
						}
						if res != 0 {
							return false
						}
					}
					return true
				}, "the buckets are not sorted actually")
				i = leftMostInvalidPosForNextRound
			}
			currentLeftMost.Copy(merged.lower)
			currentLeftMost = nil
			globalBuckets = append(globalBuckets, merged)
			r = i
			bucketCount++
			prevSum = sum
		}
	}
	if r > 0 {
		leftMost := buckets[0].lower
		for i, b := range buckets[:r] {
			if i == 0 {
				continue
			}
			res, err := leftMost.Compare(sc.TypeCtx(), b.lower, collate.GetBinaryCollator())
			if err != nil {
				return nil, err
			}
			if res > 0 {
				leftMost = b.lower
			}
		}

		merged, err := mergePartitionBuckets(sc, buckets[:r])
		if err != nil {
			return nil, err
		}
		leftMost.Copy(merged.lower)
		globalBuckets = append(globalBuckets, merged)
	}
	for i := 0; i < len(buckets); i++ {
		releasebucket4MergingForRecycle(buckets[i])
	}
	// Because we merge backwards, we need to flip the slices.
	slices.Reverse(globalBuckets)

	for i := 1; i < len(globalBuckets); i++ {
		globalBuckets[i].Count = globalBuckets[i].Count + globalBuckets[i-1].Count
	}

	// Recalculate repeats
	// TODO: optimize it later since it's a simple but not the fastest implementation whose complexity is O(nBkt * nHist * log(nBkt))
	for _, bucket := range globalBuckets {
		var repeat float64
		for _, hist := range hists {
			histRowCount, _ := hist.EqualRowCount(nil, *bucket.upper, isIndex)
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

// sortBucketsByUpperBound the bucket by upper bound first, then by lower bound.
// If bkt[i].upper = bkt[i+1].upper, then we'll get bkt[i].lower < bkt[i+1].lower.
func sortBucketsByUpperBound(ctx types.Context, buckets []*bucket4Merging) error {
	var sortError error
	slices.SortFunc(buckets, func(i, j *bucket4Merging) int {
		res, err := i.upper.Compare(ctx, j.upper, collate.GetBinaryCollator())
		if err != nil {
			sortError = err
		}
		if res != 0 {
			return res
		}
		res, err = i.lower.Compare(ctx, j.lower, collate.GetBinaryCollator())
		if err != nil {
			sortError = err
		}
		return res
	})
	return sortError
}

const (
	// AllLoaded indicates all statistics are loaded
	AllLoaded = iota
	// AllEvicted indicates all statistics are evicted
	AllEvicted
)

// StatsLoadedStatus indicates the status of statistics
type StatsLoadedStatus struct {
	statsInitialized bool
	evictedStatus    int
}

// NewStatsFullLoadStatus returns the status that the column/index fully loaded
func NewStatsFullLoadStatus() StatsLoadedStatus {
	return StatsLoadedStatus{
		statsInitialized: true,
		evictedStatus:    AllLoaded,
	}
}

// NewStatsAllEvictedStatus returns the status that only loads count/nullCount/NDV and doesn't load CMSketch/TopN/Histogram.
// When we load table stats, column stats is in AllEvicted status by default. CMSketch/TopN/Histogram of column is only
// loaded when we really need column stats.
func NewStatsAllEvictedStatus() StatsLoadedStatus {
	return StatsLoadedStatus{
		statsInitialized: true,
		evictedStatus:    AllEvicted,
	}
}

// Copy copies the status
func (s *StatsLoadedStatus) Copy() StatsLoadedStatus {
	return StatsLoadedStatus{
		statsInitialized: s.statsInitialized,
		evictedStatus:    s.evictedStatus,
	}
}

// IsStatsInitialized indicates whether the column/index's statistics was loaded from storage before.
// Note that `IsStatsInitialized` only can be set in initializing
func (s StatsLoadedStatus) IsStatsInitialized() bool {
	return s.statsInitialized
}

// IsLoadNeeded indicates whether it needs load statistics during LoadNeededHistograms or sync stats
// If the column/index was loaded and any statistics of it is evicting, it also needs re-load statistics.
func (s StatsLoadedStatus) IsLoadNeeded() bool {
	if s.statsInitialized {
		return s.evictedStatus > AllLoaded
	}
	// If statsInitialized is false, it means there is no stats for the column/index in the storage.
	// Hence, we don't need to trigger the task of loading the column/index stats.
	return false
}

// IsEssentialStatsLoaded indicates whether the essential statistics is loaded.
// If the column/index was loaded, and at least histogram and topN still exists, the necessary statistics is still loaded.
func (s StatsLoadedStatus) IsEssentialStatsLoaded() bool {
	return s.statsInitialized && (s.evictedStatus < AllEvicted)
}

// IsAllEvicted indicates whether all the stats got evicted or not.
func (s StatsLoadedStatus) IsAllEvicted() bool {
	return s.statsInitialized && s.evictedStatus >= AllEvicted
}

// IsFullLoad indicates whether the stats are full loaded
func (s StatsLoadedStatus) IsFullLoad() bool {
	return s.statsInitialized && s.evictedStatus == AllLoaded
}
