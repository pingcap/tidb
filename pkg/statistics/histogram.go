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
	"container/heap"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/generic"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/twmb/murmur3"
	"go.uber.org/zap"
)

const (
	outOfRangeBetweenRate float64 = 100
)

var (
	// Global static chunk for pseudo histograms to avoid chunk allocation
	globalPseudoChunkOnce sync.Once
	globalPseudoChunk     *chunk.Chunk
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

// initGlobalPseudoChunk initializes the global static chunk for pseudo histograms
func initGlobalPseudoChunk() {
	// Create a minimal empty chunk that can be shared across all pseudo histograms
	// Use a basic field type that won't cause issues when shared
	globalPseudoChunk = chunk.NewEmptyChunk([]*types.FieldType{types.NewFieldType(mysql.TypeBlob)})
}

// getGlobalPseudoChunk returns the shared static chunk for pseudo histograms
// WARNING: The returned chunk MUST NOT be modified. It is shared across all pseudo histograms.
// Pseudo histograms should never have buckets added or bounds modified.
func getGlobalPseudoChunk() *chunk.Chunk {
	globalPseudoChunkOnce.Do(initGlobalPseudoChunk)
	return globalPseudoChunk
}

// prepareFieldTypeForHistogram prepares the field type for histogram usage.
// For string types, it clones the field type and sets the collation to binary
// to avoid decoding issues with the histogram's key representation.
func prepareFieldTypeForHistogram(tp *types.FieldType) *types.FieldType {
	if tp.EvalType() == types.ETString {
		// The histogram will store the string value's 'sort key' representation of its collation.
		// If we directly set the field type's collation to its original one, we would decode the Key representation using its collation.
		// This would cause panic. So we apply a little trick here to avoid decoding it by explicitly changing the collation to 'CollationBin'.
		tp = tp.Clone()
		tp.SetCollate(charset.CollationBin)
	}
	return tp
}

// NewPseudoHistogram creates a pseudo histogram that reuses global static components
// This avoids chunk allocation while preserving field type semantics.
func NewPseudoHistogram(id int64, tp *types.FieldType) *Histogram {
	tp = prepareFieldTypeForHistogram(tp)
	return &Histogram{
		ID:                id,
		NDV:               0,
		NullCount:         0,
		LastUpdateVersion: 0,
		Tp:                tp,
		Bounds:            getGlobalPseudoChunk(),
		Buckets:           make([]Bucket, 0),
		TotColSize:        0,
		Correlation:       0,
	}
}

// NewHistogram creates a new histogram.
func NewHistogram(id, ndv, nullCount int64, version uint64, tp *types.FieldType, bucketSize int, totColSize int64) *Histogram {
	tp = prepareFieldTypeForHistogram(tp)
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

// IsAnalyzed checks whether statistics are analyzed based on stats version.
func IsAnalyzed(statsVer int64) bool {
	return statsVer != Version0
}

// IsColumnAnalyzedOrSynthesized checks whether column statistics are available based on raw storage values.
// This includes both analyzed stats (statsVer != Version0) and synthesized stats from default values
// (which have statsVer == Version0 but ndv > 0 or nullCount > 0).
// This function is used to determine the 'analyzed' flag when inserting column stats into ColAndIdxExistenceMap.
// NOTE: Synthesized stats are only applicable to column stats, not index stats.
// They are only created when adding a column with a default value. See: InsertColStats2KV
func IsColumnAnalyzedOrSynthesized(statsVer int64, ndv int64, nullCount int64) bool {
	return IsAnalyzed(statsVer) || ndv > 0 || nullCount > 0
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
	return fmt.Sprintf("num: %d lower_bound: %s upper_bound: %s repeats: %d ndv: %d", hg.BucketCount(bktID), lowerVal, upperVal, hg.Buckets[bktID].Repeat, hg.Buckets[bktID].NDV)
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
	for i := range hg.Len() {
		strs = append(strs, hg.BucketToString(i, idxCols))
	}
	return strings.Join(strs, "\n")
}

// EqualRowCount estimates the row count where the column equals to value.
// matched: return true if this returned row count is from Bucket.Repeat or bucket NDV, which is more accurate than if not.
// The input sctx is just for debug trace, you can pass nil safely if that's not needed.
func (hg *Histogram) EqualRowCount(sctx planctx.PlanContext, value types.Datum, hasBucketNDV bool) (count float64, matched bool) {
	_, bucketIdx, inBucket, match := hg.LocateBucket(sctx, value)
	if !inBucket {
		return 0, false
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
func (hg *Histogram) LocateBucket(_ planctx.PlanContext, value types.Datum) (exceed bool, bucketIdx int, inBucket, matchLastValue bool) {
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
	// All the values are null.
	if hg.Bounds.NumRows() == 0 {
		return 0, 0
	}
	exceed, bucketIdx, inBucket, match := hg.LocateBucket(sctx, value)
	if exceed {
		return hg.NotNullCount(), hg.Len() - 1
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
// The input sctx is required for stats version 2. For version 1, it is just for debug trace, you can pass nil safely.
func (hg *Histogram) BetweenRowCount(sctx planctx.PlanContext, a, b types.Datum) RowEstimate {
	lessCountA, bktIndexA := hg.LessRowCountWithBktIdx(sctx, a)
	lessCountB, bktIndexB := hg.LessRowCountWithBktIdx(sctx, b)
	rangeEst := DefaultRowEst(lessCountB - lessCountA)
	lowEqual, _ := hg.EqualRowCount(sctx, a, false)
	ndvAvg := hg.NotNullCount() / float64(hg.NDV)
	// If values fall in the same bucket, we may underestimate the fractional result. So estimate the low value (a) as an equals, and
	// estimate the high value as the default (because the input high value may be "larger" than the true high value). The range should
	// not be less than both the low+high - or the lesser of the estimate for the individual range of a or b is used as a bound.
	if rangeEst.Est < max(lowEqual, ndvAvg) && hg.NDV > 0 {
		result := min(lessCountB, hg.NotNullCount()-lessCountA)
		rangeEst = DefaultRowEst(min(result, lowEqual+ndvAvg))
	}
	// LessCounts are equal only if no valid buckets or both values are out of range
	isInValidBucket := lessCountA != lessCountB
	// If values in the same bucket, use skewRatio to adjust the range estimate to account for potential skew.
	if isInValidBucket && bktIndexA == bktIndexB {
		// sctx may be nil for stats version 1
		if sctx != nil {
			skewRatio := sctx.GetSessionVars().RiskRangeSkewRatio
			sctx.GetSessionVars().RecordRelevantOptVar(vardef.TiDBOptRiskRangeSkewRatio)
			// Worst case skew is if the range includes all the rows in the bucket
			skewEstimate := hg.Buckets[bktIndexA].Count
			if bktIndexA > 0 {
				skewEstimate -= hg.Buckets[bktIndexA-1].Count
			}
			// If range does not include last value of its bucket, remove the repeat count from the skew estimate.
			if lessCountB <= float64(hg.Buckets[bktIndexA].Count-hg.Buckets[bktIndexA].Repeat) {
				skewEstimate -= hg.Buckets[bktIndexA].Repeat
			}
			if skewRatio > 0 {
				// Cap the max estimate to 2X the estimate, but not beyond the bucket's maximum possible row count.
				// TODO: RiskRangeSkewRatio is predominantly used for outOfRangeRowCount, and
				//       it's usage is diluted here. Consider how to address this if
				//       issues are reported here regarding under-estimation for in-bucket ranges.
				rangeEst = CalculateSkewRatioCounts(rangeEst.Est, min(rangeEst.Est*2, float64(skewEstimate)), skewRatio)
			}
			// Report the full max estimate for risk estimation usage in compareCandidates (skylinePruning).
			rangeEst.MaxEst = max(rangeEst.MaxEst, float64(skewEstimate))
		}
	}
	return rangeEst
}

// CalculateSkewRatioCounts calculates the default, min, and max skew estimates given a skew ratio.
func CalculateSkewRatioCounts(estimate, skewEstimate, skewRatio float64) RowEstimate {
	skewDiff := skewEstimate - estimate
	// Add a "ratio" of the skewEstimate to adjust the default row estimate.
	skewAmt := max(0, skewDiff*skewRatio)
	maxSkewAmt := min(skewDiff, 2*skewAmt)
	return RowEstimate{estimate + skewAmt, estimate, estimate + maxSkewAmt}
}

// RowEstimate stores the min, default, and max row count estimates.
type RowEstimate struct {
	Est    float64
	MinEst float64
	MaxEst float64
}

// DefaultRowEst returns a RowEstimate with same value for all three fields
func DefaultRowEst(est float64) RowEstimate {
	return RowEstimate{est, est, est}
}

// Add adds two RowEstimates together, storing result in the first RowEstimate.
func (r *RowEstimate) Add(r1 RowEstimate) {
	r.Est += r1.Est
	r.MinEst += r1.MinEst
	r.MaxEst += r1.MaxEst
}

// AddAll adds a float64 value to all three fields of the RowEstimate and stores the result.
func (r *RowEstimate) AddAll(f float64) {
	r.Est += f
	r.MinEst += f
	r.MaxEst += f
}

// Subtract subtracts two RowEstimates together, storing result in the first RowEstimate.
func (r *RowEstimate) Subtract(r1 RowEstimate) {
	r.Est -= r1.Est
	r.MinEst -= r1.MinEst
	r.MaxEst -= r1.MaxEst
}

// MultiplyAll multiplies all three fields of the RowEstimate by a float64 value and stores the result.
func (r *RowEstimate) MultiplyAll(f float64) {
	r.Est *= f
	r.MinEst *= f
	r.MaxEst *= f
}

// DivideAll divides all three fields of the RowEstimate by a float64 value and stores the result.
func (r *RowEstimate) DivideAll(f float64) {
	r.Est /= f
	r.MinEst /= f
	r.MaxEst /= f
}

// Clamp clamps all three fields of the RowEstimate to the given min and max values.
// Don't allow MinEst to be greater than Est, or MaxEst to be less than Est.
func (r *RowEstimate) Clamp(f1, f2 float64) {
	r.Est = mathutil.Clamp(r.Est, f1, f2)
	r.MinEst = min(r.MinEst, r.Est)
	r.MinEst = mathutil.Clamp(r.MinEst, f1, f2)
	r.MaxEst = max(r.MaxEst, r.Est)
	r.MaxEst = mathutil.Clamp(r.MaxEst, f1, f2)
}

// TotalRowCount returns the total count of this histogram.
func (hg *Histogram) TotalRowCount() float64 {
	return hg.NotNullCount() + float64(hg.NullCount)
}

// AbsRowCountDifference returns the absolute difference between the realtime row count
// and the histogram's total row count, representing data changes since the last ANALYZE.
func (hg *Histogram) AbsRowCountDifference(realtimeRowCount int64) float64 {
	return math.Abs(float64(realtimeRowCount) - hg.TotalRowCount())
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
	for i := range hg.Len() {
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
	for i := range rh.Len() {
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

// calculateLeftOverlapPercent calculates the percentage for an out-of-range overlap
// on the left side of the histogram. The predicate range [l, r] overlaps with
// the region (boundL, histL), and we calculate the percentage of the shaded area.
func calculateLeftOverlapPercent(l, r, boundL, histL, histWidth float64) float64 {
	if histWidth <= 0 {
		return 0
	}
	// bound the left/right ranges of the predicates to the "left triangle" of the histogram.
	l = max(l, boundL)
	r = min(r, histL)
	// If there's no overlap after bounding, return 0.
	if l >= r {
		return 0
	}
	// NOTE: Ranges are squared to determine a triangular (linear) distribution rather than uniform.
	// Width of the histogram - squared to normalize to a percentage
	histWidthSq := math.Pow(histWidth, 2)
	// Right side of the predicate as distance from the left edge of the histogram
	rightRange := math.Pow(r-boundL, 2)
	// Left side of the predicate as distance from the left edge of the histogram
	leftRange := math.Pow(l-boundL, 2)
	return (rightRange - leftRange) / histWidthSq
}

// calculateRightOverlapPercent calculates the percentage for an out-of-range overlap
// on the right side of the histogram. The predicate range [l, r] overlaps with
// the region (histR, boundR), and we calculate the percentage of the shaded area.
func calculateRightOverlapPercent(l, r, histR, boundR, histWidth float64) float64 {
	if histWidth <= 0 {
		return 0
	}
	// bound the left/right ranges of the predicates to the "right triangle" of the histogram.
	l = max(l, histR)
	r = min(r, boundR)
	// If there's no overlap after bounding, return 0.
	if l >= r {
		return 0
	}
	// NOTE: Ranges are squared to determine a triangular (linear) distribution rather than uniform.
	// Width of the histogram - squared to normalize to a percentage
	histWidthSq := math.Pow(histWidth, 2)
	// Left side of the predicate as distance from the right edge of the histogram.
	leftRange := math.Pow(boundR-l, 2)
	// Right side of the predicate as distance from the right edge of the histogram.
	rightRange := math.Pow(boundR-r, 2)
	return (leftRange - rightRange) / histWidthSq
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
) (result RowEstimate) {
	if hg.Len() == 0 {
		return DefaultRowEst(0)
	}

	// Step 1: Calculate a default of "one value"
	// oneValue assumes "one value qualifies", and is used as a lower bound.
	histNDV = max(histNDV, 1)
	oneValue := hg.NotNullCount() / float64(histNDV)

	// Step 2: If modifications are not allowed, return the one value.
	// In OptObjectiveDeterminate mode, we can't rely on real time statistics, so default to assuming
	// one value qualifies.
	allowUseModifyCount := sctx.GetSessionVars().GetOptObjective() != vardef.OptObjectiveDeterminate
	if !allowUseModifyCount {
		return RowEstimate{Est: oneValue, MinEst: oneValue, MaxEst: oneValue}
	}

	// Step 3: Adjust oneValue if the NDV is low
	// If NDV is low, it may no longer be representative of the data since ANALYZE
	// was last run. Use a default value against realtimeRowCount.
	// If NDV is not representitative, then hg.NotNullCount may not be either.
	if float64(histNDV) < outOfRangeBetweenRate {
		oneValue = max(min(oneValue, float64(realtimeRowCount)/outOfRangeBetweenRate), 1.0)
	}
	// Step 4: Calculate how much of the statistics share a common prefix.
	// For bytes and string type, we need to cut the common prefix when converting them to scalar value.
	// Here we calculate the length of common prefix.
	// TODO: If the common prefix is large, we may underestimate the out-of-range
	// portion because we can't distinguish the values with the same prefix.
	commonPrefix := 0
	if hg.GetLower(0).Kind() == types.KindBytes || hg.GetLower(0).Kind() == types.KindString {
		// Calculate the common prefix length among the lower and upper bound of histogram and the range we want to estimate.
		commonPrefix = commonPrefixLength(hg.GetLower(0).GetBytes(),
			hg.GetUpper(hg.Len()-1).GetBytes(),
			lDatum.GetBytes(),
			rDatum.GetBytes())
	}

	// Step 5:Convert the range we want to estimate to scalar value(float64)
	l := convertDatumToScalar(lDatum, commonPrefix)
	r := convertDatumToScalar(rDatum, commonPrefix)
	unsigned := mysql.HasUnsignedFlag(hg.Tp.GetFlag())
	// If this is an unsigned column, we need to make sure values are not negative.
	// Normal negative value should have become 0. But this still might happen when met MinNotNull here.
	// Maybe it's better to do this transformation in the ranger like the normal negative value.
	// Track whether negative values were clamped to 0, to detect impossible ranges.
	var leftClamped, rightClamped bool
	if unsigned {
		if l < 0 {
			l = 0
			leftClamped = true
		}
		if r < 0 {
			r = 0
			rightClamped = true
		}
		// If both bounds collapsed to 0 due to clamping negative values, this is
		// an impossible range (e.g., unsigned_col < 0). Return 0 estimate.
		if l == 0 && r == 0 && (leftClamped || rightClamped) {
			return DefaultRowEst(0)
		}
	}

	// Step 6: Convert the lower and upper bound of the histogram to scalar value(float64)
	histL := convertDatumToScalar(hg.GetLower(0), commonPrefix)
	histR := convertDatumToScalar(hg.GetUpper(hg.Len()-1), commonPrefix)
	histWidth := histR - histL
	// If we find that the histogram width is too small or too large - we still may need to consider
	// the impact of modifications to the table. Reset the histogram width to 0.
	if histWidth < 0 {
		histWidth = 0
	}
	if math.IsInf(histWidth, 1) {
		histWidth = 0
	}
	boundL := histL - histWidth
	boundR := histR + histWidth

	// Step 7: Calculate the width of the predicate
	// TODO: If predWidth == 0, it may be because the common prefix is too large.
	// In future - out of range for equal predicates should also use this logic
	// for consistency. We need to handle both "equal" and "large common prefix".
	predWidth := r - l
	if predWidth < 0 {
		// This should never happen.
		intest.Assert(false, "Right bound should not be less than left bound")

		return DefaultRowEst(0)
	} else if predWidth == 0 {
		// Set histWidth to 0 so that we can still return a minimum of oneValue,
		// and return the max as worst case.
		histWidth = 0
	}

	// Step 8: Calculate the out of range percentages
	// Calculate left overlap percentage if the range overlaps with (boundL, histL)
	leftPercent := calculateLeftOverlapPercent(l, r, boundL, histL, histWidth)
	// Calculate right overlap percentage if the range overlaps with (histR, boundR)
	rightPercent := calculateRightOverlapPercent(l, r, histR, boundR, histWidth)

	totalPercent := min(leftPercent*0.5+rightPercent*0.5, 1.0)
	maxTotalPercent := min(leftPercent+rightPercent, 1.0)

	// Step 9: Calculate the added rows
	// Use absolute value to account for the case where rows may have been added on one side,
	// but deleted from the other, resulting in qualifying out of range rows even though
	// realtimeRowCount is less than histogram count
	addedRows := hg.AbsRowCountDifference(realtimeRowCount)
	maxAddedRows := addedRows

	// Step 10: Calculate the estimated rows
	estRows := oneValue
	skewRatio := sctx.GetSessionVars().RiskRangeSkewRatio
	sctx.GetSessionVars().RecordRelevantOptVar(vardef.TiDBOptRiskRangeSkewRatio)
	if totalPercent > 0 {
		// Multiplying addedRows by 0.5 provides the assumption that 50% "addedRows" are inside
		// the histogram range, and 50% (0.5) are out-of-range. Users can adjust this
		// magic number by setting the session variable `tidb_opt_risk_range_skew_ratio`.
		// When skewRatio > 0, estRows sets the starting point for the skew ratio calculation.
		addedRowMultiplier := 0.5
		// NOTE: Skew ratio is used twice in this function.
		// This first usage allows a user to specify a ratio that is smaller or
		// larger than the default 0.5.
		if skewRatio > 0 {
			addedRowMultiplier = skewRatio
		}
		estRows = (addedRows * addedRowMultiplier) * totalPercent
	}

	// Step 11: Calculate a potential worst case for use in final MaxEst
	// We may have missed the true lowest/highest values due to sampling OR there could be a delay in
	// updates to modifyCount (meaning modifyCount is incorrectly set to 0). So ensure we always
	// account for at least 1% of the total row count as a worst case for "addedRows".
	// We inflate this here so ONLY to impact the MaxEst value.
	if modifyCount == 0 || addedRows == 0 {
		if realtimeRowCount <= 0 {
			realtimeRowCount = int64(hg.TotalRowCount())
		}
		// Use outOfRangeBetweenRate as a divisor to get a small percentage of the approximate
		// modifyCount (since outOfRangeBetweenRate has a default value of 100).
		maxAddedRows = max(maxAddedRows, float64(realtimeRowCount)/outOfRangeBetweenRate)
	}
	if maxTotalPercent > 0 {
		// Always apply maxTotalPercent to maxAddedRows to limit scaling when the predicate has an upper bound
		maxAddedRows *= maxTotalPercent
	}

	// Step 12: Calculate the final min/max/est rows including the skew ratio adjustment
	result.MinEst = min(estRows, oneValue)
	// NOTE: Skew ratio is used twice in this function.
	// This second usage scales the estimate from the base estimate to the max estimate.
	if skewRatio > 0 {
		result = CalculateSkewRatioCounts(estRows, maxAddedRows, skewRatio)
	} else {
		// Do not scale the estimate if skew ratio is not set.
		result.Est = estRows
	}
	result.Est = max(result.Est, oneValue)
	result.MaxEst = max(result.Est, maxAddedRows)

	return result
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
	for i := range hg.Bounds.NumRows() {
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
			res := hg.BetweenRowCount(nil, types.NewBytesDatum(prefixColData), types.NewBytesDatum(kv.Key(prefixColData).PrefixNext())).Est
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

// bucketRef is a lightweight pointer to a partition histogram bucket.
type bucketRef struct {
	histIdx   uint16
	bucketIdx uint16
}

// bucketMergeEntry is one partition's current head in the k-way merge.
// The upper bound Datum is cached at insertion time — comparisons during
// heap sift use the cached value, avoiding repeated Chunk indirection.
type bucketMergeEntry struct {
	upper types.Datum // cached, extracted once when entering the heap
	lower types.Datum // cached, extracted once when entering the heap
	sc    *stmtctx.StatementContext
	hists []*Histogram
	histIdx   uint16
	bucketIdx uint16
}

// bucketMergeHeap implements container/heap for k-way merging of
// pre-sorted partition histograms. Each entry is one partition's
// current (smallest unprocessed) bucket.
type bucketMergeHeap []bucketMergeEntry

func (h bucketMergeHeap) Len() int { return len(h) }
func (h bucketMergeHeap) Less(i, j int) bool {
	res, _ := h[i].upper.Compare(h[i].sc.TypeCtx(), &h[j].upper, collate.GetBinaryCollator())
	if res != 0 {
		return res < 0
	}
	// Secondary sort by lower bound for deterministic ordering.
	res, _ = h[i].lower.Compare(h[i].sc.TypeCtx(), &h[j].lower, collate.GetBinaryCollator())
	return res < 0
}
func (h bucketMergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *bucketMergeHeap) Push(x any)   { *h = append(*h, x.(bucketMergeEntry)) }
func (h *bucketMergeHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
func (h *bucketMergeHeap) init() { heap.Init(h) }
func (h *bucketMergeHeap) peek() *bucketMergeEntry {
	if len(*h) == 0 {
		return nil
	}
	return &(*h)[0]
}

// topNEntry is a flattened TopN value from one partition, used for sorting.
type topNEntry struct {
	encoded []byte
	count   uint64
}

// MergePartTopNAndHistToGlobal merges partition-level TopN lists and
// histograms into a single global TopN and histogram via two sorted
// merge-walks:
//
// Pass 1: sorted TopN entries + sorted histogram bucket refs → bounded
// min-heap selects the global TopN. Histogram upper-bound Repeat counts
// are included so values frequent across partitions but never in any
// single partition's TopN can still be identified ("spread value" case).
//
// Pass 2: merge-walk the same sorted refs + leftover TopN entries →
// equi-depth global histogram. Values that made global TopN have their
// per-bucket Repeat subtracted; leftover TopN values are injected at
// their sorted position.
//
// Memory: ~8 MB pointer array + ~4 KB heap on 8000-partition tables,
// vs ~1.4 GB of bucket4Merging in the previous implementation.
// Time: O((N+T) log(N+T)) for the sorts + O(N+T) for the walks,
// where N = total histogram buckets, T = total TopN entries.
func MergePartTopNAndHistToGlobal(
	topNs []*TopN,
	hists []*Histogram,
	numTopN uint32,
	expBucketNumber int64,
	isIndex bool,
	killer *sqlkiller.SQLKiller,
	sc *stmtctx.StatementContext,
) (*TopN, *Histogram, error) {
	if expBucketNumber == 0 {
		return nil, nil, errors.Errorf("expBucketNumber can not be zero")
	}

	// Find the first non-nil histogram for metadata (ID, Tp, etc.).
	// If none exist, we can still merge TopN-only data.
	var firstHist *Histogram
	for _, h := range hists {
		if h != nil {
			firstHist = h
			break
		}
	}
	if firstHist == nil && len(topNs) == 0 {
		return nil, nil, nil
	}

	tz := sc.TimeZone()
	var tp byte
	if firstHist != nil {
		tp = firstHist.Tp.GetType()
		statslogutil.StatsLogger().Info("MergePartTopNAndHistToGlobal start",
			zap.Int64("histID", firstHist.ID),
			zap.Bool("isIndex", isIndex),
			zap.Int("hists", len(hists)),
			zap.Int("topNs", len(topNs)))
	}

	// ---------------------------------------------------------------
	// Pass 1: Determine global TopN via sorted merge of TopN entries
	// and histogram bucket Repeats. No counter map, no codec encoding.
	// ---------------------------------------------------------------

	// 1a. Flatten + sort all partition TopN entries by encoded key.
	var allTopN []topNEntry
	for _, topN := range topNs {
		if topN.TotalCount() == 0 {
			continue
		}
		for _, val := range topN.TopN {
			allTopN = append(allTopN, topNEntry{encoded: val.Encoded, count: val.Count})
		}
	}
	slices.SortFunc(allTopN, func(a, b topNEntry) int {
		return bytes.Compare(a.encoded, b.encoded)
	})
	statslogutil.StatsLogger().Info("MergePartTopNAndHistToGlobal step 1a: sorted partition TopN",
		zap.Int("topNEntries", len(allTopN)))

	// 1b. Collect partition metadata and build a k-way merge heap.
	// Each partition's histogram is already sorted by upper bound.
	// The heap holds one entry per non-empty partition, with the
	// current bucket's upper bound cached — O(1) per comparison
	// instead of the O(indirection) of the previous sort approach.
	var totCount, totNull, totColSize int64
	totalBuckets := 0
	for _, hist := range hists {
		totColSize += hist.TotColSize
		totNull += hist.NullCount
		histLen := hist.Len()
		totalBuckets += histLen
		if histLen > 0 {
			totCount += hist.Buckets[histLen-1].Count
		}
	}

	// Initialize the merge heap with each partition's first non-empty bucket.
	mergeHeap := make(bucketMergeHeap, 0, len(hists))
	for hi, hist := range hists {
		for bi := range hist.Len() {
			count := hist.Buckets[bi].Count
			if bi > 0 {
				count -= hist.Buckets[bi-1].Count
			}
			if count > 0 {
				mergeHeap = append(mergeHeap, bucketMergeEntry{
					histIdx:   uint16(hi),
					bucketIdx: uint16(bi),
					upper:     *hist.GetUpper(bi),
					lower:     *hist.GetLower(bi),
					sc:        sc,
					hists:     hists,
				})
				break
			}
		}
	}
	mergeHeap.init()
	statslogutil.StatsLogger().Info("MergePartTopNAndHistToGlobal step 1b: built k-way merge heap",
		zap.Int("heapSize", mergeHeap.Len()),
		zap.Int("totalBuckets", totalBuckets))

	if len(allTopN) == 0 && mergeHeap.Len() == 0 {
		if firstHist != nil {
			return nil, NewHistogram(firstHist.ID, 0, totNull, firstHist.LastUpdateVersion,
				firstHist.Tp, 0, totColSize), nil
		}
		return nil, nil, nil
	}

	// 1c. Merge-walk both sorted sequences. For each unique value,
	// sum TopN counts + histogram Repeat counts and feed into a
	// bounded min-heap of size numTopN to find the global TopN.
	//
	// O(N log K) where K = numTopN (typically 100), vs O(N log N) for
	// a full sort. The heap holds ~100 entries (~4 KB) instead of the
	// full candidates slice (~80 MB on 8000-partition tables).
	type heapEntry struct {
		encoded     []byte
		totalCount  uint64
		repeatCount uint64
	}
	topNHeap := generic.NewBoundedMinHeap(int(numTopN), func(a, b heapEntry) int {
		return cmp.Compare(a.totalCount, b.totalCount)
	})
	// sortedRefs collects bucket references in sorted order as the heap
	// produces them. Reused by Pass 2 without rebuilding the heap.
	sortedRefs := make([]bucketRef, 0, totalBuckets)

	// nextBucket pops the heap's min entry, saves it to sortedRefs,
	// advances that partition to its next non-empty bucket, and pushes
	// the new head back.
	nextBucket := func() (bucketMergeEntry, bool) {
		if mergeHeap.Len() == 0 {
			return bucketMergeEntry{}, false
		}
		entry := heap.Pop(&mergeHeap).(bucketMergeEntry)
		sortedRefs = append(sortedRefs, bucketRef{entry.histIdx, entry.bucketIdx})
		// Advance this partition to the next non-empty bucket.
		h := hists[entry.histIdx]
		for bi := int(entry.bucketIdx) + 1; bi < h.Len(); bi++ {
			count := h.Buckets[bi].Count
			if bi > 0 {
				count -= h.Buckets[bi-1].Count
			}
			if count > 0 {
				heap.Push(&mergeHeap, bucketMergeEntry{
					histIdx:   entry.histIdx,
					bucketIdx: uint16(bi),
					upper:     *h.GetUpper(bi),
					lower:     *h.GetLower(bi),
					sc:        sc,
					hists:     hists,
				})
				break
			}
		}
		return entry, true
	}

	ti := 0
	for ti < len(allTopN) || mergeHeap.Len() > 0 {
		if err := killer.HandleSignal(); err != nil {
			return nil, nil, err
		}

		var entry heapEntry
		var mergeOrd int // -1: TopN only, 0: both, 1: refs only

		if ti >= len(allTopN) {
			mergeOrd = 1
		} else if mergeHeap.Len() == 0 {
			mergeOrd = -1
		} else {
			d, err := topNMetaToDatum(TopNMeta{Encoded: allTopN[ti].encoded}, tp, isIndex, tz)
			if err != nil {
				return nil, nil, err
			}
			mergeOrd, err = d.Compare(sc.TypeCtx(),
				&mergeHeap.peek().upper,
				collate.GetBinaryCollator())
			if err != nil {
				return nil, nil, err
			}
		}

		if mergeOrd <= 0 {
			entry.encoded = allTopN[ti].encoded
			entry.totalCount += allTopN[ti].count
			ti++
			for ti < len(allTopN) && bytes.Equal(allTopN[ti].encoded, entry.encoded) {
				entry.totalCount += allTopN[ti].count
				ti++
			}
		}
		if mergeOrd >= 0 {
			// Pop all buckets with the same upper bound (from different
			// partitions). The heap groups them naturally.
			first := true
			var groupUpper types.Datum
			for mergeHeap.Len() > 0 {
				if !first {
					c, err := mergeHeap.peek().upper.Compare(sc.TypeCtx(), &groupUpper,
						collate.GetBinaryCollator())
					if err != nil {
						return nil, nil, err
					}
					if c != 0 {
						break
					}
				}
				bkt, _ := nextBucket()
				if first {
					groupUpper = bkt.upper
					first = false
				}
				repeat := hists[bkt.histIdx].Buckets[bkt.bucketIdx].Repeat
				entry.repeatCount += uint64(repeat)
				entry.totalCount += uint64(repeat)
				if entry.encoded == nil {
					if isIndex {
						entry.encoded = hists[bkt.histIdx].Bounds.GetRow(int(bkt.bucketIdx)*2 + 1).GetBytes(0)
					} else {
						var err error
						entry.encoded, err = codec.EncodeKey(tz, nil, bkt.upper)
						if err != nil {
							return nil, nil, err
						}
					}
				}
			}
		}

		if entry.totalCount > 0 {
			topNHeap.Add(entry)
		}
	}
	statslogutil.StatsLogger().Info("MergePartTopNAndHistToGlobal step 1c: merge-walked TopN + buckets",
		zap.Int("sortedRefs", len(sortedRefs)))

	// 1d. Extract global TopN from the heap.
	topNSlice := topNHeap.ToSortedSlice()
	var globalTopN *TopN
	if len(topNSlice) > 0 {
		globalTopN = NewTopN(int(numTopN))
		for _, e := range topNSlice {
			globalTopN.AppendTopN(e.encoded, e.totalCount)
		}
	}

	// Build globalTopN lookup map (encoded key → decoded Datum) for Pass 2.
	type topNInfo struct {
		datum types.Datum
	}
	globalTopNMap := make(map[hack.MutableString]*topNInfo)
	if globalTopN != nil {
		for i := range globalTopN.TopN {
			d, err := topNMetaToDatum(TopNMeta{
				Encoded: globalTopN.TopN[i].Encoded,
				Count:   globalTopN.TopN[i].Count,
			}, tp, isIndex, tz)
			if err != nil {
				return nil, nil, err
			}
			globalTopNMap[hack.String(globalTopN.TopN[i].Encoded)] = &topNInfo{datum: d}
		}
	}

	// Adjust totCount: subtract histogram-Repeat for global TopN values
	// (moved to TopN). Read repeatCount directly from the heap output.
	for _, e := range topNSlice {
		totCount -= int64(e.repeatCount)
	}
	// Add TopN-origin counts for values that didn't make global TopN.
	// Their rows were excluded from partition histograms (put into
	// partition TopN instead), so they need to enter the global histogram.
	for _, e := range allTopN {
		if _, inGlobal := globalTopNMap[hack.String(e.encoded)]; !inGlobal {
			totCount += int64(e.count)
		}
	}
	globalTopNSize := 0
	if globalTopN != nil {
		globalTopNSize = len(globalTopN.TopN)
	}
	statslogutil.StatsLogger().Info("MergePartTopNAndHistToGlobal step 1d: extracted global TopN",
		zap.Int("globalTopN", globalTopNSize),
		zap.Int64("totCount", totCount))

	// ---------------------------------------------------------------
	// Pass 2: Build equi-depth global histogram by merge-walking
	// sorted refs and allTopN together. Both are sorted in the same
	// order. TopN entries in globalTopNMap are skipped (their rows are
	// in the TopN); others are injected into the histogram at their
	// sorted position. Single pass, no bucket4Merging.
	// ---------------------------------------------------------------
	var globalHist *Histogram
	if firstHist != nil {
		globalHist = NewHistogram(firstHist.ID, 0, totNull, firstHist.LastUpdateVersion,
			firstHist.Tp, int(expBucketNumber), totColSize)
	} else {
		globalHist = NewHistogram(0, 0, totNull, 0, types.NewFieldType(mysql.TypeNull), 0, totColSize)
	}

	if totCount <= 0 || (len(sortedRefs) == 0 && len(allTopN) == 0) {
		statslogutil.StatsLogger().Info("MergePartTopNAndHistToGlobal step 2: empty histogram (early return)")
		return globalTopN, globalHist, nil
	}

	var (
		cumCount     int64
		prevCumCount int64
		bucketIdx    int64 = 1
		bucketLower  *types.Datum
		lowerFloor   *types.Datum
		lastUpper    *types.Datum
		lastRepeat   int64
		prevUpper    *types.Datum
		prevRepeat   int64
	)

	// Check if an upper bound is a global TopN value by encoding to the
	// same key format as globalTopNMap. For index histograms the bounds
	// are already encoded; for columns we call codec.EncodeKey with a
	// reusable buffer (~256 calls, once per unique upper-bound group).
	var encodeBuf []byte
	isGlobalTopNVal := func(histIdx, bucketIdx int) bool {
		if isIndex {
			encoded := hists[histIdx].Bounds.GetRow(bucketIdx*2 + 1).GetBytes(0)
			_, ok := globalTopNMap[hack.String(encoded)]
			return ok
		}
		encodeBuf, _ = codec.EncodeKey(tz, encodeBuf[:0], *hists[histIdx].GetUpper(bucketIdx))
		_, ok := globalTopNMap[hack.String(encodeBuf)]
		return ok
	}

	emitGroup := func() {
		if cumCount <= prevCumCount {
			return
		}
		threshold := totCount * bucketIdx / expBucketNumber
		if cumCount >= threshold && bucketIdx < expBucketNumber {
			cutUpper := lastUpper
			cutCount := cumCount
			cutRepeat := lastRepeat
			if prevUpper != nil && prevCumCount < threshold &&
				(threshold-prevCumCount) < (cumCount-threshold) {
				cutUpper = prevUpper
				cutCount = prevCumCount
				cutRepeat = prevRepeat
			}
			if bucketLower != nil {
				globalHist.AppendBucketWithNDV(bucketLower, cutUpper, cutCount, cutRepeat, 0)
				bucketIdx++
				lowerFloor = cutUpper
				bucketLower = nil
			}
		}
	}

	startNewGroup := func(upper *types.Datum) {
		prevCumCount = cumCount
		prevUpper = lastUpper
		prevRepeat = lastRepeat
		lastRepeat = 0
		lastUpper = upper.Clone()
	}

	addToBucket := func(lower *types.Datum, count, repeat int64) {
		cumCount += count
		lastRepeat += repeat
		lo := lower
		if lowerFloor != nil {
			c, _ := lo.Compare(sc.TypeCtx(), lowerFloor, collate.GetBinaryCollator())
			if c < 0 {
				lo = lowerFloor
			}
		}
		if bucketLower == nil {
			bucketLower = lo.Clone()
		} else {
			c, _ := lo.Compare(sc.TypeCtx(), bucketLower, collate.GetBinaryCollator())
			if c < 0 {
				bucketLower = lo.Clone()
			}
		}
	}

	// Pass 2 iterates sortedRefs (produced in sorted order by Pass 1's
	// k-way merge) and merge-walks with allTopN. mergeOrd = cmp(topN,
	// hist_upper): <0 means the next topN key sorts before the next
	// hist upper (topN-only group), >0 means hist-only, ==0 means both
	// at the same key (merged into one group).
	ri, ti2 := 0, 0
	for ri < len(sortedRefs) || ti2 < len(allTopN) {
		var mergeOrd int
		var topNDatum types.Datum
		if ri >= len(sortedRefs) {
			mergeOrd = -1
		} else if ti2 >= len(allTopN) {
			mergeOrd = 1
		} else {
			var err error
			topNDatum, err = topNMetaToDatum(TopNMeta{Encoded: allTopN[ti2].encoded}, tp, isIndex, tz)
			if err != nil {
				return nil, nil, err
			}
			mergeOrd, err = topNDatum.Compare(sc.TypeCtx(),
				hists[sortedRefs[ri].histIdx].GetUpper(int(sortedRefs[ri].bucketIdx)),
				collate.GetBinaryCollator())
			if err != nil {
				return nil, nil, err
			}
		}

		// Pure topN group: topN key sorts before the next hist upper
		// (or hist is exhausted). Skip entries already in global TopN;
		// inject leftover ones at their sorted position.
		if mergeOrd < 0 {
			key := allTopN[ti2].encoded
			topNCount := int64(allTopN[ti2].count)
			ti2++
			for ti2 < len(allTopN) && bytes.Equal(allTopN[ti2].encoded, key) {
				topNCount += int64(allTopN[ti2].count)
				ti2++
			}
			if _, inGlobal := globalTopNMap[hack.String(key)]; inGlobal || topNCount == 0 {
				continue
			}
			if ri >= len(sortedRefs) {
				var err error
				topNDatum, err = topNMetaToDatum(TopNMeta{Encoded: key}, tp, isIndex, tz)
				if err != nil {
					return nil, nil, err
				}
			}
			startNewGroup(&topNDatum)
			addToBucket(&topNDatum, topNCount, topNCount)
			emitGroup()
			continue
		}

		// Hist group (alone, or merged with a topN entry at the same key).
		startNewGroup(hists[sortedRefs[ri].histIdx].GetUpper(int(sortedRefs[ri].bucketIdx)))

		if mergeOrd == 0 {
			key := allTopN[ti2].encoded
			topNCount := int64(allTopN[ti2].count)
			ti2++
			for ti2 < len(allTopN) && bytes.Equal(allTopN[ti2].encoded, key) {
				topNCount += int64(allTopN[ti2].count)
				ti2++
			}
			if _, inGlobal := globalTopNMap[hack.String(key)]; !inGlobal && topNCount > 0 {
				addToBucket(&topNDatum, topNCount, topNCount)
			}
		}

		topNMatch := isGlobalTopNVal(int(sortedRefs[ri].histIdx), int(sortedRefs[ri].bucketIdx))

		j := ri + 1
		for j < len(sortedRefs) {
			c, err := hists[sortedRefs[j].histIdx].GetUpper(int(sortedRefs[j].bucketIdx)).Compare(
				sc.TypeCtx(), hists[sortedRefs[ri].histIdx].GetUpper(int(sortedRefs[ri].bucketIdx)),
				collate.GetBinaryCollator())
			if err != nil {
				return nil, nil, err
			}
			if c != 0 {
				break
			}
			j++
		}

		for k := ri; k < j; k++ {
			h := hists[sortedRefs[k].histIdx]
			bkt := h.Buckets[sortedRefs[k].bucketIdx]
			count := bkt.Count
			if sortedRefs[k].bucketIdx > 0 {
				count -= h.Buckets[sortedRefs[k].bucketIdx-1].Count
			}
			repeat := bkt.Repeat
			if topNMatch {
				count -= repeat
				repeat = 0
				if count <= 0 {
					continue
				}
			}
			addToBucket(h.GetLower(int(sortedRefs[k].bucketIdx)), count, repeat)
		}

		ri = j
		emitGroup()
	}

	if bucketLower != nil && lastUpper != nil {
		globalHist.AppendBucketWithNDV(bucketLower, lastUpper, cumCount, lastRepeat, 0)
	}

	if !isIndex {
		for i := range globalHist.Buckets {
			globalHist.Buckets[i].NDV = 0
		}
	}
	statslogutil.StatsLogger().Info("MergePartTopNAndHistToGlobal step 2: built global histogram",
		zap.Int("buckets", len(globalHist.Buckets)))

	return globalTopN, globalHist, nil
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
