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
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/pingcap/tidb/pkg/util/ranger"
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

// BinarySearchRemoveVal removes the value from the TopN using binary search.
func (hg *Histogram) BinarySearchRemoveVal(val *types.Datum, count int64) {
	lowIdx, highIdx := 0, hg.Len()-1
	// if hg is too small, we don't need to check the branch. because the cost is more than binary search.
	if hg.Len() > 4 {
		if cmpResult := chunk.Compare(hg.Bounds.GetRow(highIdx*2+1), 0, val); cmpResult < 0 {
			return
		}
		if cmpResult := chunk.Compare(hg.Bounds.GetRow(lowIdx), 0, val); cmpResult > 0 {
			return
		}
	}
	var midIdx = 0
	var found bool
	for lowIdx <= highIdx {
		midIdx = (lowIdx + highIdx) / 2
		cmpResult := chunk.Compare(hg.Bounds.GetRow(midIdx*2), 0, val)
		if cmpResult > 0 {
			highIdx = midIdx - 1
			continue
		}
		cmpResult = chunk.Compare(hg.Bounds.GetRow(midIdx*2+1), 0, val)
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
		midbucket.Count -= count
		if midbucket.Count < 0 {
			midbucket.Count = 0
		}
		found = true
		break
	}
	if found {
		for midIdx++; midIdx <= hg.Len()-1; midIdx++ {
			hg.Buckets[midIdx].Count -= count
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

// checkBucket4MergingIsSorted checks whether the buckets are sorted by upper bound first, then by lower bound.
// using intest.AssertFunc to avoid the check in production.
func checkBucket4MergingIsSorted(ctx types.Context, buckets []*bucket4Merging) {
	intest.AssertFunc(func() bool {
		var sortErr error
		isOrdered := slices.IsSortedFunc(buckets, func(i, j *bucket4Merging) int {
			res, err := i.upper.Compare(ctx, j.upper, collate.GetBinaryCollator())
			if err != nil {
				sortErr = err
			}
			if res != 0 {
				return res
			}
			res, err = i.lower.Compare(ctx, j.lower, collate.GetBinaryCollator())
			if err != nil {
				sortErr = err
			}
			return res
		})
		return isOrdered && sortErr == nil
	}, "the buckets are not sorted actually")
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
