// Copyright 2022 PingCAP, Inc.
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
	"math"
	"strings"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/twmb/murmur3"
	"golang.org/x/exp/slices"
)

// Index represents an index histogram.
type Index struct {
	Histogram
	CMSketch *CMSketch
	TopN     *TopN
	FMSketch *FMSketch
	ErrorRate
	StatsVer       int64 // StatsVer is the version of the current stats, used to maintain compatibility
	Info           *model.IndexInfo
	Flag           int64
	LastAnalyzePos types.Datum
	PhysicalID     int64
	StatsLoadedStatus
}

// ItemID implements TableCacheItem
func (idx *Index) ItemID() int64 {
	return idx.Info.ID
}

// IsAllEvicted indicates whether all stats evicted
func (idx *Index) IsAllEvicted() bool {
	return idx.statsInitialized && idx.evictedStatus >= allEvicted
}

func (idx *Index) dropCMS() {
	idx.CMSketch = nil
	idx.evictedStatus = onlyCmsEvicted
}

func (idx *Index) dropHist() {
	idx.Histogram.Bounds = chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeBlob)}, 0)
	idx.Histogram.Buckets = make([]Bucket, 0)
	idx.Histogram.scalars = make([]scalar, 0)
	idx.evictedStatus = allEvicted
}

func (idx *Index) dropTopN() {
	originTopNNum := int64(idx.TopN.Num())
	idx.TopN = nil
	if len(idx.Histogram.Buckets) == 0 && originTopNNum >= idx.Histogram.NDV {
		// This indicates index has topn instead of histogram
		idx.evictedStatus = allEvicted
	} else {
		idx.evictedStatus = onlyHistRemained
	}
}

func (idx *Index) getEvictedStatus() int {
	return idx.evictedStatus
}

func (idx *Index) isStatsInitialized() bool {
	return idx.statsInitialized
}

func (idx *Index) statsVer() int64 {
	return idx.StatsVer
}

func (idx *Index) isCMSExist() bool {
	return idx.CMSketch != nil
}

// IsEvicted returns whether index statistics got evicted
func (idx *Index) IsEvicted() bool {
	return idx.evictedStatus != allLoaded
}

func (idx *Index) String() string {
	return idx.Histogram.ToString(len(idx.Info.Columns))
}

// TotalRowCount returns the total count of this index.
func (idx *Index) TotalRowCount() float64 {
	idx.checkStats()
	if idx.StatsVer >= Version2 {
		return idx.Histogram.TotalRowCount() + float64(idx.TopN.TotalCount())
	}
	return idx.Histogram.TotalRowCount()
}

// IsInvalid checks if this index is invalid.
func (idx *Index) IsInvalid(collPseudo bool) bool {
	if !collPseudo {
		idx.checkStats()
	}
	return (collPseudo && idx.ErrorRate.NotAccurate()) || idx.TotalRowCount() == 0
}

// EvictAllStats evicts all stats
// Note that this function is only used for test
func (idx *Index) EvictAllStats() {
	idx.Histogram.Buckets = nil
	idx.CMSketch = nil
	idx.TopN = nil
	idx.StatsLoadedStatus.evictedStatus = allEvicted
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
	if idx.TopN != nil {
		topnMemUsage := idx.TopN.MemoryUsage()
		indexMemUsage.TopNMemUsage = topnMemUsage
		sum += topnMemUsage
	}
	indexMemUsage.TotalMemUsage = sum
	return indexMemUsage
}

var nullKeyBytes, _ = codec.EncodeKey(nil, nil, types.NewDatum(nil))

func (idx *Index) equalRowCount(b []byte, realtimeRowCount int64) float64 {
	if len(idx.Info.Columns) == 1 {
		if bytes.Equal(b, nullKeyBytes) {
			return float64(idx.Histogram.NullCount)
		}
	}
	val := types.NewBytesDatum(b)
	if idx.StatsVer < Version2 {
		if idx.Histogram.NDV > 0 && idx.outOfRange(val) {
			return outOfRangeEQSelectivity(idx.Histogram.NDV, realtimeRowCount, int64(idx.TotalRowCount())) * idx.TotalRowCount()
		}
		if idx.CMSketch != nil {
			return float64(idx.QueryBytes(b))
		}
		histRowCount, _ := idx.Histogram.equalRowCount(val, false)
		return histRowCount
	}
	// stats version == 2
	// 1. try to find this value in TopN
	if idx.TopN != nil {
		count, found := idx.TopN.QueryTopN(b)
		if found {
			return float64(count)
		}
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
	idx.checkStats()
	h1, h2 := murmur3.Sum128(d)
	if idx.TopN != nil {
		if count, ok := idx.TopN.QueryTopN(d); ok {
			return count
		}
	}
	if idx.CMSketch != nil {
		return idx.CMSketch.queryHashValue(h1, h2)
	}
	v, _ := idx.Histogram.equalRowCount(types.NewBytesDatum(d), idx.StatsVer >= Version2)
	return uint64(v)
}

// GetRowCount returns the row count of the given ranges.
// It uses the modifyCount to adjust the influence of modifications on the table.
func (idx *Index) GetRowCount(sctx sessionctx.Context, coll *HistColl, indexRanges []*ranger.Range, realtimeRowCount, modifyCount int64) (float64, error) {
	idx.checkStats()
	sc := sctx.GetSessionVars().StmtCtx
	totalCount := float64(0)
	isSingleCol := len(idx.Info.Columns) == 1
	for _, indexRange := range indexRanges {
		var count float64
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
					totalCount++
					continue
				}
				count = idx.equalRowCount(lb, realtimeRowCount)
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
			count += float64(idx.Histogram.NullCount)
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
				if idx.Histogram.Len() > 0 {
					_, lowerBkt, _, _ := idx.Histogram.locateBucket(l)
					_, upperBkt, _, _ := idx.Histogram.locateBucket(r)
					// Use Count of the Bucket before l as the lower bound.
					preCount := float64(0)
					if lowerBkt > 0 {
						preCount = float64(idx.Histogram.Buckets[lowerBkt-1].Count)
					}
					// Use Count of the Bucket where r exists as the upper bound.
					upperCnt := float64(idx.Histogram.Buckets[upperBkt].Count)
					upperLimit = upperCnt - preCount
					upperLimit += float64(idx.TopN.BetweenCount(lb, rb))
				}

				// If the result of exponential backoff strategy is larger than the result from multi-column stats,
				// 	use the upper limit from multi-column histogram instead.
				if expBackoffCnt > upperLimit {
					expBackoffCnt = upperLimit
				}
				count += expBackoffCnt
			}
		}
		if !expBackoffSuccess {
			count += idx.BetweenRowCount(l, r)
		}

		// If the current table row count has changed, we should scale the row count accordingly.
		count *= idx.GetIncreaseFactor(realtimeRowCount)

		// handling the out-of-range part
		if (idx.outOfRange(l) && !(isSingleCol && lowIsNull)) || idx.outOfRange(r) {
			count += idx.Histogram.outOfRangeRowCount(&l, &r, modifyCount)
		}
		totalCount += count
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
	colsIDs := coll.Idx2ColumnIDs[idx.Histogram.ID]
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
			count      float64
			err        error
			foundStats bool
		)
		if col, ok := coll.Columns[colID]; ok && !col.IsInvalid(sctx, coll.Pseudo) {
			foundStats = true
			count, err = coll.GetRowCountByColumnRanges(sctx, colID, tmpRan)
		}
		if idxIDs, ok := coll.ColID2IdxIDs[colID]; ok && !foundStats && len(indexRange.LowVal) > 1 {
			// Note the `len(indexRange.LowVal) > 1` condition here, it means we only recursively call
			// `GetRowCountByIndexRanges()` when the input `indexRange` is a multi-column range. This
			// check avoids infinite recursion.
			for _, idxID := range idxIDs {
				if idxID == idx.Histogram.ID {
					continue
				}
				foundStats = true
				count, err = coll.GetRowCountByIndexRanges(sctx, idxID, tmpRan)
				if err == nil {
					break
				}
			}
		}
		if !foundStats {
			continue
		}
		if err != nil {
			return 0, false, err
		}
		singleColumnEstResults = append(singleColumnEstResults, count)
	}
	// Sort them.
	slices.Sort(singleColumnEstResults)
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

func (idx *Index) checkStats() {
	if idx.IsFullLoad() {
		return
	}
	HistogramNeededItems.insert(model.TableItemID{TableID: idx.PhysicalID, ID: idx.Info.ID, IsIndex: true})
}

func (idx *Index) newIndexBySelectivity(sc *stmtctx.StatementContext, statsNode *StatsNode) (*Index, error) {
	var (
		ranLowEncode, ranHighEncode []byte
		err                         error
	)
	newIndexHist := &Index{Info: idx.Info, StatsVer: idx.StatsVer, CMSketch: idx.CMSketch, PhysicalID: idx.PhysicalID}
	newIndexHist.Histogram = *NewHistogram(idx.Histogram.ID, int64(float64(idx.Histogram.NDV)*statsNode.Selectivity), 0, 0, types.NewFieldType(mysql.TypeBlob), chunk.InitialCapacity, 0)

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
		for ; highBucketIdx < idx.Histogram.Len(); highBucketIdx++ {
			// Encoded value can only go to its next quickly. So ranHighEncode is actually range.HighVal's PrefixNext value.
			// So the Bound should also go to its PrefixNext.
			bucketLowerEncoded := idx.Histogram.Bounds.GetRow(highBucketIdx * 2).GetBytes(0)
			if bytes.Compare(ranHighEncode, kv.Key(bucketLowerEncoded).PrefixNext()) < 0 {
				break
			}
		}
		for ; lowBucketIdx < highBucketIdx; lowBucketIdx++ {
			bucketUpperEncoded := idx.Histogram.Bounds.GetRow(lowBucketIdx*2 + 1).GetBytes(0)
			if bytes.Compare(ranLowEncode, bucketUpperEncoded) <= 0 {
				break
			}
		}
		if lowBucketIdx >= idx.Histogram.Len() {
			break
		}
		for i := lowBucketIdx; i < highBucketIdx; i++ {
			newIndexHist.Histogram.Bounds.AppendRow(idx.Histogram.Bounds.GetRow(i * 2))
			newIndexHist.Histogram.Bounds.AppendRow(idx.Histogram.Bounds.GetRow(i*2 + 1))
			totCnt += idx.Histogram.bucketCount(i)
			newIndexHist.Histogram.Buckets = append(newIndexHist.Histogram.Buckets, Bucket{Repeat: idx.Histogram.Buckets[i].Repeat, Count: totCnt})
			newIndexHist.Histogram.scalars = append(newIndexHist.Histogram.scalars, idx.Histogram.scalars[i])
		}
	}
	return newIndexHist, nil
}

func (idx *Index) outOfRange(val types.Datum) bool {
	if !idx.Histogram.outOfRange(val) {
		return false
	}
	if idx.Histogram.Len() > 0 && matchPrefix(idx.Histogram.Bounds.GetRow(0), 0, &val) {
		return false
	}
	return true
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

// matchPrefix checks whether ad is the prefix of value
func matchPrefix(row chunk.Row, colIdx int, ad *types.Datum) bool {
	switch ad.Kind() {
	case types.KindString, types.KindBytes, types.KindBinaryLiteral, types.KindMysqlBit:
		return strings.HasPrefix(row.GetString(colIdx), ad.GetString())
	}
	return false
}
