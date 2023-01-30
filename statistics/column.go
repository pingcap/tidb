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
	"math"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/ranger"
	"go.uber.org/zap"
)

// Column represents a column histogram.
type Column struct {
	Histogram
	CMSketch   *CMSketch
	TopN       *TopN
	FMSketch   *FMSketch
	PhysicalID int64
	Count      int64
	Info       *model.ColumnInfo
	IsHandle   bool
	ErrorRate
	Flag           int64
	LastAnalyzePos types.Datum
	StatsVer       int64 // StatsVer is the version of the current stats, used to maintain compatibility

	// StatsLoadedStatus indicates the status of column statistics
	StatsLoadedStatus
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
	if c.TopN != nil {
		topnMemUsage := c.TopN.MemoryUsage()
		columnMemUsage.TopNMemUsage = topnMemUsage
		sum += topnMemUsage
	}
	if c.FMSketch != nil {
		fmSketchMemUsage := c.FMSketch.MemoryUsage()
		columnMemUsage.FMSketchMemUsage = fmSketchMemUsage
		sum += fmSketchMemUsage
	}
	columnMemUsage.TotalMemUsage = sum
	return columnMemUsage
}

// HistogramNeededItems stores the columns/indices whose Histograms need to be loaded from physical kv layer.
// Currently, we only load index/pk's Histogram from kv automatically. Columns' are loaded by needs.
var HistogramNeededItems = neededStatsMap{items: map[model.TableItemID]struct{}{}}

// IsInvalid checks if this column is invalid. If this column has histogram but not loaded yet, then we mark it
// as need histogram.
func (c *Column) IsInvalid(sctx sessionctx.Context, collPseudo bool) bool {
	if collPseudo && c.NotAccurate() {
		return true
	}
	if sctx != nil {
		stmtctx := sctx.GetSessionVars().StmtCtx
		if c.IsLoadNeeded() && stmtctx != nil {
			if stmtctx.StatsLoad.Timeout > 0 {
				logutil.BgLogger().Warn("Hist for column should already be loaded as sync but not found.",
					zap.String(strconv.FormatInt(c.Info.ID, 10), c.Info.Name.O))
			}
			// In some tests, the c.Info is not set, so we add this check here.
			if c.Info != nil {
				HistogramNeededItems.insert(model.TableItemID{TableID: c.PhysicalID, ID: c.Info.ID, IsIndex: false})
			}
		}
	}
	// In some cases, some statistics in column would be evicted
	// For example: the cmsketch of the column might be evicted while the histogram and the topn are still exists
	// In this case, we will think this column as valid due to we can still use the rest of the statistics to do optimize.
	return c.TotalRowCount() == 0 || (!c.IsEssentialStatsLoaded() && c.Histogram.NDV > 0)
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
		rowcount, ok := c.TopN.QueryTopN(encodedVal)
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
func (c *Column) GetColumnRowCount(sctx sessionctx.Context, ranges []*ranger.Range, realtimeRowCount, modifyCount int64, pkIsHandle bool) (float64, error) {
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
					rowCount++
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
			cnt += c.Histogram.outOfRangeRowCount(&lowVal, &highVal, modifyCount)
		}

		rowCount += cnt
	}
	rowCount = mathutil.Clamp(rowCount, 0, float64(realtimeRowCount))
	return rowCount, nil
}

// ItemID implements TableCacheItem
func (c *Column) ItemID() int64 {
	return c.Info.ID
}

// DropEvicted implements TableCacheItem
// DropEvicted drops evicted structures
func (c *Column) DropEvicted() {
	if !c.statsInitialized {
		return
	}
	switch c.evictedStatus {
	case allLoaded:
		if c.CMSketch != nil && c.StatsVer < Version2 {
			c.dropCMS()
			return
		}
		// For stats version2, there is no cms thus we directly drop topn
		c.dropTopN()
		return
	case onlyCmsEvicted:
		c.dropTopN()
		return
	default:
		return
	}
}

func (c *Column) dropCMS() {
	c.CMSketch = nil
	c.evictedStatus = onlyCmsEvicted
}

func (c *Column) dropTopN() {
	originTopNNum := int64(c.TopN.Num())
	c.TopN = nil
	if len(c.Histogram.Buckets) == 0 && originTopNNum >= c.Histogram.NDV {
		// This indicates column has topn instead of histogram
		c.evictedStatus = allEvicted
	} else {
		c.evictedStatus = onlyHistRemained
	}
}

func (c *Column) dropHist() {
	c.Histogram.Bounds = chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeBlob)}, 0)
	c.Histogram.Buckets = make([]Bucket, 0)
	c.Histogram.scalars = make([]scalar, 0)
	c.evictedStatus = allEvicted
}

// IsAllEvicted indicates whether all stats evicted
func (c *Column) IsAllEvicted() bool {
	return c.statsInitialized && c.evictedStatus >= allEvicted
}

func (c *Column) getEvictedStatus() int {
	return c.evictedStatus
}

func (c *Column) isStatsInitialized() bool {
	return c.statsInitialized
}

func (c *Column) statsVer() int64 {
	return c.StatsVer
}

func (c *Column) isCMSExist() bool {
	return c.CMSketch != nil
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

// BetweenRowCount estimates the row count for interval [l, r).
func (c *Column) BetweenRowCount(sctx sessionctx.Context, l, r types.Datum, lowEncoded, highEncoded []byte) float64 {
	histBetweenCnt := c.Histogram.BetweenRowCount(l, r)
	if c.StatsVer <= Version1 {
		return histBetweenCnt
	}
	return float64(c.TopN.BetweenCount(lowEncoded, highEncoded)) + histBetweenCnt
}

// StatusToString gets the string info of StatsLoadedStatus
func (s StatsLoadedStatus) StatusToString() string {
	if !s.statsInitialized {
		return "unInitialized"
	}
	switch s.evictedStatus {
	case allLoaded:
		return "allLoaded"
	case onlyCmsEvicted:
		return "onlyCmsEvicted"
	case onlyHistRemained:
		return "onlyHistRemained"
	case allEvicted:
		return "allEvicted"
	}
	return "unknown"
}
