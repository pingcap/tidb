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

	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/planner/util/debugtrace"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// Column represents a column histogram.
type Column struct {
	LastAnalyzePos types.Datum
	CMSketch       *CMSketch
	TopN           *TopN
	FMSketch       *FMSketch
	Info           *model.ColumnInfo
	Histogram

	// StatsLoadedStatus indicates the status of column statistics
	StatsLoadedStatus
	PhysicalID int64
	Flag       int64
	StatsVer   int64 // StatsVer is the version of the current stats, used to maintain compatibility

	IsHandle bool
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

// NotNullCount returns the count of this column which is not null.
func (c *Column) NotNullCount() float64 {
	if c.StatsVer >= Version2 {
		return c.Histogram.NotNullCount() + float64(c.TopN.TotalCount())
	}
	return c.Histogram.NotNullCount()
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

// IsInvalid checks if this column is invalid.
// If this column has histogram but not loaded yet,
// then we mark it as need histogram.
func (c *Column) IsInvalid(
	sctx sessionctx.Context,
	collPseudo bool,
) (res bool) {
	var totalCount float64
	var ndv int64
	var inValidForCollPseudo, essentialLoaded bool
	if sctx.GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(sctx)
		defer func() {
			debugtrace.RecordAnyValuesWithNames(sctx,
				"IsInvalid", res,
				"InValidForCollPseudo", inValidForCollPseudo,
				"TotalCount", totalCount,
				"NDV", ndv,
				"EssentialLoaded", essentialLoaded,
			)
			debugtrace.LeaveContextCommon(sctx)
		}()
	}
	if collPseudo {
		inValidForCollPseudo = true
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
	totalCount = c.TotalRowCount()
	essentialLoaded = c.IsEssentialStatsLoaded()
	ndv = c.Histogram.NDV
	return totalCount == 0 || (!essentialLoaded && ndv > 0)
}

// ItemID implements TableCacheItem
func (c *Column) ItemID() int64 {
	return c.Info.ID
}

// DropUnnecessaryData drops the unnecessary data for the column.
func (c *Column) DropUnnecessaryData() {
	if c.StatsVer < Version2 {
		c.CMSketch = nil
	}
	c.TopN = nil
	c.Histogram.Bounds = chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeBlob)}, 0)
	c.Histogram.Buckets = make([]Bucket, 0)
	c.Histogram.Scalars = make([]scalar, 0)
	c.evictedStatus = AllEvicted
}

// IsAllEvicted indicates whether all stats evicted
func (c *Column) IsAllEvicted() bool {
	return c.statsInitialized && c.evictedStatus >= AllEvicted
}

// GetEvictedStatus indicates the evicted status
func (c *Column) GetEvictedStatus() int {
	return c.evictedStatus
}

// IsStatsInitialized indicates whether stats is initialized
func (c *Column) IsStatsInitialized() bool {
	return c.statsInitialized
}

// GetStatsVer indicates the stats version
func (c *Column) GetStatsVer() int64 {
	return c.StatsVer
}

// IsCMSExist indicates whether CMSketch exists
func (c *Column) IsCMSExist() bool {
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

// StatusToString gets the string info of StatsLoadedStatus
func (s StatsLoadedStatus) StatusToString() string {
	if !s.statsInitialized {
		return "unInitialized"
	}
	switch s.evictedStatus {
	case AllLoaded:
		return "allLoaded"
	case AllEvicted:
		return "allEvicted"
	}
	return "unknown"
}

// IsAnalyzed indicates whether the column is analyzed.
// The set of IsAnalyzed columns is a subset of the set of StatsAvailable columns.
func (c *Column) IsAnalyzed() bool {
	return c.GetStatsVer() != Version0
}

// StatsAvailable indicates whether the column stats are collected.
// Note:
//  1. The function merely talks about whether the stats are collected, regardless of the stats loaded status.
//  2. The function is used to decide StatsLoadedStatus.statsInitialized when reading the column stats from storage.
//  3. There are two cases that StatsAvailable is true:
//     a. IsAnalyzed is true.
//     b. The column is newly-added/modified and its stats are generated according to the default value.
func (c *Column) StatsAvailable() bool {
	// Typically, when the column is analyzed, StatsVer is set to Version1/Version2, so we check IsAnalyzed().
	// However, when we add/modify a column, its stats are generated according to the default value without setting
	// StatsVer, so we check NDV > 0 || NullCount > 0 for the case.
	return c.IsAnalyzed() || c.NDV > 0 || c.NullCount > 0
}
