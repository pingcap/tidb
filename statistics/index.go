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
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/planner/util/debugtrace"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/twmb/murmur3"
)

// Index represents an index histogram.
type Index struct {
	LastAnalyzePos types.Datum
	CMSketch       *CMSketch
	TopN           *TopN
	FMSketch       *FMSketch
	Info           *model.IndexInfo
	Histogram
	StatsLoadedStatus
	StatsVer   int64 // StatsVer is the version of the current stats, used to maintain compatibility
	Flag       int64
	PhysicalID int64
}

// Copy copies the index.
func (idx *Index) Copy() *Index {
	if idx == nil {
		return nil
	}
	nc := &Index{
		PhysicalID: idx.PhysicalID,
		Flag:       idx.Flag,
		StatsVer:   idx.StatsVer,
	}
	idx.LastAnalyzePos.Copy(&nc.LastAnalyzePos)
	if idx.CMSketch != nil {
		nc.CMSketch = idx.CMSketch.Copy()
	}
	if idx.TopN != nil {
		nc.TopN = idx.TopN.Copy()
	}
	if idx.FMSketch != nil {
		nc.FMSketch = idx.FMSketch.Copy()
	}
	if idx.Info != nil {
		nc.Info = idx.Info.Clone()
	}
	nc.Histogram = *idx.Histogram.Copy()
	nc.StatsLoadedStatus = idx.StatsLoadedStatus.Copy()
	return nc
}

// ItemID implements TableCacheItem
func (idx *Index) ItemID() int64 {
	return idx.Info.ID
}

// IsAllEvicted indicates whether all stats evicted
func (idx *Index) IsAllEvicted() bool {
	return idx.statsInitialized && idx.evictedStatus >= AllEvicted
}

// GetEvictedStatus returns the evicted status
func (idx *Index) GetEvictedStatus() int {
	return idx.evictedStatus
}

// DropUnnecessaryData drops unnecessary data for index.
func (idx *Index) DropUnnecessaryData() {
	if idx.GetStatsVer() < Version2 {
		idx.CMSketch = nil
	}
	idx.TopN = nil
	idx.Histogram.Bounds = chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeBlob)}, 0)
	idx.Histogram.Buckets = make([]Bucket, 0)
	idx.Histogram.Scalars = make([]scalar, 0)
	idx.evictedStatus = AllEvicted
}

func (idx *Index) isStatsInitialized() bool {
	return idx.statsInitialized
}

// GetStatsVer returns the version of the current stats
func (idx *Index) GetStatsVer() int64 {
	return idx.StatsVer
}

// IsCMSExist returns whether CMSketch exists.
func (idx *Index) IsCMSExist() bool {
	return idx.CMSketch != nil
}

// IsEvicted returns whether index statistics got evicted
func (idx *Index) IsEvicted() bool {
	return idx.evictedStatus != AllLoaded
}

func (idx *Index) String() string {
	return idx.Histogram.ToString(len(idx.Info.Columns))
}

// TotalRowCount returns the total count of this index.
func (idx *Index) TotalRowCount() float64 {
	idx.CheckStats()
	if idx.StatsVer >= Version2 {
		return idx.Histogram.TotalRowCount() + float64(idx.TopN.TotalCount())
	}
	return idx.Histogram.TotalRowCount()
}

// IsInvalid checks if this index is invalid.
func (idx *Index) IsInvalid(sctx sessionctx.Context, collPseudo bool) (res bool) {
	if !collPseudo {
		idx.CheckStats()
	}
	var totalCount float64
	if sctx.GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(sctx)
		defer func() {
			debugtrace.RecordAnyValuesWithNames(sctx,
				"IsInvalid", res,
				"CollPseudo", collPseudo,
				"TotalCount", totalCount,
			)
			debugtrace.LeaveContextCommon(sctx)
		}()
	}
	totalCount = idx.TotalRowCount()
	return (collPseudo) || totalCount == 0
}

// EvictAllStats evicts all stats
// Note that this function is only used for test
func (idx *Index) EvictAllStats() {
	idx.Histogram.Buckets = nil
	idx.CMSketch = nil
	idx.TopN = nil
	idx.StatsLoadedStatus.evictedStatus = AllEvicted
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

// QueryBytes is used to query the count of specified bytes.
// The input sctx is just for debug trace, you can pass nil safely if that's not needed.
func (idx *Index) QueryBytes(sctx sessionctx.Context, d []byte) (result uint64) {
	idx.CheckStats()
	if sctx != nil && sctx.GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(sctx)
		defer func() {
			debugtrace.RecordAnyValuesWithNames(sctx, "Result", result)
			debugtrace.LeaveContextCommon(sctx)
		}()
	}
	h1, h2 := murmur3.Sum128(d)
	if idx.TopN != nil {
		if count, ok := idx.TopN.QueryTopN(sctx, d); ok {
			return count
		}
	}
	if idx.CMSketch != nil {
		return idx.CMSketch.queryHashValue(sctx, h1, h2)
	}
	v, _ := idx.Histogram.EqualRowCount(sctx, types.NewBytesDatum(d), idx.StatsVer >= Version2)
	return uint64(v)
}

// CheckStats will check if the index stats need to be updated.
func (idx *Index) CheckStats() {
	// When we are using stats from PseudoTable(), all column/index ID will be -1.
	if idx.IsFullLoad() || idx.PhysicalID <= 0 {
		return
	}
	HistogramNeededItems.insert(model.TableItemID{TableID: idx.PhysicalID, ID: idx.Info.ID, IsIndex: true})
}

// GetIncreaseFactor get the increase factor to adjust the final estimated count when the table is modified.
func (idx *Index) GetIncreaseFactor(realtimeRowCount int64) float64 {
	columnCount := idx.TotalRowCount()
	if columnCount == 0 {
		return 1.0
	}
	return float64(realtimeRowCount) / columnCount
}

// IsAnalyzed indicates whether the index is analyzed.
func (idx *Index) IsAnalyzed() bool {
	return idx.StatsVer != Version0
}
