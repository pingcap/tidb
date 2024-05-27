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
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/context"
	"github.com/pingcap/tidb/pkg/planner/util/debugtrace"
	"github.com/pingcap/tidb/pkg/statistics/asyncload"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
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
	StatsVer int64 // StatsVer is the version of the current stats, used to maintain compatibility
	Flag     int64
	// PhysicalID is the physical table id,
	// or it could possibly be -1, which means "stats not available".
	// The -1 case could happen in a pseudo stats table, and in this case, this stats should not trigger stats loading.
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
	return idx == nil || (idx.statsInitialized && idx.evictedStatus >= AllEvicted)
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
	if idx.StatsVer >= Version2 {
		return idx.Histogram.TotalRowCount() + float64(idx.TopN.TotalCount())
	}
	return idx.Histogram.TotalRowCount()
}

// IndexStatsIsInvalid checks whether the index has valid stats or not.
func IndexStatsIsInvalid(sctx context.PlanContext, idxStats *Index, coll *HistColl, cid int64) (res bool) {
	var totalCount float64
	if sctx.GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(sctx)
		defer func() {
			debugtrace.RecordAnyValuesWithNames(sctx,
				"IsInvalid", res,
				"CollPseudo", coll.Pseudo,
				"TotalCount", totalCount,
			)
			debugtrace.LeaveContextCommon(sctx)
		}()
	}
	// If the given index statistics is nil or we found that the index's statistics hasn't been fully loaded, we add this index to NeededItems.
	// Also, we need to check that this HistColl has its physical ID and it is permitted to trigger the stats loading.
	if (idxStats == nil || !idxStats.IsFullLoad()) && !coll.CanNotTriggerLoad {
		asyncload.AsyncLoadHistogramNeededItems.Insert(model.TableItemID{
			TableID:          coll.PhysicalID,
			ID:               cid,
			IsIndex:          true,
			IsSyncLoadFailed: sctx.GetSessionVars().StmtCtx.StatsLoad.Timeout > 0,
		}, true)
		// TODO: we can return true here. But need to fix some tests first.
	}
	if idxStats == nil {
		return true
	}
	totalCount = idxStats.TotalRowCount()
	return coll.Pseudo || totalCount == 0
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
func (idx *Index) QueryBytes(sctx context.PlanContext, d []byte) (result uint64) {
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
