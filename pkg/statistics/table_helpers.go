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
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strings"

	"go.uber.org/atomic"
)

// CopyAs creates a copy of the table with the specified writability intent.
//
// PERFORMANCE NOTE: Choose the most minimal intent for your use case. Copying is heavily
// used at scale and unnecessary cloning causes significant memory pressure. Only use
// AllDataWritable when you truly need to modify histogram data.
//
// MetaOnly: Shares all maps, only metadata modifications are safe
// ColumnMapWritable: Clones columns map, safe to add/remove columns
// IndexMapWritable: Clones indices map, safe to add/remove indices
// BothMapsWritable: Clones both maps - safe to add/remove columns and indices
// ExtendedStatsWritable: Shares all maps, safe to modify ExtendedStats field
// AllDataWritable: Deep copies everything, safe to modify all data including histograms
func (t *Table) CopyAs(intent CopyIntent) *Table {
	var columns map[int64]*Column
	var indices map[int64]*Index
	var existenceMap *ColAndIdxExistenceMap

	switch intent {
	case MetaOnly:
		columns = t.columns
		indices = t.indices
		existenceMap = t.ColAndIdxExistenceMap
	case ColumnMapWritable:
		columns = maps.Clone(t.columns)
		indices = t.indices
		if t.ColAndIdxExistenceMap != nil {
			existenceMap = t.ColAndIdxExistenceMap.Clone()
		}
	case IndexMapWritable:
		columns = t.columns
		indices = maps.Clone(t.indices)
		if t.ColAndIdxExistenceMap != nil {
			existenceMap = t.ColAndIdxExistenceMap.Clone()
		}
	case BothMapsWritable:
		columns = maps.Clone(t.columns)
		indices = maps.Clone(t.indices)
		if t.ColAndIdxExistenceMap != nil {
			existenceMap = t.ColAndIdxExistenceMap.Clone()
		}
	case ExtendedStatsWritable:
		columns = t.columns
		indices = t.indices
		existenceMap = t.ColAndIdxExistenceMap
	case AllDataWritable:
		// For deep copy, create new maps and deep copy all content
		columns = make(map[int64]*Column, len(t.columns))
		for id, col := range t.columns {
			columns[id] = col.Copy()
		}
		indices = make(map[int64]*Index, len(t.indices))
		for id, idx := range t.indices {
			indices[id] = idx.Copy()
		}
		if t.ColAndIdxExistenceMap != nil {
			existenceMap = t.ColAndIdxExistenceMap.Clone()
		}
	}

	newHistColl := HistColl{
		PhysicalID:    t.PhysicalID,
		RealtimeCount: t.RealtimeCount,
		columns:       columns,
		indices:       indices,
		Pseudo:        t.Pseudo,
		ModifyCount:   t.ModifyCount,
		StatsVer:      t.StatsVer,
	}
	nt := &Table{
		HistColl:              newHistColl,
		Version:               t.Version,
		TblInfoUpdateTS:       t.TblInfoUpdateTS,
		ColAndIdxExistenceMap: existenceMap,
		LastAnalyzeVersion:    t.LastAnalyzeVersion,
		LastStatsHistVersion:  t.LastStatsHistVersion,
	}

	// Handle ExtendedStats for deep copy vs shallow copy
	if (intent == AllDataWritable || intent == ExtendedStatsWritable) && t.ExtendedStats != nil {
		newExtStatsColl := &ExtendedStatsColl{
			Stats:             make(map[string]*ExtendedStatsItem),
			LastUpdateVersion: t.ExtendedStats.LastUpdateVersion,
		}
		maps.Copy(newExtStatsColl.Stats, t.ExtendedStats.Stats)
		nt.ExtendedStats = newExtStatsColl
	} else {
		nt.ExtendedStats = t.ExtendedStats
	}

	return nt
}

// String implements Stringer interface.
func (t *Table) String() string {
	strs := make([]string, 0, len(t.columns)+1)
	strs = append(strs, fmt.Sprintf("Table:%d RealtimeCount:%d", t.PhysicalID, t.RealtimeCount))
	cols := make([]*Column, 0, len(t.columns))
	for _, col := range t.columns {
		cols = append(cols, col)
	}
	slices.SortFunc(cols, func(i, j *Column) int { return cmp.Compare(i.ID, j.ID) })
	for _, col := range cols {
		strs = append(strs, col.String())
	}
	idxs := make([]*Index, 0, len(t.indices))
	for _, idx := range t.indices {
		idxs = append(idxs, idx)
	}
	slices.SortFunc(idxs, func(i, j *Index) int { return cmp.Compare(i.ID, j.ID) })
	for _, idx := range idxs {
		strs = append(strs, idx.String())
	}
	// TODO: concat content of ExtendedStatsColl
	return strings.Join(strs, "\n")
}

// IndexStartWithColumn finds the first index whose first column is the given column.
func (t *Table) IndexStartWithColumn(colName string) *Index {
	for _, index := range t.indices {
		if index.Info.Columns[0].Name.L == colName {
			return index
		}
	}
	return nil
}

// ColumnByName finds the statistics.Column for the given column.
func (t *Table) ColumnByName(colName string) *Column {
	for _, c := range t.columns {
		if c.Info.Name.L == colName {
			return c
		}
	}
	return nil
}

// GetStatsInfo returns their statistics according to the ID of the column or index, including histogram, CMSketch, TopN and FMSketch.
//
//	needCopy: In order to protect the item in the cache from being damaged, we need to copy the item.
func (t *Table) GetStatsInfo(id int64, isIndex bool, needCopy bool) (*Histogram, *CMSketch, *TopN, *FMSketch, bool) {
	if isIndex {
		if idxStatsInfo, ok := t.indices[id]; ok {
			if needCopy {
				return idxStatsInfo.Histogram.Copy(),
					idxStatsInfo.CMSketch.Copy(), idxStatsInfo.TopN.Copy(), idxStatsInfo.FMSketch.Copy(), true
			}
			return &idxStatsInfo.Histogram,
				idxStatsInfo.CMSketch, idxStatsInfo.TopN, idxStatsInfo.FMSketch, true
		}
		// newly added index which is not analyzed yet
		return nil, nil, nil, nil, false
	}
	if colStatsInfo, ok := t.columns[id]; ok {
		if needCopy {
			return colStatsInfo.Histogram.Copy(), colStatsInfo.CMSketch.Copy(),
				colStatsInfo.TopN.Copy(), colStatsInfo.FMSketch.Copy(), true
		}
		return &colStatsInfo.Histogram, colStatsInfo.CMSketch,
			colStatsInfo.TopN, colStatsInfo.FMSketch, true
	}
	// newly added column which is not analyzed yet
	return nil, nil, nil, nil, false
}

// IsAnalyzed checks whether the table is analyzed or not by checking its last analyze's timestamp value.
// A valid timestamp must be greater than 0.
func (t *Table) IsAnalyzed() bool {
	return t.LastAnalyzeVersion > 0
}

// IsEligibleForAnalysis checks whether the table is eligible for analysis.
func (t *Table) IsEligibleForAnalysis() bool {
	// 1. If the statistics are either not loaded or are classified as pseudo, there is no need for analyze.
	//    Pseudo statistics can be created by the optimizer, so we need to double check it.
	// 2. If the table is too small, we don't want to waste time to analyze it.
	//    Leave the opportunity to other bigger tables.
	if !t.MeetAutoAnalyzeMinCnt() || t.Pseudo {
		return false
	}

	return true
}

// MeetAutoAnalyzeMinCnt checks whether the table meets the minimum count required for auto-analyze.
func (t *Table) MeetAutoAnalyzeMinCnt() bool {
	return t != nil && t.RealtimeCount >= AutoAnalyzeMinCnt
}

// GetAnalyzeRowCount tries to get the row count of a column or an index if possible.
// This method is useful because this row count doesn't consider the modify count.
func (coll *HistColl) GetAnalyzeRowCount() float64 {
	ids := slices.Collect(maps.Keys(coll.columns))
	slices.Sort(ids)
	for _, id := range ids {
		col := coll.columns[id]
		if col != nil && col.IsFullLoad() {
			return col.TotalRowCount()
		}
	}
	clear(ids)
	ids = slices.Grow(ids, len(coll.indices))
	ids = slices.AppendSeq(ids, maps.Keys(coll.indices))
	slices.Sort(ids)
	for _, id := range ids {
		idx := coll.indices[id]
		if idx == nil {
			continue
		}
		if idx.Info != nil && idx.Info.MVIndex {
			continue
		}
		if idx.IsFullLoad() {
			return idx.TotalRowCount()
		}
	}
	return -1
}

// GetScaledRealtimeAndModifyCnt scale the RealtimeCount and ModifyCount for some special indexes where the total row
// count is different from the total row count of the table. Currently, only the mv index is this case.
// Because we will use the RealtimeCount and ModifyCount during the estimation for ranges on this index (like the upper
// bound for the out-of-range estimation logic and the IncreaseFactor logic), we can't directly use the RealtimeCount and
// ModifyCount of the table. Instead, we should scale them before using.
// For example, if the table analyze row count is 1000 and realtime row count is 1500, and the mv index total count is 5000,
// when calculating the IncreaseFactor, it should be 1500/1000 = 1.5 for normal columns/indexes, and we should use the
// same 1.5 for mv index. But obviously, use 1500/5000 would be wrong, the correct calculation should be 7500/5000 = 1.5.
// So we add this function to get this 7500.
func (coll *HistColl) GetScaledRealtimeAndModifyCnt(idxStats *Index) (realtimeCnt, modifyCnt int64) {
	// In theory, we can apply this scale logic on all indexes. But currently, we only apply it on the mv index to avoid
	// any unexpected changes caused by factors like precision difference.
	if idxStats == nil || idxStats.Info == nil || !idxStats.Info.MVIndex || !idxStats.IsFullLoad() {
		return coll.RealtimeCount, coll.ModifyCount
	}
	analyzeRowCount := coll.GetAnalyzeRowCount()
	if analyzeRowCount <= 0 {
		return coll.RealtimeCount, coll.ModifyCount
	}
	idxTotalRowCount := idxStats.TotalRowCount()
	if idxTotalRowCount <= 0 {
		return coll.RealtimeCount, coll.ModifyCount
	}
	scale := idxTotalRowCount / analyzeRowCount
	return int64(float64(coll.RealtimeCount) * scale), int64(float64(coll.ModifyCount) * scale)
}

// GetStatsHealthy calculates stats healthy if the table stats is not pseudo.
// If the table stats is pseudo, it returns 0, false, otherwise it returns stats healthy, true.
func (t *Table) GetStatsHealthy() (int64, bool) {
	if t == nil || t.Pseudo {
		return 0, false
	}
	if !t.IsAnalyzed() {
		return 0, true
	}
	var healthy int64
	count := float64(t.RealtimeCount)
	if histCount := t.GetAnalyzeRowCount(); histCount > 0 {
		count = histCount
	}
	if float64(t.ModifyCount) < count {
		healthy = int64((1.0 - float64(t.ModifyCount)/count) * 100.0)
	} else if t.ModifyCount == 0 {
		healthy = 100
	}
	return healthy, true
}

// ColumnIsLoadNeeded checks whether the column needs trigger the async/sync load.
// The Column should be visible in the table and really has analyzed statistics in the storage.
// Also, if the stats has been loaded into the memory, we also don't need to load it.
// We return the Column together with the checking result, to avoid accessing the map multiple times.
// The first bool is whether we need to load it into memory. The second bool is whether this column has stats in the system table or not.
func (t *Table) ColumnIsLoadNeeded(id int64, fullLoad bool) (col *Column, loadNeeded, hasAnalyzed bool) {
	if t.Pseudo {
		return nil, false, false
	}
	hasAnalyzed = t.ColAndIdxExistenceMap.HasAnalyzed(id, false)
	col, ok := t.columns[id]
	if !ok {
		// If The column have no stats object in memory. We need to check it by existence map.
		// If existence map says it even has no unitialized record in storage, we don't need to do anything. => Has=false, HasAnalyzed=false
		// If existence map says it has analyzed stats, we need to load it from storage. => Has=true, HasAnalyzed=true
		// If existence map says it has no analyzed stats but have a uninitialized record in storage, we need to also create a fake object. => Has=true, HasAnalyzed=false
		return nil, t.ColAndIdxExistenceMap.Has(id, false), hasAnalyzed
	}

	// If it's not analyzed yet.
	// The real check condition: !ok && !hashAnalyzed.(Has must be true since we've have the memory object so we should have the storage object)
	// After this check, we will always have ok && hasAnalyzed.
	if !hasAnalyzed {
		return nil, false, false
	}

	// Restore the condition from the simplified form:
	// 1. ok && hasAnalyzed && fullLoad && !col.IsFullLoad => need load
	// 2. ok && hasAnalyzed && !fullLoad && !col.statsInitialized => need load
	if (fullLoad && !col.IsFullLoad()) || (!fullLoad && !col.statsInitialized) {
		return col, true, true
	}

	// Otherwise don't need load it.
	return col, false, true
}

// IndexIsLoadNeeded checks whether the index needs trigger the async/sync load.
// The Index should be visible in the table and really has analyzed statistics in the storage.
// Also, if the stats has been loaded into the memory, we also don't need to load it.
// We return the Index together with the checking result, to avoid accessing the map multiple times.
func (t *Table) IndexIsLoadNeeded(id int64) (*Index, bool) {
	idx, ok := t.indices[id]
	// If the index is not in the memory, and we have its stats in the storage. We need to trigger the load.
	if !ok && t.ColAndIdxExistenceMap.HasAnalyzed(id, true) {
		return nil, true
	}
	// If the index is in the memory, we check its embedded func.
	if ok && idx.IsAnalyzed() && !idx.IsFullLoad() {
		return idx, true
	}
	return idx, false
}

// RatioOfPseudoEstimate means if modifyCount / statsTblCount is greater than this ratio, we think the stats is invalid
// and use pseudo estimation.
var RatioOfPseudoEstimate = atomic.NewFloat64(0.7)

// IsInitialized returns true if any column/index stats of the table is initialized.
func (t *Table) IsInitialized() bool {
	for _, col := range t.columns {
		if col != nil && col.IsStatsInitialized() {
			return true
		}
	}
	for _, idx := range t.indices {
		if idx != nil && idx.IsStatsInitialized() {
			return true
		}
	}
	return false
}

// IsOutdated returns true if the table stats is outdated.
func (t *Table) IsOutdated() bool {
	rowcount := t.GetAnalyzeRowCount()
	if rowcount < 0 {
		rowcount = float64(t.RealtimeCount)
	}
	if rowcount > 0 && float64(t.ModifyCount)/rowcount > RatioOfPseudoEstimate.Load() {
		return true
	}
	return false
}

// ReleaseAndPutToPool releases data structures of Table and put itself back to pool.
func (t *Table) ReleaseAndPutToPool() {
	for _, col := range t.columns {
		col.FMSketch.DestroyAndPutToPool()
	}
	clear(t.columns)
	for _, idx := range t.indices {
		idx.FMSketch.DestroyAndPutToPool()
	}
	clear(t.indices)
}

