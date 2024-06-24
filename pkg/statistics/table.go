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
	"slices"
	"strings"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/context"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"go.uber.org/atomic"
	"golang.org/x/exp/maps"
)

const (
	// PseudoVersion means the pseudo statistics version is 0.
	PseudoVersion uint64 = 0

	// PseudoRowCount export for other pkg to use.
	// When we haven't analyzed a table, we use pseudo statistics to estimate costs.
	// It has row count 10000, equal condition selects 1/1000 of total rows, less condition selects 1/3 of total rows,
	// between condition selects 1/40 of total rows.
	PseudoRowCount = 10000
)

var (
	// Below functions are used to solve cycle import problem.
	// Note: all functions below will be removed after finishing moving all estimation functions into the cardinality package.

	// GetRowCountByIndexRanges is a function type to get row count by index ranges.
	GetRowCountByIndexRanges func(sctx context.PlanContext, coll *HistColl, idxID int64, indexRanges []*ranger.Range) (result float64, err error)

	// GetRowCountByIntColumnRanges is a function type to get row count by int column ranges.
	GetRowCountByIntColumnRanges func(sctx context.PlanContext, coll *HistColl, colID int64, intRanges []*ranger.Range) (result float64, err error)

	// GetRowCountByColumnRanges is a function type to get row count by column ranges.
	GetRowCountByColumnRanges func(sctx context.PlanContext, coll *HistColl, colID int64, colRanges []*ranger.Range) (result float64, err error)
)

// Table represents statistics for a table.
type Table struct {
	ExtendedStats *ExtendedStatsColl

	ColAndIdxExistenceMap *ColAndIdxExistenceMap
	HistColl
	Version uint64
	// It's the timestamp of the last analyze time.
	LastAnalyzeVersion uint64
	// TblInfoUpdateTS is the UpdateTS of the TableInfo used when filling this struct.
	// It is the schema version of the corresponding table. It is used to skip redundant
	// loading of stats, i.e, if the cached stats is already update-to-date with mysql.stats_xxx tables,
	// and the schema of the table does not change, we don't need to load the stats for this
	// table again.
	TblInfoUpdateTS uint64

	IsPkIsHandle bool
}

// ColAndIdxExistenceMap is the meta map for statistics.Table.
// It can tell whether a column/index really has its statistics. So we won't send useless kv request when we do online stats loading.
type ColAndIdxExistenceMap struct {
	colInfoMap  map[int64]*model.ColumnInfo
	colAnalyzed map[int64]bool
	idxInfoMap  map[int64]*model.IndexInfo
	idxAnalyzed map[int64]bool
}

// SomeAnalyzed checks whether some part of the table is analyzed.
// The newly added column/index might not have its stats.
func (m *ColAndIdxExistenceMap) SomeAnalyzed() bool {
	if m == nil {
		return false
	}
	for _, v := range m.colAnalyzed {
		if v {
			return true
		}
	}
	for _, v := range m.idxAnalyzed {
		if v {
			return true
		}
	}
	return false
}

// Has checks whether a column/index stats exists.
// This method only checks whether the given item exists or not.
// Don't check whether it has statistics or not.
func (m *ColAndIdxExistenceMap) Has(id int64, isIndex bool) bool {
	if isIndex {
		_, ok := m.idxInfoMap[id]
		return ok
	}
	_, ok := m.colInfoMap[id]
	return ok
}

// HasAnalyzed checks whether a column/index stats exists and it has stats.
// TODO: the map should only keep the analyzed cols.
// There's three possible status of column/index's statistics:
//  1. We don't have this column/index.
//  2. We have it, but it hasn't been analyzed yet.
//  3. We have it and its statistics.
//
// To figure out three status, we use HasAnalyzed's TRUE value to represents the status 3. The Has's FALSE to represents the status 1.
func (m *ColAndIdxExistenceMap) HasAnalyzed(id int64, isIndex bool) bool {
	if isIndex {
		analyzed, ok := m.idxAnalyzed[id]
		return ok && analyzed
	}
	analyzed, ok := m.colAnalyzed[id]
	return ok && analyzed
}

// InsertCol inserts a column with its meta into the map.
func (m *ColAndIdxExistenceMap) InsertCol(id int64, info *model.ColumnInfo, analyzed bool) {
	m.colInfoMap[id] = info
	m.colAnalyzed[id] = analyzed
}

// GetCol gets the meta data of the given column.
func (m *ColAndIdxExistenceMap) GetCol(id int64) *model.ColumnInfo {
	return m.colInfoMap[id]
}

// InsertIndex inserts an index with its meta into the map.
func (m *ColAndIdxExistenceMap) InsertIndex(id int64, info *model.IndexInfo, analyzed bool) {
	m.idxInfoMap[id] = info
	m.idxAnalyzed[id] = analyzed
}

// GetIndex gets the meta data of the given index.
func (m *ColAndIdxExistenceMap) GetIndex(id int64) *model.IndexInfo {
	return m.idxInfoMap[id]
}

// IsEmpty checks whether the map is empty.
func (m *ColAndIdxExistenceMap) IsEmpty() bool {
	return len(m.colInfoMap)+len(m.idxInfoMap) == 0
}

// Clone deeply copies the map.
func (m *ColAndIdxExistenceMap) Clone() *ColAndIdxExistenceMap {
	mm := NewColAndIndexExistenceMap(len(m.colInfoMap), len(m.idxInfoMap))
	mm.colInfoMap = maps.Clone(m.colInfoMap)
	mm.colAnalyzed = maps.Clone(m.colAnalyzed)
	mm.idxAnalyzed = maps.Clone(m.idxAnalyzed)
	mm.idxInfoMap = maps.Clone(m.idxInfoMap)
	return mm
}

// NewColAndIndexExistenceMap return a new object with the given capcity.
func NewColAndIndexExistenceMap(colCap, idxCap int) *ColAndIdxExistenceMap {
	return &ColAndIdxExistenceMap{
		colInfoMap:  make(map[int64]*model.ColumnInfo, colCap),
		colAnalyzed: make(map[int64]bool, colCap),
		idxInfoMap:  make(map[int64]*model.IndexInfo, idxCap),
		idxAnalyzed: make(map[int64]bool, idxCap),
	}
}

// ColAndIdxExistenceMapIsEqual is used in testing, checking whether the two are equal.
func ColAndIdxExistenceMapIsEqual(m1, m2 *ColAndIdxExistenceMap) bool {
	return maps.Equal(m1.colAnalyzed, m2.colAnalyzed) && maps.Equal(m1.idxAnalyzed, m2.idxAnalyzed)
}

// ExtendedStatsItem is the cached item of a mysql.stats_extended record.
type ExtendedStatsItem struct {
	StringVals string
	ColIDs     []int64
	ScalarVals float64
	Tp         uint8
}

// ExtendedStatsColl is a collection of cached items for mysql.stats_extended records.
type ExtendedStatsColl struct {
	Stats             map[string]*ExtendedStatsItem
	LastUpdateVersion uint64
}

// NewExtendedStatsColl allocate an ExtendedStatsColl struct.
func NewExtendedStatsColl() *ExtendedStatsColl {
	return &ExtendedStatsColl{Stats: make(map[string]*ExtendedStatsItem)}
}

const (
	// ExtendedStatsInited is the status for extended stats which are just registered but have not been analyzed yet.
	ExtendedStatsInited uint8 = iota
	// ExtendedStatsAnalyzed is the status for extended stats which have been collected in analyze.
	ExtendedStatsAnalyzed
	// ExtendedStatsDeleted is the status for extended stats which were dropped. These "deleted" records would be removed from storage by GCStats().
	ExtendedStatsDeleted
)

// HistColl is a collection of histogram. It collects enough information for plan to calculate the selectivity.
type HistColl struct {
	// Note that when used in a query, Column use UniqueID as the key while Indices use the index ID in the
	// metadata. (See GenerateHistCollFromColumnInfo() for details)
	Columns    map[int64]*Column
	Indices    map[int64]*Index
	PhysicalID int64
	// TODO: add AnalyzeCount here
	RealtimeCount int64 // RealtimeCount is the current table row count, maintained by applying stats delta based on AnalyzeCount.
	ModifyCount   int64 // Total modify count in a table.

	// The version of the statistics, refer to Version0, Version1, Version2 and so on.
	StatsVer int
	// HavePhysicalID is true means this HistColl is from single table and have its ID's information.
	// The physical id is used when try to load column stats from storage.
	HavePhysicalID bool
	Pseudo         bool

	/*
		Fields below are only used in a query, like for estimation, and they will be useless when stored in
		the stats cache. (See GenerateHistCollFromColumnInfo() for details)
	*/

	CanNotTriggerLoad bool
	// Idx2ColUniqueIDs maps the index id to its column UniqueIDs. It's used to calculate the selectivity in planner.
	Idx2ColUniqueIDs map[int64][]int64
	// ColUniqueID2IdxIDs maps the column UniqueID to a list index ids whose first column is it.
	// It's used to calculate the selectivity in planner.
	ColUniqueID2IdxIDs map[int64][]int64
	// UniqueID2colInfoID maps the column UniqueID to its ID in the metadata.
	UniqueID2colInfoID map[int64]int64
	// MVIdx2Columns maps the index id to its columns by expression.Column.
	// For normal index, the column id is enough, as we already have in Idx2ColUniqueIDs. But currently, mv index needs more
	// information to match the filter against the mv index columns, and we need this map to provide this information.
	MVIdx2Columns map[int64][]*expression.Column
}

// TableMemoryUsage records tbl memory usage
type TableMemoryUsage struct {
	ColumnsMemUsage map[int64]CacheItemMemoryUsage
	IndicesMemUsage map[int64]CacheItemMemoryUsage
	TableID         int64
	TotalMemUsage   int64
}

// TotalIdxTrackingMemUsage returns total indices' tracking memory usage
func (t *TableMemoryUsage) TotalIdxTrackingMemUsage() (sum int64) {
	for _, idx := range t.IndicesMemUsage {
		sum += idx.TrackingMemUsage()
	}
	return sum
}

// TotalColTrackingMemUsage returns total columns' tracking memory usage
func (t *TableMemoryUsage) TotalColTrackingMemUsage() (sum int64) {
	for _, col := range t.ColumnsMemUsage {
		sum += col.TrackingMemUsage()
	}
	return sum
}

// TotalTrackingMemUsage return total tracking memory usage
func (t *TableMemoryUsage) TotalTrackingMemUsage() int64 {
	return t.TotalIdxTrackingMemUsage() + t.TotalColTrackingMemUsage()
}

// TableCacheItem indicates the unit item stored in statsCache, eg: Column/Index
type TableCacheItem interface {
	ItemID() int64
	MemoryUsage() CacheItemMemoryUsage
	IsAllEvicted() bool
	GetEvictedStatus() int

	DropUnnecessaryData()
	IsStatsInitialized() bool
	GetStatsVer() int64
}

// CacheItemMemoryUsage indicates the memory usage of TableCacheItem
type CacheItemMemoryUsage interface {
	ItemID() int64
	TotalMemoryUsage() int64
	TrackingMemUsage() int64
	HistMemUsage() int64
	TopnMemUsage() int64
	CMSMemUsage() int64
}

// ColumnMemUsage records column memory usage
type ColumnMemUsage struct {
	ColumnID          int64
	HistogramMemUsage int64
	CMSketchMemUsage  int64
	FMSketchMemUsage  int64
	TopNMemUsage      int64
	TotalMemUsage     int64
}

// TotalMemoryUsage implements CacheItemMemoryUsage
func (c *ColumnMemUsage) TotalMemoryUsage() int64 {
	return c.TotalMemUsage
}

// ItemID implements CacheItemMemoryUsage
func (c *ColumnMemUsage) ItemID() int64 {
	return c.ColumnID
}

// TrackingMemUsage implements CacheItemMemoryUsage
func (c *ColumnMemUsage) TrackingMemUsage() int64 {
	return c.CMSketchMemUsage + c.TopNMemUsage + c.HistogramMemUsage
}

// HistMemUsage implements CacheItemMemoryUsage
func (c *ColumnMemUsage) HistMemUsage() int64 {
	return c.HistogramMemUsage
}

// TopnMemUsage implements CacheItemMemoryUsage
func (c *ColumnMemUsage) TopnMemUsage() int64 {
	return c.TopNMemUsage
}

// CMSMemUsage implements CacheItemMemoryUsage
func (c *ColumnMemUsage) CMSMemUsage() int64 {
	return c.CMSketchMemUsage
}

// IndexMemUsage records index memory usage
type IndexMemUsage struct {
	IndexID           int64
	HistogramMemUsage int64
	CMSketchMemUsage  int64
	TopNMemUsage      int64
	TotalMemUsage     int64
}

// TotalMemoryUsage implements CacheItemMemoryUsage
func (c *IndexMemUsage) TotalMemoryUsage() int64 {
	return c.TotalMemUsage
}

// ItemID implements CacheItemMemoryUsage
func (c *IndexMemUsage) ItemID() int64 {
	return c.IndexID
}

// TrackingMemUsage implements CacheItemMemoryUsage
func (c *IndexMemUsage) TrackingMemUsage() int64 {
	return c.CMSketchMemUsage + c.TopNMemUsage + c.HistogramMemUsage
}

// HistMemUsage implements CacheItemMemoryUsage
func (c *IndexMemUsage) HistMemUsage() int64 {
	return c.HistogramMemUsage
}

// TopnMemUsage implements CacheItemMemoryUsage
func (c *IndexMemUsage) TopnMemUsage() int64 {
	return c.TopNMemUsage
}

// CMSMemUsage implements CacheItemMemoryUsage
func (c *IndexMemUsage) CMSMemUsage() int64 {
	return c.CMSketchMemUsage
}

// MemoryUsage returns the total memory usage of this Table.
// it will only calc the size of Columns and Indices stats data of table.
// We ignore the size of other metadata in Table
func (t *Table) MemoryUsage() *TableMemoryUsage {
	tMemUsage := &TableMemoryUsage{
		TableID:         t.PhysicalID,
		ColumnsMemUsage: make(map[int64]CacheItemMemoryUsage),
		IndicesMemUsage: make(map[int64]CacheItemMemoryUsage),
	}
	for _, col := range t.Columns {
		if col != nil {
			colMemUsage := col.MemoryUsage()
			tMemUsage.ColumnsMemUsage[colMemUsage.ItemID()] = colMemUsage
			tMemUsage.TotalMemUsage += colMemUsage.TotalMemoryUsage()
		}
	}
	for _, index := range t.Indices {
		if index != nil {
			idxMemUsage := index.MemoryUsage()
			tMemUsage.IndicesMemUsage[idxMemUsage.ItemID()] = idxMemUsage
			tMemUsage.TotalMemUsage += idxMemUsage.TotalMemoryUsage()
		}
	}
	return tMemUsage
}

// Copy copies the current table.
func (t *Table) Copy() *Table {
	newHistColl := HistColl{
		PhysicalID:     t.PhysicalID,
		HavePhysicalID: t.HavePhysicalID,
		RealtimeCount:  t.RealtimeCount,
		Columns:        make(map[int64]*Column, len(t.Columns)),
		Indices:        make(map[int64]*Index, len(t.Indices)),
		Pseudo:         t.Pseudo,
		ModifyCount:    t.ModifyCount,
		StatsVer:       t.StatsVer,
	}
	for id, col := range t.Columns {
		newHistColl.Columns[id] = col.Copy()
	}
	for id, idx := range t.Indices {
		newHistColl.Indices[id] = idx.Copy()
	}
	nt := &Table{
		HistColl:           newHistColl,
		Version:            t.Version,
		TblInfoUpdateTS:    t.TblInfoUpdateTS,
		IsPkIsHandle:       t.IsPkIsHandle,
		LastAnalyzeVersion: t.LastAnalyzeVersion,
	}
	if t.ExtendedStats != nil {
		newExtStatsColl := &ExtendedStatsColl{
			Stats:             make(map[string]*ExtendedStatsItem),
			LastUpdateVersion: t.ExtendedStats.LastUpdateVersion,
		}
		for name, item := range t.ExtendedStats.Stats {
			newExtStatsColl.Stats[name] = item
		}
		nt.ExtendedStats = newExtStatsColl
	}
	if t.ColAndIdxExistenceMap != nil {
		nt.ColAndIdxExistenceMap = t.ColAndIdxExistenceMap.Clone()
	}
	return nt
}

// ShallowCopy copies the current table.
// It's different from Copy(). Only the struct Table (and also the embedded HistColl) is copied here.
// The internal containers, like t.Columns and t.Indices, and the stats, like TopN and Histogram are not copied.
func (t *Table) ShallowCopy() *Table {
	newHistColl := HistColl{
		PhysicalID:     t.PhysicalID,
		HavePhysicalID: t.HavePhysicalID,
		RealtimeCount:  t.RealtimeCount,
		Columns:        t.Columns,
		Indices:        t.Indices,
		Pseudo:         t.Pseudo,
		ModifyCount:    t.ModifyCount,
		StatsVer:       t.StatsVer,
	}
	nt := &Table{
		HistColl:              newHistColl,
		Version:               t.Version,
		TblInfoUpdateTS:       t.TblInfoUpdateTS,
		ExtendedStats:         t.ExtendedStats,
		ColAndIdxExistenceMap: t.ColAndIdxExistenceMap,
		LastAnalyzeVersion:    t.LastAnalyzeVersion,
	}
	return nt
}

// String implements Stringer interface.
func (t *Table) String() string {
	strs := make([]string, 0, len(t.Columns)+1)
	strs = append(strs, fmt.Sprintf("Table:%d RealtimeCount:%d", t.PhysicalID, t.RealtimeCount))
	cols := make([]*Column, 0, len(t.Columns))
	for _, col := range t.Columns {
		cols = append(cols, col)
	}
	slices.SortFunc(cols, func(i, j *Column) int { return cmp.Compare(i.ID, j.ID) })
	for _, col := range cols {
		strs = append(strs, col.String())
	}
	idxs := make([]*Index, 0, len(t.Indices))
	for _, idx := range t.Indices {
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
	for _, index := range t.Indices {
		if index.Info.Columns[0].Name.L == colName {
			return index
		}
	}
	return nil
}

// ColumnByName finds the statistics.Column for the given column.
func (t *Table) ColumnByName(colName string) *Column {
	for _, c := range t.Columns {
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
		if idxStatsInfo, ok := t.Indices[id]; ok {
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
	if colStatsInfo, ok := t.Columns[id]; ok {
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

// GetAnalyzeRowCount tries to get the row count of a column or an index if possible.
// This method is useful because this row count doesn't consider the modify count.
func (coll *HistColl) GetAnalyzeRowCount() float64 {
	ids := maps.Keys(coll.Columns)
	slices.Sort(ids)
	for _, id := range ids {
		col := coll.Columns[id]
		if col != nil && col.IsFullLoad() {
			return col.TotalRowCount()
		}
	}
	ids = maps.Keys(coll.Indices)
	slices.Sort(ids)
	for _, id := range ids {
		idx := coll.Indices[id]
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
	scale := idxStats.TotalRowCount() / analyzeRowCount
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
// The Column should be visible in the table and really has analyzed statistics in the stroage.
// Also, if the stats has been loaded into the memory, we also don't need to load it.
// We return the Column together with the checking result, to avoid accessing the map multiple times.
// The first bool is whether we have it in memory. The second bool is whether this column has stats in the system table or not.
func (t *Table) ColumnIsLoadNeeded(id int64, fullLoad bool) (*Column, bool, bool) {
	if t.Pseudo {
		return nil, false, false
	}
	col, ok := t.Columns[id]
	hasAnalyzed := t.ColAndIdxExistenceMap.HasAnalyzed(id, false)

	// If it's not analyzed yet.
	if !hasAnalyzed {
		// If we don't have it in memory, we create a fake hist for pseudo estimation (see handleOneItemTask()).
		if !ok {
			// If we don't have this column. We skip it.
			// It's something ridiculous. But it's possible that the stats don't have some ColumnInfo.
			// We need to find a way to maintain it more correctly.
			return nil, t.ColAndIdxExistenceMap.Has(id, false), false
		}
		// Otherwise we don't need to load it.
		return nil, false, false
	}

	// Restore the condition from the simplified form:
	// 1. !ok && hasAnalyzed => need load
	// 2. ok && hasAnalyzed && fullLoad && !col.IsFullLoad => need load
	// 3. ok && hasAnalyzed && !fullLoad && !col.statsInitialized => need load
	if !ok || (fullLoad && !col.IsFullLoad()) || (!fullLoad && !col.statsInitialized) {
		return col, true, true
	}

	// Otherwise don't need load it.
	return col, false, true
}

// IndexIsLoadNeeded checks whether the index needs trigger the async/sync load.
// The Index should be visible in the table and really has analyzed statistics in the stroage.
// Also, if the stats has been loaded into the memory, we also don't need to load it.
// We return the Index together with the checking result, to avoid accessing the map multiple times.
func (t *Table) IndexIsLoadNeeded(id int64) (*Index, bool) {
	idx, ok := t.Indices[id]
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
	for _, col := range t.Columns {
		if col != nil && col.IsStatsInitialized() {
			return true
		}
	}
	for _, idx := range t.Indices {
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
	for _, col := range t.Columns {
		col.FMSketch.DestroyAndPutToPool()
	}
	maps.Clear(t.Columns)
	for _, idx := range t.Indices {
		idx.FMSketch.DestroyAndPutToPool()
	}
	maps.Clear(t.Indices)
}

// ID2UniqueID generates a new HistColl whose `Columns` is built from UniqueID of given columns.
func (coll *HistColl) ID2UniqueID(columns []*expression.Column) *HistColl {
	cols := make(map[int64]*Column)
	for _, col := range columns {
		colHist, ok := coll.Columns[col.ID]
		if ok {
			cols[col.UniqueID] = colHist
		}
	}
	newColl := &HistColl{
		PhysicalID:     coll.PhysicalID,
		HavePhysicalID: coll.HavePhysicalID,
		Pseudo:         coll.Pseudo,
		RealtimeCount:  coll.RealtimeCount,
		ModifyCount:    coll.ModifyCount,
		Columns:        cols,
	}
	return newColl
}

// GenerateHistCollFromColumnInfo generates a new HistColl whose ColUniqueID2IdxIDs and Idx2ColUniqueIDs is built from the given parameter.
func (coll *HistColl) GenerateHistCollFromColumnInfo(tblInfo *model.TableInfo, columns []*expression.Column) *HistColl {
	newColHistMap := make(map[int64]*Column)
	colInfoID2UniqueID := make(map[int64]int64, len(columns))
	uniqueID2colInfoID := make(map[int64]int64, len(columns))
	idxID2idxInfo := make(map[int64]*model.IndexInfo)
	for _, col := range columns {
		colInfoID2UniqueID[col.ID] = col.UniqueID
		uniqueID2colInfoID[col.UniqueID] = col.ID
	}
	for id, colHist := range coll.Columns {
		uniqueID, ok := colInfoID2UniqueID[id]
		// Collect the statistics by the given columns.
		if ok {
			newColHistMap[uniqueID] = colHist
		}
	}
	for _, idxInfo := range tblInfo.Indices {
		idxID2idxInfo[idxInfo.ID] = idxInfo
	}
	newIdxHistMap := make(map[int64]*Index)
	idx2Columns := make(map[int64][]int64)
	colID2IdxIDs := make(map[int64][]int64)
	mvIdx2Columns := make(map[int64][]*expression.Column)
	for id, idxHist := range coll.Indices {
		idxInfo := idxID2idxInfo[id]
		if idxInfo == nil {
			continue
		}
		ids := make([]int64, 0, len(idxInfo.Columns))
		for _, idxCol := range idxInfo.Columns {
			uniqueID, ok := colInfoID2UniqueID[tblInfo.Columns[idxCol.Offset].ID]
			if !ok {
				break
			}
			ids = append(ids, uniqueID)
		}
		// If the length of the id list is 0, this index won't be used in this query.
		if len(ids) == 0 {
			continue
		}
		colID2IdxIDs[ids[0]] = append(colID2IdxIDs[ids[0]], idxHist.ID)
		newIdxHistMap[idxHist.ID] = idxHist
		idx2Columns[idxHist.ID] = ids
		if idxInfo.MVIndex {
			cols, ok := PrepareCols4MVIndex(tblInfo, idxInfo, columns, true)
			if ok {
				mvIdx2Columns[id] = cols
			}
		}
	}
	for _, idxIDs := range colID2IdxIDs {
		slices.Sort(idxIDs)
	}
	newColl := &HistColl{
		PhysicalID:         coll.PhysicalID,
		HavePhysicalID:     coll.HavePhysicalID,
		Pseudo:             coll.Pseudo,
		RealtimeCount:      coll.RealtimeCount,
		ModifyCount:        coll.ModifyCount,
		Columns:            newColHistMap,
		Indices:            newIdxHistMap,
		ColUniqueID2IdxIDs: colID2IdxIDs,
		Idx2ColUniqueIDs:   idx2Columns,
		UniqueID2colInfoID: uniqueID2colInfoID,
		MVIdx2Columns:      mvIdx2Columns,
	}
	return newColl
}

// PseudoTable creates a pseudo table statistics.
// Usually, we don't want to trigger stats loading for pseudo table.
// But there are exceptional cases. In such cases, we should pass allowTriggerLoading as true.
// Such case could possibly happen in getStatsTable().
func PseudoTable(tblInfo *model.TableInfo, allowTriggerLoading bool, allowFillHistMeta bool) *Table {
	pseudoHistColl := HistColl{
		RealtimeCount:     PseudoRowCount,
		PhysicalID:        tblInfo.ID,
		HavePhysicalID:    true,
		Columns:           make(map[int64]*Column, 2),
		Indices:           make(map[int64]*Index, 2),
		Pseudo:            true,
		CanNotTriggerLoad: !allowTriggerLoading,
	}
	t := &Table{
		HistColl:              pseudoHistColl,
		ColAndIdxExistenceMap: NewColAndIndexExistenceMap(len(tblInfo.Columns), len(tblInfo.Indices)),
	}
	for _, col := range tblInfo.Columns {
		// The column is public to use. Also we should check the column is not hidden since hidden means that it's used by expression index.
		// We would not collect stats for the hidden column and we won't use the hidden column to estimate.
		// Thus we don't create pseudo stats for it.
		if col.State == model.StatePublic && !col.Hidden {
			t.ColAndIdxExistenceMap.InsertCol(col.ID, col, false)
			if allowFillHistMeta {
				t.Columns[col.ID] = &Column{
					PhysicalID: tblInfo.ID,
					Info:       col,
					IsHandle:   tblInfo.PKIsHandle && mysql.HasPriKeyFlag(col.GetFlag()),
					Histogram:  *NewHistogram(col.ID, 0, 0, 0, &col.FieldType, 0, 0),
				}
			}
		}
	}
	for _, idx := range tblInfo.Indices {
		if idx.State == model.StatePublic {
			t.ColAndIdxExistenceMap.InsertIndex(idx.ID, idx, false)
			if allowFillHistMeta {
				t.Indices[idx.ID] = &Index{
					PhysicalID: tblInfo.ID,
					Info:       idx,
					Histogram:  *NewHistogram(idx.ID, 0, 0, 0, types.NewFieldType(mysql.TypeBlob), 0, 0),
				}
			}
		}
	}
	return t
}

// CheckAnalyzeVerOnTable checks whether the given version is the one from the tbl.
// If not, it will return false and set the version to the tbl's.
// We use this check to make sure all the statistics of the table are in the same version.
func CheckAnalyzeVerOnTable(tbl *Table, version *int) bool {
	if tbl.StatsVer != Version0 && tbl.StatsVer != *version {
		*version = tbl.StatsVer
		return false
	}
	return true
}

// PrepareCols4MVIndex helps to identify the columns of an MV index. We need this information for estimation.
// This logic is shared between the estimation logic and the access path generation logic. We'd like to put the mv index
// related functions together in the planner/core package. So we use this trick here to avoid the import cycle.
var PrepareCols4MVIndex func(
	tableInfo *model.TableInfo,
	mvIndex *model.IndexInfo,
	tblCols []*expression.Column,
	checkOnly1ArrayTypeCol bool,
) (idxCols []*expression.Column, ok bool)
