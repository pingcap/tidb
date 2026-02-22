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

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"go.uber.org/atomic"
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

// CopyIntent specifies what data structures are safe to modify in the copied table.
type CopyIntent uint8

const (
	// MetaOnly shares all maps - only table metadata is safe to modify
	MetaOnly CopyIntent = iota

	// ColumnMapWritable clones columns map - safe to add/remove columns
	ColumnMapWritable

	// IndexMapWritable clones indices map - safe to add/remove indices
	IndexMapWritable

	// BothMapsWritable clones both maps - safe to add/remove columns and indices
	BothMapsWritable

	// ExtendedStatsWritable shares all maps - safe to modify ExtendedStats field
	ExtendedStatsWritable

	// AllDataWritable deep copies everything - safe to modify all data including histograms
	AllDataWritable
)

// AutoAnalyzeMinCnt means if the count of table is less than this value, we don't need to do auto analyze.
// Exported for testing.
var AutoAnalyzeMinCnt int64 = 1000

var (
	// Below functions are used to solve cycle import problem.
	// Note: all functions below will be removed after finishing moving all estimation functions into the cardinality package.

	// GetRowCountByIndexRanges is a function type to get row count by index ranges.
	GetRowCountByIndexRanges func(sctx planctx.PlanContext, coll *HistColl, idxID int64, indexRanges []*ranger.Range, idxCol []*expression.Column) (result RowEstimate, err error)

	// GetRowCountByColumnRanges is a function type to get row count by column ranges.
	GetRowCountByColumnRanges func(sctx planctx.PlanContext, coll *HistColl, colID int64, colRanges []*ranger.Range, pkIsHandle bool) (result RowEstimate, err error)
)

// Table represents statistics for a table.
type Table struct {
	ExtendedStats *ExtendedStatsColl

	ColAndIdxExistenceMap *ColAndIdxExistenceMap
	HistColl
	Version uint64
	// It's the timestamp of the last analyze time.
	// We used it in auto-analyze to determine if this table has been analyzed.
	// The source of this field comes from two parts:
	// 1. Initialized by snapshot when loading stats_meta.
	// 2. Updated by the analysis time of a specific column or index when loading the histogram of the column or index.
	LastAnalyzeVersion uint64
	// LastStatsHistVersion is the mvcc version of the last update of histograms.
	// It differs from LastAnalyzeVersion because it can be influenced by some DDL.
	// e.g. When we execute ALTER TABLE ADD COLUMN, there'll be new record inserted into mysql.stats_histograms.
	//      We need to load the corresponding one into memory too.
	// It's used to skip redundant loading of stats, i.e, if the cached stats is already update-to-date with mysql.stats_xxx tables,
	// and the schema of the table does not change, we don't need to load the stats for this table again.
	// Stats' sync load/async load should not change this field since they are not table-level update.
	// It's hard to deal with the upgrade compatibility of this field, the field will not take effect unless
	// auto analyze or DDL happened on the table.
	LastStatsHistVersion uint64
	// TblInfoUpdateTS is the UpdateTS of the TableInfo used when filling this struct.
	// It is the schema version of the corresponding table. It is used to skip redundant
	// loading of stats, i.e, if the cached stats is already update-to-date with mysql.stats_xxx tables,
	// and the schema of the table does not change, we don't need to load the stats for this
	// table again.
	// TODO: it can be removed now that we've have LastAnalyseVersion and LastStatsHistVersion.
	TblInfoUpdateTS uint64

	IsPkIsHandle bool
}

// ColAndIdxExistenceMap is the meta map for statistics.Table.
// It can tell whether a column/index really has its statistics. So we won't send useless kv request when we do online stats loading.
type ColAndIdxExistenceMap struct {
	colAnalyzed map[int64]bool
	idxAnalyzed map[int64]bool
}

// DeleteColNotFound deletes the column with the given id.
func (m *ColAndIdxExistenceMap) DeleteColNotFound(id int64) {
	delete(m.colAnalyzed, id)
}

// DeleteIdxNotFound deletes the index with the given id.
func (m *ColAndIdxExistenceMap) DeleteIdxNotFound(id int64) {
	delete(m.idxAnalyzed, id)
}

// HasAnalyzed checks whether a column/index stats exists and it has stats.
// TODO: the map should only keep the analyzed cols.
// There's three possible status of column/index's statistics:
//  1. We don't have this column/index.
//  2. We have it, but it hasn't been analyzed yet.
//  3. We have it and its statistics.
//
// To figure out three status, we use HasAnalyzed's TRUE value to represents the status 3. The Has's FALSE to represents the status 1.
// Begin from v8.5.2, the 1. case becomes a nearly invalid case. It's just a middle state between happening of the DDL and the completion of the stats' ddl handler.
// But we may need to deal with the 1. for the upgrade compatibility.
func (m *ColAndIdxExistenceMap) HasAnalyzed(id int64, isIndex bool) bool {
	if isIndex {
		analyzed, ok := m.idxAnalyzed[id]
		return ok && analyzed
	}
	analyzed, ok := m.colAnalyzed[id]
	return ok && analyzed
}

// Has checks whether a column/index stats exists.
func (m *ColAndIdxExistenceMap) Has(id int64, isIndex bool) bool {
	if isIndex {
		_, ok := m.idxAnalyzed[id]
		return ok
	}
	_, ok := m.colAnalyzed[id]
	return ok
}

// InsertCol inserts a column with its meta into the map.
func (m *ColAndIdxExistenceMap) InsertCol(id int64, analyzed bool) {
	m.colAnalyzed[id] = analyzed
}

// InsertIndex inserts an index with its meta into the map.
func (m *ColAndIdxExistenceMap) InsertIndex(id int64, analyzed bool) {
	m.idxAnalyzed[id] = analyzed
}

// IsEmpty checks whether the map is empty.
func (m *ColAndIdxExistenceMap) IsEmpty() bool {
	return len(m.colAnalyzed)+len(m.idxAnalyzed) == 0
}

// ColNum returns the number of columns in the map.
func (m *ColAndIdxExistenceMap) ColNum() int {
	return len(m.colAnalyzed)
}

// Clone deeply copies the map.
func (m *ColAndIdxExistenceMap) Clone() *ColAndIdxExistenceMap {
	mm := NewColAndIndexExistenceMap(len(m.colAnalyzed), len(m.idxAnalyzed))
	mm.colAnalyzed = maps.Clone(m.colAnalyzed)
	mm.idxAnalyzed = maps.Clone(m.idxAnalyzed)
	return mm
}

const (
	defaultColCap = 16
	defaultIdxCap = 4
)

// NewColAndIndexExistenceMapWithoutSize return a new object with default capacity.
func NewColAndIndexExistenceMapWithoutSize() *ColAndIdxExistenceMap {
	return &ColAndIdxExistenceMap{
		colAnalyzed: make(map[int64]bool, defaultColCap),
		idxAnalyzed: make(map[int64]bool, defaultIdxCap),
	}
}

// NewColAndIndexExistenceMap return a new object with the given capcity.
func NewColAndIndexExistenceMap(colCap, idxCap int) *ColAndIdxExistenceMap {
	return &ColAndIdxExistenceMap{
		colAnalyzed: make(map[int64]bool, colCap),
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

// HistColl is a collection of histograms. It collects enough information for plan to calculate the selectivity.
type HistColl struct {
	// Note that when used in a query, Column use UniqueID as the key while Indices use the index ID in the
	// metadata. (See GenerateHistCollFromColumnInfo() for details)
	columns    map[int64]*Column
	indices    map[int64]*Index
	PhysicalID int64
	// TODO: add AnalyzeCount here
	RealtimeCount int64 // RealtimeCount is the current table row count, maintained by applying stats delta based on AnalyzeCount.
	ModifyCount   int64 // Total modify count in a table.

	// The version of the statistics, refer to Version0, Version1, Version2 and so on.
	StatsVer int
	Pseudo   bool

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

// NewHistColl creates a new HistColl.
func NewHistColl(id int64, realtimeCnt, modifyCnt int64, colNum, idxNum int) *HistColl {
	return &HistColl{
		columns:            make(map[int64]*Column, colNum),
		indices:            make(map[int64]*Index, idxNum),
		PhysicalID:         id,
		RealtimeCount:      realtimeCnt,
		ModifyCount:        modifyCnt,
		Idx2ColUniqueIDs:   make(map[int64][]int64),
		ColUniqueID2IdxIDs: make(map[int64][]int64),
		UniqueID2colInfoID: make(map[int64]int64),
		MVIdx2Columns:      make(map[int64][]*expression.Column),
	}
}

// NewHistCollWithColsAndIdxs creates a new HistColl with given columns and indices.
func NewHistCollWithColsAndIdxs(id int64, realtimeCnt, modifyCnt int64, cols map[int64]*Column, idxs map[int64]*Index) *HistColl {
	return &HistColl{
		columns:            cols,
		indices:            idxs,
		PhysicalID:         id,
		RealtimeCount:      realtimeCnt,
		ModifyCount:        modifyCnt,
		Idx2ColUniqueIDs:   make(map[int64][]int64),
		ColUniqueID2IdxIDs: make(map[int64][]int64),
		UniqueID2colInfoID: make(map[int64]int64),
		MVIdx2Columns:      make(map[int64][]*expression.Column),
	}
}

// SetCol sets the column with the given id.
func (coll *HistColl) SetCol(id int64, col *Column) {
	coll.columns[id] = col
}

// SetIdx sets the index with the given id.
func (coll *HistColl) SetIdx(id int64, idx *Index) {
	coll.indices[id] = idx
}

// GetCol gets the column with the given id.
func (coll *HistColl) GetCol(id int64) *Column {
	return coll.columns[id]
}

// GetIdx gets the index with the given id.
func (coll *HistColl) GetIdx(id int64) *Index {
	return coll.indices[id]
}

// ForEachColumnImmutable iterates all columns in the HistColl.
// The bool return value of f is used to control the iteration. If f returns true, the iteration will be stopped.
// Warning: Don't change the content when calling this function.
func (coll *HistColl) ForEachColumnImmutable(f func(int64, *Column) bool) {
	for id, col := range coll.columns {
		if f(id, col) {
			return
		}
	}
}

// ForEachIndexImmutable iterates all columns in the HistColl.
// The bool return value of f is used to control the iteration. If f returns true, the iteration will be stopped.
// WARNING: Don't change the content when calling this function.
func (coll *HistColl) ForEachIndexImmutable(f func(int64, *Index) bool) {
	for id, idx := range coll.indices {
		if f(id, idx) {
			return
		}
	}
}

// ColNum returns the number of columns in the HistColl.
func (coll *HistColl) ColNum() int {
	return len(coll.columns)
}

// IdxNum returns the number of indices in the HistColl.
func (coll *HistColl) IdxNum() int {
	return len(coll.indices)
}

// DelCol deletes the column with the given id.
func (t *Table) DelCol(id int64) {
	delete(t.columns, id)
	t.ColAndIdxExistenceMap.DeleteColNotFound(id)
}

// DelIdx deletes the index with the given id.
func (t *Table) DelIdx(id int64) {
	delete(t.indices, id)
	t.ColAndIdxExistenceMap.DeleteIdxNotFound(id)
}

// StableOrderColSlice returns a slice of columns in stable order.
func (coll *HistColl) StableOrderColSlice() []*Column {
	cols := make([]*Column, 0, len(coll.columns))
	for _, col := range coll.columns {
		cols = append(cols, col)
	}
	slices.SortFunc(cols, func(c1, c2 *Column) int {
		return cmp.Compare(c1.ID, c2.ID)
	})
	return cols
}

// GetColSlice returns a slice of columns without order.
func (coll *HistColl) GetColSlice() []*Column {
	cols := make([]*Column, 0, len(coll.columns))
	for _, col := range coll.columns {
		cols = append(cols, col)
	}
	return cols
}

// StableOrderIdxSlice returns a slice of indices in stable order.
func (coll *HistColl) StableOrderIdxSlice() []*Index {
	idxs := make([]*Index, 0, len(coll.indices))
	for _, idx := range coll.indices {
		idxs = append(idxs, idx)
	}
	slices.SortFunc(idxs, func(i1, i2 *Index) int {
		return cmp.Compare(i1.ID, i2.ID)
	})
	return idxs
}

// GetIdxSlice returns a slice of indices without order.
func (coll *HistColl) GetIdxSlice() []*Index {
	idxs := make([]*Index, 0, len(coll.indices))
	for _, idx := range coll.indices {
		idxs = append(idxs, idx)
	}
	return idxs
}

// SetAllIndexFullLoadForBootstrap sets all indices' stats loaded status to full load for bootstrap.
func (coll *HistColl) SetAllIndexFullLoadForBootstrap() {
	for _, idx := range coll.indices {
		idx.StatsLoadedStatus = NewStatsFullLoadStatus()
	}
}

// CalcPreScalar calculates the pre-calculated scalar for all columns and indices.
func (coll *HistColl) CalcPreScalar() {
	for _, idx := range coll.indices {
		for i := 1; i < idx.Len(); i++ {
			idx.Buckets[i].Count += idx.Buckets[i-1].Count
		}
		idx.PreCalculateScalar()
	}
	for _, col := range coll.columns {
		for i := 1; i < col.Len(); i++ {
			col.Buckets[i].Count += col.Buckets[i-1].Count
		}
		col.PreCalculateScalar()
	}
}

// DropEvicted will drop the unnecessary data for all columns and indices. It's triggerred by stats cache.
func (coll *HistColl) DropEvicted() {
	for _, col := range coll.columns {
		if !col.IsStatsInitialized() || col.GetEvictedStatus() == AllEvicted {
			continue
		}
		col.DropUnnecessaryData()
	}
	for _, idx := range coll.indices {
		if !idx.IsStatsInitialized() || idx.GetEvictedStatus() == AllEvicted {
			continue
		}
		idx.DropUnnecessaryData()
	}
}


