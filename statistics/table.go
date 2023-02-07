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
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/tracing"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

const (
	pseudoEqualRate   = 1000
	pseudoLessRate    = 3
	pseudoBetweenRate = 40
	pseudoColSize     = 8.0

	outOfRangeBetweenRate = 100
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

// Table represents statistics for a table.
type Table struct {
	HistColl
	Version       uint64
	Name          string
	ExtendedStats *ExtendedStatsColl
	// TblInfoUpdateTS is the UpdateTS of the TableInfo used when filling this struct.
	// It is the schema version of the corresponding table. It is used to skip redundant
	// loading of stats, i.e, if the cached stats is already update-to-date with mysql.stats_xxx tables,
	// and the schema of the table does not change, we don't need to load the stats for this
	// table again.
	TblInfoUpdateTS uint64
}

// ExtendedStatsItem is the cached item of a mysql.stats_extended record.
type ExtendedStatsItem struct {
	ColIDs     []int64
	Tp         uint8
	ScalarVals float64
	StringVals string
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
	PhysicalID int64
	Columns    map[int64]*Column
	Indices    map[int64]*Index
	// Idx2ColumnIDs maps the index id to its column ids. It's used to calculate the selectivity in planner.
	Idx2ColumnIDs map[int64][]int64
	// ColID2IdxIDs maps the column id to a list index ids whose first column is it. It's used to calculate the selectivity in planner.
	ColID2IdxIDs map[int64][]int64
	Count        int64
	ModifyCount  int64 // Total modify count in a table.

	// HavePhysicalID is true means this HistColl is from single table and have its ID's information.
	// The physical id is used when try to load column stats from storage.
	HavePhysicalID bool
	Pseudo         bool
}

// TableMemoryUsage records tbl memory usage
type TableMemoryUsage struct {
	TableID         int64
	TotalMemUsage   int64
	ColumnsMemUsage map[int64]CacheItemMemoryUsage
	IndicesMemUsage map[int64]CacheItemMemoryUsage
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

	dropCMS()
	dropTopN()
	dropHist()
	isStatsInitialized() bool
	getEvictedStatus() int
	statsVer() int64
	isCMSExist() bool
}

// DropEvicted drop stats for table column/index
func DropEvicted(item TableCacheItem) {
	if !item.isStatsInitialized() {
		return
	}
	switch item.getEvictedStatus() {
	case allLoaded:
		if item.isCMSExist() && item.statsVer() < Version2 {
			item.dropCMS()
			return
		}
		// For stats version2, there is no cms thus we directly drop topn
		item.dropTopN()
		return
	case onlyCmsEvicted:
		item.dropTopN()
		return
	case onlyHistRemained:
		item.dropHist()
		return
	default:
		return
	}
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
		Count:          t.Count,
		Columns:        make(map[int64]*Column, len(t.Columns)),
		Indices:        make(map[int64]*Index, len(t.Indices)),
		Pseudo:         t.Pseudo,
		ModifyCount:    t.ModifyCount,
	}
	for id, col := range t.Columns {
		newHistColl.Columns[id] = col
	}
	for id, idx := range t.Indices {
		newHistColl.Indices[id] = idx
	}
	nt := &Table{
		HistColl:        newHistColl,
		Version:         t.Version,
		Name:            t.Name,
		TblInfoUpdateTS: t.TblInfoUpdateTS,
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
	return nt
}

// String implements Stringer interface.
func (t *Table) String() string {
	strs := make([]string, 0, len(t.Columns)+1)
	strs = append(strs, fmt.Sprintf("Table:%d Count:%d", t.PhysicalID, t.Count))
	cols := make([]*Column, 0, len(t.Columns))
	for _, col := range t.Columns {
		cols = append(cols, col)
	}
	slices.SortFunc(cols, func(i, j *Column) bool { return i.ID < j.ID })
	for _, col := range cols {
		strs = append(strs, col.String())
	}
	idxs := make([]*Index, 0, len(t.Indices))
	for _, idx := range t.Indices {
		idxs = append(idxs, idx)
	}
	slices.SortFunc(idxs, func(i, j *Index) bool { return i.ID < j.ID })
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
func (t *Table) GetStatsInfo(ID int64, isIndex bool) (int64, *Histogram, *CMSketch, *TopN, *FMSketch, bool) {
	if isIndex {
		if idxStatsInfo, ok := t.Indices[ID]; ok {
			return int64(idxStatsInfo.TotalRowCount()), idxStatsInfo.Histogram.Copy(), idxStatsInfo.CMSketch.Copy(), idxStatsInfo.TopN.Copy(), idxStatsInfo.FMSketch.Copy(), true
		}
		// newly added index which is not analyzed yet
		return 0, nil, nil, nil, nil, false
	}
	if colStatsInfo, ok := t.Columns[ID]; ok {
		return int64(colStatsInfo.TotalRowCount()), colStatsInfo.Histogram.Copy(), colStatsInfo.CMSketch.Copy(), colStatsInfo.TopN.Copy(), colStatsInfo.FMSketch.Copy(), true
	}
	// newly added column which is not analyzed yet
	return 0, nil, nil, nil, nil, false
}

// GetColRowCount tries to get the row count of the a column if possible.
// This method is useful because this row count doesn't consider the modify count.
func (t *Table) GetColRowCount() float64 {
	IDs := make([]int64, 0, len(t.Columns))
	for id := range t.Columns {
		IDs = append(IDs, id)
	}
	slices.Sort(IDs)
	for _, id := range IDs {
		col := t.Columns[id]
		// need to make sure stats on this column is loaded.
		// TODO: use the new method to check if it's loaded
		if col != nil && !(col.Histogram.NDV > 0 && col.notNullCount() == 0) && col.TotalRowCount() != 0 {
			return col.TotalRowCount()
		}
	}
	return -1
}

// GetStatsHealthy calculates stats healthy if the table stats is not pseudo.
// If the table stats is pseudo, it returns 0, false, otherwise it returns stats healthy, true.
func (t *Table) GetStatsHealthy() (int64, bool) {
	if t == nil || t.Pseudo {
		return 0, false
	}
	var healthy int64
	count := float64(t.Count)
	if histCount := t.GetColRowCount(); histCount > 0 {
		count = histCount
	}
	if float64(t.ModifyCount) < count {
		healthy = int64((1.0 - float64(t.ModifyCount)/count) * 100.0)
	} else if t.ModifyCount == 0 {
		healthy = 100
	}
	return healthy, true
}

type neededStatsMap struct {
	m     sync.RWMutex
	items map[model.TableItemID]struct{}
}

func (n *neededStatsMap) AllItems() []model.TableItemID {
	n.m.RLock()
	keys := make([]model.TableItemID, 0, len(n.items))
	for key := range n.items {
		keys = append(keys, key)
	}
	n.m.RUnlock()
	return keys
}

func (n *neededStatsMap) insert(col model.TableItemID) {
	n.m.Lock()
	n.items[col] = struct{}{}
	n.m.Unlock()
}

func (n *neededStatsMap) Delete(col model.TableItemID) {
	n.m.Lock()
	delete(n.items, col)
	n.m.Unlock()
}

func (n *neededStatsMap) Length() int {
	n.m.RLock()
	defer n.m.RUnlock()
	return len(n.items)
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
	rowcount := t.GetColRowCount()
	if rowcount < 0 {
		rowcount = float64(t.Count)
	}
	if rowcount > 0 && float64(t.ModifyCount)/rowcount > RatioOfPseudoEstimate.Load() {
		return true
	}
	return false
}

// ColumnGreaterRowCount estimates the row count where the column greater than value.
func (t *Table) ColumnGreaterRowCount(sctx sessionctx.Context, value types.Datum, colID int64) float64 {
	c, ok := t.Columns[colID]
	if !ok || c.IsInvalid(sctx, t.Pseudo) {
		return float64(t.Count) / pseudoLessRate
	}
	return c.greaterRowCount(value) * c.GetIncreaseFactor(t.Count)
}

// ColumnLessRowCount estimates the row count where the column less than value. Note that null values are not counted.
func (t *Table) ColumnLessRowCount(sctx sessionctx.Context, value types.Datum, colID int64) float64 {
	c, ok := t.Columns[colID]
	if !ok || c.IsInvalid(sctx, t.Pseudo) {
		return float64(t.Count) / pseudoLessRate
	}
	return c.lessRowCount(value) * c.GetIncreaseFactor(t.Count)
}

// ColumnBetweenRowCount estimates the row count where column greater or equal to a and less than b.
func (t *Table) ColumnBetweenRowCount(sctx sessionctx.Context, a, b types.Datum, colID int64) (float64, error) {
	sc := sctx.GetSessionVars().StmtCtx
	c, ok := t.Columns[colID]
	if !ok || c.IsInvalid(sctx, t.Pseudo) {
		return float64(t.Count) / pseudoBetweenRate, nil
	}
	aEncoded, err := codec.EncodeKey(sc, nil, a)
	if err != nil {
		return 0, err
	}
	bEncoded, err := codec.EncodeKey(sc, nil, b)
	if err != nil {
		return 0, err
	}
	count := c.BetweenRowCount(sctx, a, b, aEncoded, bEncoded)
	if a.IsNull() {
		count += float64(c.NullCount)
	}
	return count * c.GetIncreaseFactor(t.Count), nil
}

// ColumnEqualRowCount estimates the row count where the column equals to value.
func (t *Table) ColumnEqualRowCount(sctx sessionctx.Context, value types.Datum, colID int64) (float64, error) {
	c, ok := t.Columns[colID]
	if !ok || c.IsInvalid(sctx, t.Pseudo) {
		return float64(t.Count) / pseudoEqualRate, nil
	}
	encodedVal, err := codec.EncodeKey(sctx.GetSessionVars().StmtCtx, nil, value)
	if err != nil {
		return 0, err
	}
	result, err := c.equalRowCount(sctx, value, encodedVal, t.ModifyCount)
	result *= c.GetIncreaseFactor(t.Count)
	return result, errors.Trace(err)
}

// GetRowCountByIntColumnRanges estimates the row count by a slice of IntColumnRange.
func (coll *HistColl) GetRowCountByIntColumnRanges(sctx sessionctx.Context, colID int64, intRanges []*ranger.Range) (result float64, err error) {
	sc := sctx.GetSessionVars().StmtCtx
	c, ok := coll.Columns[colID]
	if c != nil {
		recordUsedItemStatsStatus(sctx, c.StatsLoadedStatus, coll.PhysicalID, colID, false)
	}
	if !ok || c.IsInvalid(sctx, coll.Pseudo) {
		if len(intRanges) == 0 {
			return 0, nil
		}
		if intRanges[0].LowVal[0].Kind() == types.KindInt64 {
			result = getPseudoRowCountBySignedIntRanges(intRanges, float64(coll.Count))
		} else {
			result = getPseudoRowCountByUnsignedIntRanges(intRanges, float64(coll.Count))
		}
		if sc.EnableOptimizerCETrace && ok {
			CETraceRange(sctx, coll.PhysicalID, []string{c.Info.Name.O}, intRanges, "Column Stats-Pseudo", uint64(result))
		}
		return result, nil
	}
	result, err = c.GetColumnRowCount(sctx, intRanges, coll.Count, coll.ModifyCount, true)
	if sc.EnableOptimizerCETrace {
		CETraceRange(sctx, coll.PhysicalID, []string{c.Info.Name.O}, intRanges, "Column Stats", uint64(result))
	}
	return result, errors.Trace(err)
}

// GetRowCountByColumnRanges estimates the row count by a slice of Range.
func (coll *HistColl) GetRowCountByColumnRanges(sctx sessionctx.Context, colID int64, colRanges []*ranger.Range) (float64, error) {
	sc := sctx.GetSessionVars().StmtCtx
	c, ok := coll.Columns[colID]
	if c != nil {
		recordUsedItemStatsStatus(sctx, c.StatsLoadedStatus, coll.PhysicalID, colID, false)
	}
	if !ok || c.IsInvalid(sctx, coll.Pseudo) {
		result, err := GetPseudoRowCountByColumnRanges(sc, float64(coll.Count), colRanges, 0)
		if err == nil && sc.EnableOptimizerCETrace && ok {
			CETraceRange(sctx, coll.PhysicalID, []string{c.Info.Name.O}, colRanges, "Column Stats-Pseudo", uint64(result))
		}
		return result, err
	}
	result, err := c.GetColumnRowCount(sctx, colRanges, coll.Count, coll.ModifyCount, false)
	if sc.EnableOptimizerCETrace {
		CETraceRange(sctx, coll.PhysicalID, []string{c.Info.Name.O}, colRanges, "Column Stats", uint64(result))
	}
	return result, errors.Trace(err)
}

// GetRowCountByIndexRanges estimates the row count by a slice of Range.
func (coll *HistColl) GetRowCountByIndexRanges(sctx sessionctx.Context, idxID int64, indexRanges []*ranger.Range) (float64, error) {
	sc := sctx.GetSessionVars().StmtCtx
	idx, ok := coll.Indices[idxID]
	colNames := make([]string, 0, 8)
	if ok {
		for _, col := range idx.Info.Columns {
			colNames = append(colNames, col.Name.O)
		}
	}
	if idx != nil {
		recordUsedItemStatsStatus(sctx, idx.StatsLoadedStatus, coll.PhysicalID, idxID, true)
	}
	if !ok || idx.IsInvalid(coll.Pseudo) {
		colsLen := -1
		if idx != nil && idx.Info.Unique {
			colsLen = len(idx.Info.Columns)
		}
		result, err := getPseudoRowCountByIndexRanges(sc, indexRanges, float64(coll.Count), colsLen)
		if err == nil && sc.EnableOptimizerCETrace && ok {
			CETraceRange(sctx, coll.PhysicalID, colNames, indexRanges, "Index Stats-Pseudo", uint64(result))
		}
		return result, err
	}
	var result float64
	var err error
	if idx.CMSketch != nil && idx.StatsVer == Version1 {
		result, err = coll.getIndexRowCount(sctx, idxID, indexRanges)
	} else {
		result, err = idx.GetRowCount(sctx, coll, indexRanges, coll.Count, coll.ModifyCount)
	}
	if sc.EnableOptimizerCETrace {
		CETraceRange(sctx, coll.PhysicalID, colNames, indexRanges, "Index Stats", uint64(result))
	}
	return result, errors.Trace(err)
}

// CETraceRange appends a list of ranges and related information into CE trace
func CETraceRange(sctx sessionctx.Context, tableID int64, colNames []string, ranges []*ranger.Range, tp string, rowCount uint64) {
	sc := sctx.GetSessionVars().StmtCtx
	allPoint := true
	for _, ran := range ranges {
		if !ran.IsPointNullable(sctx) {
			allPoint = false
			break
		}
	}
	if allPoint {
		tp = tp + "-Point"
	} else {
		tp = tp + "-Range"
	}
	expr, err := ranger.RangesToString(sc, ranges, colNames)
	if err != nil {
		logutil.BgLogger().Debug("[OptimizerTrace] Failed to trace CE of ranges", zap.Error(err))
	}
	// We don't need to record meaningless expressions.
	if expr == "" || expr == "true" || expr == "false" {
		return
	}
	CERecord := tracing.CETraceRecord{
		TableID:  tableID,
		Type:     tp,
		Expr:     expr,
		RowCount: rowCount,
	}
	sc.OptimizerCETrace = append(sc.OptimizerCETrace, &CERecord)
}

func (coll *HistColl) findAvailableStatsForCol(sctx sessionctx.Context, uniqueID int64) (isIndex bool, idx int64) {
	// try to find available stats in column stats
	if colStats, ok := coll.Columns[uniqueID]; ok && colStats != nil && !colStats.IsInvalid(sctx, coll.Pseudo) {
		return false, uniqueID
	}
	// try to find available stats in single column index stats (except for prefix index)
	for idxStatsIdx, cols := range coll.Idx2ColumnIDs {
		if len(cols) == 1 && cols[0] == uniqueID {
			idxStats, ok := coll.Indices[idxStatsIdx]
			if ok &&
				idxStats.Info.Columns[0].Length == types.UnspecifiedLength &&
				!idxStats.IsInvalid(coll.Pseudo) {
				return true, idxStatsIdx
			}
		}
	}
	return false, -1
}

// GetSelectivityByFilter try to estimate selectivity of expressions by evaluate the expressions using TopN and NULL.
// The data represented by the Histogram would use the defaultSelectivity parameter as the selectivity.
// Currently, this method can only handle expressions involving a single column.
func (coll *HistColl) GetSelectivityByFilter(sctx sessionctx.Context,
	defaultSelectivity float64,
	filters []expression.Expression) (ok bool, selectivity float64, err error) {
	// 1. Make sure the expressions
	//   (1) are safe to be evaluated here,
	//   (2) involve only one column,
	//   (3) and this column is not a "new collation" string column so that we're able to restore values from the stats.
	for _, filter := range filters {
		if expression.IsMutableEffectsExpr(filter) {
			return false, 0, nil
		}
	}
	if expression.ContainCorrelatedColumn(filters) {
		return false, 0, nil
	}
	cols := expression.ExtractColumnsFromExpressions(nil, filters, nil)
	if len(cols) != 1 {
		return false, 0, nil
	}
	col := cols[0]
	tp := col.RetType
	if types.IsString(tp.GetType()) && collate.NewCollationEnabled() && !collate.IsBinCollation(tp.GetCollate()) {
		return false, 0, nil
	}

	// 2. Get the available stats, make sure it's a ver2 stats and get the needed data structure from it.
	isIndex, i := coll.findAvailableStatsForCol(sctx, col.UniqueID)
	if i < 0 {
		return false, 0, nil
	}
	var statsVer, nullCnt int64
	var histTotalCnt, totalCnt float64
	var topnTotalCnt uint64
	var hist *Histogram
	var topn *TopN
	if isIndex {
		stats := coll.Indices[i]
		statsVer = stats.StatsVer
		hist = &stats.Histogram
		nullCnt = hist.NullCount
		topn = stats.TopN
	} else {
		stats := coll.Columns[i]
		statsVer = stats.StatsVer
		hist = &stats.Histogram
		nullCnt = hist.NullCount
		topn = stats.TopN
	}
	// Only in stats ver2, we can assume that: TopN + Histogram + NULL == All data
	if statsVer != Version2 {
		return false, 0, nil
	}
	topnTotalCnt = topn.TotalCount()
	histTotalCnt = hist.notNullCount()
	totalCnt = float64(topnTotalCnt) + histTotalCnt + float64(nullCnt)

	var topNSel, histSel, nullSel float64

	// Prepare for evaluation.

	// For execution, we use Column.Index instead of Column.UniqueID to locate a column.
	// We have only one column here, so we set it to 0.
	originalIndex := col.Index
	col.Index = 0
	defer func() {
		// Restore the original Index to avoid unexpected situation.
		col.Index = originalIndex
	}()
	size := 1
	if topn != nil {
		size = len(topn.TopN)
	}
	c := chunk.NewChunkWithCapacity([]*types.FieldType{tp}, size)
	selected := make([]bool, 0, size)

	// 3. Calculate the TopN part selectivity.
	// This stage is considered as the core functionality of this method, errors in this stage would make this entire method fail.
	var topNSelectedCnt uint64
	if topn != nil {
		for _, item := range topn.TopN {
			_, val, err := codec.DecodeOne(item.Encoded)
			if err != nil {
				return false, 0, err
			}
			c.AppendDatum(0, &val)
		}
		selected, err = expression.VectorizedFilter(sctx, filters, chunk.NewIterator4Chunk(c), selected)
		if err != nil {
			return false, 0, err
		}
		for i, isTrue := range selected {
			if isTrue {
				topNSelectedCnt += topn.TopN[i].Count
			}
		}
	}
	topNSel = float64(topNSelectedCnt) / totalCnt

	// 4. Calculate the Histogram part selectivity.
	histSel = defaultSelectivity * histTotalCnt / totalCnt

	// 5. Calculate the NULL part selectivity.
	// Errors in this staged would be returned, but would not make this entire method fail.
	c.Reset()
	c.AppendNull(0)
	selected = selected[:0]
	selected, err = expression.VectorizedFilter(sctx, filters, chunk.NewIterator4Chunk(c), selected)
	if err != nil || len(selected) != 1 {
		nullSel = defaultSelectivity * float64(nullCnt) / totalCnt
	} else if selected[0] {
		nullSel = float64(nullCnt) / totalCnt
	} else {
		nullSel = 0
	}

	// 6. Get the final result.
	res := topNSel + histSel + nullSel
	return true, res, err
}

// PseudoAvgCountPerValue gets a pseudo average count if histogram not exists.
func (t *Table) PseudoAvgCountPerValue() float64 {
	return float64(t.Count) / pseudoEqualRate
}

// GetOrdinalOfRangeCond gets the ordinal of the position range condition,
// if not exist, it returns the end position.
func GetOrdinalOfRangeCond(sc *stmtctx.StatementContext, ran *ranger.Range) int {
	for i := range ran.LowVal {
		a, b := ran.LowVal[i], ran.HighVal[i]
		cmp, err := a.Compare(sc, &b, ran.Collators[0])
		if err != nil {
			return 0
		}
		if cmp != 0 {
			return i
		}
	}
	return len(ran.LowVal)
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
		Count:          coll.Count,
		ModifyCount:    coll.ModifyCount,
		Columns:        cols,
	}
	return newColl
}

// GenerateHistCollFromColumnInfo generates a new HistColl whose ColID2IdxIDs and IdxID2ColIDs is built from the given parameter.
func (coll *HistColl) GenerateHistCollFromColumnInfo(infos []*model.ColumnInfo, columns []*expression.Column) *HistColl {
	newColHistMap := make(map[int64]*Column)
	colInfoID2UniqueID := make(map[int64]int64, len(columns))
	colNames2UniqueID := make(map[string]int64)
	for _, col := range columns {
		colInfoID2UniqueID[col.ID] = col.UniqueID
	}
	for _, colInfo := range infos {
		uniqueID, ok := colInfoID2UniqueID[colInfo.ID]
		if ok {
			colNames2UniqueID[colInfo.Name.L] = uniqueID
		}
	}
	for id, colHist := range coll.Columns {
		uniqueID, ok := colInfoID2UniqueID[id]
		// Collect the statistics by the given columns.
		if ok {
			newColHistMap[uniqueID] = colHist
		}
	}
	newIdxHistMap := make(map[int64]*Index)
	idx2Columns := make(map[int64][]int64)
	colID2IdxIDs := make(map[int64][]int64)
	for _, idxHist := range coll.Indices {
		ids := make([]int64, 0, len(idxHist.Info.Columns))
		for _, idxCol := range idxHist.Info.Columns {
			uniqueID, ok := colNames2UniqueID[idxCol.Name.L]
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
	}
	for _, idxIDs := range colID2IdxIDs {
		slices.Sort(idxIDs)
	}
	newColl := &HistColl{
		PhysicalID:     coll.PhysicalID,
		HavePhysicalID: coll.HavePhysicalID,
		Pseudo:         coll.Pseudo,
		Count:          coll.Count,
		ModifyCount:    coll.ModifyCount,
		Columns:        newColHistMap,
		Indices:        newIdxHistMap,
		ColID2IdxIDs:   colID2IdxIDs,
		Idx2ColumnIDs:  idx2Columns,
	}
	return newColl
}

// isSingleColIdxNullRange checks if a range is [NULL, NULL] on a single-column index.
func isSingleColIdxNullRange(idx *Index, ran *ranger.Range) bool {
	if len(idx.Info.Columns) > 1 {
		return false
	}
	l, h := ran.LowVal[0], ran.HighVal[0]
	if l.IsNull() && h.IsNull() {
		return true
	}
	return false
}

// outOfRangeEQSelectivity estimates selectivities for out-of-range values.
// It assumes all modifications are insertions and all new-inserted rows are uniformly distributed
// and has the same distribution with analyzed rows, which means each unique value should have the
// same number of rows(Tot/NDV) of it.
func outOfRangeEQSelectivity(ndv, realtimeRowCount, columnRowCount int64) float64 {
	increaseRowCount := realtimeRowCount - columnRowCount
	if increaseRowCount <= 0 {
		return 0 // it must be 0 since the histogram contains the whole data
	}
	if ndv < outOfRangeBetweenRate {
		ndv = outOfRangeBetweenRate // avoid inaccurate selectivity caused by small NDV
	}
	selectivity := 1 / float64(ndv)
	if selectivity*float64(columnRowCount) > float64(increaseRowCount) {
		selectivity = float64(increaseRowCount) / float64(columnRowCount)
	}
	return selectivity
}

// crossValidationSelectivity gets the selectivity of multi-column equal conditions by cross validation.
func (coll *HistColl) crossValidationSelectivity(sctx sessionctx.Context, idx *Index, usedColsLen int, idxPointRange *ranger.Range) (float64, float64, error) {
	minRowCount := math.MaxFloat64
	cols := coll.Idx2ColumnIDs[idx.ID]
	crossValidationSelectivity := 1.0
	totalRowCount := idx.TotalRowCount()
	for i, colID := range cols {
		if i >= usedColsLen {
			break
		}
		if col, ok := coll.Columns[colID]; ok {
			if col.IsInvalid(sctx, coll.Pseudo) {
				continue
			}
			// Since the column range is point range(LowVal is equal to HighVal), we need to set both LowExclude and HighExclude to false.
			// Otherwise we would get 0.0 estRow from GetColumnRowCount.
			rang := ranger.Range{
				LowVal:      []types.Datum{idxPointRange.LowVal[i]},
				LowExclude:  false,
				HighVal:     []types.Datum{idxPointRange.HighVal[i]},
				HighExclude: false,
				Collators:   []collate.Collator{idxPointRange.Collators[i]},
			}

			rowCount, err := col.GetColumnRowCount(sctx, []*ranger.Range{&rang}, coll.Count, coll.ModifyCount, col.IsHandle)
			if err != nil {
				return 0, 0, err
			}
			crossValidationSelectivity = crossValidationSelectivity * (rowCount / totalRowCount)

			if rowCount < minRowCount {
				minRowCount = rowCount
			}
		}
	}
	return minRowCount, crossValidationSelectivity, nil
}

// getEqualCondSelectivity gets the selectivity of the equal conditions.
func (coll *HistColl) getEqualCondSelectivity(sctx sessionctx.Context, idx *Index, bytes []byte, usedColsLen int, idxPointRange *ranger.Range) (float64, error) {
	coverAll := len(idx.Info.Columns) == usedColsLen
	// In this case, the row count is at most 1.
	if idx.Info.Unique && coverAll {
		return 1.0 / idx.TotalRowCount(), nil
	}
	val := types.NewBytesDatum(bytes)
	if idx.outOfRange(val) {
		// When the value is out of range, we could not found this value in the CM Sketch,
		// so we use heuristic methods to estimate the selectivity.
		if idx.NDV > 0 && coverAll {
			return outOfRangeEQSelectivity(idx.NDV, coll.Count, int64(idx.TotalRowCount())), nil
		}
		// The equal condition only uses prefix columns of the index.
		colIDs := coll.Idx2ColumnIDs[idx.ID]
		var ndv int64
		for i, colID := range colIDs {
			if i >= usedColsLen {
				break
			}
			if col, ok := coll.Columns[colID]; ok {
				ndv = mathutil.Max(ndv, col.Histogram.NDV)
			}
		}
		return outOfRangeEQSelectivity(ndv, coll.Count, int64(idx.TotalRowCount())), nil
	}

	minRowCount, crossValidationSelectivity, err := coll.crossValidationSelectivity(sctx, idx, usedColsLen, idxPointRange)
	if err != nil {
		return 0, nil
	}

	idxCount := float64(idx.QueryBytes(bytes))
	if minRowCount < idxCount {
		return crossValidationSelectivity, nil
	}
	return idxCount / idx.TotalRowCount(), nil
}

func (coll *HistColl) getIndexRowCount(sctx sessionctx.Context, idxID int64, indexRanges []*ranger.Range) (float64, error) {
	sc := sctx.GetSessionVars().StmtCtx
	idx := coll.Indices[idxID]
	totalCount := float64(0)
	for _, ran := range indexRanges {
		rangePosition := GetOrdinalOfRangeCond(sc, ran)
		var rangeVals []types.Datum
		// Try to enum the last range values.
		if rangePosition != len(ran.LowVal) {
			rangeVals = enumRangeValues(ran.LowVal[rangePosition], ran.HighVal[rangePosition], ran.LowExclude, ran.HighExclude)
			if rangeVals != nil {
				rangePosition++
			}
		}
		// If first one is range, just use the previous way to estimate; if it is [NULL, NULL] range
		// on single-column index, use previous way as well, because CMSketch does not contain null
		// values in this case.
		if rangePosition == 0 || isSingleColIdxNullRange(idx, ran) {
			count, err := idx.GetRowCount(sctx, nil, []*ranger.Range{ran}, coll.Count, coll.ModifyCount)
			if err != nil {
				return 0, errors.Trace(err)
			}
			totalCount += count
			continue
		}
		var selectivity float64
		// use CM Sketch to estimate the equal conditions
		if rangeVals == nil {
			bytes, err := codec.EncodeKey(sc, nil, ran.LowVal[:rangePosition]...)
			if err != nil {
				return 0, errors.Trace(err)
			}
			selectivity, err = coll.getEqualCondSelectivity(sctx, idx, bytes, rangePosition, ran)
			if err != nil {
				return 0, errors.Trace(err)
			}
		} else {
			bytes, err := codec.EncodeKey(sc, nil, ran.LowVal[:rangePosition-1]...)
			if err != nil {
				return 0, errors.Trace(err)
			}
			prefixLen := len(bytes)
			for _, val := range rangeVals {
				bytes = bytes[:prefixLen]
				bytes, err = codec.EncodeKey(sc, bytes, val)
				if err != nil {
					return 0, err
				}
				res, err := coll.getEqualCondSelectivity(sctx, idx, bytes, rangePosition, ran)
				if err != nil {
					return 0, errors.Trace(err)
				}
				selectivity += res
			}
		}
		// use histogram to estimate the range condition
		if rangePosition != len(ran.LowVal) {
			rang := ranger.Range{
				LowVal:      []types.Datum{ran.LowVal[rangePosition]},
				LowExclude:  ran.LowExclude,
				HighVal:     []types.Datum{ran.HighVal[rangePosition]},
				HighExclude: ran.HighExclude,
				Collators:   []collate.Collator{ran.Collators[rangePosition]},
			}
			var count float64
			var err error
			colIDs := coll.Idx2ColumnIDs[idxID]
			var colID int64
			if rangePosition >= len(colIDs) {
				colID = -1
			} else {
				colID = colIDs[rangePosition]
			}
			// prefer index stats over column stats
			if idxIDs, ok := coll.ColID2IdxIDs[colID]; ok && len(idxIDs) > 0 {
				idxID := idxIDs[0]
				count, err = coll.GetRowCountByIndexRanges(sctx, idxID, []*ranger.Range{&rang})
			} else {
				count, err = coll.GetRowCountByColumnRanges(sctx, colID, []*ranger.Range{&rang})
			}
			if err != nil {
				return 0, errors.Trace(err)
			}
			selectivity = selectivity * count / idx.TotalRowCount()
		}
		totalCount += selectivity * idx.TotalRowCount()
	}
	if totalCount > idx.TotalRowCount() {
		totalCount = idx.TotalRowCount()
	}
	return totalCount, nil
}

const fakePhysicalID int64 = -1

// PseudoTable creates a pseudo table statistics.
func PseudoTable(tblInfo *model.TableInfo) *Table {
	pseudoHistColl := HistColl{
		Count:          PseudoRowCount,
		PhysicalID:     tblInfo.ID,
		HavePhysicalID: true,
		Columns:        make(map[int64]*Column, len(tblInfo.Columns)),
		Indices:        make(map[int64]*Index, len(tblInfo.Indices)),
		Pseudo:         true,
	}
	t := &Table{
		HistColl: pseudoHistColl,
	}
	for _, col := range tblInfo.Columns {
		// The column is public to use. Also we should check the column is not hidden since hidden means that it's used by expression index.
		// We would not collect stats for the hidden column and we won't use the hidden column to estimate.
		// Thus we don't create pseudo stats for it.
		if col.State == model.StatePublic && !col.Hidden {
			t.Columns[col.ID] = &Column{
				PhysicalID: fakePhysicalID,
				Info:       col,
				IsHandle:   tblInfo.PKIsHandle && mysql.HasPriKeyFlag(col.GetFlag()),
				Histogram:  *NewHistogram(col.ID, 0, 0, 0, &col.FieldType, 0, 0),
			}
		}
	}
	for _, idx := range tblInfo.Indices {
		if idx.State == model.StatePublic {
			t.Indices[idx.ID] = &Index{
				PhysicalID: fakePhysicalID,
				Info:       idx,
				Histogram:  *NewHistogram(idx.ID, 0, 0, 0, types.NewFieldType(mysql.TypeBlob), 0, 0)}
		}
	}
	return t
}

func getPseudoRowCountByIndexRanges(sc *stmtctx.StatementContext, indexRanges []*ranger.Range,
	tableRowCount float64, colsLen int) (float64, error) {
	if tableRowCount == 0 {
		return 0, nil
	}
	var totalCount float64
	for _, indexRange := range indexRanges {
		count := tableRowCount
		i, err := indexRange.PrefixEqualLen(sc)
		if err != nil {
			return 0, errors.Trace(err)
		}
		if i == colsLen && !indexRange.LowExclude && !indexRange.HighExclude {
			totalCount += 1.0
			continue
		}
		if i >= len(indexRange.LowVal) {
			i = len(indexRange.LowVal) - 1
		}
		rowCount, err := GetPseudoRowCountByColumnRanges(sc, tableRowCount, []*ranger.Range{indexRange}, i)
		if err != nil {
			return 0, errors.Trace(err)
		}
		count = count / tableRowCount * rowCount
		// If the condition is a = 1, b = 1, c = 1, d = 1, we think every a=1, b=1, c=1 only filtrate 1/100 data,
		// so as to avoid collapsing too fast.
		for j := 0; j < i; j++ {
			count = count / float64(100)
		}
		totalCount += count
	}
	if totalCount > tableRowCount {
		totalCount = tableRowCount / 3.0
	}
	return totalCount, nil
}

// GetPseudoRowCountByColumnRanges calculate the row count by the ranges if there's no statistics information for this column.
func GetPseudoRowCountByColumnRanges(sc *stmtctx.StatementContext, tableRowCount float64, columnRanges []*ranger.Range, colIdx int) (float64, error) {
	var rowCount float64
	for _, ran := range columnRanges {
		if ran.LowVal[colIdx].Kind() == types.KindNull && ran.HighVal[colIdx].Kind() == types.KindMaxValue {
			rowCount += tableRowCount
		} else if ran.LowVal[colIdx].Kind() == types.KindMinNotNull {
			nullCount := tableRowCount / pseudoEqualRate
			if ran.HighVal[colIdx].Kind() == types.KindMaxValue {
				rowCount += tableRowCount - nullCount
			} else {
				lessCount := tableRowCount / pseudoLessRate
				rowCount += lessCount - nullCount
			}
		} else if ran.HighVal[colIdx].Kind() == types.KindMaxValue {
			rowCount += tableRowCount / pseudoLessRate
		} else {
			compare, err := ran.LowVal[colIdx].Compare(sc, &ran.HighVal[colIdx], ran.Collators[colIdx])
			if err != nil {
				return 0, errors.Trace(err)
			}
			if compare == 0 {
				rowCount += tableRowCount / pseudoEqualRate
			} else {
				rowCount += tableRowCount / pseudoBetweenRate
			}
		}
	}
	if rowCount > tableRowCount {
		rowCount = tableRowCount
	}
	return rowCount, nil
}

func getPseudoRowCountBySignedIntRanges(intRanges []*ranger.Range, tableRowCount float64) float64 {
	var rowCount float64
	for _, rg := range intRanges {
		var cnt float64
		low := rg.LowVal[0].GetInt64()
		if rg.LowVal[0].Kind() == types.KindNull || rg.LowVal[0].Kind() == types.KindMinNotNull {
			low = math.MinInt64
		}
		high := rg.HighVal[0].GetInt64()
		if rg.HighVal[0].Kind() == types.KindMaxValue {
			high = math.MaxInt64
		}
		if low == math.MinInt64 && high == math.MaxInt64 {
			cnt = tableRowCount
		} else if low == math.MinInt64 {
			cnt = tableRowCount / pseudoLessRate
		} else if high == math.MaxInt64 {
			cnt = tableRowCount / pseudoLessRate
		} else {
			if low == high {
				cnt = 1 // When primary key is handle, the equal row count is at most one.
			} else {
				cnt = tableRowCount / pseudoBetweenRate
			}
		}
		if high-low > 0 && cnt > float64(high-low) {
			cnt = float64(high - low)
		}
		rowCount += cnt
	}
	if rowCount > tableRowCount {
		rowCount = tableRowCount
	}
	return rowCount
}

func getPseudoRowCountByUnsignedIntRanges(intRanges []*ranger.Range, tableRowCount float64) float64 {
	var rowCount float64
	for _, rg := range intRanges {
		var cnt float64
		low := rg.LowVal[0].GetUint64()
		if rg.LowVal[0].Kind() == types.KindNull || rg.LowVal[0].Kind() == types.KindMinNotNull {
			low = 0
		}
		high := rg.HighVal[0].GetUint64()
		if rg.HighVal[0].Kind() == types.KindMaxValue {
			high = math.MaxUint64
		}
		if low == 0 && high == math.MaxUint64 {
			cnt = tableRowCount
		} else if low == 0 {
			cnt = tableRowCount / pseudoLessRate
		} else if high == math.MaxUint64 {
			cnt = tableRowCount / pseudoLessRate
		} else {
			if low == high {
				cnt = 1 // When primary key is handle, the equal row count is at most one.
			} else {
				cnt = tableRowCount / pseudoBetweenRate
			}
		}
		if high > low && cnt > float64(high-low) {
			cnt = float64(high - low)
		}
		rowCount += cnt
	}
	if rowCount > tableRowCount {
		rowCount = tableRowCount
	}
	return rowCount
}

// GetAvgRowSize computes average row size for given columns.
func (coll *HistColl) GetAvgRowSize(ctx sessionctx.Context, cols []*expression.Column, isEncodedKey bool, isForScan bool) (size float64) {
	sessionVars := ctx.GetSessionVars()
	if coll.Pseudo || len(coll.Columns) == 0 || coll.Count == 0 {
		size = pseudoColSize * float64(len(cols))
	} else {
		for _, col := range cols {
			colHist, ok := coll.Columns[col.UniqueID]
			// Normally this would not happen, it is for compatibility with old version stats which
			// does not include TotColSize.
			if !ok || (!colHist.IsHandle && colHist.TotColSize == 0 && (colHist.NullCount != coll.Count)) {
				size += pseudoColSize
				continue
			}
			// We differentiate if the column is encoded as key or value, because the resulted size
			// is different.
			if sessionVars.EnableChunkRPC && !isForScan {
				size += colHist.AvgColSizeChunkFormat(coll.Count)
			} else {
				size += colHist.AvgColSize(coll.Count, isEncodedKey)
			}
		}
	}
	if sessionVars.EnableChunkRPC && !isForScan {
		// Add 1/8 byte for each column's nullBitMap byte.
		return size + float64(len(cols))/8
	}
	// Add 1 byte for each column's flag byte. See `encode` for details.
	return size + float64(len(cols))
}

// GetAvgRowSizeListInDisk computes average row size for given columns.
func (coll *HistColl) GetAvgRowSizeListInDisk(cols []*expression.Column) (size float64) {
	if coll.Pseudo || len(coll.Columns) == 0 || coll.Count == 0 {
		for _, col := range cols {
			size += float64(chunk.EstimateTypeWidth(col.GetType()))
		}
	} else {
		for _, col := range cols {
			colHist, ok := coll.Columns[col.UniqueID]
			// Normally this would not happen, it is for compatibility with old version stats which
			// does not include TotColSize.
			if !ok || (!colHist.IsHandle && colHist.TotColSize == 0 && (colHist.NullCount != coll.Count)) {
				size += float64(chunk.EstimateTypeWidth(col.GetType()))
				continue
			}
			size += colHist.AvgColSizeListInDisk(coll.Count)
		}
	}
	// Add 8 byte for each column's size record. See `ListInDisk` for details.
	return size + float64(8*len(cols))
}

// GetTableAvgRowSize computes average row size for a table scan, exclude the index key-value pairs.
func (coll *HistColl) GetTableAvgRowSize(ctx sessionctx.Context, cols []*expression.Column, storeType kv.StoreType, handleInCols bool) (size float64) {
	size = coll.GetAvgRowSize(ctx, cols, false, true)
	switch storeType {
	case kv.TiKV:
		size += tablecodec.RecordRowKeyLen
		// The `cols` for TiKV always contain the row_id, so prefix row size subtract its length.
		size -= 8
	case kv.TiFlash:
		if !handleInCols {
			size += 8 /* row_id length */
		}
	}
	return
}

// GetIndexAvgRowSize computes average row size for a index scan.
func (coll *HistColl) GetIndexAvgRowSize(ctx sessionctx.Context, cols []*expression.Column, isUnique bool) (size float64) {
	size = coll.GetAvgRowSize(ctx, cols, true, true)
	// tablePrefix(1) + tableID(8) + indexPrefix(2) + indexID(8)
	// Because the cols for index scan always contain the handle, so we don't add the rowID here.
	size += 19
	if !isUnique {
		// add the len("_")
		size++
	}
	return
}

// CheckAnalyzeVerOnTable checks whether the given version is the one from the tbl.
// If not, it will return false and set the version to the tbl's.
// We use this check to make sure all the statistics of the table are in the same version.
func CheckAnalyzeVerOnTable(tbl *Table, version *int) bool {
	for _, col := range tbl.Columns {
		// Version0 means no statistics is collected currently.
		if col.StatsVer == Version0 {
			continue
		}
		if col.StatsVer != int64(*version) {
			*version = int(col.StatsVer)
			return false
		}
		// If we found one column and the version is the same, we can directly return since all the versions from this table is the same.
		return true
	}
	for _, idx := range tbl.Indices {
		// Version0 means no statistics is collected currently.
		if idx.StatsVer == Version0 {
			continue
		}
		if idx.StatsVer != int64(*version) {
			*version = int(idx.StatsVer)
			return false
		}
		// If we found one column and the version is the same, we can directly return since all the versions from this table is the same.
		return true
	}
	// This table has no statistics yet. We can directly return true.
	return true
}

// recordUsedItemStatsStatus only records un-FullLoad item load status during user query
func recordUsedItemStatsStatus(sctx sessionctx.Context, loadStatus StatsLoadedStatus,
	tableID, id int64, isIndex bool) {
	if loadStatus.IsFullLoad() {
		return
	}
	stmtCtx := sctx.GetSessionVars().StmtCtx
	item := model.TableItemID{TableID: tableID, ID: id, IsIndex: isIndex}
	// For some testcases, it skips ResetContextOfStmt to init StatsLoadStatus
	if stmtCtx.StatsLoadStatus == nil {
		stmtCtx.StatsLoadStatus = make(map[model.TableItemID]string)
	}
	stmtCtx.StatsLoadStatus[item] = loadStatus.StatusToString()
}
