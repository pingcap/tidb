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
// See the License for the specific language governing permissions and
// limitations under the License.

package statistics

import (
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/ranger"
	log "github.com/sirupsen/logrus"
)

const (
	// When we haven't analyzed a table, we use pseudo statistics to estimate costs.
	// It has row count 10000, equal condition selects 1/1000 of total rows, less condition selects 1/3 of total rows,
	// between condition selects 1/40 of total rows.
	pseudoRowCount    = 10000
	pseudoEqualRate   = 1000
	pseudoLessRate    = 3
	pseudoBetweenRate = 40
)

// Table represents statistics for a table.
type Table struct {
	TableID     int64
	Columns     map[int64]*Column
	Indices     map[int64]*Index
	colName2Idx map[string]int64 // map column name to index id
	colName2ID  map[string]int64 // map column name to column id
	Count       int64            // Total row count in a table.
	ModifyCount int64            // Total modify count in a table.
	Version     uint64
	Pseudo      bool
	PKIsHandle  bool
}

func (t *Table) copy() *Table {
	nt := &Table{
		TableID:     t.TableID,
		Count:       t.Count,
		ModifyCount: t.ModifyCount,
		Version:     t.Version,
		Pseudo:      t.Pseudo,
		Columns:     make(map[int64]*Column),
		Indices:     make(map[int64]*Index),
		colName2Idx: make(map[string]int64),
		colName2ID:  make(map[string]int64),
	}
	for id, col := range t.Columns {
		nt.Columns[id] = col
	}
	for id, idx := range t.Indices {
		nt.Indices[id] = idx
	}
	for name, id := range t.colName2Idx {
		nt.colName2Idx[name] = id
	}
	for name, id := range t.colName2ID {
		nt.colName2ID[name] = id
	}
	return nt
}

func (t *Table) buildColNameMapper() {
	for id, col := range t.Columns {
		t.colName2ID[col.Info.Name.L] = id
	}
	for id, idx := range t.Indices {
		// use this index to estimate the column stats
		t.colName2Idx[idx.Info.Columns[0].Name.L] = id
	}
}

func (h *Handle) cmSketchFromStorage(tblID int64, isIndex, histID int64) (*CMSketch, error) {
	selSQL := fmt.Sprintf("select cm_sketch from mysql.stats_histograms where table_id = %d and is_index = %d and hist_id = %d", tblID, isIndex, histID)
	rows, _, err := h.restrictedExec.ExecRestrictedSQL(nil, selSQL)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return decodeCMSketch(rows[0].GetBytes(0))
}

func (h *Handle) indexStatsFromStorage(row types.Row, table *Table, tableInfo *model.TableInfo) error {
	histID := row.GetInt64(2)
	distinct := row.GetInt64(3)
	histVer := row.GetUint64(4)
	nullCount := row.GetInt64(5)
	idx := table.Indices[histID]
	errorRate := ErrorRate{}
	if isAnalyzed(row.GetInt64(8)) {
		h.mu.Lock()
		h.mu.rateMap.clear(table.TableID, histID, true)
		h.mu.Unlock()
	} else if idx != nil {
		errorRate = idx.ErrorRate
	}
	for _, idxInfo := range tableInfo.Indices {
		if histID != idxInfo.ID {
			continue
		}
		if idx == nil || idx.LastUpdateVersion < histVer {
			hg, err := h.histogramFromStorage(tableInfo.ID, histID, types.NewFieldType(mysql.TypeBlob), distinct, 1, histVer, nullCount, 0)
			if err != nil {
				return errors.Trace(err)
			}
			cms, err := h.cmSketchFromStorage(tableInfo.ID, 1, idxInfo.ID)
			if err != nil {
				return errors.Trace(err)
			}
			idx = &Index{Histogram: *hg, CMSketch: cms, Info: idxInfo, ErrorRate: errorRate, statsVer: row.GetInt64(7)}
		}
		break
	}
	if idx != nil {
		table.Indices[histID] = idx
	} else {
		log.Debugf("We cannot find index id %d in table info %s now. It may be deleted.", histID, tableInfo.Name)
	}
	return nil
}

func (h *Handle) columnStatsFromStorage(row types.Row, table *Table, tableInfo *model.TableInfo, loadAll bool) error {
	histID := row.GetInt64(2)
	distinct := row.GetInt64(3)
	histVer := row.GetUint64(4)
	nullCount := row.GetInt64(5)
	totColSize := row.GetInt64(6)
	col := table.Columns[histID]
	errorRate := ErrorRate{}
	if isAnalyzed(row.GetInt64(8)) {
		h.mu.Lock()
		h.mu.rateMap.clear(table.TableID, histID, false)
		h.mu.Unlock()
	} else if col != nil {
		errorRate = col.ErrorRate
	}
	for _, colInfo := range tableInfo.Columns {
		if histID != colInfo.ID {
			continue
		}
		isHandle := tableInfo.PKIsHandle && mysql.HasPriKeyFlag(colInfo.Flag)
		// We will not load buckets if:
		// 1. Lease > 0, and:
		// 2. this column is not handle, and:
		// 3. the column doesn't has buckets before, and:
		// 4. loadAll is false.
		notNeedLoad := h.Lease > 0 &&
			!isHandle &&
			(col == nil || col.Len() == 0 && col.LastUpdateVersion < histVer) &&
			!loadAll
		if notNeedLoad {
			count, err := h.columnCountFromStorage(table.TableID, histID)
			if err != nil {
				return errors.Trace(err)
			}
			col = &Column{
				Histogram: Histogram{
					ID:                histID,
					NDV:               distinct,
					NullCount:         nullCount,
					tp:                &colInfo.FieldType,
					LastUpdateVersion: histVer,
					TotColSize:        totColSize,
				},
				Info:      colInfo,
				Count:     count + nullCount,
				ErrorRate: errorRate,
			}
			break
		}
		if col == nil || col.LastUpdateVersion < histVer || loadAll {
			hg, err := h.histogramFromStorage(tableInfo.ID, histID, &colInfo.FieldType, distinct, 0, histVer, nullCount, totColSize)
			if err != nil {
				return errors.Trace(err)
			}
			cms, err := h.cmSketchFromStorage(tableInfo.ID, 0, colInfo.ID)
			if err != nil {
				return errors.Trace(err)
			}
			col = &Column{
				Histogram: *hg,
				Info:      colInfo,
				CMSketch:  cms,
				Count:     int64(hg.totalRowCount()),
				ErrorRate: errorRate,
			}
		}
		break
	}
	if col != nil {
		table.Columns[col.ID] = col
	} else {
		// If we didn't find a Column or Index in tableInfo, we won't load the histogram for it.
		// But don't worry, next lease the ddl will be updated, and we will load a same table for two times to
		// avoid error.
		log.Debugf("We cannot find column id %d in table info %s now. It may be deleted.", histID, tableInfo.Name)
	}
	return nil
}

// tableStatsFromStorage loads table stats info from storage.
func (h *Handle) tableStatsFromStorage(tableInfo *model.TableInfo, loadAll bool) (*Table, error) {
	table, ok := h.statsCache.Load().(statsCache)[tableInfo.ID]
	// If table stats is pseudo, we also need to copy it, since we will use the column stats when
	// the average error rate of it is small.
	if !ok {
		table = &Table{
			TableID:     tableInfo.ID,
			Columns:     make(map[int64]*Column, len(tableInfo.Columns)),
			Indices:     make(map[int64]*Index, len(tableInfo.Indices)),
			colName2Idx: make(map[string]int64),
			colName2ID:  make(map[string]int64),
		}
	} else {
		// We copy it before writing to avoid race.
		table = table.copy()
	}
	table.Pseudo = false
	selSQL := fmt.Sprintf("select table_id, is_index, hist_id, distinct_count, version, null_count, tot_col_size, stats_ver, flag from mysql.stats_histograms where table_id = %d", tableInfo.ID)
	rows, _, err := h.restrictedExec.ExecRestrictedSQL(nil, selSQL)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Check deleted table.
	if len(rows) == 0 {
		return nil, nil
	}
	for _, row := range rows {
		if row.GetInt64(1) > 0 {
			if err := h.indexStatsFromStorage(row, table, tableInfo); err != nil {
				return nil, errors.Trace(err)
			}
		} else {
			if err := h.columnStatsFromStorage(row, table, tableInfo, loadAll); err != nil {
				return nil, errors.Trace(err)
			}
		}
	}
	table.buildColNameMapper()
	return table, nil
}

// String implements Stringer interface.
func (t *Table) String() string {
	strs := make([]string, 0, len(t.Columns)+1)
	strs = append(strs, fmt.Sprintf("Table:%d Count:%d", t.TableID, t.Count))
	for _, col := range t.Columns {
		strs = append(strs, col.String())
	}
	for _, col := range t.Indices {
		strs = append(strs, col.String())
	}
	return strings.Join(strs, "\n")
}

type tableColumnID struct {
	tableID  int64
	columnID int64
}

type neededColumnMap struct {
	m    sync.Mutex
	cols map[tableColumnID]struct{}
}

func (n *neededColumnMap) allCols() []tableColumnID {
	n.m.Lock()
	keys := make([]tableColumnID, 0, len(n.cols))
	for key := range n.cols {
		keys = append(keys, key)
	}
	n.m.Unlock()
	return keys
}

func (n *neededColumnMap) insert(col tableColumnID) {
	n.m.Lock()
	n.cols[col] = struct{}{}
	n.m.Unlock()
}

func (n *neededColumnMap) delete(col tableColumnID) {
	n.m.Lock()
	delete(n.cols, col)
	n.m.Unlock()
}

var histogramNeededColumns = neededColumnMap{cols: map[tableColumnID]struct{}{}}

// RatioOfPseudoEstimate means if modifyCount / statsTblCount is greater than this ratio, we think the stats is invalid
// and use pseudo estimation.
var RatioOfPseudoEstimate = 0.7

// IsOutdated returns true if the table stats is outdated.
func (t *Table) IsOutdated() bool {
	if t.Count > 0 && float64(t.ModifyCount)/float64(t.Count) > RatioOfPseudoEstimate {
		return true
	}
	return false
}

// ColumnIsInvalid checks if this column is invalid. If this column has histogram but not loaded yet, then we mark it
// as need histogram.
func (t *Table) ColumnIsInvalid(sc *stmtctx.StatementContext, colID int64) bool {
	col, ok := t.Columns[colID]
	if !ok || t.Pseudo && col.IsPseudo() {
		return true
	}
	if col.NDV > 0 && col.Len() == 0 {
		sc.SetHistogramsNotLoad()
		histogramNeededColumns.insert(tableColumnID{tableID: t.TableID, columnID: colID})
	}
	return col.totalRowCount() == 0 || (col.NDV > 0 && col.Len() == 0)
}

// ColumnGreaterRowCount estimates the row count where the column greater than value.
func (t *Table) ColumnGreaterRowCount(sc *stmtctx.StatementContext, value types.Datum, colID int64) float64 {
	if t.ColumnIsInvalid(sc, colID) {
		return float64(t.Count) / pseudoLessRate
	}
	c := t.Columns[colID]
	return c.greaterRowCount(value) * c.getIncreaseFactor(t.Count)
}

// ColumnLessRowCount estimates the row count where the column less than value.
func (t *Table) ColumnLessRowCount(sc *stmtctx.StatementContext, value types.Datum, colID int64) float64 {
	if t.ColumnIsInvalid(sc, colID) {
		return float64(t.Count) / pseudoLessRate
	}
	c := t.Columns[colID]
	return c.lessRowCount(value) * c.getIncreaseFactor(t.Count)
}

// ColumnBetweenRowCount estimates the row count where column greater or equal to a and less than b.
func (t *Table) ColumnBetweenRowCount(sc *stmtctx.StatementContext, a, b types.Datum, colID int64) float64 {
	if t.ColumnIsInvalid(sc, colID) {
		return float64(t.Count) / pseudoBetweenRate
	}
	c := t.Columns[colID]
	return c.betweenRowCount(a, b) * c.getIncreaseFactor(t.Count)
}

// ColumnEqualRowCount estimates the row count where the column equals to value.
func (t *Table) ColumnEqualRowCount(sc *stmtctx.StatementContext, value types.Datum, colID int64) (float64, error) {
	if t.ColumnIsInvalid(sc, colID) {
		return float64(t.Count) / pseudoEqualRate, nil
	}
	c := t.Columns[colID]
	result, err := c.equalRowCount(sc, value)
	result *= c.getIncreaseFactor(t.Count)
	return result, errors.Trace(err)
}

// GetRowCountByIntColumnRanges estimates the row count by a slice of IntColumnRange.
func (t *Table) GetRowCountByIntColumnRanges(sc *stmtctx.StatementContext, colID int64, intRanges []*ranger.Range) (float64, error) {
	if t.ColumnIsInvalid(sc, colID) {
		if len(intRanges) == 0 {
			return float64(t.Count), nil
		}
		if intRanges[0].LowVal[0].Kind() == types.KindInt64 {
			return getPseudoRowCountBySignedIntRanges(intRanges, float64(t.Count)), nil
		}
		return getPseudoRowCountByUnsignedIntRanges(intRanges, float64(t.Count)), nil
	}
	c := t.Columns[colID]
	result, err := c.getColumnRowCount(sc, intRanges)
	result *= c.getIncreaseFactor(t.Count)
	return result, errors.Trace(err)
}

// GetRowCountByColumnRanges estimates the row count by a slice of Range.
func (t *Table) GetRowCountByColumnRanges(sc *stmtctx.StatementContext, colID int64, colRanges []*ranger.Range) (float64, error) {
	if t.ColumnIsInvalid(sc, colID) {
		return getPseudoRowCountByColumnRanges(sc, float64(t.Count), colRanges, 0)
	}
	c := t.Columns[colID]
	result, err := c.getColumnRowCount(sc, colRanges)
	result *= c.getIncreaseFactor(t.Count)
	return result, errors.Trace(err)
}

// GetRowCountByIndexRanges estimates the row count by a slice of Range.
func (t *Table) GetRowCountByIndexRanges(sc *stmtctx.StatementContext, idxID int64, indexRanges []*ranger.Range) (float64, error) {
	idx := t.Indices[idxID]
	if idx == nil || t.Pseudo && idx.IsPseudo() || idx.Len() == 0 {
		colsLen := -1
		if idx != nil && idx.Info.Unique {
			colsLen = len(idx.Info.Columns)
		}
		return getPseudoRowCountByIndexRanges(sc, indexRanges, float64(t.Count), colsLen)
	}
	var result float64
	var err error
	if idx.CMSketch != nil && idx.statsVer == version1 {
		result, err = t.getIndexRowCount(sc, idx, indexRanges)
	} else {
		result, err = idx.getRowCount(sc, indexRanges)
	}
	result *= idx.getIncreaseFactor(t.Count)
	return result, errors.Trace(err)
}

// PseudoAvgCountPerCount gets a pseudo average count if histogram not exists.
func (t *Table) PseudoAvgCountPerCount() float64 {
	return float64(t.Count) / pseudoEqualRate
}

// getOrdinalOfRangeCond gets the ordinal of the position range condition,
// if not exist, it returns the end position.
func getOrdinalOfRangeCond(sc *stmtctx.StatementContext, ran *ranger.Range) int {
	for i := range ran.LowVal {
		a, b := ran.LowVal[i], ran.HighVal[i]
		cmp, err := a.CompareDatum(sc, &b)
		if err != nil {
			return 0
		}
		if cmp != 0 {
			return i
		}
	}
	return len(ran.LowVal)
}

func (t *Table) getIndexRowCount(sc *stmtctx.StatementContext, idx *Index, indexRanges []*ranger.Range) (float64, error) {
	totalCount := float64(0)
	for _, ran := range indexRanges {
		rangePosition := getOrdinalOfRangeCond(sc, ran)
		// first one is range, just use the previous way to estimate
		if rangePosition == 0 {
			count, err := idx.getRowCount(sc, []*ranger.Range{ran})
			if err != nil {
				return 0, errors.Trace(err)
			}
			totalCount += count
			continue
		}
		selectivity := 1.0
		// use CM Sketch to estimate the equal conditions
		bytes, err := codec.EncodeKey(sc, nil, ran.LowVal[:rangePosition]...)
		if err != nil {
			return 0, errors.Trace(err)
		}
		selectivity = float64(idx.CMSketch.queryBytes(bytes)) / float64(t.Count)
		// use histogram to estimate the range condition
		if rangePosition != len(ran.LowVal) {
			rang := ranger.Range{
				LowVal:      []types.Datum{ran.LowVal[rangePosition]},
				LowExclude:  ran.LowExclude,
				HighVal:     []types.Datum{ran.HighVal[rangePosition]},
				HighExclude: ran.HighExclude,
			}
			var count float64
			var err error
			colName := idx.Info.Columns[rangePosition].Name.L
			// prefer index stats over column stats
			if idx, ok := t.colName2Idx[colName]; ok {
				count, err = t.GetRowCountByIndexRanges(sc, idx, []*ranger.Range{&rang})
			} else {
				count, err = t.GetRowCountByColumnRanges(sc, t.colName2ID[colName], []*ranger.Range{&rang})
			}
			if err != nil {
				return 0, errors.Trace(err)
			}
			selectivity = selectivity * count / float64(t.Count)
		}
		totalCount += selectivity * float64(t.Count)
	}
	if totalCount > idx.totalRowCount() {
		totalCount = idx.totalRowCount()
	}
	return totalCount, nil
}

// PseudoTable creates a pseudo table statistics.
func PseudoTable(tblInfo *model.TableInfo) *Table {
	t := &Table{
		TableID:    tblInfo.ID,
		Pseudo:     true,
		Count:      pseudoRowCount,
		PKIsHandle: tblInfo.PKIsHandle,
		Columns:    make(map[int64]*Column, len(tblInfo.Columns)),
		Indices:    make(map[int64]*Index, len(tblInfo.Indices)),
	}
	for _, col := range tblInfo.Columns {
		if col.State == model.StatePublic {
			t.Columns[col.ID] = &Column{Info: col}
		}
	}
	for _, idx := range tblInfo.Indices {
		if idx.State == model.StatePublic {
			t.Indices[idx.ID] = &Index{Info: idx}
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
		rowCount, err := getPseudoRowCountByColumnRanges(sc, tableRowCount, []*ranger.Range{indexRange}, i)
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

func getPseudoRowCountByColumnRanges(sc *stmtctx.StatementContext, tableRowCount float64, columnRanges []*ranger.Range, colIdx int) (float64, error) {
	var rowCount float64
	var err error
	for _, ran := range columnRanges {
		if ran.LowVal[colIdx].Kind() == types.KindNull && ran.HighVal[colIdx].Kind() == types.KindMaxValue {
			rowCount += tableRowCount
		} else if ran.LowVal[colIdx].Kind() == types.KindMinNotNull {
			var nullCount float64
			nullCount = tableRowCount / pseudoEqualRate
			if ran.HighVal[colIdx].Kind() == types.KindMaxValue {
				rowCount += tableRowCount - nullCount
			} else if err == nil {
				lessCount := tableRowCount / pseudoLessRate
				rowCount += lessCount - nullCount
			}
		} else if ran.HighVal[colIdx].Kind() == types.KindMaxValue {
			rowCount += tableRowCount / pseudoLessRate
		} else {
			compare, err1 := ran.LowVal[colIdx].CompareDatum(sc, &ran.HighVal[colIdx])
			if err1 != nil {
				return 0, errors.Trace(err1)
			}
			if compare == 0 {
				rowCount += tableRowCount / pseudoEqualRate
			} else {
				rowCount += tableRowCount / pseudoBetweenRate
			}
		}
		if err != nil {
			return 0, errors.Trace(err)
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
