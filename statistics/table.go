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

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/types"
)

const (
	// Default number of buckets a column histogram has.
	defaultBucketCount = 256

	// When we haven't analyzed a table, we use pseudo statistics to estimate costs.
	// It has row count 10000000, equal condition selects 1/1000 of total rows, less condition selects 1/3 of total rows,
	// between condition selects 1/40 of total rows.
	pseudoRowCount    = 10000000
	pseudoEqualRate   = 1000
	pseudoLessRate    = 3
	pseudoBetweenRate = 40
)

// Table represents statistics for a table.
type Table struct {
	TableID int64
	Columns map[int64]*Column
	Indices map[int64]*Index
	Count   int64 // Total row count in a table.
	Pseudo  bool
}

func (t *Table) copy() *Table {
	nt := &Table{
		TableID: t.TableID,
		Count:   t.Count,
		Pseudo:  t.Pseudo,
		Columns: make(map[int64]*Column),
		Indices: make(map[int64]*Index),
	}
	for id, col := range t.Columns {
		nt.Columns[id] = col
	}
	for id, idx := range t.Indices {
		nt.Indices[id] = idx
	}
	return nt
}

// tableStatsFromStorage loads table stats info from storage.
func (h *Handle) tableStatsFromStorage(tableInfo *model.TableInfo, count int64) (*Table, error) {
	table, ok := h.statsCache.Load().(statsCache)[tableInfo.ID]
	if !ok {
		table = &Table{
			Columns: make(map[int64]*Column, len(tableInfo.Columns)),
			Indices: make(map[int64]*Index, len(tableInfo.Indices)),
		}
	} else {
		// We copy it before writing to avoid race.
		table = table.copy()
	}
	table.TableID = tableInfo.ID
	table.Count = count

	selSQL := fmt.Sprintf("select table_id, is_index, hist_id, distinct_count, version from mysql.stats_histograms where table_id = %d", tableInfo.ID)
	rows, _, err := h.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(h.ctx, selSQL)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Check deleted table.
	if len(rows) == 0 {
		return nil, nil
	}
	for _, row := range rows {
		distinct := row.Data[3].GetInt64()
		histID := row.Data[2].GetInt64()
		histVer := row.Data[4].GetUint64()
		if row.Data[1].GetInt64() > 0 {
			// process index
			idx := table.Indices[histID]
			for _, idxInfo := range tableInfo.Indices {
				if histID == idxInfo.ID {
					if idx == nil || idx.LastUpdateVersion < histVer {
						hg, err := h.histogramFromStorage(tableInfo.ID, histID, nil, distinct, 1, histVer)
						if err != nil {
							return nil, errors.Trace(err)
						}
						idx = &Index{Histogram: *hg}
					}
					break
				}
			}
			if idx != nil {
				table.Indices[histID] = idx
			} else {
				log.Warnf("We cannot find index id %d in table info %s now. It may be deleted.", histID, tableInfo.Name)
			}
		} else {
			// process column
			col := table.Columns[histID]
			for _, colInfo := range tableInfo.Columns {
				if histID == colInfo.ID {
					if col == nil || col.LastUpdateVersion < histVer {
						hg, err := h.histogramFromStorage(tableInfo.ID, histID, &colInfo.FieldType, distinct, 0, histVer)
						if err != nil {
							return nil, errors.Trace(err)
						}
						col = &Column{Histogram: *hg}
					}
					break
				}
			}
			if col != nil {
				table.Columns[col.ID] = col
			} else {
				// If we didn't find a Column or Index in tableInfo, we won't load the histogram for it.
				// But don't worry, next lease the ddl will be updated, and we will load a same table for two times to
				// avoid error.
				log.Warnf("We cannot find column id %d in table info %s now. It may be deleted.", histID, tableInfo.Name)
			}
		}
	}
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

// columnIsInvalid checks if this column is invalid.
func (t *Table) columnIsInvalid(colInfo *model.ColumnInfo) bool {
	if t.Pseudo {
		return true
	}
	_, ok := t.Columns[colInfo.ID]
	return !ok
}

// ColumnGreaterRowCount estimates the row count where the column greater than value.
func (t *Table) ColumnGreaterRowCount(sc *variable.StatementContext, value types.Datum, colInfo *model.ColumnInfo) (float64, error) {
	if t.columnIsInvalid(colInfo) {
		return float64(t.Count) / pseudoLessRate, nil
	}
	return t.Columns[colInfo.ID].greaterRowCount(sc, value)
}

// ColumnLessRowCount estimates the row count where the column less than value.
func (t *Table) ColumnLessRowCount(sc *variable.StatementContext, value types.Datum, colInfo *model.ColumnInfo) (float64, error) {
	if t.columnIsInvalid(colInfo) {
		return float64(t.Count) / pseudoLessRate, nil
	}
	return t.Columns[colInfo.ID].lessRowCount(sc, value)
}

// ColumnBetweenRowCount estimates the row count where column greater or equal to a and less than b.
func (t *Table) ColumnBetweenRowCount(sc *variable.StatementContext, a, b types.Datum, colInfo *model.ColumnInfo) (float64, error) {
	if t.columnIsInvalid(colInfo) {
		return float64(t.Count) / pseudoBetweenRate, nil
	}
	return t.Columns[colInfo.ID].betweenRowCount(sc, a, b)
}

// ColumnEqualRowCount estimates the row count where the column equals to value.
func (t *Table) ColumnEqualRowCount(sc *variable.StatementContext, value types.Datum, colInfo *model.ColumnInfo) (float64, error) {
	if t.columnIsInvalid(colInfo) {
		return float64(t.Count) / pseudoEqualRate, nil
	}
	return t.Columns[colInfo.ID].equalRowCount(sc, value)
}

// GetRowCountByIntColumnRanges estimates the row count by a slice of IntColumnRange.
func (t *Table) GetRowCountByIntColumnRanges(sc *variable.StatementContext, colID int64, intRanges []types.IntColumnRange) (float64, error) {
	c := t.Columns[colID]
	if t.Pseudo || c == nil || len(c.Buckets) == 0 {
		return getPseudoRowCountByIntRanges(intRanges, float64(t.Count)), nil
	}
	return c.getIntColumnRowCount(sc, intRanges, float64(t.Count))
}

// GetRowCountByIndexRanges estimates the row count by a slice of IndexRange.
func (t *Table) GetRowCountByIndexRanges(sc *variable.StatementContext, idxID int64, indexRanges []*types.IndexRange, inAndEQCnt int) (float64, error) {
	idx := t.Indices[idxID]
	if t.Pseudo || idx == nil || len(idx.Buckets) == 0 {
		return getPseudoRowCountByIndexRanges(sc, indexRanges, inAndEQCnt, float64(t.Count))
	}
	return idx.getRowCount(sc, indexRanges, inAndEQCnt)
}

// PseudoTable creates a pseudo table statistics when statistic can not be found in KV store.
func PseudoTable(tableID int64) *Table {
	t := &Table{TableID: tableID, Pseudo: true}
	t.Count = pseudoRowCount
	t.Columns = make(map[int64]*Column)
	t.Indices = make(map[int64]*Index)
	return t
}

func getPseudoRowCountByIndexRanges(sc *variable.StatementContext, indexRanges []*types.IndexRange, inAndEQCnt int,
	tableRowCount float64) (float64, error) {
	var totalCount float64
	for _, indexRange := range indexRanges {
		count := tableRowCount
		i := len(indexRange.LowVal) - 1
		if i > inAndEQCnt {
			i = inAndEQCnt
		}
		colRange := types.ColumnRange{Low: indexRange.LowVal[i], High: indexRange.HighVal[i]}
		rowCount, err := getPseudoRowCountByColumnRange(sc, tableRowCount, colRange)
		if err != nil {
			return 0, errors.Trace(err)
		}
		count = count / float64(tableRowCount) * float64(rowCount)
		// If the condition is a = 1, b = 1, c = 1, d = 1, we think every a=1, b=1, c=1 only filtrate 1/100 data,
		// so as to avoid collapsing too fast.
		for j := 0; j < i; j++ {
			count = count / float64(100)
		}
		totalCount += count
	}
	// To avoid the totalCount become too small.
	if uint64(totalCount) < 1000 {
		// We will not let the row count less than 1000 to avoid collapsing too fast in the future calculation.
		totalCount = 1000.0
	}
	if totalCount > tableRowCount {
		totalCount = tableRowCount / 3.0
	}
	return totalCount, nil
}

func getPseudoRowCountByColumnRange(sc *variable.StatementContext, tableRowCount float64, ran types.ColumnRange) (float64, error) {
	var rowCount float64
	var err error
	if ran.Low.Kind() == types.KindNull && ran.High.Kind() == types.KindMaxValue {
		return tableRowCount, nil
	} else if ran.Low.Kind() == types.KindMinNotNull {
		var nullCount float64
		nullCount = tableRowCount / pseudoEqualRate
		if ran.High.Kind() == types.KindMaxValue {
			rowCount = tableRowCount - nullCount
		} else if err == nil {
			lessCount := tableRowCount / pseudoLessRate
			rowCount = lessCount - nullCount
		}
	} else if ran.High.Kind() == types.KindMaxValue {
		rowCount = tableRowCount / pseudoLessRate
	} else {
		compare, err1 := ran.Low.CompareDatum(sc, ran.High)
		if err1 != nil {
			return 0, errors.Trace(err1)
		}
		if compare == 0 {
			rowCount = tableRowCount / pseudoEqualRate
		} else {
			rowCount = tableRowCount / pseudoBetweenRate
		}
	}
	if err != nil {
		return 0, errors.Trace(err)
	}
	return rowCount, nil
}

func getPseudoRowCountByIntRanges(intRanges []types.IntColumnRange, tableRowCount float64) float64 {
	var rowCount float64
	for _, rg := range intRanges {
		var cnt float64
		if rg.LowVal == math.MinInt64 && rg.HighVal == math.MaxInt64 {
			cnt = tableRowCount
		} else if rg.LowVal == math.MinInt64 {
			cnt = tableRowCount / pseudoLessRate
		} else if rg.HighVal == math.MaxInt64 {
			cnt = tableRowCount / pseudoLessRate
		} else {
			if rg.LowVal == rg.HighVal {
				cnt = tableRowCount / pseudoEqualRate
			} else {
				cnt = tableRowCount / pseudoBetweenRate
			}
		}
		if rg.HighVal-rg.LowVal > 0 && cnt > float64(rg.HighVal-rg.LowVal) {
			cnt = float64(rg.HighVal - rg.LowVal)
		}
		rowCount += cnt
	}
	if rowCount > tableRowCount {
		rowCount = tableRowCount
	}
	return rowCount
}
