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

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/types"
)

const (
	// Default number of buckets a column histogram has.
	defaultBucketCount = 256

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
	Count       int64 // Total row count in a table.
	ModifyCount int64 // Total modify count in a table.
	Version     uint64
	Pseudo      bool
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
func (h *Handle) tableStatsFromStorage(tableInfo *model.TableInfo) (*Table, error) {
	table, ok := h.statsCache.Load().(statsCache)[tableInfo.ID]
	if !ok {
		table = &Table{
			TableID: tableInfo.ID,
			Columns: make(map[int64]*Column, len(tableInfo.Columns)),
			Indices: make(map[int64]*Index, len(tableInfo.Indices)),
		}
	} else {
		// We copy it before writing to avoid race.
		table = table.copy()
	}
	selSQL := fmt.Sprintf("select table_id, is_index, hist_id, distinct_count, version, null_count from mysql.stats_histograms where table_id = %d", tableInfo.ID)
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
		nullCount := row.Data[5].GetInt64()
		if row.Data[1].GetInt64() > 0 {
			// process index
			idx := table.Indices[histID]
			for _, idxInfo := range tableInfo.Indices {
				if histID == idxInfo.ID {
					if idx == nil || idx.LastUpdateVersion < histVer {
						hg, err := histogramFromStorage(h.ctx, tableInfo.ID, histID, nil, distinct, 1, histVer, nullCount)
						if err != nil {
							return nil, errors.Trace(err)
						}
						idx = &Index{Histogram: *hg, Info: idxInfo}
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
						hg, err := histogramFromStorage(h.ctx, tableInfo.ID, histID, &colInfo.FieldType, distinct, 0, histVer, nullCount)
						if err != nil {
							return nil, errors.Trace(err)
						}
						col = &Column{Histogram: *hg, Info: colInfo}
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

// ColumnIsInvalid checks if this column is invalid.
func (t *Table) ColumnIsInvalid(colInfo *model.ColumnInfo) bool {
	if t.Pseudo {
		return true
	}
	col, ok := t.Columns[colInfo.ID]
	return !ok || len(col.Buckets) == 0
}

// ColumnGreaterRowCount estimates the row count where the column greater than value.
func (t *Table) ColumnGreaterRowCount(sc *variable.StatementContext, value types.Datum, colInfo *model.ColumnInfo) (float64, error) {
	if t.ColumnIsInvalid(colInfo) {
		return float64(t.Count) / pseudoLessRate, nil
	}
	hist := t.Columns[colInfo.ID]
	result, err := hist.greaterRowCount(sc, value)
	result *= hist.getIncreaseFactor(t.Count)
	return result, errors.Trace(err)
}

// ColumnLessRowCount estimates the row count where the column less than value.
func (t *Table) ColumnLessRowCount(sc *variable.StatementContext, value types.Datum, colInfo *model.ColumnInfo) (float64, error) {
	if t.ColumnIsInvalid(colInfo) {
		return float64(t.Count) / pseudoLessRate, nil
	}
	hist := t.Columns[colInfo.ID]
	result, err := hist.lessRowCount(sc, value)
	result *= hist.getIncreaseFactor(t.Count)
	return result, errors.Trace(err)
}

// ColumnBetweenRowCount estimates the row count where column greater or equal to a and less than b.
func (t *Table) ColumnBetweenRowCount(sc *variable.StatementContext, a, b types.Datum, colInfo *model.ColumnInfo) (float64, error) {
	if t.ColumnIsInvalid(colInfo) {
		return float64(t.Count) / pseudoBetweenRate, nil
	}
	hist := t.Columns[colInfo.ID]
	result, err := hist.betweenRowCount(sc, a, b)
	result *= hist.getIncreaseFactor(t.Count)
	return result, errors.Trace(err)
}

// ColumnEqualRowCount estimates the row count where the column equals to value.
func (t *Table) ColumnEqualRowCount(sc *variable.StatementContext, value types.Datum, colInfo *model.ColumnInfo) (float64, error) {
	if t.ColumnIsInvalid(colInfo) {
		return float64(t.Count) / pseudoEqualRate, nil
	}
	hist := t.Columns[colInfo.ID]
	result, err := hist.equalRowCount(sc, value)
	result *= hist.getIncreaseFactor(t.Count)
	return result, errors.Trace(err)
}

// GetRowCountByIntColumnRanges estimates the row count by a slice of IntColumnRange.
func (t *Table) GetRowCountByIntColumnRanges(sc *variable.StatementContext, colID int64, intRanges []types.IntColumnRange) (float64, error) {
	c := t.Columns[colID]
	if t.Pseudo || c == nil || len(c.Buckets) == 0 {
		return getPseudoRowCountByIntRanges(intRanges, float64(t.Count)), nil
	}
	result, err := c.getIntColumnRowCount(sc, intRanges)
	result *= c.getIncreaseFactor(t.Count)
	return result, errors.Trace(err)
}

// GetRowCountByColumnRanges estimates the row count by a slice of ColumnRange.
func (t *Table) GetRowCountByColumnRanges(sc *variable.StatementContext, colID int64, colRanges []*types.ColumnRange) (float64, error) {
	c := t.Columns[colID]
	if t.Pseudo || c == nil || len(c.Buckets) == 0 {
		return getPseudoRowCountByColumnRanges(sc, float64(t.Count), colRanges)
	}
	result, err := c.getColumnRowCount(sc, colRanges)
	result *= c.getIncreaseFactor(t.Count)
	return result, errors.Trace(err)
}

// GetRowCountByIndexRanges estimates the row count by a slice of IndexRange.
func (t *Table) GetRowCountByIndexRanges(sc *variable.StatementContext, idxID int64, indexRanges []*types.IndexRange) (float64, error) {
	idx := t.Indices[idxID]
	if t.Pseudo || idx == nil || len(idx.Buckets) == 0 {
		return getPseudoRowCountByIndexRanges(sc, indexRanges, float64(t.Count))
	}
	result, err := idx.getRowCount(sc, indexRanges)
	result *= idx.getIncreaseFactor(t.Count)
	return result, errors.Trace(err)
}

// PseudoTable creates a pseudo table statistics when statistic can not be found in KV store.
func PseudoTable(tableID int64) *Table {
	t := &Table{TableID: tableID, Pseudo: true}
	t.Count = pseudoRowCount
	t.Columns = make(map[int64]*Column)
	t.Indices = make(map[int64]*Index)
	return t
}

func getPseudoRowCountByIndexRanges(sc *variable.StatementContext, indexRanges []*types.IndexRange,
	tableRowCount float64) (float64, error) {
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
		if i >= len(indexRange.LowVal) {
			i = len(indexRange.LowVal) - 1
		}
		colRange := []*types.ColumnRange{{Low: indexRange.LowVal[i], High: indexRange.HighVal[i]}}
		rowCount, err := getPseudoRowCountByColumnRanges(sc, tableRowCount, colRange)
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

func getPseudoRowCountByColumnRanges(sc *variable.StatementContext, tableRowCount float64, columnRanges []*types.ColumnRange) (float64, error) {
	var rowCount float64
	var err error
	for _, ran := range columnRanges {
		if ran.Low.Kind() == types.KindNull && ran.High.Kind() == types.KindMaxValue {
			rowCount += tableRowCount
		} else if ran.Low.Kind() == types.KindMinNotNull {
			var nullCount float64
			nullCount = tableRowCount / pseudoEqualRate
			if ran.High.Kind() == types.KindMaxValue {
				rowCount += tableRowCount - nullCount
			} else if err == nil {
				lessCount := tableRowCount / pseudoLessRate
				rowCount += lessCount - nullCount
			}
		} else if ran.High.Kind() == types.KindMaxValue {
			rowCount += tableRowCount / pseudoLessRate
		} else {
			compare, err1 := ran.Low.CompareDatum(sc, &ran.High)
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
