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
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/context"
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
	Info    *model.TableInfo
	Columns map[int64]*Column
	Indices map[int64]*Column
	Count   int64 // Total row count in a table.
	Pseudo  bool
}

// SaveToStorage saves stats table to storage.
func (h *Handle) SaveToStorage(ctx context.Context, t *Table) error {
	_, err := ctx.(sqlexec.SQLExecutor).Execute("begin")
	if err != nil {
		return errors.Trace(err)
	}
	txn := ctx.Txn()
	version := txn.StartTS()
	h.SetTableStats(t.Info.ID, t, version)
	deleteSQL := fmt.Sprintf("delete from mysql.stats_meta where table_id = %d", t.Info.ID)
	_, err = ctx.(sqlexec.SQLExecutor).Execute(deleteSQL)
	if err != nil {
		return errors.Trace(err)
	}
	insertSQL := fmt.Sprintf("insert into mysql.stats_meta (version, table_id, count) values (%d, %d, %d)", version, t.Info.ID, t.Count)
	_, err = ctx.(sqlexec.SQLExecutor).Execute(insertSQL)
	if err != nil {
		return errors.Trace(err)
	}
	deleteSQL = fmt.Sprintf("delete from mysql.stats_histograms where table_id = %d", t.Info.ID)
	_, err = ctx.(sqlexec.SQLExecutor).Execute(deleteSQL)
	if err != nil {
		return errors.Trace(err)
	}
	deleteSQL = fmt.Sprintf("delete from mysql.stats_buckets where table_id = %d", t.Info.ID)
	_, err = ctx.(sqlexec.SQLExecutor).Execute(deleteSQL)
	if err != nil {
		return errors.Trace(err)
	}
	for _, col := range t.Columns {
		err = col.saveToStorage(ctx, t.Info.ID, 0)
		if err != nil {
			return errors.Trace(err)
		}
	}
	for _, idx := range t.Indices {
		err = idx.saveToStorage(ctx, t.Info.ID, 1)
		if err != nil {
			return errors.Trace(err)
		}
	}
	_, err = ctx.(sqlexec.SQLExecutor).Execute("commit")
	return errors.Trace(err)
}

// TableStatsFromStorage loads table stats info from storage.
func (h *Handle) TableStatsFromStorage(ctx context.Context, info *model.TableInfo, count int64) (*Table, error) {
	table := &Table{
		Info:    info,
		Count:   count,
		Columns: make(map[int64]*Column, len(info.Columns)),
		Indices: make(map[int64]*Column, len(info.Indices)),
	}
	selSQL := fmt.Sprintf("select table_id, is_index, hist_id, distinct_count from mysql.stats_histograms where table_id = %d", info.ID)
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, selSQL)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// indexCount and columnCount record the number of indices and columns in table stats. If the number don't match with
	// tableInfo, we will return pseudo table.
	// TODO: In fact, we can return pseudo column.
	indexCount, columnCount := 0, 0
	for _, row := range rows {
		distinct := row.Data[3].GetInt64()
		histID := row.Data[2].GetInt64()
		if row.Data[1].GetInt64() > 0 {
			// process index
			var col *Column
			for _, idxInfo := range info.Indices {
				if histID == idxInfo.ID {
					col, err = colStatsFromStorage(ctx, info.ID, histID, nil, distinct, 1)
					if err != nil {
						return nil, errors.Trace(err)
					}
					break
				}
			}
			if col != nil {
				table.Indices[col.ID] = col
				indexCount++
			} else {
				log.Warnf("We cannot find index id %d in table %s now. It may be deleted.", histID, info.Name)
			}
		} else {
			// process column
			var col *Column
			for _, colInfo := range info.Columns {
				if histID == colInfo.ID {
					col, err = colStatsFromStorage(ctx, info.ID, histID, &colInfo.FieldType, distinct, 0)
					if err != nil {
						return nil, errors.Trace(err)
					}
					break
				}
			}
			if col != nil {
				table.Columns[col.ID] = col
				columnCount++
			} else {
				log.Warnf("We cannot find column id %d in table %s now. It may be deleted.", histID, info.Name)
			}
		}
	}
	if indexCount != len(info.Indices) {
		return nil, errors.New("The number of indices doesn't match with the schema")
	}
	if columnCount != len(info.Columns) {
		return nil, errors.New("The number of columns doesn't match with the schema")
	}
	return table, nil
}

// String implements Stringer interface.
func (t *Table) String() string {
	strs := make([]string, 0, len(t.Columns)+1)
	strs = append(strs, fmt.Sprintf("Table:%d count:%d", t.Info.ID, t.Count))
	for _, col := range t.Columns {
		strs = append(strs, col.String())
	}
	return strings.Join(strs, "\n")
}

// ColumnIsInvalid checks if this column is invalid. Exported for test.
func (t *Table) ColumnIsInvalid(colInfo *model.ColumnInfo) bool {
	if t.Pseudo {
		return true
	}
	_, ok := t.Columns[colInfo.ID]
	return !ok
}

// ColumnGreaterRowCount estimates the row count where the column greater than value.
func (t *Table) ColumnGreaterRowCount(sc *variable.StatementContext, value types.Datum, colInfo *model.ColumnInfo) (int64, error) {
	if t.ColumnIsInvalid(colInfo) {
		return t.Count / pseudoLessRate, nil
	}
	return t.Columns[colInfo.ID].GreaterRowCount(sc, value)
}

// ColumnLessRowCount estimates the row count where the column less than value.
func (t *Table) ColumnLessRowCount(sc *variable.StatementContext, value types.Datum, colInfo *model.ColumnInfo) (int64, error) {
	if t.ColumnIsInvalid(colInfo) {
		return t.Count / pseudoLessRate, nil
	}
	return t.Columns[colInfo.ID].LessRowCount(sc, value)
}

// ColumnBetweenRowCount estimates the row count where column greater or equal to a and less than b.
func (t *Table) ColumnBetweenRowCount(sc *variable.StatementContext, a, b types.Datum, colInfo *model.ColumnInfo) (int64, error) {
	if t.ColumnIsInvalid(colInfo) {
		return t.Count / pseudoBetweenRate, nil
	}
	return t.Columns[colInfo.ID].BetweenRowCount(sc, a, b)
}

// ColumnEqualRowCount estimates the row count where the column equals to value.
func (t *Table) ColumnEqualRowCount(sc *variable.StatementContext, value types.Datum, colInfo *model.ColumnInfo) (int64, error) {
	if t.ColumnIsInvalid(colInfo) {
		return t.Count / pseudoEqualRate, nil
	}
	return t.Columns[colInfo.ID].EqualRowCount(sc, value)
}

// PseudoTable creates a pseudo table statistics when statistic can not be found in KV store.
func PseudoTable(ti *model.TableInfo) *Table {
	t := &Table{Info: ti, Pseudo: true}
	t.Count = pseudoRowCount
	t.Columns = make(map[int64]*Column, len(ti.Columns))
	t.Indices = make(map[int64]*Column, len(ti.Indices))
	for _, v := range ti.Columns {
		c := &Column{
			ID:  v.ID,
			NDV: pseudoRowCount / 2,
		}
		t.Columns[v.ID] = c
	}
	for _, v := range ti.Indices {
		c := &Column{
			ID:  v.ID,
			NDV: pseudoRowCount / 2,
		}
		t.Indices[v.ID] = c
	}
	return t
}
