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

package perfschema

import (
	"fmt"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/stmtsummary"
)

const (
	tableNameEventsStatementsSummaryByDigest = "events_statements_summary_by_digest"
)

// perfSchemaTable stands for the fake table all its data is in the memory.
type perfSchemaTable struct {
	infoschema.VirtualTable
	meta *model.TableInfo
	cols []*table.Column
}

var pluginTable = make(map[string]func(autoid.Allocator, *model.TableInfo) (table.Table, error))

// RegisterTable registers a new table into TiDB.
func RegisterTable(tableName, sql string,
	tableFromMeta func(autoid.Allocator, *model.TableInfo) (table.Table, error)) {
	perfSchemaTables = append(perfSchemaTables, sql)
	pluginTable[tableName] = tableFromMeta
}

func tableFromMeta(alloc autoid.Allocator, meta *model.TableInfo) (table.Table, error) {
	if f, ok := pluginTable[meta.Name.L]; ok {
		ret, err := f(alloc, meta)
		return ret, err
	}
	return createPerfSchemaTable(meta), nil
}

// createPerfSchemaTable creates all perfSchemaTables
func createPerfSchemaTable(meta *model.TableInfo) *perfSchemaTable {
	columns := make([]*table.Column, 0, len(meta.Columns))
	for _, colInfo := range meta.Columns {
		col := table.ToColumn(colInfo)
		columns = append(columns, col)
	}
	t := &perfSchemaTable{
		meta: meta,
		cols: columns,
	}
	return t
}

// Cols implements table.Table Type interface.
func (vt *perfSchemaTable) Cols() []*table.Column {
	return vt.cols
}

// WritableCols implements table.Table Type interface.
func (vt *perfSchemaTable) WritableCols() []*table.Column {
	return vt.cols
}

// GetID implements table.Table GetID interface.
func (vt *perfSchemaTable) GetPhysicalID() int64 {
	return vt.meta.ID
}

// Meta implements table.Table Type interface.
func (vt *perfSchemaTable) Meta() *model.TableInfo {
	return vt.meta
}

func (vt *perfSchemaTable) getRows(ctx sessionctx.Context, cols []*table.Column) (fullRows [][]types.Datum, err error) {
	switch vt.meta.Name.O {
	case tableNameEventsStatementsSummaryByDigest:
		fullRows = dataForEventsStatementsSummaryByDigest(ctx)
	}
	if len(cols) == len(vt.cols) {
		return
	}
	rows := make([][]types.Datum, len(fullRows))
	for i, fullRow := range fullRows {
		row := make([]types.Datum, len(cols))
		for j, col := range cols {
			row[j] = fullRow[col.Offset]
		}
		rows[i] = row
	}
	return rows, nil
}

// IterRecords implements table.Table IterRecords interface.
func (vt *perfSchemaTable) IterRecords(ctx sessionctx.Context, startKey kv.Key, cols []*table.Column,
	fn table.RecordIterFunc) error {
	if len(startKey) != 0 {
		return table.ErrUnsupportedOp
	}
	rows, err := vt.getRows(ctx, cols)
	if err != nil {
		return err
	}
	for i, row := range rows {
		more, err := fn(int64(i), row, cols)
		if err != nil {
			return err
		}
		if !more {
			break
		}
	}
	return nil
}

// RowWithCols implements table.Table RowWithCols interface.
func (vt *perfSchemaTable) RowWithCols(ctx sessionctx.Context, h int64, cols []*table.Column) ([]types.Datum, error) {
	return nil, table.ErrUnsupportedOp
}

// Row implements table.Table Row interface.
func (vt *perfSchemaTable) Row(ctx sessionctx.Context, h int64) ([]types.Datum, error) {
	return nil, table.ErrUnsupportedOp
}

// Indices implements table.Table Indices interface.
func (vt *perfSchemaTable) Indices() []table.Index {
	return nil
}

// WritableIndices implements table.Table WritableIndices interface.
func (vt *perfSchemaTable) WritableIndices() []table.Index {
	return nil
}

// DeletableIndices implements table.Table DeletableIndices interface.
func (vt *perfSchemaTable) DeletableIndices() []table.Index {
	return nil
}

// RecordPrefix implements table.Table RecordPrefix interface.
func (vt *perfSchemaTable) RecordPrefix() kv.Key {
	return nil
}

// IndexPrefix implements table.Table IndexPrefix interface.
func (vt *perfSchemaTable) IndexPrefix() kv.Key {
	return nil
}

// FirstKey implements table.Table FirstKey interface.
func (vt *perfSchemaTable) FirstKey() kv.Key {
	return nil
}

// RecordKey implements table.Table RecordKey interface.
func (vt *perfSchemaTable) RecordKey(h int64) kv.Key {
	return nil
}

// AddRecord implements table.Table AddRecord interface.
func (vt *perfSchemaTable) AddRecord(ctx sessionctx.Context, r []types.Datum, opts ...table.AddRecordOption) (recordID int64, err error) {
	fmt.Println("add record")
	return 0, table.ErrUnsupportedOp
}

// RemoveRecord implements table.Table RemoveRecord interface.
func (vt *perfSchemaTable) RemoveRecord(ctx sessionctx.Context, h int64, r []types.Datum) error {
	return table.ErrUnsupportedOp
}

// UpdateRecord implements table.Table UpdateRecord interface.
func (vt *perfSchemaTable) UpdateRecord(ctx sessionctx.Context, h int64, oldData, newData []types.Datum, touched []bool) error {
	fmt.Println("update record")
	return table.ErrUnsupportedOp
}

// AllocHandle implements table.Table AllocHandle interface.
func (vt *perfSchemaTable) AllocHandle(ctx sessionctx.Context) (int64, error) {
	return 0, table.ErrUnsupportedOp
}

// Allocator implements table.Table Allocator interface.
func (vt *perfSchemaTable) Allocator(ctx sessionctx.Context) autoid.Allocator {
	return nil
}

// RebaseAutoID implements table.Table RebaseAutoID interface.
func (vt *perfSchemaTable) RebaseAutoID(ctx sessionctx.Context, newBase int64, isSetStep bool) error {
	return table.ErrUnsupportedOp
}

// Seek implements table.Table Seek interface.
func (vt *perfSchemaTable) Seek(ctx sessionctx.Context, h int64) (int64, bool, error) {
	return 0, false, table.ErrUnsupportedOp
}

// Type implements table.Table Type interface.
func (vt *perfSchemaTable) Type() table.Type {
	return table.VirtualTable
}

func dataForEventsStatementsSummaryByDigest(ctx sessionctx.Context) [][]types.Datum {
	return stmtsummary.StmtSummary.ToDatum()
}
