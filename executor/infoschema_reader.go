// Copyright 2019 PingCAP, Inc.
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


package executor

import (
	"context"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"sort"
)

// InfoschemaReaderExec executes infoschema information retrieving from the cluster components
type InfoschemaReaderExec struct {
	baseExecutor
	t					  table.Table
	iter                  kv.Iterator
	columns               []*model.ColumnInfo
	ChunkList             *chunk.List
	ChunkIdx              int
}

// Open implements the Executor Open interface.
func (e *InfoschemaReaderExec) Open(ctx context.Context) error {
	e.iter = nil
	e.ChunkList = nil
	return nil
}

// Next implements the Executor Next interface.
func (e *InfoschemaReaderExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.GrowAndReset(e.maxChunkSize)
	if e.ChunkList == nil {
		e.ChunkList = chunk.NewList(retTypes(e), e.initCap, e.maxChunkSize)
		columns := make([]*table.Column, e.schema.Len())
		for i, colInfo := range e.columns {
			columns[i] = table.ToColumn(colInfo)
		}
		mutableRow := chunk.MutRowFromTypes(retTypes(e))
		err := e.t.IterRecords(e.ctx,nil, columns, func(h int64, rec []types.Datum, cols []*table.Column) (bool, error) {
			mutableRow.SetDatums(rec...)
			e.ChunkList.AppendRow(mutableRow.ToRow())
			return true, nil
		})
		if err != nil {
			return err
		}
	}
	// no more data.
	if e.ChunkIdx >= e.ChunkList.NumChunks() {
		return nil
	}
	virtualTableChunk := e.ChunkList.GetChunk(e.ChunkIdx)
	e.ChunkIdx++
	chk.SwapColumns(virtualTableChunk)
	return nil
}

func dataForSchemata(ctx sessionctx.Context, schemas []*model.DBInfo) [][]types.Datum {
	checker := privilege.GetPrivilegeManager(ctx)
	rows := make([][]types.Datum, 0, len(schemas))

	for _, schema := range schemas {

		charset := mysql.DefaultCharset
		collation := mysql.DefaultCollationName

		if len(schema.Charset) > 0 {
			charset = schema.Charset // Overwrite default
		}

		if len(schema.Collate) > 0 {
			collation = schema.Collate // Overwrite default
		}

		if checker != nil && !checker.RequestVerification(ctx.GetSessionVars().ActiveRoles, schema.Name.L, "", "", mysql.AllPrivMask) {
			continue
		}
		record := types.MakeDatums(
			infoschema.CatalogVal,    // CATALOG_NAME
			schema.Name.O, // SCHEMA_NAME
			charset,       // DEFAULT_CHARACTER_SET_NAME
			collation,     // DEFAULT_COLLATION_NAME
			nil,
		)
		rows = append(rows, record)
	}
	return rows
}

type infoschemaDataTable struct {
	meta *model.TableInfo
	cols []*table.Column
	tp   table.Type
}

func createInfoschemaDataTable(meta *model.TableInfo) table.Table {
	columns := make([]*table.Column, len(meta.Columns))
	for i, col := range meta.Columns {
		columns[i] = table.ToColumn(col)
	}
	tp := table.VirtualTable
	return &infoschemaDataTable{meta: meta, cols: columns, tp: tp}
}

func (it *infoschemaDataTable) getRows(ctx sessionctx.Context, cols []*table.Column) (fullRows [][]types.Datum, err error) {
	is := infoschema.GetInfoSchema(ctx)
	dbs := is.AllSchemas()
	sort.Sort(infoschema.SchemasSorter(dbs))
	switch it.meta.Name.O {
	case infoschema.TableSchemata:
		fullRows = dataForSchemata(ctx, dbs)
	}
	if err != nil {
		return nil, err
	}
	if len(cols) == len(it.cols) {
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
func (it *infoschemaDataTable) IterRecords(ctx sessionctx.Context, startKey kv.Key, cols []*table.Column,
	fn table.RecordIterFunc) error {
	if len(startKey) != 0 {
		return table.ErrUnsupportedOp
	}
	rows, err := it.getRows(ctx, cols)
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
func (it *infoschemaDataTable) RowWithCols(ctx sessionctx.Context, h int64, cols []*table.Column) ([]types.Datum, error) {
	return nil, table.ErrUnsupportedOp
}

// Row implements table.Table Row interface.
func (it *infoschemaDataTable) Row(ctx sessionctx.Context, h int64) ([]types.Datum, error) {
	return nil, table.ErrUnsupportedOp
}

// Cols implements table.Table Cols interface.
func (it *infoschemaDataTable) Cols() []*table.Column {
	return it.cols
}

// VisibleCols implements table.Table VisibleCols interface.
func (it *infoschemaDataTable) VisibleCols() []*table.Column {
	return it.cols
}

// HiddenCols implements table.Table HiddenCols interface.
func (it *infoschemaDataTable) HiddenCols() []*table.Column {
	return nil
}

// WritableCols implements table.Table WritableCols interface.
func (it *infoschemaDataTable) WritableCols() []*table.Column {
	return it.cols
}

// Indices implements table.Table Indices interface.
func (it *infoschemaDataTable) Indices() []table.Index {
	return nil
}

// WritableIndices implements table.Table WritableIndices interface.
func (it *infoschemaDataTable) WritableIndices() []table.Index {
	return nil
}

// DeletableIndices implements table.Table DeletableIndices interface.
func (it *infoschemaDataTable) DeletableIndices() []table.Index {
	return nil
}

// RecordPrefix implements table.Table RecordPrefix interface.
func (it *infoschemaDataTable) RecordPrefix() kv.Key {
	return nil
}

// IndexPrefix implements table.Table IndexPrefix interface.
func (it *infoschemaDataTable) IndexPrefix() kv.Key {
	return nil
}

// FirstKey implements table.Table FirstKey interface.
func (it *infoschemaDataTable) FirstKey() kv.Key {
	return nil
}

// RecordKey implements table.Table RecordKey interface.
func (it *infoschemaDataTable) RecordKey(h int64) kv.Key {
	return nil
}

// AddRecord implements table.Table AddRecord interface.
func (it *infoschemaDataTable) AddRecord(ctx sessionctx.Context, r []types.Datum, opts ...table.AddRecordOption) (recordID int64, err error) {
	return 0, table.ErrUnsupportedOp
}

// RemoveRecord implements table.Table RemoveRecord interface.
func (it *infoschemaDataTable) RemoveRecord(ctx sessionctx.Context, h int64, r []types.Datum) error {
	return table.ErrUnsupportedOp
}

// UpdateRecord implements table.Table UpdateRecord interface.
func (it *infoschemaDataTable) UpdateRecord(ctx sessionctx.Context, h int64, oldData, newData []types.Datum, touched []bool) error {
	return table.ErrUnsupportedOp
}

// AllocHandle implements table.Table AllocHandle interface.
func (it *infoschemaDataTable) AllocHandle(ctx sessionctx.Context) (int64, error) {
	return 0, table.ErrUnsupportedOp
}

// AllocHandleIDs implements table.Table AllocHandleIDs interface.
func (it *infoschemaDataTable) AllocHandleIDs(ctx sessionctx.Context, n uint64) (int64, int64, error) {
	return 0, 0, table.ErrUnsupportedOp
}

// Allocator implements table.Table Allocator interface.
func (it *infoschemaDataTable) Allocator(_ sessionctx.Context, _ autoid.AllocatorType) autoid.Allocator {
	return nil
}

// AllAllocators implements table.Table AllAllocators interface.
func (it *infoschemaDataTable) AllAllocators(_ sessionctx.Context) autoid.Allocators {
	return nil
}

// RebaseAutoID implements table.Table RebaseAutoID interface.
func (it *infoschemaDataTable) RebaseAutoID(ctx sessionctx.Context, newBase int64, isSetStep bool) error {
	return table.ErrUnsupportedOp
}

// Meta implements table.Table Meta interface.
func (it *infoschemaDataTable) Meta() *model.TableInfo {
	return it.meta
}

// GetPhysicalID implements table.Table GetPhysicalID interface.
func (it *infoschemaDataTable) GetPhysicalID() int64 {
	return it.meta.ID
}

// Seek implements table.Table Seek interface.
func (it *infoschemaDataTable) Seek(ctx sessionctx.Context, h int64) (int64, bool, error) {
	return 0, false, table.ErrUnsupportedOp
}

// Type implements table.Table Type interface.
func (it *infoschemaDataTable) Type() table.Type {
	return it.tp
}
