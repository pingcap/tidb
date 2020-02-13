// Copyright 2020 PingCAP, Inc.
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
	"sort"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

// InfoschemaReaderExec executes infoschema information retrieving
type InfoschemaReaderExec struct {
	baseExecutor
	table     *model.TableInfo
	columns   []*model.ColumnInfo
	chunkList *chunk.List
	chunkIdx  int
}

// Open implements the Executor Open interface.
func (e *InfoschemaReaderExec) Open(ctx context.Context) error {
	e.chunkList = nil
	return nil
}

// Next implements the Executor Next interface.
func (e *InfoschemaReaderExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.GrowAndReset(e.maxChunkSize)
	if e.chunkList == nil {
		e.chunkList = chunk.NewList(retTypes(e), e.initCap, e.maxChunkSize)
		mutableRow := chunk.MutRowFromTypes(retTypes(e))
		rows, err := e.getRows(e.ctx, e.columns)
		if err != nil {
			return err
		}
		for _, row := range rows {
			mutableRow.SetDatums(row...)
			e.chunkList.AppendRow(mutableRow.ToRow())
		}
	}
	// no more data.
	if e.chunkIdx >= e.chunkList.NumChunks() {
		return nil
	}
	virtualTableChunk := e.chunkList.GetChunk(e.chunkIdx)
	e.chunkIdx++
	chk.SwapColumns(virtualTableChunk)
	return nil
}

func (e *InfoschemaReaderExec) getRows(ctx sessionctx.Context, cols []*model.ColumnInfo) (fullRows [][]types.Datum, err error) {
	is := infoschema.GetInfoSchema(ctx)
	dbs := is.AllSchemas()
	sort.Sort(infoschema.SchemasSorter(dbs))
	switch e.table.Name.O {
	case infoschema.TableSchemata:
		fullRows = dataForSchemata(ctx, dbs)
	}
	if err != nil {
		return nil, err
	}
	if len(cols) == len(e.columns) {
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
			infoschema.CatalogVal, // CATALOG_NAME
			schema.Name.O,         // SCHEMA_NAME
			charset,               // DEFAULT_CHARACTER_SET_NAME
			collation,             // DEFAULT_COLLATION_NAME
			nil,
		)
		rows = append(rows, record)
	}
	return rows
}
