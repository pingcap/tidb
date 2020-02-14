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

type infoschemaRetriever interface {
	retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error)
	close() error
}

// InfoschemaReaderExec executes infoschema information retrieving
type InfoschemaReaderExec struct {
	baseExecutor
	retriever infoschemaRetriever
}

// Next implements the Executor Next interface.
func (e *InfoschemaReaderExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	rows, err := e.retriever.retrieve(ctx, e.ctx)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		req.Reset()
		return nil
	}
	req.GrowAndReset(len(rows))
	mutableRow := chunk.MutRowFromTypes(retTypes(e))
	for _, row := range rows {
		mutableRow.SetDatums(row...)
		req.AppendRow(mutableRow.ToRow())
	}
	return nil
}

// Close implements the Executor Close interface.
func (e *InfoschemaReaderExec) Close() error {
	return e.retriever.close()
}

type schemataRetriever struct {
	table       *model.TableInfo
	columns     []*model.ColumnInfo
	dbs         []*model.DBInfo
	dbIdx       int
	retrieved   bool
	initialized bool
}

// retrieve implements the infoschemaRetriever interface
func (e *schemataRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if e.retrieved {
		return nil, nil
	}
	if !e.initialized {
		is := infoschema.GetInfoSchema(sctx)
		dbs := is.AllSchemas()
		sort.Sort(infoschema.SchemasSorter(dbs))
		e.dbs = dbs
		e.initialized = true
	}
	fullRows := e.dataForSchemata(sctx)

	if len(e.columns) == len(e.table.Columns) {
		return fullRows, nil
	}
	rows := make([][]types.Datum, len(fullRows))
	for i, fullRow := range fullRows {
		row := make([]types.Datum, len(e.columns))
		for j, col := range e.columns {
			row[j] = fullRow[col.Offset]
		}
		rows[i] = row
	}
	return rows, nil
}

func (e *schemataRetriever) dataForSchemata(ctx sessionctx.Context) [][]types.Datum {
	checker := privilege.GetPrivilegeManager(ctx)
	rows := make([][]types.Datum, 0, 1024)
	maxCount := 1024
	remainCount := maxCount
	if e.dbIdx+maxCount > len(e.dbs) {
		remainCount = maxCount - (e.dbIdx + maxCount - len(e.dbs))
		e.retrieved = true
	}
	for i := e.dbIdx; i < e.dbIdx+remainCount; i++ {
		charset := mysql.DefaultCharset
		collation := mysql.DefaultCollationName

		if len(e.dbs[i].Charset) > 0 {
			charset = e.dbs[i].Charset // Overwrite default
		}

		if len(e.dbs[i].Collate) > 0 {
			collation = e.dbs[i].Collate // Overwrite default
		}

		if checker != nil && !checker.RequestVerification(ctx.GetSessionVars().ActiveRoles, e.dbs[i].Name.L, "", "", mysql.AllPrivMask) {
			continue
		}
		record := types.MakeDatums(
			infoschema.CatalogVal, // CATALOG_NAME
			e.dbs[i].Name.O,       // SCHEMA_NAME
			charset,               // DEFAULT_CHARACTER_SET_NAME
			collation,             // DEFAULT_COLLATION_NAME
			nil,
		)
		rows = append(rows, record)
	}
	e.dbIdx += remainCount
	return rows
}

func (schemataRetriever) close() error { return nil }
