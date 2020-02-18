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
	"github.com/cznic/mathutil"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/chunk"
	"sort"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
)

type memtableRetriever struct {
	dummyCloser
	table       *model.TableInfo
	columns     []*model.ColumnInfo
	rows        [][]types.Datum
	rowIdx      int
	retrieved   bool
	initialized bool
}

// retrieve implements the infoschemaRetriever interface
func (e *memtableRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if e.retrieved {
		return nil, nil
	}

	//Cache the ret full rows in schemataRetriever
	if !e.initialized {
		is := infoschema.GetInfoSchema(sctx)
		dbs := is.AllSchemas()
		sort.Sort(infoschema.SchemasSorter(dbs))
		var err error
		switch e.table.Name.O {
		case infoschema.TableSchemata:
			e.rows = dataForSchemata(sctx, dbs)
		case infoschema.TableViews:
			e.rows, err = dataForViews(sctx, dbs)
		}
		if err != nil {
			return nil, err
		}
		e.initialized = true
	}

	//Adjust the amount of each return
	maxCount := 1024
	retCount := maxCount
	if e.rowIdx+maxCount > len(e.rows) {
		retCount = len(e.rows) - e.rowIdx
		e.retrieved = true
	}
	ret := make([][]types.Datum, retCount)
	for i := e.rowIdx; i < e.rowIdx+retCount; i++ {
		ret[i-e.rowIdx] = e.rows[i]
	}
	e.rowIdx += retCount
	if len(e.columns) == len(e.table.Columns) {
		return ret, nil
	}
	rows := make([][]types.Datum, len(ret))
	for i, fullRow := range ret {
		row := make([]types.Datum, len(e.columns))
		for j, col := range e.columns {
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

func dataForViews(ctx sessionctx.Context, schemas []*model.DBInfo) ([][]types.Datum, error) {
	checker := privilege.GetPrivilegeManager(ctx)
	var rows [][]types.Datum
	for _, schema := range schemas {
		for _, table := range schema.Tables {
			if !table.IsView() {
				continue
			}
			collation := table.Collate
			charset := table.Charset
			if collation == "" {
				collation = mysql.DefaultCollationName
			}
			if charset == "" {
				charset = mysql.DefaultCharset
			}
			if checker != nil && !checker.RequestVerification(ctx.GetSessionVars().ActiveRoles, schema.Name.L, table.Name.L, "", mysql.AllPrivMask) {
				continue
			}
			record := types.MakeDatums(
				infoschema.CatalogVal,           // TABLE_CATALOG
				schema.Name.O,                   // TABLE_SCHEMA
				table.Name.O,                    // TABLE_NAME
				table.View.SelectStmt,           // VIEW_DEFINITION
				table.View.CheckOption.String(), // CHECK_OPTION
				"NO",                            // IS_UPDATABLE
				table.View.Definer.String(),     // DEFINER
				table.View.Security.String(),    // SECURITY_TYPE
				charset,                         // CHARACTER_SET_CLIENT
				collation,                       // COLLATION_CONNECTION
			)
			rows = append(rows, record)
		}
	}
	return rows, nil
}

type DDLJobsReaderExec struct {
	baseExecutor
	cursor         int
	runningJobs    []*model.Job
	historyJobIter *meta.LastJobIterator
	cacheJobs      []*model.Job
	is             infoschema.InfoSchema
}

// retrieve implements the infoschemaRetriever interface
func (e *DDLJobsReaderExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}
	txn, err := e.ctx.Txn(true)
	if err != nil {
		return err
	}
	jobs, err := admin.GetDDLJobs(txn)
	if err != nil {
		return err
	}
	m := meta.NewMeta(txn)
	e.historyJobIter, err = m.GetLastHistoryDDLJobsIterator()
	if err != nil {
		return err
	}
	e.runningJobs = append(e.runningJobs, jobs...)
	e.cursor = 0
	return nil
}

// Next implements the Executor Next interface.
func (e *DDLJobsReaderExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	if !e.historyJobIter.Valid() {
		return nil
	}
	count := 0

	// Append running ddl jobs.
	if e.cursor < len(e.runningJobs) {
		num := mathutil.Min(req.Capacity(), len(e.runningJobs)-e.cursor)
		for i := e.cursor; i < e.cursor+num; i++ {
			e.appendJobToChunk(req, e.runningJobs[i])
		}
		e.cursor += num
		count += num
	}
	var err error

	// Append history ddl jobs.
	if count < req.Capacity() {
		e.cacheJobs, err = e.historyJobIter.GetNextJobs(req.Capacity()-count, e.cacheJobs)
		if err != nil {
			return err
		}
		for _, job := range e.cacheJobs {
			e.appendJobToChunk(req, job)
		}
		e.cursor += len(e.cacheJobs)
	}
	return nil
}

func (e *DDLJobsReaderExec) appendJobToChunk(req *chunk.Chunk, job *model.Job) {
	req.AppendInt64(0, job.ID)
	schemaName := job.SchemaName
	tableName := ""
	finishTS := uint64(0)
	if job.BinlogInfo != nil {
		finishTS = job.BinlogInfo.FinishedTS
		if job.BinlogInfo.TableInfo != nil {
			tableName = job.BinlogInfo.TableInfo.Name.L
		}
		if len(schemaName) == 0 && job.BinlogInfo.DBInfo != nil {
			schemaName = job.BinlogInfo.DBInfo.Name.L
		}
	}
	// For compatibility, the old version of DDL Job wasn't store the schema name and table name.
	if len(schemaName) == 0 {
		schemaName = getSchemaName(e.is, job.SchemaID)
	}
	if len(tableName) == 0 {
		tableName = getTableName(e.is, job.TableID)
	}
	req.AppendString(1, schemaName)
	req.AppendString(2, tableName)
	req.AppendString(3, job.Type.String())
	req.AppendString(4, job.SchemaState.String())
	req.AppendInt64(5, job.SchemaID)
	req.AppendInt64(6, job.TableID)
	req.AppendInt64(7, job.RowCount)
	req.AppendString(8, model.TSConvert2Time(job.StartTS).String())
	if finishTS > 0 {
		req.AppendString(9, model.TSConvert2Time(finishTS).String())
	} else {
		req.AppendString(9, "")
	}
	req.AppendString(10, job.State.String())
}
