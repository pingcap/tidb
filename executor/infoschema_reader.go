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
	"fmt"
	"sort"
	"strings"

	"github.com/cznic/mathutil"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
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
		case infoschema.TableTiDBIndexes:
			e.rows, err = dataForIndexes(sctx, dbs)
		case infoschema.TableViews:
			e.rows, err = dataForViews(sctx, dbs)
		case infoschema.TableEngines:
			e.rows = dataForEngines()
		case infoschema.TableCharacterSets:
			e.rows = dataForCharacterSets()
		case infoschema.TableCollations:
			e.rows = dataForCollations()
		case infoschema.TableKeyColumn:
			e.rows = dataForKeyColumnUsage(sctx, dbs)
		case infoschema.TableCollationCharacterSetApplicability:
			e.rows = dataForCollationCharacterSetApplicability()
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

func dataForIndexes(ctx sessionctx.Context, schemas []*model.DBInfo) ([][]types.Datum, error) {
	checker := privilege.GetPrivilegeManager(ctx)
	var rows [][]types.Datum
	for _, schema := range schemas {
		for _, tb := range schema.Tables {
			if checker != nil && !checker.RequestVerification(ctx.GetSessionVars().ActiveRoles, schema.Name.L, tb.Name.L, "", mysql.AllPrivMask) {
				continue
			}

			if tb.PKIsHandle {
				var pkCol *model.ColumnInfo
				for _, col := range tb.Cols() {
					if mysql.HasPriKeyFlag(col.Flag) {
						pkCol = col
						break
					}
				}
				record := types.MakeDatums(
					schema.Name.O, // TABLE_SCHEMA
					tb.Name.O,     // TABLE_NAME
					0,             // NON_UNIQUE
					"PRIMARY",     // KEY_NAME
					1,             // SEQ_IN_INDEX
					pkCol.Name.O,  // COLUMN_NAME
					nil,           // SUB_PART
					"",            // INDEX_COMMENT
					"NULL",        // Expression
					0,             // INDEX_ID
				)
				rows = append(rows, record)
			}
			for _, idxInfo := range tb.Indices {
				if idxInfo.State != model.StatePublic {
					continue
				}
				for i, col := range idxInfo.Columns {
					nonUniq := 1
					if idxInfo.Unique {
						nonUniq = 0
					}
					var subPart interface{}
					if col.Length != types.UnspecifiedLength {
						subPart = col.Length
					}
					colName := col.Name.O
					expression := "NULL"
					tblCol := tb.Columns[col.Offset]
					if tblCol.Hidden {
						colName = "NULL"
						expression = fmt.Sprintf("(%s)", tblCol.GeneratedExprString)
					}
					record := types.MakeDatums(
						schema.Name.O,   // TABLE_SCHEMA
						tb.Name.O,       // TABLE_NAME
						nonUniq,         // NON_UNIQUE
						idxInfo.Name.O,  // KEY_NAME
						i+1,             // SEQ_IN_INDEX
						colName,         // COLUMN_NAME
						subPart,         // SUB_PART
						idxInfo.Comment, // INDEX_COMMENT
						expression,      // Expression
						idxInfo.ID,      // INDEX_ID
					)
					rows = append(rows, record)
				}
			}
		}
	}
	return rows, nil
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

// DDLJobsReaderExec executes DDLJobs information retrieving.
type DDLJobsReaderExec struct {
	baseExecutor
	DDLJobExecInitializer

	cacheJobs []*model.Job
	is        infoschema.InfoSchema
}

// Open implements the Executor Next interface.
func (e *DDLJobsReaderExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}
	txn, err := e.ctx.Txn(true)
	if err != nil {
		return err
	}
	err = e.DDLJobExecInitializer.initial(txn)
	if err != nil {
		return err
	}
	return nil
}

// Next implements the Executor Next interface.
func (e *DDLJobsReaderExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	checker := privilege.GetPrivilegeManager(e.ctx)
	count := 0

	// Append running DDL jobs.
	if e.cursor < len(e.runningJobs) {
		num := mathutil.Min(req.Capacity(), len(e.runningJobs)-e.cursor)
		for i := e.cursor; i < e.cursor+num; i++ {
			e.appendJobToChunk(req, e.runningJobs[i], checker)
		}
		e.cursor += num
		count += num
	}
	var err error

	// Append history DDL jobs.
	if count < req.Capacity() {
		e.cacheJobs, err = e.historyJobIter.GetLastJobs(req.Capacity()-count, e.cacheJobs)
		if err != nil {
			return err
		}
		for _, job := range e.cacheJobs {
			e.appendJobToChunk(req, job, checker)
		}
		e.cursor += len(e.cacheJobs)
	}
	return nil
}

func (e *DDLJobsReaderExec) appendJobToChunk(req *chunk.Chunk, job *model.Job, checker privilege.Manager) {
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

	// Check the privilege.
	if checker != nil && !checker.RequestVerification(e.ctx.GetSessionVars().ActiveRoles, strings.ToLower(schemaName), strings.ToLower(tableName), "", mysql.AllPrivMask) {
		return
	}

	req.AppendInt64(0, job.ID)
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
	req.AppendString(11, job.Query)
}

func dataForEngines() (rows [][]types.Datum) {
	rows = append(rows,
		types.MakeDatums(
			"InnoDB",  // Engine
			"DEFAULT", // Support
			"Supports transactions, row-level locking, and foreign keys", // Comment
			"YES", // Transactions
			"YES", // XA
			"YES", // Savepoints
		),
	)
	return rows
}

func dataForCharacterSets() (records [][]types.Datum) {
	charsets := charset.GetSupportedCharsets()
	for _, charset := range charsets {
		records = append(records,
			types.MakeDatums(charset.Name, charset.DefaultCollation, charset.Desc, charset.Maxlen),
		)
	}
	return records
}

func dataForCollations() (records [][]types.Datum) {
	collations := charset.GetSupportedCollations()
	for _, collation := range collations {
		isDefault := ""
		if collation.IsDefault {
			isDefault = "Yes"
		}
		records = append(records,
			types.MakeDatums(collation.Name, collation.CharsetName, collation.ID, isDefault, "Yes", 1),
		)
	}
	return records
}

func dataForCollationCharacterSetApplicability() (records [][]types.Datum) {
	collations := charset.GetSupportedCollations()
	for _, collation := range collations {
		records = append(records,
			types.MakeDatums(collation.Name, collation.CharsetName),
		)
	}
	return records
}

func dataForKeyColumnUsage(ctx sessionctx.Context, schemas []*model.DBInfo) [][]types.Datum {
	checker := privilege.GetPrivilegeManager(ctx)
	rows := make([][]types.Datum, 0, len(schemas)) // The capacity is not accurate, but it is not a big problem.
	for _, schema := range schemas {
		for _, table := range schema.Tables {
			if checker != nil && !checker.RequestVerification(ctx.GetSessionVars().ActiveRoles, schema.Name.L, table.Name.L, "", mysql.AllPrivMask) {
				continue
			}
			rs := keyColumnUsageInTable(schema, table)
			rows = append(rows, rs...)
		}
	}
	return rows
}

func keyColumnUsageInTable(schema *model.DBInfo, table *model.TableInfo) [][]types.Datum {
	var rows [][]types.Datum
	if table.PKIsHandle {
		for _, col := range table.Columns {
			if mysql.HasPriKeyFlag(col.Flag) {
				record := types.MakeDatums(
					infoschema.CatalogVal,        // CONSTRAINT_CATALOG
					schema.Name.O,                // CONSTRAINT_SCHEMA
					infoschema.PrimaryConstraint, // CONSTRAINT_NAME
					infoschema.CatalogVal,        // TABLE_CATALOG
					schema.Name.O,                // TABLE_SCHEMA
					table.Name.O,                 // TABLE_NAME
					col.Name.O,                   // COLUMN_NAME
					1,                            // ORDINAL_POSITION
					1,                            // POSITION_IN_UNIQUE_CONSTRAINT
					nil,                          // REFERENCED_TABLE_SCHEMA
					nil,                          // REFERENCED_TABLE_NAME
					nil,                          // REFERENCED_COLUMN_NAME
				)
				rows = append(rows, record)
				break
			}
		}
	}
	nameToCol := make(map[string]*model.ColumnInfo, len(table.Columns))
	for _, c := range table.Columns {
		nameToCol[c.Name.L] = c
	}
	for _, index := range table.Indices {
		var idxName string
		if index.Primary {
			idxName = infoschema.PrimaryConstraint
		} else if index.Unique {
			idxName = index.Name.O
		} else {
			// Only handle unique/primary key
			continue
		}
		for i, key := range index.Columns {
			col := nameToCol[key.Name.L]
			record := types.MakeDatums(
				infoschema.CatalogVal, // CONSTRAINT_CATALOG
				schema.Name.O,         // CONSTRAINT_SCHEMA
				idxName,               // CONSTRAINT_NAME
				infoschema.CatalogVal, // TABLE_CATALOG
				schema.Name.O,         // TABLE_SCHEMA
				table.Name.O,          // TABLE_NAME
				col.Name.O,            // COLUMN_NAME
				i+1,                   // ORDINAL_POSITION,
				nil,                   // POSITION_IN_UNIQUE_CONSTRAINT
				nil,                   // REFERENCED_TABLE_SCHEMA
				nil,                   // REFERENCED_TABLE_NAME
				nil,                   // REFERENCED_COLUMN_NAME
			)
			rows = append(rows, record)
		}
	}
	for _, fk := range table.ForeignKeys {
		fkRefCol := ""
		if len(fk.RefCols) > 0 {
			fkRefCol = fk.RefCols[0].O
		}
		for i, key := range fk.Cols {
			col := nameToCol[key.L]
			record := types.MakeDatums(
				infoschema.CatalogVal, // CONSTRAINT_CATALOG
				schema.Name.O,         // CONSTRAINT_SCHEMA
				fk.Name.O,             // CONSTRAINT_NAME
				infoschema.CatalogVal, // TABLE_CATALOG
				schema.Name.O,         // TABLE_SCHEMA
				table.Name.O,          // TABLE_NAME
				col.Name.O,            // COLUMN_NAME
				i+1,                   // ORDINAL_POSITION,
				1,                     // POSITION_IN_UNIQUE_CONSTRAINT
				schema.Name.O,         // REFERENCED_TABLE_SCHEMA
				fk.RefTable.O,         // REFERENCED_TABLE_NAME
				fkRefCol,              // REFERENCED_COLUMN_NAME
			)
			rows = append(rows, record)
		}
	}
	return rows
}
