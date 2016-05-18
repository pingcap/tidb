// Copyright 2015 PingCAP, Inc.
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

package infoschema

import (
	"strings"
	"sync/atomic"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/perfschema"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/terror"
	// import table implementation to init table.TableFromMeta
	_ "github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util/types"
)

var (
	// ErrDatabaseDropExists returns for dropping a non-existent database.
	ErrDatabaseDropExists = terror.ClassSchema.New(codeDBDropExists, "database doesn't exist")
	// ErrDatabaseNotExists returns for database not exists.
	ErrDatabaseNotExists = terror.ClassSchema.New(codeDatabaseNotExists, "database not exists")
	// ErrTableNotExists returns for table not exists.
	ErrTableNotExists = terror.ClassSchema.New(codeTableNotExists, "table not exists")
	// ErrColumnNotExists returns for column not exists.
	ErrColumnNotExists = terror.ClassSchema.New(codeColumnNotExists, "field not exists")
	// ErrForeignKeyNotMatch returns for foreign key not match.
	ErrForeignKeyNotMatch = terror.ClassSchema.New(codeCannotAddForeign, "foreign key not match")
	// ErrForeignKeyExists returns for foreign key exists.
	ErrForeignKeyExists = terror.ClassSchema.New(codeCannotAddForeign, "foreign key already exists")
	// ErrForeignKeyNotExists returns for foreign key not exists.
	ErrForeignKeyNotExists = terror.ClassSchema.New(codeForeignKeyNotExists, "foreign key not exists")
	// ErrDatabaseExists returns for database already exists.
	ErrDatabaseExists = terror.ClassSchema.New(codeDatabaseExists, "database already exists")
	// ErrTableExists returns for table already exists.
	ErrTableExists = terror.ClassSchema.New(codeTableExists, "table already exists")
	// ErrTableDropExists returns for dropping a non-existent table.
	ErrTableDropExists = terror.ClassSchema.New(codeBadTable, "unknown table")
	// ErrColumnExists returns for column already exists.
	ErrColumnExists = terror.ClassSchema.New(codeColumnExists, "Duplicate column")
	// ErrIndexExists returns for index already exists.
	ErrIndexExists = terror.ClassSchema.New(codeIndexExists, "Duplicate Index")
)

// InfoSchema is the interface used to retrieve the schema information.
// It works as a in memory cache and doesn't handle any schema change.
// InfoSchema is read-only, and the returned value is a copy.
// TODO: add more methods to retrieve tables and columns.
type InfoSchema interface {
	SchemaByName(schema model.CIStr) (*model.DBInfo, bool)
	SchemaExists(schema model.CIStr) bool
	TableByName(schema, table model.CIStr) (table.Table, error)
	TableExists(schema, table model.CIStr) bool
	ColumnByName(schema, table, column model.CIStr) (*model.ColumnInfo, bool)
	ColumnExists(schema, table, column model.CIStr) bool
	IndexByName(schema, table, index model.CIStr) (*model.IndexInfo, bool)
	SchemaByID(id int64) (*model.DBInfo, bool)
	TableByID(id int64) (table.Table, bool)
	AllocByID(id int64) (autoid.Allocator, bool)
	ColumnByID(id int64) (*model.ColumnInfo, bool)
	ColumnIndicesByID(id int64) ([]*model.IndexInfo, bool)
	AllSchemaNames() []string
	AllSchemas() []*model.DBInfo
	Clone() (result []*model.DBInfo)
	SchemaTables(schema model.CIStr) []table.Table
	SchemaMetaVersion() int64
}

// Information Schema Name.
const (
	Name = "INFORMATION_SCHEMA"
)

type infoSchema struct {
	schemaNameToID  map[string]int64
	tableNameToID   map[tableName]int64
	columnNameToID  map[columnName]int64
	schemas         map[int64]*model.DBInfo
	tables          map[int64]table.Table
	tableAllocators map[int64]autoid.Allocator
	columns         map[int64]*model.ColumnInfo
	indices         map[indexName]*model.IndexInfo
	columnIndices   map[int64][]*model.IndexInfo

	// We should check version when change schema.
	schemaMetaVersion int64
}

// MockInfoSchema only serves for test.
func MockInfoSchema(tbList []*model.TableInfo) InfoSchema {
	result := &infoSchema{}
	result.schemaNameToID = make(map[string]int64)
	result.tableNameToID = make(map[tableName]int64)
	result.schemas = make(map[int64]*model.DBInfo)
	result.tables = make(map[int64]table.Table)

	result.schemaNameToID["test"] = 0
	result.schemas[0] = &model.DBInfo{ID: 0, Name: model.NewCIStr("test"), Tables: tbList}
	for i, tb := range tbList {
		result.tableNameToID[tableName{schema: "test", table: tb.Name.L}] = int64(i)
		result.tables[int64(i)] = table.MockTableFromMeta(tb)
	}
	return result
}

var _ InfoSchema = (*infoSchema)(nil)

type tableName struct {
	schema string
	table  string
}

type columnName struct {
	tableName
	name string
}

type indexName struct {
	tableName
	name string
}

func (is *infoSchema) SchemaByName(schema model.CIStr) (val *model.DBInfo, ok bool) {
	id, ok := is.schemaNameToID[schema.L]
	if !ok {
		return
	}
	val, ok = is.schemas[id]
	return
}

func (is *infoSchema) SchemaMetaVersion() int64 {
	return is.schemaMetaVersion
}

func (is *infoSchema) SchemaExists(schema model.CIStr) bool {
	_, ok := is.schemaNameToID[schema.L]
	return ok
}

func (is *infoSchema) TableByName(schema, table model.CIStr) (t table.Table, err error) {
	id, ok := is.tableNameToID[tableName{schema: schema.L, table: table.L}]
	if !ok {
		return nil, ErrTableNotExists.Gen("table %s.%s does not exist", schema, table)
	}
	t = is.tables[id]
	return
}

func (is *infoSchema) TableExists(schema, table model.CIStr) bool {
	_, ok := is.tableNameToID[tableName{schema: schema.L, table: table.L}]
	return ok
}

func (is *infoSchema) ColumnByName(schema, table, column model.CIStr) (val *model.ColumnInfo, ok bool) {
	id, ok := is.columnNameToID[columnName{tableName: tableName{schema: schema.L, table: table.L}, name: column.L}]
	if !ok {
		return
	}
	val, ok = is.columns[id]
	return
}

func (is *infoSchema) ColumnExists(schema, table, column model.CIStr) bool {
	_, ok := is.columnNameToID[columnName{tableName: tableName{schema: schema.L, table: table.L}, name: column.L}]
	return ok
}

func (is *infoSchema) IndexByName(schema, table, index model.CIStr) (val *model.IndexInfo, ok bool) {
	val, ok = is.indices[indexName{tableName: tableName{schema: schema.L, table: table.L}, name: index.L}]
	return
}

func (is *infoSchema) SchemaByID(id int64) (val *model.DBInfo, ok bool) {
	val, ok = is.schemas[id]
	return
}

func (is *infoSchema) TableByID(id int64) (val table.Table, ok bool) {
	val, ok = is.tables[id]
	return
}

func (is *infoSchema) AllocByID(id int64) (val autoid.Allocator, ok bool) {
	val, ok = is.tableAllocators[id]
	return
}

func (is *infoSchema) ColumnByID(id int64) (val *model.ColumnInfo, ok bool) {
	val, ok = is.columns[id]
	return
}

func (is *infoSchema) ColumnIndicesByID(id int64) (indices []*model.IndexInfo, ok bool) {
	indices, ok = is.columnIndices[id]
	return
}

func (is *infoSchema) AllSchemaNames() (names []string) {
	for _, v := range is.schemas {
		names = append(names, v.Name.O)
	}
	return
}

func (is *infoSchema) AllSchemas() (schemas []*model.DBInfo) {
	for _, v := range is.schemas {
		schemas = append(schemas, v)
	}
	return
}

func (is *infoSchema) SchemaTables(schema model.CIStr) (tables []table.Table) {
	di, ok := is.SchemaByName(schema)
	if !ok {
		return
	}
	for _, ti := range di.Tables {
		tables = append(tables, is.tables[ti.ID])
	}
	return
}

func (is *infoSchema) Clone() (result []*model.DBInfo) {
	for _, v := range is.schemas {
		result = append(result, v.Clone())
	}
	return
}

// Handle handles information schema, including getting and setting.
type Handle struct {
	value     atomic.Value
	store     kv.Storage
	memSchema *memSchemaHandle
}

// NewHandle creates a new Handle.
func NewHandle(store kv.Storage) (*Handle, error) {
	h := &Handle{
		store: store,
	}
	// init memory tables
	var err error
	h.memSchema, err = newMemSchemaHandle()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return h, nil
}

// Init memory schemas including infoschema and perfshcema.
func newMemSchemaHandle() (*memSchemaHandle, error) {
	h := &memSchemaHandle{
		nameToTable: make(map[string]table.Table),
	}
	err := initMemoryTables(h)
	if err != nil {
		return nil, errors.Trace(err)
	}
	initMemoryTables(h)
	h.perfHandle, err = perfschema.NewPerfHandle()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return h, nil
}

// memSchemaHandle is used to store memory schema information.
type memSchemaHandle struct {
	// Information Schema
	isDB          *model.DBInfo
	schemataTbl   table.Table
	tablesTbl     table.Table
	columnsTbl    table.Table
	statisticsTbl table.Table
	charsetTbl    table.Table
	collationsTbl table.Table
	filesTbl      table.Table
	defTbl        table.Table
	profilingTbl  table.Table
	partitionsTbl table.Table
	nameToTable   map[string]table.Table
	// Performance Schema
	perfHandle perfschema.PerfSchema
}

func initMemoryTables(h *memSchemaHandle) error {
	// Init Information_Schema
	var (
		err error
		tbl table.Table
	)
	dbID := autoid.GenLocalSchemaID()
	isTables := make([]*model.TableInfo, 0, len(tableNameToColumns))
	for name, cols := range tableNameToColumns {
		meta := buildTableMeta(name, cols)
		isTables = append(isTables, meta)
		meta.ID = autoid.GenLocalSchemaID()
		for _, c := range meta.Columns {
			c.ID = autoid.GenLocalSchemaID()
		}
		alloc := autoid.NewMemoryAllocator(dbID)
		tbl, err = createMemoryTable(meta, alloc)
		if err != nil {
			return errors.Trace(err)
		}
		h.nameToTable[meta.Name.L] = tbl
	}
	h.schemataTbl = h.nameToTable[strings.ToLower(tableSchemata)]
	h.tablesTbl = h.nameToTable[strings.ToLower(tableTables)]
	h.columnsTbl = h.nameToTable[strings.ToLower(tableColumns)]
	h.statisticsTbl = h.nameToTable[strings.ToLower(tableStatistics)]
	h.charsetTbl = h.nameToTable[strings.ToLower(tableCharacterSets)]
	h.collationsTbl = h.nameToTable[strings.ToLower(tableCollations)]

	// CharacterSets/Collations contain static data. Init them now.
	err = insertData(h.charsetTbl, dataForCharacterSets())
	if err != nil {
		return errors.Trace(err)
	}
	err = insertData(h.collationsTbl, dataForColltions())
	if err != nil {
		return errors.Trace(err)
	}
	// create db
	h.isDB = &model.DBInfo{
		ID:      dbID,
		Name:    model.NewCIStr(Name),
		Charset: mysql.DefaultCharset,
		Collate: mysql.DefaultCollationName,
		Tables:  isTables,
	}
	return nil
}

func insertData(tbl table.Table, rows [][]types.Datum) error {
	for _, r := range rows {
		_, err := tbl.AddRecord(nil, r)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func refillTable(tbl table.Table, rows [][]types.Datum) error {
	err := tbl.Truncate(nil)
	if err != nil {
		return errors.Trace(err)
	}
	return insertData(tbl, rows)
}

// Set sets DBInfo to information schema.
func (h *Handle) Set(newInfo []*model.DBInfo, schemaMetaVersion int64) error {
	info := &infoSchema{
		schemaNameToID:    map[string]int64{},
		tableNameToID:     map[tableName]int64{},
		columnNameToID:    map[columnName]int64{},
		schemas:           map[int64]*model.DBInfo{},
		tables:            map[int64]table.Table{},
		tableAllocators:   map[int64]autoid.Allocator{},
		columns:           map[int64]*model.ColumnInfo{},
		indices:           map[indexName]*model.IndexInfo{},
		columnIndices:     map[int64][]*model.IndexInfo{},
		schemaMetaVersion: schemaMetaVersion,
	}
	var err error
	var hasOldInfo bool
	infoschema := h.Get()
	if infoschema != nil {
		hasOldInfo = true
	}
	for _, di := range newInfo {
		info.schemas[di.ID] = di
		info.schemaNameToID[di.Name.L] = di.ID
		for _, t := range di.Tables {
			alloc := autoid.NewAllocator(h.store, di.ID)
			if hasOldInfo {
				val, ok := infoschema.AllocByID(t.ID)
				if ok {
					alloc = val
				}
			}
			info.tableAllocators[t.ID] = alloc
			info.tables[t.ID], err = table.TableFromMeta(alloc, t)
			if err != nil {
				return errors.Trace(err)
			}
			tname := tableName{di.Name.L, t.Name.L}
			info.tableNameToID[tname] = t.ID
			for _, c := range t.Columns {
				info.columns[c.ID] = c
				info.columnNameToID[columnName{tname, c.Name.L}] = c.ID
			}
			for _, idx := range t.Indices {
				info.indices[indexName{tname, idx.Name.L}] = idx
				for _, idxCol := range idx.Columns {
					columnID := t.Columns[idxCol.Offset].ID
					columnIndices := info.columnIndices[columnID]
					info.columnIndices[columnID] = append(columnIndices, idx)
				}
			}
		}
	}
	// Build Information_Schema
	info.schemaNameToID[h.memSchema.isDB.Name.L] = h.memSchema.isDB.ID
	info.schemas[h.memSchema.isDB.ID] = h.memSchema.isDB
	for _, t := range h.memSchema.isDB.Tables {
		tbl, ok := h.memSchema.nameToTable[t.Name.L]
		if !ok {
			return ErrTableNotExists.Gen("table `%s` is missing.", t.Name)
		}
		info.tables[t.ID] = tbl
		tname := tableName{h.memSchema.isDB.Name.L, t.Name.L}
		info.tableNameToID[tname] = t.ID
		for _, c := range t.Columns {
			info.columns[c.ID] = c
			info.columnNameToID[columnName{tname, c.Name.L}] = c.ID
		}
	}

	// Add Performance_Schema
	psDB := h.memSchema.perfHandle.GetDBMeta()
	info.schemaNameToID[psDB.Name.L] = psDB.ID
	info.schemas[psDB.ID] = psDB
	for _, t := range psDB.Tables {
		tbl, ok := h.memSchema.perfHandle.GetTable(t.Name.O)
		if !ok {
			return ErrTableNotExists.Gen("table `%s` is missing.", t.Name)
		}
		info.tables[t.ID] = tbl
		tname := tableName{psDB.Name.L, t.Name.L}
		info.tableNameToID[tname] = t.ID
		for _, c := range t.Columns {
			info.columns[c.ID] = c
			info.columnNameToID[columnName{tname, c.Name.L}] = c.ID
		}
	}
	// Should refill some tables in Information_Schema.
	// schemata/tables/columns/statistics
	dbNames := make([]string, 0, len(info.schemas))
	dbInfos := make([]*model.DBInfo, 0, len(info.schemas))
	for _, v := range info.schemas {
		dbNames = append(dbNames, v.Name.L)
		dbInfos = append(dbInfos, v)
	}
	err = refillTable(h.memSchema.schemataTbl, dataForSchemata(dbNames))
	if err != nil {
		return errors.Trace(err)
	}
	err = refillTable(h.memSchema.tablesTbl, dataForTables(dbInfos))
	if err != nil {
		return errors.Trace(err)
	}
	err = refillTable(h.memSchema.columnsTbl, dataForColumns(dbInfos))
	if err != nil {
		return errors.Trace(err)
	}
	err = refillTable(h.memSchema.statisticsTbl, dataForStatistics(dbInfos))
	if err != nil {
		return errors.Trace(err)
	}
	h.value.Store(info)
	return nil
}

// Get gets information schema from Handle.
func (h *Handle) Get() InfoSchema {
	v := h.value.Load()
	schema, _ := v.(InfoSchema)
	return schema
}

// GetPerfHandle gets performance schema from handle.
func (h *Handle) GetPerfHandle() perfschema.PerfSchema {
	return h.memSchema.perfHandle
}

// Schema error codes.
const (
	codeDBDropExists      terror.ErrCode = 1008
	codeDatabaseNotExists                = 1049
	codeTableNotExists                   = 1146
	codeColumnNotExists                  = 1054

	codeCannotAddForeign    = 1215
	codeForeignKeyNotExists = 1091

	codeDatabaseExists = 1007
	codeTableExists    = 1050
	codeBadTable       = 1051
	codeColumnExists   = 1060
	codeIndexExists    = 1831
)

func init() {
	schemaMySQLErrCodes := map[terror.ErrCode]uint16{
		codeDBDropExists:        mysql.ErrDBDropExists,
		codeDatabaseNotExists:   mysql.ErrBadDB,
		codeTableNotExists:      mysql.ErrNoSuchTable,
		codeColumnNotExists:     mysql.ErrBadField,
		codeCannotAddForeign:    mysql.ErrCannotAddForeign,
		codeForeignKeyNotExists: mysql.ErrCantDropFieldOrKey,
		codeDatabaseExists:      mysql.ErrDBCreateExists,
		codeTableExists:         mysql.ErrTableExists,
		codeBadTable:            mysql.ErrBadTable,
		codeColumnExists:        mysql.ErrDupFieldName,
		codeIndexExists:         mysql.ErrDupIndex,
	}
	terror.ErrClassToMySQLCodes[terror.ClassSchema] = schemaMySQLErrCodes
}
