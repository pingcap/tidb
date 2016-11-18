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
	"sort"
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
	"github.com/pingcap/tidb/table/tables"
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
	SchemaByID(id int64) (*model.DBInfo, bool)
	TableByID(id int64) (table.Table, bool)
	AllocByID(id int64) (autoid.Allocator, bool)
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

type sortedTables []table.Table

func (s sortedTables) Len() int {
	return len(s)
}

func (s sortedTables) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s sortedTables) Less(i, j int) bool {
	return s[i].Meta().ID < s[j].Meta().ID
}

func (s sortedTables) searchTable(id int64) int {
	idx := sort.Search(len(s), func(i int) bool {
		return s[i].Meta().ID >= id
	})
	if idx == len(s) || s[idx].Meta().ID != id {
		return -1
	}
	return idx
}

type schemaTables struct {
	dbInfo *model.DBInfo
	tables map[string]table.Table
}

const bucketCount = 512

type infoSchema struct {
	schemaMap map[string]*schemaTables

	// sortedTablesBuckets is a slice of sortedTables, a table's bucket index is (tableID % bucketCount).
	sortedTablesBuckets []sortedTables

	// We should check version when change schema.
	schemaMetaVersion int64
}

// MockInfoSchema only serves for test.
func MockInfoSchema(tbList []*model.TableInfo) InfoSchema {
	result := &infoSchema{}
	result.schemaMap = make(map[string]*schemaTables)
	result.sortedTablesBuckets = make([]sortedTables, bucketCount)
	dbInfo := &model.DBInfo{ID: 0, Name: model.NewCIStr("test"), Tables: tbList}
	tableNames := &schemaTables{
		dbInfo: dbInfo,
		tables: make(map[string]table.Table),
	}
	result.schemaMap["test"] = tableNames
	for _, tb := range tbList {
		tbl := table.MockTableFromMeta(tb)
		tableNames.tables[tb.Name.L] = tbl
		bucketIdx := tableBucketIdx(tb.ID)
		result.sortedTablesBuckets[bucketIdx] = append(result.sortedTablesBuckets[bucketIdx], tbl)
	}
	for i := range result.sortedTablesBuckets {
		sort.Sort(result.sortedTablesBuckets[i])
	}
	return result
}

var _ InfoSchema = (*infoSchema)(nil)

func (is *infoSchema) SchemaByName(schema model.CIStr) (val *model.DBInfo, ok bool) {
	tableNames, ok := is.schemaMap[schema.L]
	if !ok {
		return
	}
	return tableNames.dbInfo, true
}

func (is *infoSchema) SchemaMetaVersion() int64 {
	return is.schemaMetaVersion
}

func (is *infoSchema) SchemaExists(schema model.CIStr) bool {
	_, ok := is.schemaMap[schema.L]
	return ok
}

func (is *infoSchema) TableByName(schema, table model.CIStr) (t table.Table, err error) {
	if tbNames, ok := is.schemaMap[schema.L]; ok {
		if t, ok = tbNames.tables[table.L]; ok {
			return
		}
	}
	return nil, ErrTableNotExists.Gen("table %s.%s does not exist", schema, table)
}

func (is *infoSchema) TableExists(schema, table model.CIStr) bool {
	if tbNames, ok := is.schemaMap[schema.L]; ok {
		if _, ok = tbNames.tables[table.L]; ok {
			return true
		}
	}
	return false
}

func (is *infoSchema) SchemaByID(id int64) (val *model.DBInfo, ok bool) {
	for _, v := range is.schemaMap {
		if v.dbInfo.ID == id {
			return v.dbInfo, true
		}
	}
	return nil, false
}

func (is *infoSchema) TableByID(id int64) (val table.Table, ok bool) {
	slice := is.sortedTablesBuckets[tableBucketIdx(id)]
	idx := slice.searchTable(id)
	if idx == -1 {
		return nil, false
	}
	return slice[idx], true
}

func (is *infoSchema) AllocByID(id int64) (autoid.Allocator, bool) {
	tbl, ok := is.TableByID(id)
	if !ok {
		return nil, false
	}
	return tbl.Allocator(), true
}

func (is *infoSchema) AllSchemaNames() (names []string) {
	for _, v := range is.schemaMap {
		names = append(names, v.dbInfo.Name.O)
	}
	return
}

func (is *infoSchema) AllSchemas() (schemas []*model.DBInfo) {
	for _, v := range is.schemaMap {
		schemas = append(schemas, v.dbInfo)
	}
	return
}

func (is *infoSchema) SchemaTables(schema model.CIStr) (tables []table.Table) {
	schemaTables, ok := is.schemaMap[schema.L]
	if !ok {
		return
	}
	for _, tbl := range schemaTables.tables {
		tables = append(tables, tbl)
	}
	return
}

func (is *infoSchema) Clone() (result []*model.DBInfo) {
	for _, v := range is.schemaMap {
		result = append(result, v.dbInfo.Clone())
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
	h.perfHandle, err = perfschema.NewPerfHandle()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return h, nil
}

// memSchemaHandle is used to store memory schema information.
type memSchemaHandle struct {
	// Information Schema
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
	for _, tblInfo := range infoSchemaDB.Tables {
		alloc := autoid.NewMemoryAllocator(infoSchemaDB.ID)
		tbl, err = createMemoryTable(tblInfo, alloc)
		if err != nil {
			return errors.Trace(err)
		}
		h.nameToTable[tblInfo.Name.L] = tbl
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

func refillMemoryTable(tbl table.Table, rows [][]types.Datum) error {
	tbl.(*tables.MemoryTable).Truncate()
	return insertData(tbl, rows)
}

func (h *Handle) refillMemoryTables(schemas []*model.DBInfo) error {
	dbNames := make([]string, 0, len(schemas))
	for _, v := range schemas {
		dbNames = append(dbNames, v.Name.L)
	}
	err := refillMemoryTable(h.memSchema.schemataTbl, dataForSchemata(dbNames))
	if err != nil {
		return errors.Trace(err)
	}
	err = refillMemoryTable(h.memSchema.tablesTbl, dataForTables(schemas))
	if err != nil {
		return errors.Trace(err)
	}
	err = refillMemoryTable(h.memSchema.columnsTbl, dataForColumns(schemas))
	if err != nil {
		return errors.Trace(err)
	}
	err = refillMemoryTable(h.memSchema.statisticsTbl, dataForStatistics(schemas))
	return errors.Trace(err)
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

// EmptyClone creates a new Handle with the same store and memSchema, but the value is not set.
func (h *Handle) EmptyClone() *Handle {
	newHandle := &Handle{
		store:     h.store,
		memSchema: h.memSchema,
	}
	return newHandle
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
	initInfoSchemaDB()
}

var (
	infoSchemaDB *model.DBInfo
)

func initInfoSchemaDB() {
	dbID := autoid.GenLocalSchemaID()
	infoSchemaTables := make([]*model.TableInfo, 0, len(tableNameToColumns))
	for name, cols := range tableNameToColumns {
		tableInfo := buildTableMeta(name, cols)
		infoSchemaTables = append(infoSchemaTables, tableInfo)
		tableInfo.ID = autoid.GenLocalSchemaID()
		for _, c := range tableInfo.Columns {
			c.ID = autoid.GenLocalSchemaID()
		}
	}
	infoSchemaDB = &model.DBInfo{
		ID:      dbID,
		Name:    model.NewCIStr(Name),
		Charset: mysql.DefaultCharset,
		Collate: mysql.DefaultCollationName,
		Tables:  infoSchemaTables,
	}
}
