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
	"context"
	"sort"
	"sync/atomic"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	// ErrDatabaseDropExists returns for dropping a non-existent database.
	ErrDatabaseDropExists = terror.ClassSchema.New(codeDBDropExists, "Can't drop database '%s'; database doesn't exist")
	// ErrDatabaseNotExists returns for database not exists.
	ErrDatabaseNotExists = terror.ClassSchema.New(codeDatabaseNotExists, "Unknown database '%s'")
	// ErrTableNotExists returns for table not exists.
	ErrTableNotExists = terror.ClassSchema.New(codeTableNotExists, "Table '%s.%s' doesn't exist")
	// ErrColumnNotExists returns for column not exists.
	ErrColumnNotExists = terror.ClassSchema.New(codeColumnNotExists, "Unknown column '%s' in '%s'")
	// ErrForeignKeyNotMatch returns for foreign key not match.
	ErrForeignKeyNotMatch = terror.ClassSchema.New(codeWrongFkDef, "Incorrect foreign key definition for '%s': Key reference and table reference don't match")
	// ErrCannotAddForeign returns for foreign key exists.
	ErrCannotAddForeign = terror.ClassSchema.New(codeCannotAddForeign, "Cannot add foreign key constraint")
	// ErrForeignKeyNotExists returns for foreign key not exists.
	ErrForeignKeyNotExists = terror.ClassSchema.New(codeForeignKeyNotExists, "Can't DROP '%s'; check that column/key exists")
	// ErrDatabaseExists returns for database already exists.
	ErrDatabaseExists = terror.ClassSchema.New(codeDatabaseExists, "Can't create database '%s'; database exists")
	// ErrTableExists returns for table already exists.
	ErrTableExists = terror.ClassSchema.New(codeTableExists, "Table '%s' already exists")
	// ErrTableDropExists returns for dropping a non-existent table.
	ErrTableDropExists = terror.ClassSchema.New(codeBadTable, "Unknown table '%s'")
	// ErrUserDropExists returns for dropping a non-existent user.
	ErrUserDropExists = terror.ClassSchema.New(codeBadUser, "User %s does not exist.")
	// ErrColumnExists returns for column already exists.
	ErrColumnExists = terror.ClassSchema.New(codeColumnExists, "Duplicate column name '%s'")
	// ErrIndexExists returns for index already exists.
	ErrIndexExists = terror.ClassSchema.New(codeIndexExists, "Duplicate Index")
	// ErrKeyNameDuplicate returns for index duplicate when rename index.
	ErrKeyNameDuplicate = terror.ClassSchema.New(codeKeyNameDuplicate, "Duplicate key name '%s'")
	// ErrKeyNotExists returns for index not exists.
	ErrKeyNotExists = terror.ClassSchema.New(codeKeyNotExists, "Key '%s' doesn't exist in table '%s'")
	// ErrMultiplePriKey returns for multiple primary keys.
	ErrMultiplePriKey = terror.ClassSchema.New(codeMultiplePriKey, "Multiple primary key defined")
	// ErrTooManyKeyParts returns for too many key parts.
	ErrTooManyKeyParts = terror.ClassSchema.New(codeTooManyKeyParts, "Too many key parts specified; max %d parts allowed")
	// ErrTableNotLockedForWrite returns for write tables when only hold the table read lock.
	ErrTableNotLockedForWrite = terror.ClassSchema.New(codeErrTableNotLockedForWrite, mysql.MySQLErrName[mysql.ErrTableNotLockedForWrite])
	// ErrTableNotLocked returns when session has explicitly lock tables, then visit unlocked table will return this error.
	ErrTableNotLocked = terror.ClassSchema.New(codeErrTableNotLocked, mysql.MySQLErrName[mysql.ErrTableNotLocked])
	// ErrNonuniqTable returns when none unique tables errors.
	ErrNonuniqTable = terror.ClassSchema.New(codeErrTableNotLocked, mysql.MySQLErrName[mysql.ErrNonuniqTable])
	// ErrTableLocked returns when the table was locked by other session.
	ErrTableLocked = terror.ClassSchema.New(codeTableLocked, mysql.MySQLErrName[mysql.ErrTableLocked])
	// ErrAccessDenied return when the user doesn't have the permission to access the table.
	ErrAccessDenied = terror.ClassSchema.New(codeErrAccessDenied, mysql.MySQLErrName[mysql.ErrAccessDenied])
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
	SchemaByTable(tableInfo *model.TableInfo) (*model.DBInfo, bool)
	TableByID(id int64) (table.Table, bool)
	AllocByID(id int64) (autoid.Allocators, bool)
	AllSchemaNames() []string
	AllSchemas() []*model.DBInfo
	Clone() (result []*model.DBInfo)
	SchemaTables(schema model.CIStr) []table.Table
	SchemaMetaVersion() int64
	// TableIsView indicates whether the schema.table is a view.
	TableIsView(schema, table model.CIStr) bool
	FindTableByPartitionID(partitionID int64) (table.Table, *model.DBInfo)
}

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

	// schemaMetaVersion is the version of schema, and we should check version when change schema.
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

// MockInfoSchemaWithSchemaVer only serves for test.
func MockInfoSchemaWithSchemaVer(tbList []*model.TableInfo, schemaVer int64) InfoSchema {
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
	result.schemaMetaVersion = schemaVer
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
	return nil, ErrTableNotExists.GenWithStackByArgs(schema, table)
}

func (is *infoSchema) TableIsView(schema, table model.CIStr) bool {
	if tbNames, ok := is.schemaMap[schema.L]; ok {
		if t, ok := tbNames.tables[table.L]; ok {
			return t.Meta().IsView()
		}
	}
	return false
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

func (is *infoSchema) SchemaByTable(tableInfo *model.TableInfo) (val *model.DBInfo, ok bool) {
	if tableInfo == nil {
		return nil, false
	}
	for _, v := range is.schemaMap {
		if tbl, ok := v.tables[tableInfo.Name.L]; ok {
			if tbl.Meta().ID == tableInfo.ID {
				return v.dbInfo, true
			}
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

func (is *infoSchema) AllocByID(id int64) (autoid.Allocators, bool) {
	tbl, ok := is.TableByID(id)
	if !ok {
		return nil, false
	}
	return tbl.AllAllocators(nil), true
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

// FindTableByPartitionID finds the partition-table info by the partitionID.
// FindTableByPartitionID will traverse all the tables to find the partitionID partition in which partition-table.
func (is *infoSchema) FindTableByPartitionID(partitionID int64) (table.Table, *model.DBInfo) {
	for _, v := range is.schemaMap {
		for _, tbl := range v.tables {
			pi := tbl.Meta().GetPartitionInfo()
			if pi == nil {
				continue
			}
			for _, p := range pi.Definitions {
				if p.ID == partitionID {
					return tbl, v.dbInfo
				}
			}
		}
	}
	return nil, nil
}

func (is *infoSchema) Clone() (result []*model.DBInfo) {
	for _, v := range is.schemaMap {
		result = append(result, v.dbInfo.Clone())
	}
	return
}

// Handle handles information schema, including getting and setting.
type Handle struct {
	value atomic.Value
	store kv.Storage
}

// NewHandle creates a new Handle.
func NewHandle(store kv.Storage) *Handle {
	h := &Handle{
		store: store,
	}
	return h
}

// Get gets information schema from Handle.
func (h *Handle) Get() InfoSchema {
	v := h.value.Load()
	schema, _ := v.(InfoSchema)
	return schema
}

// IsValid uses to check whether handle value is valid.
func (h *Handle) IsValid() bool {
	return h.value.Load() != nil
}

// EmptyClone creates a new Handle with the same store and memSchema, but the value is not set.
func (h *Handle) EmptyClone() *Handle {
	newHandle := &Handle{
		store: h.store,
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
	codeWrongFkDef          = 1239

	codeDatabaseExists   = 1007
	codeTableExists      = 1050
	codeBadTable         = 1051
	codeBadUser          = 3162
	codeColumnExists     = 1060
	codeIndexExists      = 1831
	codeMultiplePriKey   = 1068
	codeTooManyKeyParts  = 1070
	codeKeyNameDuplicate = 1061
	codeKeyNotExists     = 1176

	codeErrTableNotLockedForWrite = mysql.ErrTableNotLockedForWrite
	codeErrTableNotLocked         = mysql.ErrTableNotLocked
	codeErrNonuniqTable           = mysql.ErrNonuniqTable
	codeErrAccessDenied           = mysql.ErrAccessDenied
	codeTableLocked               = mysql.ErrTableLocked
)

func init() {
	schemaMySQLErrCodes := map[terror.ErrCode]uint16{
		codeDBDropExists:              mysql.ErrDBDropExists,
		codeDatabaseNotExists:         mysql.ErrBadDB,
		codeTableNotExists:            mysql.ErrNoSuchTable,
		codeColumnNotExists:           mysql.ErrBadField,
		codeCannotAddForeign:          mysql.ErrCannotAddForeign,
		codeWrongFkDef:                mysql.ErrWrongFkDef,
		codeForeignKeyNotExists:       mysql.ErrCantDropFieldOrKey,
		codeDatabaseExists:            mysql.ErrDBCreateExists,
		codeTableExists:               mysql.ErrTableExists,
		codeBadTable:                  mysql.ErrBadTable,
		codeBadUser:                   mysql.ErrBadUser,
		codeColumnExists:              mysql.ErrDupFieldName,
		codeIndexExists:               mysql.ErrDupIndex,
		codeMultiplePriKey:            mysql.ErrMultiplePriKey,
		codeTooManyKeyParts:           mysql.ErrTooManyKeyParts,
		codeKeyNameDuplicate:          mysql.ErrDupKeyName,
		codeKeyNotExists:              mysql.ErrKeyDoesNotExist,
		codeErrTableNotLockedForWrite: mysql.ErrTableNotLockedForWrite,
		codeErrTableNotLocked:         mysql.ErrTableNotLocked,
		codeErrNonuniqTable:           mysql.ErrNonuniqTable,
		mysql.ErrAccessDenied:         mysql.ErrAccessDenied,
		codeTableLocked:               mysql.ErrTableLocked,
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
		Name:    util.InformationSchemaName,
		Charset: mysql.DefaultCharset,
		Collate: mysql.DefaultCollationName,
		Tables:  infoSchemaTables,
	}
}

// IsMemoryDB checks if the db is in memory.
func IsMemoryDB(dbName string) bool {
	if dbName == "information_schema" {
		return true
	}
	for _, driver := range drivers {
		if driver.DBInfo.Name.L == dbName {
			return true
		}
	}
	return false
}

// HasAutoIncrementColumn checks whether the table has auto_increment columns, if so, return true and the column name.
func HasAutoIncrementColumn(tbInfo *model.TableInfo) (bool, string) {
	for _, col := range tbInfo.Columns {
		if mysql.HasAutoIncrementFlag(col.Flag) {
			return true, col.Name.L
		}
	}
	return false, ""
}

// GetInfoSchema gets TxnCtx InfoSchema if snapshot schema is not set,
// Otherwise, snapshot schema is returned.
func GetInfoSchema(ctx sessionctx.Context) InfoSchema {
	return GetInfoSchemaBySessionVars(ctx.GetSessionVars())
}

// GetInfoSchemaBySessionVars gets TxnCtx InfoSchema if snapshot schema is not set,
// Otherwise, snapshot schema is returned.
func GetInfoSchemaBySessionVars(sessVar *variable.SessionVars) InfoSchema {
	var is InfoSchema
	if snap := sessVar.SnapshotInfoschema; snap != nil {
		is = snap.(InfoSchema)
		logutil.Logger(context.Background()).Info("use snapshot schema", zap.Uint64("conn", sessVar.ConnectionID), zap.Int64("schemaVersion", is.SchemaMetaVersion()))
	} else {
		is = sessVar.TxnCtx.InfoSchema.(InfoSchema)
	}
	return is
}
