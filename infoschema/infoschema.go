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
	"encoding/json"
	"sync/atomic"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/table"
	// import table implementation to init table.TableFromMeta
	_ "github.com/pingcap/tidb/table/tables"
)

// InfoSchema is the interface used to retrieve the schema information.
// It works as a in memory cache and doesn't handle any schema change.
// InfoSchema is read-only, and the returned value is a copy.
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
	ColumnByID(id int64) (*model.ColumnInfo, bool)
	ColumnIndices(id int64) []*model.IndexInfo
	AllSchemaNames() []string
	AllSchemas() []*model.DBInfo
	Clone() (result []*model.DBInfo)
	SchemaTables(schema model.CIStr) []table.Table
	// TODO: add more methods to retrieve tables and columns.
}

// Infomation Schema Name.
const (
	Name = "INFORMATION_SCHEMA"
)

type infoSchema struct {
	schemaNameToID map[string]int64
	tableNameToID  map[tableName]int64
	columnNameToID map[columnName]int64
	schemas        map[int64]*model.DBInfo
	tables         map[int64]table.Table
	columns        map[int64]*model.ColumnInfo
	indices        map[indexName]*model.IndexInfo
	columnIndices  map[int64][]*model.IndexInfo
}

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

func (is *infoSchema) SchemaExists(schema model.CIStr) bool {
	_, ok := is.schemaNameToID[schema.L]
	return ok
}

func (is *infoSchema) TableByName(schema, table model.CIStr) (t table.Table, err error) {
	id, ok := is.tableNameToID[tableName{schema: schema.L, table: table.L}]
	if !ok {
		return nil, errors.Errorf("table %s.%s does not exist", schema, table)
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

func (is *infoSchema) ColumnByID(id int64) (val *model.ColumnInfo, ok bool) {
	val, ok = is.columns[id]
	return
}

func (is *infoSchema) ColumnIndices(id int64) (indices []*model.IndexInfo) {
	return is.columnIndices[id]
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
		// TODO: this is a temporary solution, change to real clone later.
		b, _ := json.Marshal(v)
		var newInfo model.DBInfo
		json.Unmarshal(b, &newInfo)
		result = append(result, &newInfo)
	}
	return
}

// Handle handles information schema, including getting and setting.
type Handle struct {
	value atomic.Value
	store kv.Storage
	// We should to check version when change schema
	schemaMetaVersion int64
}

// NewHandle creates a new Handle.
func NewHandle(store kv.Storage) *Handle {
	return &Handle{
		store: store,
	}
}

func (h *Handle) SetSchemaMetaVersion(ver int64) {
	h.schemaMetaVersion = ver
}

// SchemaMetaVersion return version of schema meta
func (h *Handle) SchemaMetaVersion() int64 {
	return h.schemaMetaVersion
}

// Set sets DBInfo to information schema.
func (h *Handle) Set(newInfo []*model.DBInfo) {
	info := &infoSchema{
		schemaNameToID: map[string]int64{},
		tableNameToID:  map[tableName]int64{},
		columnNameToID: map[columnName]int64{},
		schemas:        map[int64]*model.DBInfo{},
		tables:         map[int64]table.Table{},
		columns:        map[int64]*model.ColumnInfo{},
		indices:        map[indexName]*model.IndexInfo{},
		columnIndices:  map[int64][]*model.IndexInfo{},
	}
	for _, di := range newInfo {
		info.schemas[di.ID] = di
		info.schemaNameToID[di.Name.L] = di.ID
		for _, t := range di.Tables {
			alloc := autoid.NewAllocator(h.store)
			info.tables[t.ID] = table.TableFromMeta(di.Name.L, alloc, t)
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
	h.value.Store(info)
}

// Get gets information schema from Handle.
func (h *Handle) Get() InfoSchema {
	return h.value.Load().(InfoSchema)
}
