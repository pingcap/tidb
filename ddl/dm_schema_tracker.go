// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
)

// infoStore is a simple structure that stores DBInfo and TableInfo. It's not thread-safe.
type infoStore struct {
	lowerCaseTableNames int // same as variable lower_case_table_names

	dbs    map[string]*model.DBInfo
	tables map[string]map[string]*model.TableInfo
}

func newInfoStore(lowerCaseTableNames int) *infoStore {
	return &infoStore{
		lowerCaseTableNames: lowerCaseTableNames,
		dbs:                 map[string]*model.DBInfo{},
		tables:              map[string]map[string]*model.TableInfo{},
	}
}

func (i *infoStore) ciStr2Key(name model.CIStr) string {
	if i.lowerCaseTableNames == 0 {
		return name.O
	}
	return name.L
}

// SchemaByName is exported to be used when infoStore is embedded into another public struct.
func (i *infoStore) SchemaByName(name model.CIStr) *model.DBInfo {
	key := i.ciStr2Key(name)
	return i.dbs[key]
}

func (i *infoStore) putSchema(dbInfo *model.DBInfo) {
	key := i.ciStr2Key(dbInfo.Name)
	i.dbs[key] = dbInfo
	if i.tables[key] == nil {
		i.tables[key] = map[string]*model.TableInfo{}
	}
}

func (i *infoStore) deleteSchema(name model.CIStr) bool {
	key := i.ciStr2Key(name)
	_, ok := i.dbs[key]
	if !ok {
		return false
	}
	delete(i.dbs, key)
	delete(i.tables, key)
	return true
}

// TableByName is exported to be used when infoStore is embedded into another public struct.
func (i *infoStore) TableByName(schema, table model.CIStr) (*model.TableInfo, error) {
	schemaKey := i.ciStr2Key(schema)
	tables, ok := i.tables[schemaKey]
	if !ok {
		return nil, infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schema)
	}

	tableKey := i.ciStr2Key(table)
	tbl, ok := tables[tableKey]
	if !ok {
		return nil, infoschema.ErrTableNotExists.GenWithStackByArgs(schema, table)
	}
	return tbl, nil
}

func (i *infoStore) putTable(schemaName model.CIStr, tblInfo *model.TableInfo) error {
	schemaKey := i.ciStr2Key(schemaName)
	tables, ok := i.tables[schemaKey]
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schemaName)
	}
	tableKey := i.ciStr2Key(tblInfo.Name)
	tables[tableKey] = tblInfo
	return nil
}

func (i *infoStore) deleteTable(schema, table model.CIStr) error {
	schemaKey := i.ciStr2Key(schema)
	tables, ok := i.tables[schemaKey]
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schema)
	}

	tableKey := i.ciStr2Key(table)
	_, ok = tables[tableKey]
	if !ok {
		return infoschema.ErrTableNotExists.GenWithStackByArgs(schema, table)
	}
	delete(tables, tableKey)
	return nil
}

// infoSchemaAdaptor convert infoStore to infoschema.InfoSchema, it only implements a part of InfoSchema interface to be
// used by DDL interface.
type infoSchemaAdaptor struct {
	infoschema.InfoSchema
	inner *infoStore
}

// SchemaByName implements the InfoSchema interface.
func (i infoSchemaAdaptor) SchemaByName(schema model.CIStr) (*model.DBInfo, bool) {
	dbInfo := i.inner.SchemaByName(schema)
	return dbInfo, dbInfo != nil
}

// TableExists implements the InfoSchema interface.
func (i infoSchemaAdaptor) TableExists(schema, table model.CIStr) bool {
	tableInfo, _ := i.inner.TableByName(schema, table)
	return tableInfo != nil
}

// TableIsView implements the InfoSchema interface.
func (i infoSchemaAdaptor) TableIsView(schema, table model.CIStr) bool {
	tableInfo, _ := i.inner.TableByName(schema, table)
	if tableInfo == nil {
		return false
	}
	return tableInfo.IsView()
}

// TableByName implements the InfoSchema interface.
func (i infoSchemaAdaptor) TableByName(schema, table model.CIStr) (t table.Table, err error) {
	tableInfo, err := i.inner.TableByName(schema, table)
	if err != nil {
		return nil, err
	}
	return tables.MockTableFromMeta(tableInfo), nil
}
