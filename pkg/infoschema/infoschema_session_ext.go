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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package infoschema

import (
	stdctx "context"
	"sync"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/table"
)

// SessionExtendedInfoSchema implements InfoSchema
// Local temporary table has a loose relationship with database.
// So when a database is dropped, its temporary tables still exist and can be returned by TableByName/TableByID.
type SessionExtendedInfoSchema struct {
	InfoSchema
	LocalTemporaryTablesOnce sync.Once
	LocalTemporaryTables     *SessionTables
	MdlTables                *SessionTables
}

// TableByName implements InfoSchema.TableByName
func (ts *SessionExtendedInfoSchema) TableByName(ctx stdctx.Context, schema, table ast.CIStr) (table.Table, error) {
	if ts.LocalTemporaryTables != nil {
		if tbl, ok := ts.LocalTemporaryTables.TableByName(ctx, schema, table); ok {
			return tbl, nil
		}
	}

	if ts.MdlTables != nil {
		if tbl, ok := ts.MdlTables.TableByName(ctx, schema, table); ok {
			return tbl, nil
		}
	}

	return ts.InfoSchema.TableByName(ctx, schema, table)
}

// TableInfoByName implements InfoSchema.TableInfoByName
func (ts *SessionExtendedInfoSchema) TableInfoByName(schema, table ast.CIStr) (*model.TableInfo, error) {
	tbl, err := ts.TableByName(stdctx.Background(), schema, table)
	return getTableInfo(tbl), err
}

// TableInfoByID implements InfoSchema.TableInfoByID
func (ts *SessionExtendedInfoSchema) TableInfoByID(id int64) (*model.TableInfo, bool) {
	tbl, ok := ts.TableByID(stdctx.Background(), id)
	return getTableInfo(tbl), ok
}

// FindTableInfoByPartitionID implements InfoSchema.FindTableInfoByPartitionID
func (ts *SessionExtendedInfoSchema) FindTableInfoByPartitionID(
	partitionID int64,
) (*model.TableInfo, *model.DBInfo, *model.PartitionDefinition) {
	tbl, db, partDef := ts.FindTableByPartitionID(partitionID)
	return getTableInfo(tbl), db, partDef
}

// TableByID implements InfoSchema.TableByID
func (ts *SessionExtendedInfoSchema) TableByID(ctx stdctx.Context, id int64) (table.Table, bool) {
	if !tableIDIsValid(id) {
		return nil, false
	}

	if ts.LocalTemporaryTables != nil {
		if tbl, ok := ts.LocalTemporaryTables.TableByID(id); ok {
			return tbl, true
		}
	}

	if ts.MdlTables != nil {
		if tbl, ok := ts.MdlTables.TableByID(id); ok {
			return tbl, true
		}
	}

	return ts.InfoSchema.TableByID(ctx, id)
}

// SchemaByID implements InfoSchema.SchemaByID, it returns a stale DBInfo even if it's dropped.
func (ts *SessionExtendedInfoSchema) SchemaByID(id int64) (*model.DBInfo, bool) {
	if ts.LocalTemporaryTables != nil {
		if db, ok := ts.LocalTemporaryTables.SchemaByID(id); ok {
			return db, true
		}
	}

	if ts.MdlTables != nil {
		if tbl, ok := ts.MdlTables.SchemaByID(id); ok {
			return tbl, true
		}
	}

	ret, ok := ts.InfoSchema.SchemaByID(id)
	return ret, ok
}

// UpdateTableInfo implements InfoSchema.SchemaByTable.
func (ts *SessionExtendedInfoSchema) UpdateTableInfo(db *model.DBInfo, tableInfo table.Table) error {
	if ts.MdlTables == nil {
		ts.MdlTables = NewSessionTables()
	}
	err := ts.MdlTables.AddTable(db, tableInfo)
	if err != nil {
		return err
	}
	return nil
}

// HasTemporaryTable returns whether information schema has temporary table
func (ts *SessionExtendedInfoSchema) HasTemporaryTable() bool {
	return ts.LocalTemporaryTables != nil && ts.LocalTemporaryTables.Count() > 0 || ts.InfoSchema.HasTemporaryTable()
}

// DetachTemporaryTableInfoSchema returns a new SessionExtendedInfoSchema without temporary tables
func (ts *SessionExtendedInfoSchema) DetachTemporaryTableInfoSchema() *SessionExtendedInfoSchema {
	return &SessionExtendedInfoSchema{
		InfoSchema: ts.InfoSchema,
		MdlTables:  ts.MdlTables,
	}
}

// FindTableByTblOrPartID looks for table.Table for the given id in the InfoSchema.
// The id can be either a table id or a partition id.
// If the id is a table id, the corresponding table.Table will be returned, and the second return value is nil.
// If the id is a partition id, the corresponding table.Table and PartitionDefinition will be returned.
// If the id is not found in the InfoSchema, nil will be returned for both return values.
func FindTableByTblOrPartID(is InfoSchema, id int64) (table.Table, *model.PartitionDefinition) {
	tbl, ok := is.TableByID(stdctx.Background(), id)
	if ok {
		return tbl, nil
	}
	tbl, _, partDef := is.FindTableByPartitionID(id)
	return tbl, partDef
}

func getTableInfo(tbl table.Table) *model.TableInfo {
	if tbl == nil {
		return nil
	}
	return tbl.Meta()
}

func getTableInfoList(tables []table.Table) []*model.TableInfo {
	if tables == nil {
		return nil
	}

	infoLost := make([]*model.TableInfo, 0, len(tables))
	for _, tbl := range tables {
		infoLost = append(infoLost, tbl.Meta())
	}
	return infoLost
}
