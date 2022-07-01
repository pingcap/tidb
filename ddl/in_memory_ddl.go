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
	"context"
	"strings"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"

	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	pumpcli "github.com/pingcap/tidb/tidb-binlog/pump_client"
	"github.com/pingcap/tidb/util/dbterror"
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

type infoSchemaAdaptor struct {
	infoschema.InfoSchema
	inner *infoStore
}

func (i infoSchemaAdaptor) SchemaByName(schema model.CIStr) (*model.DBInfo, bool) {
	dbInfo := i.inner.SchemaByName(schema)
	return dbInfo, dbInfo != nil
}

func (i infoSchemaAdaptor) TableExists(schema, table model.CIStr) bool {
	tableInfo, _ := i.inner.TableByName(schema, table)
	return tableInfo != nil
}

func (i infoSchemaAdaptor) TableIsView(schema, table model.CIStr) bool {
	tableInfo, _ := i.inner.TableByName(schema, table)
	if tableInfo == nil {
		return false
	}
	return tableInfo.IsView()
}

func (i infoSchemaAdaptor) TableByName(schema, table model.CIStr) (t table.Table, err error) {
	tableInfo, err := i.inner.TableByName(schema, table)
	if err != nil {
		return nil, err
	}
	return tables.MockTableFromMeta(tableInfo), nil
}

var _ DDL = SchemaTracker{}

type SchemaTracker struct {
	*infoStore
}

func NewInMemoryDDL(lowerCaseTableNames int) SchemaTracker {
	return SchemaTracker{
		infoStore: newInfoStore(lowerCaseTableNames),
	}
}

func (d SchemaTracker) CreateSchema(ctx sessionctx.Context, stmt *ast.CreateDatabaseStmt) error {
	// we only consider explicit charset/collate, if not found, fallback to default charset/collate.
	charsetOpt := ast.CharsetOpt{}
	for _, val := range stmt.Options {
		switch val.Tp {
		case ast.DatabaseOptionCharset:
			charsetOpt.Chs = val.Value
		case ast.DatabaseOptionCollate:
			charsetOpt.Col = val.Value
		}
	}

	chs, coll, err := ResolveCharsetCollation(charsetOpt)
	if err != nil {
		return errors.Trace(err)
	}

	dbInfo := &model.DBInfo{Name: stmt.Name, Charset: chs, Collate: coll}
	onExist := OnExistError
	if stmt.IfNotExists {
		onExist = OnExistIgnore
	}
	return d.CreateSchemaWithInfo(ctx, dbInfo, onExist)
}

func (d SchemaTracker) CreateSchemaWithInfo(ctx sessionctx.Context, dbInfo *model.DBInfo, onExist OnExist) error {
	oldInfo := d.SchemaByName(dbInfo.Name)
	if oldInfo != nil {
		if onExist == OnExistIgnore {
			return nil
		}
		// not support MariaDB's CREATE OR REPLACE SCHEMA
		return infoschema.ErrDatabaseExists.GenWithStackByArgs(dbInfo.Name)
	}
	d.putSchema(dbInfo)
	return nil
}

func (d SchemaTracker) AlterSchema(ctx sessionctx.Context, stmt *ast.AlterDatabaseStmt) error {
	dbInfo := d.SchemaByName(stmt.Name)
	if dbInfo == nil {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(stmt.Name.O)
	}

	for _, val := range stmt.Options {
		switch val.Tp {
		case ast.DatabaseOptionCharset:
			dbInfo.Charset = val.Value
		case ast.DatabaseOptionCollate:
			dbInfo.Collate = val.Value
		}
	}

	return nil
}

func (d SchemaTracker) DropSchema(ctx sessionctx.Context, stmt *ast.DropDatabaseStmt) error {
	ok := d.deleteSchema(stmt.Name)
	if !ok {
		if stmt.IfExists {
			return nil
		}
		return infoschema.ErrDatabaseDropExists.GenWithStackByArgs(stmt.Name)
	}
	return nil
}

func (d SchemaTracker) CreateTable(ctx sessionctx.Context, s *ast.CreateTableStmt) error {
	ident := ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name}
	schema := d.SchemaByName(ident.Schema)
	if schema == nil {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}

	var referTbl *model.TableInfo
	if s.ReferTable != nil {
		_, err := d.TableByName(s.ReferTable.Schema, s.ReferTable.Name)
		if err != nil {
			return infoschema.ErrTableNotExists.GenWithStackByArgs(s.ReferTable.Schema, s.ReferTable.Name)
		}
	}

	// build tableInfo
	var (
		tbInfo *model.TableInfo
		err    error
	)
	if s.ReferTable != nil {
		tbInfo, err = buildTableInfoWithLike(ctx, ident, referTbl, s)
	} else {
		tbInfo, err = buildTableInfoWithStmt(ctx, s, schema.Charset, schema.Collate, schema.PlacementPolicyRef)
	}
	if err != nil {
		return errors.Trace(err)
	}

	onExist := OnExistError
	if s.IfNotExists {
		onExist = OnExistIgnore
	}

	return d.CreateTableWithInfo(ctx, schema.Name, tbInfo, onExist)
}

func (d SchemaTracker) CreateTableWithInfo(
	ctx sessionctx.Context,
	dbName model.CIStr,
	info *model.TableInfo,
	onExist OnExist,
) error {
	schema := d.SchemaByName(dbName)
	if schema == nil {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(dbName)
	}

	oldTable, _ := d.TableByName(dbName, info.Name)
	if oldTable != nil {
		switch onExist {
		case OnExistIgnore:
			return nil
		case OnExistReplace:
			return d.putTable(dbName, info)
		default:
			return infoschema.ErrTableExists.GenWithStackByArgs(ast.Ident{Schema: dbName, Name: info.Name})
		}
	}

	return d.putTable(dbName, info)
}

func (d SchemaTracker) CreateView(ctx sessionctx.Context, s *ast.CreateViewStmt) error {
	viewInfo, err := buildViewInfo(ctx, s)
	if err != nil {
		return err
	}

	cols := make([]*table.Column, len(s.Cols))
	for i, v := range s.Cols {
		cols[i] = table.ToColumn(&model.ColumnInfo{
			Name:   v,
			ID:     int64(i),
			Offset: i,
			State:  model.StatePublic,
		})
	}

	tbInfo, err := buildTableInfo(ctx, s.ViewName.Name, cols, nil, "", "")
	if err != nil {
		return err
	}
	tbInfo.View = viewInfo

	onExist := OnExistError
	if s.OrReplace {
		onExist = OnExistReplace
	}

	return d.CreateTableWithInfo(ctx, s.ViewName.Schema, tbInfo, onExist)
}

func (d SchemaTracker) DropTable(ctx sessionctx.Context, stmt *ast.DropTableStmt) (err error) {
	notExistTables := make([]string, 0, len(stmt.Tables))
	for _, name := range stmt.Tables {
		tb, err := d.TableByName(name.Schema, name.Name)
		if err != nil || !tb.IsBaseTable() {
			if stmt.IfExists {
				continue
			}

			id := ast.Ident{Schema: name.Schema, Name: name.Name}
			notExistTables = append(notExistTables, id.String())
			continue
		}

		_ = d.deleteTable(name.Schema, name.Name)
	}

	if len(notExistTables) > 0 {
		return infoschema.ErrTableDropExists.GenWithStackByArgs(strings.Join(notExistTables, ","))
	}
	return nil
}

func (d SchemaTracker) RecoverTable(ctx sessionctx.Context, recoverInfo *RecoverInfo) (err error) {
	return nil
}

func (d SchemaTracker) DropView(ctx sessionctx.Context, stmt *ast.DropTableStmt) (err error) {
	notExistTables := make([]string, 0, len(stmt.Tables))
	for _, name := range stmt.Tables {
		tb, err := d.TableByName(name.Schema, name.Name)
		if err != nil {
			if stmt.IfExists {
				continue
			}

			id := ast.Ident{Schema: name.Schema, Name: name.Name}
			notExistTables = append(notExistTables, id.String())
			continue
		}

		if !tb.IsView() {
			return dbterror.ErrWrongObject.GenWithStackByArgs(name.Schema, name.Name, "VIEW")
		}

		_ = d.deleteTable(name.Schema, name.Name)
	}

	if len(notExistTables) > 0 {
		return infoschema.ErrTableDropExists.GenWithStackByArgs(strings.Join(notExistTables, ","))
	}
	return nil
}

func (d SchemaTracker) CreateIndex(ctx sessionctx.Context, stmt *ast.CreateIndexStmt) error {
	ident := ast.Ident{Schema: stmt.Table.Schema, Name: stmt.Table.Name}
	return d.createIndex(ctx, ident, stmt.KeyType, model.NewCIStr(stmt.IndexName),
		stmt.IndexPartSpecifications, stmt.IndexOption, stmt.IfNotExists)
}

func (d SchemaTracker) createIndex(ctx sessionctx.Context, ti ast.Ident, keyType ast.IndexKeyType, indexName model.CIStr,
	indexPartSpecifications []*ast.IndexPartSpecification, indexOption *ast.IndexOption, ifNotExists bool) error {

	unique := keyType == ast.IndexKeyTypeUnique
	tblInfo, err := d.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return err
	}
	t := tables.MockTableFromMeta(tblInfo)

	// Deal with anonymous index.
	if len(indexName.L) == 0 {
		colName := model.NewCIStr("expression_index")
		if indexPartSpecifications[0].Column != nil {
			colName = indexPartSpecifications[0].Column.Name
		}
		indexName = getAnonymousIndex(t, colName, model.NewCIStr(""))
	}

	if indexInfo := tblInfo.FindIndexByName(indexName.L); indexInfo != nil {
		if ifNotExists {
			return nil
		}
		return dbterror.ErrDupKeyName.GenWithStack("index already exist %s", indexName)
	}

	// Skip build hidden column for expression index

	indexInfo, err := buildIndexInfo(nil, tblInfo, indexName, indexPartSpecifications, model.StatePublic)
	if err != nil {
		return err
	}
	if indexOption != nil {
		indexInfo.Comment = indexOption.Comment
		if indexOption.Visibility == ast.IndexVisibilityInvisible {
			indexInfo.Invisible = true
		}
		if indexOption.Tp == model.IndexTypeInvalid {
			// Use btree as default index type.
			indexInfo.Tp = model.IndexTypeBtree
		} else {
			indexInfo.Tp = indexOption.Tp
		}
	} else {
		// Use btree as default index type.
		indexInfo.Tp = model.IndexTypeBtree
	}
	indexInfo.Primary = false
	indexInfo.Unique = unique
	indexInfo.ID = allocateIndexID(tblInfo)
	tblInfo.Indices = append(tblInfo.Indices, indexInfo)

	addIndexColumnFlag(tblInfo, indexInfo)
	return nil
}

func (d SchemaTracker) DropIndex(ctx sessionctx.Context, stmt *ast.DropIndexStmt) error {
	ti := ast.Ident{Schema: stmt.Table.Schema, Name: stmt.Table.Name}
	err := d.dropIndex(ctx, ti, model.NewCIStr(stmt.IndexName), stmt.IfExists)
	if (infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableNotExists.Equal(err)) && stmt.IfExists {
		err = nil
	}
	return err
}

func (d SchemaTracker) dropIndex(ctx sessionctx.Context, ti ast.Ident, indexName model.CIStr, ifExists bool) error {
	tblInfo, err := d.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return infoschema.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name)
	}
	t := tables.MockTableFromMeta(tblInfo)

	indexInfo := tblInfo.FindIndexByName(indexName.L)

	_, err = checkIsDropPrimaryKey(indexName, indexInfo, t)
	if err != nil {
		return err
	}

	if indexInfo == nil {
		if ifExists {
			return nil
		}
		return dbterror.ErrCantDropFieldOrKey.GenWithStack("index %s doesn't exist", indexName)
	}

	// Check for drop index on auto_increment column.
	err = checkDropIndexOnAutoIncrementColumn(t.Meta(), indexInfo)
	if err != nil {
		return err
	}

	newIndices := make([]*model.IndexInfo, 0, len(tblInfo.Indices))
	for _, idx := range tblInfo.Indices {
		if idx.Name.L != indexInfo.Name.L {
			newIndices = append(newIndices, idx)
		}
	}
	tblInfo.Indices = newIndices
	// Set column index flag.
	dropIndexColumnFlag(tblInfo, indexInfo)
	return nil
}

func (d SchemaTracker) AlterTable(ctx context.Context, sctx sessionctx.Context, stmt *ast.AlterTableStmt) error {
	//TODO implement me
	panic("implement me")
}

func (d SchemaTracker) TruncateTable(ctx sessionctx.Context, tableIdent ast.Ident) error {
	return nil
}

func (d SchemaTracker) RenameTable(ctx sessionctx.Context, stmt *ast.RenameTableStmt) error {
	oldIdents := make([]ast.Ident, 0, len(stmt.TableToTables))
	newIdents := make([]ast.Ident, 0, len(stmt.TableToTables))
	for _, tablePair := range stmt.TableToTables {
		oldIdent := ast.Ident{Schema: tablePair.OldTable.Schema, Name: tablePair.OldTable.Name}
		newIdent := ast.Ident{Schema: tablePair.NewTable.Schema, Name: tablePair.NewTable.Name}
		oldIdents = append(oldIdents, oldIdent)
		newIdents = append(newIdents, newIdent)
	}

	tablesCache := make(map[string]int64)
	is := infoSchemaAdaptor{inner: d.infoStore}
	for i := range oldIdents {
		_, _, err := extractTblInfos(is, oldIdents[i], newIdents[i], false, tablesCache)
		if err != nil {
			return err
		}
	}

	for i := range oldIdents {
		tableInfo, err := d.TableByName(oldIdents[i].Schema, oldIdents[i].Name)
		if err != nil {
			return err
		}
		if err = d.deleteTable(oldIdents[i].Schema, oldIdents[i].Name); err != nil {
			return err
		}
		tableInfo.Name = newIdents[i].Name
		if err = d.putTable(newIdents[i].Schema, tableInfo); err != nil {
			return err
		}
	}
	return nil
}

func (d SchemaTracker) LockTables(ctx sessionctx.Context, stmt *ast.LockTablesStmt) error {
	return nil
}

func (d SchemaTracker) UnlockTables(ctx sessionctx.Context, lockedTables []model.TableLockTpInfo) error {
	return nil

}

func (d SchemaTracker) CleanupTableLock(ctx sessionctx.Context, tables []*ast.TableName) error {
	return nil

}

func (d SchemaTracker) UpdateTableReplicaInfo(ctx sessionctx.Context, physicalID int64, available bool) error {
	return nil

}

func (d SchemaTracker) RepairTable(ctx sessionctx.Context, table *ast.TableName, createStmt *ast.CreateTableStmt) error {
	return nil

}

func (d SchemaTracker) CreateSequence(ctx sessionctx.Context, stmt *ast.CreateSequenceStmt) error {
	return nil
}

func (d SchemaTracker) DropSequence(ctx sessionctx.Context, stmt *ast.DropSequenceStmt) (err error) {
	return nil

}

func (d SchemaTracker) AlterSequence(ctx sessionctx.Context, stmt *ast.AlterSequenceStmt) error {
	return nil
}

func (d SchemaTracker) CreatePlacementPolicy(ctx sessionctx.Context, stmt *ast.CreatePlacementPolicyStmt) error {
	return nil

}

func (d SchemaTracker) DropPlacementPolicy(ctx sessionctx.Context, stmt *ast.DropPlacementPolicyStmt) error {
	return nil

}

func (d SchemaTracker) AlterPlacementPolicy(ctx sessionctx.Context, stmt *ast.AlterPlacementPolicyStmt) error {
	return nil

}

func (d SchemaTracker) BatchCreateTableWithInfo(ctx sessionctx.Context, schema model.CIStr, info []*model.TableInfo, onExist OnExist) error {
	for _, tableInfo := range info {
		if err := d.CreateTableWithInfo(ctx, schema, tableInfo, onExist); err != nil {
			return err
		}
	}
	return nil
}

func (d SchemaTracker) CreatePlacementPolicyWithInfo(ctx sessionctx.Context, policy *model.PolicyInfo, onExist OnExist) error {
	return nil

}

func (d SchemaTracker) Start(ctxPool *pools.ResourcePool) error {
	return nil

}

func (d SchemaTracker) GetLease() time.Duration {
	return 0
}

func (d SchemaTracker) Stats(vars *variable.SessionVars) (map[string]interface{}, error) {
	return nil, nil
}

func (d SchemaTracker) GetScope(status string) variable.ScopeFlag {
	return 0
}

func (d SchemaTracker) Stop() error {
	return nil
}

func (d SchemaTracker) RegisterStatsHandle(handle *handle.Handle) {}

func (d SchemaTracker) SchemaSyncer() util.SchemaSyncer {
	return nil
}

func (d SchemaTracker) OwnerManager() owner.Manager {
	return nil
}

func (d SchemaTracker) GetID() string {
	return "in-memory-ddl"
}

func (d SchemaTracker) GetTableMaxHandle(ctx *JobContext, startTS uint64, tbl table.PhysicalTable) (kv.Handle, bool, error) {
	return nil, false, nil
}

func (d SchemaTracker) SetBinlogClient(client *pumpcli.PumpsClient) {}

func (d SchemaTracker) GetHook() Callback {
	return nil
}

func (d SchemaTracker) SetHook(h Callback) {}

func (d SchemaTracker) DoDDLJob(ctx sessionctx.Context, job *model.Job) error {
	return nil
}
