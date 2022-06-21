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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package ddl

import (
	"context"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/table"
	pumpcli "github.com/pingcap/tidb/tidb-binlog/pump_client"
	"github.com/pingcap/tidb/util/dbterror"
)

// TODO: thread-safe?
type inMemoryInfoSchema struct {
	lowerCaseTableNames int // same as variable lower_case_table_names

	dbs    map[string]*model.DBInfo
	tables map[string]*model.TableInfo
}

func newInMemoryInfoSchema(lowerCaseTableNames int) *inMemoryInfoSchema {
	return &inMemoryInfoSchema{
		lowerCaseTableNames: lowerCaseTableNames,
		dbs:                 map[string]*model.DBInfo{},
		tables:              map[string]*model.TableInfo{},
	}
}

func (i *inMemoryInfoSchema) ciStr2Key(name model.CIStr) string {
	if i.lowerCaseTableNames == 0 {
		return name.O
	}
	return name.L
}

func (i *inMemoryInfoSchema) SchemaByName(name model.CIStr) (*model.DBInfo, bool) {
	key := i.ciStr2Key(name)
	db, ok := i.dbs[key]
	return db, ok
}

func (i *inMemoryInfoSchema) PutSchema(dbInfo *model.DBInfo) {
	key := i.ciStr2Key(dbInfo.Name)
	i.dbs[key] = dbInfo
}

func (i *inMemoryInfoSchema) DeleteSchema(name model.CIStr) bool {
	key := i.ciStr2Key(name)
	dbInfo, ok := i.dbs[key]
	if !ok {
		return false
	}
	delete(i.dbs, key)
	for _, tblInfo := range dbInfo.Tables {
		delete(i.tables, i.ciStr2Key(tblInfo.Name))
	}
	return true
}

func (i *inMemoryInfoSchema) TableByName(schema, table model.CIStr) (*model.TableInfo, bool) {
	schemaKey := i.ciStr2Key(schema)
	_, ok := i.dbs[schemaKey]
	if !ok {
		// return a meaningful error?
		return nil, false
	}

	tableKey := i.ciStr2Key(table)
	tbl, ok := i.tables[tableKey]
	return tbl, ok
}

func (i *inMemoryInfoSchema) PutTable(schemaName model.CIStr, tblInfo *model.TableInfo) error {
	schemaKey := i.ciStr2Key(schemaName)
	dbInfo, ok := i.dbs[schemaKey]
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schemaName)
	}
	dbInfo.Tables = append(dbInfo.Tables, tblInfo)
	tableKey := i.ciStr2Key(tblInfo.Name)
	i.tables[tableKey] = tblInfo
	return nil
}

func (i *inMemoryInfoSchema) DeleteTable(schema, table model.CIStr) error {
	schemaKey := i.ciStr2Key(schema)
	dbInfo, ok := i.dbs[schemaKey]
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schema)
	}

	tableKey := i.ciStr2Key(table)
	_, ok = i.tables[tableKey]
	if !ok {
		return nil
	}
	delete(i.tables, tableKey)
	for idx, toCheck := range dbInfo.Tables {
		toCheckKey := i.ciStr2Key(toCheck.Name)
		if toCheckKey == tableKey {
			last := dbInfo.Tables[len(dbInfo.Tables)-1]
			dbInfo.Tables[idx] = last
			dbInfo.Tables = dbInfo.Tables[:len(dbInfo.Tables)-1]
			return nil
		}
	}
	return nil
}

var _ DDL = InMemoryDDL{}

type InMemoryDDL struct {
	*inMemoryInfoSchema
}

func NewInMemoryDDL(lowerCaseTableNames int) InMemoryDDL {
	return InMemoryDDL{
		inMemoryInfoSchema: newInMemoryInfoSchema(lowerCaseTableNames),
	}
}

func (i InMemoryDDL) CreateSchema(
	ctx sessionctx.Context,
	schema model.CIStr,
	charsetInfo *ast.CharsetOpt,
	_ *model.PolicyRefInfo,
) error {
	dbInfo := &model.DBInfo{Name: schema}
	if charsetInfo != nil {
		dbInfo.Charset = charsetInfo.Chs
		dbInfo.Collate = charsetInfo.Col
	} else {
		dbInfo.Charset, dbInfo.Collate = charset.GetDefaultCharsetAndCollate()
	}

	return i.CreateSchemaWithInfo(ctx, dbInfo, OnExistError)
}

func (i InMemoryDDL) CreateSchemaWithInfo(ctx sessionctx.Context, dbInfo *model.DBInfo, onExist OnExist) error {
	_, ok := i.SchemaByName(dbInfo.Name)
	if ok {
		err := infoschema.ErrDatabaseExists.GenWithStackByArgs(dbInfo.Name)
		switch onExist {
		case OnExistIgnore:
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		case OnExistError, OnExistReplace:
			// FIXME: can we implement MariaDB's CREATE OR REPLACE SCHEMA?
			return err
		}
	}
	i.PutSchema(dbInfo)
	return nil
}

func (i InMemoryDDL) AlterSchema(ctx sessionctx.Context, stmt *ast.AlterDatabaseStmt) error {
	// TODO: change it to CIStr
	dbName := model.NewCIStr(stmt.Name)
	dbInfo, ok := i.SchemaByName(dbName)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(dbName.O)
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

func (i InMemoryDDL) DropSchema(ctx sessionctx.Context, schema model.CIStr) error {
	ok := i.DeleteSchema(schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists
	}
	return nil
}

func (i InMemoryDDL) CreateTable(ctx sessionctx.Context, s *ast.CreateTableStmt) error {
	ident := ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name}
	schema, ok := i.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}

	var referTbl *model.TableInfo
	if s.ReferTable != nil {
		referIdent := ast.Ident{Schema: s.ReferTable.Schema, Name: s.ReferTable.Name}
		_, ok := i.SchemaByName(referIdent.Schema)
		if !ok {
			return infoschema.ErrTableNotExists.GenWithStackByArgs(referIdent.Schema, referIdent.Name)
		}
		referTbl, ok = i.TableByName(referIdent.Schema, referIdent.Name)
		if !ok {
			return infoschema.ErrTableNotExists.GenWithStackByArgs(referIdent.Schema, referIdent.Name)
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

	return i.CreateTableWithInfo(ctx, schema.Name, tbInfo, onExist)
}

func (i InMemoryDDL) CreateTableWithInfo(
	ctx sessionctx.Context,
	dbName model.CIStr,
	info *model.TableInfo,
	onExist OnExist,
) error {
	_, ok := i.SchemaByName(dbName)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(dbName)
	}

	_, ok = i.TableByName(dbName, info.Name)
	// table exists, but if_not_exists flags is true, so we ignore this error.
	if ok {
		err := infoschema.ErrTableExists.GenWithStackByArgs(ast.Ident{Schema: dbName, Name: info.Name})
		switch onExist {
		case OnExistIgnore:
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		case OnExistReplace:
		default:
			return err
		}
	}
	return i.PutTable(dbName, info)
}

func (i InMemoryDDL) CreateView(ctx sessionctx.Context, s *ast.CreateViewStmt) error {
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

	tblCharset := ""
	tblCollate := ""
	if v, ok := ctx.GetSessionVars().GetSystemVar(variable.CharacterSetConnection); ok {
		tblCharset = v
	}
	if v, ok := ctx.GetSessionVars().GetSystemVar(variable.CollationConnection); ok {
		tblCollate = v
	}

	tbInfo, err := buildTableInfo(ctx, s.ViewName.Name, cols, nil, tblCharset, tblCollate)
	if err != nil {
		return err
	}
	tbInfo.View = viewInfo

	onExist := OnExistError
	if s.OrReplace {
		onExist = OnExistReplace
	}

	return i.CreateTableWithInfo(ctx, s.ViewName.Schema, tbInfo, onExist)
}

func (i InMemoryDDL) DropTable(ctx sessionctx.Context, ti ast.Ident) (err error) {
	_, ok := i.SchemaByName(ti.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ti.Schema)
	}
	tb, ok := i.TableByName(ti.Schema, ti.Name)
	if !ok {
		return infoschema.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name)
	}

	if tb.IsView() {
		return infoschema.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name)
	}
	if tb.IsSequence() {
		return infoschema.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name)
	}
	return i.DeleteTable(ti.Schema, ti.Name)
}

func (i InMemoryDDL) RecoverTable(ctx sessionctx.Context, recoverInfo *RecoverInfo) (err error) {
	return nil
}

func (i InMemoryDDL) DropView(ctx sessionctx.Context, ti ast.Ident) (err error) {
	_, ok := i.SchemaByName(ti.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ti.Schema)
	}
	tb, ok := i.TableByName(ti.Schema, ti.Name)
	if !ok {
		return infoschema.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name)
	}

	if !tb.IsView() {
		return dbterror.ErrWrongObject.GenWithStackByArgs(ti.Schema, ti.Name, "VIEW")
	}
	return i.DeleteTable(ti.Schema, ti.Name)
}

func (i InMemoryDDL) CreateIndex(ctx sessionctx.Context, tableIdent ast.Ident, keyType ast.IndexKeyType, indexName model.CIStr, columnNames []*ast.IndexPartSpecification, indexOption *ast.IndexOption, ifNotExists bool) error {
	//TODO implement me
	panic("implement me")
}

func (i InMemoryDDL) DropIndex(ctx sessionctx.Context, tableIdent ast.Ident, indexName model.CIStr, ifExists bool) error {
	//TODO implement me
	panic("implement me")
}

func (i InMemoryDDL) AlterTable(ctx context.Context, sctx sessionctx.Context, tableIdent ast.Ident, spec []*ast.AlterTableSpec) error {
	//TODO implement me
	panic("implement me")
}

func (i InMemoryDDL) TruncateTable(ctx sessionctx.Context, tableIdent ast.Ident) error {
	//TODO implement me
	panic("implement me")
}

func (i InMemoryDDL) RenameTable(ctx sessionctx.Context, oldTableIdent, newTableIdent ast.Ident, isAlterTable bool) error {
	//TODO implement me
	panic("implement me")
}

func (i InMemoryDDL) RenameTables(ctx sessionctx.Context, oldTableIdent, newTableIdent []ast.Ident, isAlterTable bool) error {
	//TODO implement me
	panic("implement me")
}

func (i InMemoryDDL) LockTables(ctx sessionctx.Context, stmt *ast.LockTablesStmt) error {
	//TODO implement me
	panic("implement me")
}

func (i InMemoryDDL) UnlockTables(ctx sessionctx.Context, lockedTables []model.TableLockTpInfo) error {
	//TODO implement me
	panic("implement me")
}

func (i InMemoryDDL) CleanupTableLock(ctx sessionctx.Context, tables []*ast.TableName) error {
	//TODO implement me
	panic("implement me")
}

func (i InMemoryDDL) UpdateTableReplicaInfo(ctx sessionctx.Context, physicalID int64, available bool) error {
	//TODO implement me
	panic("implement me")
}

func (i InMemoryDDL) RepairTable(ctx sessionctx.Context, table *ast.TableName, createStmt *ast.CreateTableStmt) error {
	//TODO implement me
	panic("implement me")
}

func (i InMemoryDDL) CreateSequence(ctx sessionctx.Context, stmt *ast.CreateSequenceStmt) error {
	//TODO implement me
	panic("implement me")
}

func (i InMemoryDDL) DropSequence(ctx sessionctx.Context, tableIdent ast.Ident, ifExists bool) (err error) {
	//TODO implement me
	panic("implement me")
}

func (i InMemoryDDL) AlterSequence(ctx sessionctx.Context, stmt *ast.AlterSequenceStmt) error {
	//TODO implement me
	panic("implement me")
}

func (i InMemoryDDL) CreatePlacementPolicy(ctx sessionctx.Context, stmt *ast.CreatePlacementPolicyStmt) error {
	//TODO implement me
	panic("implement me")
}

func (i InMemoryDDL) DropPlacementPolicy(ctx sessionctx.Context, stmt *ast.DropPlacementPolicyStmt) error {
	//TODO implement me
	panic("implement me")
}

func (i InMemoryDDL) AlterPlacementPolicy(ctx sessionctx.Context, stmt *ast.AlterPlacementPolicyStmt) error {
	//TODO implement me
	panic("implement me")
}

func (i InMemoryDDL) BatchCreateTableWithInfo(ctx sessionctx.Context, schema model.CIStr, info []*model.TableInfo, onExist OnExist) error {
	//TODO implement me
	panic("implement me")
}

func (i InMemoryDDL) CreatePlacementPolicyWithInfo(ctx sessionctx.Context, policy *model.PolicyInfo, onExist OnExist) error {
	//TODO implement me
	panic("implement me")
}

func (i InMemoryDDL) Start(ctxPool *pools.ResourcePool) error {
	//TODO implement me
	panic("implement me")
}

func (i InMemoryDDL) GetLease() time.Duration {
	//TODO implement me
	panic("implement me")
}

func (i InMemoryDDL) Stats(vars *variable.SessionVars) (map[string]interface{}, error) {
	//TODO implement me
	panic("implement me")
}

func (i InMemoryDDL) GetScope(status string) variable.ScopeFlag {
	//TODO implement me
	panic("implement me")
}

func (i InMemoryDDL) Stop() error {
	//TODO implement me
	panic("implement me")
}

func (i InMemoryDDL) RegisterStatsHandle(handle *handle.Handle) {
	//TODO implement me
	panic("implement me")
}

func (i InMemoryDDL) SchemaSyncer() util.SchemaSyncer {
	//TODO implement me
	panic("implement me")
}

func (i InMemoryDDL) OwnerManager() owner.Manager {
	//TODO implement me
	panic("implement me")
}

func (i InMemoryDDL) GetID() string {
	//TODO implement me
	panic("implement me")
}

func (i InMemoryDDL) GetTableMaxHandle(ctx *JobContext, startTS uint64, tbl table.PhysicalTable) (kv.Handle, bool, error) {
	//TODO implement me
	panic("implement me")
}

func (i InMemoryDDL) SetBinlogClient(client *pumpcli.PumpsClient) {
	//TODO implement me
	panic("implement me")
}

func (i InMemoryDDL) GetHook() Callback {
	//TODO implement me
	panic("implement me")
}

func (i InMemoryDDL) SetHook(h Callback) {
	//TODO implement me
	panic("implement me")
}

func (i InMemoryDDL) DoDDLJob(ctx sessionctx.Context, job *model.Job) error {
	//TODO implement me
	panic("implement me")
}
