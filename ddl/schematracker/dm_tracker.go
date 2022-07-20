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

package schematracker

import (
	"context"
	"strings"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/ddl"
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
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/dbterror"
)

var _ ddl.DDL = SchemaTracker{}

// SchemaTracker is used to track schema changes by DM. It implements DDL interface and by applying DDL, it updates the
// table structure to keep tracked with upstream changes.
type SchemaTracker struct {
	*InfoStore
}

// NewSchemaTracker creates a SchemaTracker. lowerCaseTableNames has the same meaning as MySQL variable lower_case_table_names.
func NewSchemaTracker(lowerCaseTableNames int) SchemaTracker {
	return SchemaTracker{
		InfoStore: NewInfoStore(lowerCaseTableNames),
	}
}

// CreateSchema implements the DDL interface.
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

	chs, coll, err := ddl.ResolveCharsetCollation(charsetOpt)
	if err != nil {
		return errors.Trace(err)
	}

	dbInfo := &model.DBInfo{Name: stmt.Name, Charset: chs, Collate: coll}
	onExist := ddl.OnExistError
	if stmt.IfNotExists {
		onExist = ddl.OnExistIgnore
	}
	return d.CreateSchemaWithInfo(ctx, dbInfo, onExist)
}

func (d SchemaTracker) createTestDB() {
	_ = d.CreateSchema(nil, &ast.CreateDatabaseStmt{
		Name: model.NewCIStr("test"),
	})
}

// CreateSchemaWithInfo implements the DDL interface.
func (d SchemaTracker) CreateSchemaWithInfo(ctx sessionctx.Context, dbInfo *model.DBInfo, onExist ddl.OnExist) error {
	oldInfo := d.SchemaByName(dbInfo.Name)
	if oldInfo != nil {
		if onExist == ddl.OnExistIgnore {
			return nil
		}
		// not support MariaDB's CREATE OR REPLACE SCHEMA
		return infoschema.ErrDatabaseExists.GenWithStackByArgs(dbInfo.Name)
	}
	d.PutSchema(dbInfo)
	return nil
}

// AlterSchema implements the DDL interface.
func (d SchemaTracker) AlterSchema(ctx sessionctx.Context, stmt *ast.AlterDatabaseStmt) error {
	dbInfo := d.SchemaByName(stmt.Name)
	if dbInfo == nil {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(stmt.Name.O)
	}

	// Resolve target charset and collation from options.
	var (
		toCharset, toCollate string
		err                  error
	)

	for _, val := range stmt.Options {
		switch val.Tp {
		case ast.DatabaseOptionCharset:
			if toCharset == "" {
				toCharset = val.Value
			} else if toCharset != val.Value {
				return dbterror.ErrConflictingDeclarations.GenWithStackByArgs(toCharset, val.Value)
			}
		case ast.DatabaseOptionCollate:
			info, errGetCollate := collate.GetCollationByName(val.Value)
			if errGetCollate != nil {
				return errors.Trace(errGetCollate)
			}
			if toCharset == "" {
				toCharset = info.CharsetName
			} else if toCharset != info.CharsetName {
				return dbterror.ErrConflictingDeclarations.GenWithStackByArgs(toCharset, info.CharsetName)
			}
			toCollate = info.Name
		}
	}
	if toCharset == "" {
		if toCollate, err = charset.GetDefaultCollation(toCharset); err != nil {
			return errors.Trace(err)
		}
	}

	dbInfo.Charset = toCharset
	dbInfo.Collate = toCollate

	return nil
}

// DropSchema implements the DDL interface.
func (d SchemaTracker) DropSchema(ctx sessionctx.Context, stmt *ast.DropDatabaseStmt) error {
	ok := d.DeleteSchema(stmt.Name)
	if !ok {
		if stmt.IfExists {
			return nil
		}
		return infoschema.ErrDatabaseDropExists.GenWithStackByArgs(stmt.Name)
	}
	return nil
}

// CreateTable implements the DDL interface.
func (d SchemaTracker) CreateTable(ctx sessionctx.Context, s *ast.CreateTableStmt) error {
	ident := ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name}
	schema := d.SchemaByName(ident.Schema)
	if schema == nil {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}

	// suppress ErrTooLongKey
	ctx.GetSessionVars().StrictSQLMode = false

	var (
		referTbl *model.TableInfo
		err      error
	)
	if s.ReferTable != nil {
		referTbl, err = d.TableByName(s.ReferTable.Schema, s.ReferTable.Name)
		if err != nil {
			return infoschema.ErrTableNotExists.GenWithStackByArgs(s.ReferTable.Schema, s.ReferTable.Name)
		}
	}

	// build tableInfo
	var (
		tbInfo *model.TableInfo
	)
	if s.ReferTable != nil {
		tbInfo, err = ddl.BuildTableInfoWithLike(ctx, ident, referTbl, s)
	} else {
		tbInfo, err = ddl.BuildTableInfoWithStmt(ctx, s, schema.Charset, schema.Collate, nil)
	}
	if err != nil {
		return errors.Trace(err)
	}

	// TODO: to reuse the constant fold of expression in partition range definition we use CheckTableInfoValidWithStmt,
	// but it may also introduce unwanted limit check in DM's use case. Should check it later.
	if err = ddl.CheckTableInfoValidWithStmt(ctx, tbInfo, s); err != nil {
		return err
	}

	onExist := ddl.OnExistError
	if s.IfNotExists {
		onExist = ddl.OnExistIgnore
	}

	return d.CreateTableWithInfo(ctx, schema.Name, tbInfo, onExist)
}

// CreateTableWithInfo implements the DDL interface.
func (d SchemaTracker) CreateTableWithInfo(
	ctx sessionctx.Context,
	dbName model.CIStr,
	info *model.TableInfo,
	onExist ddl.OnExist,
) error {
	schema := d.SchemaByName(dbName)
	if schema == nil {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(dbName)
	}

	oldTable, _ := d.TableByName(dbName, info.Name)
	if oldTable != nil {
		switch onExist {
		case ddl.OnExistIgnore:
			return nil
		case ddl.OnExistReplace:
			return d.PutTable(dbName, info)
		default:
			return infoschema.ErrTableExists.GenWithStackByArgs(ast.Ident{Schema: dbName, Name: info.Name})
		}
	}

	return d.PutTable(dbName, info)
}

// CreateView implements the DDL interface.
func (d SchemaTracker) CreateView(ctx sessionctx.Context, s *ast.CreateViewStmt) error {
	viewInfo, err := ddl.BuildViewInfo(ctx, s)
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

	tbInfo, err := ddl.BuildTableInfo(ctx, s.ViewName.Name, cols, nil, "", "")
	if err != nil {
		return err
	}
	tbInfo.View = viewInfo

	onExist := ddl.OnExistError
	if s.OrReplace {
		onExist = ddl.OnExistReplace
	}

	return d.CreateTableWithInfo(ctx, s.ViewName.Schema, tbInfo, onExist)
}

// DropTable implements the DDL interface.
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
			// For statement dropping multiple tables, we should return error after try to drop all tables.
			continue
		}

		// Without IF EXISTS, the statement drops all named tables that do exist, and returns an error indicating which
		// nonexisting tables it was unable to drop.
		// https://dev.mysql.com/doc/refman/5.7/en/drop-table.html
		_ = d.DeleteTable(name.Schema, name.Name)
	}

	if len(notExistTables) > 0 {
		return infoschema.ErrTableDropExists.GenWithStackByArgs(strings.Join(notExistTables, ","))
	}
	return nil
}

// RecoverTable implements the DDL interface, which is no-op in DM's case.
func (d SchemaTracker) RecoverTable(ctx sessionctx.Context, recoverInfo *ddl.RecoverInfo) (err error) {
	return nil
}

// DropView implements the DDL interface.
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

		// the behaviour is fast fail when type is wrong.
		if !tb.IsView() {
			return dbterror.ErrWrongObject.GenWithStackByArgs(name.Schema, name.Name, "VIEW")
		}

		_ = d.DeleteTable(name.Schema, name.Name)
	}

	if len(notExistTables) > 0 {
		return infoschema.ErrTableDropExists.GenWithStackByArgs(strings.Join(notExistTables, ","))
	}
	return nil
}

// CreateIndex implements the DDL interface.
func (d SchemaTracker) CreateIndex(ctx sessionctx.Context, stmt *ast.CreateIndexStmt) error {
	panic("not implemented")
}

// DropIndex implements the DDL interface.
func (d SchemaTracker) DropIndex(ctx sessionctx.Context, stmt *ast.DropIndexStmt) error {
	panic("not implemented")
}

// AlterTable implements the DDL interface.
func (d SchemaTracker) AlterTable(ctx context.Context, sctx sessionctx.Context, stmt *ast.AlterTableStmt) error {
	panic("not implemented")
}

// TruncateTable implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) TruncateTable(ctx sessionctx.Context, tableIdent ast.Ident) error {
	return nil
}

// RenameTable implements the DDL interface.
func (d SchemaTracker) RenameTable(ctx sessionctx.Context, stmt *ast.RenameTableStmt) error {
	oldIdents := make([]ast.Ident, 0, len(stmt.TableToTables))
	newIdents := make([]ast.Ident, 0, len(stmt.TableToTables))
	for _, tablePair := range stmt.TableToTables {
		oldIdent := ast.Ident{Schema: tablePair.OldTable.Schema, Name: tablePair.OldTable.Name}
		newIdent := ast.Ident{Schema: tablePair.NewTable.Schema, Name: tablePair.NewTable.Name}
		oldIdents = append(oldIdents, oldIdent)
		newIdents = append(newIdents, newIdent)
	}
	return d.renameTable(ctx, oldIdents, newIdents, false)
}

// renameTable is used by RenameTable and AlterTable.
func (d SchemaTracker) renameTable(ctx sessionctx.Context, oldIdents, newIdents []ast.Ident, isAlterTable bool) error {
	tablesCache := make(map[string]int64)
	is := InfoStoreAdaptor{inner: d.InfoStore}
	for i := range oldIdents {
		_, _, err := ddl.ExtractTblInfos(is, oldIdents[i], newIdents[i], isAlterTable, tablesCache)
		if err != nil {
			return err
		}
	}

	for i := range oldIdents {
		tableInfo, err := d.TableByName(oldIdents[i].Schema, oldIdents[i].Name)
		if err != nil {
			return err
		}
		if err = d.DeleteTable(oldIdents[i].Schema, oldIdents[i].Name); err != nil {
			return err
		}
		tableInfo.Name = newIdents[i].Name
		if err = d.PutTable(newIdents[i].Schema, tableInfo); err != nil {
			return err
		}
	}
	return nil
}

// LockTables implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) LockTables(ctx sessionctx.Context, stmt *ast.LockTablesStmt) error {
	return nil
}

// UnlockTables implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) UnlockTables(ctx sessionctx.Context, lockedTables []model.TableLockTpInfo) error {
	return nil

}

// CleanupTableLock implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) CleanupTableLock(ctx sessionctx.Context, tables []*ast.TableName) error {
	return nil

}

// UpdateTableReplicaInfo implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) UpdateTableReplicaInfo(ctx sessionctx.Context, physicalID int64, available bool) error {
	return nil

}

// RepairTable implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) RepairTable(ctx sessionctx.Context, table *ast.TableName, createStmt *ast.CreateTableStmt) error {
	return nil

}

// CreateSequence implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) CreateSequence(ctx sessionctx.Context, stmt *ast.CreateSequenceStmt) error {
	return nil
}

// DropSequence implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) DropSequence(ctx sessionctx.Context, stmt *ast.DropSequenceStmt) (err error) {
	return nil

}

// AlterSequence implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) AlterSequence(ctx sessionctx.Context, stmt *ast.AlterSequenceStmt) error {
	return nil
}

// CreatePlacementPolicy implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) CreatePlacementPolicy(ctx sessionctx.Context, stmt *ast.CreatePlacementPolicyStmt) error {
	return nil

}

// DropPlacementPolicy implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) DropPlacementPolicy(ctx sessionctx.Context, stmt *ast.DropPlacementPolicyStmt) error {
	return nil

}

// AlterPlacementPolicy implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) AlterPlacementPolicy(ctx sessionctx.Context, stmt *ast.AlterPlacementPolicyStmt) error {
	return nil

}

// BatchCreateTableWithInfo implements the DDL interface, it will call CreateTableWithInfo for each table.
func (d SchemaTracker) BatchCreateTableWithInfo(ctx sessionctx.Context, schema model.CIStr, info []*model.TableInfo, onExist ddl.OnExist) error {
	panic("not implemented")
}

// CreatePlacementPolicyWithInfo implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) CreatePlacementPolicyWithInfo(ctx sessionctx.Context, policy *model.PolicyInfo, onExist ddl.OnExist) error {
	return nil
}

// Start implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) Start(ctxPool *pools.ResourcePool) error {
	return nil

}

// GetLease implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) GetLease() time.Duration {
	return 0
}

// Stats implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) Stats(vars *variable.SessionVars) (map[string]interface{}, error) {
	return nil, nil
}

// GetScope implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) GetScope(status string) variable.ScopeFlag {
	return 0
}

// Stop implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) Stop() error {
	return nil
}

// RegisterStatsHandle implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) RegisterStatsHandle(handle *handle.Handle) {}

// SchemaSyncer implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) SchemaSyncer() util.SchemaSyncer {
	return nil
}

// OwnerManager implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) OwnerManager() owner.Manager {
	return nil
}

// GetID implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) GetID() string {
	return "schema-tracker"
}

// GetTableMaxHandle implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) GetTableMaxHandle(ctx *ddl.JobContext, startTS uint64, tbl table.PhysicalTable) (kv.Handle, bool, error) {
	return nil, false, nil
}

// SetBinlogClient implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) SetBinlogClient(client *pumpcli.PumpsClient) {}

// GetHook implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) GetHook() ddl.Callback {
	return nil
}

// SetHook implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) SetHook(h ddl.Callback) {}

// GetInfoSchemaWithInterceptor implements the DDL interface.
func (d SchemaTracker) GetInfoSchemaWithInterceptor(ctx sessionctx.Context) infoschema.InfoSchema {
	panic("not implemented")
}

// DoDDLJob implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) DoDDLJob(ctx sessionctx.Context, job *model.Job) error {
	return nil
}
