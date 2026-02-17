// Copyright 2016 PingCAP, Inc.
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
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"go.uber.org/zap"
)

// If one drop those tables by mistake, it's difficult to recover.
// In the worst case, the whole TiDB cluster fails to bootstrap, so we prevent user from dropping them.
var systemTables = map[string]struct{}{
	"tidb":                 {},
	"gc_delete_range":      {},
	"gc_delete_range_done": {},
}

func isUndroppableTable(schema, table string, tableInfo *model.TableInfo) bool {
	if isReservedSchemaObjInNextGen(tableInfo.ID) {
		return true
	}
	if schema == mysql.WorkloadSchema {
		return true
	}
	if schema != mysql.SystemDB {
		return false
	}
	if _, ok := systemTables[table]; ok {
		return true
	}
	return false
}

type objectType int

const (
	tableObject objectType = iota
	viewObject
	sequenceObject
)

// dropTableObject provides common logic to DROP TABLE/VIEW/SEQUENCE.
func (e *executor) dropTableObject(
	ctx sessionctx.Context,
	objects []*ast.TableName,
	ifExists bool,
	tableObjectType objectType,
) error {
	var (
		notExistTables []string
		sessVars       = ctx.GetSessionVars()
		is             = e.infoCache.GetLatest()
		dropExistErr   *terror.Error
		jobType        model.ActionType
	)

	var (
		objectIdents []ast.Ident
		fkCheck      bool
	)
	switch tableObjectType {
	case tableObject:
		dropExistErr = infoschema.ErrTableDropExists
		jobType = model.ActionDropTable
		objectIdents = make([]ast.Ident, len(objects))
		fkCheck = ctx.GetSessionVars().ForeignKeyChecks
		for i, tn := range objects {
			objectIdents[i] = ast.Ident{Schema: tn.Schema, Name: tn.Name}
		}
		for _, tn := range objects {
			if referredFK := checkTableHasForeignKeyReferred(is, tn.Schema.L, tn.Name.L, objectIdents, fkCheck); referredFK != nil {
				return errors.Trace(dbterror.ErrForeignKeyCannotDropParent.GenWithStackByArgs(tn.Name, referredFK.ChildFKName, referredFK.ChildTable))
			}
		}
	case viewObject:
		dropExistErr = infoschema.ErrTableDropExists
		jobType = model.ActionDropView
	case sequenceObject:
		dropExistErr = infoschema.ErrSequenceDropExists
		jobType = model.ActionDropSequence
	}
	for _, tn := range objects {
		fullti := ast.Ident{Schema: tn.Schema, Name: tn.Name}
		schema, ok := is.SchemaByName(tn.Schema)
		if !ok {
			// TODO: we should return special error for table not exist, checking "not exist" is not enough,
			// because some other errors may contain this error string too.
			notExistTables = append(notExistTables, fullti.String())
			continue
		}
		tableInfo, err := is.TableByName(e.ctx, tn.Schema, tn.Name)
		if err != nil && infoschema.ErrTableNotExists.Equal(err) {
			notExistTables = append(notExistTables, fullti.String())
			continue
		} else if err != nil {
			return err
		}
		if err = dbutil.CheckTableModeIsNormal(tableInfo.Meta().Name, tableInfo.Meta().Mode); err != nil {
			return err
		}

		// prechecks before build DDL job

		// Protect important system table from been dropped by a mistake.
		// I can hardly find a case that a user really need to do this.
		if isUndroppableTable(tn.Schema.L, tn.Name.L, tableInfo.Meta()) {
			return dbterror.ErrForbiddenDDL.FastGenByArgs(fmt.Sprintf("Drop tidb system table '%s.%s'", tn.Schema.L, tn.Name.L))
		}
		switch tableObjectType {
		case tableObject:
			if !tableInfo.Meta().IsBaseTable() {
				notExistTables = append(notExistTables, fullti.String())
				continue
			}

			tempTableType := tableInfo.Meta().TempTableType
			if config.CheckTableBeforeDrop && tempTableType == model.TempTableNone {
				err := adminCheckTableBeforeDrop(ctx, fullti)
				if err != nil {
					return err
				}
			}

			if tableInfo.Meta().TableCacheStatusType != model.TableCacheStatusDisable {
				return dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Drop Table")
			}
		case viewObject:
			if !tableInfo.Meta().IsView() {
				return dbterror.ErrWrongObject.GenWithStackByArgs(fullti.Schema, fullti.Name, "VIEW")
			}
		case sequenceObject:
			if !tableInfo.Meta().IsSequence() {
				err = dbterror.ErrWrongObject.GenWithStackByArgs(fullti.Schema, fullti.Name, "SEQUENCE")
				if ifExists {
					ctx.GetSessionVars().StmtCtx.AppendNote(err)
					continue
				}
				return err
			}
		}

		job := &model.Job{
			Version:        model.GetJobVerInUse(),
			SchemaID:       schema.ID,
			TableID:        tableInfo.Meta().ID,
			SchemaName:     schema.Name.L,
			SchemaState:    schema.State,
			TableName:      tableInfo.Meta().Name.L,
			Type:           jobType,
			BinlogInfo:     &model.HistoryInfo{},
			CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
			SQLMode:        ctx.GetSessionVars().SQLMode,
		}
		args := &model.DropTableArgs{
			Identifiers: objectIdents,
			FKCheck:     fkCheck,
		}

		err = e.doDDLJob2(ctx, job, args)
		if infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableNotExists.Equal(err) {
			notExistTables = append(notExistTables, fullti.String())
			continue
		} else if err != nil {
			return errors.Trace(err)
		}

		// unlock table after drop
		if tableObjectType != tableObject {
			continue
		}
		if !config.TableLockEnabled() {
			continue
		}
		if ok, _ := ctx.CheckTableLocked(tableInfo.Meta().ID); ok {
			ctx.ReleaseTableLockByTableIDs([]int64{tableInfo.Meta().ID})
		}
	}
	if len(notExistTables) > 0 && !ifExists {
		return dropExistErr.FastGenByArgs(strings.Join(notExistTables, ","))
	}
	// We need add warning when use if exists.
	if len(notExistTables) > 0 && ifExists {
		for _, table := range notExistTables {
			sessVars.StmtCtx.AppendNote(dropExistErr.FastGenByArgs(table))
		}
	}
	return nil
}

// adminCheckTableBeforeDrop runs `admin check table` for the table to be dropped.
// Actually this function doesn't do anything specific for `DROP TABLE`, but to avoid
// using it in other places by mistake, it's named like this.
func adminCheckTableBeforeDrop(ctx sessionctx.Context, fullti ast.Ident) error {
	logutil.DDLLogger().Warn("admin check table before drop",
		zap.String("database", fullti.Schema.O),
		zap.String("table", fullti.Name.O),
	)
	exec := ctx.GetRestrictedSQLExecutor()
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)

	// `tidb_enable_fast_table_check` is already the default value, and some feature (e.g. partial index)
	// doesn't support admin check with `tidb_enable_fast_table_check = OFF`, so we just set it to `ON` here.
	// TODO: set the value of `tidb_enable_fast_table_check` to 'ON' for all internal sessions if it's OK.
	originalFastTableCheck := ctx.GetSessionVars().FastCheckTable
	_, _, err := exec.ExecRestrictedSQL(internalCtx, nil, "set tidb_enable_fast_table_check = 'ON';")
	if err != nil {
		return err
	}
	if !originalFastTableCheck {
		defer func() {
			_, _, err = exec.ExecRestrictedSQL(internalCtx, nil, "set tidb_enable_fast_table_check = 'OFF';")
			if err != nil {
				logutil.DDLLogger().Warn("set tidb_enable_fast_table_check = 'OFF' failed", zap.Error(err))
			}
		}()
	}
	_, _, err = exec.ExecRestrictedSQL(internalCtx, nil, "admin check table %n.%n", fullti.Schema.O, fullti.Name.O)
	if err != nil {
		return err
	}
	return nil
}

// DropTable will proceed even if some table in the list does not exists.
func (e *executor) DropTable(ctx sessionctx.Context, stmt *ast.DropTableStmt) (err error) {
	return e.dropTableObject(ctx, stmt.Tables, stmt.IfExists, tableObject)
}

// DropView will proceed even if some view in the list does not exists.
func (e *executor) DropView(ctx sessionctx.Context, stmt *ast.DropTableStmt) (err error) {
	return e.dropTableObject(ctx, stmt.Tables, stmt.IfExists, viewObject)
}

func (e *executor) TruncateTable(ctx sessionctx.Context, ti ast.Ident) error {
	schema, tb, err := e.getSchemaAndTableByIdent(ti)
	if err != nil {
		return errors.Trace(err)
	}
	tblInfo := tb.Meta()
	if tblInfo.IsView() || tblInfo.IsSequence() {
		return infoschema.ErrTableNotExists.GenWithStackByArgs(schema.Name.O, tblInfo.Name.O)
	}
	if tblInfo.TableCacheStatusType != model.TableCacheStatusDisable {
		return dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Truncate Table")
	}
	if isReservedSchemaObjInNextGen(tblInfo.ID) {
		return dbterror.ErrForbiddenDDL.FastGenByArgs(fmt.Sprintf("Truncate system table '%s.%s'", schema.Name.L, tblInfo.Name.L))
	}
	fkCheck := ctx.GetSessionVars().ForeignKeyChecks
	referredFK := checkTableHasForeignKeyReferred(e.infoCache.GetLatest(), ti.Schema.L, ti.Name.L, []ast.Ident{{Name: ti.Name, Schema: ti.Schema}}, fkCheck)
	if referredFK != nil {
		msg := fmt.Sprintf("`%s`.`%s` CONSTRAINT `%s`", referredFK.ChildSchema, referredFK.ChildTable, referredFK.ChildFKName)
		return errors.Trace(dbterror.ErrTruncateIllegalForeignKey.GenWithStackByArgs(msg))
	}

	var oldPartitionIDs []int64
	if tblInfo.Partition != nil {
		oldPartitionIDs = make([]int64, 0, len(tblInfo.Partition.Definitions))
		for _, def := range tblInfo.Partition.Definitions {
			oldPartitionIDs = append(oldPartitionIDs, def.ID)
		}
	}
	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        tblInfo.ID,
		SchemaName:     schema.Name.L,
		TableName:      tblInfo.Name.L,
		Type:           model.ActionTruncateTable,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
		SessionVars:    make(map[string]string),
	}
	job.AddSystemVars(vardef.TiDBScatterRegion, getScatterScopeFromSessionctx(ctx))
	args := &model.TruncateTableArgs{
		FKCheck:         fkCheck,
		OldPartitionIDs: oldPartitionIDs,
	}
	err = e.doDDLJob2(ctx, job, args)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (e *executor) RenameTable(ctx sessionctx.Context, s *ast.RenameTableStmt) error {
	isAlterTable := false
	var err error
	if len(s.TableToTables) == 1 {
		oldIdent := ast.Ident{Schema: s.TableToTables[0].OldTable.Schema, Name: s.TableToTables[0].OldTable.Name}
		newIdent := ast.Ident{Schema: s.TableToTables[0].NewTable.Schema, Name: s.TableToTables[0].NewTable.Name}
		err = e.renameTable(ctx, oldIdent, newIdent, isAlterTable)
	} else {
		oldIdents := make([]ast.Ident, 0, len(s.TableToTables))
		newIdents := make([]ast.Ident, 0, len(s.TableToTables))
		for _, tables := range s.TableToTables {
			oldIdent := ast.Ident{Schema: tables.OldTable.Schema, Name: tables.OldTable.Name}
			newIdent := ast.Ident{Schema: tables.NewTable.Schema, Name: tables.NewTable.Name}
			oldIdents = append(oldIdents, oldIdent)
			newIdents = append(newIdents, newIdent)
		}
		err = e.renameTables(ctx, oldIdents, newIdents, isAlterTable)
	}
	return err
}

func (e *executor) renameTable(ctx sessionctx.Context, oldIdent, newIdent ast.Ident, isAlterTable bool) error {
	is := e.infoCache.GetLatest()
	tables := make(map[string]int64)
	schemas, tableID, err := ExtractTblInfos(is, oldIdent, newIdent, isAlterTable, tables)
	if err != nil {
		return err
	}

	if schemas == nil {
		return nil
	}

	if tbl, ok := is.TableByID(e.ctx, tableID); ok {
		if tbl.Meta().TableCacheStatusType != model.TableCacheStatusDisable {
			return errors.Trace(dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Rename Table"))
		}
		if err = dbutil.CheckTableModeIsNormal(tbl.Meta().Name, tbl.Meta().Mode); err != nil {
			return err
		}
		if isReservedSchemaObjInNextGen(tbl.Meta().ID) {
			return dbterror.ErrForbiddenDDL.FastGenByArgs(fmt.Sprintf("Rename system table '%s.%s'", schemas[0].Name.L, oldIdent.Name.L))
		}
	}

	job := &model.Job{
		SchemaID:       schemas[1].ID,
		TableID:        tableID,
		SchemaName:     schemas[1].Name.L,
		TableName:      oldIdent.Name.L,
		Type:           model.ActionRenameTable,
		Version:        model.GetJobVerInUse(),
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{
			{Database: schemas[0].Name.L, Table: oldIdent.Name.L},
			{Database: schemas[1].Name.L, Table: newIdent.Name.L},
		},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}

	args := &model.RenameTableArgs{
		OldSchemaID:   schemas[0].ID,
		OldSchemaName: schemas[0].Name,
		NewTableName:  newIdent.Name,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func (e *executor) renameTables(ctx sessionctx.Context, oldIdents, newIdents []ast.Ident, isAlterTable bool) error {
	is := e.infoCache.GetLatest()
	involveSchemaInfo := make([]model.InvolvingSchemaInfo, 0, len(oldIdents)*2)

	var schemas []*model.DBInfo
	var tableID int64
	var err error

	tables := make(map[string]int64)
	infos := make([]*model.RenameTableArgs, 0, len(oldIdents))
	for i := range oldIdents {
		schemas, tableID, err = ExtractTblInfos(is, oldIdents[i], newIdents[i], isAlterTable, tables)
		if err != nil {
			return err
		}

		if t, ok := is.TableByID(e.ctx, tableID); ok {
			if t.Meta().TableCacheStatusType != model.TableCacheStatusDisable {
				return errors.Trace(dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Rename Tables"))
			}
			if err = dbutil.CheckTableModeIsNormal(t.Meta().Name, t.Meta().Mode); err != nil {
				return err
			}
			if isReservedSchemaObjInNextGen(t.Meta().ID) {
				return dbterror.ErrForbiddenDDL.FastGenByArgs(fmt.Sprintf("Rename system table '%s.%s'", schemas[0].Name.L, oldIdents[i].Name.L))
			}
		}

		infos = append(infos, &model.RenameTableArgs{
			OldSchemaID:   schemas[0].ID,
			OldSchemaName: schemas[0].Name,
			OldTableName:  oldIdents[i].Name,
			NewSchemaID:   schemas[1].ID,
			NewTableName:  newIdents[i].Name,
			TableID:       tableID,
		})

		involveSchemaInfo = append(involveSchemaInfo,
			model.InvolvingSchemaInfo{
				Database: schemas[0].Name.L, Table: oldIdents[i].Name.L,
			},
			model.InvolvingSchemaInfo{
				Database: schemas[1].Name.L, Table: newIdents[i].Name.L,
			},
		)
	}

	job := &model.Job{
		Version:             model.GetJobVerInUse(),
		SchemaID:            schemas[1].ID,
		TableID:             infos[0].TableID,
		SchemaName:          schemas[1].Name.L,
		Type:                model.ActionRenameTables,
		BinlogInfo:          &model.HistoryInfo{},
		CDCWriteSource:      ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: involveSchemaInfo,
		SQLMode:             ctx.GetSessionVars().SQLMode,
	}

	args := &model.RenameTablesArgs{RenameTableInfos: infos}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

// ExtractTblInfos extracts the table information from the infoschema.
func ExtractTblInfos(is infoschema.InfoSchema, oldIdent, newIdent ast.Ident, isAlterTable bool, tables map[string]int64) ([]*model.DBInfo, int64, error) {
	oldSchema, ok := is.SchemaByName(oldIdent.Schema)
	if !ok {
		if isAlterTable {
			return nil, 0, infoschema.ErrTableNotExists.GenWithStackByArgs(oldIdent.Schema, oldIdent.Name)
		}
		if tableExists(is, newIdent, tables) {
			return nil, 0, infoschema.ErrTableExists.GenWithStackByArgs(newIdent)
		}
		return nil, 0, infoschema.ErrTableNotExists.GenWithStackByArgs(oldIdent.Schema, oldIdent.Name)
	}
	if !tableExists(is, oldIdent, tables) {
		if isAlterTable {
			return nil, 0, infoschema.ErrTableNotExists.GenWithStackByArgs(oldIdent.Schema, oldIdent.Name)
		}
		if tableExists(is, newIdent, tables) {
			return nil, 0, infoschema.ErrTableExists.GenWithStackByArgs(newIdent)
		}
		return nil, 0, infoschema.ErrTableNotExists.GenWithStackByArgs(oldIdent.Schema, oldIdent.Name)
	}
	if isAlterTable && newIdent.Schema.L == oldIdent.Schema.L && newIdent.Name.L == oldIdent.Name.L {
		// oldIdent is equal to newIdent, do nothing
		return nil, 0, nil
	}
	//View can be renamed only in the same schema. Compatible with mysql
	if infoschema.TableIsView(is, oldIdent.Schema, oldIdent.Name) {
		if oldIdent.Schema != newIdent.Schema {
			return nil, 0, infoschema.ErrForbidSchemaChange.GenWithStackByArgs(oldIdent.Schema, newIdent.Schema)
		}
	}

	newSchema, ok := is.SchemaByName(newIdent.Schema)
	if !ok {
		return nil, 0, dbterror.ErrErrorOnRename.GenWithStackByArgs(
			fmt.Sprintf("%s.%s", oldIdent.Schema, oldIdent.Name),
			fmt.Sprintf("%s.%s", newIdent.Schema, newIdent.Name),
			168,
			fmt.Sprintf("Database `%s` doesn't exist", newIdent.Schema))
	}
	if tableExists(is, newIdent, tables) {
		return nil, 0, infoschema.ErrTableExists.GenWithStackByArgs(newIdent)
	}
	if err := checkTooLongTable(newIdent.Name); err != nil {
		return nil, 0, errors.Trace(err)
	}
	oldTableID := getTableID(is, oldIdent, tables)
	oldIdentKey := getIdentKey(oldIdent)
	tables[oldIdentKey] = tableNotExist
	newIdentKey := getIdentKey(newIdent)
	tables[newIdentKey] = oldTableID
	return []*model.DBInfo{oldSchema, newSchema}, oldTableID, nil
}

func tableExists(is infoschema.InfoSchema, ident ast.Ident, tables map[string]int64) bool {
	identKey := getIdentKey(ident)
	tableID, ok := tables[identKey]
	if (ok && tableID != tableNotExist) || (!ok && is.TableExists(ident.Schema, ident.Name)) {
		return true
	}
	return false
}

func getTableID(is infoschema.InfoSchema, ident ast.Ident, tables map[string]int64) int64 {
	identKey := getIdentKey(ident)
	tableID, ok := tables[identKey]
	if !ok {
		oldTbl, err := is.TableByName(context.Background(), ident.Schema, ident.Name)
		if err != nil {
			return tableNotExist
		}
		tableID = oldTbl.Meta().ID
	}
	return tableID
}

func getIdentKey(ident ast.Ident) string {
	return fmt.Sprintf("%s.%s", ident.Schema.L, ident.Name.L)
}
