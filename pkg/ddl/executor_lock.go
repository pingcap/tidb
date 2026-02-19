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

package ddl

import (
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
)

// LockTables uses to execute lock tables statement.
func (e *executor) LockTables(ctx sessionctx.Context, stmt *ast.LockTablesStmt) error {
	lockTables := make([]model.TableLockTpInfo, 0, len(stmt.TableLocks))
	sessionInfo := model.SessionInfo{
		ServerID:  e.uuid,
		SessionID: ctx.GetSessionVars().ConnectionID,
	}
	uniqueTableID := make(map[int64]struct{})
	involveSchemaInfo := make([]model.InvolvingSchemaInfo, 0, len(stmt.TableLocks))
	// Check whether the table was already locked by another.
	for _, tl := range stmt.TableLocks {
		tb := tl.Table
		err := throwErrIfInMemOrSysDB(ctx, tb.Schema.L)
		if err != nil {
			return err
		}
		schema, t, err := e.getSchemaAndTableByIdent(ast.Ident{Schema: tb.Schema, Name: tb.Name})
		if err != nil {
			return errors.Trace(err)
		}
		if t.Meta().IsView() || t.Meta().IsSequence() {
			return table.ErrUnsupportedOp.GenWithStackByArgs()
		}

		err = checkTableLocked(t.Meta(), tl.Type, sessionInfo)
		if err != nil {
			return err
		}
		if _, ok := uniqueTableID[t.Meta().ID]; ok {
			return infoschema.ErrNonuniqTable.GenWithStackByArgs(t.Meta().Name)
		}
		uniqueTableID[t.Meta().ID] = struct{}{}
		lockTables = append(lockTables, model.TableLockTpInfo{SchemaID: schema.ID, TableID: t.Meta().ID, Tp: tl.Type})
		involveSchemaInfo = append(involveSchemaInfo, model.InvolvingSchemaInfo{
			Database: schema.Name.L,
			Table:    t.Meta().Name.L,
		})
	}

	unlockTables := ctx.GetAllTableLocks()
	args := &model.LockTablesArgs{
		LockTables:   lockTables,
		UnlockTables: unlockTables,
		SessionInfo:  sessionInfo,
	}
	job := &model.Job{
		Version:             model.GetJobVerInUse(),
		SchemaID:            lockTables[0].SchemaID,
		TableID:             lockTables[0].TableID,
		Type:                model.ActionLockTable,
		BinlogInfo:          &model.HistoryInfo{},
		CDCWriteSource:      ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: involveSchemaInfo,
		SQLMode:             ctx.GetSessionVars().SQLMode,
	}
	// AddTableLock here is avoiding this job was executed successfully but the session was killed before return.
	ctx.AddTableLock(lockTables)
	err := e.doDDLJob2(ctx, job, args)
	if err == nil {
		ctx.ReleaseTableLocks(unlockTables)
		ctx.AddTableLock(lockTables)
	}
	return errors.Trace(err)
}

// UnlockTables uses to execute unlock tables statement.
func (e *executor) UnlockTables(ctx sessionctx.Context, unlockTables []model.TableLockTpInfo) error {
	if len(unlockTables) == 0 {
		return nil
	}
	args := &model.LockTablesArgs{
		UnlockTables: unlockTables,
		SessionInfo: model.SessionInfo{
			ServerID:  e.uuid,
			SessionID: ctx.GetSessionVars().ConnectionID,
		},
	}

	involveSchemaInfo := make([]model.InvolvingSchemaInfo, 0, len(unlockTables))
	is := e.infoCache.GetLatest()
	for _, t := range unlockTables {
		schema, ok := is.SchemaByID(t.SchemaID)
		if !ok {
			continue
		}
		tbl, ok := is.TableByID(e.ctx, t.TableID)
		if !ok {
			continue
		}
		involveSchemaInfo = append(involveSchemaInfo, model.InvolvingSchemaInfo{
			Database: schema.Name.L,
			Table:    tbl.Meta().Name.L,
		})
	}
	job := &model.Job{
		Version:             model.GetJobVerInUse(),
		SchemaID:            unlockTables[0].SchemaID,
		TableID:             unlockTables[0].TableID,
		Type:                model.ActionUnlockTable,
		BinlogInfo:          &model.HistoryInfo{},
		CDCWriteSource:      ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: involveSchemaInfo,
		SQLMode:             ctx.GetSessionVars().SQLMode,
	}

	err := e.doDDLJob2(ctx, job, args)
	if err == nil {
		ctx.ReleaseAllTableLocks()
	}
	return errors.Trace(err)
}

func (e *executor) AlterTableMode(sctx sessionctx.Context, args *model.AlterTableModeArgs) error {
	is := e.infoCache.GetLatest()

	schema, ok := is.SchemaByID(args.SchemaID)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(fmt.Sprintf("SchemaID: %v", args.SchemaID))
	}

	table, ok := is.TableByID(e.ctx, args.TableID)
	if !ok {
		return infoschema.ErrTableNotExists.GenWithStackByArgs(
			schema.Name, fmt.Sprintf("TableID: %d", args.TableID))
	}

	ok = validateTableMode(table.Meta().Mode, args.TableMode)
	if !ok {
		return infoschema.ErrInvalidTableModeSet.GenWithStackByArgs(table.Meta().Mode, args.TableMode, table.Meta().Name.O)
	}
	if table.Meta().Mode == args.TableMode {
		return nil
	}

	job := &model.Job{
		Version:        model.JobVersion2,
		SchemaID:       args.SchemaID,
		TableID:        args.TableID,
		SchemaName:     schema.Name.O,
		Type:           model.ActionAlterTableMode,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: sctx.GetSessionVars().CDCWriteSource,
		SQLMode:        sctx.GetSessionVars().SQLMode,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{
			{
				Database: schema.Name.L,
				Table:    table.Meta().Name.L,
			},
		},
	}
	sctx.SetValue(sessionctx.QueryString, "skip")
	err := e.doDDLJob2(sctx, job, args)
	return errors.Trace(err)
}

func throwErrIfInMemOrSysDB(ctx sessionctx.Context, dbLowerName string) error {
	if metadef.IsMemOrSysDB(dbLowerName) {
		if ctx.GetSessionVars().User != nil {
			return infoschema.ErrAccessDenied.GenWithStackByArgs(ctx.GetSessionVars().User.Username, ctx.GetSessionVars().User.Hostname)
		}
		return infoschema.ErrAccessDenied.GenWithStackByArgs("", "")
	}
	return nil
}

func (e *executor) CleanupTableLock(ctx sessionctx.Context, tables []*ast.TableName) error {
	uniqueTableID := make(map[int64]struct{})
	cleanupTables := make([]model.TableLockTpInfo, 0, len(tables))
	unlockedTablesNum := 0
	involvingSchemaInfo := make([]model.InvolvingSchemaInfo, 0, len(tables))
	// Check whether the table was already locked by another.
	for _, tb := range tables {
		err := throwErrIfInMemOrSysDB(ctx, tb.Schema.L)
		if err != nil {
			return err
		}
		schema, t, err := e.getSchemaAndTableByIdent(ast.Ident{Schema: tb.Schema, Name: tb.Name})
		if err != nil {
			return errors.Trace(err)
		}
		if t.Meta().IsView() || t.Meta().IsSequence() {
			return table.ErrUnsupportedOp
		}
		// Maybe the table t was not locked, but still try to unlock this table.
		// If we skip unlock the table here, the job maybe not consistent with the job.Query.
		// eg: unlock tables t1,t2;  If t2 is not locked and skip here, then the job will only unlock table t1,
		// and this behaviour is not consistent with the sql query.
		if !t.Meta().IsLocked() {
			unlockedTablesNum++
		}
		if _, ok := uniqueTableID[t.Meta().ID]; ok {
			return infoschema.ErrNonuniqTable.GenWithStackByArgs(t.Meta().Name)
		}
		uniqueTableID[t.Meta().ID] = struct{}{}
		cleanupTables = append(cleanupTables, model.TableLockTpInfo{SchemaID: schema.ID, TableID: t.Meta().ID})
		involvingSchemaInfo = append(involvingSchemaInfo, model.InvolvingSchemaInfo{
			Database: schema.Name.L,
			Table:    t.Meta().Name.L,
		})
	}
	// If the num of cleanupTables is 0, or all cleanupTables is unlocked, just return here.
	if len(cleanupTables) == 0 || len(cleanupTables) == unlockedTablesNum {
		return nil
	}

	args := &model.LockTablesArgs{
		UnlockTables: cleanupTables,
		IsCleanup:    true,
	}
	job := &model.Job{
		Version:             model.GetJobVerInUse(),
		SchemaID:            cleanupTables[0].SchemaID,
		TableID:             cleanupTables[0].TableID,
		Type:                model.ActionUnlockTable,
		BinlogInfo:          &model.HistoryInfo{},
		CDCWriteSource:      ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: involvingSchemaInfo,
		SQLMode:             ctx.GetSessionVars().SQLMode,
	}
	err := e.doDDLJob2(ctx, job, args)
	if err == nil {
		ctx.ReleaseTableLocks(cleanupTables)
	}
	return errors.Trace(err)
}
