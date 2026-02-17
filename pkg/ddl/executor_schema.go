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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/infoschema"
	infoschemacontext "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"go.uber.org/zap"
)

func (e *executor) CreateSchema(ctx sessionctx.Context, stmt *ast.CreateDatabaseStmt) (err error) {
	var placementPolicyRef *model.PolicyRefInfo
	sessionVars := ctx.GetSessionVars()

	// If no charset and/or collation is specified use collation_server and character_set_server
	charsetOpt := ast.CharsetOpt{}
	if sessionVars.GlobalVarsAccessor != nil {
		charsetOpt.Col, err = sessionVars.GetSessionOrGlobalSystemVar(context.Background(), vardef.CollationServer)
		if err != nil {
			return err
		}
		charsetOpt.Chs, err = sessionVars.GetSessionOrGlobalSystemVar(context.Background(), vardef.CharacterSetServer)
		if err != nil {
			return err
		}
	}

	explicitCharset := false
	explicitCollation := false
	for _, val := range stmt.Options {
		switch val.Tp {
		case ast.DatabaseOptionCharset:
			charsetOpt.Chs = val.Value
			explicitCharset = true
		case ast.DatabaseOptionCollate:
			charsetOpt.Col = val.Value
			explicitCollation = true
		case ast.DatabaseOptionPlacementPolicy:
			placementPolicyRef = &model.PolicyRefInfo{
				Name: ast.NewCIStr(val.Value),
			}
		}
	}

	if charsetOpt.Col != "" {
		coll, err := collate.GetCollationByName(charsetOpt.Col)
		if err != nil {
			return err
		}

		// The collation is not valid for the specified character set.
		// Try to remove any of them, but not if they are explicitly defined.
		if coll.CharsetName != charsetOpt.Chs {
			if explicitCollation && !explicitCharset {
				// Use the explicitly set collation, not the implicit charset.
				charsetOpt.Chs = ""
			}
			if !explicitCollation && explicitCharset {
				// Use the explicitly set charset, not the (session) collation.
				charsetOpt.Col = ""
			}
		}
	}
	if !explicitCollation && explicitCharset {
		coll := getDefaultCollationForUTF8MB4(charsetOpt.Chs, ctx.GetSessionVars().DefaultCollationForUTF8MB4)
		if len(coll) != 0 {
			charsetOpt.Col = coll
		}
	}
	dbInfo := &model.DBInfo{Name: stmt.Name}
	chs, coll, err := ResolveCharsetCollation([]ast.CharsetOpt{charsetOpt}, ctx.GetSessionVars().DefaultCollationForUTF8MB4)
	if err != nil {
		return errors.Trace(err)
	}
	dbInfo.Charset = chs
	dbInfo.Collate = coll
	dbInfo.PlacementPolicyRef = placementPolicyRef

	onExist := OnExistError
	if stmt.IfNotExists {
		onExist = OnExistIgnore
	}
	return e.CreateSchemaWithInfo(ctx, dbInfo, onExist)
}

func (e *executor) CreateSchemaWithInfo(
	ctx sessionctx.Context,
	dbInfo *model.DBInfo,
	onExist OnExist,
) error {
	is := e.infoCache.GetLatest()
	_, ok := is.SchemaByName(dbInfo.Name)
	if ok {
		// since this error may be seen as error, keep it stack info.
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

	if err := checkTooLongSchema(dbInfo.Name); err != nil {
		return errors.Trace(err)
	}

	if err := checkCharsetAndCollation(dbInfo.Charset, dbInfo.Collate); err != nil {
		return errors.Trace(err)
	}

	if err := handleDatabasePlacement(ctx, dbInfo); err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaName:     dbInfo.Name.L,
		Type:           model.ActionCreateSchema,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			Database: dbInfo.Name.L,
			Table:    model.InvolvingAll,
		}},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}
	args := &model.CreateSchemaArgs{
		DBInfo: dbInfo,
	}
	if ref := dbInfo.PlacementPolicyRef; ref != nil {
		job.InvolvingSchemaInfo = append(job.InvolvingSchemaInfo, model.InvolvingSchemaInfo{
			Policy: ref.Name.L,
			Mode:   model.SharedInvolving,
		})
	}

	err := e.doDDLJob2(ctx, job, args)

	if infoschema.ErrDatabaseExists.Equal(err) && onExist == OnExistIgnore {
		ctx.GetSessionVars().StmtCtx.AppendNote(err)
		return nil
	}

	return errors.Trace(err)
}

func (e *executor) ModifySchemaCharsetAndCollate(ctx sessionctx.Context, stmt *ast.AlterDatabaseStmt, toCharset, toCollate string) (err error) {
	if toCollate == "" {
		if toCollate, err = GetDefaultCollation(toCharset, ctx.GetSessionVars().DefaultCollationForUTF8MB4); err != nil {
			return errors.Trace(err)
		}
	}

	// Check if need to change charset/collation.
	dbName := stmt.Name
	is := e.infoCache.GetLatest()
	dbInfo, ok := is.SchemaByName(dbName)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(dbName.O)
	}
	if dbInfo.Charset == toCharset && dbInfo.Collate == toCollate {
		return nil
	}
	// Do the DDL job.
	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       dbInfo.ID,
		SchemaName:     dbInfo.Name.L,
		Type:           model.ActionModifySchemaCharsetAndCollate,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			Database: dbInfo.Name.L,
			Table:    model.InvolvingAll,
		}},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}
	args := &model.ModifySchemaArgs{
		ToCharset: toCharset,
		ToCollate: toCollate,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func (e *executor) ModifySchemaDefaultPlacement(ctx sessionctx.Context, stmt *ast.AlterDatabaseStmt, placementPolicyRef *model.PolicyRefInfo) (err error) {
	dbName := stmt.Name
	is := e.infoCache.GetLatest()
	dbInfo, ok := is.SchemaByName(dbName)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(dbName.O)
	}

	if checkIgnorePlacementDDL(ctx) {
		return nil
	}

	placementPolicyRef, err = checkAndNormalizePlacementPolicy(ctx, placementPolicyRef)
	if err != nil {
		return err
	}

	// Do the DDL job.
	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       dbInfo.ID,
		SchemaName:     dbInfo.Name.L,
		Type:           model.ActionModifySchemaDefaultPlacement,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			Database: dbInfo.Name.L,
			Table:    model.InvolvingAll,
		}},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}
	args := &model.ModifySchemaArgs{PolicyRef: placementPolicyRef}

	if placementPolicyRef != nil {
		job.InvolvingSchemaInfo = append(job.InvolvingSchemaInfo, model.InvolvingSchemaInfo{
			Policy: placementPolicyRef.Name.L,
			Mode:   model.SharedInvolving,
		})
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

// getPendingTiFlashTableCount counts unavailable TiFlash replica by iterating all tables in infoCache.
func (e *executor) getPendingTiFlashTableCount(originVersion int64, pendingCount uint32) (int64, uint32) {
	is := e.infoCache.GetLatest()
	// If there are no schema change since last time(can be weird).
	if is.SchemaMetaVersion() == originVersion {
		return originVersion, pendingCount
	}
	cnt := uint32(0)
	dbs := is.ListTablesWithSpecialAttribute(infoschemacontext.TiFlashAttribute)
	for _, db := range dbs {
		if metadef.IsMemOrSysDB(db.DBName.L) {
			continue
		}
		for _, tbl := range db.TableInfos {
			if tbl.TiFlashReplica != nil && !tbl.TiFlashReplica.Available {
				cnt++
			}
		}
	}
	return is.SchemaMetaVersion(), cnt
}

func isSessionDone(sctx sessionctx.Context) (bool, uint32) {
	done := false
	killed := sctx.GetSessionVars().SQLKiller.HandleSignal() == exeerrors.ErrQueryInterrupted
	if killed {
		return true, 1
	}
	failpoint.Inject("BatchAddTiFlashSendDone", func(val failpoint.Value) {
		done = val.(bool)
	})
	return done, 0
}

func (e *executor) waitPendingTableThreshold(sctx sessionctx.Context, schemaID int64, tableID int64, originVersion int64, pendingCount uint32, threshold uint32) (bool, int64, uint32, bool) {
	configRetry := tiflashCheckPendingTablesRetry
	configWaitTime := tiflashCheckPendingTablesWaitTime
	failpoint.Inject("FastFailCheckTiFlashPendingTables", func(value failpoint.Value) {
		configRetry = value.(int)
		configWaitTime = time.Millisecond * 200
	})

	for range configRetry {
		done, killed := isSessionDone(sctx)
		if done {
			logutil.DDLLogger().Info("abort batch add TiFlash replica", zap.Int64("schemaID", schemaID), zap.Uint32("isKilled", killed))
			return true, originVersion, pendingCount, false
		}
		originVersion, pendingCount = e.getPendingTiFlashTableCount(originVersion, pendingCount)
		delay := time.Duration(0)
		if pendingCount < threshold {
			// If there are not many unavailable tables, we don't need a force check.
			return false, originVersion, pendingCount, false
		}
		logutil.DDLLogger().Info("too many unavailable tables, wait",
			zap.Uint32("threshold", threshold),
			zap.Uint32("currentPendingCount", pendingCount),
			zap.Int64("schemaID", schemaID),
			zap.Int64("tableID", tableID),
			zap.Duration("time", configWaitTime))
		delay = configWaitTime
		time.Sleep(delay)
	}
	logutil.DDLLogger().Info("too many unavailable tables, timeout", zap.Int64("schemaID", schemaID), zap.Int64("tableID", tableID))
	// If timeout here, we will trigger a ddl job, to force sync schema. However, it doesn't mean we remove limiter,
	// so there is a force check immediately after that.
	return false, originVersion, pendingCount, true
}

func (e *executor) ModifySchemaSetTiFlashReplica(sctx sessionctx.Context, stmt *ast.AlterDatabaseStmt, tiflashReplica *ast.TiFlashReplicaSpec) error {
	dbName := stmt.Name
	is := e.infoCache.GetLatest()
	dbInfo, ok := is.SchemaByName(dbName)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(dbName.O)
	}

	if metadef.IsMemOrSysDB(dbInfo.Name.L) {
		return errors.Trace(dbterror.ErrUnsupportedTiFlashOperationForSysOrMemTable)
	}

	tbls, err := is.SchemaTableInfos(context.Background(), dbInfo.Name)
	if err != nil {
		return errors.Trace(err)
	}

	total := len(tbls)
	succ := 0
	skip := 0
	fail := 0
	oneFail := int64(0)

	if total == 0 {
		return infoschema.ErrEmptyDatabase.GenWithStack("Empty database '%v'", dbName.O)
	}
	err = checkTiFlashReplicaCount(sctx, tiflashReplica.Count)
	if err != nil {
		return errors.Trace(err)
	}

	var originVersion int64
	var pendingCount uint32
	forceCheck := false

	logutil.DDLLogger().Info("start batch add TiFlash replicas", zap.Int("total", total), zap.Int64("schemaID", dbInfo.ID))
	threshold := uint32(sctx.GetSessionVars().BatchPendingTiFlashCount)

	for _, tbl := range tbls {
		done, killed := isSessionDone(sctx)
		if done {
			logutil.DDLLogger().Info("abort batch add TiFlash replica", zap.Int64("schemaID", dbInfo.ID), zap.Uint32("isKilled", killed))
			return nil
		}

		tbReplicaInfo := tbl.TiFlashReplica
		if !shouldModifyTiFlashReplica(tbReplicaInfo, tiflashReplica) {
			logutil.DDLLogger().Info("skip repeated processing table",
				zap.Int64("tableID", tbl.ID),
				zap.Int64("schemaID", dbInfo.ID),
				zap.String("tableName", tbl.Name.String()),
				zap.String("schemaName", dbInfo.Name.String()))
			skip++
			continue
		}

		// If table is not supported, add err to warnings.
		err = isTableTiFlashSupported(dbName, tbl)
		if err != nil {
			logutil.DDLLogger().Info("skip processing table", zap.Int64("tableID", tbl.ID),
				zap.Int64("schemaID", dbInfo.ID),
				zap.String("tableName", tbl.Name.String()),
				zap.String("schemaName", dbInfo.Name.String()),
				zap.Error(err))
			sctx.GetSessionVars().StmtCtx.AppendNote(err)
			skip++
			continue
		}

		// Alter `tiflashCheckPendingTablesLimit` tables are handled, we need to check if we have reached threshold.
		if (succ+fail)%tiflashCheckPendingTablesLimit == 0 || forceCheck {
			// We can execute one probing ddl to the latest schema, if we timeout in `pendingFunc`.
			// However, we shall mark `forceCheck` to true, because we may still reach `threshold`.
			finished := false
			finished, originVersion, pendingCount, forceCheck = e.waitPendingTableThreshold(sctx, dbInfo.ID, tbl.ID, originVersion, pendingCount, threshold)
			if finished {
				logutil.DDLLogger().Info("abort batch add TiFlash replica", zap.Int64("schemaID", dbInfo.ID))
				return nil
			}
		}

		job := &model.Job{
			Version:        model.GetJobVerInUse(),
			SchemaID:       dbInfo.ID,
			SchemaName:     dbInfo.Name.L,
			TableID:        tbl.ID,
			Type:           model.ActionSetTiFlashReplica,
			BinlogInfo:     &model.HistoryInfo{},
			CDCWriteSource: sctx.GetSessionVars().CDCWriteSource,
			InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
				Database: dbInfo.Name.L,
				Table:    model.InvolvingAll,
			}},
			SQLMode: sctx.GetSessionVars().SQLMode,
		}
		args := &model.SetTiFlashReplicaArgs{TiflashReplica: *tiflashReplica}
		err := e.doDDLJob2(sctx, job, args)
		if err != nil {
			oneFail = tbl.ID
			fail++
			logutil.DDLLogger().Info("processing schema table error",
				zap.Int64("tableID", tbl.ID),
				zap.Int64("schemaID", dbInfo.ID),
				zap.Stringer("tableName", tbl.Name),
				zap.Stringer("schemaName", dbInfo.Name),
				zap.Error(err))
		} else {
			succ++
		}
	}
	failStmt := ""
	if fail > 0 {
		failStmt = fmt.Sprintf("(including table %v)", oneFail)
	}
	msg := fmt.Sprintf("In total %v tables: %v succeed, %v failed%v, %v skipped", total, succ, fail, failStmt, skip)
	sctx.GetSessionVars().StmtCtx.SetMessage(msg)
	logutil.DDLLogger().Info("finish batch add TiFlash replica", zap.Int64("schemaID", dbInfo.ID))
	return nil
}

func (e *executor) AlterTablePlacement(ctx sessionctx.Context, ident ast.Ident, placementPolicyRef *model.PolicyRefInfo) (err error) {
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}

	tb, err := is.TableByName(e.ctx, ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}

	if checkIgnorePlacementDDL(ctx) {
		return nil
	}

	tblInfo := tb.Meta()
	if tblInfo.TempTableType != model.TempTableNone {
		return errors.Trace(dbterror.ErrOptOnTemporaryTable.GenWithStackByArgs("placement"))
	}

	placementPolicyRef, err = checkAndNormalizePlacementPolicy(ctx, placementPolicyRef)
	if err != nil {
		return err
	}

	var involvingSchemaInfo []model.InvolvingSchemaInfo
	if placementPolicyRef != nil {
		involvingSchemaInfo = []model.InvolvingSchemaInfo{
			{
				Database: schema.Name.L,
				Table:    tblInfo.Name.L,
			},
			{
				Policy: placementPolicyRef.Name.L,
				Mode:   model.SharedInvolving,
			},
		}
	}

	job := &model.Job{
		Version:             model.GetJobVerInUse(),
		SchemaID:            schema.ID,
		TableID:             tblInfo.ID,
		SchemaName:          schema.Name.L,
		TableName:           tblInfo.Name.L,
		Type:                model.ActionAlterTablePlacement,
		BinlogInfo:          &model.HistoryInfo{},
		CDCWriteSource:      ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: involvingSchemaInfo,
		SQLMode:             ctx.GetSessionVars().SQLMode,
	}
	args := &model.AlterTablePlacementArgs{
		PlacementPolicyRef: placementPolicyRef,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func checkMultiSchemaSpecs(_ sessionctx.Context, specs []*ast.DatabaseOption) error {
	hasSetTiFlashReplica := false
	if len(specs) == 1 {
		return nil
	}
	for _, spec := range specs {
		if spec.Tp == ast.DatabaseSetTiFlashReplica {
			if hasSetTiFlashReplica {
				return dbterror.ErrRunMultiSchemaChanges.FastGenByArgs(model.ActionSetTiFlashReplica.String())
			}
			hasSetTiFlashReplica = true
		}
	}
	return nil
}

func (e *executor) AlterSchema(sctx sessionctx.Context, stmt *ast.AlterDatabaseStmt) (err error) {
	// Resolve target charset and collation from options.
	var (
		toCharset, toCollate     string
		isAlterCharsetAndCollate bool
		placementPolicyRef       *model.PolicyRefInfo
		tiflashReplica           *ast.TiFlashReplicaSpec
	)

	err = checkMultiSchemaSpecs(sctx, stmt.Options)
	if err != nil {
		return err
	}

	for _, val := range stmt.Options {
		switch val.Tp {
		case ast.DatabaseOptionCharset:
			if toCharset == "" {
				toCharset = val.Value
			} else if toCharset != val.Value {
				return dbterror.ErrConflictingDeclarations.GenWithStackByArgs(toCharset, val.Value)
			}
			isAlterCharsetAndCollate = true
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
			isAlterCharsetAndCollate = true
		case ast.DatabaseOptionPlacementPolicy:
			placementPolicyRef = &model.PolicyRefInfo{Name: ast.NewCIStr(val.Value)}
		case ast.DatabaseSetTiFlashReplica:
			tiflashReplica = val.TiFlashReplica
		}
	}

	if isAlterCharsetAndCollate {
		if err = e.ModifySchemaCharsetAndCollate(sctx, stmt, toCharset, toCollate); err != nil {
			return err
		}
	}
	if placementPolicyRef != nil {
		if err = e.ModifySchemaDefaultPlacement(sctx, stmt, placementPolicyRef); err != nil {
			return err
		}
	}
	if tiflashReplica != nil {
		if err = e.ModifySchemaSetTiFlashReplica(sctx, stmt, tiflashReplica); err != nil {
			return err
		}
	}
	return nil
}

func (e *executor) DropSchema(ctx sessionctx.Context, stmt *ast.DropDatabaseStmt) (err error) {
	is := e.infoCache.GetLatest()
	old, ok := is.SchemaByName(stmt.Name)
	if !ok {
		if stmt.IfExists {
			return nil
		}
		return infoschema.ErrDatabaseDropExists.GenWithStackByArgs(stmt.Name)
	}
	if isReservedSchemaObjInNextGen(old.ID) {
		return dbterror.ErrForbiddenDDL.FastGenByArgs(fmt.Sprintf("Drop '%s' database", old.Name.L))
	}
	fkCheck := ctx.GetSessionVars().ForeignKeyChecks
	err = checkDatabaseHasForeignKeyReferred(e.ctx, is, old.Name, fkCheck)
	if err != nil {
		return err
	}
	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       old.ID,
		SchemaName:     old.Name.L,
		SchemaState:    old.State,
		Type:           model.ActionDropSchema,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			Database: old.Name.L,
			Table:    model.InvolvingAll,
		}},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}
	args := &model.DropSchemaArgs{
		FKCheck: fkCheck,
	}

	err = e.doDDLJob2(ctx, job, args)
	if err != nil {
		if infoschema.ErrDatabaseNotExists.Equal(err) {
			if stmt.IfExists {
				return nil
			}
			return infoschema.ErrDatabaseDropExists.GenWithStackByArgs(stmt.Name)
		}
		return errors.Trace(err)
	}
	if !config.TableLockEnabled() {
		return nil
	}

	// Clear table locks hold by the session.
	tbs, err := is.SchemaSimpleTableInfos(e.ctx, stmt.Name)
	if err != nil {
		return errors.Trace(err)
	}

	lockTableIDs := make([]int64, 0)
	for _, tb := range tbs {
		if ok, _ := ctx.CheckTableLocked(tb.ID); ok {
			lockTableIDs = append(lockTableIDs, tb.ID)
		}
	}
	ctx.ReleaseTableLockByTableIDs(lockTableIDs)
	return nil
}

func (e *executor) RecoverSchema(ctx sessionctx.Context, recoverSchemaInfo *model.RecoverSchemaInfo) error {
	involvedSchemas := []model.InvolvingSchemaInfo{{
		Database: recoverSchemaInfo.DBInfo.Name.L,
		Table:    model.InvolvingAll,
	}}
	if recoverSchemaInfo.OldSchemaName.L != recoverSchemaInfo.DBInfo.Name.L {
		involvedSchemas = append(involvedSchemas, model.InvolvingSchemaInfo{
			Database: recoverSchemaInfo.OldSchemaName.L,
			Table:    model.InvolvingAll,
		})
	}
	recoverSchemaInfo.State = model.StateNone
	job := &model.Job{
		Version:             model.GetJobVerInUse(),
		Type:                model.ActionRecoverSchema,
		BinlogInfo:          &model.HistoryInfo{},
		CDCWriteSource:      ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: involvedSchemas,
		SQLMode:             ctx.GetSessionVars().SQLMode,
	}

	args := &model.RecoverArgs{
		RecoverInfo: recoverSchemaInfo,
		CheckFlag:   recoverCheckFlagNone,
	}
	err := e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}
