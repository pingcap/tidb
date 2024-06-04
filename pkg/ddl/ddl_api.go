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
	"bytes"
	"context"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/label"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/resourcegroup"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	rg "github.com/pingcap/tidb/pkg/domain/resourcegroup"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression"
	exprctx "github.com/pingcap/tidb/pkg/expression/context"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	field_types "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/domainutil"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"github.com/tikv/client-go/v2/oracle"
	kvutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

const (
	expressionIndexPrefix = "_V$"
	changingColumnPrefix  = "_Col$_"
	changingIndexPrefix   = "_Idx$_"
	tableNotExist         = -1
	tinyBlobMaxLength     = 255
	blobMaxLength         = 65535
	mediumBlobMaxLength   = 16777215
	longBlobMaxLength     = 4294967295
	// When setting the placement policy with "PLACEMENT POLICY `default`",
	// it means to remove placement policy from the specified object.
	defaultPlacementPolicyName        = "default"
	tiflashCheckPendingTablesWaitTime = 3000 * time.Millisecond
	// Once tiflashCheckPendingTablesLimit is reached, we trigger a limiter detection.
	tiflashCheckPendingTablesLimit = 100
	tiflashCheckPendingTablesRetry = 7
)

var errCheckConstraintIsOff = errors.NewNoStackError(variable.TiDBEnableCheckConstraint + " is off")

func (d *ddl) CreateSchema(ctx sessionctx.Context, stmt *ast.CreateDatabaseStmt) (err error) {
	var placementPolicyRef *model.PolicyRefInfo
	sessionVars := ctx.GetSessionVars()

	// If no charset and/or collation is specified use collation_server and character_set_server
	charsetOpt := ast.CharsetOpt{}
	if sessionVars.GlobalVarsAccessor != nil {
		charsetOpt.Col, err = sessionVars.GetSessionOrGlobalSystemVar(context.Background(), variable.CollationServer)
		if err != nil {
			return err
		}
		charsetOpt.Chs, err = sessionVars.GetSessionOrGlobalSystemVar(context.Background(), variable.CharacterSetServer)
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
				Name: model.NewCIStr(val.Value),
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
		coll, err := getDefaultCollationForUTF8MB4(ctx.GetSessionVars(), charsetOpt.Chs)
		if err != nil {
			return err
		}
		if len(coll) != 0 {
			charsetOpt.Col = coll
		}
	}
	dbInfo := &model.DBInfo{Name: stmt.Name}
	chs, coll, err := ResolveCharsetCollation(ctx.GetSessionVars(), charsetOpt)
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
	return d.CreateSchemaWithInfo(ctx, dbInfo, onExist)
}

func (d *ddl) CreateSchemaWithInfo(
	ctx sessionctx.Context,
	dbInfo *model.DBInfo,
	onExist OnExist,
) error {
	is := d.GetInfoSchemaWithInterceptor(ctx)
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

	// FIXME: support `tryRetainID`.
	genIDs, err := d.genGlobalIDs(1)
	if err != nil {
		return errors.Trace(err)
	}
	dbInfo.ID = genIDs[0]

	job := &model.Job{
		SchemaID:       dbInfo.ID,
		SchemaName:     dbInfo.Name.L,
		Type:           model.ActionCreateSchema,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           []any{dbInfo},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)

	if infoschema.ErrDatabaseExists.Equal(err) && onExist == OnExistIgnore {
		ctx.GetSessionVars().StmtCtx.AppendNote(err)
		return nil
	}

	return errors.Trace(err)
}

func (d *ddl) ModifySchemaCharsetAndCollate(ctx sessionctx.Context, stmt *ast.AlterDatabaseStmt, toCharset, toCollate string) (err error) {
	if toCollate == "" {
		if toCollate, err = GetDefaultCollation(ctx.GetSessionVars(), toCharset); err != nil {
			return errors.Trace(err)
		}
	}

	// Check if need to change charset/collation.
	dbName := stmt.Name
	is := d.GetInfoSchemaWithInterceptor(ctx)
	dbInfo, ok := is.SchemaByName(dbName)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(dbName.O)
	}
	if dbInfo.Charset == toCharset && dbInfo.Collate == toCollate {
		return nil
	}
	// Do the DDL job.
	job := &model.Job{
		SchemaID:       dbInfo.ID,
		SchemaName:     dbInfo.Name.L,
		Type:           model.ActionModifySchemaCharsetAndCollate,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           []any{toCharset, toCollate},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

func (d *ddl) ModifySchemaDefaultPlacement(ctx sessionctx.Context, stmt *ast.AlterDatabaseStmt, placementPolicyRef *model.PolicyRefInfo) (err error) {
	dbName := stmt.Name
	is := d.GetInfoSchemaWithInterceptor(ctx)
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
		SchemaID:       dbInfo.ID,
		SchemaName:     dbInfo.Name.L,
		Type:           model.ActionModifySchemaDefaultPlacement,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           []any{placementPolicyRef},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

// getPendingTiFlashTableCount counts unavailable TiFlash replica by iterating all tables in infoCache.
func (d *ddl) getPendingTiFlashTableCount(sctx sessionctx.Context, originVersion int64, pendingCount uint32) (int64, uint32) {
	is := d.GetInfoSchemaWithInterceptor(sctx)
	dbNames := is.AllSchemaNames()
	// If there are no schema change since last time(can be weird).
	if is.SchemaMetaVersion() == originVersion {
		return originVersion, pendingCount
	}
	cnt := uint32(0)
	for _, dbName := range dbNames {
		if util.IsMemOrSysDB(dbName.L) {
			continue
		}
		for _, tbl := range is.SchemaTables(dbName) {
			if tbl.Meta().TiFlashReplica != nil && !tbl.Meta().TiFlashReplica.Available {
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

func (d *ddl) waitPendingTableThreshold(sctx sessionctx.Context, schemaID int64, tableID int64, originVersion int64, pendingCount uint32, threshold uint32) (bool, int64, uint32, bool) {
	configRetry := tiflashCheckPendingTablesRetry
	configWaitTime := tiflashCheckPendingTablesWaitTime
	failpoint.Inject("FastFailCheckTiFlashPendingTables", func(value failpoint.Value) {
		configRetry = value.(int)
		configWaitTime = time.Millisecond * 200
	})

	for retry := 0; retry < configRetry; retry++ {
		done, killed := isSessionDone(sctx)
		if done {
			logutil.DDLLogger().Info("abort batch add TiFlash replica", zap.Int64("schemaID", schemaID), zap.Uint32("isKilled", killed))
			return true, originVersion, pendingCount, false
		}
		originVersion, pendingCount = d.getPendingTiFlashTableCount(sctx, originVersion, pendingCount)
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

func (d *ddl) ModifySchemaSetTiFlashReplica(sctx sessionctx.Context, stmt *ast.AlterDatabaseStmt, tiflashReplica *ast.TiFlashReplicaSpec) error {
	dbName := stmt.Name
	is := d.GetInfoSchemaWithInterceptor(sctx)
	dbInfo, ok := is.SchemaByName(dbName)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(dbName.O)
	}

	if util.IsMemOrSysDB(dbInfo.Name.L) {
		return errors.Trace(dbterror.ErrUnsupportedTiFlashOperationForSysOrMemTable)
	}

	tbls := is.SchemaTables(dbInfo.Name)
	total := len(tbls)
	succ := 0
	skip := 0
	fail := 0
	oneFail := int64(0)

	if total == 0 {
		return infoschema.ErrEmptyDatabase.GenWithStack("Empty database '%v'", dbName.O)
	}
	err := checkTiFlashReplicaCount(sctx, tiflashReplica.Count)
	if err != nil {
		return errors.Trace(err)
	}

	var originVersion int64
	var pendingCount uint32
	forceCheck := false

	logutil.DDLLogger().Info("start batch add TiFlash replicas", zap.Int("total", total), zap.Int64("schemaID", dbInfo.ID))
	threshold := uint32(sctx.GetSessionVars().BatchPendingTiFlashCount)

	for _, tbl := range tbls {
		tbl := tbl.Meta()
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
			finished, originVersion, pendingCount, forceCheck = d.waitPendingTableThreshold(sctx, dbInfo.ID, tbl.ID, originVersion, pendingCount, threshold)
			if finished {
				logutil.DDLLogger().Info("abort batch add TiFlash replica", zap.Int64("schemaID", dbInfo.ID))
				return nil
			}
		}

		job := &model.Job{
			SchemaID:       dbInfo.ID,
			SchemaName:     dbInfo.Name.L,
			TableID:        tbl.ID,
			Type:           model.ActionSetTiFlashReplica,
			BinlogInfo:     &model.HistoryInfo{},
			Args:           []any{*tiflashReplica},
			CDCWriteSource: sctx.GetSessionVars().CDCWriteSource,
			SQLMode:        sctx.GetSessionVars().SQLMode,
		}
		err := d.DoDDLJob(sctx, job)
		err = d.callHookOnChanged(job, err)
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

func (d *ddl) AlterTablePlacement(ctx sessionctx.Context, ident ast.Ident, placementPolicyRef *model.PolicyRefInfo) (err error) {
	is := d.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}

	tb, err := is.TableByName(ident.Schema, ident.Name)
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

	job := &model.Job{
		SchemaID:       schema.ID,
		TableID:        tblInfo.ID,
		SchemaName:     schema.Name.L,
		TableName:      tblInfo.Name.L,
		Type:           model.ActionAlterTablePlacement,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		Args:           []any{placementPolicyRef},
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

func checkAndNormalizePlacementPolicy(ctx sessionctx.Context, placementPolicyRef *model.PolicyRefInfo) (*model.PolicyRefInfo, error) {
	if placementPolicyRef == nil {
		return nil, nil
	}

	if placementPolicyRef.Name.L == defaultPlacementPolicyName {
		// When policy name is 'default', it means to remove the placement settings
		return nil, nil
	}

	policy, ok := sessiontxn.GetTxnManager(ctx).GetTxnInfoSchema().PolicyByName(placementPolicyRef.Name)
	if !ok {
		return nil, errors.Trace(infoschema.ErrPlacementPolicyNotExists.GenWithStackByArgs(placementPolicyRef.Name))
	}

	placementPolicyRef.ID = policy.ID
	return placementPolicyRef, nil
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

func (d *ddl) AlterSchema(sctx sessionctx.Context, stmt *ast.AlterDatabaseStmt) (err error) {
	// Resolve target charset and collation from options.
	var (
		toCharset, toCollate                                         string
		isAlterCharsetAndCollate, isAlterPlacement, isTiFlashReplica bool
		placementPolicyRef                                           *model.PolicyRefInfo
		tiflashReplica                                               *ast.TiFlashReplicaSpec
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
			placementPolicyRef = &model.PolicyRefInfo{Name: model.NewCIStr(val.Value)}
			isAlterPlacement = true
		case ast.DatabaseSetTiFlashReplica:
			tiflashReplica = val.TiFlashReplica
			isTiFlashReplica = true
		}
	}

	if isAlterCharsetAndCollate {
		if err = d.ModifySchemaCharsetAndCollate(sctx, stmt, toCharset, toCollate); err != nil {
			return err
		}
	}
	if isAlterPlacement {
		if err = d.ModifySchemaDefaultPlacement(sctx, stmt, placementPolicyRef); err != nil {
			return err
		}
	}
	if isTiFlashReplica {
		if err = d.ModifySchemaSetTiFlashReplica(sctx, stmt, tiflashReplica); err != nil {
			return err
		}
	}
	return nil
}

func (d *ddl) DropSchema(ctx sessionctx.Context, stmt *ast.DropDatabaseStmt) (err error) {
	is := d.GetInfoSchemaWithInterceptor(ctx)
	old, ok := is.SchemaByName(stmt.Name)
	if !ok {
		if stmt.IfExists {
			return nil
		}
		return infoschema.ErrDatabaseDropExists.GenWithStackByArgs(stmt.Name)
	}
	fkCheck := ctx.GetSessionVars().ForeignKeyChecks
	err = checkDatabaseHasForeignKeyReferred(is, old.Name, fkCheck)
	if err != nil {
		return err
	}
	job := &model.Job{
		SchemaID:       old.ID,
		SchemaName:     old.Name.L,
		SchemaState:    old.State,
		Type:           model.ActionDropSchema,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           []any{fkCheck},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
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
	tbs := is.SchemaTables(stmt.Name)
	lockTableIDs := make([]int64, 0)
	for _, tb := range tbs {
		if ok, _ := ctx.CheckTableLocked(tb.Meta().ID); ok {
			lockTableIDs = append(lockTableIDs, tb.Meta().ID)
		}
	}
	ctx.ReleaseTableLockByTableIDs(lockTableIDs)
	return nil
}

func (d *ddl) RecoverSchema(ctx sessionctx.Context, recoverSchemaInfo *RecoverSchemaInfo) error {
	recoverSchemaInfo.State = model.StateNone
	job := &model.Job{
		Type:           model.ActionRecoverSchema,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		Args:           []any{recoverSchemaInfo, recoverCheckFlagNone},
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			Database: recoverSchemaInfo.Name.L,
			Table:    model.InvolvingAll,
		}},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}
	err := d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

func checkTooLongSchema(schema model.CIStr) error {
	if utf8.RuneCountInString(schema.L) > mysql.MaxDatabaseNameLength {
		return dbterror.ErrTooLongIdent.GenWithStackByArgs(schema)
	}
	return nil
}

func checkTooLongTable(table model.CIStr) error {
	if utf8.RuneCountInString(table.L) > mysql.MaxTableNameLength {
		return dbterror.ErrTooLongIdent.GenWithStackByArgs(table)
	}
	return nil
}

func checkTooLongIndex(index model.CIStr) error {
	if utf8.RuneCountInString(index.L) > mysql.MaxIndexIdentifierLen {
		return dbterror.ErrTooLongIdent.GenWithStackByArgs(index)
	}
	return nil
}

func checkTooLongColumn(col model.CIStr) error {
	if utf8.RuneCountInString(col.L) > mysql.MaxColumnNameLength {
		return dbterror.ErrTooLongIdent.GenWithStackByArgs(col)
	}
	return nil
}

func checkTooLongForeignKey(fk model.CIStr) error {
	if utf8.RuneCountInString(fk.L) > mysql.MaxForeignKeyIdentifierLen {
		return dbterror.ErrTooLongIdent.GenWithStackByArgs(fk)
	}
	return nil
}

func setColumnFlagWithConstraint(colMap map[string]*table.Column, v *ast.Constraint) {
	switch v.Tp {
	case ast.ConstraintPrimaryKey:
		for _, key := range v.Keys {
			if key.Expr != nil {
				continue
			}
			c, ok := colMap[key.Column.Name.L]
			if !ok {
				continue
			}
			c.AddFlag(mysql.PriKeyFlag)
			// Primary key can not be NULL.
			c.AddFlag(mysql.NotNullFlag)
			setNoDefaultValueFlag(c, c.DefaultValue != nil)
		}
	case ast.ConstraintUniq, ast.ConstraintUniqIndex, ast.ConstraintUniqKey:
		for i, key := range v.Keys {
			if key.Expr != nil {
				continue
			}
			c, ok := colMap[key.Column.Name.L]
			if !ok {
				continue
			}
			if i == 0 {
				// Only the first column can be set
				// if unique index has multi columns,
				// the flag should be MultipleKeyFlag.
				// See https://dev.mysql.com/doc/refman/5.7/en/show-columns.html
				if len(v.Keys) > 1 {
					c.AddFlag(mysql.MultipleKeyFlag)
				} else {
					c.AddFlag(mysql.UniqueKeyFlag)
				}
			}
		}
	case ast.ConstraintKey, ast.ConstraintIndex:
		for i, key := range v.Keys {
			if key.Expr != nil {
				continue
			}
			c, ok := colMap[key.Column.Name.L]
			if !ok {
				continue
			}
			if i == 0 {
				// Only the first column can be set.
				c.AddFlag(mysql.MultipleKeyFlag)
			}
		}
	}
}

func buildColumnsAndConstraints(
	ctx sessionctx.Context,
	colDefs []*ast.ColumnDef,
	constraints []*ast.Constraint,
	tblCharset string,
	tblCollate string,
) ([]*table.Column, []*ast.Constraint, error) {
	// outPriKeyConstraint is the primary key constraint out of column definition. such as: create table t1 (id int , age int, primary key(id));
	var outPriKeyConstraint *ast.Constraint
	for _, v := range constraints {
		if v.Tp == ast.ConstraintPrimaryKey {
			outPriKeyConstraint = v
			break
		}
	}
	cols := make([]*table.Column, 0, len(colDefs))
	colMap := make(map[string]*table.Column, len(colDefs))

	for i, colDef := range colDefs {
		if field_types.TiDBStrictIntegerDisplayWidth {
			switch colDef.Tp.GetType() {
			case mysql.TypeTiny:
				// No warning for BOOL-like tinyint(1)
				if colDef.Tp.GetFlen() != types.UnspecifiedLength && colDef.Tp.GetFlen() != 1 {
					ctx.GetSessionVars().StmtCtx.AppendWarning(
						dbterror.ErrWarnDeprecatedIntegerDisplayWidth.FastGenByArgs(),
					)
				}
			case mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
				if colDef.Tp.GetFlen() != types.UnspecifiedLength {
					ctx.GetSessionVars().StmtCtx.AppendWarning(
						dbterror.ErrWarnDeprecatedIntegerDisplayWidth.FastGenByArgs(),
					)
				}
			}
		}
		col, cts, err := buildColumnAndConstraint(ctx, i, colDef, outPriKeyConstraint, tblCharset, tblCollate)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		col.State = model.StatePublic
		if mysql.HasZerofillFlag(col.GetFlag()) {
			ctx.GetSessionVars().StmtCtx.AppendWarning(
				dbterror.ErrWarnDeprecatedZerofill.FastGenByArgs(),
			)
		}
		constraints = append(constraints, cts...)
		cols = append(cols, col)
		colMap[colDef.Name.Name.L] = col
	}
	// Traverse table Constraints and set col.flag.
	for _, v := range constraints {
		setColumnFlagWithConstraint(colMap, v)
	}
	return cols, constraints, nil
}

func getDefaultCollationForUTF8MB4(sessVars *variable.SessionVars, cs string) (string, error) {
	if sessVars == nil || cs != charset.CharsetUTF8MB4 {
		return "", nil
	}
	defaultCollation, err := sessVars.GetSessionOrGlobalSystemVar(context.Background(), variable.DefaultCollationForUTF8MB4)
	if err != nil {
		return "", err
	}
	return defaultCollation, nil
}

// GetDefaultCollation returns the default collation for charset and handle the default collation for UTF8MB4.
func GetDefaultCollation(sessVars *variable.SessionVars, cs string) (string, error) {
	coll, err := getDefaultCollationForUTF8MB4(sessVars, cs)
	if err != nil {
		return "", errors.Trace(err)
	}
	if coll != "" {
		return coll, nil
	}

	coll, err = charset.GetDefaultCollation(cs)
	if err != nil {
		return "", errors.Trace(err)
	}
	return coll, nil
}

// ResolveCharsetCollation will resolve the charset and collate by the order of parameters:
// * If any given ast.CharsetOpt is not empty, the resolved charset and collate will be returned.
// * If all ast.CharsetOpts are empty, the default charset and collate will be returned.
func ResolveCharsetCollation(sessVars *variable.SessionVars, charsetOpts ...ast.CharsetOpt) (chs string, coll string, err error) {
	for _, v := range charsetOpts {
		if v.Col != "" {
			collation, err := collate.GetCollationByName(v.Col)
			if err != nil {
				return "", "", errors.Trace(err)
			}
			if v.Chs != "" && collation.CharsetName != v.Chs {
				return "", "", charset.ErrCollationCharsetMismatch.GenWithStackByArgs(v.Col, v.Chs)
			}
			return collation.CharsetName, v.Col, nil
		}
		if v.Chs != "" {
			coll, err := GetDefaultCollation(sessVars, v.Chs)
			if err != nil {
				return "", "", errors.Trace(err)
			}
			return v.Chs, coll, nil
		}
	}
	chs, coll = charset.GetDefaultCharsetAndCollate()
	utf8mb4Coll, err := getDefaultCollationForUTF8MB4(sessVars, chs)
	if err != nil {
		return "", "", errors.Trace(err)
	}
	if utf8mb4Coll != "" {
		return chs, utf8mb4Coll, nil
	}
	return chs, coll, nil
}

// OverwriteCollationWithBinaryFlag is used to handle the case like
//
//	CREATE TABLE t (a VARCHAR(255) BINARY) CHARSET utf8 COLLATE utf8_general_ci;
//
// The 'BINARY' sets the column collation to *_bin according to the table charset.
func OverwriteCollationWithBinaryFlag(sessVars *variable.SessionVars, colDef *ast.ColumnDef, chs, coll string) (newChs string, newColl string) {
	ignoreBinFlag := colDef.Tp.GetCharset() != "" && (colDef.Tp.GetCollate() != "" || containsColumnOption(colDef, ast.ColumnOptionCollate))
	if ignoreBinFlag {
		return chs, coll
	}
	needOverwriteBinColl := types.IsString(colDef.Tp.GetType()) && mysql.HasBinaryFlag(colDef.Tp.GetFlag())
	if needOverwriteBinColl {
		newColl, err := GetDefaultCollation(sessVars, chs)
		if err != nil {
			return chs, coll
		}
		return chs, newColl
	}
	return chs, coll
}

func typesNeedCharset(tp byte) bool {
	switch tp {
	case mysql.TypeString, mysql.TypeVarchar, mysql.TypeVarString,
		mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob,
		mysql.TypeEnum, mysql.TypeSet:
		return true
	}
	return false
}

func setCharsetCollationFlenDecimal(tp *types.FieldType, colName, colCharset, colCollate string, sessVars *variable.SessionVars) error {
	var err error
	if typesNeedCharset(tp.GetType()) {
		tp.SetCharset(colCharset)
		tp.SetCollate(colCollate)
	} else {
		tp.SetCharset(charset.CharsetBin)
		tp.SetCollate(charset.CharsetBin)
	}

	// Use default value for flen or decimal when they are unspecified.
	defaultFlen, defaultDecimal := mysql.GetDefaultFieldLengthAndDecimal(tp.GetType())
	if tp.GetDecimal() == types.UnspecifiedLength {
		tp.SetDecimal(defaultDecimal)
	}
	if tp.GetFlen() == types.UnspecifiedLength {
		tp.SetFlen(defaultFlen)
		if mysql.HasUnsignedFlag(tp.GetFlag()) && tp.GetType() != mysql.TypeLonglong && mysql.IsIntegerType(tp.GetType()) {
			// Issue #4684: the flen of unsigned integer(except bigint) is 1 digit shorter than signed integer
			// because it has no prefix "+" or "-" character.
			tp.SetFlen(tp.GetFlen() - 1)
		}
	} else {
		// Adjust the field type for blob/text types if the flen is set.
		if err = adjustBlobTypesFlen(tp, colCharset); err != nil {
			return err
		}
	}
	return checkTooBigFieldLengthAndTryAutoConvert(tp, colName, sessVars)
}

func decodeEnumSetBinaryLiteralToUTF8(tp *types.FieldType, chs string) {
	if tp.GetType() != mysql.TypeEnum && tp.GetType() != mysql.TypeSet {
		return
	}
	enc := charset.FindEncoding(chs)
	for i, elem := range tp.GetElems() {
		if !tp.GetElemIsBinaryLit(i) {
			continue
		}
		s, err := enc.Transform(nil, hack.Slice(elem), charset.OpDecodeReplace)
		if err != nil {
			logutil.DDLLogger().Warn("decode enum binary literal to utf-8 failed", zap.Error(err))
		}
		tp.SetElem(i, string(hack.String(s)))
	}
	tp.CleanElemIsBinaryLit()
}

// buildColumnAndConstraint builds table.Column and ast.Constraint from the parameters.
// outPriKeyConstraint is the primary key constraint out of column definition. For example:
// `create table t1 (id int , age int, primary key(id));`
func buildColumnAndConstraint(
	ctx sessionctx.Context,
	offset int,
	colDef *ast.ColumnDef,
	outPriKeyConstraint *ast.Constraint,
	tblCharset string,
	tblCollate string,
) (*table.Column, []*ast.Constraint, error) {
	if colName := colDef.Name.Name.L; colName == model.ExtraHandleName.L {
		return nil, nil, dbterror.ErrWrongColumnName.GenWithStackByArgs(colName)
	}

	// specifiedCollate refers to the last collate specified in colDef.Options.
	chs, coll, err := getCharsetAndCollateInColumnDef(ctx.GetSessionVars(), colDef)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	chs, coll, err = ResolveCharsetCollation(ctx.GetSessionVars(),
		ast.CharsetOpt{Chs: chs, Col: coll},
		ast.CharsetOpt{Chs: tblCharset, Col: tblCollate},
	)
	chs, coll = OverwriteCollationWithBinaryFlag(ctx.GetSessionVars(), colDef, chs, coll)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	if err := setCharsetCollationFlenDecimal(colDef.Tp, colDef.Name.Name.O, chs, coll, ctx.GetSessionVars()); err != nil {
		return nil, nil, errors.Trace(err)
	}
	decodeEnumSetBinaryLiteralToUTF8(colDef.Tp, chs)
	col, cts, err := columnDefToCol(ctx, offset, colDef, outPriKeyConstraint)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return col, cts, nil
}

// checkColumnDefaultValue checks the default value of the column.
// In non-strict SQL mode, if the default value of the column is an empty string, the default value can be ignored.
// In strict SQL mode, TEXT/BLOB/JSON can't have not null default values.
// In NO_ZERO_DATE SQL mode, TIMESTAMP/DATE/DATETIME type can't have zero date like '0000-00-00' or '0000-00-00 00:00:00'.
func checkColumnDefaultValue(ctx exprctx.BuildContext, col *table.Column, value any) (bool, any, error) {
	hasDefaultValue := true
	if value != nil && (col.GetType() == mysql.TypeJSON ||
		col.GetType() == mysql.TypeTinyBlob || col.GetType() == mysql.TypeMediumBlob ||
		col.GetType() == mysql.TypeLongBlob || col.GetType() == mysql.TypeBlob) {
		// In non-strict SQL mode.
		if !ctx.GetEvalCtx().SQLMode().HasStrictMode() && value == "" {
			if col.GetType() == mysql.TypeBlob || col.GetType() == mysql.TypeLongBlob {
				// The TEXT/BLOB default value can be ignored.
				hasDefaultValue = false
			}
			// In non-strict SQL mode, if the column type is json and the default value is null, it is initialized to an empty array.
			if col.GetType() == mysql.TypeJSON {
				value = `null`
			}
			ctx.GetEvalCtx().AppendWarning(dbterror.ErrBlobCantHaveDefault.FastGenByArgs(col.Name.O))
			return hasDefaultValue, value, nil
		}
		// In strict SQL mode or default value is not an empty string.
		return hasDefaultValue, value, dbterror.ErrBlobCantHaveDefault.GenWithStackByArgs(col.Name.O)
	}
	if value != nil && ctx.GetEvalCtx().SQLMode().HasNoZeroDateMode() &&
		ctx.GetEvalCtx().SQLMode().HasStrictMode() && types.IsTypeTime(col.GetType()) {
		if vv, ok := value.(string); ok {
			timeValue, err := expression.GetTimeValue(ctx, vv, col.GetType(), col.GetDecimal(), nil)
			if err != nil {
				return hasDefaultValue, value, errors.Trace(err)
			}
			if timeValue.GetMysqlTime().CoreTime() == types.ZeroCoreTime {
				return hasDefaultValue, value, types.ErrInvalidDefault.GenWithStackByArgs(col.Name.O)
			}
		}
	}
	return hasDefaultValue, value, nil
}

func checkSequenceDefaultValue(col *table.Column) error {
	if mysql.IsIntegerType(col.GetType()) {
		return nil
	}
	return dbterror.ErrColumnTypeUnsupportedNextValue.GenWithStackByArgs(col.ColumnInfo.Name.O)
}

func convertTimestampDefaultValToUTC(ctx sessionctx.Context, defaultVal any, col *table.Column) (any, error) {
	if defaultVal == nil || col.GetType() != mysql.TypeTimestamp {
		return defaultVal, nil
	}
	if vv, ok := defaultVal.(string); ok {
		if vv != types.ZeroDatetimeStr && !strings.EqualFold(vv, ast.CurrentTimestamp) {
			t, err := types.ParseTime(ctx.GetSessionVars().StmtCtx.TypeCtx(), vv, col.GetType(), col.GetDecimal())
			if err != nil {
				return defaultVal, errors.Trace(err)
			}
			err = t.ConvertTimeZone(ctx.GetSessionVars().Location(), time.UTC)
			if err != nil {
				return defaultVal, errors.Trace(err)
			}
			defaultVal = t.String()
		}
	}
	return defaultVal, nil
}

// isExplicitTimeStamp is used to check if explicit_defaults_for_timestamp is on or off.
// Check out this link for more details.
// https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_explicit_defaults_for_timestamp
func isExplicitTimeStamp() bool {
	// TODO: implement the behavior as MySQL when explicit_defaults_for_timestamp = off, then this function could return false.
	return true
}

// processColumnFlags is used by columnDefToCol and processColumnOptions. It is intended to unify behaviors on `create/add` and `modify/change` statements. Check tidb#issue#19342.
func processColumnFlags(col *table.Column) {
	if col.FieldType.EvalType().IsStringKind() {
		if col.GetCharset() == charset.CharsetBin {
			col.AddFlag(mysql.BinaryFlag)
		} else {
			col.DelFlag(mysql.BinaryFlag)
		}
	}
	if col.GetType() == mysql.TypeBit {
		// For BIT field, it's charset is binary but does not have binary flag.
		col.DelFlag(mysql.BinaryFlag)
		col.AddFlag(mysql.UnsignedFlag)
	}
	if col.GetType() == mysql.TypeYear {
		// For Year field, it's charset is binary but does not have binary flag.
		col.DelFlag(mysql.BinaryFlag)
		col.AddFlag(mysql.ZerofillFlag)
	}

	// If you specify ZEROFILL for a numeric column, MySQL automatically adds the UNSIGNED attribute to the column.
	// See https://dev.mysql.com/doc/refman/5.7/en/numeric-type-overview.html for more details.
	// But some types like bit and year, won't show its unsigned flag in `show create table`.
	if mysql.HasZerofillFlag(col.GetFlag()) {
		col.AddFlag(mysql.UnsignedFlag)
	}
}

func adjustBlobTypesFlen(tp *types.FieldType, colCharset string) error {
	cs, err := charset.GetCharsetInfo(colCharset)
	// when we meet the unsupported charset, we do not adjust.
	if err != nil {
		return err
	}
	l := tp.GetFlen() * cs.Maxlen
	if tp.GetType() == mysql.TypeBlob {
		if l <= tinyBlobMaxLength {
			logutil.DDLLogger().Info(fmt.Sprintf("Automatically convert BLOB(%d) to TINYBLOB", tp.GetFlen()))
			tp.SetFlen(tinyBlobMaxLength)
			tp.SetType(mysql.TypeTinyBlob)
		} else if l <= blobMaxLength {
			tp.SetFlen(blobMaxLength)
		} else if l <= mediumBlobMaxLength {
			logutil.DDLLogger().Info(fmt.Sprintf("Automatically convert BLOB(%d) to MEDIUMBLOB", tp.GetFlen()))
			tp.SetFlen(mediumBlobMaxLength)
			tp.SetType(mysql.TypeMediumBlob)
		} else if l <= longBlobMaxLength {
			logutil.DDLLogger().Info(fmt.Sprintf("Automatically convert BLOB(%d) to LONGBLOB", tp.GetFlen()))
			tp.SetFlen(longBlobMaxLength)
			tp.SetType(mysql.TypeLongBlob)
		}
	}
	return nil
}

// columnDefToCol converts ColumnDef to Col and TableConstraints.
// outPriKeyConstraint is the primary key constraint out of column definition. such as: create table t1 (id int , age int, primary key(id));
func columnDefToCol(ctx sessionctx.Context, offset int, colDef *ast.ColumnDef, outPriKeyConstraint *ast.Constraint) (*table.Column, []*ast.Constraint, error) {
	var constraints = make([]*ast.Constraint, 0)
	col := table.ToColumn(&model.ColumnInfo{
		Offset:    offset,
		Name:      colDef.Name.Name,
		FieldType: *colDef.Tp,
		// TODO: remove this version field after there is no old version.
		Version: model.CurrLatestColumnInfoVersion,
	})

	if !isExplicitTimeStamp() {
		// Check and set TimestampFlag, OnUpdateNowFlag and NotNullFlag.
		if col.GetType() == mysql.TypeTimestamp {
			col.AddFlag(mysql.TimestampFlag | mysql.OnUpdateNowFlag | mysql.NotNullFlag)
		}
	}
	var err error
	setOnUpdateNow := false
	hasDefaultValue := false
	hasNullFlag := false
	if colDef.Options != nil {
		length := types.UnspecifiedLength

		keys := []*ast.IndexPartSpecification{
			{
				Column: colDef.Name,
				Length: length,
			},
		}

		var sb strings.Builder
		restoreFlags := format.RestoreStringSingleQuotes | format.RestoreKeyWordLowercase | format.RestoreNameBackQuotes |
			format.RestoreSpacesAroundBinaryOperation | format.RestoreWithoutSchemaName | format.RestoreWithoutTableName
		restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)

		for _, v := range colDef.Options {
			switch v.Tp {
			case ast.ColumnOptionNotNull:
				col.AddFlag(mysql.NotNullFlag)
			case ast.ColumnOptionNull:
				col.DelFlag(mysql.NotNullFlag)
				removeOnUpdateNowFlag(col)
				hasNullFlag = true
			case ast.ColumnOptionAutoIncrement:
				col.AddFlag(mysql.AutoIncrementFlag | mysql.NotNullFlag)
			case ast.ColumnOptionPrimaryKey:
				// Check PriKeyFlag first to avoid extra duplicate constraints.
				if col.GetFlag()&mysql.PriKeyFlag == 0 {
					constraint := &ast.Constraint{Tp: ast.ConstraintPrimaryKey, Keys: keys,
						Option: &ast.IndexOption{PrimaryKeyTp: v.PrimaryKeyTp}}
					constraints = append(constraints, constraint)
					col.AddFlag(mysql.PriKeyFlag)
					// Add NotNullFlag early so that processColumnFlags() can see it.
					col.AddFlag(mysql.NotNullFlag)
				}
			case ast.ColumnOptionUniqKey:
				// Check UniqueFlag first to avoid extra duplicate constraints.
				if col.GetFlag()&mysql.UniqueFlag == 0 {
					constraint := &ast.Constraint{Tp: ast.ConstraintUniqKey, Keys: keys}
					constraints = append(constraints, constraint)
					col.AddFlag(mysql.UniqueKeyFlag)
				}
			case ast.ColumnOptionDefaultValue:
				hasDefaultValue, err = SetDefaultValue(ctx, col, v)
				if err != nil {
					return nil, nil, errors.Trace(err)
				}
				removeOnUpdateNowFlag(col)
			case ast.ColumnOptionOnUpdate:
				// TODO: Support other time functions.
				if !(col.GetType() == mysql.TypeTimestamp || col.GetType() == mysql.TypeDatetime) {
					return nil, nil, dbterror.ErrInvalidOnUpdate.GenWithStackByArgs(col.Name)
				}
				if !expression.IsValidCurrentTimestampExpr(v.Expr, colDef.Tp) {
					return nil, nil, dbterror.ErrInvalidOnUpdate.GenWithStackByArgs(col.Name)
				}
				col.AddFlag(mysql.OnUpdateNowFlag)
				setOnUpdateNow = true
			case ast.ColumnOptionComment:
				err := setColumnComment(ctx, col, v)
				if err != nil {
					return nil, nil, errors.Trace(err)
				}
			case ast.ColumnOptionGenerated:
				sb.Reset()
				err = v.Expr.Restore(restoreCtx)
				if err != nil {
					return nil, nil, errors.Trace(err)
				}
				col.GeneratedExprString = sb.String()
				col.GeneratedStored = v.Stored
				_, dependColNames, err := findDependedColumnNames(model.NewCIStr(""), model.NewCIStr(""), colDef)
				if err != nil {
					return nil, nil, errors.Trace(err)
				}
				col.Dependences = dependColNames
			case ast.ColumnOptionCollate:
				if field_types.HasCharset(colDef.Tp) {
					col.FieldType.SetCollate(v.StrValue)
				}
			case ast.ColumnOptionFulltext:
				ctx.GetSessionVars().StmtCtx.AppendWarning(dbterror.ErrTableCantHandleFt.FastGenByArgs())
			case ast.ColumnOptionCheck:
				if !variable.EnableCheckConstraint.Load() {
					ctx.GetSessionVars().StmtCtx.AppendWarning(errCheckConstraintIsOff)
				} else {
					// Check the column CHECK constraint dependency lazily, after fill all the name.
					// Extract column constraint from column option.
					constraint := &ast.Constraint{
						Tp:           ast.ConstraintCheck,
						Expr:         v.Expr,
						Enforced:     v.Enforced,
						Name:         v.ConstraintName,
						InColumn:     true,
						InColumnName: colDef.Name.Name.O,
					}
					constraints = append(constraints, constraint)
				}
			}
		}
	}

	if err = processAndCheckDefaultValueAndColumn(ctx, col, outPriKeyConstraint, hasDefaultValue, setOnUpdateNow, hasNullFlag); err != nil {
		return nil, nil, errors.Trace(err)
	}
	return col, constraints, nil
}

func restoreFuncCall(expr *ast.FuncCallExpr) (string, error) {
	var sb strings.Builder
	restoreFlags := format.RestoreStringSingleQuotes | format.RestoreKeyWordLowercase | format.RestoreNameBackQuotes |
		format.RestoreSpacesAroundBinaryOperation
	restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)
	if err := expr.Restore(restoreCtx); err != nil {
		return "", err
	}
	return sb.String(), nil
}

// getFuncCallDefaultValue gets the default column value of function-call expression.
func getFuncCallDefaultValue(col *table.Column, option *ast.ColumnOption, expr *ast.FuncCallExpr) (any, bool, error) {
	switch expr.FnName.L {
	case ast.CurrentTimestamp, ast.CurrentDate: // CURRENT_TIMESTAMP() and CURRENT_DATE()
		tp, fsp := col.FieldType.GetType(), col.FieldType.GetDecimal()
		if tp == mysql.TypeTimestamp || tp == mysql.TypeDatetime {
			defaultFsp := 0
			if len(expr.Args) == 1 {
				if val := expr.Args[0].(*driver.ValueExpr); val != nil {
					defaultFsp = int(val.GetInt64())
				}
			}
			if defaultFsp != fsp {
				return nil, false, dbterror.ErrInvalidDefaultValue.GenWithStackByArgs(col.Name.O)
			}
		}
		return nil, false, nil
	case ast.NextVal:
		// handle default next value of sequence. (keep the expr string)
		str, err := getSequenceDefaultValue(option)
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		return str, true, nil
	case ast.Rand, ast.UUID, ast.UUIDToBin: // RAND(), UUID() and UUID_TO_BIN()
		if err := expression.VerifyArgsWrapper(expr.FnName.L, len(expr.Args)); err != nil {
			return nil, false, errors.Trace(err)
		}
		str, err := restoreFuncCall(expr)
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		col.DefaultIsExpr = true
		return str, false, nil
	case ast.DateFormat: // DATE_FORMAT()
		if err := expression.VerifyArgsWrapper(expr.FnName.L, len(expr.Args)); err != nil {
			return nil, false, errors.Trace(err)
		}
		// Support DATE_FORMAT(NOW(),'%Y-%m'), DATE_FORMAT(NOW(),'%Y-%m-%d'),
		// DATE_FORMAT(NOW(),'%Y-%m-%d %H.%i.%s'), DATE_FORMAT(NOW(),'%Y-%m-%d %H:%i:%s').
		nowFunc, ok := expr.Args[0].(*ast.FuncCallExpr)
		if ok && nowFunc.FnName.L == ast.Now {
			if err := expression.VerifyArgsWrapper(nowFunc.FnName.L, len(nowFunc.Args)); err != nil {
				return nil, false, errors.Trace(err)
			}
			valExpr, isValue := expr.Args[1].(ast.ValueExpr)
			if !isValue || (valExpr.GetString() != "%Y-%m" && valExpr.GetString() != "%Y-%m-%d" &&
				valExpr.GetString() != "%Y-%m-%d %H.%i.%s" && valExpr.GetString() != "%Y-%m-%d %H:%i:%s") {
				return nil, false, dbterror.ErrDefValGeneratedNamedFunctionIsNotAllowed.GenWithStackByArgs(col.Name.String(), valExpr)
			}
			str, err := restoreFuncCall(expr)
			if err != nil {
				return nil, false, errors.Trace(err)
			}
			col.DefaultIsExpr = true
			return str, false, nil
		}
		return nil, false, dbterror.ErrDefValGeneratedNamedFunctionIsNotAllowed.GenWithStackByArgs(col.Name.String(),
			fmt.Sprintf("%s with disallowed args", expr.FnName.String()))
	case ast.Replace:
		if err := expression.VerifyArgsWrapper(expr.FnName.L, len(expr.Args)); err != nil {
			return nil, false, errors.Trace(err)
		}
		funcCall := expr.Args[0]
		// Support REPLACE(CONVERT(UPPER(UUID()) USING UTF8MB4), '-', ''))
		if convertFunc, ok := funcCall.(*ast.FuncCallExpr); ok && convertFunc.FnName.L == ast.Convert {
			if err := expression.VerifyArgsWrapper(convertFunc.FnName.L, len(convertFunc.Args)); err != nil {
				return nil, false, errors.Trace(err)
			}
			funcCall = convertFunc.Args[0]
		}
		// Support REPLACE(UPPER(UUID()), '-', '').
		if upperFunc, ok := funcCall.(*ast.FuncCallExpr); ok && upperFunc.FnName.L == ast.Upper {
			if err := expression.VerifyArgsWrapper(upperFunc.FnName.L, len(upperFunc.Args)); err != nil {
				return nil, false, errors.Trace(err)
			}
			if uuidFunc, ok := upperFunc.Args[0].(*ast.FuncCallExpr); ok && uuidFunc.FnName.L == ast.UUID {
				if err := expression.VerifyArgsWrapper(uuidFunc.FnName.L, len(uuidFunc.Args)); err != nil {
					return nil, false, errors.Trace(err)
				}
				str, err := restoreFuncCall(expr)
				if err != nil {
					return nil, false, errors.Trace(err)
				}
				col.DefaultIsExpr = true
				return str, false, nil
			}
		}
		return nil, false, dbterror.ErrDefValGeneratedNamedFunctionIsNotAllowed.GenWithStackByArgs(col.Name.String(),
			fmt.Sprintf("%s with disallowed args", expr.FnName.String()))
	case ast.Upper:
		if err := expression.VerifyArgsWrapper(expr.FnName.L, len(expr.Args)); err != nil {
			return nil, false, errors.Trace(err)
		}
		// Support UPPER(SUBSTRING_INDEX(USER(), '@', 1)).
		if substringIndexFunc, ok := expr.Args[0].(*ast.FuncCallExpr); ok && substringIndexFunc.FnName.L == ast.SubstringIndex {
			if err := expression.VerifyArgsWrapper(substringIndexFunc.FnName.L, len(substringIndexFunc.Args)); err != nil {
				return nil, false, errors.Trace(err)
			}
			if userFunc, ok := substringIndexFunc.Args[0].(*ast.FuncCallExpr); ok && userFunc.FnName.L == ast.User {
				if err := expression.VerifyArgsWrapper(userFunc.FnName.L, len(userFunc.Args)); err != nil {
					return nil, false, errors.Trace(err)
				}
				valExpr, isValue := substringIndexFunc.Args[1].(ast.ValueExpr)
				if !isValue || valExpr.GetString() != "@" {
					return nil, false, dbterror.ErrDefValGeneratedNamedFunctionIsNotAllowed.GenWithStackByArgs(col.Name.String(), valExpr)
				}
				str, err := restoreFuncCall(expr)
				if err != nil {
					return nil, false, errors.Trace(err)
				}
				col.DefaultIsExpr = true
				return str, false, nil
			}
		}
		return nil, false, dbterror.ErrDefValGeneratedNamedFunctionIsNotAllowed.GenWithStackByArgs(col.Name.String(),
			fmt.Sprintf("%s with disallowed args", expr.FnName.String()))
	case ast.StrToDate: // STR_TO_DATE()
		if err := expression.VerifyArgsWrapper(expr.FnName.L, len(expr.Args)); err != nil {
			return nil, false, errors.Trace(err)
		}
		// Support STR_TO_DATE('1980-01-01', '%Y-%m-%d').
		if _, ok1 := expr.Args[0].(ast.ValueExpr); ok1 {
			if _, ok2 := expr.Args[1].(ast.ValueExpr); ok2 {
				str, err := restoreFuncCall(expr)
				if err != nil {
					return nil, false, errors.Trace(err)
				}
				col.DefaultIsExpr = true
				return str, false, nil
			}
		}
		return nil, false, dbterror.ErrDefValGeneratedNamedFunctionIsNotAllowed.GenWithStackByArgs(col.Name.String(),
			fmt.Sprintf("%s with disallowed args", expr.FnName.String()))
	default:
		return nil, false, dbterror.ErrDefValGeneratedNamedFunctionIsNotAllowed.GenWithStackByArgs(col.Name.String(), expr.FnName.String())
	}
}

// getDefaultValue will get the default value for column.
// 1: get the expr restored string for the column which uses sequence next value as default value.
// 2: get specific default value for the other column.
func getDefaultValue(ctx exprctx.BuildContext, col *table.Column, option *ast.ColumnOption) (any, bool, error) {
	// handle default value with function call
	tp, fsp := col.FieldType.GetType(), col.FieldType.GetDecimal()
	if x, ok := option.Expr.(*ast.FuncCallExpr); ok {
		val, isSeqExpr, err := getFuncCallDefaultValue(col, option, x)
		if val != nil || isSeqExpr || err != nil {
			return val, isSeqExpr, err
		}
		// If the function call is ast.CurrentTimestamp, it needs to be continuously processed.
	}

	if tp == mysql.TypeTimestamp || tp == mysql.TypeDatetime || tp == mysql.TypeDate {
		vd, err := expression.GetTimeValue(ctx, option.Expr, tp, fsp, nil)
		value := vd.GetValue()
		if err != nil {
			return nil, false, dbterror.ErrInvalidDefaultValue.GenWithStackByArgs(col.Name.O)
		}

		// Value is nil means `default null`.
		if value == nil {
			return nil, false, nil
		}

		// If value is types.Time, convert it to string.
		if vv, ok := value.(types.Time); ok {
			return vv.String(), false, nil
		}

		return value, false, nil
	}

	// evaluate the non-function-call expr to a certain value.
	v, err := expression.EvalSimpleAst(ctx, option.Expr)
	if err != nil {
		return nil, false, errors.Trace(err)
	}

	if v.IsNull() {
		return nil, false, nil
	}

	if v.Kind() == types.KindBinaryLiteral || v.Kind() == types.KindMysqlBit {
		if types.IsTypeBlob(tp) || tp == mysql.TypeJSON {
			// BLOB/TEXT/JSON column cannot have a default value.
			// Skip the unnecessary decode procedure.
			return v.GetString(), false, err
		}
		if tp == mysql.TypeBit || tp == mysql.TypeString || tp == mysql.TypeVarchar ||
			tp == mysql.TypeVarString || tp == mysql.TypeEnum || tp == mysql.TypeSet {
			// For BinaryLiteral or bit fields, we decode the default value to utf8 string.
			str, err := v.GetBinaryStringDecoded(types.StrictFlags, col.GetCharset())
			if err != nil {
				// Overwrite the decoding error with invalid default value error.
				err = dbterror.ErrInvalidDefaultValue.GenWithStackByArgs(col.Name.O)
			}
			return str, false, err
		}
		// For other kind of fields (e.g. INT), we supply its integer as string value.
		value, err := v.GetBinaryLiteral().ToInt(ctx.GetEvalCtx().TypeCtx())
		if err != nil {
			return nil, false, err
		}
		return strconv.FormatUint(value, 10), false, nil
	}

	switch tp {
	case mysql.TypeSet:
		val, err := getSetDefaultValue(v, col)
		return val, false, err
	case mysql.TypeEnum:
		val, err := getEnumDefaultValue(v, col)
		return val, false, err
	case mysql.TypeDuration, mysql.TypeDate:
		if v, err = v.ConvertTo(ctx.GetEvalCtx().TypeCtx(), &col.FieldType); err != nil {
			return "", false, errors.Trace(err)
		}
	case mysql.TypeBit:
		if v.Kind() == types.KindInt64 || v.Kind() == types.KindUint64 {
			// For BIT fields, convert int into BinaryLiteral.
			return types.NewBinaryLiteralFromUint(v.GetUint64(), -1).ToString(), false, nil
		}
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeFloat, mysql.TypeDouble:
		// For these types, convert it to standard format firstly.
		// like integer fields, convert it into integer string literals. like convert "1.25" into "1" and "2.8" into "3".
		// if raise a error, we will use original expression. We will handle it in check phase
		if temp, err := v.ConvertTo(ctx.GetEvalCtx().TypeCtx(), &col.FieldType); err == nil {
			v = temp
		}
	}

	val, err := v.ToString()
	return val, false, err
}

func getSequenceDefaultValue(c *ast.ColumnOption) (expr string, err error) {
	var sb strings.Builder
	restoreFlags := format.RestoreStringSingleQuotes | format.RestoreKeyWordLowercase | format.RestoreNameBackQuotes |
		format.RestoreSpacesAroundBinaryOperation
	restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)
	if err := c.Expr.Restore(restoreCtx); err != nil {
		return "", err
	}
	return sb.String(), nil
}

// getSetDefaultValue gets the default value for the set type. See https://dev.mysql.com/doc/refman/5.7/en/set.html.
func getSetDefaultValue(v types.Datum, col *table.Column) (string, error) {
	if v.Kind() == types.KindInt64 {
		setCnt := len(col.GetElems())
		maxLimit := int64(1<<uint(setCnt) - 1)
		val := v.GetInt64()
		if val < 1 || val > maxLimit {
			return "", dbterror.ErrInvalidDefaultValue.GenWithStackByArgs(col.Name.O)
		}
		setVal, err := types.ParseSetValue(col.GetElems(), uint64(val))
		if err != nil {
			return "", errors.Trace(err)
		}
		v.SetMysqlSet(setVal, col.GetCollate())
		return v.ToString()
	}

	str, err := v.ToString()
	if err != nil {
		return "", errors.Trace(err)
	}
	if str == "" {
		return str, nil
	}
	setVal, err := types.ParseSetName(col.GetElems(), str, col.GetCollate())
	if err != nil {
		return "", dbterror.ErrInvalidDefaultValue.GenWithStackByArgs(col.Name.O)
	}
	v.SetMysqlSet(setVal, col.GetCollate())

	return v.ToString()
}

// getEnumDefaultValue gets the default value for the enum type. See https://dev.mysql.com/doc/refman/5.7/en/enum.html.
func getEnumDefaultValue(v types.Datum, col *table.Column) (string, error) {
	if v.Kind() == types.KindInt64 {
		val := v.GetInt64()
		if val < 1 || val > int64(len(col.GetElems())) {
			return "", dbterror.ErrInvalidDefaultValue.GenWithStackByArgs(col.Name.O)
		}
		enumVal, err := types.ParseEnumValue(col.GetElems(), uint64(val))
		if err != nil {
			return "", errors.Trace(err)
		}
		v.SetMysqlEnum(enumVal, col.GetCollate())
		return v.ToString()
	}
	str, err := v.ToString()
	if err != nil {
		return "", errors.Trace(err)
	}
	// Ref: https://dev.mysql.com/doc/refman/8.0/en/enum.html
	// Trailing spaces are automatically deleted from ENUM member values in the table definition when a table is created.
	str = strings.TrimRight(str, " ")
	enumVal, err := types.ParseEnumName(col.GetElems(), str, col.GetCollate())
	if err != nil {
		return "", dbterror.ErrInvalidDefaultValue.GenWithStackByArgs(col.Name.O)
	}
	v.SetMysqlEnum(enumVal, col.GetCollate())

	return v.ToString()
}

func removeOnUpdateNowFlag(c *table.Column) {
	// For timestamp Col, if it is set null or default value,
	// OnUpdateNowFlag should be removed.
	if mysql.HasTimestampFlag(c.GetFlag()) {
		c.DelFlag(mysql.OnUpdateNowFlag)
	}
}

func processDefaultValue(c *table.Column, hasDefaultValue bool, setOnUpdateNow bool) {
	setTimestampDefaultValue(c, hasDefaultValue, setOnUpdateNow)

	setYearDefaultValue(c, hasDefaultValue)

	// Set `NoDefaultValueFlag` if this field doesn't have a default value and
	// it is `not null` and not an `AUTO_INCREMENT` field or `TIMESTAMP` field.
	setNoDefaultValueFlag(c, hasDefaultValue)
}

func setYearDefaultValue(c *table.Column, hasDefaultValue bool) {
	if hasDefaultValue {
		return
	}

	if c.GetType() == mysql.TypeYear && mysql.HasNotNullFlag(c.GetFlag()) {
		if err := c.SetDefaultValue("0000"); err != nil {
			logutil.DDLLogger().Error("set default value failed", zap.Error(err))
		}
	}
}

func setTimestampDefaultValue(c *table.Column, hasDefaultValue bool, setOnUpdateNow bool) {
	if hasDefaultValue {
		return
	}

	// For timestamp Col, if is not set default value or not set null, use current timestamp.
	if mysql.HasTimestampFlag(c.GetFlag()) && mysql.HasNotNullFlag(c.GetFlag()) {
		if setOnUpdateNow {
			if err := c.SetDefaultValue(types.ZeroDatetimeStr); err != nil {
				logutil.DDLLogger().Error("set default value failed", zap.Error(err))
			}
		} else {
			if err := c.SetDefaultValue(strings.ToUpper(ast.CurrentTimestamp)); err != nil {
				logutil.DDLLogger().Error("set default value failed", zap.Error(err))
			}
		}
	}
}

func setNoDefaultValueFlag(c *table.Column, hasDefaultValue bool) {
	if hasDefaultValue {
		return
	}

	if !mysql.HasNotNullFlag(c.GetFlag()) {
		return
	}

	// Check if it is an `AUTO_INCREMENT` field or `TIMESTAMP` field.
	if !mysql.HasAutoIncrementFlag(c.GetFlag()) && !mysql.HasTimestampFlag(c.GetFlag()) {
		c.AddFlag(mysql.NoDefaultValueFlag)
	}
}

func checkDefaultValue(ctx exprctx.BuildContext, c *table.Column, hasDefaultValue bool) (err error) {
	if !hasDefaultValue {
		return nil
	}

	if c.GetDefaultValue() != nil {
		if c.DefaultIsExpr {
			if mysql.HasAutoIncrementFlag(c.GetFlag()) {
				return types.ErrInvalidDefault.GenWithStackByArgs(c.Name)
			}
			return nil
		}
		_, err = table.GetColDefaultValue(
			exprctx.CtxWithHandleTruncateErrLevel(ctx, errctx.LevelError),
			c.ToInfo(),
		)
		if err != nil {
			return types.ErrInvalidDefault.GenWithStackByArgs(c.Name)
		}
		return nil
	}
	// Primary key default null is invalid.
	if mysql.HasPriKeyFlag(c.GetFlag()) {
		return dbterror.ErrPrimaryCantHaveNull
	}

	// Set not null but default null is invalid.
	if mysql.HasNotNullFlag(c.GetFlag()) {
		return types.ErrInvalidDefault.GenWithStackByArgs(c.Name)
	}

	return nil
}

// checkPriKeyConstraint check all parts of a PRIMARY KEY must be NOT NULL
func checkPriKeyConstraint(col *table.Column, hasDefaultValue, hasNullFlag bool, outPriKeyConstraint *ast.Constraint) error {
	// Primary key should not be null.
	if mysql.HasPriKeyFlag(col.GetFlag()) && hasDefaultValue && col.GetDefaultValue() == nil {
		return types.ErrInvalidDefault.GenWithStackByArgs(col.Name)
	}
	// Set primary key flag for outer primary key constraint.
	// Such as: create table t1 (id int , age int, primary key(id))
	if !mysql.HasPriKeyFlag(col.GetFlag()) && outPriKeyConstraint != nil {
		for _, key := range outPriKeyConstraint.Keys {
			if key.Expr == nil && key.Column.Name.L != col.Name.L {
				continue
			}
			col.AddFlag(mysql.PriKeyFlag)
			break
		}
	}
	// Primary key should not be null.
	if mysql.HasPriKeyFlag(col.GetFlag()) && hasNullFlag {
		return dbterror.ErrPrimaryCantHaveNull
	}
	return nil
}

func checkColumnValueConstraint(col *table.Column, collation string) error {
	if col.GetType() != mysql.TypeEnum && col.GetType() != mysql.TypeSet {
		return nil
	}
	valueMap := make(map[string]bool, len(col.GetElems()))
	ctor := collate.GetCollator(collation)
	enumLengthLimit := config.GetGlobalConfig().EnableEnumLengthLimit
	desc, err := charset.GetCharsetInfo(col.GetCharset())
	if err != nil {
		return errors.Trace(err)
	}
	for i := range col.GetElems() {
		val := string(ctor.Key(col.GetElems()[i]))
		// According to MySQL 8.0 Refman:
		// The maximum supported length of an individual ENUM element is M <= 255 and (M x w) <= 1020,
		// where M is the element literal length and w is the number of bytes required for the maximum-length character in the character set.
		// See https://dev.mysql.com/doc/refman/8.0/en/string-type-syntax.html for more details.
		if enumLengthLimit && (len(val) > 255 || len(val)*desc.Maxlen > 1020) {
			return dbterror.ErrTooLongValueForType.GenWithStackByArgs(col.Name)
		}
		if _, ok := valueMap[val]; ok {
			tpStr := "ENUM"
			if col.GetType() == mysql.TypeSet {
				tpStr = "SET"
			}
			return types.ErrDuplicatedValueInType.GenWithStackByArgs(col.Name, col.GetElems()[i], tpStr)
		}
		valueMap[val] = true
	}
	return nil
}

func checkDuplicateColumn(cols []*model.ColumnInfo) error {
	colNames := set.StringSet{}
	for _, col := range cols {
		colName := col.Name
		if colNames.Exist(colName.L) {
			return infoschema.ErrColumnExists.GenWithStackByArgs(colName.O)
		}
		colNames.Insert(colName.L)
	}
	return nil
}

func containsColumnOption(colDef *ast.ColumnDef, opTp ast.ColumnOptionType) bool {
	for _, option := range colDef.Options {
		if option.Tp == opTp {
			return true
		}
	}
	return false
}

// IsAutoRandomColumnID returns true if the given column ID belongs to an auto_random column.
func IsAutoRandomColumnID(tblInfo *model.TableInfo, colID int64) bool {
	if !tblInfo.ContainsAutoRandomBits() {
		return false
	}
	if tblInfo.PKIsHandle {
		return tblInfo.GetPkColInfo().ID == colID
	} else if tblInfo.IsCommonHandle {
		pk := tables.FindPrimaryIndex(tblInfo)
		if pk == nil {
			return false
		}
		offset := pk.Columns[0].Offset
		return tblInfo.Columns[offset].ID == colID
	}
	return false
}

func checkGeneratedColumn(ctx sessionctx.Context, schemaName model.CIStr, tableName model.CIStr, colDefs []*ast.ColumnDef) error {
	var colName2Generation = make(map[string]columnGenerationInDDL, len(colDefs))
	var exists bool
	var autoIncrementColumn string
	for i, colDef := range colDefs {
		for _, option := range colDef.Options {
			if option.Tp == ast.ColumnOptionGenerated {
				if err := checkIllegalFn4Generated(colDef.Name.Name.L, typeColumn, option.Expr); err != nil {
					return errors.Trace(err)
				}
			}
		}
		if containsColumnOption(colDef, ast.ColumnOptionAutoIncrement) {
			exists, autoIncrementColumn = true, colDef.Name.Name.L
		}
		generated, depCols, err := findDependedColumnNames(schemaName, tableName, colDef)
		if err != nil {
			return errors.Trace(err)
		}
		if !generated {
			colName2Generation[colDef.Name.Name.L] = columnGenerationInDDL{
				position:  i,
				generated: false,
			}
		} else {
			colName2Generation[colDef.Name.Name.L] = columnGenerationInDDL{
				position:    i,
				generated:   true,
				dependences: depCols,
			}
		}
	}

	// Check whether the generated column refers to any auto-increment columns
	if exists {
		if !ctx.GetSessionVars().EnableAutoIncrementInGenerated {
			for colName, generated := range colName2Generation {
				if _, found := generated.dependences[autoIncrementColumn]; found {
					return dbterror.ErrGeneratedColumnRefAutoInc.GenWithStackByArgs(colName)
				}
			}
		}
	}

	for _, colDef := range colDefs {
		colName := colDef.Name.Name.L
		if err := verifyColumnGeneration(colName2Generation, colName); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func checkTooLongColumns(cols []*model.ColumnInfo) error {
	for _, col := range cols {
		if err := checkTooLongColumn(col.Name); err != nil {
			return err
		}
	}
	return nil
}

func checkTooManyColumns(colDefs []*model.ColumnInfo) error {
	if uint32(len(colDefs)) > atomic.LoadUint32(&config.GetGlobalConfig().TableColumnCountLimit) {
		return dbterror.ErrTooManyFields
	}
	return nil
}

func checkTooManyIndexes(idxDefs []*model.IndexInfo) error {
	if len(idxDefs) > config.GetGlobalConfig().IndexLimit {
		return dbterror.ErrTooManyKeys.GenWithStackByArgs(config.GetGlobalConfig().IndexLimit)
	}
	return nil
}

// checkColumnsAttributes checks attributes for multiple columns.
func checkColumnsAttributes(colDefs []*model.ColumnInfo) error {
	for _, colDef := range colDefs {
		if err := checkColumnAttributes(colDef.Name.O, &colDef.FieldType); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func checkColumnFieldLength(col *table.Column) error {
	if col.GetType() == mysql.TypeVarchar {
		if err := types.IsVarcharTooBigFieldLength(col.GetFlen(), col.Name.O, col.GetCharset()); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

// checkColumnAttributes check attributes for single column.
func checkColumnAttributes(colName string, tp *types.FieldType) error {
	switch tp.GetType() {
	case mysql.TypeNewDecimal, mysql.TypeDouble, mysql.TypeFloat:
		if tp.GetFlen() < tp.GetDecimal() {
			return types.ErrMBiggerThanD.GenWithStackByArgs(colName)
		}
	case mysql.TypeDatetime, mysql.TypeDuration, mysql.TypeTimestamp:
		if tp.GetDecimal() != types.UnspecifiedFsp && (tp.GetDecimal() < types.MinFsp || tp.GetDecimal() > types.MaxFsp) {
			return types.ErrTooBigPrecision.GenWithStackByArgs(tp.GetDecimal(), colName, types.MaxFsp)
		}
	}
	return nil
}

func checkDuplicateConstraint(namesMap map[string]bool, name string, constraintType ast.ConstraintType) error {
	if name == "" {
		return nil
	}
	nameLower := strings.ToLower(name)
	if namesMap[nameLower] {
		switch constraintType {
		case ast.ConstraintForeignKey:
			return dbterror.ErrFkDupName.GenWithStackByArgs(name)
		case ast.ConstraintCheck:
			return dbterror.ErrCheckConstraintDupName.GenWithStackByArgs(name)
		default:
			return dbterror.ErrDupKeyName.GenWithStackByArgs(name)
		}
	}
	namesMap[nameLower] = true
	return nil
}

func setEmptyCheckConstraintName(tableLowerName string, namesMap map[string]bool, constrs []*ast.Constraint) {
	cnt := 1
	constraintPrefix := tableLowerName + "_chk_"
	for _, constr := range constrs {
		if constr.Name == "" {
			constrName := fmt.Sprintf("%s%d", constraintPrefix, cnt)
			for {
				// loop until find constrName that haven't been used.
				if !namesMap[constrName] {
					namesMap[constrName] = true
					break
				}
				cnt++
				constrName = fmt.Sprintf("%s%d", constraintPrefix, cnt)
			}
			constr.Name = constrName
		}
	}
}

func setEmptyConstraintName(namesMap map[string]bool, constr *ast.Constraint) {
	if constr.Name == "" && len(constr.Keys) > 0 {
		var colName string
		for _, keyPart := range constr.Keys {
			if keyPart.Expr != nil {
				colName = "expression_index"
			}
		}
		if colName == "" {
			colName = constr.Keys[0].Column.Name.O
		}
		constrName := colName
		i := 2
		if strings.EqualFold(constrName, mysql.PrimaryKeyName) {
			constrName = fmt.Sprintf("%s_%d", constrName, 2)
			i = 3
		}
		for namesMap[constrName] {
			// We loop forever until we find constrName that haven't been used.
			constrName = fmt.Sprintf("%s_%d", colName, i)
			i++
		}
		constr.Name = constrName
		namesMap[constrName] = true
	}
}

func checkConstraintNames(tableName model.CIStr, constraints []*ast.Constraint) error {
	constrNames := map[string]bool{}
	fkNames := map[string]bool{}

	// Check not empty constraint name whether is duplicated.
	for _, constr := range constraints {
		if constr.Tp == ast.ConstraintForeignKey {
			err := checkDuplicateConstraint(fkNames, constr.Name, constr.Tp)
			if err != nil {
				return errors.Trace(err)
			}
		} else {
			err := checkDuplicateConstraint(constrNames, constr.Name, constr.Tp)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}

	// Set empty constraint names.
	checkConstraints := make([]*ast.Constraint, 0, len(constraints))
	for _, constr := range constraints {
		if constr.Tp != ast.ConstraintForeignKey {
			setEmptyConstraintName(constrNames, constr)
		}
		if constr.Tp == ast.ConstraintCheck {
			checkConstraints = append(checkConstraints, constr)
		}
	}
	// Set check constraint name under its order.
	if len(checkConstraints) > 0 {
		setEmptyCheckConstraintName(tableName.L, constrNames, checkConstraints)
	}
	return nil
}

// checkInvisibleIndexOnPK check if primary key is invisible index.
// Note: PKIsHandle == true means the table already has a visible primary key,
// we do not need do a check for this case and return directly,
// because whether primary key is invisible has been check when creating table.
func checkInvisibleIndexOnPK(tblInfo *model.TableInfo) error {
	if tblInfo.PKIsHandle {
		return nil
	}
	pk := tblInfo.GetPrimaryKey()
	if pk != nil && pk.Invisible {
		return dbterror.ErrPKIndexCantBeInvisible
	}
	return nil
}

func setTableAutoRandomBits(ctx sessionctx.Context, tbInfo *model.TableInfo, colDefs []*ast.ColumnDef) error {
	for _, col := range colDefs {
		if containsColumnOption(col, ast.ColumnOptionAutoRandom) {
			if col.Tp.GetType() != mysql.TypeLonglong {
				return dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(
					fmt.Sprintf(autoid.AutoRandomOnNonBigIntColumn, types.TypeStr(col.Tp.GetType())))
			}
			switch {
			case tbInfo.PKIsHandle:
				if tbInfo.GetPkName().L != col.Name.Name.L {
					errMsg := fmt.Sprintf(autoid.AutoRandomMustFirstColumnInPK, col.Name.Name.O)
					return dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(errMsg)
				}
			case tbInfo.IsCommonHandle:
				pk := tables.FindPrimaryIndex(tbInfo)
				if pk == nil {
					return dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomNoClusteredPKErrMsg)
				}
				if col.Name.Name.L != pk.Columns[0].Name.L {
					errMsg := fmt.Sprintf(autoid.AutoRandomMustFirstColumnInPK, col.Name.Name.O)
					return dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(errMsg)
				}
			default:
				return dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomNoClusteredPKErrMsg)
			}

			if containsColumnOption(col, ast.ColumnOptionAutoIncrement) {
				return dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomIncompatibleWithAutoIncErrMsg)
			}
			if containsColumnOption(col, ast.ColumnOptionDefaultValue) {
				return dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomIncompatibleWithDefaultValueErrMsg)
			}

			shardBits, rangeBits, err := extractAutoRandomBitsFromColDef(col)
			if err != nil {
				return errors.Trace(err)
			}
			tbInfo.AutoRandomBits = shardBits
			tbInfo.AutoRandomRangeBits = rangeBits

			shardFmt := autoid.NewShardIDFormat(col.Tp, shardBits, rangeBits)
			if shardFmt.IncrementalBits < autoid.AutoRandomIncBitsMin {
				return dbterror.ErrInvalidAutoRandom.FastGenByArgs(autoid.AutoRandomIncrementalBitsTooSmall)
			}
			msg := fmt.Sprintf(autoid.AutoRandomAvailableAllocTimesNote, shardFmt.IncrementalBitsCapacity())
			ctx.GetSessionVars().StmtCtx.AppendNote(errors.NewNoStackError(msg))
		}
	}
	return nil
}

func extractAutoRandomBitsFromColDef(colDef *ast.ColumnDef) (shardBits, rangeBits uint64, err error) {
	for _, op := range colDef.Options {
		if op.Tp == ast.ColumnOptionAutoRandom {
			shardBits, err = autoid.AutoRandomShardBitsNormalize(op.AutoRandOpt.ShardBits, colDef.Name.Name.O)
			if err != nil {
				return 0, 0, err
			}
			rangeBits, err = autoid.AutoRandomRangeBitsNormalize(op.AutoRandOpt.RangeBits)
			if err != nil {
				return 0, 0, err
			}
			return shardBits, rangeBits, nil
		}
	}
	return 0, 0, nil
}

// BuildTableInfo creates a TableInfo.
func BuildTableInfo(
	ctx sessionctx.Context,
	tableName model.CIStr,
	cols []*table.Column,
	constraints []*ast.Constraint,
	charset string,
	collate string,
) (tbInfo *model.TableInfo, err error) {
	tbInfo = &model.TableInfo{
		Name:    tableName,
		Version: model.CurrLatestTableInfoVersion,
		Charset: charset,
		Collate: collate,
	}
	tblColumns := make([]*table.Column, 0, len(cols))
	existedColsMap := make(map[string]struct{}, len(cols))
	for _, v := range cols {
		v.ID = AllocateColumnID(tbInfo)
		tbInfo.Columns = append(tbInfo.Columns, v.ToInfo())
		tblColumns = append(tblColumns, table.ToColumn(v.ToInfo()))
		existedColsMap[v.Name.L] = struct{}{}
	}
	foreignKeyID := tbInfo.MaxForeignKeyID
	for _, constr := range constraints {
		// Build hidden columns if necessary.
		hiddenCols, err := buildHiddenColumnInfoWithCheck(ctx, constr.Keys, model.NewCIStr(constr.Name), tbInfo, tblColumns)
		if err != nil {
			return nil, err
		}
		for _, hiddenCol := range hiddenCols {
			hiddenCol.State = model.StatePublic
			hiddenCol.ID = AllocateColumnID(tbInfo)
			hiddenCol.Offset = len(tbInfo.Columns)
			tbInfo.Columns = append(tbInfo.Columns, hiddenCol)
			tblColumns = append(tblColumns, table.ToColumn(hiddenCol))
		}
		// Check clustered on non-primary key.
		if constr.Option != nil && constr.Option.PrimaryKeyTp != model.PrimaryKeyTypeDefault &&
			constr.Tp != ast.ConstraintPrimaryKey {
			return nil, dbterror.ErrUnsupportedClusteredSecondaryKey
		}
		if constr.Tp == ast.ConstraintForeignKey {
			var fkName model.CIStr
			foreignKeyID++
			if constr.Name != "" {
				fkName = model.NewCIStr(constr.Name)
			} else {
				fkName = model.NewCIStr(fmt.Sprintf("fk_%d", foreignKeyID))
			}
			if model.FindFKInfoByName(tbInfo.ForeignKeys, fkName.L) != nil {
				return nil, infoschema.ErrCannotAddForeign
			}
			fk, err := buildFKInfo(fkName, constr.Keys, constr.Refer, cols)
			if err != nil {
				return nil, err
			}
			fk.State = model.StatePublic

			tbInfo.ForeignKeys = append(tbInfo.ForeignKeys, fk)
			continue
		}
		if constr.Tp == ast.ConstraintPrimaryKey {
			lastCol, err := CheckPKOnGeneratedColumn(tbInfo, constr.Keys)
			if err != nil {
				return nil, err
			}
			isSingleIntPK := isSingleIntPK(constr, lastCol)
			if ShouldBuildClusteredIndex(ctx, constr.Option, isSingleIntPK) {
				if isSingleIntPK {
					tbInfo.PKIsHandle = true
				} else {
					tbInfo.IsCommonHandle = true
					tbInfo.CommonHandleVersion = 1
				}
			}
			if tbInfo.HasClusteredIndex() {
				// Primary key cannot be invisible.
				if constr.Option != nil && constr.Option.Visibility == ast.IndexVisibilityInvisible {
					return nil, dbterror.ErrPKIndexCantBeInvisible
				}
			}
			if tbInfo.PKIsHandle {
				continue
			}
		}

		if constr.Tp == ast.ConstraintFulltext {
			ctx.GetSessionVars().StmtCtx.AppendWarning(dbterror.ErrTableCantHandleFt.FastGenByArgs())
			continue
		}

		var (
			indexName       = constr.Name
			primary, unique bool
		)

		// Check if the index is primary or unique.
		switch constr.Tp {
		case ast.ConstraintPrimaryKey:
			primary = true
			unique = true
			indexName = mysql.PrimaryKeyName
		case ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
			unique = true
		}

		// check constraint
		if constr.Tp == ast.ConstraintCheck {
			if !variable.EnableCheckConstraint.Load() {
				ctx.GetSessionVars().StmtCtx.AppendWarning(errCheckConstraintIsOff)
				continue
			}
			// Since column check constraint dependency has been done in columnDefToCol.
			// Here do the table check constraint dependency check, table constraint
			// can only refer the columns in defined columns of the table.
			// Refer: https://dev.mysql.com/doc/refman/8.0/en/create-table-check-constraints.html
			if ok, err := table.IsSupportedExpr(constr); !ok {
				return nil, err
			}
			var dependedCols []model.CIStr
			dependedColsMap := findDependentColsInExpr(constr.Expr)
			if !constr.InColumn {
				dependedCols = make([]model.CIStr, 0, len(dependedColsMap))
				for k := range dependedColsMap {
					if _, ok := existedColsMap[k]; !ok {
						// The table constraint depended on a non-existed column.
						return nil, dbterror.ErrTableCheckConstraintReferUnknown.GenWithStackByArgs(constr.Name, k)
					}
					dependedCols = append(dependedCols, model.NewCIStr(k))
				}
			} else {
				// Check the column-type constraint dependency.
				if len(dependedColsMap) > 1 {
					return nil, dbterror.ErrColumnCheckConstraintReferOther.GenWithStackByArgs(constr.Name)
				} else if len(dependedColsMap) == 0 {
					// If dependedCols is empty, the expression must be true/false.
					valExpr, ok := constr.Expr.(*driver.ValueExpr)
					if !ok || !mysql.HasIsBooleanFlag(valExpr.GetType().GetFlag()) {
						return nil, errors.Trace(errors.New("unsupported expression in check constraint"))
					}
				} else {
					if _, ok := dependedColsMap[constr.InColumnName]; !ok {
						return nil, dbterror.ErrColumnCheckConstraintReferOther.GenWithStackByArgs(constr.Name)
					}
					dependedCols = []model.CIStr{model.NewCIStr(constr.InColumnName)}
				}
			}
			// check auto-increment column
			if table.ContainsAutoIncrementCol(dependedCols, tbInfo) {
				return nil, dbterror.ErrCheckConstraintRefersAutoIncrementColumn.GenWithStackByArgs(constr.Name)
			}
			// check foreign key
			if err := table.HasForeignKeyRefAction(tbInfo.ForeignKeys, constraints, constr, dependedCols); err != nil {
				return nil, err
			}
			// build constraint meta info.
			constraintInfo, err := buildConstraintInfo(tbInfo, dependedCols, constr, model.StatePublic)
			if err != nil {
				return nil, errors.Trace(err)
			}
			// check if the expression is bool type
			if err := table.IfCheckConstraintExprBoolType(ctx.GetExprCtx().GetEvalCtx(), constraintInfo, tbInfo); err != nil {
				return nil, err
			}
			constraintInfo.ID = allocateConstraintID(tbInfo)
			tbInfo.Constraints = append(tbInfo.Constraints, constraintInfo)
			continue
		}

		// build index info.
		idxInfo, err := BuildIndexInfo(
			ctx,
			tbInfo.Columns,
			model.NewCIStr(indexName),
			primary,
			unique,
			false,
			constr.Keys,
			constr.Option,
			model.StatePublic,
		)
		if err != nil {
			return nil, errors.Trace(err)
		}

		if len(hiddenCols) > 0 {
			AddIndexColumnFlag(tbInfo, idxInfo)
		}
		sessionVars := ctx.GetSessionVars()
		_, err = validateCommentLength(sessionVars.StmtCtx.ErrCtx(), sessionVars.SQLMode, idxInfo.Name.String(), &idxInfo.Comment, dbterror.ErrTooLongIndexComment)
		if err != nil {
			return nil, errors.Trace(err)
		}
		idxInfo.ID = AllocateIndexID(tbInfo)
		tbInfo.Indices = append(tbInfo.Indices, idxInfo)
	}

	err = addIndexForForeignKey(ctx, tbInfo)
	return tbInfo, err
}

// addIndexForForeignKey uses to auto create an index for the foreign key if the table doesn't have any index cover the
// foreign key columns.
func addIndexForForeignKey(ctx sessionctx.Context, tbInfo *model.TableInfo) error {
	if len(tbInfo.ForeignKeys) == 0 {
		return nil
	}
	var handleCol *model.ColumnInfo
	if tbInfo.PKIsHandle {
		handleCol = tbInfo.GetPkColInfo()
	}
	for _, fk := range tbInfo.ForeignKeys {
		if fk.Version < model.FKVersion1 {
			continue
		}
		if handleCol != nil && len(fk.Cols) == 1 && handleCol.Name.L == fk.Cols[0].L {
			continue
		}
		if model.FindIndexByColumns(tbInfo, tbInfo.Indices, fk.Cols...) != nil {
			continue
		}
		idxName := fk.Name
		if tbInfo.FindIndexByName(idxName.L) != nil {
			return dbterror.ErrDupKeyName.GenWithStack("duplicate key name %s", fk.Name.O)
		}
		keys := make([]*ast.IndexPartSpecification, 0, len(fk.Cols))
		for _, col := range fk.Cols {
			keys = append(keys, &ast.IndexPartSpecification{
				Column: &ast.ColumnName{Name: col},
				Length: types.UnspecifiedLength,
			})
		}
		idxInfo, err := BuildIndexInfo(ctx, tbInfo.Columns, idxName, false, false, false, keys, nil, model.StatePublic)
		if err != nil {
			return errors.Trace(err)
		}
		idxInfo.ID = AllocateIndexID(tbInfo)
		tbInfo.Indices = append(tbInfo.Indices, idxInfo)
	}
	return nil
}

func indexColumnsLen(cols []*model.ColumnInfo, idxCols []*model.IndexColumn) (colLen int, err error) {
	for _, idxCol := range idxCols {
		col := model.FindColumnInfo(cols, idxCol.Name.L)
		if col == nil {
			err = dbterror.ErrKeyColumnDoesNotExits.GenWithStack("column does not exist: %s", idxCol.Name.L)
			return
		}
		var l int
		l, err = getIndexColumnLength(col, idxCol.Length)
		if err != nil {
			return
		}
		colLen += l
	}
	return
}

func isSingleIntPK(constr *ast.Constraint, lastCol *model.ColumnInfo) bool {
	if len(constr.Keys) != 1 {
		return false
	}
	switch lastCol.GetType() {
	case mysql.TypeLong, mysql.TypeLonglong,
		mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24:
		return true
	}
	return false
}

// ShouldBuildClusteredIndex is used to determine whether the CREATE TABLE statement should build a clustered index table.
func ShouldBuildClusteredIndex(ctx sessionctx.Context, opt *ast.IndexOption, isSingleIntPK bool) bool {
	if opt == nil || opt.PrimaryKeyTp == model.PrimaryKeyTypeDefault {
		switch ctx.GetSessionVars().EnableClusteredIndex {
		case variable.ClusteredIndexDefModeOn:
			return true
		case variable.ClusteredIndexDefModeIntOnly:
			return !config.GetGlobalConfig().AlterPrimaryKey && isSingleIntPK
		default:
			return false
		}
	}
	return opt.PrimaryKeyTp == model.PrimaryKeyTypeClustered
}

// checkTableInfoValidExtra is like checkTableInfoValid, but also assumes the
// table info comes from untrusted source and performs further checks such as
// name length and column count.
// (checkTableInfoValid is also used in repairing objects which don't perform
// these checks. Perhaps the two functions should be merged together regardless?)
func checkTableInfoValidExtra(tbInfo *model.TableInfo) error {
	if err := checkTooLongTable(tbInfo.Name); err != nil {
		return err
	}

	if err := checkDuplicateColumn(tbInfo.Columns); err != nil {
		return err
	}
	if err := checkTooLongColumns(tbInfo.Columns); err != nil {
		return err
	}
	if err := checkTooManyColumns(tbInfo.Columns); err != nil {
		return errors.Trace(err)
	}
	if err := checkTooManyIndexes(tbInfo.Indices); err != nil {
		return errors.Trace(err)
	}
	if err := checkColumnsAttributes(tbInfo.Columns); err != nil {
		return errors.Trace(err)
	}

	// FIXME: perform checkConstraintNames
	if err := checkCharsetAndCollation(tbInfo.Charset, tbInfo.Collate); err != nil {
		return errors.Trace(err)
	}

	oldState := tbInfo.State
	tbInfo.State = model.StatePublic
	err := checkTableInfoValid(tbInfo)
	tbInfo.State = oldState
	return err
}

// CheckTableInfoValidWithStmt exposes checkTableInfoValidWithStmt to SchemaTracker. Maybe one day we can delete it.
func CheckTableInfoValidWithStmt(ctx sessionctx.Context, tbInfo *model.TableInfo, s *ast.CreateTableStmt) (err error) {
	return checkTableInfoValidWithStmt(ctx, tbInfo, s)
}

func checkTableInfoValidWithStmt(ctx sessionctx.Context, tbInfo *model.TableInfo, s *ast.CreateTableStmt) (err error) {
	// All of these rely on the AST structure of expressions, which were
	// lost in the model (got serialized into strings).
	if err := checkGeneratedColumn(ctx, s.Table.Schema, tbInfo.Name, s.Cols); err != nil {
		return errors.Trace(err)
	}

	// Check if table has a primary key if required.
	if !ctx.GetSessionVars().InRestrictedSQL && ctx.GetSessionVars().PrimaryKeyRequired && len(tbInfo.GetPkName().String()) == 0 {
		return infoschema.ErrTableWithoutPrimaryKey
	}
	if tbInfo.Partition != nil {
		if err := checkPartitionDefinitionConstraints(ctx, tbInfo); err != nil {
			return errors.Trace(err)
		}
		if s.Partition != nil {
			if err := checkPartitionFuncType(ctx, s.Partition.Expr, s.Table.Schema.O, tbInfo); err != nil {
				return errors.Trace(err)
			}
			if err := checkPartitioningKeysConstraints(ctx, s, tbInfo); err != nil {
				return errors.Trace(err)
			}
		}
	}
	if tbInfo.TTLInfo != nil {
		if err := checkTTLInfoValid(ctx, s.Table.Schema, tbInfo); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func checkPartitionDefinitionConstraints(ctx sessionctx.Context, tbInfo *model.TableInfo) error {
	var err error
	if err = checkPartitionNameUnique(tbInfo.Partition); err != nil {
		return errors.Trace(err)
	}
	if err = checkAddPartitionTooManyPartitions(uint64(len(tbInfo.Partition.Definitions))); err != nil {
		return err
	}
	if err = checkAddPartitionOnTemporaryMode(tbInfo); err != nil {
		return err
	}
	if err = checkPartitionColumnsUnique(tbInfo); err != nil {
		return err
	}

	switch tbInfo.Partition.Type {
	case model.PartitionTypeRange:
		err = checkPartitionByRange(ctx, tbInfo)
	case model.PartitionTypeHash, model.PartitionTypeKey:
		err = checkPartitionByHash(ctx, tbInfo)
	case model.PartitionTypeList:
		err = checkPartitionByList(ctx, tbInfo)
	}
	return errors.Trace(err)
}

// checkTableInfoValid uses to check table info valid. This is used to validate table info.
func checkTableInfoValid(tblInfo *model.TableInfo) error {
	_, err := tables.TableFromMeta(autoid.NewAllocators(false), tblInfo)
	if err != nil {
		return err
	}
	return checkInvisibleIndexOnPK(tblInfo)
}

// BuildTableInfoWithLike builds a new table info according to CREATE TABLE ... LIKE statement.
func BuildTableInfoWithLike(ctx sessionctx.Context, ident ast.Ident, referTblInfo *model.TableInfo, s *ast.CreateTableStmt) (*model.TableInfo, error) {
	// Check the referred table is a real table object.
	if referTblInfo.IsSequence() || referTblInfo.IsView() {
		return nil, dbterror.ErrWrongObject.GenWithStackByArgs(ident.Schema, referTblInfo.Name, "BASE TABLE")
	}
	tblInfo := *referTblInfo
	if err := setTemporaryType(ctx, &tblInfo, s); err != nil {
		return nil, errors.Trace(err)
	}
	// Check non-public column and adjust column offset.
	newColumns := referTblInfo.Cols()
	newIndices := make([]*model.IndexInfo, 0, len(tblInfo.Indices))
	for _, idx := range tblInfo.Indices {
		if idx.State == model.StatePublic {
			newIndices = append(newIndices, idx)
		}
	}
	tblInfo.Columns = newColumns
	tblInfo.Indices = newIndices
	tblInfo.Name = ident.Name
	tblInfo.AutoIncID = 0
	tblInfo.ForeignKeys = nil
	// Ignore TiFlash replicas for temporary tables.
	if s.TemporaryKeyword != ast.TemporaryNone {
		tblInfo.TiFlashReplica = nil
	} else if tblInfo.TiFlashReplica != nil {
		replica := *tblInfo.TiFlashReplica
		// Keep the tiflash replica setting, remove the replica available status.
		replica.AvailablePartitionIDs = nil
		replica.Available = false
		tblInfo.TiFlashReplica = &replica
	}
	if referTblInfo.Partition != nil {
		pi := *referTblInfo.Partition
		pi.Definitions = make([]model.PartitionDefinition, len(referTblInfo.Partition.Definitions))
		copy(pi.Definitions, referTblInfo.Partition.Definitions)
		tblInfo.Partition = &pi
	}

	if referTblInfo.TTLInfo != nil {
		tblInfo.TTLInfo = referTblInfo.TTLInfo.Clone()
	}
	renameCheckConstraint(&tblInfo)
	return &tblInfo, nil
}

func renameCheckConstraint(tblInfo *model.TableInfo) {
	for _, cons := range tblInfo.Constraints {
		cons.Name = model.NewCIStr("")
		cons.Table = tblInfo.Name
	}
	setNameForConstraintInfo(tblInfo.Name.L, map[string]bool{}, tblInfo.Constraints)
}

// BuildTableInfoFromAST builds model.TableInfo from a SQL statement.
// Note: TableID and PartitionID are left as uninitialized value.
func BuildTableInfoFromAST(s *ast.CreateTableStmt) (*model.TableInfo, error) {
	return buildTableInfoWithCheck(mock.NewContext(), s, mysql.DefaultCharset, "", nil)
}

// buildTableInfoWithCheck builds model.TableInfo from a SQL statement.
// Note: TableID and PartitionIDs are left as uninitialized value.
func buildTableInfoWithCheck(ctx sessionctx.Context, s *ast.CreateTableStmt, dbCharset, dbCollate string, placementPolicyRef *model.PolicyRefInfo) (*model.TableInfo, error) {
	tbInfo, err := BuildTableInfoWithStmt(ctx, s, dbCharset, dbCollate, placementPolicyRef)
	if err != nil {
		return nil, err
	}
	// Fix issue 17952 which will cause partition range expr can't be parsed as Int.
	// checkTableInfoValidWithStmt will do the constant fold the partition expression first,
	// then checkTableInfoValidExtra will pass the tableInfo check successfully.
	if err = checkTableInfoValidWithStmt(ctx, tbInfo, s); err != nil {
		return nil, err
	}
	if err = checkTableInfoValidExtra(tbInfo); err != nil {
		return nil, err
	}
	return tbInfo, nil
}

// BuildSessionTemporaryTableInfo builds model.TableInfo from a SQL statement.
func BuildSessionTemporaryTableInfo(ctx sessionctx.Context, is infoschema.InfoSchema, s *ast.CreateTableStmt, dbCharset, dbCollate string, placementPolicyRef *model.PolicyRefInfo) (*model.TableInfo, error) {
	ident := ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name}
	//build tableInfo
	var tbInfo *model.TableInfo
	var referTbl table.Table
	var err error
	if s.ReferTable != nil {
		referIdent := ast.Ident{Schema: s.ReferTable.Schema, Name: s.ReferTable.Name}
		_, ok := is.SchemaByName(referIdent.Schema)
		if !ok {
			return nil, infoschema.ErrTableNotExists.GenWithStackByArgs(referIdent.Schema, referIdent.Name)
		}
		referTbl, err = is.TableByName(referIdent.Schema, referIdent.Name)
		if err != nil {
			return nil, infoschema.ErrTableNotExists.GenWithStackByArgs(referIdent.Schema, referIdent.Name)
		}
		tbInfo, err = BuildTableInfoWithLike(ctx, ident, referTbl.Meta(), s)
	} else {
		tbInfo, err = buildTableInfoWithCheck(ctx, s, dbCharset, dbCollate, placementPolicyRef)
	}
	return tbInfo, err
}

// BuildTableInfoWithStmt builds model.TableInfo from a SQL statement without validity check
func BuildTableInfoWithStmt(ctx sessionctx.Context, s *ast.CreateTableStmt, dbCharset, dbCollate string, placementPolicyRef *model.PolicyRefInfo) (*model.TableInfo, error) {
	colDefs := s.Cols
	tableCharset, tableCollate, err := GetCharsetAndCollateInTableOption(ctx.GetSessionVars(), 0, s.Options)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tableCharset, tableCollate, err = ResolveCharsetCollation(ctx.GetSessionVars(),
		ast.CharsetOpt{Chs: tableCharset, Col: tableCollate},
		ast.CharsetOpt{Chs: dbCharset, Col: dbCollate},
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// The column charset haven't been resolved here.
	cols, newConstraints, err := buildColumnsAndConstraints(ctx, colDefs, s.Constraints, tableCharset, tableCollate)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = checkConstraintNames(s.Table.Name, newConstraints)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var tbInfo *model.TableInfo
	tbInfo, err = BuildTableInfo(ctx, s.Table.Name, cols, newConstraints, tableCharset, tableCollate)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if err = setTemporaryType(ctx, tbInfo, s); err != nil {
		return nil, errors.Trace(err)
	}

	if err = setTableAutoRandomBits(ctx, tbInfo, colDefs); err != nil {
		return nil, errors.Trace(err)
	}

	if err = handleTableOptions(s.Options, tbInfo); err != nil {
		return nil, errors.Trace(err)
	}

	sessionVars := ctx.GetSessionVars()
	if _, err = validateCommentLength(sessionVars.StmtCtx.ErrCtx(), sessionVars.SQLMode, tbInfo.Name.L, &tbInfo.Comment, dbterror.ErrTooLongTableComment); err != nil {
		return nil, errors.Trace(err)
	}

	if tbInfo.TempTableType == model.TempTableNone && tbInfo.PlacementPolicyRef == nil && placementPolicyRef != nil {
		// Set the defaults from Schema. Note: they are mutual exclusive!
		tbInfo.PlacementPolicyRef = placementPolicyRef
	}

	// After handleTableOptions, so the partitions can get defaults from Table level
	err = buildTablePartitionInfo(ctx, s.Partition, tbInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return tbInfo, nil
}

func (d *ddl) assignTableID(tbInfo *model.TableInfo) error {
	genIDs, err := d.genGlobalIDs(1)
	if err != nil {
		return errors.Trace(err)
	}
	tbInfo.ID = genIDs[0]
	return nil
}

func (d *ddl) assignPartitionIDs(defs []model.PartitionDefinition) error {
	genIDs, err := d.genGlobalIDs(len(defs))
	if err != nil {
		return errors.Trace(err)
	}
	for i := range defs {
		defs[i].ID = genIDs[i]
	}
	return nil
}

func (d *ddl) CreateTable(ctx sessionctx.Context, s *ast.CreateTableStmt) (err error) {
	ident := ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name}
	is := d.GetInfoSchemaWithInterceptor(ctx)
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}

	var referTbl table.Table
	if s.ReferTable != nil {
		referIdent := ast.Ident{Schema: s.ReferTable.Schema, Name: s.ReferTable.Name}
		_, ok := is.SchemaByName(referIdent.Schema)
		if !ok {
			return infoschema.ErrTableNotExists.GenWithStackByArgs(referIdent.Schema, referIdent.Name)
		}
		referTbl, err = is.TableByName(referIdent.Schema, referIdent.Name)
		if err != nil {
			return infoschema.ErrTableNotExists.GenWithStackByArgs(referIdent.Schema, referIdent.Name)
		}
	}

	// build tableInfo
	var tbInfo *model.TableInfo
	if s.ReferTable != nil {
		tbInfo, err = BuildTableInfoWithLike(ctx, ident, referTbl.Meta(), s)
	} else {
		tbInfo, err = BuildTableInfoWithStmt(ctx, s, schema.Charset, schema.Collate, schema.PlacementPolicyRef)
	}
	if err != nil {
		return errors.Trace(err)
	}

	if err = checkTableInfoValidWithStmt(ctx, tbInfo, s); err != nil {
		return err
	}
	if err = checkTableForeignKeysValid(ctx, is, schema.Name.L, tbInfo); err != nil {
		return err
	}

	onExist := OnExistError
	if s.IfNotExists {
		onExist = OnExistIgnore
	}

	return d.CreateTableWithInfo(ctx, schema.Name, tbInfo, onExist)
}

func setTemporaryType(_ sessionctx.Context, tbInfo *model.TableInfo, s *ast.CreateTableStmt) error {
	switch s.TemporaryKeyword {
	case ast.TemporaryGlobal:
		tbInfo.TempTableType = model.TempTableGlobal
		// "create global temporary table ... on commit preserve rows"
		if !s.OnCommitDelete {
			return errors.Trace(dbterror.ErrUnsupportedOnCommitPreserve)
		}
	case ast.TemporaryLocal:
		tbInfo.TempTableType = model.TempTableLocal
	default:
		tbInfo.TempTableType = model.TempTableNone
	}
	return nil
}

// createTableWithInfoJob returns the table creation job.
// WARNING: it may return a nil job, which means you don't need to submit any DDL job.
// WARNING!!!: if retainID == true, it will not allocate ID by itself. That means if the caller
// can not promise ID is unique, then we got inconsistency.
func (d *ddl) createTableWithInfoJob(
	ctx sessionctx.Context,
	dbName model.CIStr,
	tbInfo *model.TableInfo,
	onExist OnExist,
	retainID bool,
) (job *model.Job, err error) {
	is := d.GetInfoSchemaWithInterceptor(ctx)
	schema, ok := is.SchemaByName(dbName)
	if !ok {
		return nil, infoschema.ErrDatabaseNotExists.GenWithStackByArgs(dbName)
	}

	if err = handleTablePlacement(ctx, tbInfo); err != nil {
		return nil, errors.Trace(err)
	}

	var oldViewTblID int64
	if oldTable, err := is.TableByName(schema.Name, tbInfo.Name); err == nil {
		err = infoschema.ErrTableExists.GenWithStackByArgs(ast.Ident{Schema: schema.Name, Name: tbInfo.Name})
		switch onExist {
		case OnExistIgnore:
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil, nil
		case OnExistReplace:
			// only CREATE OR REPLACE VIEW is supported at the moment.
			if tbInfo.View != nil {
				if oldTable.Meta().IsView() {
					oldViewTblID = oldTable.Meta().ID
					break
				}
				// The object to replace isn't a view.
				return nil, dbterror.ErrWrongObject.GenWithStackByArgs(dbName, tbInfo.Name, "VIEW")
			}
			return nil, err
		default:
			return nil, err
		}
	}

	if !retainID {
		if err := d.assignTableID(tbInfo); err != nil {
			return nil, errors.Trace(err)
		}

		if tbInfo.Partition != nil {
			if err := d.assignPartitionIDs(tbInfo.Partition.Definitions); err != nil {
				return nil, errors.Trace(err)
			}
		}
	}

	if err := checkTableInfoValidExtra(tbInfo); err != nil {
		return nil, err
	}

	var actionType model.ActionType
	args := []any{tbInfo}
	switch {
	case tbInfo.View != nil:
		actionType = model.ActionCreateView
		args = append(args, onExist == OnExistReplace, oldViewTblID)
	case tbInfo.Sequence != nil:
		actionType = model.ActionCreateSequence
	default:
		actionType = model.ActionCreateTable
		args = append(args, ctx.GetSessionVars().ForeignKeyChecks)
	}

	job = &model.Job{
		SchemaID:       schema.ID,
		TableID:        tbInfo.ID,
		SchemaName:     schema.Name.L,
		TableName:      tbInfo.Name.L,
		Type:           actionType,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           args,
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	return job, nil
}

func (d *ddl) createTableWithInfoPost(
	ctx sessionctx.Context,
	tbInfo *model.TableInfo,
	schemaID int64,
) error {
	var err error
	var partitions []model.PartitionDefinition
	if pi := tbInfo.GetPartitionInfo(); pi != nil {
		partitions = pi.Definitions
	}
	preSplitAndScatter(ctx, d.store, tbInfo, partitions)
	if tbInfo.AutoIncID > 1 {
		// Default tableAutoIncID base is 0.
		// If the first ID is expected to greater than 1, we need to do rebase.
		newEnd := tbInfo.AutoIncID - 1
		var allocType autoid.AllocatorType
		if tbInfo.SepAutoInc() {
			allocType = autoid.AutoIncrementType
		} else {
			allocType = autoid.RowIDAllocType
		}
		if err = d.handleAutoIncID(tbInfo, schemaID, newEnd, allocType); err != nil {
			return errors.Trace(err)
		}
	}
	// For issue https://github.com/pingcap/tidb/issues/46093
	if tbInfo.AutoIncIDExtra != 0 {
		if err = d.handleAutoIncID(tbInfo, schemaID, tbInfo.AutoIncIDExtra-1, autoid.RowIDAllocType); err != nil {
			return errors.Trace(err)
		}
	}
	if tbInfo.AutoRandID > 1 {
		// Default tableAutoRandID base is 0.
		// If the first ID is expected to greater than 1, we need to do rebase.
		newEnd := tbInfo.AutoRandID - 1
		err = d.handleAutoIncID(tbInfo, schemaID, newEnd, autoid.AutoRandomType)
	}
	return err
}

func (d *ddl) CreateTableWithInfo(
	ctx sessionctx.Context,
	dbName model.CIStr,
	tbInfo *model.TableInfo,
	cs ...CreateTableWithInfoConfigurier,
) (err error) {
	c := GetCreateTableWithInfoConfig(cs)

	job, err := d.createTableWithInfoJob(ctx, dbName, tbInfo, c.OnExist, !c.ShouldAllocTableID(tbInfo))
	if err != nil {
		return err
	}
	if job == nil {
		return nil
	}

	err = d.DoDDLJob(ctx, job)
	if err != nil {
		// table exists, but if_not_exists flags is true, so we ignore this error.
		if c.OnExist == OnExistIgnore && infoschema.ErrTableExists.Equal(err) {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			err = nil
		}
	} else {
		err = d.createTableWithInfoPost(ctx, tbInfo, job.SchemaID)
	}

	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

func (d *ddl) BatchCreateTableWithInfo(ctx sessionctx.Context,
	dbName model.CIStr,
	infos []*model.TableInfo,
	cs ...CreateTableWithInfoConfigurier,
) error {
	failpoint.Inject("RestoreBatchCreateTableEntryTooLarge", func(val failpoint.Value) {
		injectBatchSize := val.(int)
		if len(infos) > injectBatchSize {
			failpoint.Return(kv.ErrEntryTooLarge)
		}
	})
	c := GetCreateTableWithInfoConfig(cs)

	jobs := &model.Job{
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	args := make([]*model.TableInfo, 0, len(infos))

	var err error

	// 1. counts how many IDs are there
	// 2. if there is any duplicated table name
	totalID := 0
	duplication := make(map[string]struct{})
	for _, info := range infos {
		if _, ok := duplication[info.Name.L]; ok {
			err = infoschema.ErrTableExists.FastGenByArgs("can not batch create tables with same name")
			if c.OnExist == OnExistIgnore && infoschema.ErrTableExists.Equal(err) {
				ctx.GetSessionVars().StmtCtx.AppendNote(err)
				err = nil
			}
		}
		if err != nil {
			return errors.Trace(err)
		}

		duplication[info.Name.L] = struct{}{}

		totalID++
		parts := info.GetPartitionInfo()
		if parts != nil {
			totalID += len(parts.Definitions)
		}
	}

	genIDs, err := d.genGlobalIDs(totalID)
	if err != nil {
		return errors.Trace(err)
	}

	for _, info := range infos {
		if c.ShouldAllocTableID(info) {
			info.ID, genIDs = genIDs[0], genIDs[1:]

			if parts := info.GetPartitionInfo(); parts != nil {
				for i := range parts.Definitions {
					parts.Definitions[i].ID, genIDs = genIDs[0], genIDs[1:]
				}
			}
		}

		job, err := d.createTableWithInfoJob(ctx, dbName, info, c.OnExist, true)
		if err != nil {
			return errors.Trace(err)
		}
		if job == nil {
			continue
		}

		// if jobs.Type == model.ActionCreateTables, it is initialized
		// if not, initialize jobs by job.XXXX
		if jobs.Type != model.ActionCreateTables {
			jobs.Type = model.ActionCreateTables
			jobs.SchemaID = job.SchemaID
			jobs.SchemaName = job.SchemaName
		}

		// append table job args
		info, ok := job.Args[0].(*model.TableInfo)
		if !ok {
			return errors.Trace(fmt.Errorf("except table info"))
		}
		args = append(args, info)
		jobs.InvolvingSchemaInfo = append(jobs.InvolvingSchemaInfo,
			model.InvolvingSchemaInfo{
				Database: dbName.L,
				Table:    info.Name.L,
			})
	}
	if len(args) == 0 {
		return nil
	}
	jobs.Args = append(jobs.Args, args)
	jobs.Args = append(jobs.Args, ctx.GetSessionVars().ForeignKeyChecks)

	err = d.DoDDLJob(ctx, jobs)
	if err != nil {
		// table exists, but if_not_exists flags is true, so we ignore this error.
		if c.OnExist == OnExistIgnore && infoschema.ErrTableExists.Equal(err) {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			err = nil
		}
		return errors.Trace(d.callHookOnChanged(jobs, err))
	}

	for j := range args {
		if err = d.createTableWithInfoPost(ctx, args[j], jobs.SchemaID); err != nil {
			return errors.Trace(d.callHookOnChanged(jobs, err))
		}
	}

	return d.callHookOnChanged(jobs, err)
}

// BuildQueryStringFromJobs takes a slice of Jobs and concatenates their
// queries into a single query string.
// Each query is separated by a semicolon and a space.
// Trailing spaces are removed from each query, and a semicolon is appended
// if it's not already present.
func BuildQueryStringFromJobs(jobs []*model.Job) string {
	var queryBuilder strings.Builder
	for i, job := range jobs {
		q := strings.TrimSpace(job.Query)
		if !strings.HasSuffix(q, ";") {
			q += ";"
		}
		queryBuilder.WriteString(q)

		if i < len(jobs)-1 {
			queryBuilder.WriteString(" ")
		}
	}
	return queryBuilder.String()
}

// BatchCreateTableWithJobs combine CreateTableJobs to BatchCreateTableJob.
func BatchCreateTableWithJobs(jobs []*model.Job) (*model.Job, error) {
	if len(jobs) == 0 {
		return nil, errors.Trace(fmt.Errorf("expect non-empty jobs"))
	}

	var combinedJob *model.Job

	args := make([]*model.TableInfo, 0, len(jobs))
	involvingSchemaInfo := make([]model.InvolvingSchemaInfo, 0, len(jobs))
	var foreignKeyChecks bool

	// if there is any duplicated table name
	duplication := make(map[string]struct{})
	for _, job := range jobs {
		if combinedJob == nil {
			combinedJob = job.Clone()
			combinedJob.Type = model.ActionCreateTables
			combinedJob.Args = combinedJob.Args[:0]
			foreignKeyChecks = job.Args[1].(bool)
		}
		// append table job args
		info, ok := job.Args[0].(*model.TableInfo)
		if !ok {
			return nil, errors.Trace(fmt.Errorf("expect model.TableInfo, but got %T", job.Args[0]))
		}
		args = append(args, info)

		if _, ok := duplication[info.Name.L]; ok {
			// return err even if create table if not exists
			return nil, infoschema.ErrTableExists.FastGenByArgs("can not batch create tables with same name")
		}

		duplication[info.Name.L] = struct{}{}

		involvingSchemaInfo = append(involvingSchemaInfo,
			model.InvolvingSchemaInfo{
				Database: job.SchemaName,
				Table:    info.Name.L,
			})
	}

	combinedJob.Args = append(combinedJob.Args, args)
	combinedJob.Args = append(combinedJob.Args, foreignKeyChecks)
	combinedJob.InvolvingSchemaInfo = involvingSchemaInfo
	combinedJob.Query = BuildQueryStringFromJobs(jobs)

	return combinedJob, nil
}

func (d *ddl) CreatePlacementPolicyWithInfo(ctx sessionctx.Context, policy *model.PolicyInfo, onExist OnExist) error {
	if checkIgnorePlacementDDL(ctx) {
		return nil
	}

	policyName := policy.Name
	if policyName.L == defaultPlacementPolicyName {
		return errors.Trace(infoschema.ErrReservedSyntax.GenWithStackByArgs(policyName))
	}

	// Check policy existence.
	_, ok := d.GetInfoSchemaWithInterceptor(ctx).PolicyByName(policyName)
	if ok {
		err := infoschema.ErrPlacementPolicyExists.GenWithStackByArgs(policyName)
		switch onExist {
		case OnExistIgnore:
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		case OnExistError:
			return err
		}
	}

	if err := checkPolicyValidation(policy.PlacementSettings); err != nil {
		return err
	}

	policyID, err := d.genPlacementPolicyID()
	if err != nil {
		return err
	}
	policy.ID = policyID

	job := &model.Job{
		SchemaName: policy.Name.L,
		Type:       model.ActionCreatePlacementPolicy,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []any{policy, onExist == OnExistReplace},
		// CREATE PLACEMENT does not affect any schemas or tables.
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			Database: model.InvolvingNone,
			Table:    model.InvolvingNone,
		}},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

// preSplitAndScatter performs pre-split and scatter of the table's regions.
// If `pi` is not nil, will only split region for `pi`, this is used when add partition.
func preSplitAndScatter(ctx sessionctx.Context, store kv.Storage, tbInfo *model.TableInfo, parts []model.PartitionDefinition) {
	if tbInfo.TempTableType != model.TempTableNone {
		return
	}
	sp, ok := store.(kv.SplittableStore)
	if !ok || atomic.LoadUint32(&EnableSplitTableRegion) == 0 {
		return
	}
	var (
		preSplit      func()
		scatterRegion bool
	)
	val, err := ctx.GetSessionVars().GetGlobalSystemVar(context.Background(), variable.TiDBScatterRegion)
	if err != nil {
		logutil.DDLLogger().Warn("won't scatter region", zap.Error(err))
	} else {
		scatterRegion = variable.TiDBOptOn(val)
	}
	if len(parts) > 0 {
		preSplit = func() { splitPartitionTableRegion(ctx, sp, tbInfo, parts, scatterRegion) }
	} else {
		preSplit = func() { splitTableRegion(ctx, sp, tbInfo, scatterRegion) }
	}
	if scatterRegion {
		preSplit()
	} else {
		go preSplit()
	}
}

func (d *ddl) FlashbackCluster(ctx sessionctx.Context, flashbackTS uint64) error {
	logutil.DDLLogger().Info("get flashback cluster job", zap.Stringer("flashbackTS", oracle.GetTimeFromTS(flashbackTS)))
	nowTS, err := ctx.GetStore().GetOracle().GetTimestamp(d.ctx, &oracle.Option{})
	if err != nil {
		return errors.Trace(err)
	}
	gap := time.Until(oracle.GetTimeFromTS(nowTS)).Abs()
	if gap > 1*time.Second {
		ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("Gap between local time and PD TSO is %s, please check PD/system time", gap))
	}
	job := &model.Job{
		Type:       model.ActionFlashbackCluster,
		BinlogInfo: &model.HistoryInfo{},
		// The value for global variables is meaningless, it will cover during flashback cluster.
		Args: []any{
			flashbackTS,
			map[string]any{},
			true,         /* tidb_gc_enable */
			variable.On,  /* tidb_enable_auto_analyze */
			variable.Off, /* tidb_super_read_only */
			0,            /* totalRegions */
			0,            /* startTS */
			0,            /* commitTS */
			variable.On,  /* tidb_ttl_job_enable */
			[]kv.KeyRange{} /* flashback key_ranges */},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		// FLASHBACK CLUSTER affects all schemas and tables.
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			Database: model.InvolvingAll,
			Table:    model.InvolvingAll,
		}},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}
	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

func (d *ddl) RecoverTable(ctx sessionctx.Context, recoverInfo *RecoverInfo) (err error) {
	is := d.GetInfoSchemaWithInterceptor(ctx)
	schemaID, tbInfo := recoverInfo.SchemaID, recoverInfo.TableInfo
	// Check schema exist.
	schema, ok := is.SchemaByID(schemaID)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", schemaID),
		))
	}
	// Check not exist table with same name.
	if ok := is.TableExists(schema.Name, tbInfo.Name); ok {
		return infoschema.ErrTableExists.GenWithStackByArgs(tbInfo.Name)
	}

	tbInfo.State = model.StateNone
	job := &model.Job{
		SchemaID:   schemaID,
		TableID:    tbInfo.ID,
		SchemaName: schema.Name.L,
		TableName:  tbInfo.Name.L,

		Type:           model.ActionRecoverTable,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           []any{recoverInfo, recoverCheckFlagNone},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

func (d *ddl) CreateView(ctx sessionctx.Context, s *ast.CreateViewStmt) (err error) {
	viewInfo, err := BuildViewInfo(ctx, s)
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

	tbInfo, err := BuildTableInfo(ctx, s.ViewName.Name, cols, nil, tblCharset, tblCollate)
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

// BuildViewInfo builds a ViewInfo structure from an ast.CreateViewStmt.
func BuildViewInfo(_ sessionctx.Context, s *ast.CreateViewStmt) (*model.ViewInfo, error) {
	// Always Use `format.RestoreNameBackQuotes` to restore `SELECT` statement despite the `ANSI_QUOTES` SQL Mode is enabled or not.
	restoreFlag := format.RestoreStringSingleQuotes | format.RestoreKeyWordUppercase | format.RestoreNameBackQuotes
	var sb strings.Builder
	if err := s.Select.Restore(format.NewRestoreCtx(restoreFlag, &sb)); err != nil {
		return nil, err
	}

	return &model.ViewInfo{Definer: s.Definer, Algorithm: s.Algorithm,
		Security: s.Security, SelectStmt: sb.String(), CheckOption: s.CheckOption, Cols: nil}, nil
}

func checkPartitionByHash(ctx sessionctx.Context, tbInfo *model.TableInfo) error {
	return checkNoHashPartitions(ctx, tbInfo.Partition.Num)
}

// checkPartitionByRange checks validity of a "BY RANGE" partition.
func checkPartitionByRange(ctx sessionctx.Context, tbInfo *model.TableInfo) error {
	failpoint.Inject("CheckPartitionByRangeErr", func() {
		ctx.GetSessionVars().SQLKiller.SendKillSignal(sqlkiller.QueryMemoryExceeded)
		panic(ctx.GetSessionVars().SQLKiller.HandleSignal())
	})
	pi := tbInfo.Partition

	if len(pi.Columns) == 0 {
		return checkRangePartitionValue(ctx, tbInfo)
	}

	return checkRangeColumnsPartitionValue(ctx, tbInfo)
}

// checkPartitionByList checks validity of a "BY LIST" partition.
func checkPartitionByList(ctx sessionctx.Context, tbInfo *model.TableInfo) error {
	return checkListPartitionValue(ctx.GetExprCtx(), tbInfo)
}

func isValidKeyPartitionColType(fieldType types.FieldType) bool {
	switch fieldType.GetType() {
	case mysql.TypeBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeJSON, mysql.TypeGeometry:
		return false
	default:
		return true
	}
}

func isColTypeAllowedAsPartitioningCol(partType model.PartitionType, fieldType types.FieldType) bool {
	// For key partition, the permitted partition field types can be all field types except
	// BLOB, JSON, Geometry
	if partType == model.PartitionTypeKey {
		return isValidKeyPartitionColType(fieldType)
	}
	// The permitted data types are shown in the following list:
	// All integer types
	// DATE and DATETIME
	// CHAR, VARCHAR, BINARY, and VARBINARY
	// See https://dev.mysql.com/doc/mysql-partitioning-excerpt/5.7/en/partitioning-columns.html
	// Note that also TIME is allowed in MySQL. Also see https://bugs.mysql.com/bug.php?id=84362
	switch fieldType.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeDuration:
	case mysql.TypeVarchar, mysql.TypeString:
	default:
		return false
	}
	return true
}

func checkColumnsPartitionType(tbInfo *model.TableInfo) error {
	for _, col := range tbInfo.Partition.Columns {
		colInfo := tbInfo.FindPublicColumnByName(col.L)
		if colInfo == nil {
			return errors.Trace(dbterror.ErrFieldNotFoundPart)
		}
		if !isColTypeAllowedAsPartitioningCol(tbInfo.Partition.Type, colInfo.FieldType) {
			return dbterror.ErrNotAllowedTypeInPartition.GenWithStackByArgs(col.O)
		}
	}
	return nil
}

func checkRangeColumnsPartitionValue(ctx sessionctx.Context, tbInfo *model.TableInfo) error {
	// Range columns partition key supports multiple data types with integer、datetime、string.
	pi := tbInfo.Partition
	defs := pi.Definitions
	if len(defs) < 1 {
		return ast.ErrPartitionsMustBeDefined.GenWithStackByArgs("RANGE")
	}

	curr := &defs[0]
	if len(curr.LessThan) != len(pi.Columns) {
		return errors.Trace(ast.ErrPartitionColumnList)
	}
	var prev *model.PartitionDefinition
	for i := 1; i < len(defs); i++ {
		prev, curr = curr, &defs[i]
		succ, err := checkTwoRangeColumns(ctx, curr, prev, pi, tbInfo)
		if err != nil {
			return err
		}
		if !succ {
			return errors.Trace(dbterror.ErrRangeNotIncreasing)
		}
	}
	return nil
}

func checkTwoRangeColumns(ctx sessionctx.Context, curr, prev *model.PartitionDefinition, pi *model.PartitionInfo, tbInfo *model.TableInfo) (bool, error) {
	if len(curr.LessThan) != len(pi.Columns) {
		return false, errors.Trace(ast.ErrPartitionColumnList)
	}
	for i := 0; i < len(pi.Columns); i++ {
		// Special handling for MAXVALUE.
		if strings.EqualFold(curr.LessThan[i], partitionMaxValue) && !strings.EqualFold(prev.LessThan[i], partitionMaxValue) {
			// If current is maxvalue, it certainly >= previous.
			return true, nil
		}
		if strings.EqualFold(prev.LessThan[i], partitionMaxValue) {
			// Current is not maxvalue, and previous is maxvalue.
			return false, nil
		}

		// The tuples of column values used to define the partitions are strictly increasing:
		// PARTITION p0 VALUES LESS THAN (5,10,'ggg')
		// PARTITION p1 VALUES LESS THAN (10,20,'mmm')
		// PARTITION p2 VALUES LESS THAN (15,30,'sss')
		colInfo := findColumnByName(pi.Columns[i].L, tbInfo)
		succ, err := parseAndEvalBoolExpr(ctx.GetExprCtx(), curr.LessThan[i], prev.LessThan[i], colInfo, tbInfo)
		if err != nil {
			return false, err
		}

		if succ {
			return true, nil
		}
	}
	return false, nil
}

func parseAndEvalBoolExpr(ctx expression.BuildContext, l, r string, colInfo *model.ColumnInfo, tbInfo *model.TableInfo) (bool, error) {
	lexpr, err := expression.ParseSimpleExpr(ctx, l, expression.WithTableInfo("", tbInfo), expression.WithCastExprTo(&colInfo.FieldType))
	if err != nil {
		return false, err
	}
	rexpr, err := expression.ParseSimpleExpr(ctx, r, expression.WithTableInfo("", tbInfo), expression.WithCastExprTo(&colInfo.FieldType))
	if err != nil {
		return false, err
	}
	e, err := expression.NewFunctionBase(ctx, ast.GT, field_types.NewFieldType(mysql.TypeLonglong), lexpr, rexpr)
	if err != nil {
		return false, err
	}
	e.SetCharsetAndCollation(colInfo.GetCharset(), colInfo.GetCollate())
	res, _, err1 := e.EvalInt(ctx.GetEvalCtx(), chunk.Row{})
	if err1 != nil {
		return false, err1
	}
	return res > 0, nil
}

func checkCharsetAndCollation(cs string, co string) error {
	if !charset.ValidCharsetAndCollation(cs, co) {
		return dbterror.ErrUnknownCharacterSet.GenWithStackByArgs(cs)
	}
	if co != "" {
		if _, err := collate.GetCollationByName(co); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// handleAutoIncID handles auto_increment option in DDL. It creates a ID counter for the table and initiates the counter to a proper value.
// For example if the option sets auto_increment to 10. The counter will be set to 9. So the next allocated ID will be 10.
func (d *ddl) handleAutoIncID(tbInfo *model.TableInfo, schemaID int64, newEnd int64, tp autoid.AllocatorType) error {
	allocs := autoid.NewAllocatorsFromTblInfo((*asAutoIDRequirement)(d.ddlCtx), schemaID, tbInfo)
	if alloc := allocs.Get(tp); alloc != nil {
		err := alloc.Rebase(context.Background(), newEnd, false)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// SetDirectPlacementOpt tries to make the PlacementSettings assignments generic for Schema/Table/Partition
func SetDirectPlacementOpt(placementSettings *model.PlacementSettings, placementOptionType ast.PlacementOptionType, stringVal string, uintVal uint64) error {
	switch placementOptionType {
	case ast.PlacementOptionPrimaryRegion:
		placementSettings.PrimaryRegion = stringVal
	case ast.PlacementOptionRegions:
		placementSettings.Regions = stringVal
	case ast.PlacementOptionFollowerCount:
		placementSettings.Followers = uintVal
	case ast.PlacementOptionVoterCount:
		placementSettings.Voters = uintVal
	case ast.PlacementOptionLearnerCount:
		placementSettings.Learners = uintVal
	case ast.PlacementOptionSchedule:
		placementSettings.Schedule = stringVal
	case ast.PlacementOptionConstraints:
		placementSettings.Constraints = stringVal
	case ast.PlacementOptionLeaderConstraints:
		placementSettings.LeaderConstraints = stringVal
	case ast.PlacementOptionLearnerConstraints:
		placementSettings.LearnerConstraints = stringVal
	case ast.PlacementOptionFollowerConstraints:
		placementSettings.FollowerConstraints = stringVal
	case ast.PlacementOptionVoterConstraints:
		placementSettings.VoterConstraints = stringVal
	case ast.PlacementOptionSurvivalPreferences:
		placementSettings.SurvivalPreferences = stringVal
	default:
		return errors.Trace(errors.New("unknown placement policy option"))
	}
	return nil
}

// SetDirectResourceGroupSettings tries to set the ResourceGroupSettings.
func SetDirectResourceGroupSettings(groupInfo *model.ResourceGroupInfo, opt *ast.ResourceGroupOption) error {
	resourceGroupSettings := groupInfo.ResourceGroupSettings
	switch opt.Tp {
	case ast.ResourceRURate:
		resourceGroupSettings.RURate = opt.UintValue
	case ast.ResourcePriority:
		resourceGroupSettings.Priority = opt.UintValue
	case ast.ResourceUnitCPU:
		resourceGroupSettings.CPULimiter = opt.StrValue
	case ast.ResourceUnitIOReadBandwidth:
		resourceGroupSettings.IOReadBandwidth = opt.StrValue
	case ast.ResourceUnitIOWriteBandwidth:
		resourceGroupSettings.IOWriteBandwidth = opt.StrValue
	case ast.ResourceBurstableOpiton:
		// Some about BurstLimit(b):
		//   - If b == 0, that means the limiter is unlimited capacity. default use in resource controller (burst with a rate within a unlimited capacity).
		//   - If b < 0, that means the limiter is unlimited capacity and fillrate(r) is ignored, can be seen as r == Inf (burst with a inf rate within a unlimited capacity).
		//   - If b > 0, that means the limiter is limited capacity. (current not used).
		limit := int64(0)
		if opt.BoolValue {
			limit = -1
		}
		resourceGroupSettings.BurstLimit = limit
	case ast.ResourceGroupRunaway:
		if len(opt.RunawayOptionList) == 0 {
			resourceGroupSettings.Runaway = nil
		}
		for _, opt := range opt.RunawayOptionList {
			if err := SetDirectResourceGroupRunawayOption(resourceGroupSettings, opt.Tp, opt.StrValue, opt.IntValue); err != nil {
				return err
			}
		}
	case ast.ResourceGroupBackground:
		if groupInfo.Name.L != rg.DefaultResourceGroupName {
			// FIXME: this is a temporary restriction, so we don't add a error-code for it.
			return errors.New("unsupported operation. Currently, only the default resource group support change background settings")
		}
		if len(opt.BackgroundOptions) == 0 {
			resourceGroupSettings.Background = nil
		}
		for _, opt := range opt.BackgroundOptions {
			if err := SetDirectResourceGroupBackgroundOption(resourceGroupSettings, opt); err != nil {
				return err
			}
		}
	default:
		return errors.Trace(errors.New("unknown resource unit type"))
	}
	return nil
}

// SetDirectResourceGroupRunawayOption tries to set runaway part of the ResourceGroupSettings.
func SetDirectResourceGroupRunawayOption(resourceGroupSettings *model.ResourceGroupSettings, typ ast.RunawayOptionType, stringVal string, intVal int32) error {
	if resourceGroupSettings.Runaway == nil {
		resourceGroupSettings.Runaway = &model.ResourceGroupRunawaySettings{}
	}
	settings := resourceGroupSettings.Runaway
	switch typ {
	case ast.RunawayRule:
		// because execute time won't be too long, we use `time` pkg which does not support to parse unit 'd'.
		dur, err := time.ParseDuration(stringVal)
		if err != nil {
			return err
		}
		settings.ExecElapsedTimeMs = uint64(dur.Milliseconds())
	case ast.RunawayAction:
		settings.Action = model.RunawayActionType(intVal)
	case ast.RunawayWatch:
		settings.WatchType = model.RunawayWatchType(intVal)
		if len(stringVal) > 0 {
			dur, err := time.ParseDuration(stringVal)
			if err != nil {
				return err
			}
			settings.WatchDurationMs = dur.Milliseconds()
		} else {
			settings.WatchDurationMs = 0
		}
	default:
		return errors.Trace(errors.New("unknown runaway option type"))
	}
	return nil
}

// SetDirectResourceGroupBackgroundOption set background configs of the ResourceGroupSettings.
func SetDirectResourceGroupBackgroundOption(resourceGroupSettings *model.ResourceGroupSettings, opt *ast.ResourceGroupBackgroundOption) error {
	if resourceGroupSettings.Background == nil {
		resourceGroupSettings.Background = &model.ResourceGroupBackgroundSettings{}
	}
	switch opt.Type {
	case ast.BackgroundOptionTaskNames:
		jobTypes, err := parseBackgroundJobTypes(opt.StrValue)
		if err != nil {
			return err
		}
		resourceGroupSettings.Background.JobTypes = jobTypes
	default:
		return errors.Trace(errors.New("unknown background option type"))
	}
	return nil
}

func parseBackgroundJobTypes(t string) ([]string, error) {
	if len(t) == 0 {
		return []string{}, nil
	}

	segs := strings.Split(t, ",")
	res := make([]string, 0, len(segs))
	for _, s := range segs {
		ty := strings.ToLower(strings.TrimSpace(s))
		if len(ty) > 0 {
			if !slices.Contains(kvutil.ExplicitTypeList, ty) {
				return nil, infoschema.ErrResourceGroupInvalidBackgroundTaskName.GenWithStackByArgs(ty)
			}
			res = append(res, ty)
		}
	}
	return res, nil
}

// handleTableOptions updates tableInfo according to table options.
func handleTableOptions(options []*ast.TableOption, tbInfo *model.TableInfo) error {
	var ttlOptionsHandled bool

	for _, op := range options {
		switch op.Tp {
		case ast.TableOptionAutoIncrement:
			tbInfo.AutoIncID = int64(op.UintValue)
		case ast.TableOptionAutoIdCache:
			if op.UintValue > uint64(math.MaxInt64) {
				// TODO: Refine this error.
				return errors.New("table option auto_id_cache overflows int64")
			}
			tbInfo.AutoIdCache = int64(op.UintValue)
		case ast.TableOptionAutoRandomBase:
			tbInfo.AutoRandID = int64(op.UintValue)
		case ast.TableOptionComment:
			tbInfo.Comment = op.StrValue
		case ast.TableOptionCompression:
			tbInfo.Compression = op.StrValue
		case ast.TableOptionShardRowID:
			if op.UintValue > 0 && tbInfo.HasClusteredIndex() {
				return dbterror.ErrUnsupportedShardRowIDBits
			}
			tbInfo.ShardRowIDBits = op.UintValue
			if tbInfo.ShardRowIDBits > shardRowIDBitsMax {
				tbInfo.ShardRowIDBits = shardRowIDBitsMax
			}
			tbInfo.MaxShardRowIDBits = tbInfo.ShardRowIDBits
		case ast.TableOptionPreSplitRegion:
			if tbInfo.TempTableType != model.TempTableNone {
				return errors.Trace(dbterror.ErrOptOnTemporaryTable.GenWithStackByArgs("pre split regions"))
			}
			tbInfo.PreSplitRegions = op.UintValue
		case ast.TableOptionCharset, ast.TableOptionCollate:
			// We don't handle charset and collate here since they're handled in `GetCharsetAndCollateInTableOption`.
		case ast.TableOptionPlacementPolicy:
			tbInfo.PlacementPolicyRef = &model.PolicyRefInfo{
				Name: model.NewCIStr(op.StrValue),
			}
		case ast.TableOptionTTL, ast.TableOptionTTLEnable, ast.TableOptionTTLJobInterval:
			if ttlOptionsHandled {
				continue
			}

			ttlInfo, ttlEnable, ttlJobInterval, err := getTTLInfoInOptions(options)
			if err != nil {
				return err
			}
			// It's impossible that `ttlInfo` and `ttlEnable` are all nil, because we have met this option.
			// After exclude the situation `ttlInfo == nil && ttlEnable != nil`, we could say `ttlInfo != nil`
			if ttlInfo == nil {
				if ttlEnable != nil {
					return errors.Trace(dbterror.ErrSetTTLOptionForNonTTLTable.FastGenByArgs("TTL_ENABLE"))
				}
				if ttlJobInterval != nil {
					return errors.Trace(dbterror.ErrSetTTLOptionForNonTTLTable.FastGenByArgs("TTL_JOB_INTERVAL"))
				}
			}

			tbInfo.TTLInfo = ttlInfo
			ttlOptionsHandled = true
		}
	}
	shardingBits := shardingBits(tbInfo)
	if tbInfo.PreSplitRegions > shardingBits {
		tbInfo.PreSplitRegions = shardingBits
	}
	return nil
}

func shardingBits(tblInfo *model.TableInfo) uint64 {
	if tblInfo.ShardRowIDBits > 0 {
		return tblInfo.ShardRowIDBits
	}
	return tblInfo.AutoRandomBits
}

// isIgnorableSpec checks if the spec type is ignorable.
// Some specs are parsed by ignored. This is for compatibility.
func isIgnorableSpec(tp ast.AlterTableType) bool {
	// AlterTableLock/AlterTableAlgorithm are ignored.
	return tp == ast.AlterTableLock || tp == ast.AlterTableAlgorithm
}

// getCharsetAndCollateInColumnDef will iterate collate in the options, validate it by checking the charset
// of column definition. If there's no collate in the option, the default collate of column's charset will be used.
func getCharsetAndCollateInColumnDef(sessVars *variable.SessionVars, def *ast.ColumnDef) (chs, coll string, err error) {
	chs = def.Tp.GetCharset()
	coll = def.Tp.GetCollate()
	if chs != "" && coll == "" {
		if coll, err = GetDefaultCollation(sessVars, chs); err != nil {
			return "", "", errors.Trace(err)
		}
	}
	for _, opt := range def.Options {
		if opt.Tp == ast.ColumnOptionCollate {
			info, err := collate.GetCollationByName(opt.StrValue)
			if err != nil {
				return "", "", errors.Trace(err)
			}
			if chs == "" {
				chs = info.CharsetName
			} else if chs != info.CharsetName {
				return "", "", dbterror.ErrCollationCharsetMismatch.GenWithStackByArgs(info.Name, chs)
			}
			coll = info.Name
		}
	}
	return
}

// GetCharsetAndCollateInTableOption will iterate the charset and collate in the options,
// and returns the last charset and collate in options. If there is no charset in the options,
// the returns charset will be "", the same as collate.
func GetCharsetAndCollateInTableOption(sessVars *variable.SessionVars, startIdx int, options []*ast.TableOption) (chs, coll string, err error) {
	for i := startIdx; i < len(options); i++ {
		opt := options[i]
		// we set the charset to the last option. example: alter table t charset latin1 charset utf8 collate utf8_bin;
		// the charset will be utf8, collate will be utf8_bin
		switch opt.Tp {
		case ast.TableOptionCharset:
			info, err := charset.GetCharsetInfo(opt.StrValue)
			if err != nil {
				return "", "", err
			}
			if len(chs) == 0 {
				chs = info.Name
			} else if chs != info.Name {
				return "", "", dbterror.ErrConflictingDeclarations.GenWithStackByArgs(chs, info.Name)
			}
			if len(coll) == 0 {
				defaultColl, err := getDefaultCollationForUTF8MB4(sessVars, chs)
				if err != nil {
					return "", "", errors.Trace(err)
				}
				if len(defaultColl) == 0 {
					coll = info.DefaultCollation
				} else {
					coll = defaultColl
				}
			}
		case ast.TableOptionCollate:
			info, err := collate.GetCollationByName(opt.StrValue)
			if err != nil {
				return "", "", err
			}
			if len(chs) == 0 {
				chs = info.CharsetName
			} else if chs != info.CharsetName {
				return "", "", dbterror.ErrCollationCharsetMismatch.GenWithStackByArgs(info.Name, chs)
			}
			coll = info.Name
		}
	}
	return
}

// NeedToOverwriteColCharset return true for altering charset and specified CONVERT TO.
func NeedToOverwriteColCharset(options []*ast.TableOption) bool {
	for i := len(options) - 1; i >= 0; i-- {
		opt := options[i]
		if opt.Tp == ast.TableOptionCharset {
			// Only overwrite columns charset if the option contains `CONVERT TO`.
			return opt.UintValue == ast.TableOptionCharsetWithConvertTo
		}
	}
	return false
}

// resolveAlterTableAddColumns splits "add columns" to multiple spec. For example,
// `ALTER TABLE ADD COLUMN (c1 INT, c2 INT)` is split into
// `ALTER TABLE ADD COLUMN c1 INT, ADD COLUMN c2 INT`.
func resolveAlterTableAddColumns(spec *ast.AlterTableSpec) []*ast.AlterTableSpec {
	specs := make([]*ast.AlterTableSpec, 0, len(spec.NewColumns)+len(spec.NewConstraints))
	for _, col := range spec.NewColumns {
		t := *spec
		t.NewColumns = []*ast.ColumnDef{col}
		t.NewConstraints = []*ast.Constraint{}
		specs = append(specs, &t)
	}
	// Split the add constraints from AlterTableSpec.
	for _, con := range spec.NewConstraints {
		t := *spec
		t.NewColumns = []*ast.ColumnDef{}
		t.NewConstraints = []*ast.Constraint{}
		t.Constraint = con
		t.Tp = ast.AlterTableAddConstraint
		specs = append(specs, &t)
	}
	return specs
}

// ResolveAlterTableSpec resolves alter table algorithm and removes ignore table spec in specs.
// returns valid specs, and the occurred error.
func ResolveAlterTableSpec(ctx sessionctx.Context, specs []*ast.AlterTableSpec) ([]*ast.AlterTableSpec, error) {
	validSpecs := make([]*ast.AlterTableSpec, 0, len(specs))
	algorithm := ast.AlgorithmTypeDefault
	for _, spec := range specs {
		if spec.Tp == ast.AlterTableAlgorithm {
			// Find the last AlterTableAlgorithm.
			algorithm = spec.Algorithm
		}
		if isIgnorableSpec(spec.Tp) {
			continue
		}
		if spec.Tp == ast.AlterTableAddColumns && (len(spec.NewColumns) > 1 || len(spec.NewConstraints) > 0) {
			validSpecs = append(validSpecs, resolveAlterTableAddColumns(spec)...)
		} else {
			validSpecs = append(validSpecs, spec)
		}
		// TODO: Only allow REMOVE PARTITIONING as a single ALTER TABLE statement?
	}

	// Verify whether the algorithm is supported.
	for _, spec := range validSpecs {
		resolvedAlgorithm, err := ResolveAlterAlgorithm(spec, algorithm)
		if err != nil {
			// If TiDB failed to choose a better algorithm, report the error
			if resolvedAlgorithm == ast.AlgorithmTypeDefault {
				return nil, errors.Trace(err)
			}
			// For the compatibility, we return warning instead of error when a better algorithm is chosed by TiDB
			ctx.GetSessionVars().StmtCtx.AppendError(err)
		}

		spec.Algorithm = resolvedAlgorithm
	}

	// Only handle valid specs.
	return validSpecs, nil
}

func isMultiSchemaChanges(specs []*ast.AlterTableSpec) bool {
	if len(specs) > 1 {
		return true
	}
	if len(specs) == 1 && len(specs[0].NewColumns) > 1 && specs[0].Tp == ast.AlterTableAddColumns {
		return true
	}
	return false
}

func (d *ddl) AlterTable(ctx context.Context, sctx sessionctx.Context, stmt *ast.AlterTableStmt) (err error) {
	ident := ast.Ident{Schema: stmt.Table.Schema, Name: stmt.Table.Name}
	validSpecs, err := ResolveAlterTableSpec(sctx, stmt.Specs)
	if err != nil {
		return errors.Trace(err)
	}

	is := d.infoCache.GetLatest()
	tb, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(err)
	}
	if tb.Meta().IsView() || tb.Meta().IsSequence() {
		return dbterror.ErrWrongObject.GenWithStackByArgs(ident.Schema, ident.Name, "BASE TABLE")
	}
	if tb.Meta().TableCacheStatusType != model.TableCacheStatusDisable {
		if len(validSpecs) != 1 {
			return dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Alter Table")
		}
		if validSpecs[0].Tp != ast.AlterTableCache && validSpecs[0].Tp != ast.AlterTableNoCache {
			return dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Alter Table")
		}
	}
	if isMultiSchemaChanges(validSpecs) && (sctx.GetSessionVars().EnableRowLevelChecksum || variable.EnableRowLevelChecksum.Load()) {
		return dbterror.ErrRunMultiSchemaChanges.GenWithStack("Unsupported multi schema change when row level checksum is enabled")
	}
	// set name for anonymous foreign key.
	maxForeignKeyID := tb.Meta().MaxForeignKeyID
	for _, spec := range validSpecs {
		if spec.Tp == ast.AlterTableAddConstraint && spec.Constraint.Tp == ast.ConstraintForeignKey && spec.Constraint.Name == "" {
			maxForeignKeyID++
			spec.Constraint.Name = fmt.Sprintf("fk_%d", maxForeignKeyID)
		}
	}

	if len(validSpecs) > 1 {
		sctx.GetSessionVars().StmtCtx.MultiSchemaInfo = model.NewMultiSchemaInfo()
	}
	for _, spec := range validSpecs {
		var handledCharsetOrCollate bool
		var ttlOptionsHandled bool
		switch spec.Tp {
		case ast.AlterTableAddColumns:
			err = d.AddColumn(sctx, ident, spec)
		case ast.AlterTableAddPartitions, ast.AlterTableAddLastPartition:
			err = d.AddTablePartitions(sctx, ident, spec)
		case ast.AlterTableCoalescePartitions:
			err = d.CoalescePartitions(sctx, ident, spec)
		case ast.AlterTableReorganizePartition:
			err = d.ReorganizePartitions(sctx, ident, spec)
		case ast.AlterTableReorganizeFirstPartition:
			err = dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("MERGE FIRST PARTITION")
		case ast.AlterTableReorganizeLastPartition:
			err = dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("SPLIT LAST PARTITION")
		case ast.AlterTableCheckPartitions:
			err = errors.Trace(dbterror.ErrUnsupportedCheckPartition)
		case ast.AlterTableRebuildPartition:
			err = errors.Trace(dbterror.ErrUnsupportedRebuildPartition)
		case ast.AlterTableOptimizePartition:
			err = errors.Trace(dbterror.ErrUnsupportedOptimizePartition)
		case ast.AlterTableRemovePartitioning:
			err = d.RemovePartitioning(sctx, ident, spec)
		case ast.AlterTableRepairPartition:
			err = errors.Trace(dbterror.ErrUnsupportedRepairPartition)
		case ast.AlterTableDropColumn:
			err = d.DropColumn(sctx, ident, spec)
		case ast.AlterTableDropIndex:
			err = d.dropIndex(sctx, ident, model.NewCIStr(spec.Name), spec.IfExists, false)
		case ast.AlterTableDropPrimaryKey:
			err = d.dropIndex(sctx, ident, model.NewCIStr(mysql.PrimaryKeyName), spec.IfExists, false)
		case ast.AlterTableRenameIndex:
			err = d.RenameIndex(sctx, ident, spec)
		case ast.AlterTableDropPartition, ast.AlterTableDropFirstPartition:
			err = d.DropTablePartition(sctx, ident, spec)
		case ast.AlterTableTruncatePartition:
			err = d.TruncateTablePartition(sctx, ident, spec)
		case ast.AlterTableWriteable:
			if !config.TableLockEnabled() {
				return nil
			}
			tName := &ast.TableName{Schema: ident.Schema, Name: ident.Name}
			if spec.Writeable {
				err = d.CleanupTableLock(sctx, []*ast.TableName{tName})
			} else {
				lockStmt := &ast.LockTablesStmt{
					TableLocks: []ast.TableLock{
						{
							Table: tName,
							Type:  model.TableLockReadOnly,
						},
					},
				}
				err = d.LockTables(sctx, lockStmt)
			}
		case ast.AlterTableExchangePartition:
			err = d.ExchangeTablePartition(sctx, ident, spec)
		case ast.AlterTableAddConstraint:
			constr := spec.Constraint
			switch spec.Constraint.Tp {
			case ast.ConstraintKey, ast.ConstraintIndex:
				err = d.createIndex(sctx, ident, ast.IndexKeyTypeNone, model.NewCIStr(constr.Name),
					spec.Constraint.Keys, constr.Option, constr.IfNotExists)
			case ast.ConstraintUniq, ast.ConstraintUniqIndex, ast.ConstraintUniqKey:
				err = d.createIndex(sctx, ident, ast.IndexKeyTypeUnique, model.NewCIStr(constr.Name),
					spec.Constraint.Keys, constr.Option, false) // IfNotExists should be not applied
			case ast.ConstraintForeignKey:
				// NOTE: we do not handle `symbol` and `index_name` well in the parser and we do not check ForeignKey already exists,
				// so we just also ignore the `if not exists` check.
				err = d.CreateForeignKey(sctx, ident, model.NewCIStr(constr.Name), spec.Constraint.Keys, spec.Constraint.Refer)
			case ast.ConstraintPrimaryKey:
				err = d.CreatePrimaryKey(sctx, ident, model.NewCIStr(constr.Name), spec.Constraint.Keys, constr.Option)
			case ast.ConstraintFulltext:
				sctx.GetSessionVars().StmtCtx.AppendWarning(dbterror.ErrTableCantHandleFt)
			case ast.ConstraintCheck:
				if !variable.EnableCheckConstraint.Load() {
					sctx.GetSessionVars().StmtCtx.AppendWarning(errCheckConstraintIsOff)
				} else {
					err = d.CreateCheckConstraint(sctx, ident, model.NewCIStr(constr.Name), spec.Constraint)
				}
			default:
				// Nothing to do now.
			}
		case ast.AlterTableDropForeignKey:
			// NOTE: we do not check `if not exists` and `if exists` for ForeignKey now.
			err = d.DropForeignKey(sctx, ident, model.NewCIStr(spec.Name))
		case ast.AlterTableModifyColumn:
			err = d.ModifyColumn(ctx, sctx, ident, spec)
		case ast.AlterTableChangeColumn:
			err = d.ChangeColumn(ctx, sctx, ident, spec)
		case ast.AlterTableRenameColumn:
			err = d.RenameColumn(sctx, ident, spec)
		case ast.AlterTableAlterColumn:
			err = d.AlterColumn(sctx, ident, spec)
		case ast.AlterTableRenameTable:
			newIdent := ast.Ident{Schema: spec.NewTable.Schema, Name: spec.NewTable.Name}
			isAlterTable := true
			err = d.renameTable(sctx, ident, newIdent, isAlterTable)
		case ast.AlterTablePartition:
			err = d.AlterTablePartitioning(sctx, ident, spec)
		case ast.AlterTableOption:
			var placementPolicyRef *model.PolicyRefInfo
			for i, opt := range spec.Options {
				switch opt.Tp {
				case ast.TableOptionShardRowID:
					if opt.UintValue > shardRowIDBitsMax {
						opt.UintValue = shardRowIDBitsMax
					}
					err = d.ShardRowID(sctx, ident, opt.UintValue)
				case ast.TableOptionAutoIncrement:
					err = d.RebaseAutoID(sctx, ident, int64(opt.UintValue), autoid.AutoIncrementType, opt.BoolValue)
				case ast.TableOptionAutoIdCache:
					if opt.UintValue > uint64(math.MaxInt64) {
						// TODO: Refine this error.
						return errors.New("table option auto_id_cache overflows int64")
					}
					err = d.AlterTableAutoIDCache(sctx, ident, int64(opt.UintValue))
				case ast.TableOptionAutoRandomBase:
					err = d.RebaseAutoID(sctx, ident, int64(opt.UintValue), autoid.AutoRandomType, opt.BoolValue)
				case ast.TableOptionComment:
					spec.Comment = opt.StrValue
					err = d.AlterTableComment(sctx, ident, spec)
				case ast.TableOptionCharset, ast.TableOptionCollate:
					// GetCharsetAndCollateInTableOption will get the last charset and collate in the options,
					// so it should be handled only once.
					if handledCharsetOrCollate {
						continue
					}
					var toCharset, toCollate string
					toCharset, toCollate, err = GetCharsetAndCollateInTableOption(sctx.GetSessionVars(), i, spec.Options)
					if err != nil {
						return err
					}
					needsOverwriteCols := NeedToOverwriteColCharset(spec.Options)
					err = d.AlterTableCharsetAndCollate(sctx, ident, toCharset, toCollate, needsOverwriteCols)
					handledCharsetOrCollate = true
				case ast.TableOptionPlacementPolicy:
					placementPolicyRef = &model.PolicyRefInfo{
						Name: model.NewCIStr(opt.StrValue),
					}
				case ast.TableOptionEngine:
				case ast.TableOptionRowFormat:
				case ast.TableOptionTTL, ast.TableOptionTTLEnable, ast.TableOptionTTLJobInterval:
					var ttlInfo *model.TTLInfo
					var ttlEnable *bool
					var ttlJobInterval *string

					if ttlOptionsHandled {
						continue
					}
					ttlInfo, ttlEnable, ttlJobInterval, err = getTTLInfoInOptions(spec.Options)
					if err != nil {
						return err
					}
					err = d.AlterTableTTLInfoOrEnable(sctx, ident, ttlInfo, ttlEnable, ttlJobInterval)

					ttlOptionsHandled = true
				default:
					err = dbterror.ErrUnsupportedAlterTableOption
				}

				if err != nil {
					return errors.Trace(err)
				}
			}

			if placementPolicyRef != nil {
				err = d.AlterTablePlacement(sctx, ident, placementPolicyRef)
			}
		case ast.AlterTableSetTiFlashReplica:
			err = d.AlterTableSetTiFlashReplica(sctx, ident, spec.TiFlashReplica)
		case ast.AlterTableOrderByColumns:
			err = d.OrderByColumns(sctx, ident)
		case ast.AlterTableIndexInvisible:
			err = d.AlterIndexVisibility(sctx, ident, spec.IndexName, spec.Visibility)
		case ast.AlterTableAlterCheck:
			if !variable.EnableCheckConstraint.Load() {
				sctx.GetSessionVars().StmtCtx.AppendWarning(errCheckConstraintIsOff)
			} else {
				err = d.AlterCheckConstraint(sctx, ident, model.NewCIStr(spec.Constraint.Name), spec.Constraint.Enforced)
			}
		case ast.AlterTableDropCheck:
			if !variable.EnableCheckConstraint.Load() {
				sctx.GetSessionVars().StmtCtx.AppendWarning(errCheckConstraintIsOff)
			} else {
				err = d.DropCheckConstraint(sctx, ident, model.NewCIStr(spec.Constraint.Name))
			}
		case ast.AlterTableWithValidation:
			sctx.GetSessionVars().StmtCtx.AppendWarning(dbterror.ErrUnsupportedAlterTableWithValidation)
		case ast.AlterTableWithoutValidation:
			sctx.GetSessionVars().StmtCtx.AppendWarning(dbterror.ErrUnsupportedAlterTableWithoutValidation)
		case ast.AlterTableAddStatistics:
			err = d.AlterTableAddStatistics(sctx, ident, spec.Statistics, spec.IfNotExists)
		case ast.AlterTableDropStatistics:
			err = d.AlterTableDropStatistics(sctx, ident, spec.Statistics, spec.IfExists)
		case ast.AlterTableAttributes:
			err = d.AlterTableAttributes(sctx, ident, spec)
		case ast.AlterTablePartitionAttributes:
			err = d.AlterTablePartitionAttributes(sctx, ident, spec)
		case ast.AlterTablePartitionOptions:
			err = d.AlterTablePartitionOptions(sctx, ident, spec)
		case ast.AlterTableCache:
			err = d.AlterTableCache(sctx, ident)
		case ast.AlterTableNoCache:
			err = d.AlterTableNoCache(sctx, ident)
		case ast.AlterTableDisableKeys, ast.AlterTableEnableKeys:
			// Nothing to do now, see https://github.com/pingcap/tidb/issues/1051
			// MyISAM specific
		case ast.AlterTableRemoveTTL:
			// the parser makes sure we have only one `ast.AlterTableRemoveTTL` in an alter statement
			err = d.AlterTableRemoveTTL(sctx, ident)
		default:
			err = errors.Trace(dbterror.ErrUnsupportedAlterTableSpec)
		}

		if err != nil {
			return errors.Trace(err)
		}
	}

	if sctx.GetSessionVars().StmtCtx.MultiSchemaInfo != nil {
		err = d.MultiSchemaChange(sctx, ident)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (d *ddl) RebaseAutoID(ctx sessionctx.Context, ident ast.Ident, newBase int64, tp autoid.AllocatorType, force bool) error {
	schema, t, err := d.getSchemaAndTableByIdent(ctx, ident)
	if err != nil {
		return errors.Trace(err)
	}
	tbInfo := t.Meta()
	var actionType model.ActionType
	switch tp {
	case autoid.AutoRandomType:
		pkCol := tbInfo.GetPkColInfo()
		if tbInfo.AutoRandomBits == 0 || pkCol == nil {
			return errors.Trace(dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomRebaseNotApplicable))
		}
		shardFmt := autoid.NewShardIDFormat(&pkCol.FieldType, tbInfo.AutoRandomBits, tbInfo.AutoRandomRangeBits)
		if shardFmt.IncrementalMask()&newBase != newBase {
			errMsg := fmt.Sprintf(autoid.AutoRandomRebaseOverflow, newBase, shardFmt.IncrementalBitsCapacity())
			return errors.Trace(dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(errMsg))
		}
		actionType = model.ActionRebaseAutoRandomBase
	case autoid.RowIDAllocType:
		actionType = model.ActionRebaseAutoID
	case autoid.AutoIncrementType:
		actionType = model.ActionRebaseAutoID
	default:
		panic(fmt.Sprintf("unimplemented rebase autoid type %s", tp))
	}

	if !force {
		newBaseTemp, err := adjustNewBaseToNextGlobalID(ctx.GetTableCtx(), t, tp, newBase)
		if err != nil {
			return err
		}
		if newBase != newBaseTemp {
			ctx.GetSessionVars().StmtCtx.AppendWarning(
				errors.NewNoStackErrorf("Can't reset AUTO_INCREMENT to %d without FORCE option, using %d instead",
					newBase, newBaseTemp,
				))
		}
		newBase = newBaseTemp
	}
	job := &model.Job{
		SchemaID:       schema.ID,
		TableID:        tbInfo.ID,
		SchemaName:     schema.Name.L,
		TableName:      tbInfo.Name.L,
		Type:           actionType,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           []any{newBase, force},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

func adjustNewBaseToNextGlobalID(ctx table.AllocatorContext, t table.Table, tp autoid.AllocatorType, newBase int64) (int64, error) {
	alloc := t.Allocators(ctx).Get(tp)
	if alloc == nil {
		return newBase, nil
	}
	autoID, err := alloc.NextGlobalAutoID()
	if err != nil {
		return newBase, errors.Trace(err)
	}
	// If newBase < autoID, we need to do a rebase before returning.
	// Assume there are 2 TiDB servers: TiDB-A with allocator range of 0 ~ 30000; TiDB-B with allocator range of 30001 ~ 60000.
	// If the user sends SQL `alter table t1 auto_increment = 100` to TiDB-B,
	// and TiDB-B finds 100 < 30001 but returns without any handling,
	// then TiDB-A may still allocate 99 for auto_increment column. This doesn't make sense for the user.
	return int64(mathutil.Max(uint64(newBase), uint64(autoID))), nil
}

// ShardRowID shards the implicit row ID by adding shard value to the row ID's first few bits.
func (d *ddl) ShardRowID(ctx sessionctx.Context, tableIdent ast.Ident, uVal uint64) error {
	schema, t, err := d.getSchemaAndTableByIdent(ctx, tableIdent)
	if err != nil {
		return errors.Trace(err)
	}
	tbInfo := t.Meta()
	if tbInfo.TempTableType != model.TempTableNone {
		return dbterror.ErrOptOnTemporaryTable.GenWithStackByArgs("shard_row_id_bits")
	}
	if uVal == tbInfo.ShardRowIDBits {
		// Nothing need to do.
		return nil
	}
	if uVal > 0 && tbInfo.HasClusteredIndex() {
		return dbterror.ErrUnsupportedShardRowIDBits
	}
	err = verifyNoOverflowShardBits(d.sessPool, t, uVal)
	if err != nil {
		return err
	}
	job := &model.Job{
		Type:           model.ActionShardRowID,
		SchemaID:       schema.ID,
		TableID:        tbInfo.ID,
		SchemaName:     schema.Name.L,
		TableName:      tbInfo.Name.L,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           []any{uVal},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

func (d *ddl) getSchemaAndTableByIdent(ctx sessionctx.Context, tableIdent ast.Ident) (dbInfo *model.DBInfo, t table.Table, err error) {
	is := d.GetInfoSchemaWithInterceptor(ctx)
	schema, ok := is.SchemaByName(tableIdent.Schema)
	if !ok {
		return nil, nil, infoschema.ErrDatabaseNotExists.GenWithStackByArgs(tableIdent.Schema)
	}
	t, err = is.TableByName(tableIdent.Schema, tableIdent.Name)
	if err != nil {
		return nil, nil, infoschema.ErrTableNotExists.GenWithStackByArgs(tableIdent.Schema, tableIdent.Name)
	}
	return schema, t, nil
}

func checkUnsupportedColumnConstraint(col *ast.ColumnDef, ti ast.Ident) error {
	for _, constraint := range col.Options {
		switch constraint.Tp {
		case ast.ColumnOptionAutoIncrement:
			return dbterror.ErrUnsupportedAddColumn.GenWithStack("unsupported add column '%s' constraint AUTO_INCREMENT when altering '%s.%s'", col.Name, ti.Schema, ti.Name)
		case ast.ColumnOptionPrimaryKey:
			return dbterror.ErrUnsupportedAddColumn.GenWithStack("unsupported add column '%s' constraint PRIMARY KEY when altering '%s.%s'", col.Name, ti.Schema, ti.Name)
		case ast.ColumnOptionUniqKey:
			return dbterror.ErrUnsupportedAddColumn.GenWithStack("unsupported add column '%s' constraint UNIQUE KEY when altering '%s.%s'", col.Name, ti.Schema, ti.Name)
		case ast.ColumnOptionAutoRandom:
			errMsg := fmt.Sprintf(autoid.AutoRandomAlterAddColumn, col.Name, ti.Schema, ti.Name)
			return dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(errMsg)
		}
	}

	return nil
}

func checkAndCreateNewColumn(ctx sessionctx.Context, ti ast.Ident, schema *model.DBInfo, spec *ast.AlterTableSpec, t table.Table, specNewColumn *ast.ColumnDef) (*table.Column, error) {
	err := checkUnsupportedColumnConstraint(specNewColumn, ti)
	if err != nil {
		return nil, errors.Trace(err)
	}

	colName := specNewColumn.Name.Name.O
	// Check whether added column has existed.
	col := table.FindCol(t.Cols(), colName)
	if col != nil {
		err = infoschema.ErrColumnExists.GenWithStackByArgs(colName)
		if spec.IfNotExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil, nil
		}
		return nil, err
	}
	if err = checkColumnAttributes(colName, specNewColumn.Tp); err != nil {
		return nil, errors.Trace(err)
	}
	if utf8.RuneCountInString(colName) > mysql.MaxColumnNameLength {
		return nil, dbterror.ErrTooLongIdent.GenWithStackByArgs(colName)
	}

	return CreateNewColumn(ctx, schema, spec, t, specNewColumn)
}

// CreateNewColumn creates a new column according to the column information.
func CreateNewColumn(ctx sessionctx.Context, schema *model.DBInfo, spec *ast.AlterTableSpec, t table.Table, specNewColumn *ast.ColumnDef) (*table.Column, error) {
	// If new column is a generated column, do validation.
	// NOTE: we do check whether the column refers other generated
	// columns occurring later in a table, but we don't handle the col offset.
	for _, option := range specNewColumn.Options {
		if option.Tp == ast.ColumnOptionGenerated {
			if err := checkIllegalFn4Generated(specNewColumn.Name.Name.L, typeColumn, option.Expr); err != nil {
				return nil, errors.Trace(err)
			}

			if option.Stored {
				return nil, dbterror.ErrUnsupportedOnGeneratedColumn.GenWithStackByArgs("Adding generated stored column through ALTER TABLE")
			}

			_, dependColNames, err := findDependedColumnNames(schema.Name, t.Meta().Name, specNewColumn)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if !ctx.GetSessionVars().EnableAutoIncrementInGenerated {
				if err := checkAutoIncrementRef(specNewColumn.Name.Name.L, dependColNames, t.Meta()); err != nil {
					return nil, errors.Trace(err)
				}
			}
			duplicateColNames := make(map[string]struct{}, len(dependColNames))
			for k := range dependColNames {
				duplicateColNames[k] = struct{}{}
			}
			cols := t.Cols()

			if err := checkDependedColExist(dependColNames, cols); err != nil {
				return nil, errors.Trace(err)
			}

			if err := verifyColumnGenerationSingle(duplicateColNames, cols, spec.Position); err != nil {
				return nil, errors.Trace(err)
			}
		}
		// Specially, since sequence has been supported, if a newly added column has a
		// sequence nextval function as it's default value option, it won't fill the
		// known rows with specific sequence next value under current add column logic.
		// More explanation can refer: TestSequenceDefaultLogic's comment in sequence_test.go
		if option.Tp == ast.ColumnOptionDefaultValue {
			if f, ok := option.Expr.(*ast.FuncCallExpr); ok {
				switch f.FnName.L {
				case ast.NextVal:
					if _, err := getSequenceDefaultValue(option); err != nil {
						return nil, errors.Trace(err)
					}
					return nil, errors.Trace(dbterror.ErrAddColumnWithSequenceAsDefault.GenWithStackByArgs(specNewColumn.Name.Name.O))
				case ast.Rand, ast.UUID, ast.UUIDToBin, ast.Replace, ast.Upper:
					return nil, errors.Trace(dbterror.ErrBinlogUnsafeSystemFunction.GenWithStackByArgs())
				}
			}
		}
	}

	tableCharset, tableCollate, err := ResolveCharsetCollation(ctx.GetSessionVars(),
		ast.CharsetOpt{Chs: t.Meta().Charset, Col: t.Meta().Collate},
		ast.CharsetOpt{Chs: schema.Charset, Col: schema.Collate},
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Ignore table constraints now, they will be checked later.
	// We use length(t.Cols()) as the default offset firstly, we will change the column's offset later.
	col, _, err := buildColumnAndConstraint(
		ctx,
		len(t.Cols()),
		specNewColumn,
		nil,
		tableCharset,
		tableCollate,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	originDefVal, err := generateOriginDefaultValue(col.ToInfo(), ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = col.SetOriginDefaultValue(originDefVal)
	return col, err
}

// AddColumn will add a new column to the table.
func (d *ddl) AddColumn(ctx sessionctx.Context, ti ast.Ident, spec *ast.AlterTableSpec) error {
	specNewColumn := spec.NewColumns[0]
	schema, t, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		return errors.Trace(err)
	}
	tbInfo := t.Meta()
	if err = checkAddColumnTooManyColumns(len(t.Cols()) + 1); err != nil {
		return errors.Trace(err)
	}
	col, err := checkAndCreateNewColumn(ctx, ti, schema, spec, t, specNewColumn)
	if err != nil {
		return errors.Trace(err)
	}
	// Added column has existed and if_not_exists flag is true.
	if col == nil {
		return nil
	}
	err = CheckAfterPositionExists(tbInfo, spec.Position)
	if err != nil {
		return errors.Trace(err)
	}

	txn, err := ctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	bdrRole, err := meta.NewMeta(txn).GetBDRRole()
	if err != nil {
		return errors.Trace(err)
	}
	if bdrRole == string(ast.BDRRolePrimary) && deniedByBDRWhenAddColumn(specNewColumn.Options) {
		return dbterror.ErrBDRRestrictedDDL.FastGenByArgs(bdrRole)
	}

	job := &model.Job{
		SchemaID:       schema.ID,
		TableID:        tbInfo.ID,
		SchemaName:     schema.Name.L,
		TableName:      tbInfo.Name.L,
		Type:           model.ActionAddColumn,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           []any{col, spec.Position, 0, spec.IfNotExists},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

// AddTablePartitions will add a new partition to the table.
func (d *ddl) AddTablePartitions(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	is := d.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schema))
	}
	t, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}

	meta := t.Meta()
	pi := meta.GetPartitionInfo()
	if pi == nil {
		return errors.Trace(dbterror.ErrPartitionMgmtOnNonpartitioned)
	}
	if pi.Type == model.PartitionTypeHash || pi.Type == model.PartitionTypeKey {
		// Add partition for hash/key is actually a reorganize partition
		// operation and not a metadata only change!
		switch spec.Tp {
		case ast.AlterTableAddLastPartition:
			return errors.Trace(dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("LAST PARTITION of HASH/KEY partitioned table"))
		case ast.AlterTableAddPartitions:
		// only thing supported
		default:
			return errors.Trace(dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("ADD PARTITION of HASH/KEY partitioned table"))
		}
		return d.hashPartitionManagement(ctx, ident, spec, pi)
	}

	partInfo, err := BuildAddedPartitionInfo(ctx.GetExprCtx(), meta, spec)
	if err != nil {
		return errors.Trace(err)
	}
	if pi.Type == model.PartitionTypeList {
		// TODO: make sure that checks in ddl_api and ddl_worker is the same.
		err = checkAddListPartitions(meta)
		if err != nil {
			return errors.Trace(err)
		}
	}
	if err := d.assignPartitionIDs(partInfo.Definitions); err != nil {
		return errors.Trace(err)
	}

	// partInfo contains only the new added partition, we have to combine it with the
	// old partitions to check all partitions is strictly increasing.
	clonedMeta := meta.Clone()
	tmp := *partInfo
	tmp.Definitions = append(pi.Definitions, tmp.Definitions...)
	clonedMeta.Partition = &tmp
	if err := checkPartitionDefinitionConstraints(ctx, clonedMeta); err != nil {
		if dbterror.ErrSameNamePartition.Equal(err) && spec.IfNotExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return errors.Trace(err)
	}

	if err = handlePartitionPlacement(ctx, partInfo); err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		SchemaID:       schema.ID,
		TableID:        meta.ID,
		SchemaName:     schema.Name.L,
		TableName:      t.Meta().Name.L,
		Type:           model.ActionAddTablePartition,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           []any{partInfo},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	if spec.Tp == ast.AlterTableAddLastPartition && spec.Partition != nil {
		query, ok := ctx.Value(sessionctx.QueryString).(string)
		if ok {
			sqlMode := ctx.GetSessionVars().SQLMode
			var buf bytes.Buffer
			AppendPartitionDefs(partInfo, &buf, sqlMode)

			syntacticSugar := spec.Partition.PartitionMethod.OriginalText()
			syntacticStart := spec.Partition.PartitionMethod.OriginTextPosition()
			newQuery := query[:syntacticStart] + "ADD PARTITION (" + buf.String() + ")" + query[syntacticStart+len(syntacticSugar):]
			defer ctx.SetValue(sessionctx.QueryString, query)
			ctx.SetValue(sessionctx.QueryString, newQuery)
		}
	}
	err = d.DoDDLJob(ctx, job)
	if dbterror.ErrSameNamePartition.Equal(err) && spec.IfNotExists {
		ctx.GetSessionVars().StmtCtx.AppendNote(err)
		return nil
	}
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

// getReorganizedDefinitions return the definitions as they would look like after the REORGANIZE PARTITION is done.
func getReorganizedDefinitions(pi *model.PartitionInfo, firstPartIdx, lastPartIdx int, idMap map[int]struct{}) []model.PartitionDefinition {
	tmpDefs := make([]model.PartitionDefinition, 0, len(pi.Definitions)+len(pi.AddingDefinitions)-len(idMap))
	if pi.Type == model.PartitionTypeList {
		replaced := false
		for i := range pi.Definitions {
			if _, ok := idMap[i]; ok {
				if !replaced {
					tmpDefs = append(tmpDefs, pi.AddingDefinitions...)
					replaced = true
				}
				continue
			}
			tmpDefs = append(tmpDefs, pi.Definitions[i])
		}
		if !replaced {
			// For safety, for future non-partitioned table -> partitioned
			tmpDefs = append(tmpDefs, pi.AddingDefinitions...)
		}
		return tmpDefs
	}
	// Range
	tmpDefs = append(tmpDefs, pi.Definitions[:firstPartIdx]...)
	tmpDefs = append(tmpDefs, pi.AddingDefinitions...)
	if len(pi.Definitions) > (lastPartIdx + 1) {
		tmpDefs = append(tmpDefs, pi.Definitions[lastPartIdx+1:]...)
	}
	return tmpDefs
}

func getReplacedPartitionIDs(names []string, pi *model.PartitionInfo) (firstPartIdx int, lastPartIdx int, idMap map[int]struct{}, err error) {
	idMap = make(map[int]struct{})
	firstPartIdx, lastPartIdx = -1, -1
	for _, name := range names {
		nameL := strings.ToLower(name)
		partIdx := pi.FindPartitionDefinitionByName(nameL)
		if partIdx == -1 {
			return 0, 0, nil, errors.Trace(dbterror.ErrWrongPartitionName)
		}
		if _, ok := idMap[partIdx]; ok {
			return 0, 0, nil, errors.Trace(dbterror.ErrSameNamePartition)
		}
		idMap[partIdx] = struct{}{}
		if firstPartIdx == -1 {
			firstPartIdx = partIdx
		} else {
			firstPartIdx = mathutil.Min[int](firstPartIdx, partIdx)
		}
		if lastPartIdx == -1 {
			lastPartIdx = partIdx
		} else {
			lastPartIdx = mathutil.Max[int](lastPartIdx, partIdx)
		}
	}
	switch pi.Type {
	case model.PartitionTypeRange:
		if len(idMap) != (lastPartIdx - firstPartIdx + 1) {
			return 0, 0, nil, errors.Trace(dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs(
				"REORGANIZE PARTITION of RANGE; not adjacent partitions"))
		}
	case model.PartitionTypeHash, model.PartitionTypeKey:
		if len(idMap) != len(pi.Definitions) {
			return 0, 0, nil, errors.Trace(dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs(
				"REORGANIZE PARTITION of HASH/RANGE; must reorganize all partitions"))
		}
	}

	return firstPartIdx, lastPartIdx, idMap, nil
}

func getPartitionInfoTypeNone() *model.PartitionInfo {
	return &model.PartitionInfo{
		Type:   model.PartitionTypeNone,
		Enable: true,
		Definitions: []model.PartitionDefinition{{
			Name:    model.NewCIStr("pFullTable"),
			Comment: "Intermediate partition during ALTER TABLE ... PARTITION BY ...",
		}},
		Num: 1,
	}
}

// AlterTablePartitioning reorganize one set of partitions to a new set of partitions.
func (d *ddl) AlterTablePartitioning(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	schema, t, err := d.getSchemaAndTableByIdent(ctx, ident)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.FastGenByArgs(ident.Schema, ident.Name))
	}

	meta := t.Meta().Clone()
	piOld := meta.GetPartitionInfo()
	var partNames []string
	if piOld != nil {
		partNames = make([]string, 0, len(piOld.Definitions))
		for i := range piOld.Definitions {
			partNames = append(partNames, piOld.Definitions[i].Name.L)
		}
	} else {
		piOld = getPartitionInfoTypeNone()
		meta.Partition = piOld
		partNames = append(partNames, piOld.Definitions[0].Name.L)
	}
	newMeta := meta.Clone()
	err = buildTablePartitionInfo(ctx, spec.Partition, newMeta)
	if err != nil {
		return err
	}
	newPartInfo := newMeta.Partition

	for _, index := range newMeta.Indices {
		if index.Unique {
			ck, err := checkPartitionKeysConstraint(newMeta.GetPartitionInfo(), index.Columns, newMeta)
			if err != nil {
				return err
			}
			if !ck {
				if ctx.GetSessionVars().EnableGlobalIndex {
					return dbterror.ErrCancelledDDLJob.GenWithStack("global index is not supported yet for alter table partitioning")
				}
				indexTp := "UNIQUE INDEX"
				if index.Primary {
					indexTp = "PRIMARY"
				}
				return dbterror.ErrUniqueKeyNeedAllFieldsInPf.GenWithStackByArgs(indexTp)
			}
		}
	}
	if newMeta.PKIsHandle {
		indexCols := []*model.IndexColumn{{
			Name:   newMeta.GetPkName(),
			Length: types.UnspecifiedLength,
		}}
		ck, err := checkPartitionKeysConstraint(newMeta.GetPartitionInfo(), indexCols, newMeta)
		if err != nil {
			return err
		}
		if !ck {
			return dbterror.ErrUniqueKeyNeedAllFieldsInPf.GenWithStackByArgs("PRIMARY")
		}
	}

	if err = handlePartitionPlacement(ctx, newPartInfo); err != nil {
		return errors.Trace(err)
	}

	if err = d.assignPartitionIDs(newPartInfo.Definitions); err != nil {
		return errors.Trace(err)
	}
	// A new table ID would be needed for
	// the global index, which cannot be the same as the current table id,
	// since this table id will be removed in the final state when removing
	// all the data with this table id.
	var newID []int64
	newID, err = d.genGlobalIDs(1)
	if err != nil {
		return errors.Trace(err)
	}
	newPartInfo.NewTableID = newID[0]
	newPartInfo.DDLType = piOld.Type

	job := &model.Job{
		SchemaID:       schema.ID,
		TableID:        meta.ID,
		SchemaName:     schema.Name.L,
		TableName:      t.Meta().Name.L,
		Type:           model.ActionAlterTablePartitioning,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           []any{partNames, newPartInfo},
		ReorgMeta:      NewDDLReorgMeta(ctx),
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	// No preSplitAndScatter here, it will be done by the worker in onReorganizePartition instead.
	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	if err == nil {
		ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError("The statistics of new partitions will be outdated after reorganizing partitions. Please use 'ANALYZE TABLE' statement if you want to update it now"))
	}
	return errors.Trace(err)
}

// ReorganizePartitions reorganize one set of partitions to a new set of partitions.
func (d *ddl) ReorganizePartitions(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	schema, t, err := d.getSchemaAndTableByIdent(ctx, ident)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.FastGenByArgs(ident.Schema, ident.Name))
	}

	meta := t.Meta()
	pi := meta.GetPartitionInfo()
	if pi == nil {
		return dbterror.ErrPartitionMgmtOnNonpartitioned
	}
	switch pi.Type {
	case model.PartitionTypeRange, model.PartitionTypeList:
	case model.PartitionTypeHash, model.PartitionTypeKey:
		if spec.Tp != ast.AlterTableCoalescePartitions &&
			spec.Tp != ast.AlterTableAddPartitions {
			return errors.Trace(dbterror.ErrUnsupportedReorganizePartition)
		}
	default:
		return errors.Trace(dbterror.ErrUnsupportedReorganizePartition)
	}
	partNames := make([]string, 0, len(spec.PartitionNames))
	for _, name := range spec.PartitionNames {
		partNames = append(partNames, name.L)
	}
	firstPartIdx, lastPartIdx, idMap, err := getReplacedPartitionIDs(partNames, pi)
	if err != nil {
		return errors.Trace(err)
	}
	partInfo, err := BuildAddedPartitionInfo(ctx.GetExprCtx(), meta, spec)
	if err != nil {
		return errors.Trace(err)
	}
	if err = d.assignPartitionIDs(partInfo.Definitions); err != nil {
		return errors.Trace(err)
	}
	if err = checkReorgPartitionDefs(ctx, model.ActionReorganizePartition, meta, partInfo, firstPartIdx, lastPartIdx, idMap); err != nil {
		return errors.Trace(err)
	}
	if err = handlePartitionPlacement(ctx, partInfo); err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		SchemaID:       schema.ID,
		TableID:        meta.ID,
		SchemaName:     schema.Name.L,
		TableName:      t.Meta().Name.L,
		Type:           model.ActionReorganizePartition,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           []any{partNames, partInfo},
		ReorgMeta:      NewDDLReorgMeta(ctx),
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	// No preSplitAndScatter here, it will be done by the worker in onReorganizePartition instead.
	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	if err == nil {
		ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError("The statistics of related partitions will be outdated after reorganizing partitions. Please use 'ANALYZE TABLE' statement if you want to update it now"))
	}
	return errors.Trace(err)
}

// RemovePartitioning removes partitioning from a table.
func (d *ddl) RemovePartitioning(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	schema, t, err := d.getSchemaAndTableByIdent(ctx, ident)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.FastGenByArgs(ident.Schema, ident.Name))
	}

	meta := t.Meta().Clone()
	pi := meta.GetPartitionInfo()
	if pi == nil {
		return dbterror.ErrPartitionMgmtOnNonpartitioned
	}
	// TODO: Optimize for remove partitioning with a single partition
	// TODO: Add the support for this in onReorganizePartition
	// skip if only one partition
	// If there are only one partition, then we can do:
	// change the table id to the partition id
	// and keep the statistics for the partition id (which should be similar to the global statistics)
	// and it let the GC clean up the old table metadata including possible global index.

	newSpec := &ast.AlterTableSpec{}
	newSpec.Tp = spec.Tp
	defs := make([]*ast.PartitionDefinition, 1)
	defs[0] = &ast.PartitionDefinition{}
	defs[0].Name = model.NewCIStr("CollapsedPartitions")
	newSpec.PartDefinitions = defs
	partNames := make([]string, len(pi.Definitions))
	for i := range pi.Definitions {
		partNames[i] = pi.Definitions[i].Name.L
	}
	meta.Partition.Type = model.PartitionTypeNone
	partInfo, err := BuildAddedPartitionInfo(ctx.GetExprCtx(), meta, newSpec)
	if err != nil {
		return errors.Trace(err)
	}
	if err = d.assignPartitionIDs(partInfo.Definitions); err != nil {
		return errors.Trace(err)
	}
	// TODO: check where the default placement comes from (i.e. table level)
	if err = handlePartitionPlacement(ctx, partInfo); err != nil {
		return errors.Trace(err)
	}
	partInfo.NewTableID = partInfo.Definitions[0].ID

	job := &model.Job{
		SchemaID:       schema.ID,
		TableID:        meta.ID,
		SchemaName:     schema.Name.L,
		TableName:      meta.Name.L,
		Type:           model.ActionRemovePartitioning,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           []any{partNames, partInfo},
		ReorgMeta:      NewDDLReorgMeta(ctx),
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	// No preSplitAndScatter here, it will be done by the worker in onReorganizePartition instead.
	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

func checkReorgPartitionDefs(ctx sessionctx.Context, action model.ActionType, tblInfo *model.TableInfo, partInfo *model.PartitionInfo, firstPartIdx, lastPartIdx int, idMap map[int]struct{}) error {
	// partInfo contains only the new added partition, we have to combine it with the
	// old partitions to check all partitions is strictly increasing.
	pi := tblInfo.Partition
	clonedMeta := tblInfo.Clone()
	switch action {
	case model.ActionRemovePartitioning, model.ActionAlterTablePartitioning:
		clonedMeta.Partition = partInfo
		clonedMeta.ID = partInfo.NewTableID
	case model.ActionReorganizePartition:
		clonedMeta.Partition.AddingDefinitions = partInfo.Definitions
		clonedMeta.Partition.Definitions = getReorganizedDefinitions(clonedMeta.Partition, firstPartIdx, lastPartIdx, idMap)
	default:
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("partition type")
	}
	if err := checkPartitionDefinitionConstraints(ctx, clonedMeta); err != nil {
		return errors.Trace(err)
	}
	if action == model.ActionReorganizePartition {
		if pi.Type == model.PartitionTypeRange {
			if lastPartIdx == len(pi.Definitions)-1 {
				// Last partition dropped, OK to change the end range
				// Also includes MAXVALUE
				return nil
			}
			// Check if the replaced end range is the same as before
			lastAddingPartition := partInfo.Definitions[len(partInfo.Definitions)-1]
			lastOldPartition := pi.Definitions[lastPartIdx]
			if len(pi.Columns) > 0 {
				newGtOld, err := checkTwoRangeColumns(ctx, &lastAddingPartition, &lastOldPartition, pi, tblInfo)
				if err != nil {
					return errors.Trace(err)
				}
				if newGtOld {
					return errors.Trace(dbterror.ErrRangeNotIncreasing)
				}
				oldGtNew, err := checkTwoRangeColumns(ctx, &lastOldPartition, &lastAddingPartition, pi, tblInfo)
				if err != nil {
					return errors.Trace(err)
				}
				if oldGtNew {
					return errors.Trace(dbterror.ErrRangeNotIncreasing)
				}
				return nil
			}

			isUnsigned := isPartExprUnsigned(ctx.GetExprCtx().GetEvalCtx(), tblInfo)
			currentRangeValue, _, err := getRangeValue(ctx.GetExprCtx(), pi.Definitions[lastPartIdx].LessThan[0], isUnsigned)
			if err != nil {
				return errors.Trace(err)
			}
			newRangeValue, _, err := getRangeValue(ctx.GetExprCtx(), partInfo.Definitions[len(partInfo.Definitions)-1].LessThan[0], isUnsigned)
			if err != nil {
				return errors.Trace(err)
			}

			if currentRangeValue != newRangeValue {
				return errors.Trace(dbterror.ErrRangeNotIncreasing)
			}
		}
	} else {
		if len(pi.Definitions) != (lastPartIdx - firstPartIdx + 1) {
			// if not ActionReorganizePartition, require all partitions to be changed.
			return errors.Trace(dbterror.ErrAlterOperationNotSupported)
		}
	}
	return nil
}

// CoalescePartitions coalesce partitions can be used with a table that is partitioned by hash or key to reduce the number of partitions by number.
func (d *ddl) CoalescePartitions(sctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	is := d.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schema))
	}
	t, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}

	pi := t.Meta().GetPartitionInfo()
	if pi == nil {
		return errors.Trace(dbterror.ErrPartitionMgmtOnNonpartitioned)
	}

	switch pi.Type {
	case model.PartitionTypeHash, model.PartitionTypeKey:
		return d.hashPartitionManagement(sctx, ident, spec, pi)

	// Coalesce partition can only be used on hash/key partitions.
	default:
		return errors.Trace(dbterror.ErrCoalesceOnlyOnHashPartition)
	}
}

func (d *ddl) hashPartitionManagement(sctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec, pi *model.PartitionInfo) error {
	newSpec := *spec
	newSpec.PartitionNames = make([]model.CIStr, len(pi.Definitions))
	for i := 0; i < len(pi.Definitions); i++ {
		// reorganize ALL partitions into the new number of partitions
		newSpec.PartitionNames[i] = pi.Definitions[i].Name
	}
	for i := 0; i < len(newSpec.PartDefinitions); i++ {
		switch newSpec.PartDefinitions[i].Clause.(type) {
		case *ast.PartitionDefinitionClauseNone:
			// OK, expected
		case *ast.PartitionDefinitionClauseIn:
			return errors.Trace(ast.ErrPartitionWrongValues.FastGenByArgs("LIST", "IN"))
		case *ast.PartitionDefinitionClauseLessThan:
			return errors.Trace(ast.ErrPartitionWrongValues.FastGenByArgs("RANGE", "LESS THAN"))
		case *ast.PartitionDefinitionClauseHistory:
			return errors.Trace(ast.ErrPartitionWrongValues.FastGenByArgs("SYSTEM_TIME", "HISTORY"))

		default:
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs(
				"partitioning clause")
		}
	}
	if newSpec.Num < uint64(len(newSpec.PartDefinitions)) {
		newSpec.Num = uint64(len(newSpec.PartDefinitions))
	}
	if spec.Tp == ast.AlterTableCoalescePartitions {
		if newSpec.Num < 1 {
			return ast.ErrCoalescePartitionNoPartition
		}
		if newSpec.Num >= uint64(len(pi.Definitions)) {
			return dbterror.ErrDropLastPartition
		}
		if isNonDefaultPartitionOptionsUsed(pi.Definitions) {
			// The partition definitions will be copied in buildHashPartitionDefinitions()
			// if there is a non-empty list of definitions
			newSpec.PartDefinitions = []*ast.PartitionDefinition{{}}
		}
	}

	return d.ReorganizePartitions(sctx, ident, &newSpec)
}

func (d *ddl) TruncateTablePartition(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	is := d.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schema))
	}
	t, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}
	meta := t.Meta()
	if meta.GetPartitionInfo() == nil {
		return errors.Trace(dbterror.ErrPartitionMgmtOnNonpartitioned)
	}

	getTruncatedParts := func(pi *model.PartitionInfo) (*model.PartitionInfo, error) {
		if spec.OnAllPartitions {
			return pi.Clone(), nil
		}
		var defs []model.PartitionDefinition
		// MySQL allows duplicate partition names in truncate partition
		// so we filter them out through a hash
		posMap := make(map[int]bool)
		for _, name := range spec.PartitionNames {
			pos := pi.FindPartitionDefinitionByName(name.L)
			if pos < 0 {
				return nil, errors.Trace(table.ErrUnknownPartition.GenWithStackByArgs(name.L, ident.Name.O))
			}
			if _, ok := posMap[pos]; !ok {
				defs = append(defs, pi.Definitions[pos])
				posMap[pos] = true
			}
		}
		pi = pi.Clone()
		pi.Definitions = defs
		return pi, nil
	}
	pi, err := getTruncatedParts(meta.GetPartitionInfo())
	if err != nil {
		return err
	}
	pids := make([]int64, 0, len(pi.Definitions))
	for i := range pi.Definitions {
		pids = append(pids, pi.Definitions[i].ID)
	}

	genIDs, err := d.genGlobalIDs(len(pids))
	if err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		SchemaID:       schema.ID,
		TableID:        meta.ID,
		SchemaName:     schema.Name.L,
		SchemaState:    model.StatePublic,
		TableName:      t.Meta().Name.L,
		Type:           model.ActionTruncateTablePartition,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           []any{pids, genIDs},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (d *ddl) DropTablePartition(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	is := d.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schema))
	}
	t, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}
	meta := t.Meta()
	if meta.GetPartitionInfo() == nil {
		return errors.Trace(dbterror.ErrPartitionMgmtOnNonpartitioned)
	}

	if spec.Tp == ast.AlterTableDropFirstPartition {
		intervalOptions := getPartitionIntervalFromTable(ctx.GetExprCtx(), meta)
		if intervalOptions == nil {
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs(
				"FIRST PARTITION, does not seem like an INTERVAL partitioned table")
		}
		if len(spec.Partition.Definitions) != 0 {
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs(
				"FIRST PARTITION, table info already contains partition definitions")
		}
		spec.Partition.Interval = intervalOptions
		err = GeneratePartDefsFromInterval(ctx.GetExprCtx(), spec.Tp, meta, spec.Partition)
		if err != nil {
			return err
		}
		pNullOffset := 0
		if intervalOptions.NullPart {
			pNullOffset = 1
		}
		if len(spec.Partition.Definitions) == 0 ||
			len(spec.Partition.Definitions) >= len(meta.Partition.Definitions)-pNullOffset {
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs(
				"FIRST PARTITION, number of partitions does not match")
		}
		if len(spec.PartitionNames) != 0 || len(spec.Partition.Definitions) <= 1 {
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs(
				"FIRST PARTITION, given value does not generate a list of partition names to be dropped")
		}
		for i := range spec.Partition.Definitions {
			spec.PartitionNames = append(spec.PartitionNames, meta.Partition.Definitions[i+pNullOffset].Name)
		}
		// Use the last generated partition as First, i.e. do not drop the last name in the slice
		spec.PartitionNames = spec.PartitionNames[:len(spec.PartitionNames)-1]

		query, ok := ctx.Value(sessionctx.QueryString).(string)
		if ok {
			partNames := make([]string, 0, len(spec.PartitionNames))
			sqlMode := ctx.GetSessionVars().SQLMode
			for i := range spec.PartitionNames {
				partNames = append(partNames, stringutil.Escape(spec.PartitionNames[i].O, sqlMode))
			}
			syntacticSugar := spec.Partition.PartitionMethod.OriginalText()
			syntacticStart := spec.Partition.PartitionMethod.OriginTextPosition()
			newQuery := query[:syntacticStart] + "DROP PARTITION " + strings.Join(partNames, ", ") + query[syntacticStart+len(syntacticSugar):]
			defer ctx.SetValue(sessionctx.QueryString, query)
			ctx.SetValue(sessionctx.QueryString, newQuery)
		}
	}
	partNames := make([]string, len(spec.PartitionNames))
	for i, partCIName := range spec.PartitionNames {
		partNames[i] = partCIName.L
	}
	err = CheckDropTablePartition(meta, partNames)
	if err != nil {
		if dbterror.ErrDropPartitionNonExistent.Equal(err) && spec.IfExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return errors.Trace(err)
	}

	job := &model.Job{
		SchemaID:       schema.ID,
		TableID:        meta.ID,
		SchemaName:     schema.Name.L,
		SchemaState:    model.StatePublic,
		TableName:      meta.Name.L,
		Type:           model.ActionDropTablePartition,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           []any{partNames},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	err = d.DoDDLJob(ctx, job)
	if err != nil {
		if dbterror.ErrDropPartitionNonExistent.Equal(err) && spec.IfExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return errors.Trace(err)
	}
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

func checkFieldTypeCompatible(ft *types.FieldType, other *types.FieldType) bool {
	// int(1) could match the type with int(8)
	partialEqual := ft.GetType() == other.GetType() &&
		ft.GetDecimal() == other.GetDecimal() &&
		ft.GetCharset() == other.GetCharset() &&
		ft.GetCollate() == other.GetCollate() &&
		(ft.GetFlen() == other.GetFlen() || ft.StorageLength() != types.VarStorageLen) &&
		mysql.HasUnsignedFlag(ft.GetFlag()) == mysql.HasUnsignedFlag(other.GetFlag()) &&
		mysql.HasAutoIncrementFlag(ft.GetFlag()) == mysql.HasAutoIncrementFlag(other.GetFlag()) &&
		mysql.HasNotNullFlag(ft.GetFlag()) == mysql.HasNotNullFlag(other.GetFlag()) &&
		mysql.HasZerofillFlag(ft.GetFlag()) == mysql.HasZerofillFlag(other.GetFlag()) &&
		mysql.HasBinaryFlag(ft.GetFlag()) == mysql.HasBinaryFlag(other.GetFlag()) &&
		mysql.HasPriKeyFlag(ft.GetFlag()) == mysql.HasPriKeyFlag(other.GetFlag())
	if !partialEqual || len(ft.GetElems()) != len(other.GetElems()) {
		return false
	}
	for i := range ft.GetElems() {
		if ft.GetElems()[i] != other.GetElems()[i] {
			return false
		}
	}
	return true
}

func checkTiFlashReplicaCompatible(source *model.TiFlashReplicaInfo, target *model.TiFlashReplicaInfo) bool {
	if source == target {
		return true
	}
	if source == nil || target == nil {
		return false
	}
	if source.Count != target.Count ||
		source.Available != target.Available || len(source.LocationLabels) != len(target.LocationLabels) {
		return false
	}
	for i, lable := range source.LocationLabels {
		if target.LocationLabels[i] != lable {
			return false
		}
	}
	return true
}

func checkTableDefCompatible(source *model.TableInfo, target *model.TableInfo) error {
	// check temp table
	if target.TempTableType != model.TempTableNone {
		return errors.Trace(dbterror.ErrPartitionExchangeTempTable.FastGenByArgs(target.Name))
	}

	// check auto_random
	if source.AutoRandomBits != target.AutoRandomBits ||
		source.AutoRandomRangeBits != target.AutoRandomRangeBits ||
		source.Charset != target.Charset ||
		source.Collate != target.Collate ||
		source.ShardRowIDBits != target.ShardRowIDBits ||
		source.MaxShardRowIDBits != target.MaxShardRowIDBits ||
		!checkTiFlashReplicaCompatible(source.TiFlashReplica, target.TiFlashReplica) {
		return errors.Trace(dbterror.ErrTablesDifferentMetadata)
	}
	if len(source.Cols()) != len(target.Cols()) {
		return errors.Trace(dbterror.ErrTablesDifferentMetadata)
	}
	// Col compatible check
	for i, sourceCol := range source.Cols() {
		targetCol := target.Cols()[i]
		if sourceCol.IsVirtualGenerated() != targetCol.IsVirtualGenerated() {
			return dbterror.ErrUnsupportedOnGeneratedColumn.GenWithStackByArgs("Exchanging partitions for non-generated columns")
		}
		// It should strictyle compare expressions for generated columns
		if sourceCol.Name.L != targetCol.Name.L ||
			sourceCol.Hidden != targetCol.Hidden ||
			!checkFieldTypeCompatible(&sourceCol.FieldType, &targetCol.FieldType) ||
			sourceCol.GeneratedExprString != targetCol.GeneratedExprString {
			return errors.Trace(dbterror.ErrTablesDifferentMetadata)
		}
		if sourceCol.State != model.StatePublic ||
			targetCol.State != model.StatePublic {
			return errors.Trace(dbterror.ErrTablesDifferentMetadata)
		}
		if sourceCol.ID != targetCol.ID {
			return dbterror.ErrPartitionExchangeDifferentOption.GenWithStackByArgs(fmt.Sprintf("column: %s", sourceCol.Name))
		}
	}
	if len(source.Indices) != len(target.Indices) {
		return errors.Trace(dbterror.ErrTablesDifferentMetadata)
	}
	for _, sourceIdx := range source.Indices {
		if sourceIdx.Global {
			return dbterror.ErrPartitionExchangeDifferentOption.GenWithStackByArgs(fmt.Sprintf("global index: %s", sourceIdx.Name))
		}
		var compatIdx *model.IndexInfo
		for _, targetIdx := range target.Indices {
			if strings.EqualFold(sourceIdx.Name.L, targetIdx.Name.L) {
				compatIdx = targetIdx
			}
		}
		// No match index
		if compatIdx == nil {
			return errors.Trace(dbterror.ErrTablesDifferentMetadata)
		}
		// Index type is not compatible
		if sourceIdx.Tp != compatIdx.Tp ||
			sourceIdx.Unique != compatIdx.Unique ||
			sourceIdx.Primary != compatIdx.Primary {
			return errors.Trace(dbterror.ErrTablesDifferentMetadata)
		}
		// The index column
		if len(sourceIdx.Columns) != len(compatIdx.Columns) {
			return errors.Trace(dbterror.ErrTablesDifferentMetadata)
		}
		for i, sourceIdxCol := range sourceIdx.Columns {
			compatIdxCol := compatIdx.Columns[i]
			if sourceIdxCol.Length != compatIdxCol.Length ||
				sourceIdxCol.Name.L != compatIdxCol.Name.L {
				return errors.Trace(dbterror.ErrTablesDifferentMetadata)
			}
		}
		if sourceIdx.ID != compatIdx.ID {
			return dbterror.ErrPartitionExchangeDifferentOption.GenWithStackByArgs(fmt.Sprintf("index: %s", sourceIdx.Name))
		}
	}

	return nil
}

func checkExchangePartition(pt *model.TableInfo, nt *model.TableInfo) error {
	if nt.IsView() || nt.IsSequence() {
		return errors.Trace(dbterror.ErrCheckNoSuchTable)
	}
	if pt.GetPartitionInfo() == nil {
		return errors.Trace(dbterror.ErrPartitionMgmtOnNonpartitioned)
	}
	if nt.GetPartitionInfo() != nil {
		return errors.Trace(dbterror.ErrPartitionExchangePartTable.GenWithStackByArgs(nt.Name))
	}

	if len(nt.ForeignKeys) > 0 {
		return errors.Trace(dbterror.ErrPartitionExchangeForeignKey.GenWithStackByArgs(nt.Name))
	}

	return nil
}

func (d *ddl) ExchangeTablePartition(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	ptSchema, pt, err := d.getSchemaAndTableByIdent(ctx, ident)
	if err != nil {
		return errors.Trace(err)
	}

	ptMeta := pt.Meta()

	ntIdent := ast.Ident{Schema: spec.NewTable.Schema, Name: spec.NewTable.Name}

	// We should check local temporary here using session's info schema because the local temporary tables are only stored in session.
	ntLocalTempTable, err := sessiontxn.GetTxnManager(ctx).GetTxnInfoSchema().TableByName(ntIdent.Schema, ntIdent.Name)
	if err == nil && ntLocalTempTable.Meta().TempTableType == model.TempTableLocal {
		return errors.Trace(dbterror.ErrPartitionExchangeTempTable.FastGenByArgs(ntLocalTempTable.Meta().Name))
	}

	ntSchema, nt, err := d.getSchemaAndTableByIdent(ctx, ntIdent)
	if err != nil {
		return errors.Trace(err)
	}

	ntMeta := nt.Meta()

	err = checkExchangePartition(ptMeta, ntMeta)
	if err != nil {
		return errors.Trace(err)
	}

	partName := spec.PartitionNames[0].L

	// NOTE: if pt is subPartitioned, it should be checked

	defID, err := tables.FindPartitionByName(ptMeta, partName)
	if err != nil {
		return errors.Trace(err)
	}

	err = checkTableDefCompatible(ptMeta, ntMeta)
	if err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		SchemaID:       ntSchema.ID,
		TableID:        ntMeta.ID,
		SchemaName:     ntSchema.Name.L,
		TableName:      ntMeta.Name.L,
		Type:           model.ActionExchangeTablePartition,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           []any{defID, ptSchema.ID, ptMeta.ID, partName, spec.WithValidation},
		CtxVars:        []any{[]int64{ntSchema.ID, ptSchema.ID}, []int64{ntMeta.ID, ptMeta.ID}},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{
			{Database: ptSchema.Name.L, Table: ptMeta.Name.L},
			{Database: ntSchema.Name.L, Table: ntMeta.Name.L},
		},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}

	err = d.DoDDLJob(ctx, job)
	if err != nil {
		return errors.Trace(err)
	}
	ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError("after the exchange, please analyze related table of the exchange to update statistics"))
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

// DropColumn will drop a column from the table, now we don't support drop the column with index covered.
func (d *ddl) DropColumn(ctx sessionctx.Context, ti ast.Ident, spec *ast.AlterTableSpec) error {
	schema, t, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		return errors.Trace(err)
	}

	isDropable, err := checkIsDroppableColumn(ctx, d.infoCache.GetLatest(), schema, t, spec)
	if err != nil {
		return err
	}
	if !isDropable {
		return nil
	}
	colName := spec.OldColumnName.Name
	err = checkVisibleColumnCnt(t, 0, 1)
	if err != nil {
		return err
	}

	job := &model.Job{
		SchemaID:       schema.ID,
		TableID:        t.Meta().ID,
		SchemaName:     schema.Name.L,
		SchemaState:    model.StatePublic,
		TableName:      t.Meta().Name.L,
		Type:           model.ActionDropColumn,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           []any{colName, spec.IfExists},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

func checkIsDroppableColumn(ctx sessionctx.Context, is infoschema.InfoSchema, schema *model.DBInfo, t table.Table, spec *ast.AlterTableSpec) (isDrapable bool, err error) {
	tblInfo := t.Meta()
	// Check whether dropped column has existed.
	colName := spec.OldColumnName.Name
	col := table.FindCol(t.VisibleCols(), colName.L)
	if col == nil {
		err = dbterror.ErrCantDropFieldOrKey.GenWithStackByArgs(colName)
		if spec.IfExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return false, nil
		}
		return false, err
	}

	if err = isDroppableColumn(tblInfo, colName); err != nil {
		return false, errors.Trace(err)
	}
	if err = checkDropColumnWithPartitionConstraint(t, colName); err != nil {
		return false, errors.Trace(err)
	}
	// Check the column with foreign key.
	err = checkDropColumnWithForeignKeyConstraint(is, schema.Name.L, tblInfo, colName.L)
	if err != nil {
		return false, errors.Trace(err)
	}
	// Check the column with TTL config
	err = checkDropColumnWithTTLConfig(tblInfo, colName.L)
	if err != nil {
		return false, errors.Trace(err)
	}
	// We don't support dropping column with PK handle covered now.
	if col.IsPKHandleColumn(tblInfo) {
		return false, dbterror.ErrUnsupportedPKHandle
	}
	if mysql.HasAutoIncrementFlag(col.GetFlag()) && !ctx.GetSessionVars().AllowRemoveAutoInc {
		return false, dbterror.ErrCantDropColWithAutoInc
	}
	return true, nil
}

// checkDropColumnWithPartitionConstraint is used to check the partition constraint of the drop column.
func checkDropColumnWithPartitionConstraint(t table.Table, colName model.CIStr) error {
	if t.Meta().Partition == nil {
		return nil
	}
	pt, ok := t.(table.PartitionedTable)
	if !ok {
		// Should never happen!
		return errors.Trace(dbterror.ErrDependentByPartitionFunctional.GenWithStackByArgs(colName.L))
	}
	for _, name := range pt.GetPartitionColumnNames() {
		if strings.EqualFold(name.L, colName.L) {
			return errors.Trace(dbterror.ErrDependentByPartitionFunctional.GenWithStackByArgs(colName.L))
		}
	}
	return nil
}

func checkVisibleColumnCnt(t table.Table, addCnt, dropCnt int) error {
	tblInfo := t.Meta()
	visibleColumCnt := 0
	for _, column := range tblInfo.Columns {
		if !column.Hidden {
			visibleColumCnt++
		}
	}
	if visibleColumCnt+addCnt > dropCnt {
		return nil
	}
	if len(tblInfo.Columns)-visibleColumCnt > 0 {
		// There are only invisible columns.
		return dbterror.ErrTableMustHaveColumns
	}
	return dbterror.ErrCantRemoveAllFields
}

// checkModifyCharsetAndCollation returns error when the charset or collation is not modifiable.
// needRewriteCollationData is used when trying to modify the collation of a column, it is true when the column is with
// index because index of a string column is collation-aware.
func checkModifyCharsetAndCollation(toCharset, toCollate, origCharset, origCollate string, needRewriteCollationData bool) error {
	if !charset.ValidCharsetAndCollation(toCharset, toCollate) {
		return dbterror.ErrUnknownCharacterSet.GenWithStack("Unknown character set: '%s', collation: '%s'", toCharset, toCollate)
	}

	if needRewriteCollationData && collate.NewCollationEnabled() && !collate.CompatibleCollate(origCollate, toCollate) {
		return dbterror.ErrUnsupportedModifyCollation.GenWithStackByArgs(origCollate, toCollate)
	}

	if (origCharset == charset.CharsetUTF8 && toCharset == charset.CharsetUTF8MB4) ||
		(origCharset == charset.CharsetUTF8 && toCharset == charset.CharsetUTF8) ||
		(origCharset == charset.CharsetUTF8MB4 && toCharset == charset.CharsetUTF8MB4) ||
		(origCharset == charset.CharsetLatin1 && toCharset == charset.CharsetUTF8MB4) {
		// TiDB only allow utf8/latin1 to be changed to utf8mb4, or changing the collation when the charset is utf8/utf8mb4/latin1.
		return nil
	}

	if toCharset != origCharset {
		msg := fmt.Sprintf("charset from %s to %s", origCharset, toCharset)
		return dbterror.ErrUnsupportedModifyCharset.GenWithStackByArgs(msg)
	}
	if toCollate != origCollate {
		msg := fmt.Sprintf("change collate from %s to %s", origCollate, toCollate)
		return dbterror.ErrUnsupportedModifyCharset.GenWithStackByArgs(msg)
	}
	return nil
}

// checkModifyTypes checks if the 'origin' type can be modified to 'to' type no matter directly change
// or change by reorg. It returns error if the two types are incompatible and correlated change are not
// supported. However, even the two types can be change, if the "origin" type contains primary key, error will be returned.
func checkModifyTypes(origin *types.FieldType, to *types.FieldType, needRewriteCollationData bool) error {
	canReorg, err := types.CheckModifyTypeCompatible(origin, to)
	if err != nil {
		if !canReorg {
			return errors.Trace(dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs(err.Error()))
		}
		if mysql.HasPriKeyFlag(origin.GetFlag()) {
			msg := "this column has primary key flag"
			return dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs(msg)
		}
	}

	err = checkModifyCharsetAndCollation(to.GetCharset(), to.GetCollate(), origin.GetCharset(), origin.GetCollate(), needRewriteCollationData)

	if err != nil {
		if to.GetCharset() == charset.CharsetGBK || origin.GetCharset() == charset.CharsetGBK {
			return errors.Trace(err)
		}
		// column type change can handle the charset change between these two types in the process of the reorg.
		if dbterror.ErrUnsupportedModifyCharset.Equal(err) && canReorg {
			return nil
		}
	}
	return errors.Trace(err)
}

// SetDefaultValue sets the default value of the column.
func SetDefaultValue(ctx sessionctx.Context, col *table.Column, option *ast.ColumnOption) (hasDefaultValue bool, err error) {
	var value any
	var isSeqExpr bool
	value, isSeqExpr, err = getDefaultValue(
		exprctx.CtxWithHandleTruncateErrLevel(ctx.GetExprCtx(), errctx.LevelError),
		col, option,
	)
	if err != nil {
		return false, errors.Trace(err)
	}
	if isSeqExpr {
		if err := checkSequenceDefaultValue(col); err != nil {
			return false, errors.Trace(err)
		}
		col.DefaultIsExpr = isSeqExpr
	}

	// When the default value is expression, we skip check and convert.
	if !col.DefaultIsExpr {
		if hasDefaultValue, value, err = checkColumnDefaultValue(ctx.GetExprCtx(), col, value); err != nil {
			return hasDefaultValue, errors.Trace(err)
		}
		value, err = convertTimestampDefaultValToUTC(ctx, value, col)
		if err != nil {
			return hasDefaultValue, errors.Trace(err)
		}
	} else {
		hasDefaultValue = true
	}
	err = setDefaultValueWithBinaryPadding(col, value)
	if err != nil {
		return hasDefaultValue, errors.Trace(err)
	}
	return hasDefaultValue, nil
}

func setDefaultValueWithBinaryPadding(col *table.Column, value any) error {
	err := col.SetDefaultValue(value)
	if err != nil {
		return err
	}
	// https://dev.mysql.com/doc/refman/8.0/en/binary-varbinary.html
	// Set the default value for binary type should append the paddings.
	if value != nil {
		if col.GetType() == mysql.TypeString && types.IsBinaryStr(&col.FieldType) && len(value.(string)) < col.GetFlen() {
			padding := make([]byte, col.GetFlen()-len(value.(string)))
			col.DefaultValue = string(append([]byte(col.DefaultValue.(string)), padding...))
		}
	}
	return nil
}

func setColumnComment(ctx sessionctx.Context, col *table.Column, option *ast.ColumnOption) error {
	value, err := expression.EvalSimpleAst(ctx.GetExprCtx(), option.Expr)
	if err != nil {
		return errors.Trace(err)
	}
	if col.Comment, err = value.ToString(); err != nil {
		return errors.Trace(err)
	}

	sessionVars := ctx.GetSessionVars()
	col.Comment, err = validateCommentLength(sessionVars.StmtCtx.ErrCtx(), sessionVars.SQLMode, col.Name.L, &col.Comment, dbterror.ErrTooLongFieldComment)
	return errors.Trace(err)
}

// ProcessModifyColumnOptions process column options.
func ProcessModifyColumnOptions(ctx sessionctx.Context, col *table.Column, options []*ast.ColumnOption) error {
	var sb strings.Builder
	restoreFlags := format.RestoreStringSingleQuotes | format.RestoreKeyWordLowercase | format.RestoreNameBackQuotes |
		format.RestoreSpacesAroundBinaryOperation | format.RestoreWithoutSchemaName | format.RestoreWithoutSchemaName
	restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)

	var hasDefaultValue, setOnUpdateNow bool
	var err error
	var hasNullFlag bool
	for _, opt := range options {
		switch opt.Tp {
		case ast.ColumnOptionDefaultValue:
			hasDefaultValue, err = SetDefaultValue(ctx, col, opt)
			if err != nil {
				return errors.Trace(err)
			}
		case ast.ColumnOptionComment:
			err := setColumnComment(ctx, col, opt)
			if err != nil {
				return errors.Trace(err)
			}
		case ast.ColumnOptionNotNull:
			col.AddFlag(mysql.NotNullFlag)
		case ast.ColumnOptionNull:
			hasNullFlag = true
			col.DelFlag(mysql.NotNullFlag)
		case ast.ColumnOptionAutoIncrement:
			col.AddFlag(mysql.AutoIncrementFlag)
		case ast.ColumnOptionPrimaryKey:
			return errors.Trace(dbterror.ErrUnsupportedModifyColumn.GenWithStack("can't change column constraint (PRIMARY KEY)"))
		case ast.ColumnOptionUniqKey:
			return errors.Trace(dbterror.ErrUnsupportedModifyColumn.GenWithStack("can't change column constraint (UNIQUE KEY)"))
		case ast.ColumnOptionOnUpdate:
			// TODO: Support other time functions.
			if !(col.GetType() == mysql.TypeTimestamp || col.GetType() == mysql.TypeDatetime) {
				return dbterror.ErrInvalidOnUpdate.GenWithStackByArgs(col.Name)
			}
			if !expression.IsValidCurrentTimestampExpr(opt.Expr, &col.FieldType) {
				return dbterror.ErrInvalidOnUpdate.GenWithStackByArgs(col.Name)
			}
			col.AddFlag(mysql.OnUpdateNowFlag)
			setOnUpdateNow = true
		case ast.ColumnOptionGenerated:
			sb.Reset()
			err = opt.Expr.Restore(restoreCtx)
			if err != nil {
				return errors.Trace(err)
			}
			col.GeneratedExprString = sb.String()
			col.GeneratedStored = opt.Stored
			col.Dependences = make(map[string]struct{})
			// Only used by checkModifyGeneratedColumn, there is no need to set a ctor for it.
			col.GeneratedExpr = table.NewClonableExprNode(nil, opt.Expr)
			for _, colName := range FindColumnNamesInExpr(opt.Expr) {
				col.Dependences[colName.Name.L] = struct{}{}
			}
		case ast.ColumnOptionCollate:
			col.SetCollate(opt.StrValue)
		case ast.ColumnOptionReference:
			return errors.Trace(dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("can't modify with references"))
		case ast.ColumnOptionFulltext:
			return errors.Trace(dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("can't modify with full text"))
		case ast.ColumnOptionCheck:
			return errors.Trace(dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("can't modify with check"))
		// Ignore ColumnOptionAutoRandom. It will be handled later.
		case ast.ColumnOptionAutoRandom:
		default:
			return errors.Trace(dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs(fmt.Sprintf("unknown column option type: %d", opt.Tp)))
		}
	}

	if err = processAndCheckDefaultValueAndColumn(ctx, col, nil, hasDefaultValue, setOnUpdateNow, hasNullFlag); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func processAndCheckDefaultValueAndColumn(ctx sessionctx.Context, col *table.Column,
	outPriKeyConstraint *ast.Constraint, hasDefaultValue, setOnUpdateNow, hasNullFlag bool) error {
	processDefaultValue(col, hasDefaultValue, setOnUpdateNow)
	processColumnFlags(col)

	err := checkPriKeyConstraint(col, hasDefaultValue, hasNullFlag, outPriKeyConstraint)
	if err != nil {
		return errors.Trace(err)
	}
	if err = checkColumnValueConstraint(col, col.GetCollate()); err != nil {
		return errors.Trace(err)
	}
	if err = checkDefaultValue(ctx.GetExprCtx(), col, hasDefaultValue); err != nil {
		return errors.Trace(err)
	}
	if err = checkColumnFieldLength(col); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (d *ddl) getModifiableColumnJob(ctx context.Context, sctx sessionctx.Context, ident ast.Ident, originalColName model.CIStr,
	spec *ast.AlterTableSpec) (*model.Job, error) {
	is := d.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return nil, errors.Trace(infoschema.ErrDatabaseNotExists)
	}
	t, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return nil, errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}

	return GetModifiableColumnJob(ctx, sctx, is, ident, originalColName, schema, t, spec)
}

func checkModifyColumnWithGeneratedColumnsConstraint(allCols []*table.Column, oldColName model.CIStr) error {
	for _, col := range allCols {
		if col.GeneratedExpr == nil {
			continue
		}
		dependedColNames := FindColumnNamesInExpr(col.GeneratedExpr.Internal())
		for _, name := range dependedColNames {
			if name.Name.L == oldColName.L {
				if col.Hidden {
					return dbterror.ErrDependentByFunctionalIndex.GenWithStackByArgs(oldColName.O)
				}
				return dbterror.ErrDependentByGeneratedColumn.GenWithStackByArgs(oldColName.O)
			}
		}
	}
	return nil
}

// ProcessColumnCharsetAndCollation process column charset and collation
func ProcessColumnCharsetAndCollation(sctx sessionctx.Context, col *table.Column, newCol *table.Column, meta *model.TableInfo, specNewColumn *ast.ColumnDef, schema *model.DBInfo) error {
	var chs, coll string
	var err error
	// TODO: Remove it when all table versions are greater than or equal to TableInfoVersion1.
	// If newCol's charset is empty and the table's version less than TableInfoVersion1,
	// we will not modify the charset of the column. This behavior is not compatible with MySQL.
	if len(newCol.FieldType.GetCharset()) == 0 && meta.Version < model.TableInfoVersion1 {
		chs = col.FieldType.GetCharset()
		coll = col.FieldType.GetCollate()
	} else {
		chs, coll, err = getCharsetAndCollateInColumnDef(sctx.GetSessionVars(), specNewColumn)
		if err != nil {
			return errors.Trace(err)
		}
		chs, coll, err = ResolveCharsetCollation(sctx.GetSessionVars(),
			ast.CharsetOpt{Chs: chs, Col: coll},
			ast.CharsetOpt{Chs: meta.Charset, Col: meta.Collate},
			ast.CharsetOpt{Chs: schema.Charset, Col: schema.Collate},
		)
		chs, coll = OverwriteCollationWithBinaryFlag(sctx.GetSessionVars(), specNewColumn, chs, coll)
		if err != nil {
			return errors.Trace(err)
		}
	}

	if err = setCharsetCollationFlenDecimal(&newCol.FieldType, newCol.Name.O, chs, coll, sctx.GetSessionVars()); err != nil {
		return errors.Trace(err)
	}
	decodeEnumSetBinaryLiteralToUTF8(&newCol.FieldType, chs)
	return nil
}

// GetModifiableColumnJob returns a DDL job of model.ActionModifyColumn.
func GetModifiableColumnJob(
	ctx context.Context,
	sctx sessionctx.Context,
	is infoschema.InfoSchema, // WARN: is maybe nil here.
	ident ast.Ident,
	originalColName model.CIStr,
	schema *model.DBInfo,
	t table.Table,
	spec *ast.AlterTableSpec,
) (*model.Job, error) {
	var err error
	specNewColumn := spec.NewColumns[0]

	col := table.FindCol(t.Cols(), originalColName.L)
	if col == nil {
		return nil, infoschema.ErrColumnNotExists.GenWithStackByArgs(originalColName, ident.Name)
	}
	newColName := specNewColumn.Name.Name
	if newColName.L == model.ExtraHandleName.L {
		return nil, dbterror.ErrWrongColumnName.GenWithStackByArgs(newColName.L)
	}
	errG := checkModifyColumnWithGeneratedColumnsConstraint(t.Cols(), originalColName)

	// If we want to rename the column name, we need to check whether it already exists.
	if newColName.L != originalColName.L {
		c := table.FindCol(t.Cols(), newColName.L)
		if c != nil {
			return nil, infoschema.ErrColumnExists.GenWithStackByArgs(newColName)
		}

		// And also check the generated columns dependency, if some generated columns
		// depend on this column, we can't rename the column name.
		if errG != nil {
			return nil, errors.Trace(errG)
		}
	}

	// Constraints in the new column means adding new constraints. Errors should thrown,
	// which will be done by `processColumnOptions` later.
	if specNewColumn.Tp == nil {
		// Make sure the column definition is simple field type.
		return nil, errors.Trace(dbterror.ErrUnsupportedModifyColumn)
	}

	if err = checkColumnAttributes(specNewColumn.Name.OrigColName(), specNewColumn.Tp); err != nil {
		return nil, errors.Trace(err)
	}

	newCol := table.ToColumn(&model.ColumnInfo{
		ID: col.ID,
		// We use this PR(https://github.com/pingcap/tidb/pull/6274) as the dividing line to define whether it is a new version or an old version TiDB.
		// The old version TiDB initializes the column's offset and state here.
		// The new version TiDB doesn't initialize the column's offset and state, and it will do the initialization in run DDL function.
		// When we do the rolling upgrade the following may happen:
		// a new version TiDB builds the DDL job that doesn't be set the column's offset and state,
		// and the old version TiDB is the DDL owner, it doesn't get offset and state from the store. Then it will encounter errors.
		// So here we set offset and state to support the rolling upgrade.
		Offset:                col.Offset,
		State:                 col.State,
		OriginDefaultValue:    col.OriginDefaultValue,
		OriginDefaultValueBit: col.OriginDefaultValueBit,
		FieldType:             *specNewColumn.Tp,
		Name:                  newColName,
		Version:               col.Version,
	})

	if err = ProcessColumnCharsetAndCollation(sctx, col, newCol, t.Meta(), specNewColumn, schema); err != nil {
		return nil, err
	}

	if err = checkModifyColumnWithForeignKeyConstraint(is, schema.Name.L, t.Meta(), col.ColumnInfo, newCol.ColumnInfo); err != nil {
		return nil, errors.Trace(err)
	}

	// Copy index related options to the new spec.
	indexFlags := col.FieldType.GetFlag() & (mysql.PriKeyFlag | mysql.UniqueKeyFlag | mysql.MultipleKeyFlag)
	newCol.FieldType.AddFlag(indexFlags)
	if mysql.HasPriKeyFlag(col.FieldType.GetFlag()) {
		newCol.FieldType.AddFlag(mysql.NotNullFlag)
		// TODO: If user explicitly set NULL, we should throw error ErrPrimaryCantHaveNull.
	}

	if err = ProcessModifyColumnOptions(sctx, newCol, specNewColumn.Options); err != nil {
		return nil, errors.Trace(err)
	}

	if err = checkModifyTypes(&col.FieldType, &newCol.FieldType, isColumnWithIndex(col.Name.L, t.Meta().Indices)); err != nil {
		if strings.Contains(err.Error(), "Unsupported modifying collation") {
			colErrMsg := "Unsupported modifying collation of column '%s' from '%s' to '%s' when index is defined on it."
			err = dbterror.ErrUnsupportedModifyCollation.GenWithStack(colErrMsg, col.Name.L, col.GetCollate(), newCol.GetCollate())
		}
		return nil, errors.Trace(err)
	}
	needChangeColData := needChangeColumnData(col.ColumnInfo, newCol.ColumnInfo)
	if needChangeColData {
		if err = isGeneratedRelatedColumn(t.Meta(), newCol.ColumnInfo, col.ColumnInfo); err != nil {
			return nil, errors.Trace(err)
		}
		if t.Meta().Partition != nil {
			return nil, dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("table is partition table")
		}
	}

	// Check that the column change does not affect the partitioning column
	// It must keep the same type, int [unsigned], [var]char, date[time]
	if t.Meta().Partition != nil {
		pt, ok := t.(table.PartitionedTable)
		if !ok {
			// Should never happen!
			return nil, dbterror.ErrNotAllowedTypeInPartition.GenWithStackByArgs(newCol.Name.O)
		}
		isPartitioningColumn := false
		for _, name := range pt.GetPartitionColumnNames() {
			if strings.EqualFold(name.L, col.Name.L) {
				isPartitioningColumn = true
				break
			}
		}
		if isPartitioningColumn {
			// TODO: update the partitioning columns with new names if column is renamed
			// Would be an extension from MySQL which does not support it.
			if col.Name.L != newCol.Name.L {
				return nil, dbterror.ErrDependentByPartitionFunctional.GenWithStackByArgs(col.Name.L)
			}
			if !isColTypeAllowedAsPartitioningCol(t.Meta().Partition.Type, newCol.FieldType) {
				return nil, dbterror.ErrNotAllowedTypeInPartition.GenWithStackByArgs(newCol.Name.O)
			}
			pi := pt.Meta().GetPartitionInfo()
			if len(pi.Columns) == 0 {
				// non COLUMNS partitioning, only checks INTs, not their actual range
				// There are many edge cases, like when truncating SQL Mode is allowed
				// which will change the partitioning expression value resulting in a
				// different partition. Better be safe and not allow decreasing of length.
				// TODO: Should we allow it in strict mode? Wait for a use case / request.
				if newCol.FieldType.GetFlen() < col.FieldType.GetFlen() {
					return nil, dbterror.ErrUnsupportedModifyCollation.GenWithStack("Unsupported modify column, decreasing length of int may result in truncation and change of partition")
				}
			}
			// Basically only allow changes of the length/decimals for the column
			// Note that enum is not allowed, so elems are not checked
			// TODO: support partition by ENUM
			if newCol.FieldType.EvalType() != col.FieldType.EvalType() ||
				newCol.FieldType.GetFlag() != col.FieldType.GetFlag() ||
				newCol.FieldType.GetCollate() != col.FieldType.GetCollate() ||
				newCol.FieldType.GetCharset() != col.FieldType.GetCharset() {
				return nil, dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("can't change the partitioning column, since it would require reorganize all partitions")
			}
			// Generate a new PartitionInfo and validate it together with the new column definition
			// Checks if all partition definition values are compatible.
			// Similar to what buildRangePartitionDefinitions would do in terms of checks.

			tblInfo := pt.Meta()
			newTblInfo := *tblInfo
			// Replace col with newCol and see if we can generate a new SHOW CREATE TABLE
			// and reparse it and build new partition definitions (which will do additional
			// checks columns vs partition definition values
			newCols := make([]*model.ColumnInfo, 0, len(newTblInfo.Columns))
			for _, c := range newTblInfo.Columns {
				if c.ID == col.ID {
					newCols = append(newCols, newCol.ColumnInfo)
					continue
				}
				newCols = append(newCols, c)
			}
			newTblInfo.Columns = newCols

			var buf bytes.Buffer
			AppendPartitionInfo(tblInfo.GetPartitionInfo(), &buf, mysql.ModeNone)
			// The parser supports ALTER TABLE ... PARTITION BY ... even if the ddl code does not yet :)
			// Ignoring warnings
			stmt, _, err := parser.New().ParseSQL("ALTER TABLE t " + buf.String())
			if err != nil {
				// Should never happen!
				return nil, dbterror.ErrUnsupportedModifyColumn.GenWithStack("cannot parse generated PartitionInfo")
			}
			at, ok := stmt[0].(*ast.AlterTableStmt)
			if !ok || len(at.Specs) != 1 || at.Specs[0].Partition == nil {
				return nil, dbterror.ErrUnsupportedModifyColumn.GenWithStack("cannot parse generated PartitionInfo")
			}
			pAst := at.Specs[0].Partition
			_, err = buildPartitionDefinitionsInfo(
				exprctx.CtxWithHandleTruncateErrLevel(sctx.GetExprCtx(), errctx.LevelError),
				pAst.Definitions, &newTblInfo, uint64(len(newTblInfo.Partition.Definitions)),
			)
			if err != nil {
				return nil, dbterror.ErrUnsupportedModifyColumn.GenWithStack("New column does not match partition definitions: %s", err.Error())
			}
		}
	}

	// We don't support modifying column from not_auto_increment to auto_increment.
	if !mysql.HasAutoIncrementFlag(col.GetFlag()) && mysql.HasAutoIncrementFlag(newCol.GetFlag()) {
		return nil, dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("can't set auto_increment")
	}
	// Not support auto id with default value.
	if mysql.HasAutoIncrementFlag(newCol.GetFlag()) && newCol.GetDefaultValue() != nil {
		return nil, dbterror.ErrInvalidDefaultValue.GenWithStackByArgs(newCol.Name)
	}
	// Disallow modifying column from auto_increment to not auto_increment if the session variable `AllowRemoveAutoInc` is false.
	if !sctx.GetSessionVars().AllowRemoveAutoInc && mysql.HasAutoIncrementFlag(col.GetFlag()) && !mysql.HasAutoIncrementFlag(newCol.GetFlag()) {
		return nil, dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("can't remove auto_increment without @@tidb_allow_remove_auto_inc enabled")
	}

	// We support modifying the type definitions of 'null' to 'not null' now.
	var modifyColumnTp byte
	if !mysql.HasNotNullFlag(col.GetFlag()) && mysql.HasNotNullFlag(newCol.GetFlag()) {
		if err = checkForNullValue(ctx, sctx, true, ident.Schema, ident.Name, newCol.ColumnInfo, col.ColumnInfo); err != nil {
			return nil, errors.Trace(err)
		}
		// `modifyColumnTp` indicates that there is a type modification.
		modifyColumnTp = mysql.TypeNull
	}

	if err = checkColumnWithIndexConstraint(t.Meta(), col.ColumnInfo, newCol.ColumnInfo); err != nil {
		return nil, err
	}

	// As same with MySQL, we don't support modifying the stored status for generated columns.
	if err = checkModifyGeneratedColumn(sctx, schema.Name, t, col, newCol, specNewColumn, spec.Position); err != nil {
		return nil, errors.Trace(err)
	}
	if errG != nil {
		// According to issue https://github.com/pingcap/tidb/issues/24321,
		// changing the type of a column involving generating a column is prohibited.
		return nil, dbterror.ErrUnsupportedOnGeneratedColumn.GenWithStackByArgs(errG.Error())
	}

	if t.Meta().TTLInfo != nil {
		// the column referenced by TTL should be a time type
		if t.Meta().TTLInfo.ColumnName.L == originalColName.L && !types.IsTypeTime(newCol.ColumnInfo.FieldType.GetType()) {
			return nil, errors.Trace(dbterror.ErrUnsupportedColumnInTTLConfig.GenWithStackByArgs(newCol.ColumnInfo.Name.O))
		}
	}

	var newAutoRandBits uint64
	if newAutoRandBits, err = checkAutoRandom(t.Meta(), col, specNewColumn); err != nil {
		return nil, errors.Trace(err)
	}

	txn, err := sctx.Txn(true)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bdrRole, err := meta.NewMeta(txn).GetBDRRole()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if bdrRole == string(ast.BDRRolePrimary) &&
		deniedByBDRWhenModifyColumn(newCol.FieldType, col.FieldType, specNewColumn.Options) {
		return nil, dbterror.ErrBDRRestrictedDDL.FastGenByArgs(bdrRole)
	}

	job := &model.Job{
		SchemaID:       schema.ID,
		TableID:        t.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      t.Meta().Name.L,
		Type:           model.ActionModifyColumn,
		BinlogInfo:     &model.HistoryInfo{},
		ReorgMeta:      NewDDLReorgMeta(sctx),
		CtxVars:        []any{needChangeColData},
		Args:           []any{&newCol.ColumnInfo, originalColName, spec.Position, modifyColumnTp, newAutoRandBits},
		CDCWriteSource: sctx.GetSessionVars().CDCWriteSource,
		SQLMode:        sctx.GetSessionVars().SQLMode,
	}
	return job, nil
}

// checkColumnWithIndexConstraint is used to check the related index constraint of the modified column.
// Index has a max-prefix-length constraint. eg: a varchar(100), index idx(a), modifying column a to a varchar(4000)
// will cause index idx to break the max-prefix-length constraint.
func checkColumnWithIndexConstraint(tbInfo *model.TableInfo, originalCol, newCol *model.ColumnInfo) error {
	columns := make([]*model.ColumnInfo, 0, len(tbInfo.Columns))
	columns = append(columns, tbInfo.Columns...)
	// Replace old column with new column.
	for i, col := range columns {
		if col.Name.L != originalCol.Name.L {
			continue
		}
		columns[i] = newCol.Clone()
		columns[i].Name = originalCol.Name
		break
	}

	pkIndex := tables.FindPrimaryIndex(tbInfo)

	checkOneIndex := func(indexInfo *model.IndexInfo) (err error) {
		var modified bool
		for _, col := range indexInfo.Columns {
			if col.Name.L == originalCol.Name.L {
				modified = true
				break
			}
		}
		if !modified {
			return
		}
		err = checkIndexInModifiableColumns(columns, indexInfo.Columns)
		if err != nil {
			return
		}
		err = checkIndexPrefixLength(columns, indexInfo.Columns)
		return
	}

	// Check primary key first.
	var err error

	if pkIndex != nil {
		err = checkOneIndex(pkIndex)
		if err != nil {
			return err
		}
	}

	// Check secondary indexes.
	for _, indexInfo := range tbInfo.Indices {
		if indexInfo.Primary {
			continue
		}
		// the second param should always be set to true, check index length only if it was modified
		// checkOneIndex needs one param only.
		err = checkOneIndex(indexInfo)
		if err != nil {
			return err
		}
	}
	return nil
}

func checkIndexInModifiableColumns(columns []*model.ColumnInfo, idxColumns []*model.IndexColumn) error {
	for _, ic := range idxColumns {
		col := model.FindColumnInfo(columns, ic.Name.L)
		if col == nil {
			return dbterror.ErrKeyColumnDoesNotExits.GenWithStack("column does not exist: %s", ic.Name)
		}

		prefixLength := types.UnspecifiedLength
		if types.IsTypePrefixable(col.FieldType.GetType()) && col.FieldType.GetFlen() > ic.Length {
			// When the index column is changed, prefix length is only valid
			// if the type is still prefixable and larger than old prefix length.
			prefixLength = ic.Length
		}
		if err := checkIndexColumn(nil, col, prefixLength); err != nil {
			return err
		}
	}
	return nil
}

func isClusteredPKColumn(col *table.Column, tblInfo *model.TableInfo) bool {
	switch {
	case tblInfo.PKIsHandle:
		return mysql.HasPriKeyFlag(col.GetFlag())
	case tblInfo.IsCommonHandle:
		pk := tables.FindPrimaryIndex(tblInfo)
		for _, c := range pk.Columns {
			if c.Name.L == col.Name.L {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func checkAutoRandom(tableInfo *model.TableInfo, originCol *table.Column, specNewColumn *ast.ColumnDef) (uint64, error) {
	var oldShardBits, oldRangeBits uint64
	if isClusteredPKColumn(originCol, tableInfo) {
		oldShardBits = tableInfo.AutoRandomBits
		oldRangeBits = tableInfo.AutoRandomRangeBits
	}
	newShardBits, newRangeBits, err := extractAutoRandomBitsFromColDef(specNewColumn)
	if err != nil {
		return 0, errors.Trace(err)
	}
	switch {
	case oldShardBits == newShardBits:
	case oldShardBits < newShardBits:
		addingAutoRandom := oldShardBits == 0
		if addingAutoRandom {
			convFromAutoInc := mysql.HasAutoIncrementFlag(originCol.GetFlag()) && originCol.IsPKHandleColumn(tableInfo)
			if !convFromAutoInc {
				return 0, dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomAlterChangeFromAutoInc)
			}
		}
		if autoid.AutoRandomShardBitsMax < newShardBits {
			errMsg := fmt.Sprintf(autoid.AutoRandomOverflowErrMsg,
				autoid.AutoRandomShardBitsMax, newShardBits, specNewColumn.Name.Name.O)
			return 0, dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(errMsg)
		}
		// increasing auto_random shard bits is allowed.
	case oldShardBits > newShardBits:
		if newShardBits == 0 {
			return 0, dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomAlterErrMsg)
		}
		return 0, dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomDecreaseBitErrMsg)
	}

	modifyingAutoRandCol := oldShardBits > 0 || newShardBits > 0
	if modifyingAutoRandCol {
		// Disallow changing the column field type.
		if originCol.GetType() != specNewColumn.Tp.GetType() {
			return 0, dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomModifyColTypeErrMsg)
		}
		if originCol.GetType() != mysql.TypeLonglong {
			return 0, dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(fmt.Sprintf(autoid.AutoRandomOnNonBigIntColumn, types.TypeStr(originCol.GetType())))
		}
		// Disallow changing from auto_random to auto_increment column.
		if containsColumnOption(specNewColumn, ast.ColumnOptionAutoIncrement) {
			return 0, dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomIncompatibleWithAutoIncErrMsg)
		}
		// Disallow specifying a default value on auto_random column.
		if containsColumnOption(specNewColumn, ast.ColumnOptionDefaultValue) {
			return 0, dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomIncompatibleWithDefaultValueErrMsg)
		}
	}
	if rangeBitsIsChanged(oldRangeBits, newRangeBits) {
		return 0, dbterror.ErrInvalidAutoRandom.FastGenByArgs(autoid.AutoRandomUnsupportedAlterRangeBits)
	}
	return newShardBits, nil
}

func rangeBitsIsChanged(oldBits, newBits uint64) bool {
	if oldBits == 0 {
		oldBits = autoid.AutoRandomRangeBitsDefault
	}
	if newBits == 0 {
		newBits = autoid.AutoRandomRangeBitsDefault
	}
	return oldBits != newBits
}

// ChangeColumn renames an existing column and modifies the column's definition,
// currently we only support limited kind of changes
// that do not need to change or check data on the table.
func (d *ddl) ChangeColumn(ctx context.Context, sctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	specNewColumn := spec.NewColumns[0]
	if len(specNewColumn.Name.Schema.O) != 0 && ident.Schema.L != specNewColumn.Name.Schema.L {
		return dbterror.ErrWrongDBName.GenWithStackByArgs(specNewColumn.Name.Schema.O)
	}
	if len(spec.OldColumnName.Schema.O) != 0 && ident.Schema.L != spec.OldColumnName.Schema.L {
		return dbterror.ErrWrongDBName.GenWithStackByArgs(spec.OldColumnName.Schema.O)
	}
	if len(specNewColumn.Name.Table.O) != 0 && ident.Name.L != specNewColumn.Name.Table.L {
		return dbterror.ErrWrongTableName.GenWithStackByArgs(specNewColumn.Name.Table.O)
	}
	if len(spec.OldColumnName.Table.O) != 0 && ident.Name.L != spec.OldColumnName.Table.L {
		return dbterror.ErrWrongTableName.GenWithStackByArgs(spec.OldColumnName.Table.O)
	}

	job, err := d.getModifiableColumnJob(ctx, sctx, ident, spec.OldColumnName.Name, spec)
	if err != nil {
		if infoschema.ErrColumnNotExists.Equal(err) && spec.IfExists {
			sctx.GetSessionVars().StmtCtx.AppendNote(infoschema.ErrColumnNotExists.FastGenByArgs(spec.OldColumnName.Name, ident.Name))
			return nil
		}
		return errors.Trace(err)
	}

	err = d.DoDDLJob(sctx, job)
	// column not exists, but if_exists flags is true, so we ignore this error.
	if infoschema.ErrColumnNotExists.Equal(err) && spec.IfExists {
		sctx.GetSessionVars().StmtCtx.AppendNote(err)
		return nil
	}
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

// RenameColumn renames an existing column.
func (d *ddl) RenameColumn(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	oldColName := spec.OldColumnName.Name
	newColName := spec.NewColumnName.Name

	schema, tbl, err := d.getSchemaAndTableByIdent(ctx, ident)
	if err != nil {
		return errors.Trace(err)
	}

	oldCol := table.FindCol(tbl.VisibleCols(), oldColName.L)
	if oldCol == nil {
		return infoschema.ErrColumnNotExists.GenWithStackByArgs(oldColName, ident.Name)
	}
	// check if column can rename with check constraint
	err = IsColumnRenameableWithCheckConstraint(oldCol.Name, tbl.Meta())
	if err != nil {
		return err
	}

	if oldColName.L == newColName.L {
		return nil
	}
	if newColName.L == model.ExtraHandleName.L {
		return dbterror.ErrWrongColumnName.GenWithStackByArgs(newColName.L)
	}

	allCols := tbl.Cols()
	colWithNewNameAlreadyExist := table.FindCol(allCols, newColName.L) != nil
	if colWithNewNameAlreadyExist {
		return infoschema.ErrColumnExists.GenWithStackByArgs(newColName)
	}

	// Check generated expression.
	err = checkModifyColumnWithGeneratedColumnsConstraint(allCols, oldColName)
	if err != nil {
		return errors.Trace(err)
	}
	err = checkDropColumnWithPartitionConstraint(tbl, oldColName)
	if err != nil {
		return errors.Trace(err)
	}

	newCol := oldCol.Clone()
	newCol.Name = newColName
	job := &model.Job{
		SchemaID:       schema.ID,
		TableID:        tbl.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      tbl.Meta().Name.L,
		Type:           model.ActionModifyColumn,
		BinlogInfo:     &model.HistoryInfo{},
		ReorgMeta:      NewDDLReorgMeta(ctx),
		Args:           []any{&newCol, oldColName, spec.Position, 0, 0},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

// ModifyColumn does modification on an existing column, currently we only support limited kind of changes
// that do not need to change or check data on the table.
func (d *ddl) ModifyColumn(ctx context.Context, sctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	specNewColumn := spec.NewColumns[0]
	if len(specNewColumn.Name.Schema.O) != 0 && ident.Schema.L != specNewColumn.Name.Schema.L {
		return dbterror.ErrWrongDBName.GenWithStackByArgs(specNewColumn.Name.Schema.O)
	}
	if len(specNewColumn.Name.Table.O) != 0 && ident.Name.L != specNewColumn.Name.Table.L {
		return dbterror.ErrWrongTableName.GenWithStackByArgs(specNewColumn.Name.Table.O)
	}

	originalColName := specNewColumn.Name.Name
	job, err := d.getModifiableColumnJob(ctx, sctx, ident, originalColName, spec)
	if err != nil {
		if infoschema.ErrColumnNotExists.Equal(err) && spec.IfExists {
			sctx.GetSessionVars().StmtCtx.AppendNote(infoschema.ErrColumnNotExists.FastGenByArgs(originalColName, ident.Name))
			return nil
		}
		return errors.Trace(err)
	}

	err = d.DoDDLJob(sctx, job)
	// column not exists, but if_exists flags is true, so we ignore this error.
	if infoschema.ErrColumnNotExists.Equal(err) && spec.IfExists {
		sctx.GetSessionVars().StmtCtx.AppendNote(err)
		return nil
	}
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

func (d *ddl) AlterColumn(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	specNewColumn := spec.NewColumns[0]
	is := d.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name)
	}
	t, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name)
	}

	colName := specNewColumn.Name.Name
	// Check whether alter column has existed.
	oldCol := table.FindCol(t.Cols(), colName.L)
	if oldCol == nil {
		return dbterror.ErrBadField.GenWithStackByArgs(colName, ident.Name)
	}
	col := table.ToColumn(oldCol.Clone())

	// Clean the NoDefaultValueFlag value.
	col.DelFlag(mysql.NoDefaultValueFlag)
	col.DefaultIsExpr = false
	if len(specNewColumn.Options) == 0 {
		err = col.SetDefaultValue(nil)
		if err != nil {
			return errors.Trace(err)
		}
		col.AddFlag(mysql.NoDefaultValueFlag)
	} else {
		if IsAutoRandomColumnID(t.Meta(), col.ID) {
			return dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomIncompatibleWithDefaultValueErrMsg)
		}
		hasDefaultValue, err := SetDefaultValue(ctx, col, specNewColumn.Options[0])
		if err != nil {
			return errors.Trace(err)
		}
		if err = checkDefaultValue(ctx.GetExprCtx(), col, hasDefaultValue); err != nil {
			return errors.Trace(err)
		}
	}

	job := &model.Job{
		SchemaID:       schema.ID,
		TableID:        t.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      t.Meta().Name.L,
		Type:           model.ActionSetDefaultValue,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           []any{col},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

// AlterTableComment updates the table comment information.
func (d *ddl) AlterTableComment(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	is := d.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}

	tb, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}
	sessionVars := ctx.GetSessionVars()
	if _, err = validateCommentLength(sessionVars.StmtCtx.ErrCtx(), sessionVars.SQLMode, ident.Name.L, &spec.Comment, dbterror.ErrTooLongTableComment); err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		SchemaID:       schema.ID,
		TableID:        tb.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      tb.Meta().Name.L,
		Type:           model.ActionModifyTableComment,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           []any{spec.Comment},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

// AlterTableAutoIDCache updates the table comment information.
func (d *ddl) AlterTableAutoIDCache(ctx sessionctx.Context, ident ast.Ident, newCache int64) error {
	schema, tb, err := d.getSchemaAndTableByIdent(ctx, ident)
	if err != nil {
		return errors.Trace(err)
	}
	tbInfo := tb.Meta()
	if (newCache == 1 && tbInfo.AutoIdCache != 1) ||
		(newCache != 1 && tbInfo.AutoIdCache == 1) {
		return fmt.Errorf("Can't Alter AUTO_ID_CACHE between 1 and non-1, the underlying implementation is different")
	}

	job := &model.Job{
		SchemaID:       schema.ID,
		TableID:        tb.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      tb.Meta().Name.L,
		Type:           model.ActionModifyTableAutoIdCache,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           []any{newCache},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

// AlterTableCharsetAndCollate changes the table charset and collate.
func (d *ddl) AlterTableCharsetAndCollate(ctx sessionctx.Context, ident ast.Ident, toCharset, toCollate string, needsOverwriteCols bool) error {
	// use the last one.
	if toCharset == "" && toCollate == "" {
		return dbterror.ErrUnknownCharacterSet.GenWithStackByArgs(toCharset)
	}

	is := d.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}

	tb, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}

	if toCharset == "" {
		// charset does not change.
		toCharset = tb.Meta().Charset
	}

	if toCollate == "" {
		// Get the default collation of the charset.
		toCollate, err = GetDefaultCollation(ctx.GetSessionVars(), toCharset)
		if err != nil {
			return errors.Trace(err)
		}
	}
	doNothing, err := checkAlterTableCharset(tb.Meta(), schema, toCharset, toCollate, needsOverwriteCols)
	if err != nil {
		return err
	}
	if doNothing {
		return nil
	}

	job := &model.Job{
		SchemaID:       schema.ID,
		TableID:        tb.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      tb.Meta().Name.L,
		Type:           model.ActionModifyTableCharsetAndCollate,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           []any{toCharset, toCollate, needsOverwriteCols},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

func shouldModifyTiFlashReplica(tbReplicaInfo *model.TiFlashReplicaInfo, replicaInfo *ast.TiFlashReplicaSpec) bool {
	if tbReplicaInfo != nil && tbReplicaInfo.Count == replicaInfo.Count &&
		len(tbReplicaInfo.LocationLabels) == len(replicaInfo.Labels) {
		for i, label := range tbReplicaInfo.LocationLabels {
			if replicaInfo.Labels[i] != label {
				return true
			}
		}
		return false
	}
	return true
}

// addHypoTiFlashReplicaIntoCtx adds this hypothetical tiflash replica into this ctx.
func (*ddl) setHypoTiFlashReplica(ctx sessionctx.Context, schemaName, tableName model.CIStr, replicaInfo *ast.TiFlashReplicaSpec) error {
	sctx := ctx.GetSessionVars()
	if sctx.HypoTiFlashReplicas == nil {
		sctx.HypoTiFlashReplicas = make(map[string]map[string]struct{})
	}
	if sctx.HypoTiFlashReplicas[schemaName.L] == nil {
		sctx.HypoTiFlashReplicas[schemaName.L] = make(map[string]struct{})
	}
	if replicaInfo.Count > 0 { // add replicas
		sctx.HypoTiFlashReplicas[schemaName.L][tableName.L] = struct{}{}
	} else { // delete replicas
		delete(sctx.HypoTiFlashReplicas[schemaName.L], tableName.L)
	}
	return nil
}

// AlterTableSetTiFlashReplica sets the TiFlash replicas info.
func (d *ddl) AlterTableSetTiFlashReplica(ctx sessionctx.Context, ident ast.Ident, replicaInfo *ast.TiFlashReplicaSpec) error {
	schema, tb, err := d.getSchemaAndTableByIdent(ctx, ident)
	if err != nil {
		return errors.Trace(err)
	}

	err = isTableTiFlashSupported(schema.Name, tb.Meta())
	if err != nil {
		return errors.Trace(err)
	}

	tbReplicaInfo := tb.Meta().TiFlashReplica
	if !shouldModifyTiFlashReplica(tbReplicaInfo, replicaInfo) {
		return nil
	}

	if replicaInfo.Hypo {
		return d.setHypoTiFlashReplica(ctx, schema.Name, tb.Meta().Name, replicaInfo)
	}

	err = checkTiFlashReplicaCount(ctx, replicaInfo.Count)
	if err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		SchemaID:       schema.ID,
		TableID:        tb.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      tb.Meta().Name.L,
		Type:           model.ActionSetTiFlashReplica,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           []any{*replicaInfo},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

// AlterTableTTLInfoOrEnable submit ddl job to change table info according to the ttlInfo, or ttlEnable
// at least one of the `ttlInfo`, `ttlEnable` or `ttlCronJobSchedule` should be not nil.
// When `ttlInfo` is nil, and `ttlEnable` is not, it will use the original `.TTLInfo` in the table info and modify the
// `.Enable`. If the `.TTLInfo` in the table info is empty, this function will return an error.
// When `ttlInfo` is nil, and `ttlCronJobSchedule` is not, it will use the original `.TTLInfo` in the table info and modify the
// `.JobInterval`. If the `.TTLInfo` in the table info is empty, this function will return an error.
// When `ttlInfo` is not nil, it simply submits the job with the `ttlInfo` and ignore the `ttlEnable`.
func (d *ddl) AlterTableTTLInfoOrEnable(ctx sessionctx.Context, ident ast.Ident, ttlInfo *model.TTLInfo, ttlEnable *bool, ttlCronJobSchedule *string) error {
	is := d.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}

	tb, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}

	tblInfo := tb.Meta().Clone()
	tableID := tblInfo.ID
	tableName := tblInfo.Name.L

	var job *model.Job
	if ttlInfo != nil {
		tblInfo.TTLInfo = ttlInfo
		err = checkTTLInfoValid(ctx, ident.Schema, tblInfo)
		if err != nil {
			return err
		}
	} else {
		if tblInfo.TTLInfo == nil {
			if ttlEnable != nil {
				return errors.Trace(dbterror.ErrSetTTLOptionForNonTTLTable.FastGenByArgs("TTL_ENABLE"))
			}
			if ttlCronJobSchedule != nil {
				return errors.Trace(dbterror.ErrSetTTLOptionForNonTTLTable.FastGenByArgs("TTL_JOB_INTERVAL"))
			}
		}
	}

	job = &model.Job{
		SchemaID:       schema.ID,
		TableID:        tableID,
		SchemaName:     schema.Name.L,
		TableName:      tableName,
		Type:           model.ActionAlterTTLInfo,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           []any{ttlInfo, ttlEnable, ttlCronJobSchedule},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

func (d *ddl) AlterTableRemoveTTL(ctx sessionctx.Context, ident ast.Ident) error {
	is := d.infoCache.GetLatest()

	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}

	tb, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}

	tblInfo := tb.Meta().Clone()
	tableID := tblInfo.ID
	tableName := tblInfo.Name.L

	if tblInfo.TTLInfo != nil {
		job := &model.Job{
			SchemaID:       schema.ID,
			TableID:        tableID,
			SchemaName:     schema.Name.L,
			TableName:      tableName,
			Type:           model.ActionAlterTTLRemove,
			BinlogInfo:     &model.HistoryInfo{},
			CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
			SQLMode:        ctx.GetSessionVars().SQLMode,
		}
		err = d.DoDDLJob(ctx, job)
		err = d.callHookOnChanged(job, err)
		return errors.Trace(err)
	}

	return nil
}

func isTableTiFlashSupported(dbName model.CIStr, tbl *model.TableInfo) error {
	// Memory tables and system tables are not supported by TiFlash
	if util.IsMemOrSysDB(dbName.L) {
		return errors.Trace(dbterror.ErrUnsupportedTiFlashOperationForSysOrMemTable)
	} else if tbl.TempTableType != model.TempTableNone {
		return dbterror.ErrOptOnTemporaryTable.GenWithStackByArgs("set on tiflash")
	} else if tbl.IsView() || tbl.IsSequence() {
		return dbterror.ErrWrongObject.GenWithStackByArgs(dbName, tbl.Name, "BASE TABLE")
	}

	// Tables that has charset are not supported by TiFlash
	for _, col := range tbl.Cols() {
		_, ok := charset.TiFlashSupportedCharsets[col.GetCharset()]
		if !ok {
			return dbterror.ErrUnsupportedTiFlashOperationForUnsupportedCharsetTable.GenWithStackByArgs(col.GetCharset())
		}
	}

	return nil
}

func checkTiFlashReplicaCount(ctx sessionctx.Context, replicaCount uint64) error {
	// Check the tiflash replica count should be less than the total tiflash stores.
	tiflashStoreCnt, err := infoschema.GetTiFlashStoreCount(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	if replicaCount > tiflashStoreCnt {
		return errors.Errorf("the tiflash replica count: %d should be less than the total tiflash server count: %d", replicaCount, tiflashStoreCnt)
	}
	return nil
}

// AlterTableAddStatistics registers extended statistics for a table.
func (d *ddl) AlterTableAddStatistics(ctx sessionctx.Context, ident ast.Ident, stats *ast.StatisticsSpec, ifNotExists bool) error {
	if !ctx.GetSessionVars().EnableExtendedStats {
		return errors.New("Extended statistics feature is not generally available now, and tidb_enable_extended_stats is OFF")
	}
	// Not support Cardinality and Dependency statistics type for now.
	if stats.StatsType == ast.StatsTypeCardinality || stats.StatsType == ast.StatsTypeDependency {
		return errors.New("Cardinality and Dependency statistics types are not supported now")
	}
	_, tbl, err := d.getSchemaAndTableByIdent(ctx, ident)
	if err != nil {
		return err
	}
	tblInfo := tbl.Meta()
	if tblInfo.GetPartitionInfo() != nil {
		return errors.New("Extended statistics on partitioned tables are not supported now")
	}
	colIDs := make([]int64, 0, 2)
	colIDSet := make(map[int64]struct{}, 2)
	// Check whether columns exist.
	for _, colName := range stats.Columns {
		col := table.FindCol(tbl.VisibleCols(), colName.Name.L)
		if col == nil {
			return infoschema.ErrColumnNotExists.GenWithStackByArgs(colName.Name, ident.Name)
		}
		if stats.StatsType == ast.StatsTypeCorrelation && tblInfo.PKIsHandle && mysql.HasPriKeyFlag(col.GetFlag()) {
			ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError("No need to create correlation statistics on the integer primary key column"))
			return nil
		}
		if _, exist := colIDSet[col.ID]; exist {
			return errors.Errorf("Cannot create extended statistics on duplicate column names '%s'", colName.Name.L)
		}
		colIDSet[col.ID] = struct{}{}
		colIDs = append(colIDs, col.ID)
	}
	if len(colIDs) != 2 && (stats.StatsType == ast.StatsTypeCorrelation || stats.StatsType == ast.StatsTypeDependency) {
		return errors.New("Only support Correlation and Dependency statistics types on 2 columns")
	}
	if len(colIDs) < 1 && stats.StatsType == ast.StatsTypeCardinality {
		return errors.New("Only support Cardinality statistics type on at least 2 columns")
	}
	// TODO: check whether covering index exists for cardinality / dependency types.

	// Call utilities of statistics.Handle to modify system tables instead of doing DML directly,
	// because locking in Handle can guarantee the correctness of `version` in system tables.
	return d.ddlCtx.statsHandle.InsertExtendedStats(stats.StatsName, colIDs, int(stats.StatsType), tblInfo.ID, ifNotExists)
}

// AlterTableDropStatistics logically deletes extended statistics for a table.
func (d *ddl) AlterTableDropStatistics(ctx sessionctx.Context, ident ast.Ident, stats *ast.StatisticsSpec, ifExists bool) error {
	if !ctx.GetSessionVars().EnableExtendedStats {
		return errors.New("Extended statistics feature is not generally available now, and tidb_enable_extended_stats is OFF")
	}
	_, tbl, err := d.getSchemaAndTableByIdent(ctx, ident)
	if err != nil {
		return err
	}
	tblInfo := tbl.Meta()
	// Call utilities of statistics.Handle to modify system tables instead of doing DML directly,
	// because locking in Handle can guarantee the correctness of `version` in system tables.
	return d.ddlCtx.statsHandle.MarkExtendedStatsDeleted(stats.StatsName, tblInfo.ID, ifExists)
}

// UpdateTableReplicaInfo updates the table flash replica infos.
func (d *ddl) UpdateTableReplicaInfo(ctx sessionctx.Context, physicalID int64, available bool) error {
	is := d.infoCache.GetLatest()
	tb, ok := is.TableByID(physicalID)
	if !ok {
		tb, _, _ = is.FindTableByPartitionID(physicalID)
		if tb == nil {
			return infoschema.ErrTableNotExists.GenWithStack("Table which ID = %d does not exist.", physicalID)
		}
	}
	tbInfo := tb.Meta()
	if tbInfo.TiFlashReplica == nil || (tbInfo.ID == physicalID && tbInfo.TiFlashReplica.Available == available) ||
		(tbInfo.ID != physicalID && available == tbInfo.TiFlashReplica.IsPartitionAvailable(physicalID)) {
		return nil
	}

	db, ok := infoschema.SchemaByTable(is, tbInfo)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStack("Database of table `%s` does not exist.", tb.Meta().Name)
	}

	job := &model.Job{
		SchemaID:       db.ID,
		TableID:        tb.Meta().ID,
		SchemaName:     db.Name.L,
		TableName:      tb.Meta().Name.L,
		Type:           model.ActionUpdateTiFlashReplicaStatus,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           []any{available, physicalID},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	err := d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

// checkAlterTableCharset uses to check is it possible to change the charset of table.
// This function returns 2 variable:
// doNothing: if doNothing is true, means no need to change any more, because the target charset is same with the charset of table.
// err: if err is not nil, means it is not possible to change table charset to target charset.
func checkAlterTableCharset(tblInfo *model.TableInfo, dbInfo *model.DBInfo, toCharset, toCollate string, needsOverwriteCols bool) (doNothing bool, err error) {
	origCharset := tblInfo.Charset
	origCollate := tblInfo.Collate
	// Old version schema charset maybe modified when load schema if TreatOldVersionUTF8AsUTF8MB4 was enable.
	// So even if the origCharset equal toCharset, we still need to do the ddl for old version schema.
	if origCharset == toCharset && origCollate == toCollate && tblInfo.Version >= model.TableInfoVersion2 {
		// nothing to do.
		doNothing = true
		for _, col := range tblInfo.Columns {
			if col.GetCharset() == charset.CharsetBin {
				continue
			}
			if col.GetCharset() == toCharset && col.GetCollate() == toCollate {
				continue
			}
			doNothing = false
		}
		if doNothing {
			return doNothing, nil
		}
	}

	// This DDL will update the table charset to default charset.
	origCharset, origCollate, err = ResolveCharsetCollation(nil,
		ast.CharsetOpt{Chs: origCharset, Col: origCollate},
		ast.CharsetOpt{Chs: dbInfo.Charset, Col: dbInfo.Collate},
	)
	if err != nil {
		return doNothing, err
	}

	if err = checkModifyCharsetAndCollation(toCharset, toCollate, origCharset, origCollate, false); err != nil {
		return doNothing, err
	}
	if !needsOverwriteCols {
		// If we don't change the charset and collation of columns, skip the next checks.
		return doNothing, nil
	}

	for _, col := range tblInfo.Columns {
		if col.GetType() == mysql.TypeVarchar {
			if err = types.IsVarcharTooBigFieldLength(col.GetFlen(), col.Name.O, toCharset); err != nil {
				return doNothing, err
			}
		}
		if col.GetCharset() == charset.CharsetBin {
			continue
		}
		if len(col.GetCharset()) == 0 {
			continue
		}
		if err = checkModifyCharsetAndCollation(toCharset, toCollate, col.GetCharset(), col.GetCollate(), isColumnWithIndex(col.Name.L, tblInfo.Indices)); err != nil {
			if strings.Contains(err.Error(), "Unsupported modifying collation") {
				colErrMsg := "Unsupported converting collation of column '%s' from '%s' to '%s' when index is defined on it."
				err = dbterror.ErrUnsupportedModifyCollation.GenWithStack(colErrMsg, col.Name.L, col.GetCollate(), toCollate)
			}
			return doNothing, err
		}
	}
	return doNothing, nil
}

// RenameIndex renames an index.
// In TiDB, indexes are case-insensitive (so index 'a' and 'A" are considered the same index),
// but index names are case-sensitive (we can rename index 'a' to 'A')
func (d *ddl) RenameIndex(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	is := d.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}

	tb, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}
	if tb.Meta().TableCacheStatusType != model.TableCacheStatusDisable {
		return errors.Trace(dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Rename Index"))
	}
	duplicate, err := ValidateRenameIndex(spec.FromKey, spec.ToKey, tb.Meta())
	if duplicate {
		return nil
	}
	if err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		SchemaID:       schema.ID,
		TableID:        tb.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      tb.Meta().Name.L,
		Type:           model.ActionRenameIndex,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           []any{spec.FromKey, spec.ToKey},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

// If one drop those tables by mistake, it's difficult to recover.
// In the worst case, the whole TiDB cluster fails to bootstrap, so we prevent user from dropping them.
var systemTables = map[string]struct{}{
	"tidb":                 {},
	"gc_delete_range":      {},
	"gc_delete_range_done": {},
}

func isUndroppableTable(schema, table string) bool {
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
func (d *ddl) dropTableObject(
	ctx sessionctx.Context,
	objects []*ast.TableName,
	ifExists bool,
	tableObjectType objectType,
) error {
	var (
		notExistTables []string
		sessVars       = ctx.GetSessionVars()
		is             = d.GetInfoSchemaWithInterceptor(ctx)
		dropExistErr   *terror.Error
		jobType        model.ActionType
	)

	var jobArgs []any
	switch tableObjectType {
	case tableObject:
		dropExistErr = infoschema.ErrTableDropExists
		jobType = model.ActionDropTable
		objectIdents := make([]ast.Ident, len(objects))
		fkCheck := ctx.GetSessionVars().ForeignKeyChecks
		jobArgs = []any{objectIdents, fkCheck}
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
		tableInfo, err := is.TableByName(tn.Schema, tn.Name)
		if err != nil && infoschema.ErrTableNotExists.Equal(err) {
			notExistTables = append(notExistTables, fullti.String())
			continue
		} else if err != nil {
			return err
		}

		// prechecks before build DDL job

		// Protect important system table from been dropped by a mistake.
		// I can hardly find a case that a user really need to do this.
		if isUndroppableTable(tn.Schema.L, tn.Name.L) {
			return errors.Errorf("Drop tidb system table '%s.%s' is forbidden", tn.Schema.L, tn.Name.L)
		}
		switch tableObjectType {
		case tableObject:
			if !tableInfo.Meta().IsBaseTable() {
				notExistTables = append(notExistTables, fullti.String())
				continue
			}

			tempTableType := tableInfo.Meta().TempTableType
			if config.CheckTableBeforeDrop && tempTableType == model.TempTableNone {
				logutil.DDLLogger().Warn("admin check table before drop",
					zap.String("database", fullti.Schema.O),
					zap.String("table", fullti.Name.O),
				)
				exec := ctx.GetRestrictedSQLExecutor()
				internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
				_, _, err := exec.ExecRestrictedSQL(internalCtx, nil, "admin check table %n.%n", fullti.Schema.O, fullti.Name.O)
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
			SchemaID:       schema.ID,
			TableID:        tableInfo.Meta().ID,
			SchemaName:     schema.Name.L,
			SchemaState:    schema.State,
			TableName:      tableInfo.Meta().Name.L,
			Type:           jobType,
			BinlogInfo:     &model.HistoryInfo{},
			Args:           jobArgs,
			CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
			SQLMode:        ctx.GetSessionVars().SQLMode,
		}

		err = d.DoDDLJob(ctx, job)
		err = d.callHookOnChanged(job, err)
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

// DropTable will proceed even if some table in the list does not exists.
func (d *ddl) DropTable(ctx sessionctx.Context, stmt *ast.DropTableStmt) (err error) {
	return d.dropTableObject(ctx, stmt.Tables, stmt.IfExists, tableObject)
}

// DropView will proceed even if some view in the list does not exists.
func (d *ddl) DropView(ctx sessionctx.Context, stmt *ast.DropTableStmt) (err error) {
	return d.dropTableObject(ctx, stmt.Tables, stmt.IfExists, viewObject)
}

func (d *ddl) TruncateTable(ctx sessionctx.Context, ti ast.Ident) error {
	schema, tb, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		return errors.Trace(err)
	}
	if tb.Meta().IsView() || tb.Meta().IsSequence() {
		return infoschema.ErrTableNotExists.GenWithStackByArgs(schema.Name.O, tb.Meta().Name.O)
	}
	if tb.Meta().TableCacheStatusType != model.TableCacheStatusDisable {
		return dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Truncate Table")
	}
	fkCheck := ctx.GetSessionVars().ForeignKeyChecks
	referredFK := checkTableHasForeignKeyReferred(d.GetInfoSchemaWithInterceptor(ctx), ti.Schema.L, ti.Name.L, []ast.Ident{{Name: ti.Name, Schema: ti.Schema}}, fkCheck)
	if referredFK != nil {
		msg := fmt.Sprintf("`%s`.`%s` CONSTRAINT `%s`", referredFK.ChildSchema, referredFK.ChildTable, referredFK.ChildFKName)
		return errors.Trace(dbterror.ErrTruncateIllegalForeignKey.GenWithStackByArgs(msg))
	}

	ids := 1
	if tb.Meta().Partition != nil {
		ids += len(tb.Meta().Partition.Definitions)
	}
	genIDs, err := d.genGlobalIDs(ids)
	if err != nil {
		return errors.Trace(err)
	}
	newTableID := genIDs[0]
	job := &model.Job{
		SchemaID:       schema.ID,
		TableID:        tb.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      tb.Meta().Name.L,
		Type:           model.ActionTruncateTable,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           []any{newTableID, fkCheck, genIDs[1:]},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	if ok, _ := ctx.CheckTableLocked(tb.Meta().ID); ok && config.TableLockEnabled() {
		// AddTableLock here to avoid this ddl job was executed successfully but the session was been kill before return.
		// The session will release all table locks it holds, if we don't add the new locking table id here,
		// the session may forget to release the new locked table id when this ddl job was executed successfully
		// but the session was killed before return.
		ctx.AddTableLock([]model.TableLockTpInfo{{SchemaID: schema.ID, TableID: newTableID, Tp: tb.Meta().Lock.Tp}})
	}
	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	if err != nil {
		if config.TableLockEnabled() {
			ctx.ReleaseTableLockByTableIDs([]int64{newTableID})
		}
		return errors.Trace(err)
	}

	if !config.TableLockEnabled() {
		return nil
	}
	if ok, _ := ctx.CheckTableLocked(tb.Meta().ID); ok {
		ctx.ReleaseTableLockByTableIDs([]int64{tb.Meta().ID})
	}
	return nil
}

func (d *ddl) RenameTable(ctx sessionctx.Context, s *ast.RenameTableStmt) error {
	isAlterTable := false
	var err error
	if len(s.TableToTables) == 1 {
		oldIdent := ast.Ident{Schema: s.TableToTables[0].OldTable.Schema, Name: s.TableToTables[0].OldTable.Name}
		newIdent := ast.Ident{Schema: s.TableToTables[0].NewTable.Schema, Name: s.TableToTables[0].NewTable.Name}
		err = d.renameTable(ctx, oldIdent, newIdent, isAlterTable)
	} else {
		oldIdents := make([]ast.Ident, 0, len(s.TableToTables))
		newIdents := make([]ast.Ident, 0, len(s.TableToTables))
		for _, tables := range s.TableToTables {
			oldIdent := ast.Ident{Schema: tables.OldTable.Schema, Name: tables.OldTable.Name}
			newIdent := ast.Ident{Schema: tables.NewTable.Schema, Name: tables.NewTable.Name}
			oldIdents = append(oldIdents, oldIdent)
			newIdents = append(newIdents, newIdent)
		}
		err = d.renameTables(ctx, oldIdents, newIdents, isAlterTable)
	}
	return err
}

func (d *ddl) renameTable(ctx sessionctx.Context, oldIdent, newIdent ast.Ident, isAlterTable bool) error {
	is := d.GetInfoSchemaWithInterceptor(ctx)
	tables := make(map[string]int64)
	schemas, tableID, err := ExtractTblInfos(is, oldIdent, newIdent, isAlterTable, tables)
	if err != nil {
		return err
	}

	if schemas == nil {
		return nil
	}

	if tbl, ok := is.TableByID(tableID); ok {
		if tbl.Meta().TableCacheStatusType != model.TableCacheStatusDisable {
			return errors.Trace(dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Rename Table"))
		}
	}

	job := &model.Job{
		SchemaID:       schemas[1].ID,
		TableID:        tableID,
		SchemaName:     schemas[1].Name.L,
		TableName:      oldIdent.Name.L,
		Type:           model.ActionRenameTable,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		Args:           []any{schemas[0].ID, newIdent.Name, schemas[0].Name},
		CtxVars:        []any{[]int64{schemas[0].ID, schemas[1].ID}, []int64{tableID}},
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{
			{Database: schemas[0].Name.L, Table: oldIdent.Name.L},
			{Database: schemas[1].Name.L, Table: newIdent.Name.L},
		},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}

	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

func (d *ddl) renameTables(ctx sessionctx.Context, oldIdents, newIdents []ast.Ident, isAlterTable bool) error {
	is := d.GetInfoSchemaWithInterceptor(ctx)
	oldTableNames := make([]*model.CIStr, 0, len(oldIdents))
	tableNames := make([]*model.CIStr, 0, len(oldIdents))
	oldSchemaIDs := make([]int64, 0, len(oldIdents))
	newSchemaIDs := make([]int64, 0, len(oldIdents))
	tableIDs := make([]int64, 0, len(oldIdents))
	oldSchemaNames := make([]*model.CIStr, 0, len(oldIdents))
	involveSchemaInfo := make([]model.InvolvingSchemaInfo, 0, len(oldIdents)*2)

	var schemas []*model.DBInfo
	var tableID int64
	var err error

	tables := make(map[string]int64)
	for i := 0; i < len(oldIdents); i++ {
		schemas, tableID, err = ExtractTblInfos(is, oldIdents[i], newIdents[i], isAlterTable, tables)
		if err != nil {
			return err
		}

		if t, ok := is.TableByID(tableID); ok {
			if t.Meta().TableCacheStatusType != model.TableCacheStatusDisable {
				return errors.Trace(dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Rename Tables"))
			}
		}

		tableIDs = append(tableIDs, tableID)
		oldTableNames = append(oldTableNames, &oldIdents[i].Name)
		tableNames = append(tableNames, &newIdents[i].Name)
		oldSchemaIDs = append(oldSchemaIDs, schemas[0].ID)
		newSchemaIDs = append(newSchemaIDs, schemas[1].ID)
		oldSchemaNames = append(oldSchemaNames, &schemas[0].Name)
		involveSchemaInfo = append(involveSchemaInfo, model.InvolvingSchemaInfo{
			Database: schemas[0].Name.L, Table: oldIdents[i].Name.L,
		}, model.InvolvingSchemaInfo{
			Database: schemas[1].Name.L, Table: newIdents[i].Name.L,
		})
	}

	job := &model.Job{
		SchemaID:            schemas[1].ID,
		TableID:             tableIDs[0],
		SchemaName:          schemas[1].Name.L,
		Type:                model.ActionRenameTables,
		BinlogInfo:          &model.HistoryInfo{},
		CDCWriteSource:      ctx.GetSessionVars().CDCWriteSource,
		Args:                []any{oldSchemaIDs, newSchemaIDs, tableNames, tableIDs, oldSchemaNames, oldTableNames},
		CtxVars:             []any{append(oldSchemaIDs, newSchemaIDs...), tableIDs},
		InvolvingSchemaInfo: involveSchemaInfo,
		SQLMode:             ctx.GetSessionVars().SQLMode,
	}

	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
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
		oldTbl, err := is.TableByName(ident.Schema, ident.Name)
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

// GetName4AnonymousIndex returns a valid name for anonymous index.
func GetName4AnonymousIndex(t table.Table, colName model.CIStr, idxName model.CIStr) model.CIStr {
	// `id` is used to indicated the index name's suffix.
	id := 2
	l := len(t.Indices())
	indexName := colName
	if idxName.O != "" {
		// Use the provided index name, it only happens when the original index name is too long and be truncated.
		indexName = idxName
		id = 3
	}
	if strings.EqualFold(indexName.L, mysql.PrimaryKeyName) {
		indexName = model.NewCIStr(fmt.Sprintf("%s_%d", colName.O, id))
		id = 3
	}
	for i := 0; i < l; i++ {
		if t.Indices()[i].Meta().Name.L == indexName.L {
			indexName = model.NewCIStr(fmt.Sprintf("%s_%d", colName.O, id))
			if err := checkTooLongIndex(indexName); err != nil {
				indexName = GetName4AnonymousIndex(t, model.NewCIStr(colName.O[:30]), model.NewCIStr(fmt.Sprintf("%s_%d", colName.O[:30], 2)))
			}
			i = -1
			id++
		}
	}
	return indexName
}

func (d *ddl) CreatePrimaryKey(ctx sessionctx.Context, ti ast.Ident, indexName model.CIStr,
	indexPartSpecifications []*ast.IndexPartSpecification, indexOption *ast.IndexOption) error {
	if indexOption != nil && indexOption.PrimaryKeyTp == model.PrimaryKeyTypeClustered {
		return dbterror.ErrUnsupportedModifyPrimaryKey.GenWithStack("Adding clustered primary key is not supported. " +
			"Please consider adding NONCLUSTERED primary key instead")
	}
	schema, t, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		return errors.Trace(err)
	}

	if err = checkTooLongIndex(indexName); err != nil {
		return dbterror.ErrTooLongIdent.GenWithStackByArgs(mysql.PrimaryKeyName)
	}

	indexName = model.NewCIStr(mysql.PrimaryKeyName)
	if indexInfo := t.Meta().FindIndexByName(indexName.L); indexInfo != nil ||
		// If the table's PKIsHandle is true, it also means that this table has a primary key.
		t.Meta().PKIsHandle {
		return infoschema.ErrMultiplePriKey
	}

	// Primary keys cannot include expression index parts. A primary key requires the generated column to be stored,
	// but expression index parts are implemented as virtual generated columns, not stored generated columns.
	for _, idxPart := range indexPartSpecifications {
		if idxPart.Expr != nil {
			return dbterror.ErrFunctionalIndexPrimaryKey
		}
	}

	tblInfo := t.Meta()
	// Check before the job is put to the queue.
	// This check is redundant, but useful. If DDL check fail before the job is put
	// to job queue, the fail path logic is super fast.
	// After DDL job is put to the queue, and if the check fail, TiDB will run the DDL cancel logic.
	// The recover step causes DDL wait a few seconds, makes the unit test painfully slow.
	// For same reason, decide whether index is global here.
	indexColumns, _, err := buildIndexColumns(ctx, tblInfo.Columns, indexPartSpecifications)
	if err != nil {
		return errors.Trace(err)
	}
	if _, err = CheckPKOnGeneratedColumn(tblInfo, indexPartSpecifications); err != nil {
		return err
	}

	global := false
	if tblInfo.GetPartitionInfo() != nil {
		ck, err := checkPartitionKeysConstraint(tblInfo.GetPartitionInfo(), indexColumns, tblInfo)
		if err != nil {
			return err
		}
		if !ck {
			if !ctx.GetSessionVars().EnableGlobalIndex {
				return dbterror.ErrUniqueKeyNeedAllFieldsInPf.GenWithStackByArgs("PRIMARY")
			}
			// index columns does not contain all partition columns, must set global
			global = true
		}
	}

	// May be truncate comment here, when index comment too long and sql_mode is't strict.
	if indexOption != nil {
		sessionVars := ctx.GetSessionVars()
		if _, err = validateCommentLength(sessionVars.StmtCtx.ErrCtx(), sessionVars.SQLMode, indexName.String(), &indexOption.Comment, dbterror.ErrTooLongIndexComment); err != nil {
			return errors.Trace(err)
		}
	}

	unique := true
	sqlMode := ctx.GetSessionVars().SQLMode
	job := &model.Job{
		SchemaID:       schema.ID,
		TableID:        t.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      t.Meta().Name.L,
		Type:           model.ActionAddPrimaryKey,
		BinlogInfo:     &model.HistoryInfo{},
		ReorgMeta:      nil,
		Args:           []any{unique, indexName, indexPartSpecifications, indexOption, sqlMode, nil, global},
		Priority:       ctx.GetSessionVars().DDLReorgPriority,
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	reorgMeta, err := newReorgMetaFromVariables(job, ctx)
	if err != nil {
		return err
	}
	job.ReorgMeta = reorgMeta

	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

func precheckBuildHiddenColumnInfo(
	indexPartSpecifications []*ast.IndexPartSpecification,
	indexName model.CIStr,
) error {
	for i, idxPart := range indexPartSpecifications {
		if idxPart.Expr == nil {
			continue
		}
		name := fmt.Sprintf("%s_%s_%d", expressionIndexPrefix, indexName, i)
		if utf8.RuneCountInString(name) > mysql.MaxColumnNameLength {
			// TODO: Refine the error message.
			return dbterror.ErrTooLongIdent.GenWithStackByArgs("hidden column")
		}
		// TODO: Refine the error message.
		if err := checkIllegalFn4Generated(indexName.L, typeIndex, idxPart.Expr); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func buildHiddenColumnInfoWithCheck(ctx sessionctx.Context, indexPartSpecifications []*ast.IndexPartSpecification, indexName model.CIStr, tblInfo *model.TableInfo, existCols []*table.Column) ([]*model.ColumnInfo, error) {
	if err := precheckBuildHiddenColumnInfo(indexPartSpecifications, indexName); err != nil {
		return nil, err
	}
	return BuildHiddenColumnInfo(ctx, indexPartSpecifications, indexName, tblInfo, existCols)
}

// BuildHiddenColumnInfo builds hidden column info.
func BuildHiddenColumnInfo(ctx sessionctx.Context, indexPartSpecifications []*ast.IndexPartSpecification, indexName model.CIStr, tblInfo *model.TableInfo, existCols []*table.Column) ([]*model.ColumnInfo, error) {
	hiddenCols := make([]*model.ColumnInfo, 0, len(indexPartSpecifications))
	for i, idxPart := range indexPartSpecifications {
		if idxPart.Expr == nil {
			continue
		}
		idxPart.Column = &ast.ColumnName{Name: model.NewCIStr(fmt.Sprintf("%s_%s_%d", expressionIndexPrefix, indexName, i))}
		// Check whether the hidden columns have existed.
		col := table.FindCol(existCols, idxPart.Column.Name.L)
		if col != nil {
			// TODO: Use expression index related error.
			return nil, infoschema.ErrColumnExists.GenWithStackByArgs(col.Name.String())
		}
		idxPart.Length = types.UnspecifiedLength
		// The index part is an expression, prepare a hidden column for it.

		var sb strings.Builder
		restoreFlags := format.RestoreStringSingleQuotes | format.RestoreKeyWordLowercase | format.RestoreNameBackQuotes |
			format.RestoreSpacesAroundBinaryOperation | format.RestoreWithoutSchemaName | format.RestoreWithoutTableName
		restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)
		sb.Reset()
		err := idxPart.Expr.Restore(restoreCtx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		expr, err := expression.BuildSimpleExpr(ctx.GetExprCtx(), idxPart.Expr,
			expression.WithTableInfo(ctx.GetSessionVars().CurrentDB, tblInfo),
			expression.WithAllowCastArray(true),
		)
		if err != nil {
			// TODO: refine the error message.
			return nil, err
		}
		if _, ok := expr.(*expression.Column); ok {
			return nil, dbterror.ErrFunctionalIndexOnField
		}

		colInfo := &model.ColumnInfo{
			Name:                idxPart.Column.Name,
			GeneratedExprString: sb.String(),
			GeneratedStored:     false,
			Version:             model.CurrLatestColumnInfoVersion,
			Dependences:         make(map[string]struct{}),
			Hidden:              true,
			FieldType:           *expr.GetType(ctx.GetExprCtx().GetEvalCtx()),
		}
		// Reset some flag, it may be caused by wrong type infer. But it's not easy to fix them all, so reset them here for safety.
		colInfo.DelFlag(mysql.PriKeyFlag | mysql.UniqueKeyFlag | mysql.AutoIncrementFlag)

		if colInfo.GetType() == mysql.TypeDatetime || colInfo.GetType() == mysql.TypeDate || colInfo.GetType() == mysql.TypeTimestamp || colInfo.GetType() == mysql.TypeDuration {
			if colInfo.FieldType.GetDecimal() == types.UnspecifiedLength {
				colInfo.FieldType.SetDecimal(types.MaxFsp)
			}
		}
		// For an array, the collation is set to "binary". The collation has no effect on the array itself (as it's usually
		// regarded as a JSON), but will influence how TiKV handles the index value.
		if colInfo.FieldType.IsArray() {
			colInfo.SetCharset("binary")
			colInfo.SetCollate("binary")
		}
		checkDependencies := make(map[string]struct{})
		for _, colName := range FindColumnNamesInExpr(idxPart.Expr) {
			colInfo.Dependences[colName.Name.L] = struct{}{}
			checkDependencies[colName.Name.L] = struct{}{}
		}
		if err = checkDependedColExist(checkDependencies, existCols); err != nil {
			return nil, errors.Trace(err)
		}
		if !ctx.GetSessionVars().EnableAutoIncrementInGenerated {
			if err = checkExpressionIndexAutoIncrement(indexName.O, colInfo.Dependences, tblInfo); err != nil {
				return nil, errors.Trace(err)
			}
		}
		idxPart.Expr = nil
		hiddenCols = append(hiddenCols, colInfo)
	}
	return hiddenCols, nil
}

func (d *ddl) CreateIndex(ctx sessionctx.Context, stmt *ast.CreateIndexStmt) error {
	ident := ast.Ident{Schema: stmt.Table.Schema, Name: stmt.Table.Name}
	return d.createIndex(ctx, ident, stmt.KeyType, model.NewCIStr(stmt.IndexName),
		stmt.IndexPartSpecifications, stmt.IndexOption, stmt.IfNotExists)
}

// addHypoIndexIntoCtx adds this index as a hypo-index into this ctx.
func (*ddl) addHypoIndexIntoCtx(ctx sessionctx.Context, schemaName, tableName model.CIStr, indexInfo *model.IndexInfo) error {
	sctx := ctx.GetSessionVars()
	indexName := indexInfo.Name

	if sctx.HypoIndexes == nil {
		sctx.HypoIndexes = make(map[string]map[string]map[string]*model.IndexInfo)
	}
	if sctx.HypoIndexes[schemaName.L] == nil {
		sctx.HypoIndexes[schemaName.L] = make(map[string]map[string]*model.IndexInfo)
	}
	if sctx.HypoIndexes[schemaName.L][tableName.L] == nil {
		sctx.HypoIndexes[schemaName.L][tableName.L] = make(map[string]*model.IndexInfo)
	}
	if _, exist := sctx.HypoIndexes[schemaName.L][tableName.L][indexName.L]; exist {
		return errors.Trace(errors.Errorf("conflict hypo index name %s", indexName.L))
	}

	sctx.HypoIndexes[schemaName.L][tableName.L][indexName.L] = indexInfo
	return nil
}

func (d *ddl) createIndex(ctx sessionctx.Context, ti ast.Ident, keyType ast.IndexKeyType, indexName model.CIStr,
	indexPartSpecifications []*ast.IndexPartSpecification, indexOption *ast.IndexOption, ifNotExists bool) error {
	// not support Spatial and FullText index
	if keyType == ast.IndexKeyTypeFullText || keyType == ast.IndexKeyTypeSpatial {
		return dbterror.ErrUnsupportedIndexType.GenWithStack("FULLTEXT and SPATIAL index is not supported")
	}
	unique := keyType == ast.IndexKeyTypeUnique
	schema, t, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		return errors.Trace(err)
	}

	if t.Meta().TableCacheStatusType != model.TableCacheStatusDisable {
		return errors.Trace(dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Create Index"))
	}
	// Deal with anonymous index.
	if len(indexName.L) == 0 {
		colName := model.NewCIStr("expression_index")
		if indexPartSpecifications[0].Column != nil {
			colName = indexPartSpecifications[0].Column.Name
		}
		indexName = GetName4AnonymousIndex(t, colName, model.NewCIStr(""))
	}

	if indexInfo := t.Meta().FindIndexByName(indexName.L); indexInfo != nil {
		if indexInfo.State != model.StatePublic {
			// NOTE: explicit error message. See issue #18363.
			err = dbterror.ErrDupKeyName.GenWithStack("Duplicate key name '%s'; "+
				"a background job is trying to add the same index, "+
				"please check by `ADMIN SHOW DDL JOBS`", indexName)
		} else {
			err = dbterror.ErrDupKeyName.GenWithStackByArgs(indexName)
		}
		if ifNotExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return err
	}

	if err = checkTooLongIndex(indexName); err != nil {
		return errors.Trace(err)
	}

	tblInfo := t.Meta()

	// Build hidden columns if necessary.
	hiddenCols, err := buildHiddenColumnInfoWithCheck(ctx, indexPartSpecifications, indexName, t.Meta(), t.Cols())
	if err != nil {
		return err
	}
	if err = checkAddColumnTooManyColumns(len(t.Cols()) + len(hiddenCols)); err != nil {
		return errors.Trace(err)
	}

	finalColumns := make([]*model.ColumnInfo, len(tblInfo.Columns), len(tblInfo.Columns)+len(hiddenCols))
	copy(finalColumns, tblInfo.Columns)
	finalColumns = append(finalColumns, hiddenCols...)
	// Check before the job is put to the queue.
	// This check is redundant, but useful. If DDL check fail before the job is put
	// to job queue, the fail path logic is super fast.
	// After DDL job is put to the queue, and if the check fail, TiDB will run the DDL cancel logic.
	// The recover step causes DDL wait a few seconds, makes the unit test painfully slow.
	// For same reason, decide whether index is global here.
	indexColumns, _, err := buildIndexColumns(ctx, finalColumns, indexPartSpecifications)
	if err != nil {
		return errors.Trace(err)
	}

	global := false
	if unique && tblInfo.GetPartitionInfo() != nil {
		ck, err := checkPartitionKeysConstraint(tblInfo.GetPartitionInfo(), indexColumns, tblInfo)
		if err != nil {
			return err
		}
		if !ck {
			if !ctx.GetSessionVars().EnableGlobalIndex {
				return dbterror.ErrUniqueKeyNeedAllFieldsInPf.GenWithStackByArgs("UNIQUE INDEX")
			}
			// index columns does not contain all partition columns, must set global
			global = true
		}
	}
	// May be truncate comment here, when index comment too long and sql_mode is't strict.
	if indexOption != nil {
		sessionVars := ctx.GetSessionVars()
		if _, err = validateCommentLength(sessionVars.StmtCtx.ErrCtx(), sessionVars.SQLMode, indexName.String(), &indexOption.Comment, dbterror.ErrTooLongIndexComment); err != nil {
			return errors.Trace(err)
		}
	}

	if indexOption != nil && indexOption.Tp == model.IndexTypeHypo { // for hypo-index
		indexInfo, err := BuildIndexInfo(ctx, tblInfo.Columns, indexName, false, unique, global,
			indexPartSpecifications, indexOption, model.StatePublic)
		if err != nil {
			return err
		}
		return d.addHypoIndexIntoCtx(ctx, ti.Schema, ti.Name, indexInfo)
	}

	chs, coll := ctx.GetSessionVars().GetCharsetInfo()
	job := &model.Job{
		SchemaID:       schema.ID,
		TableID:        t.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      t.Meta().Name.L,
		Type:           model.ActionAddIndex,
		BinlogInfo:     &model.HistoryInfo{},
		ReorgMeta:      nil,
		Args:           []any{unique, indexName, indexPartSpecifications, indexOption, hiddenCols, global},
		Priority:       ctx.GetSessionVars().DDLReorgPriority,
		Charset:        chs,
		Collate:        coll,
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	reorgMeta, err := newReorgMetaFromVariables(job, ctx)
	if err != nil {
		return err
	}
	job.ReorgMeta = reorgMeta

	err = d.DoDDLJob(ctx, job)
	// key exists, but if_not_exists flags is true, so we ignore this error.
	if dbterror.ErrDupKeyName.Equal(err) && ifNotExists {
		ctx.GetSessionVars().StmtCtx.AppendNote(err)
		return nil
	}
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

func newReorgMetaFromVariables(job *model.Job, sctx sessionctx.Context) (*model.DDLReorgMeta, error) {
	reorgMeta := NewDDLReorgMeta(sctx)
	reorgMeta.IsDistReorg = variable.EnableDistTask.Load()
	reorgMeta.IsFastReorg = variable.EnableFastReorg.Load()
	reorgMeta.TargetScope = variable.ServiceScope.Load()

	if reorgMeta.IsDistReorg && !reorgMeta.IsFastReorg {
		return nil, dbterror.ErrUnsupportedDistTask
	}
	if hasSysDB(job) {
		if reorgMeta.IsDistReorg {
			logutil.DDLLogger().Info("cannot use distributed task execution on system DB",
				zap.Stringer("job", job))
		}
		reorgMeta.IsDistReorg = false
		reorgMeta.IsFastReorg = false
		failpoint.Inject("reorgMetaRecordFastReorgDisabled", func(_ failpoint.Value) {
			LastReorgMetaFastReorgDisabled = true
		})
	}
	return reorgMeta, nil
}

// LastReorgMetaFastReorgDisabled is used for test.
var LastReorgMetaFastReorgDisabled bool

func buildFKInfo(fkName model.CIStr, keys []*ast.IndexPartSpecification, refer *ast.ReferenceDef, cols []*table.Column) (*model.FKInfo, error) {
	if len(keys) != len(refer.IndexPartSpecifications) {
		return nil, infoschema.ErrForeignKeyNotMatch.GenWithStackByArgs(fkName, "Key reference and table reference don't match")
	}
	if err := checkTooLongForeignKey(fkName); err != nil {
		return nil, err
	}
	if err := checkTooLongSchema(refer.Table.Schema); err != nil {
		return nil, err
	}
	if err := checkTooLongTable(refer.Table.Name); err != nil {
		return nil, err
	}

	// all base columns of stored generated columns
	baseCols := make(map[string]struct{})
	for _, col := range cols {
		if col.IsGenerated() && col.GeneratedStored {
			for name := range col.Dependences {
				baseCols[name] = struct{}{}
			}
		}
	}

	fkInfo := &model.FKInfo{
		Name:      fkName,
		RefSchema: refer.Table.Schema,
		RefTable:  refer.Table.Name,
		Cols:      make([]model.CIStr, len(keys)),
	}
	if variable.EnableForeignKey.Load() {
		fkInfo.Version = model.FKVersion1
	}

	for i, key := range keys {
		// Check add foreign key to generated columns
		// For more detail, see https://dev.mysql.com/doc/refman/8.0/en/innodb-foreign-key-constraints.html#innodb-foreign-key-generated-columns
		for _, col := range cols {
			if col.Name.L != key.Column.Name.L {
				continue
			}
			if col.IsGenerated() {
				// Check foreign key on virtual generated columns
				if !col.GeneratedStored {
					return nil, infoschema.ErrForeignKeyCannotUseVirtualColumn.GenWithStackByArgs(fkInfo.Name.O, col.Name.O)
				}

				// Check wrong reference options of foreign key on stored generated columns
				switch refer.OnUpdate.ReferOpt {
				case model.ReferOptionCascade, model.ReferOptionSetNull, model.ReferOptionSetDefault:
					//nolint: gosec
					return nil, dbterror.ErrWrongFKOptionForGeneratedColumn.GenWithStackByArgs("ON UPDATE " + refer.OnUpdate.ReferOpt.String())
				}
				switch refer.OnDelete.ReferOpt {
				case model.ReferOptionSetNull, model.ReferOptionSetDefault:
					//nolint: gosec
					return nil, dbterror.ErrWrongFKOptionForGeneratedColumn.GenWithStackByArgs("ON DELETE " + refer.OnDelete.ReferOpt.String())
				}
				continue
			}
			// Check wrong reference options of foreign key on base columns of stored generated columns
			if _, ok := baseCols[col.Name.L]; ok {
				switch refer.OnUpdate.ReferOpt {
				case model.ReferOptionCascade, model.ReferOptionSetNull, model.ReferOptionSetDefault:
					return nil, infoschema.ErrCannotAddForeign
				}
				switch refer.OnDelete.ReferOpt {
				case model.ReferOptionCascade, model.ReferOptionSetNull, model.ReferOptionSetDefault:
					return nil, infoschema.ErrCannotAddForeign
				}
			}
		}
		col := table.FindCol(cols, key.Column.Name.O)
		if col == nil {
			return nil, dbterror.ErrKeyColumnDoesNotExits.GenWithStackByArgs(key.Column.Name)
		}
		if mysql.HasNotNullFlag(col.GetFlag()) && (refer.OnDelete.ReferOpt == model.ReferOptionSetNull || refer.OnUpdate.ReferOpt == model.ReferOptionSetNull) {
			return nil, infoschema.ErrForeignKeyColumnNotNull.GenWithStackByArgs(col.Name.O, fkName)
		}
		fkInfo.Cols[i] = key.Column.Name
	}

	fkInfo.RefCols = make([]model.CIStr, len(refer.IndexPartSpecifications))
	for i, key := range refer.IndexPartSpecifications {
		if err := checkTooLongColumn(key.Column.Name); err != nil {
			return nil, err
		}
		fkInfo.RefCols[i] = key.Column.Name
	}

	fkInfo.OnDelete = int(refer.OnDelete.ReferOpt)
	fkInfo.OnUpdate = int(refer.OnUpdate.ReferOpt)

	return fkInfo, nil
}

func (d *ddl) CreateForeignKey(ctx sessionctx.Context, ti ast.Ident, fkName model.CIStr, keys []*ast.IndexPartSpecification, refer *ast.ReferenceDef) error {
	is := d.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ti.Schema)
	}

	t, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name))
	}
	if t.Meta().TempTableType != model.TempTableNone {
		return infoschema.ErrCannotAddForeign
	}

	if fkName.L == "" {
		fkName = model.NewCIStr(fmt.Sprintf("fk_%d", t.Meta().MaxForeignKeyID+1))
	}
	err = checkFKDupName(t.Meta(), fkName)
	if err != nil {
		return err
	}
	fkInfo, err := buildFKInfo(fkName, keys, refer, t.Cols())
	if err != nil {
		return errors.Trace(err)
	}
	fkCheck := ctx.GetSessionVars().ForeignKeyChecks
	err = checkAddForeignKeyValid(is, schema.Name.L, t.Meta(), fkInfo, fkCheck)
	if err != nil {
		return err
	}
	if model.FindIndexByColumns(t.Meta(), t.Meta().Indices, fkInfo.Cols...) == nil {
		// Need to auto create index for fk cols
		if ctx.GetSessionVars().StmtCtx.MultiSchemaInfo == nil {
			ctx.GetSessionVars().StmtCtx.MultiSchemaInfo = model.NewMultiSchemaInfo()
		}
		indexPartSpecifications := make([]*ast.IndexPartSpecification, 0, len(fkInfo.Cols))
		for _, col := range fkInfo.Cols {
			indexPartSpecifications = append(indexPartSpecifications, &ast.IndexPartSpecification{
				Column: &ast.ColumnName{Name: col},
				Length: types.UnspecifiedLength, // Index prefixes on foreign key columns are not supported.
			})
		}
		indexOption := &ast.IndexOption{}
		err = d.createIndex(ctx, ti, ast.IndexKeyTypeNone, fkInfo.Name, indexPartSpecifications, indexOption, false)
		if err != nil {
			return err
		}
	}

	job := &model.Job{
		SchemaID:       schema.ID,
		TableID:        t.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      t.Meta().Name.L,
		Type:           model.ActionAddForeignKey,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           []any{fkInfo, fkCheck},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

func (d *ddl) DropForeignKey(ctx sessionctx.Context, ti ast.Ident, fkName model.CIStr) error {
	is := d.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ti.Schema)
	}

	t, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name))
	}

	job := &model.Job{
		SchemaID:       schema.ID,
		TableID:        t.Meta().ID,
		SchemaName:     schema.Name.L,
		SchemaState:    model.StatePublic,
		TableName:      t.Meta().Name.L,
		Type:           model.ActionDropForeignKey,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           []any{fkName},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

func (d *ddl) DropIndex(ctx sessionctx.Context, stmt *ast.DropIndexStmt) error {
	ti := ast.Ident{Schema: stmt.Table.Schema, Name: stmt.Table.Name}
	err := d.dropIndex(ctx, ti, model.NewCIStr(stmt.IndexName), stmt.IfExists, stmt.IsHypo)
	if (infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableNotExists.Equal(err)) && stmt.IfExists {
		err = nil
	}
	return err
}

// dropHypoIndexFromCtx drops this hypo-index from this ctx.
func (*ddl) dropHypoIndexFromCtx(ctx sessionctx.Context, schema, table, index model.CIStr, ifExists bool) error {
	sctx := ctx.GetSessionVars()
	if sctx.HypoIndexes != nil &&
		sctx.HypoIndexes[schema.L] != nil &&
		sctx.HypoIndexes[schema.L][table.L] != nil &&
		sctx.HypoIndexes[schema.L][table.L][index.L] != nil {
		delete(sctx.HypoIndexes[schema.L][table.L], index.L)
		return nil
	}
	if !ifExists {
		return dbterror.ErrCantDropFieldOrKey.GenWithStack("index %s doesn't exist", index)
	}
	return nil
}

// dropIndex drops the specified index.
// isHypo is used to indicate whether this operation is for a hypo-index.
func (d *ddl) dropIndex(ctx sessionctx.Context, ti ast.Ident, indexName model.CIStr, ifExists, isHypo bool) error {
	is := d.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists)
	}
	t, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name))
	}
	if t.Meta().TableCacheStatusType != model.TableCacheStatusDisable {
		return errors.Trace(dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Drop Index"))
	}

	if isHypo {
		return d.dropHypoIndexFromCtx(ctx, ti.Schema, ti.Name, indexName, ifExists)
	}

	indexInfo := t.Meta().FindIndexByName(indexName.L)

	isPK, err := CheckIsDropPrimaryKey(indexName, indexInfo, t)
	if err != nil {
		return err
	}

	if !ctx.GetSessionVars().InRestrictedSQL && ctx.GetSessionVars().PrimaryKeyRequired && isPK {
		return infoschema.ErrTableWithoutPrimaryKey
	}

	if indexInfo == nil {
		err = dbterror.ErrCantDropFieldOrKey.GenWithStack("index %s doesn't exist", indexName)
		if ifExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return err
	}

	err = checkIndexNeededInForeignKey(is, schema.Name.L, t.Meta(), indexInfo)
	if err != nil {
		return err
	}

	jobTp := model.ActionDropIndex
	if isPK {
		jobTp = model.ActionDropPrimaryKey
	}

	job := &model.Job{
		SchemaID:       schema.ID,
		TableID:        t.Meta().ID,
		SchemaName:     schema.Name.L,
		SchemaState:    indexInfo.State,
		TableName:      t.Meta().Name.L,
		Type:           jobTp,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           []any{indexName, ifExists},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

// CheckIsDropPrimaryKey checks if we will drop PK, there are many PK implementations so we provide a helper function.
func CheckIsDropPrimaryKey(indexName model.CIStr, indexInfo *model.IndexInfo, t table.Table) (bool, error) {
	var isPK bool
	if indexName.L == strings.ToLower(mysql.PrimaryKeyName) &&
		// Before we fixed #14243, there might be a general index named `primary` but not a primary key.
		(indexInfo == nil || indexInfo.Primary) {
		isPK = true
	}
	if isPK {
		// If the table's PKIsHandle is true, we can't find the index from the table. So we check the value of PKIsHandle.
		if indexInfo == nil && !t.Meta().PKIsHandle {
			return isPK, dbterror.ErrCantDropFieldOrKey.GenWithStackByArgs("PRIMARY")
		}
		if t.Meta().IsCommonHandle || t.Meta().PKIsHandle {
			return isPK, dbterror.ErrUnsupportedModifyPrimaryKey.GenWithStack("Unsupported drop primary key when the table is using clustered index")
		}
	}

	return isPK, nil
}

func isDroppableColumn(tblInfo *model.TableInfo, colName model.CIStr) error {
	if ok, dep, isHidden := hasDependentByGeneratedColumn(tblInfo, colName); ok {
		if isHidden {
			return dbterror.ErrDependentByFunctionalIndex.GenWithStackByArgs(dep)
		}
		return dbterror.ErrDependentByGeneratedColumn.GenWithStackByArgs(dep)
	}

	if len(tblInfo.Columns) == 1 {
		return dbterror.ErrCantRemoveAllFields.GenWithStack("can't drop only column %s in table %s",
			colName, tblInfo.Name)
	}
	// We only support dropping column with single-value none Primary Key index covered now.
	err := isColumnCanDropWithIndex(colName.L, tblInfo.Indices)
	if err != nil {
		return err
	}
	err = IsColumnDroppableWithCheckConstraint(colName, tblInfo)
	if err != nil {
		return err
	}
	return nil
}

// validateCommentLength checks comment length of table, column, or index
// If comment length is more than the standard length truncate it
// and store the comment length upto the standard comment length size.
func validateCommentLength(ec errctx.Context, sqlMode mysql.SQLMode, name string, comment *string, errTooLongComment *terror.Error) (string, error) {
	if comment == nil {
		return "", nil
	}

	maxLen := MaxCommentLength
	// The maximum length of table comment in MySQL 5.7 is 2048
	// Other comment is 1024
	switch errTooLongComment {
	case dbterror.ErrTooLongTableComment:
		maxLen *= 2
	case dbterror.ErrTooLongFieldComment, dbterror.ErrTooLongIndexComment, dbterror.ErrTooLongTablePartitionComment:
	default:
		// add more types of terror.Error if need
	}
	if len(*comment) > maxLen {
		err := errTooLongComment.GenWithStackByArgs(name, maxLen)
		if sqlMode.HasStrictMode() {
			// may be treated like an error.
			return "", err
		}
		ec.AppendWarning(err)
		*comment = (*comment)[:maxLen]
	}
	return *comment, nil
}

// BuildAddedPartitionInfo build alter table add partition info
func BuildAddedPartitionInfo(ctx expression.BuildContext, meta *model.TableInfo, spec *ast.AlterTableSpec) (*model.PartitionInfo, error) {
	numParts := uint64(0)
	switch meta.Partition.Type {
	case model.PartitionTypeNone:
		// OK
	case model.PartitionTypeList:
		if len(spec.PartDefinitions) == 0 {
			return nil, ast.ErrPartitionsMustBeDefined.GenWithStackByArgs(meta.Partition.Type)
		}
		err := checkListPartitions(spec.PartDefinitions)
		if err != nil {
			return nil, err
		}

	case model.PartitionTypeRange:
		if spec.Tp == ast.AlterTableAddLastPartition {
			err := buildAddedPartitionDefs(ctx, meta, spec)
			if err != nil {
				return nil, err
			}
			spec.PartDefinitions = spec.Partition.Definitions
		} else {
			if len(spec.PartDefinitions) == 0 {
				return nil, ast.ErrPartitionsMustBeDefined.GenWithStackByArgs(meta.Partition.Type)
			}
		}
	case model.PartitionTypeHash, model.PartitionTypeKey:
		switch spec.Tp {
		case ast.AlterTableRemovePartitioning:
			numParts = 1
		default:
			return nil, errors.Trace(dbterror.ErrUnsupportedAddPartition)
		case ast.AlterTableCoalescePartitions:
			if int(spec.Num) >= len(meta.Partition.Definitions) {
				return nil, dbterror.ErrDropLastPartition
			}
			numParts = uint64(len(meta.Partition.Definitions)) - spec.Num
		case ast.AlterTableAddPartitions:
			if len(spec.PartDefinitions) > 0 {
				numParts = uint64(len(meta.Partition.Definitions)) + uint64(len(spec.PartDefinitions))
			} else {
				numParts = uint64(len(meta.Partition.Definitions)) + spec.Num
			}
		}
	default:
		// we don't support ADD PARTITION for all other partition types yet.
		return nil, errors.Trace(dbterror.ErrUnsupportedAddPartition)
	}

	part := &model.PartitionInfo{
		Type:    meta.Partition.Type,
		Expr:    meta.Partition.Expr,
		Columns: meta.Partition.Columns,
		Enable:  meta.Partition.Enable,
	}

	defs, err := buildPartitionDefinitionsInfo(ctx, spec.PartDefinitions, meta, numParts)
	if err != nil {
		return nil, err
	}

	part.Definitions = defs
	part.Num = uint64(len(defs))
	return part, nil
}

func buildAddedPartitionDefs(ctx expression.BuildContext, meta *model.TableInfo, spec *ast.AlterTableSpec) error {
	partInterval := getPartitionIntervalFromTable(ctx, meta)
	if partInterval == nil {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs(
			"LAST PARTITION, does not seem like an INTERVAL partitioned table")
	}
	if partInterval.MaxValPart {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("LAST PARTITION when MAXVALUE partition exists")
	}

	spec.Partition.Interval = partInterval

	if len(spec.PartDefinitions) > 0 {
		return errors.Trace(dbterror.ErrUnsupportedAddPartition)
	}
	return GeneratePartDefsFromInterval(ctx, spec.Tp, meta, spec.Partition)
}

func checkAndGetColumnsTypeAndValuesMatch(ctx expression.BuildContext, colTypes []types.FieldType, exprs []ast.ExprNode) ([]types.Datum, error) {
	// Validate() has already checked len(colNames) = len(exprs)
	// create table ... partition by range columns (cols)
	// partition p0 values less than (expr)
	// check the type of cols[i] and expr is consistent.
	valDatums := make([]types.Datum, 0, len(colTypes))
	for i, colExpr := range exprs {
		if _, ok := colExpr.(*ast.MaxValueExpr); ok {
			valDatums = append(valDatums, types.NewStringDatum(partitionMaxValue))
			continue
		}
		if d, ok := colExpr.(*ast.DefaultExpr); ok {
			if d.Name != nil {
				return nil, dbterror.ErrWrongTypeColumnValue.GenWithStackByArgs()
			}
			continue
		}
		colType := colTypes[i]
		val, err := expression.EvalSimpleAst(ctx, colExpr)
		if err != nil {
			return nil, err
		}
		// Check val.ConvertTo(colType) doesn't work, so we need this case by case check.
		vkind := val.Kind()
		switch colType.GetType() {
		case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeDuration:
			switch vkind {
			case types.KindString, types.KindBytes, types.KindNull:
			default:
				return nil, dbterror.ErrWrongTypeColumnValue.GenWithStackByArgs()
			}
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
			switch vkind {
			case types.KindInt64, types.KindUint64, types.KindNull:
			default:
				return nil, dbterror.ErrWrongTypeColumnValue.GenWithStackByArgs()
			}
		case mysql.TypeFloat, mysql.TypeDouble:
			switch vkind {
			case types.KindFloat32, types.KindFloat64, types.KindNull:
			default:
				return nil, dbterror.ErrWrongTypeColumnValue.GenWithStackByArgs()
			}
		case mysql.TypeString, mysql.TypeVarString:
			switch vkind {
			case types.KindString, types.KindBytes, types.KindNull, types.KindBinaryLiteral:
			default:
				return nil, dbterror.ErrWrongTypeColumnValue.GenWithStackByArgs()
			}
		}
		evalCtx := ctx.GetEvalCtx()
		newVal, err := val.ConvertTo(evalCtx.TypeCtx(), &colType)
		if err != nil {
			return nil, dbterror.ErrWrongTypeColumnValue.GenWithStackByArgs()
		}
		valDatums = append(valDatums, newVal)
	}
	return valDatums, nil
}

// LockTables uses to execute lock tables statement.
func (d *ddl) LockTables(ctx sessionctx.Context, stmt *ast.LockTablesStmt) error {
	lockTables := make([]model.TableLockTpInfo, 0, len(stmt.TableLocks))
	sessionInfo := model.SessionInfo{
		ServerID:  d.GetID(),
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
		schema, t, err := d.getSchemaAndTableByIdent(ctx, ast.Ident{Schema: tb.Schema, Name: tb.Name})
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
	arg := &LockTablesArg{
		LockTables:   lockTables,
		UnlockTables: unlockTables,
		SessionInfo:  sessionInfo,
	}
	job := &model.Job{
		SchemaID:            lockTables[0].SchemaID,
		TableID:             lockTables[0].TableID,
		Type:                model.ActionLockTable,
		BinlogInfo:          &model.HistoryInfo{},
		Args:                []any{arg},
		CDCWriteSource:      ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: involveSchemaInfo,
		SQLMode:             ctx.GetSessionVars().SQLMode,
	}
	// AddTableLock here is avoiding this job was executed successfully but the session was killed before return.
	ctx.AddTableLock(lockTables)
	err := d.DoDDLJob(ctx, job)
	if err == nil {
		ctx.ReleaseTableLocks(unlockTables)
		ctx.AddTableLock(lockTables)
	}
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

// UnlockTables uses to execute unlock tables statement.
func (d *ddl) UnlockTables(ctx sessionctx.Context, unlockTables []model.TableLockTpInfo) error {
	if len(unlockTables) == 0 {
		return nil
	}
	arg := &LockTablesArg{
		UnlockTables: unlockTables,
		SessionInfo: model.SessionInfo{
			ServerID:  d.GetID(),
			SessionID: ctx.GetSessionVars().ConnectionID,
		},
	}
	job := &model.Job{
		SchemaID:       unlockTables[0].SchemaID,
		TableID:        unlockTables[0].TableID,
		Type:           model.ActionUnlockTable,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           []any{arg},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	err := d.DoDDLJob(ctx, job)
	if err == nil {
		ctx.ReleaseAllTableLocks()
	}
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

// CleanDeadTableLock uses to clean dead table locks.
func (d *ddl) CleanDeadTableLock(unlockTables []model.TableLockTpInfo, se model.SessionInfo) error {
	if len(unlockTables) == 0 {
		return nil
	}
	arg := &LockTablesArg{
		UnlockTables: unlockTables,
		SessionInfo:  se,
	}
	job := &model.Job{
		SchemaID:   unlockTables[0].SchemaID,
		TableID:    unlockTables[0].TableID,
		Type:       model.ActionUnlockTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []any{arg},
	}

	ctx, err := d.sessPool.Get()
	if err != nil {
		return err
	}
	defer d.sessPool.Put(ctx)
	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

func throwErrIfInMemOrSysDB(ctx sessionctx.Context, dbLowerName string) error {
	if util.IsMemOrSysDB(dbLowerName) {
		if ctx.GetSessionVars().User != nil {
			return infoschema.ErrAccessDenied.GenWithStackByArgs(ctx.GetSessionVars().User.Username, ctx.GetSessionVars().User.Hostname)
		}
		return infoschema.ErrAccessDenied.GenWithStackByArgs("", "")
	}
	return nil
}

func (d *ddl) CleanupTableLock(ctx sessionctx.Context, tables []*ast.TableName) error {
	uniqueTableID := make(map[int64]struct{})
	cleanupTables := make([]model.TableLockTpInfo, 0, len(tables))
	unlockedTablesNum := 0
	// Check whether the table was already locked by another.
	for _, tb := range tables {
		err := throwErrIfInMemOrSysDB(ctx, tb.Schema.L)
		if err != nil {
			return err
		}
		schema, t, err := d.getSchemaAndTableByIdent(ctx, ast.Ident{Schema: tb.Schema, Name: tb.Name})
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
	}
	// If the num of cleanupTables is 0, or all cleanupTables is unlocked, just return here.
	if len(cleanupTables) == 0 || len(cleanupTables) == unlockedTablesNum {
		return nil
	}

	arg := &LockTablesArg{
		UnlockTables: cleanupTables,
		IsCleanup:    true,
	}
	job := &model.Job{
		SchemaID:       cleanupTables[0].SchemaID,
		TableID:        cleanupTables[0].TableID,
		Type:           model.ActionUnlockTable,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           []any{arg},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	err := d.DoDDLJob(ctx, job)
	if err == nil {
		ctx.ReleaseTableLocks(cleanupTables)
	}
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

// LockTablesArg is the argument for LockTables, export for test.
type LockTablesArg struct {
	LockTables    []model.TableLockTpInfo
	IndexOfLock   int
	UnlockTables  []model.TableLockTpInfo
	IndexOfUnlock int
	SessionInfo   model.SessionInfo
	IsCleanup     bool
}

func (d *ddl) RepairTable(ctx sessionctx.Context, createStmt *ast.CreateTableStmt) error {
	// Existence of DB and table has been checked in the preprocessor.
	oldTableInfo, ok := (ctx.Value(domainutil.RepairedTable)).(*model.TableInfo)
	if !ok || oldTableInfo == nil {
		return dbterror.ErrRepairTableFail.GenWithStack("Failed to get the repaired table")
	}
	oldDBInfo, ok := (ctx.Value(domainutil.RepairedDatabase)).(*model.DBInfo)
	if !ok || oldDBInfo == nil {
		return dbterror.ErrRepairTableFail.GenWithStack("Failed to get the repaired database")
	}
	// By now only support same DB repair.
	if createStmt.Table.Schema.L != oldDBInfo.Name.L {
		return dbterror.ErrRepairTableFail.GenWithStack("Repaired table should in same database with the old one")
	}

	// It is necessary to specify the table.ID and partition.ID manually.
	newTableInfo, err := buildTableInfoWithCheck(ctx, createStmt, oldTableInfo.Charset, oldTableInfo.Collate, oldTableInfo.PlacementPolicyRef)
	if err != nil {
		return errors.Trace(err)
	}
	// Override newTableInfo with oldTableInfo's element necessary.
	// TODO: There may be more element assignments here, and the new TableInfo should be verified with the actual data.
	newTableInfo.ID = oldTableInfo.ID
	if err = checkAndOverridePartitionID(newTableInfo, oldTableInfo); err != nil {
		return err
	}
	newTableInfo.AutoIncID = oldTableInfo.AutoIncID
	// If any old columnInfo has lost, that means the old column ID lost too, repair failed.
	for i, newOne := range newTableInfo.Columns {
		old := oldTableInfo.FindPublicColumnByName(newOne.Name.L)
		if old == nil {
			return dbterror.ErrRepairTableFail.GenWithStackByArgs("Column " + newOne.Name.L + " has lost")
		}
		if newOne.GetType() != old.GetType() {
			return dbterror.ErrRepairTableFail.GenWithStackByArgs("Column " + newOne.Name.L + " type should be the same")
		}
		if newOne.GetFlen() != old.GetFlen() {
			logutil.DDLLogger().Warn("admin repair table : Column " + newOne.Name.L + " flen is not equal to the old one")
		}
		newTableInfo.Columns[i].ID = old.ID
	}
	// If any old indexInfo has lost, that means the index ID lost too, so did the data, repair failed.
	for i, newOne := range newTableInfo.Indices {
		old := getIndexInfoByNameAndColumn(oldTableInfo, newOne)
		if old == nil {
			return dbterror.ErrRepairTableFail.GenWithStackByArgs("Index " + newOne.Name.L + " has lost")
		}
		if newOne.Tp != old.Tp {
			return dbterror.ErrRepairTableFail.GenWithStackByArgs("Index " + newOne.Name.L + " type should be the same")
		}
		newTableInfo.Indices[i].ID = old.ID
	}

	newTableInfo.State = model.StatePublic
	err = checkTableInfoValid(newTableInfo)
	if err != nil {
		return err
	}
	newTableInfo.State = model.StateNone

	job := &model.Job{
		SchemaID:       oldDBInfo.ID,
		TableID:        newTableInfo.ID,
		SchemaName:     oldDBInfo.Name.L,
		TableName:      newTableInfo.Name.L,
		Type:           model.ActionRepairTable,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           []any{newTableInfo},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	err = d.DoDDLJob(ctx, job)
	if err == nil {
		// Remove the old TableInfo from repairInfo before domain reload.
		domainutil.RepairInfo.RemoveFromRepairInfo(oldDBInfo.Name.L, oldTableInfo.Name.L)
	}
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

func (d *ddl) OrderByColumns(ctx sessionctx.Context, ident ast.Ident) error {
	_, tb, err := d.getSchemaAndTableByIdent(ctx, ident)
	if err != nil {
		return errors.Trace(err)
	}
	if tb.Meta().GetPkColInfo() != nil {
		ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("ORDER BY ignored as there is a user-defined clustered index in the table '%s'", ident.Name))
	}
	return nil
}

func (d *ddl) CreateSequence(ctx sessionctx.Context, stmt *ast.CreateSequenceStmt) error {
	ident := ast.Ident{Name: stmt.Name.Name, Schema: stmt.Name.Schema}
	sequenceInfo, err := buildSequenceInfo(stmt, ident)
	if err != nil {
		return err
	}
	// TiDB describe the sequence within a tableInfo, as a same-level object of a table and view.
	tbInfo, err := BuildTableInfo(ctx, ident.Name, nil, nil, "", "")
	if err != nil {
		return err
	}
	tbInfo.Sequence = sequenceInfo

	onExist := OnExistError
	if stmt.IfNotExists {
		onExist = OnExistIgnore
	}

	return d.CreateTableWithInfo(ctx, ident.Schema, tbInfo, onExist)
}

func (d *ddl) AlterSequence(ctx sessionctx.Context, stmt *ast.AlterSequenceStmt) error {
	ident := ast.Ident{Name: stmt.Name.Name, Schema: stmt.Name.Schema}
	is := d.GetInfoSchemaWithInterceptor(ctx)
	// Check schema existence.
	db, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}
	// Check table existence.
	tbl, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		if stmt.IfExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return err
	}
	if !tbl.Meta().IsSequence() {
		return dbterror.ErrWrongObject.GenWithStackByArgs(ident.Schema, ident.Name, "SEQUENCE")
	}

	// Validate the new sequence option value in old sequenceInfo.
	oldSequenceInfo := tbl.Meta().Sequence
	copySequenceInfo := *oldSequenceInfo
	_, _, err = alterSequenceOptions(stmt.SeqOptions, ident, &copySequenceInfo)
	if err != nil {
		return err
	}

	job := &model.Job{
		SchemaID:       db.ID,
		TableID:        tbl.Meta().ID,
		SchemaName:     db.Name.L,
		TableName:      tbl.Meta().Name.L,
		Type:           model.ActionAlterSequence,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           []any{ident, stmt.SeqOptions},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

func (d *ddl) DropSequence(ctx sessionctx.Context, stmt *ast.DropSequenceStmt) (err error) {
	return d.dropTableObject(ctx, stmt.Sequences, stmt.IfExists, sequenceObject)
}

func (d *ddl) AlterIndexVisibility(ctx sessionctx.Context, ident ast.Ident, indexName model.CIStr, visibility ast.IndexVisibility) error {
	schema, tb, err := d.getSchemaAndTableByIdent(ctx, ident)
	if err != nil {
		return err
	}

	invisible := false
	if visibility == ast.IndexVisibilityInvisible {
		invisible = true
	}

	skip, err := validateAlterIndexVisibility(ctx, indexName, invisible, tb.Meta())
	if err != nil {
		return errors.Trace(err)
	}
	if skip {
		return nil
	}

	job := &model.Job{
		SchemaID:       schema.ID,
		TableID:        tb.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      tb.Meta().Name.L,
		Type:           model.ActionAlterIndexVisibility,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           []any{indexName, invisible},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

func (d *ddl) AlterTableAttributes(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	schema, tb, err := d.getSchemaAndTableByIdent(ctx, ident)
	if err != nil {
		return errors.Trace(err)
	}
	meta := tb.Meta()

	rule := label.NewRule()
	err = rule.ApplyAttributesSpec(spec.AttributesSpec)
	if err != nil {
		return dbterror.ErrInvalidAttributesSpec.GenWithStackByArgs(err)
	}
	ids := getIDs([]*model.TableInfo{meta})
	rule.Reset(schema.Name.L, meta.Name.L, "", ids...)

	job := &model.Job{
		SchemaID:       schema.ID,
		TableID:        meta.ID,
		SchemaName:     schema.Name.L,
		TableName:      meta.Name.L,
		Type:           model.ActionAlterTableAttributes,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           []any{rule},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	err = d.DoDDLJob(ctx, job)
	if err != nil {
		return errors.Trace(err)
	}

	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

func (d *ddl) AlterTablePartitionAttributes(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) (err error) {
	schema, tb, err := d.getSchemaAndTableByIdent(ctx, ident)
	if err != nil {
		return errors.Trace(err)
	}

	meta := tb.Meta()
	if meta.Partition == nil {
		return errors.Trace(dbterror.ErrPartitionMgmtOnNonpartitioned)
	}

	partitionID, err := tables.FindPartitionByName(meta, spec.PartitionNames[0].L)
	if err != nil {
		return errors.Trace(err)
	}

	rule := label.NewRule()
	err = rule.ApplyAttributesSpec(spec.AttributesSpec)
	if err != nil {
		return dbterror.ErrInvalidAttributesSpec.GenWithStackByArgs(err)
	}
	rule.Reset(schema.Name.L, meta.Name.L, spec.PartitionNames[0].L, partitionID)

	job := &model.Job{
		SchemaID:       schema.ID,
		TableID:        meta.ID,
		SchemaName:     schema.Name.L,
		TableName:      meta.Name.L,
		Type:           model.ActionAlterTablePartitionAttributes,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           []any{partitionID, rule},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	err = d.DoDDLJob(ctx, job)
	if err != nil {
		return errors.Trace(err)
	}

	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

func (d *ddl) AlterTablePartitionOptions(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) (err error) {
	var policyRefInfo *model.PolicyRefInfo
	if spec.Options != nil {
		for _, op := range spec.Options {
			switch op.Tp {
			case ast.TableOptionPlacementPolicy:
				policyRefInfo = &model.PolicyRefInfo{
					Name: model.NewCIStr(op.StrValue),
				}
			default:
				return errors.Trace(errors.New("unknown partition option"))
			}
		}
	}

	if policyRefInfo != nil {
		err = d.AlterTablePartitionPlacement(ctx, ident, spec, policyRefInfo)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (d *ddl) AlterTablePartitionPlacement(ctx sessionctx.Context, tableIdent ast.Ident, spec *ast.AlterTableSpec, policyRefInfo *model.PolicyRefInfo) (err error) {
	schema, tb, err := d.getSchemaAndTableByIdent(ctx, tableIdent)
	if err != nil {
		return errors.Trace(err)
	}

	tblInfo := tb.Meta()
	if tblInfo.Partition == nil {
		return errors.Trace(dbterror.ErrPartitionMgmtOnNonpartitioned)
	}

	partitionID, err := tables.FindPartitionByName(tblInfo, spec.PartitionNames[0].L)
	if err != nil {
		return errors.Trace(err)
	}

	if checkIgnorePlacementDDL(ctx) {
		return nil
	}

	policyRefInfo, err = checkAndNormalizePlacementPolicy(ctx, policyRefInfo)
	if err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		SchemaID:       schema.ID,
		TableID:        tblInfo.ID,
		SchemaName:     schema.Name.L,
		TableName:      tblInfo.Name.L,
		Type:           model.ActionAlterTablePartitionPlacement,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           []any{partitionID, policyRefInfo},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

func buildPolicyInfo(name model.CIStr, options []*ast.PlacementOption) (*model.PolicyInfo, error) {
	policyInfo := &model.PolicyInfo{PlacementSettings: &model.PlacementSettings{}}
	policyInfo.Name = name
	for _, opt := range options {
		err := SetDirectPlacementOpt(policyInfo.PlacementSettings, opt.Tp, opt.StrValue, opt.UintValue)
		if err != nil {
			return nil, err
		}
	}
	return policyInfo, nil
}

func removeTablePlacement(tbInfo *model.TableInfo) bool {
	hasPlacementSettings := false
	if tbInfo.PlacementPolicyRef != nil {
		tbInfo.PlacementPolicyRef = nil
		hasPlacementSettings = true
	}

	if removePartitionPlacement(tbInfo.Partition) {
		hasPlacementSettings = true
	}

	return hasPlacementSettings
}

func removePartitionPlacement(partInfo *model.PartitionInfo) bool {
	if partInfo == nil {
		return false
	}

	hasPlacementSettings := false
	for i := range partInfo.Definitions {
		def := &partInfo.Definitions[i]
		if def.PlacementPolicyRef != nil {
			def.PlacementPolicyRef = nil
			hasPlacementSettings = true
		}
	}
	return hasPlacementSettings
}

func handleDatabasePlacement(ctx sessionctx.Context, dbInfo *model.DBInfo) error {
	if dbInfo.PlacementPolicyRef == nil {
		return nil
	}

	sessVars := ctx.GetSessionVars()
	if sessVars.PlacementMode == variable.PlacementModeIgnore {
		dbInfo.PlacementPolicyRef = nil
		sessVars.StmtCtx.AppendNote(
			errors.NewNoStackErrorf("Placement is ignored when TIDB_PLACEMENT_MODE is '%s'", variable.PlacementModeIgnore),
		)
		return nil
	}

	var err error
	dbInfo.PlacementPolicyRef, err = checkAndNormalizePlacementPolicy(ctx, dbInfo.PlacementPolicyRef)
	return err
}

func handleTablePlacement(ctx sessionctx.Context, tbInfo *model.TableInfo) error {
	sessVars := ctx.GetSessionVars()
	if sessVars.PlacementMode == variable.PlacementModeIgnore && removeTablePlacement(tbInfo) {
		sessVars.StmtCtx.AppendNote(
			errors.NewNoStackErrorf("Placement is ignored when TIDB_PLACEMENT_MODE is '%s'", variable.PlacementModeIgnore),
		)
		return nil
	}

	var err error
	tbInfo.PlacementPolicyRef, err = checkAndNormalizePlacementPolicy(ctx, tbInfo.PlacementPolicyRef)
	if err != nil {
		return err
	}

	if tbInfo.Partition != nil {
		for i := range tbInfo.Partition.Definitions {
			partition := &tbInfo.Partition.Definitions[i]
			partition.PlacementPolicyRef, err = checkAndNormalizePlacementPolicy(ctx, partition.PlacementPolicyRef)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func handlePartitionPlacement(ctx sessionctx.Context, partInfo *model.PartitionInfo) error {
	sessVars := ctx.GetSessionVars()
	if sessVars.PlacementMode == variable.PlacementModeIgnore && removePartitionPlacement(partInfo) {
		sessVars.StmtCtx.AppendNote(
			errors.NewNoStackErrorf("Placement is ignored when TIDB_PLACEMENT_MODE is '%s'", variable.PlacementModeIgnore),
		)
		return nil
	}

	var err error
	for i := range partInfo.Definitions {
		partition := &partInfo.Definitions[i]
		partition.PlacementPolicyRef, err = checkAndNormalizePlacementPolicy(ctx, partition.PlacementPolicyRef)
		if err != nil {
			return err
		}
	}
	return nil
}

func checkIgnorePlacementDDL(ctx sessionctx.Context) bool {
	sessVars := ctx.GetSessionVars()
	if sessVars.PlacementMode == variable.PlacementModeIgnore {
		sessVars.StmtCtx.AppendNote(
			errors.NewNoStackErrorf("Placement is ignored when TIDB_PLACEMENT_MODE is '%s'", variable.PlacementModeIgnore),
		)
		return true
	}
	return false
}

// AddResourceGroup implements the DDL interface, creates a resource group.
func (d *ddl) AddResourceGroup(ctx sessionctx.Context, stmt *ast.CreateResourceGroupStmt) (err error) {
	groupName := stmt.ResourceGroupName
	groupInfo := &model.ResourceGroupInfo{Name: groupName, ResourceGroupSettings: model.NewResourceGroupSettings()}
	groupInfo, err = buildResourceGroup(groupInfo, stmt.ResourceGroupOptionList)
	if err != nil {
		return err
	}

	if _, ok := d.GetInfoSchemaWithInterceptor(ctx).ResourceGroupByName(groupName); ok {
		if stmt.IfNotExists {
			err = infoschema.ErrResourceGroupExists.FastGenByArgs(groupName)
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return infoschema.ErrResourceGroupExists.GenWithStackByArgs(groupName)
	}

	if err := d.checkResourceGroupValidation(groupInfo); err != nil {
		return err
	}

	logutil.DDLLogger().Debug("create resource group", zap.String("name", groupName.O), zap.Stringer("resource group settings", groupInfo.ResourceGroupSettings))
	groupIDs, err := d.genGlobalIDs(1)
	if err != nil {
		return err
	}
	groupInfo.ID = groupIDs[0]

	job := &model.Job{
		SchemaName:     groupName.L,
		Type:           model.ActionCreateResourceGroup,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		Args:           []any{groupInfo, false},
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			Database: model.InvolvingNone,
			Table:    model.InvolvingNone,
		}},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}
	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	return err
}

func (*ddl) checkResourceGroupValidation(groupInfo *model.ResourceGroupInfo) error {
	_, err := resourcegroup.NewGroupFromOptions(groupInfo.Name.L, groupInfo.ResourceGroupSettings)
	return err
}

// DropResourceGroup implements the DDL interface.
func (d *ddl) DropResourceGroup(ctx sessionctx.Context, stmt *ast.DropResourceGroupStmt) (err error) {
	groupName := stmt.ResourceGroupName
	if groupName.L == rg.DefaultResourceGroupName {
		return resourcegroup.ErrDroppingInternalResourceGroup
	}
	is := d.GetInfoSchemaWithInterceptor(ctx)
	// Check group existence.
	group, ok := is.ResourceGroupByName(groupName)
	if !ok {
		err = infoschema.ErrResourceGroupNotExists.GenWithStackByArgs(groupName)
		if stmt.IfExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return err
	}

	// check to see if some user has dependency on the group
	checker := privilege.GetPrivilegeManager(ctx)
	if checker == nil {
		return errors.New("miss privilege checker")
	}
	user, matched := checker.MatchUserResourceGroupName(groupName.L)
	if matched {
		err = errors.Errorf("user [%s] depends on the resource group to drop", user)
		return err
	}

	job := &model.Job{
		SchemaID:       group.ID,
		SchemaName:     group.Name.L,
		Type:           model.ActionDropResourceGroup,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		Args:           []any{groupName},
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			Database: model.InvolvingNone,
			Table:    model.InvolvingNone,
		}},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}
	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	return err
}

func buildResourceGroup(oldGroup *model.ResourceGroupInfo, options []*ast.ResourceGroupOption) (*model.ResourceGroupInfo, error) {
	groupInfo := &model.ResourceGroupInfo{Name: oldGroup.Name, ID: oldGroup.ID, ResourceGroupSettings: model.NewResourceGroupSettings()}
	if oldGroup.ResourceGroupSettings != nil {
		*groupInfo.ResourceGroupSettings = *oldGroup.ResourceGroupSettings
	}
	for _, opt := range options {
		err := SetDirectResourceGroupSettings(groupInfo, opt)
		if err != nil {
			return nil, err
		}
	}
	groupInfo.ResourceGroupSettings.Adjust()
	return groupInfo, nil
}

// AlterResourceGroup implements the DDL interface.
func (d *ddl) AlterResourceGroup(ctx sessionctx.Context, stmt *ast.AlterResourceGroupStmt) (err error) {
	groupName := stmt.ResourceGroupName
	is := d.GetInfoSchemaWithInterceptor(ctx)
	// Check group existence.
	group, ok := is.ResourceGroupByName(groupName)
	if !ok {
		err := infoschema.ErrResourceGroupNotExists.GenWithStackByArgs(groupName)
		if stmt.IfExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return err
	}
	newGroupInfo, err := buildResourceGroup(group, stmt.ResourceGroupOptionList)
	if err != nil {
		return errors.Trace(err)
	}

	if err := d.checkResourceGroupValidation(newGroupInfo); err != nil {
		return err
	}

	logutil.DDLLogger().Debug("alter resource group", zap.String("name", groupName.L), zap.Stringer("new resource group settings", newGroupInfo.ResourceGroupSettings))

	job := &model.Job{
		SchemaID:       newGroupInfo.ID,
		SchemaName:     newGroupInfo.Name.L,
		Type:           model.ActionAlterResourceGroup,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		Args:           []any{newGroupInfo},
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			Database: model.InvolvingNone,
			Table:    model.InvolvingNone,
		}},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}
	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	return err
}

func (d *ddl) CreatePlacementPolicy(ctx sessionctx.Context, stmt *ast.CreatePlacementPolicyStmt) (err error) {
	if checkIgnorePlacementDDL(ctx) {
		return nil
	}

	if stmt.OrReplace && stmt.IfNotExists {
		return dbterror.ErrWrongUsage.GenWithStackByArgs("OR REPLACE", "IF NOT EXISTS")
	}

	policyInfo, err := buildPolicyInfo(stmt.PolicyName, stmt.PlacementOptions)
	if err != nil {
		return errors.Trace(err)
	}

	var onExists OnExist
	switch {
	case stmt.IfNotExists:
		onExists = OnExistIgnore
	case stmt.OrReplace:
		onExists = OnExistReplace
	default:
		onExists = OnExistError
	}

	return d.CreatePlacementPolicyWithInfo(ctx, policyInfo, onExists)
}

func (d *ddl) DropPlacementPolicy(ctx sessionctx.Context, stmt *ast.DropPlacementPolicyStmt) (err error) {
	if checkIgnorePlacementDDL(ctx) {
		return nil
	}
	policyName := stmt.PolicyName
	is := d.GetInfoSchemaWithInterceptor(ctx)
	// Check policy existence.
	policy, ok := is.PolicyByName(policyName)
	if !ok {
		err = infoschema.ErrPlacementPolicyNotExists.GenWithStackByArgs(policyName)
		if stmt.IfExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return err
	}

	if err = CheckPlacementPolicyNotInUseFromInfoSchema(is, policy); err != nil {
		return err
	}

	job := &model.Job{
		SchemaID:       policy.ID,
		SchemaName:     policy.Name.L,
		Type:           model.ActionDropPlacementPolicy,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		Args:           []any{policyName},
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			Database: model.InvolvingNone,
			Table:    model.InvolvingNone,
		}},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}
	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

func (d *ddl) AlterPlacementPolicy(ctx sessionctx.Context, stmt *ast.AlterPlacementPolicyStmt) (err error) {
	if checkIgnorePlacementDDL(ctx) {
		return nil
	}
	policyName := stmt.PolicyName
	is := d.GetInfoSchemaWithInterceptor(ctx)
	// Check policy existence.
	policy, ok := is.PolicyByName(policyName)
	if !ok {
		return infoschema.ErrPlacementPolicyNotExists.GenWithStackByArgs(policyName)
	}

	newPolicyInfo, err := buildPolicyInfo(policy.Name, stmt.PlacementOptions)
	if err != nil {
		return errors.Trace(err)
	}

	err = checkPolicyValidation(newPolicyInfo.PlacementSettings)
	if err != nil {
		return err
	}

	job := &model.Job{
		SchemaID:       policy.ID,
		SchemaName:     policy.Name.L,
		Type:           model.ActionAlterPlacementPolicy,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		Args:           []any{newPolicyInfo},
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			Database: model.InvolvingNone,
			Table:    model.InvolvingNone,
		}},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}
	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

func (d *ddl) AlterTableCache(sctx sessionctx.Context, ti ast.Ident) (err error) {
	schema, t, err := d.getSchemaAndTableByIdent(sctx, ti)
	if err != nil {
		return err
	}
	// if a table is already in cache state, return directly
	if t.Meta().TableCacheStatusType == model.TableCacheStatusEnable {
		return nil
	}

	// forbidden cache table in system database.
	if util.IsMemOrSysDB(schema.Name.L) {
		return errors.Trace(dbterror.ErrUnsupportedAlterCacheForSysTable)
	} else if t.Meta().TempTableType != model.TempTableNone {
		return dbterror.ErrOptOnTemporaryTable.GenWithStackByArgs("alter temporary table cache")
	}

	if t.Meta().Partition != nil {
		return dbterror.ErrOptOnCacheTable.GenWithStackByArgs("partition mode")
	}

	succ, err := checkCacheTableSize(d.store, t.Meta().ID)
	if err != nil {
		return errors.Trace(err)
	}
	if !succ {
		return dbterror.ErrOptOnCacheTable.GenWithStackByArgs("table too large")
	}

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	ddlQuery, _ := sctx.Value(sessionctx.QueryString).(string)
	// Initialize the cached table meta lock info in `mysql.table_cache_meta`.
	// The operation shouldn't fail in most cases, and if it does, return the error directly.
	// This DML and the following DDL is not atomic, that's not a problem.
	_, _, err = sctx.GetRestrictedSQLExecutor().ExecRestrictedSQL(ctx, nil,
		"replace into mysql.table_cache_meta values (%?, 'NONE', 0, 0)", t.Meta().ID)
	if err != nil {
		return errors.Trace(err)
	}

	sctx.SetValue(sessionctx.QueryString, ddlQuery)

	job := &model.Job{
		SchemaID:       schema.ID,
		SchemaName:     schema.Name.L,
		TableName:      t.Meta().Name.L,
		TableID:        t.Meta().ID,
		Type:           model.ActionAlterCacheTable,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           []any{},
		CDCWriteSource: sctx.GetSessionVars().CDCWriteSource,
		SQLMode:        sctx.GetSessionVars().SQLMode,
	}

	err = d.DoDDLJob(sctx, job)
	return d.callHookOnChanged(job, err)
}

func checkCacheTableSize(store kv.Storage, tableID int64) (bool, error) {
	const cacheTableSizeLimit = 64 * (1 << 20) // 64M
	succ := true
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnCacheTable)
	err := kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
		txn.SetOption(kv.RequestSourceType, kv.InternalTxnCacheTable)
		prefix := tablecodec.GenTablePrefix(tableID)
		it, err := txn.Iter(prefix, prefix.PrefixNext())
		if err != nil {
			return errors.Trace(err)
		}
		defer it.Close()

		totalSize := 0
		for it.Valid() && it.Key().HasPrefix(prefix) {
			key := it.Key()
			value := it.Value()
			totalSize += len(key)
			totalSize += len(value)

			if totalSize > cacheTableSizeLimit {
				succ = false
				break
			}

			err = it.Next()
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	})
	return succ, err
}

func (d *ddl) AlterTableNoCache(ctx sessionctx.Context, ti ast.Ident) (err error) {
	schema, t, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		return err
	}
	// if a table is not in cache state, return directly
	if t.Meta().TableCacheStatusType == model.TableCacheStatusDisable {
		return nil
	}

	job := &model.Job{
		SchemaID:       schema.ID,
		SchemaName:     schema.Name.L,
		TableName:      t.Meta().Name.L,
		TableID:        t.Meta().ID,
		Type:           model.ActionAlterNoCacheTable,
		BinlogInfo:     &model.HistoryInfo{},
		Args:           []any{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	err = d.DoDDLJob(ctx, job)
	return d.callHookOnChanged(job, err)
}

// checkTooBigFieldLengthAndTryAutoConvert will check whether the field length is too big
// in non-strict mode and varchar column. If it is, will try to adjust to blob or text, see issue #30328
func checkTooBigFieldLengthAndTryAutoConvert(tp *types.FieldType, colName string, sessVars *variable.SessionVars) error {
	if sessVars != nil && !sessVars.SQLMode.HasStrictMode() && tp.GetType() == mysql.TypeVarchar {
		err := types.IsVarcharTooBigFieldLength(tp.GetFlen(), colName, tp.GetCharset())
		if err != nil && terror.ErrorEqual(types.ErrTooBigFieldLength, err) {
			tp.SetType(mysql.TypeBlob)
			if err = adjustBlobTypesFlen(tp, tp.GetCharset()); err != nil {
				return err
			}
			if tp.GetCharset() == charset.CharsetBin {
				sessVars.StmtCtx.AppendWarning(dbterror.ErrAutoConvert.FastGenByArgs(colName, "VARBINARY", "BLOB"))
			} else {
				sessVars.StmtCtx.AppendWarning(dbterror.ErrAutoConvert.FastGenByArgs(colName, "VARCHAR", "TEXT"))
			}
		}
	}
	return nil
}

func (d *ddl) CreateCheckConstraint(ctx sessionctx.Context, ti ast.Ident, constrName model.CIStr, constr *ast.Constraint) error {
	schema, t, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		return errors.Trace(err)
	}
	if constraintInfo := t.Meta().FindConstraintInfoByName(constrName.L); constraintInfo != nil {
		return infoschema.ErrCheckConstraintDupName.GenWithStackByArgs(constrName.L)
	}

	// allocate the temporary constraint name for dependency-check-error-output below.
	constrNames := map[string]bool{}
	for _, constr := range t.Meta().Constraints {
		constrNames[constr.Name.L] = true
	}
	setEmptyCheckConstraintName(t.Meta().Name.L, constrNames, []*ast.Constraint{constr})

	// existedColsMap can be used to check the existence of depended.
	existedColsMap := make(map[string]struct{})
	cols := t.Cols()
	for _, v := range cols {
		existedColsMap[v.Name.L] = struct{}{}
	}
	// check expression if supported
	if ok, err := table.IsSupportedExpr(constr); !ok {
		return err
	}

	dependedColsMap := findDependentColsInExpr(constr.Expr)
	dependedCols := make([]model.CIStr, 0, len(dependedColsMap))
	for k := range dependedColsMap {
		if _, ok := existedColsMap[k]; !ok {
			// The table constraint depended on a non-existed column.
			return dbterror.ErrBadField.GenWithStackByArgs(k, "check constraint "+constr.Name+" expression")
		}
		dependedCols = append(dependedCols, model.NewCIStr(k))
	}

	// build constraint meta info.
	tblInfo := t.Meta()

	// check auto-increment column
	if table.ContainsAutoIncrementCol(dependedCols, tblInfo) {
		return dbterror.ErrCheckConstraintRefersAutoIncrementColumn.GenWithStackByArgs(constr.Name)
	}
	// check foreign key
	if err := table.HasForeignKeyRefAction(tblInfo.ForeignKeys, nil, constr, dependedCols); err != nil {
		return err
	}
	constraintInfo, err := buildConstraintInfo(tblInfo, dependedCols, constr, model.StateNone)
	if err != nil {
		return errors.Trace(err)
	}
	// check if the expression is bool type
	if err := table.IfCheckConstraintExprBoolType(ctx.GetExprCtx().GetEvalCtx(), constraintInfo, tblInfo); err != nil {
		return err
	}
	job := &model.Job{
		SchemaID:       schema.ID,
		TableID:        tblInfo.ID,
		SchemaName:     schema.Name.L,
		TableName:      tblInfo.Name.L,
		Type:           model.ActionAddCheckConstraint,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		Args:           []any{constraintInfo},
		Priority:       ctx.GetSessionVars().DDLReorgPriority,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

func (d *ddl) DropCheckConstraint(ctx sessionctx.Context, ti ast.Ident, constrName model.CIStr) error {
	is := d.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists)
	}
	t, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name))
	}
	tblInfo := t.Meta()

	constraintInfo := tblInfo.FindConstraintInfoByName(constrName.L)
	if constraintInfo == nil {
		return dbterror.ErrConstraintNotFound.GenWithStackByArgs(constrName)
	}

	job := &model.Job{
		SchemaID:       schema.ID,
		TableID:        tblInfo.ID,
		SchemaName:     schema.Name.L,
		TableName:      tblInfo.Name.L,
		Type:           model.ActionDropCheckConstraint,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		Args:           []any{constrName},
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

func (d *ddl) AlterCheckConstraint(ctx sessionctx.Context, ti ast.Ident, constrName model.CIStr, enforced bool) error {
	is := d.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists)
	}
	t, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name))
	}
	tblInfo := t.Meta()

	constraintInfo := tblInfo.FindConstraintInfoByName(constrName.L)
	if constraintInfo == nil {
		return dbterror.ErrConstraintNotFound.GenWithStackByArgs(constrName)
	}

	job := &model.Job{
		SchemaID:       schema.ID,
		TableID:        tblInfo.ID,
		SchemaName:     schema.Name.L,
		TableName:      tblInfo.Name.L,
		Type:           model.ActionAlterCheckConstraint,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		Args:           []any{constrName, enforced},
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	err = d.DoDDLJob(ctx, job)
	err = d.callHookOnChanged(job, err)
	return errors.Trace(err)
}

// NewDDLReorgMeta create a DDL ReorgMeta.
func NewDDLReorgMeta(ctx sessionctx.Context) *model.DDLReorgMeta {
	tzName, tzOffset := ddlutil.GetTimeZone(ctx)
	return &model.DDLReorgMeta{
		SQLMode:           ctx.GetSessionVars().SQLMode,
		Warnings:          make(map[errors.ErrorID]*terror.Error),
		WarningsCount:     make(map[errors.ErrorID]int64),
		Location:          &model.TimeZoneLocation{Name: tzName, Offset: tzOffset},
		ResourceGroupName: ctx.GetSessionVars().StmtCtx.ResourceGroupName,
		Version:           model.CurrentReorgMetaVersion,
	}
}
