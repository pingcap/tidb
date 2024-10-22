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
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	infoschemacontext "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/privilege"
	rg "github.com/pingcap/tidb/pkg/resourcegroup"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/statistics/handle"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/domainutil"
	"github.com/pingcap/tidb/pkg/util/generic"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"github.com/tikv/client-go/v2/oracle"
	pdhttp "github.com/tikv/pd/client/http"
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

// Executor is the interface for executing DDL statements.
// it's mostly called by SQL executor.
// DDL statements are converted into DDL jobs, JobSubmitter will submit the jobs
// to DDL job table. Then jobScheduler will schedule them to run on workers
// asynchronously in parallel. Executor will wait them to finish.
type Executor interface {
	CreateSchema(ctx sessionctx.Context, stmt *ast.CreateDatabaseStmt) error
	AlterSchema(sctx sessionctx.Context, stmt *ast.AlterDatabaseStmt) error
	DropSchema(ctx sessionctx.Context, stmt *ast.DropDatabaseStmt) error
	CreateTable(ctx sessionctx.Context, stmt *ast.CreateTableStmt) error
	CreateView(ctx sessionctx.Context, stmt *ast.CreateViewStmt) error
	DropTable(ctx sessionctx.Context, stmt *ast.DropTableStmt) (err error)
	RecoverTable(ctx sessionctx.Context, recoverTableInfo *model.RecoverTableInfo) (err error)
	RecoverSchema(ctx sessionctx.Context, recoverSchemaInfo *model.RecoverSchemaInfo) error
	DropView(ctx sessionctx.Context, stmt *ast.DropTableStmt) (err error)
	CreateIndex(ctx sessionctx.Context, stmt *ast.CreateIndexStmt) error
	DropIndex(ctx sessionctx.Context, stmt *ast.DropIndexStmt) error
	AlterTable(ctx context.Context, sctx sessionctx.Context, stmt *ast.AlterTableStmt) error
	TruncateTable(ctx sessionctx.Context, tableIdent ast.Ident) error
	RenameTable(ctx sessionctx.Context, stmt *ast.RenameTableStmt) error
	LockTables(ctx sessionctx.Context, stmt *ast.LockTablesStmt) error
	UnlockTables(ctx sessionctx.Context, lockedTables []model.TableLockTpInfo) error
	CleanupTableLock(ctx sessionctx.Context, tables []*ast.TableName) error
	UpdateTableReplicaInfo(ctx sessionctx.Context, physicalID int64, available bool) error
	RepairTable(ctx sessionctx.Context, createStmt *ast.CreateTableStmt) error
	CreateSequence(ctx sessionctx.Context, stmt *ast.CreateSequenceStmt) error
	DropSequence(ctx sessionctx.Context, stmt *ast.DropSequenceStmt) (err error)
	AlterSequence(ctx sessionctx.Context, stmt *ast.AlterSequenceStmt) error
	CreatePlacementPolicy(ctx sessionctx.Context, stmt *ast.CreatePlacementPolicyStmt) error
	DropPlacementPolicy(ctx sessionctx.Context, stmt *ast.DropPlacementPolicyStmt) error
	AlterPlacementPolicy(ctx sessionctx.Context, stmt *ast.AlterPlacementPolicyStmt) error
	AddResourceGroup(ctx sessionctx.Context, stmt *ast.CreateResourceGroupStmt) error
	AlterResourceGroup(ctx sessionctx.Context, stmt *ast.AlterResourceGroupStmt) error
	DropResourceGroup(ctx sessionctx.Context, stmt *ast.DropResourceGroupStmt) error
	FlashbackCluster(ctx sessionctx.Context, flashbackTS uint64) error

	// CreateSchemaWithInfo creates a database (schema) given its database info.
	//
	// WARNING: the DDL owns the `info` after calling this function, and will modify its fields
	// in-place. If you want to keep using `info`, please call Clone() first.
	CreateSchemaWithInfo(
		ctx sessionctx.Context,
		info *model.DBInfo,
		onExist OnExist) error

	// CreateTableWithInfo creates a table, view or sequence given its table info.
	//
	// WARNING: the DDL owns the `info` after calling this function, and will modify its fields
	// in-place. If you want to keep using `info`, please call Clone() first.
	CreateTableWithInfo(
		ctx sessionctx.Context,
		schema pmodel.CIStr,
		info *model.TableInfo,
		involvingRef []model.InvolvingSchemaInfo,
		cs ...CreateTableOption) error

	// BatchCreateTableWithInfo is like CreateTableWithInfo, but can handle multiple tables.
	BatchCreateTableWithInfo(ctx sessionctx.Context,
		schema pmodel.CIStr,
		info []*model.TableInfo,
		cs ...CreateTableOption) error

	// CreatePlacementPolicyWithInfo creates a placement policy
	//
	// WARNING: the DDL owns the `policy` after calling this function, and will modify its fields
	// in-place. If you want to keep using `policy`, please call Clone() first.
	CreatePlacementPolicyWithInfo(ctx sessionctx.Context, policy *model.PolicyInfo, onExist OnExist) error
}

// ExecutorForTest is the interface for executing DDL statements in tests.
// TODO remove it later
type ExecutorForTest interface {
	// DoDDLJob does the DDL job, it's exported for test.
	DoDDLJob(ctx sessionctx.Context, job *model.Job) error
	// DoDDLJobWrapper similar to DoDDLJob, but with JobWrapper as input.
	DoDDLJobWrapper(ctx sessionctx.Context, jobW *JobWrapper) error
}

// all fields are shared with ddl now.
type executor struct {
	sessPool    *sess.Pool
	statsHandle *handle.Handle

	ctx        context.Context
	uuid       string
	store      kv.Storage
	autoidCli  *autoid.ClientDiscover
	infoCache  *infoschema.InfoCache
	limitJobCh chan *JobWrapper
	lease      time.Duration // lease is schema lease, default 45s, see config.Lease.
	// ddlJobDoneChMap is used to notify the session that the DDL job is finished.
	// jobID -> chan struct{}
	ddlJobDoneChMap *generic.SyncMap[int64, chan struct{}]
}

var _ Executor = (*executor)(nil)
var _ ExecutorForTest = (*executor)(nil)

func (e *executor) CreateSchema(ctx sessionctx.Context, stmt *ast.CreateDatabaseStmt) (err error) {
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
				Name: pmodel.NewCIStr(val.Value),
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
		if util.IsMemOrSysDB(db.DBName.L) {
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

	for retry := 0; retry < configRetry; retry++ {
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

	if util.IsMemOrSysDB(dbInfo.Name.L) {
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
			placementPolicyRef = &model.PolicyRefInfo{Name: pmodel.NewCIStr(val.Value)}
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

func checkTooLongSchema(schema pmodel.CIStr) error {
	if utf8.RuneCountInString(schema.L) > mysql.MaxDatabaseNameLength {
		return dbterror.ErrTooLongIdent.GenWithStackByArgs(schema)
	}
	return nil
}

func checkTooLongTable(table pmodel.CIStr) error {
	if utf8.RuneCountInString(table.L) > mysql.MaxTableNameLength {
		return dbterror.ErrTooLongIdent.GenWithStackByArgs(table)
	}
	return nil
}

func checkTooLongIndex(index pmodel.CIStr) error {
	if utf8.RuneCountInString(index.L) > mysql.MaxIndexIdentifierLen {
		return dbterror.ErrTooLongIdent.GenWithStackByArgs(index)
	}
	return nil
}

func checkTooLongColumn(col pmodel.CIStr) error {
	if utf8.RuneCountInString(col.L) > mysql.MaxColumnNameLength {
		return dbterror.ErrTooLongIdent.GenWithStackByArgs(col)
	}
	return nil
}

func checkTooLongForeignKey(fk pmodel.CIStr) error {
	if utf8.RuneCountInString(fk.L) > mysql.MaxForeignKeyIdentifierLen {
		return dbterror.ErrTooLongIdent.GenWithStackByArgs(fk)
	}
	return nil
}

func getDefaultCollationForUTF8MB4(cs string, defaultUTF8MB4Coll string) string {
	if cs == charset.CharsetUTF8MB4 {
		return defaultUTF8MB4Coll
	}
	return ""
}

// GetDefaultCollation returns the default collation for charset and handle the default collation for UTF8MB4.
func GetDefaultCollation(cs string, defaultUTF8MB4Collation string) (string, error) {
	coll := getDefaultCollationForUTF8MB4(cs, defaultUTF8MB4Collation)
	if coll != "" {
		return coll, nil
	}

	coll, err := charset.GetDefaultCollation(cs)
	if err != nil {
		return "", errors.Trace(err)
	}
	return coll, nil
}

// ResolveCharsetCollation will resolve the charset and collate by the order of parameters:
// * If any given ast.CharsetOpt is not empty, the resolved charset and collate will be returned.
// * If all ast.CharsetOpts are empty, the default charset and collate will be returned.
func ResolveCharsetCollation(charsetOpts []ast.CharsetOpt, utf8MB4DefaultColl string) (chs string, coll string, err error) {
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
			coll, err := GetDefaultCollation(v.Chs, utf8MB4DefaultColl)
			if err != nil {
				return "", "", errors.Trace(err)
			}
			return v.Chs, coll, nil
		}
	}
	chs, coll = charset.GetDefaultCharsetAndCollate()
	utf8mb4Coll := getDefaultCollationForUTF8MB4(chs, utf8MB4DefaultColl)
	if utf8mb4Coll != "" {
		return chs, utf8mb4Coll, nil
	}
	return chs, coll, nil
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

// checkGlobalIndex check if the index is allowed to have global index
func checkGlobalIndex(ec errctx.Context, tblInfo *model.TableInfo, indexInfo *model.IndexInfo) error {
	pi := tblInfo.GetPartitionInfo()
	isPartitioned := pi != nil && pi.Type != pmodel.PartitionTypeNone
	if indexInfo.Global {
		if !isPartitioned {
			// Makes no sense with LOCAL/GLOBAL index for non-partitioned tables, since we don't support
			// partitioning an index differently from the table partitioning.
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("Global Index on non-partitioned table")
		}
		// TODO: remove limitation
		if !indexInfo.Unique {
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("GLOBAL IndexOption on non-unique index")
		}
		// TODO: remove limitation
		// check that not all partitioned columns are included.
		inAllPartitionColumns, err := checkPartitionKeysConstraint(pi, indexInfo.Columns, tblInfo)
		if err != nil {
			return errors.Trace(err)
		}
		if inAllPartitionColumns {
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("Global Index including all columns in the partitioning expression")
		}
		validateGlobalIndexWithGeneratedColumns(ec, tblInfo, indexInfo.Name.O, indexInfo.Columns)
	}
	return nil
}

// checkGlobalIndexes check if global index is supported.
func checkGlobalIndexes(ec errctx.Context, tblInfo *model.TableInfo) error {
	for _, indexInfo := range tblInfo.Indices {
		err := checkGlobalIndex(ec, tblInfo, indexInfo)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *executor) CreateTable(ctx sessionctx.Context, s *ast.CreateTableStmt) (err error) {
	ident := ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name}
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}

	var (
		referTbl     table.Table
		involvingRef []model.InvolvingSchemaInfo
	)
	if s.ReferTable != nil {
		referIdent := ast.Ident{Schema: s.ReferTable.Schema, Name: s.ReferTable.Name}
		_, ok := is.SchemaByName(referIdent.Schema)
		if !ok {
			return infoschema.ErrTableNotExists.GenWithStackByArgs(referIdent.Schema, referIdent.Name)
		}
		referTbl, err = is.TableByName(e.ctx, referIdent.Schema, referIdent.Name)
		if err != nil {
			return infoschema.ErrTableNotExists.GenWithStackByArgs(referIdent.Schema, referIdent.Name)
		}
		involvingRef = append(involvingRef, model.InvolvingSchemaInfo{
			Database: s.ReferTable.Schema.L,
			Table:    s.ReferTable.Name.L,
			Mode:     model.SharedInvolving,
		})
	}

	// build tableInfo
	metaBuildCtx := NewMetaBuildContextWithSctx(ctx)
	var tbInfo *model.TableInfo
	if s.ReferTable != nil {
		tbInfo, err = BuildTableInfoWithLike(ident, referTbl.Meta(), s)
	} else {
		tbInfo, err = BuildTableInfoWithStmt(metaBuildCtx, s, schema.Charset, schema.Collate, schema.PlacementPolicyRef)
	}
	if err != nil {
		return errors.Trace(err)
	}

	if err = rewritePartitionQueryString(ctx, s.Partition, tbInfo); err != nil {
		return err
	}

	if err = checkTableInfoValidWithStmt(metaBuildCtx, tbInfo, s); err != nil {
		return err
	}
	if err = checkTableForeignKeysValid(ctx, is, schema.Name.L, tbInfo); err != nil {
		return err
	}

	onExist := OnExistError
	if s.IfNotExists {
		onExist = OnExistIgnore
	}

	return e.CreateTableWithInfo(ctx, schema.Name, tbInfo, involvingRef, WithOnExist(onExist))
}

// createTableWithInfoJob returns the table creation job.
// WARNING: it may return a nil job, which means you don't need to submit any DDL job.
func (e *executor) createTableWithInfoJob(
	ctx sessionctx.Context,
	dbName pmodel.CIStr,
	tbInfo *model.TableInfo,
	involvingRef []model.InvolvingSchemaInfo,
	cfg CreateTableConfig,
) (jobW *JobWrapper, err error) {
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(dbName)
	if !ok {
		return nil, infoschema.ErrDatabaseNotExists.GenWithStackByArgs(dbName)
	}

	if err = handleTablePlacement(ctx, tbInfo); err != nil {
		return nil, errors.Trace(err)
	}

	var oldViewTblID int64
	if oldTable, err := is.TableByName(e.ctx, schema.Name, tbInfo.Name); err == nil {
		err = infoschema.ErrTableExists.GenWithStackByArgs(ast.Ident{Schema: schema.Name, Name: tbInfo.Name})
		switch cfg.OnExist {
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

	if err := checkTableInfoValidExtra(ctx.GetSessionVars().StmtCtx.ErrCtx(), ctx.GetStore(), dbName, tbInfo); err != nil {
		return nil, err
	}

	var actionType model.ActionType
	switch {
	case tbInfo.View != nil:
		actionType = model.ActionCreateView
	case tbInfo.Sequence != nil:
		actionType = model.ActionCreateSequence
	default:
		actionType = model.ActionCreateTable
	}

	var involvingSchemas []model.InvolvingSchemaInfo
	sharedInvolvingFromTableInfo := getSharedInvolvingSchemaInfo(tbInfo)

	if sum := len(involvingRef) + len(sharedInvolvingFromTableInfo); sum > 0 {
		involvingSchemas = make([]model.InvolvingSchemaInfo, 0, sum+1)
		involvingSchemas = append(involvingSchemas, model.InvolvingSchemaInfo{
			Database: schema.Name.L,
			Table:    tbInfo.Name.L,
		})
		involvingSchemas = append(involvingSchemas, involvingRef...)
		involvingSchemas = append(involvingSchemas, sharedInvolvingFromTableInfo...)
	}

	job := &model.Job{
		Version:             model.GetJobVerInUse(),
		SchemaID:            schema.ID,
		SchemaName:          schema.Name.L,
		TableName:           tbInfo.Name.L,
		Type:                actionType,
		BinlogInfo:          &model.HistoryInfo{},
		CDCWriteSource:      ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: involvingSchemas,
		SQLMode:             ctx.GetSessionVars().SQLMode,
	}
	args := &model.CreateTableArgs{
		TableInfo:      tbInfo,
		OnExistReplace: cfg.OnExist == OnExistReplace,
		OldViewTblID:   oldViewTblID,
		FKCheck:        ctx.GetSessionVars().ForeignKeyChecks,
	}
	return NewJobWrapperWithArgs(job, args, cfg.IDAllocated), nil
}

func getSharedInvolvingSchemaInfo(info *model.TableInfo) []model.InvolvingSchemaInfo {
	ret := make([]model.InvolvingSchemaInfo, 0, len(info.ForeignKeys)+1)
	for _, fk := range info.ForeignKeys {
		ret = append(ret, model.InvolvingSchemaInfo{
			Database: fk.RefSchema.L,
			Table:    fk.RefTable.L,
			Mode:     model.SharedInvolving,
		})
	}
	if ref := info.PlacementPolicyRef; ref != nil {
		ret = append(ret, model.InvolvingSchemaInfo{
			Policy: ref.Name.L,
			Mode:   model.SharedInvolving,
		})
	}
	return ret
}

func (e *executor) createTableWithInfoPost(
	ctx sessionctx.Context,
	tbInfo *model.TableInfo,
	schemaID int64,
) error {
	var err error
	var partitions []model.PartitionDefinition
	if pi := tbInfo.GetPartitionInfo(); pi != nil {
		partitions = pi.Definitions
	}
	preSplitAndScatter(ctx, e.store, tbInfo, partitions)
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
		if err = e.handleAutoIncID(tbInfo, schemaID, newEnd, allocType); err != nil {
			return errors.Trace(err)
		}
	}
	// For issue https://github.com/pingcap/tidb/issues/46093
	if tbInfo.AutoIncIDExtra != 0 {
		if err = e.handleAutoIncID(tbInfo, schemaID, tbInfo.AutoIncIDExtra-1, autoid.RowIDAllocType); err != nil {
			return errors.Trace(err)
		}
	}
	if tbInfo.AutoRandID > 1 {
		// Default tableAutoRandID base is 0.
		// If the first ID is expected to greater than 1, we need to do rebase.
		newEnd := tbInfo.AutoRandID - 1
		err = e.handleAutoIncID(tbInfo, schemaID, newEnd, autoid.AutoRandomType)
	}
	return err
}

func (e *executor) CreateTableWithInfo(
	ctx sessionctx.Context,
	dbName pmodel.CIStr,
	tbInfo *model.TableInfo,
	involvingRef []model.InvolvingSchemaInfo,
	cs ...CreateTableOption,
) (err error) {
	c := GetCreateTableConfig(cs)

	jobW, err := e.createTableWithInfoJob(ctx, dbName, tbInfo, involvingRef, c)
	if err != nil {
		return err
	}
	if jobW == nil {
		return nil
	}

	err = e.DoDDLJobWrapper(ctx, jobW)
	if err != nil {
		// table exists, but if_not_exists flags is true, so we ignore this error.
		if c.OnExist == OnExistIgnore && infoschema.ErrTableExists.Equal(err) {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			err = nil
		}
	} else {
		err = e.createTableWithInfoPost(ctx, tbInfo, jobW.SchemaID)
	}

	return errors.Trace(err)
}

func (e *executor) BatchCreateTableWithInfo(ctx sessionctx.Context,
	dbName pmodel.CIStr,
	infos []*model.TableInfo,
	cs ...CreateTableOption,
) error {
	failpoint.Inject("RestoreBatchCreateTableEntryTooLarge", func(val failpoint.Value) {
		injectBatchSize := val.(int)
		if len(infos) > injectBatchSize {
			failpoint.Return(kv.ErrEntryTooLarge)
		}
	})
	c := GetCreateTableConfig(cs)

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	var err error

	// check if there are any duplicated table names
	duplication := make(map[string]struct{})
	// TODO filter those duplicated info out.
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
	}

	args := &model.BatchCreateTableArgs{
		Tables: make([]*model.CreateTableArgs, 0, len(infos)),
	}
	for _, info := range infos {
		jobItem, err := e.createTableWithInfoJob(ctx, dbName, info, nil, c)
		if err != nil {
			return errors.Trace(err)
		}
		if jobItem == nil {
			continue
		}

		// if jobW.Type == model.ActionCreateTables, it is initialized
		// if not, initialize jobW by job.XXXX
		if job.Type != model.ActionCreateTables {
			job.Type = model.ActionCreateTables
			job.SchemaID = jobItem.SchemaID
			job.SchemaName = jobItem.SchemaName
		}

		// append table job args
		args.Tables = append(args.Tables, jobItem.JobArgs.(*model.CreateTableArgs))
		job.InvolvingSchemaInfo = append(job.InvolvingSchemaInfo, model.InvolvingSchemaInfo{
			Database: dbName.L,
			Table:    info.Name.L,
		})
		if sharedInv := getSharedInvolvingSchemaInfo(info); len(sharedInv) > 0 {
			job.InvolvingSchemaInfo = append(job.InvolvingSchemaInfo, sharedInv...)
		}
	}
	if len(args.Tables) == 0 {
		return nil
	}

	jobW := NewJobWrapperWithArgs(job, args, c.IDAllocated)
	err = e.DoDDLJobWrapper(ctx, jobW)
	if err != nil {
		// table exists, but if_not_exists flags is true, so we ignore this error.
		if c.OnExist == OnExistIgnore && infoschema.ErrTableExists.Equal(err) {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			err = nil
		}
		return errors.Trace(err)
	}

	for _, tblArgs := range args.Tables {
		if err = e.createTableWithInfoPost(ctx, tblArgs.TableInfo, jobW.SchemaID); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (e *executor) CreatePlacementPolicyWithInfo(ctx sessionctx.Context, policy *model.PolicyInfo, onExist OnExist) error {
	if checkIgnorePlacementDDL(ctx) {
		return nil
	}

	policyName := policy.Name
	if policyName.L == defaultPlacementPolicyName {
		return errors.Trace(infoschema.ErrReservedSyntax.GenWithStackByArgs(policyName))
	}

	// Check policy existence.
	_, ok := e.infoCache.GetLatest().PolicyByName(policyName)
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

	policyID, err := e.genPlacementPolicyID()
	if err != nil {
		return err
	}
	policy.ID = policyID

	job := &model.Job{
		Version:    model.GetJobVerInUse(),
		SchemaName: policy.Name.L,
		Type:       model.ActionCreatePlacementPolicy,
		BinlogInfo: &model.HistoryInfo{},
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			Policy: policy.Name.L,
		}},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	args := &model.PlacementPolicyArgs{
		Policy:         policy,
		ReplaceOnExist: onExist == OnExistReplace,
	}
	err = e.doDDLJob2(ctx, job, args)
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
		preSplit     func()
		scatterScope string
	)
	val, ok := ctx.GetSessionVars().GetSystemVar(variable.TiDBScatterRegion)
	if !ok {
		logutil.DDLLogger().Warn("get system variable met problem, won't scatter region")
	} else {
		scatterScope = val
	}
	if len(parts) > 0 {
		preSplit = func() { splitPartitionTableRegion(ctx, sp, tbInfo, parts, scatterScope) }
	} else {
		preSplit = func() { splitTableRegion(ctx, sp, tbInfo, scatterScope) }
	}
	if scatterScope != variable.ScatterOff {
		preSplit()
	} else {
		go preSplit()
	}
}

func (e *executor) FlashbackCluster(ctx sessionctx.Context, flashbackTS uint64) error {
	logutil.DDLLogger().Info("get flashback cluster job", zap.Stringer("flashbackTS", oracle.GetTimeFromTS(flashbackTS)))
	nowTS, err := ctx.GetStore().GetOracle().GetTimestamp(e.ctx, &oracle.Option{})
	if err != nil {
		return errors.Trace(err)
	}
	gap := time.Until(oracle.GetTimeFromTS(nowTS)).Abs()
	if gap > 1*time.Second {
		ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("Gap between local time and PD TSO is %s, please check PD/system time", gap))
	}
	job := &model.Job{
		Version:    model.GetJobVerInUse(),
		Type:       model.ActionFlashbackCluster,
		BinlogInfo: &model.HistoryInfo{},
		// The value for global variables is meaningless, it will cover during flashback cluster.
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		// FLASHBACK CLUSTER affects all schemas and tables.
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			Database: model.InvolvingAll,
			Table:    model.InvolvingAll,
		}},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}

	args := &model.FlashbackClusterArgs{
		FlashbackTS:       flashbackTS,
		PDScheduleValue:   map[string]any{},
		EnableGC:          true,
		EnableAutoAnalyze: true,
		EnableTTLJob:      true,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func (e *executor) RecoverTable(ctx sessionctx.Context, recoverTableInfo *model.RecoverTableInfo) (err error) {
	is := e.infoCache.GetLatest()
	schemaID, tbInfo := recoverTableInfo.SchemaID, recoverTableInfo.TableInfo
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

	// for "flashback table xxx to yyy"
	// Note: this case only allow change table name, schema remains the same.
	var involvedSchemas []model.InvolvingSchemaInfo
	if recoverTableInfo.OldTableName != tbInfo.Name.L {
		involvedSchemas = []model.InvolvingSchemaInfo{
			{Database: schema.Name.L, Table: recoverTableInfo.OldTableName},
			{Database: schema.Name.L, Table: tbInfo.Name.L},
		}
	}

	tbInfo.State = model.StateNone
	job := &model.Job{
		Version:             model.GetJobVerInUse(),
		SchemaID:            schemaID,
		TableID:             tbInfo.ID,
		SchemaName:          schema.Name.L,
		TableName:           tbInfo.Name.L,
		Type:                model.ActionRecoverTable,
		BinlogInfo:          &model.HistoryInfo{},
		InvolvingSchemaInfo: involvedSchemas,
		CDCWriteSource:      ctx.GetSessionVars().CDCWriteSource,
		SQLMode:             ctx.GetSessionVars().SQLMode,
	}

	args := &model.RecoverArgs{
		RecoverInfo: &model.RecoverSchemaInfo{
			RecoverTableInfos: []*model.RecoverTableInfo{recoverTableInfo},
		},
		CheckFlag: recoverCheckFlagNone}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func (e *executor) CreateView(ctx sessionctx.Context, s *ast.CreateViewStmt) (err error) {
	viewInfo, err := BuildViewInfo(s)
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

	tbInfo, err := BuildTableInfo(NewMetaBuildContextWithSctx(ctx), s.ViewName.Name, cols, nil, tblCharset, tblCollate)
	if err != nil {
		return err
	}
	tbInfo.View = viewInfo

	onExist := OnExistError
	if s.OrReplace {
		onExist = OnExistReplace
	}

	return e.CreateTableWithInfo(ctx, s.ViewName.Schema, tbInfo, nil, WithOnExist(onExist))
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
func (e *executor) handleAutoIncID(tbInfo *model.TableInfo, schemaID int64, newEnd int64, tp autoid.AllocatorType) error {
	allocs := autoid.NewAllocatorsFromTblInfo(e.getAutoIDRequirement(), schemaID, tbInfo)
	if alloc := allocs.Get(tp); alloc != nil {
		err := alloc.Rebase(context.Background(), newEnd, false)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (e *executor) getAutoIDRequirement() autoid.Requirement {
	return &asAutoIDRequirement{
		store:     e.store,
		autoidCli: e.autoidCli,
	}
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

// GetCharsetAndCollateInTableOption will iterate the charset and collate in the options,
// and returns the last charset and collate in options. If there is no charset in the options,
// the returns charset will be "", the same as collate.
func GetCharsetAndCollateInTableOption(startIdx int, options []*ast.TableOption, defaultUTF8MB4Coll string) (chs, coll string, err error) {
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
				defaultColl := getDefaultCollationForUTF8MB4(chs, defaultUTF8MB4Coll)
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

func (e *executor) AlterTable(ctx context.Context, sctx sessionctx.Context, stmt *ast.AlterTableStmt) (err error) {
	ident := ast.Ident{Schema: stmt.Table.Schema, Name: stmt.Table.Name}
	validSpecs, err := ResolveAlterTableSpec(sctx, stmt.Specs)
	if err != nil {
		return errors.Trace(err)
	}

	is := e.infoCache.GetLatest()
	tb, err := is.TableByName(ctx, ident.Schema, ident.Name)
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
		// after MultiSchemaInfo is set, DoDDLJob will collect all jobs into
		// MultiSchemaInfo and skip running them. Then we will run them in
		// d.multiSchemaChange all at once.
		sctx.GetSessionVars().StmtCtx.MultiSchemaInfo = model.NewMultiSchemaInfo()
	}
	for _, spec := range validSpecs {
		var handledCharsetOrCollate bool
		var ttlOptionsHandled bool
		switch spec.Tp {
		case ast.AlterTableAddColumns:
			err = e.AddColumn(sctx, ident, spec)
		case ast.AlterTableAddPartitions, ast.AlterTableAddLastPartition:
			err = e.AddTablePartitions(sctx, ident, spec)
		case ast.AlterTableCoalescePartitions:
			err = e.CoalescePartitions(sctx, ident, spec)
		case ast.AlterTableReorganizePartition:
			err = e.ReorganizePartitions(sctx, ident, spec)
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
			err = e.RemovePartitioning(sctx, ident, spec)
		case ast.AlterTableRepairPartition:
			err = errors.Trace(dbterror.ErrUnsupportedRepairPartition)
		case ast.AlterTableDropColumn:
			err = e.DropColumn(sctx, ident, spec)
		case ast.AlterTableDropIndex:
			err = e.dropIndex(sctx, ident, pmodel.NewCIStr(spec.Name), spec.IfExists, false)
		case ast.AlterTableDropPrimaryKey:
			err = e.dropIndex(sctx, ident, pmodel.NewCIStr(mysql.PrimaryKeyName), spec.IfExists, false)
		case ast.AlterTableRenameIndex:
			err = e.RenameIndex(sctx, ident, spec)
		case ast.AlterTableDropPartition, ast.AlterTableDropFirstPartition:
			err = e.DropTablePartition(sctx, ident, spec)
		case ast.AlterTableTruncatePartition:
			err = e.TruncateTablePartition(sctx, ident, spec)
		case ast.AlterTableWriteable:
			if !config.TableLockEnabled() {
				return nil
			}
			tName := &ast.TableName{Schema: ident.Schema, Name: ident.Name}
			if spec.Writeable {
				err = e.CleanupTableLock(sctx, []*ast.TableName{tName})
			} else {
				lockStmt := &ast.LockTablesStmt{
					TableLocks: []ast.TableLock{
						{
							Table: tName,
							Type:  pmodel.TableLockReadOnly,
						},
					},
				}
				err = e.LockTables(sctx, lockStmt)
			}
		case ast.AlterTableExchangePartition:
			err = e.ExchangeTablePartition(sctx, ident, spec)
		case ast.AlterTableAddConstraint:
			constr := spec.Constraint
			switch spec.Constraint.Tp {
			case ast.ConstraintKey, ast.ConstraintIndex:
				err = e.createIndex(sctx, ident, ast.IndexKeyTypeNone, pmodel.NewCIStr(constr.Name),
					spec.Constraint.Keys, constr.Option, constr.IfNotExists)
			case ast.ConstraintUniq, ast.ConstraintUniqIndex, ast.ConstraintUniqKey:
				err = e.createIndex(sctx, ident, ast.IndexKeyTypeUnique, pmodel.NewCIStr(constr.Name),
					spec.Constraint.Keys, constr.Option, false) // IfNotExists should be not applied
			case ast.ConstraintForeignKey:
				// NOTE: we do not handle `symbol` and `index_name` well in the parser and we do not check ForeignKey already exists,
				// so we just also ignore the `if not exists` check.
				err = e.CreateForeignKey(sctx, ident, pmodel.NewCIStr(constr.Name), spec.Constraint.Keys, spec.Constraint.Refer)
			case ast.ConstraintPrimaryKey:
				err = e.CreatePrimaryKey(sctx, ident, pmodel.NewCIStr(constr.Name), spec.Constraint.Keys, constr.Option)
			case ast.ConstraintFulltext:
				sctx.GetSessionVars().StmtCtx.AppendWarning(dbterror.ErrTableCantHandleFt)
			case ast.ConstraintCheck:
				if !variable.EnableCheckConstraint.Load() {
					sctx.GetSessionVars().StmtCtx.AppendWarning(errCheckConstraintIsOff)
				} else {
					err = e.CreateCheckConstraint(sctx, ident, pmodel.NewCIStr(constr.Name), spec.Constraint)
				}
			case ast.ConstraintVector:
				err = e.createVectorIndex(sctx, ident, pmodel.NewCIStr(constr.Name), spec.Constraint.Keys, constr.Option, constr.IfNotExists)
			default:
				// Nothing to do now.
			}
		case ast.AlterTableDropForeignKey:
			// NOTE: we do not check `if not exists` and `if exists` for ForeignKey now.
			err = e.DropForeignKey(sctx, ident, pmodel.NewCIStr(spec.Name))
		case ast.AlterTableModifyColumn:
			err = e.ModifyColumn(ctx, sctx, ident, spec)
		case ast.AlterTableChangeColumn:
			err = e.ChangeColumn(ctx, sctx, ident, spec)
		case ast.AlterTableRenameColumn:
			err = e.RenameColumn(sctx, ident, spec)
		case ast.AlterTableAlterColumn:
			err = e.AlterColumn(sctx, ident, spec)
		case ast.AlterTableRenameTable:
			newIdent := ast.Ident{Schema: spec.NewTable.Schema, Name: spec.NewTable.Name}
			isAlterTable := true
			err = e.renameTable(sctx, ident, newIdent, isAlterTable)
		case ast.AlterTablePartition:
			err = e.AlterTablePartitioning(sctx, ident, spec)
		case ast.AlterTableOption:
			var placementPolicyRef *model.PolicyRefInfo
			for i, opt := range spec.Options {
				switch opt.Tp {
				case ast.TableOptionShardRowID:
					if opt.UintValue > variable.MaxShardRowIDBits {
						opt.UintValue = variable.MaxShardRowIDBits
					}
					err = e.ShardRowID(sctx, ident, opt.UintValue)
				case ast.TableOptionAutoIncrement:
					err = e.RebaseAutoID(sctx, ident, int64(opt.UintValue), autoid.AutoIncrementType, opt.BoolValue)
				case ast.TableOptionAutoIdCache:
					if opt.UintValue > uint64(math.MaxInt64) {
						// TODO: Refine this error.
						return errors.New("table option auto_id_cache overflows int64")
					}
					err = e.AlterTableAutoIDCache(sctx, ident, int64(opt.UintValue))
				case ast.TableOptionAutoRandomBase:
					err = e.RebaseAutoID(sctx, ident, int64(opt.UintValue), autoid.AutoRandomType, opt.BoolValue)
				case ast.TableOptionComment:
					spec.Comment = opt.StrValue
					err = e.AlterTableComment(sctx, ident, spec)
				case ast.TableOptionCharset, ast.TableOptionCollate:
					// GetCharsetAndCollateInTableOption will get the last charset and collate in the options,
					// so it should be handled only once.
					if handledCharsetOrCollate {
						continue
					}
					var toCharset, toCollate string
					toCharset, toCollate, err = GetCharsetAndCollateInTableOption(i, spec.Options, sctx.GetSessionVars().DefaultCollationForUTF8MB4)
					if err != nil {
						return err
					}
					needsOverwriteCols := NeedToOverwriteColCharset(spec.Options)
					err = e.AlterTableCharsetAndCollate(sctx, ident, toCharset, toCollate, needsOverwriteCols)
					handledCharsetOrCollate = true
				case ast.TableOptionPlacementPolicy:
					placementPolicyRef = &model.PolicyRefInfo{
						Name: pmodel.NewCIStr(opt.StrValue),
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
					err = e.AlterTableTTLInfoOrEnable(sctx, ident, ttlInfo, ttlEnable, ttlJobInterval)

					ttlOptionsHandled = true
				default:
					err = dbterror.ErrUnsupportedAlterTableOption
				}

				if err != nil {
					return errors.Trace(err)
				}
			}

			if placementPolicyRef != nil {
				err = e.AlterTablePlacement(sctx, ident, placementPolicyRef)
			}
		case ast.AlterTableSetTiFlashReplica:
			err = e.AlterTableSetTiFlashReplica(sctx, ident, spec.TiFlashReplica)
		case ast.AlterTableOrderByColumns:
			err = e.OrderByColumns(sctx, ident)
		case ast.AlterTableIndexInvisible:
			err = e.AlterIndexVisibility(sctx, ident, spec.IndexName, spec.Visibility)
		case ast.AlterTableAlterCheck:
			if !variable.EnableCheckConstraint.Load() {
				sctx.GetSessionVars().StmtCtx.AppendWarning(errCheckConstraintIsOff)
			} else {
				err = e.AlterCheckConstraint(sctx, ident, pmodel.NewCIStr(spec.Constraint.Name), spec.Constraint.Enforced)
			}
		case ast.AlterTableDropCheck:
			if !variable.EnableCheckConstraint.Load() {
				sctx.GetSessionVars().StmtCtx.AppendWarning(errCheckConstraintIsOff)
			} else {
				err = e.DropCheckConstraint(sctx, ident, pmodel.NewCIStr(spec.Constraint.Name))
			}
		case ast.AlterTableWithValidation:
			sctx.GetSessionVars().StmtCtx.AppendWarning(dbterror.ErrUnsupportedAlterTableWithValidation)
		case ast.AlterTableWithoutValidation:
			sctx.GetSessionVars().StmtCtx.AppendWarning(dbterror.ErrUnsupportedAlterTableWithoutValidation)
		case ast.AlterTableAddStatistics:
			err = e.AlterTableAddStatistics(sctx, ident, spec.Statistics, spec.IfNotExists)
		case ast.AlterTableDropStatistics:
			err = e.AlterTableDropStatistics(sctx, ident, spec.Statistics, spec.IfExists)
		case ast.AlterTableAttributes:
			err = e.AlterTableAttributes(sctx, ident, spec)
		case ast.AlterTablePartitionAttributes:
			err = e.AlterTablePartitionAttributes(sctx, ident, spec)
		case ast.AlterTablePartitionOptions:
			err = e.AlterTablePartitionOptions(sctx, ident, spec)
		case ast.AlterTableCache:
			err = e.AlterTableCache(sctx, ident)
		case ast.AlterTableNoCache:
			err = e.AlterTableNoCache(sctx, ident)
		case ast.AlterTableDisableKeys, ast.AlterTableEnableKeys:
			// Nothing to do now, see https://github.com/pingcap/tidb/issues/1051
			// MyISAM specific
		case ast.AlterTableRemoveTTL:
			// the parser makes sure we have only one `ast.AlterTableRemoveTTL` in an alter statement
			err = e.AlterTableRemoveTTL(sctx, ident)
		default:
			err = errors.Trace(dbterror.ErrUnsupportedAlterTableSpec)
		}

		if err != nil {
			return errors.Trace(err)
		}
	}

	if sctx.GetSessionVars().StmtCtx.MultiSchemaInfo != nil {
		info := sctx.GetSessionVars().StmtCtx.MultiSchemaInfo
		sctx.GetSessionVars().StmtCtx.MultiSchemaInfo = nil
		err = e.multiSchemaChange(sctx, ident, info)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (e *executor) multiSchemaChange(ctx sessionctx.Context, ti ast.Ident, info *model.MultiSchemaInfo) error {
	subJobs := info.SubJobs
	if len(subJobs) == 0 {
		return nil
	}
	schema, t, err := e.getSchemaAndTableByIdent(ti)
	if err != nil {
		return errors.Trace(err)
	}

	var involvingSchemaInfo []model.InvolvingSchemaInfo
	for _, j := range subJobs {
		if j.Type == model.ActionAddForeignKey {
			ref := j.JobArgs.(*model.AddForeignKeyArgs).FkInfo
			involvingSchemaInfo = append(involvingSchemaInfo, model.InvolvingSchemaInfo{
				Database: ref.RefSchema.L,
				Table:    ref.RefTable.L,
				Mode:     model.SharedInvolving,
			})
		}
	}

	if len(involvingSchemaInfo) > 0 {
		involvingSchemaInfo = append(involvingSchemaInfo, model.InvolvingSchemaInfo{
			Database: schema.Name.L,
			Table:    t.Meta().Name.L,
		})
	}

	job := &model.Job{
		Version:             model.GetJobVerInUse(),
		SchemaID:            schema.ID,
		TableID:             t.Meta().ID,
		SchemaName:          schema.Name.L,
		TableName:           t.Meta().Name.L,
		Type:                model.ActionMultiSchemaChange,
		BinlogInfo:          &model.HistoryInfo{},
		MultiSchemaInfo:     info,
		ReorgMeta:           nil,
		CDCWriteSource:      ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: involvingSchemaInfo,
		SQLMode:             ctx.GetSessionVars().SQLMode,
	}
	if containsDistTaskSubJob(subJobs) {
		job.ReorgMeta, err = newReorgMetaFromVariables(job, ctx)
		if err != nil {
			return err
		}
	} else {
		job.ReorgMeta = NewDDLReorgMeta(ctx)
	}

	err = checkMultiSchemaInfo(info, t)
	if err != nil {
		return errors.Trace(err)
	}
	mergeAddIndex(info)
	return e.DoDDLJob(ctx, job)
}

func containsDistTaskSubJob(subJobs []*model.SubJob) bool {
	for _, sub := range subJobs {
		if sub.Type == model.ActionAddIndex ||
			sub.Type == model.ActionAddPrimaryKey {
			return true
		}
	}
	return false
}

func (e *executor) RebaseAutoID(ctx sessionctx.Context, ident ast.Ident, newBase int64, tp autoid.AllocatorType, force bool) error {
	schema, t, err := e.getSchemaAndTableByIdent(ident)
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
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        tbInfo.ID,
		SchemaName:     schema.Name.L,
		TableName:      tbInfo.Name.L,
		Type:           actionType,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	args := &model.RebaseAutoIDArgs{
		NewBase: newBase,
		Force:   force,
	}
	err = e.doDDLJob2(ctx, job, args)
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
func (e *executor) ShardRowID(ctx sessionctx.Context, tableIdent ast.Ident, uVal uint64) error {
	schema, t, err := e.getSchemaAndTableByIdent(tableIdent)
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
	err = verifyNoOverflowShardBits(e.sessPool, t, uVal)
	if err != nil {
		return err
	}
	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		Type:           model.ActionShardRowID,
		SchemaID:       schema.ID,
		TableID:        tbInfo.ID,
		SchemaName:     schema.Name.L,
		TableName:      tbInfo.Name.L,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	args := &model.ShardRowIDArgs{ShardRowIDBits: uVal}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func (e *executor) getSchemaAndTableByIdent(tableIdent ast.Ident) (dbInfo *model.DBInfo, t table.Table, err error) {
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(tableIdent.Schema)
	if !ok {
		return nil, nil, infoschema.ErrDatabaseNotExists.GenWithStackByArgs(tableIdent.Schema)
	}
	t, err = is.TableByName(e.ctx, tableIdent.Schema, tableIdent.Name)
	if err != nil {
		return nil, nil, infoschema.ErrTableNotExists.GenWithStackByArgs(tableIdent.Schema, tableIdent.Name)
	}
	return schema, t, nil
}

// AddColumn will add a new column to the table.
func (e *executor) AddColumn(ctx sessionctx.Context, ti ast.Ident, spec *ast.AlterTableSpec) error {
	specNewColumn := spec.NewColumns[0]
	schema, t, err := e.getSchemaAndTableByIdent(ti)
	if err != nil {
		return errors.Trace(err)
	}
	failpoint.InjectCall("afterGetSchemaAndTableByIdent", ctx)
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
	bdrRole, err := meta.NewMutator(txn).GetBDRRole()
	if err != nil {
		return errors.Trace(err)
	}
	if bdrRole == string(ast.BDRRolePrimary) && deniedByBDRWhenAddColumn(specNewColumn.Options) {
		return dbterror.ErrBDRRestrictedDDL.FastGenByArgs(bdrRole)
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        tbInfo.ID,
		SchemaName:     schema.Name.L,
		TableName:      tbInfo.Name.L,
		Type:           model.ActionAddColumn,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	args := &model.TableColumnArgs{
		Col:                col.ColumnInfo,
		Pos:                spec.Position,
		IgnoreExistenceErr: spec.IfNotExists,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

// AddTablePartitions will add a new partition to the table.
func (e *executor) AddTablePartitions(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schema))
	}
	t, err := is.TableByName(e.ctx, ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}

	meta := t.Meta()
	pi := meta.GetPartitionInfo()
	if pi == nil {
		return errors.Trace(dbterror.ErrPartitionMgmtOnNonpartitioned)
	}
	if pi.Type == pmodel.PartitionTypeHash || pi.Type == pmodel.PartitionTypeKey {
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
		return e.hashPartitionManagement(ctx, ident, spec, pi)
	}

	partInfo, err := BuildAddedPartitionInfo(ctx.GetExprCtx(), meta, spec)
	if err != nil {
		return errors.Trace(err)
	}
	if pi.Type == pmodel.PartitionTypeList {
		// TODO: make sure that checks in ddl_api and ddl_worker is the same.
		if meta.Partition.GetDefaultListPartition() != -1 {
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("ADD List partition, already contains DEFAULT partition. Please use REORGANIZE PARTITION instead")
		}
	}

	// partInfo contains only the new added partition, we have to combine it with the
	// old partitions to check all partitions is strictly increasing.
	clonedMeta := meta.Clone()
	tmp := *partInfo
	tmp.Definitions = append(pi.Definitions, tmp.Definitions...)
	clonedMeta.Partition = &tmp
	if err := checkPartitionDefinitionConstraints(ctx.GetExprCtx(), clonedMeta); err != nil {
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
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        meta.ID,
		SchemaName:     schema.Name.L,
		TableName:      t.Meta().Name.L,
		Type:           model.ActionAddTablePartition,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	args := &model.TablePartitionArgs{
		PartInfo: partInfo,
	}

	if spec.Tp == ast.AlterTableAddLastPartition && spec.Partition != nil {
		query, ok := ctx.Value(sessionctx.QueryString).(string)
		if ok {
			sqlMode := ctx.GetSessionVars().SQLMode
			var buf bytes.Buffer
			AppendPartitionDefs(partInfo, &buf, sqlMode)

			syntacticSugar := spec.Partition.PartitionMethod.OriginalText()
			syntacticStart := strings.Index(query, syntacticSugar)
			if syntacticStart == -1 {
				logutil.DDLLogger().Error("Can't find related PARTITION definition in prepare stmt",
					zap.String("PARTITION definition", syntacticSugar), zap.String("prepare stmt", query))
				return errors.Errorf("Can't find related PARTITION definition in PREPARE STMT")
			}
			newQuery := query[:syntacticStart] + "ADD PARTITION (" + buf.String() + ")" + query[syntacticStart+len(syntacticSugar):]
			defer ctx.SetValue(sessionctx.QueryString, query)
			ctx.SetValue(sessionctx.QueryString, newQuery)
		}
	}
	err = e.doDDLJob2(ctx, job, args)
	if dbterror.ErrSameNamePartition.Equal(err) && spec.IfNotExists {
		ctx.GetSessionVars().StmtCtx.AppendNote(err)
		return nil
	}
	return errors.Trace(err)
}

// getReorganizedDefinitions return the definitions as they would look like after the REORGANIZE PARTITION is done.
func getReorganizedDefinitions(pi *model.PartitionInfo, firstPartIdx, lastPartIdx int, idMap map[int]struct{}) []model.PartitionDefinition {
	tmpDefs := make([]model.PartitionDefinition, 0, len(pi.Definitions)+len(pi.AddingDefinitions)-len(idMap))
	if pi.Type == pmodel.PartitionTypeList {
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
	case pmodel.PartitionTypeRange:
		if len(idMap) != (lastPartIdx - firstPartIdx + 1) {
			return 0, 0, nil, errors.Trace(dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs(
				"REORGANIZE PARTITION of RANGE; not adjacent partitions"))
		}
	case pmodel.PartitionTypeHash, pmodel.PartitionTypeKey:
		if len(idMap) != len(pi.Definitions) {
			return 0, 0, nil, errors.Trace(dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs(
				"REORGANIZE PARTITION of HASH/RANGE; must reorganize all partitions"))
		}
	}

	return firstPartIdx, lastPartIdx, idMap, nil
}

func getPartitionInfoTypeNone() *model.PartitionInfo {
	return &model.PartitionInfo{
		Type:   pmodel.PartitionTypeNone,
		Enable: true,
		Definitions: []model.PartitionDefinition{{
			Name:    pmodel.NewCIStr("pFullTable"),
			Comment: "Intermediate partition during ALTER TABLE ... PARTITION BY ...",
		}},
		Num: 1,
	}
}

// AlterTablePartitioning reorganize one set of partitions to a new set of partitions.
func (e *executor) AlterTablePartitioning(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	schema, t, err := e.getSchemaAndTableByIdent(ident)
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

	err = buildTablePartitionInfo(NewMetaBuildContextWithSctx(ctx), spec.Partition, newMeta)
	if err != nil {
		return err
	}

	newPartInfo := newMeta.Partition
	if err = rewritePartitionQueryString(ctx, spec.Partition, newMeta); err != nil {
		return errors.Trace(err)
	}

	if err = handlePartitionPlacement(ctx, newPartInfo); err != nil {
		return errors.Trace(err)
	}

	newPartInfo.DDLType = piOld.Type

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        meta.ID,
		SchemaName:     schema.Name.L,
		TableName:      t.Meta().Name.L,
		Type:           model.ActionAlterTablePartitioning,
		BinlogInfo:     &model.HistoryInfo{},
		ReorgMeta:      NewDDLReorgMeta(ctx),
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	args := &model.TablePartitionArgs{
		PartNames: partNames,
		PartInfo:  newPartInfo,
	}
	// No preSplitAndScatter here, it will be done by the worker in onReorganizePartition instead.
	err = e.doDDLJob2(ctx, job, args)
	if err == nil {
		ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError("The statistics of new partitions will be outdated after reorganizing partitions. Please use 'ANALYZE TABLE' statement if you want to update it now"))
	}
	return errors.Trace(err)
}

// ReorganizePartitions reorganize one set of partitions to a new set of partitions.
func (e *executor) ReorganizePartitions(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	schema, t, err := e.getSchemaAndTableByIdent(ident)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.FastGenByArgs(ident.Schema, ident.Name))
	}

	meta := t.Meta()
	pi := meta.GetPartitionInfo()
	if pi == nil {
		return dbterror.ErrPartitionMgmtOnNonpartitioned
	}
	switch pi.Type {
	case pmodel.PartitionTypeRange, pmodel.PartitionTypeList:
	case pmodel.PartitionTypeHash, pmodel.PartitionTypeKey:
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
	if err = checkReorgPartitionDefs(ctx, model.ActionReorganizePartition, meta, partInfo, firstPartIdx, lastPartIdx, idMap); err != nil {
		return errors.Trace(err)
	}
	if err = handlePartitionPlacement(ctx, partInfo); err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        meta.ID,
		SchemaName:     schema.Name.L,
		TableName:      t.Meta().Name.L,
		Type:           model.ActionReorganizePartition,
		BinlogInfo:     &model.HistoryInfo{},
		ReorgMeta:      NewDDLReorgMeta(ctx),
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	args := &model.TablePartitionArgs{
		PartNames: partNames,
		PartInfo:  partInfo,
	}

	// No preSplitAndScatter here, it will be done by the worker in onReorganizePartition instead.
	err = e.doDDLJob2(ctx, job, args)
	failpoint.InjectCall("afterReorganizePartition")
	if err == nil {
		ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError("The statistics of related partitions will be outdated after reorganizing partitions. Please use 'ANALYZE TABLE' statement if you want to update it now"))
	}
	return errors.Trace(err)
}

// RemovePartitioning removes partitioning from a table.
func (e *executor) RemovePartitioning(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	schema, t, err := e.getSchemaAndTableByIdent(ident)
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
	defs[0].Name = pmodel.NewCIStr("CollapsedPartitions")
	newSpec.PartDefinitions = defs
	partNames := make([]string, len(pi.Definitions))
	for i := range pi.Definitions {
		partNames[i] = pi.Definitions[i].Name.L
	}
	meta.Partition.Type = pmodel.PartitionTypeNone
	partInfo, err := BuildAddedPartitionInfo(ctx.GetExprCtx(), meta, newSpec)
	if err != nil {
		return errors.Trace(err)
	}
	// TODO: check where the default placement comes from (i.e. table level)
	if err = handlePartitionPlacement(ctx, partInfo); err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        meta.ID,
		SchemaName:     schema.Name.L,
		TableName:      meta.Name.L,
		Type:           model.ActionRemovePartitioning,
		BinlogInfo:     &model.HistoryInfo{},
		ReorgMeta:      NewDDLReorgMeta(ctx),
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	args := &model.TablePartitionArgs{
		PartNames: partNames,
		PartInfo:  partInfo,
	}

	// No preSplitAndScatter here, it will be done by the worker in onReorganizePartition instead.
	err = e.doDDLJob2(ctx, job, args)
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
	if err := checkPartitionDefinitionConstraints(ctx.GetExprCtx(), clonedMeta); err != nil {
		return errors.Trace(err)
	}
	if action == model.ActionReorganizePartition {
		if pi.Type == pmodel.PartitionTypeRange {
			if lastPartIdx == len(pi.Definitions)-1 {
				// Last partition dropped, OK to change the end range
				// Also includes MAXVALUE
				return nil
			}
			// Check if the replaced end range is the same as before
			lastAddingPartition := partInfo.Definitions[len(partInfo.Definitions)-1]
			lastOldPartition := pi.Definitions[lastPartIdx]
			if len(pi.Columns) > 0 {
				newGtOld, err := checkTwoRangeColumns(ctx.GetExprCtx(), &lastAddingPartition, &lastOldPartition, pi, tblInfo)
				if err != nil {
					return errors.Trace(err)
				}
				if newGtOld {
					return errors.Trace(dbterror.ErrRangeNotIncreasing)
				}
				oldGtNew, err := checkTwoRangeColumns(ctx.GetExprCtx(), &lastOldPartition, &lastAddingPartition, pi, tblInfo)
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
func (e *executor) CoalescePartitions(sctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schema))
	}
	t, err := is.TableByName(e.ctx, ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}

	pi := t.Meta().GetPartitionInfo()
	if pi == nil {
		return errors.Trace(dbterror.ErrPartitionMgmtOnNonpartitioned)
	}

	switch pi.Type {
	case pmodel.PartitionTypeHash, pmodel.PartitionTypeKey:
		return e.hashPartitionManagement(sctx, ident, spec, pi)

	// Coalesce partition can only be used on hash/key partitions.
	default:
		return errors.Trace(dbterror.ErrCoalesceOnlyOnHashPartition)
	}
}

func (e *executor) hashPartitionManagement(sctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec, pi *model.PartitionInfo) error {
	newSpec := *spec
	newSpec.PartitionNames = make([]pmodel.CIStr, len(pi.Definitions))
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

	return e.ReorganizePartitions(sctx, ident, &newSpec)
}

func (e *executor) TruncateTablePartition(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schema))
	}
	t, err := is.TableByName(e.ctx, ident.Schema, ident.Name)
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

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        meta.ID,
		SchemaName:     schema.Name.L,
		SchemaState:    model.StatePublic,
		TableName:      t.Meta().Name.L,
		Type:           model.ActionTruncateTablePartition,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	args := &model.TruncateTableArgs{
		OldPartitionIDs: pids,
		// job submitter will fill new partition IDs.
	}

	err = e.doDDLJob2(ctx, job, args)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (e *executor) DropTablePartition(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schema))
	}
	t, err := is.TableByName(e.ctx, ident.Schema, ident.Name)
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
			syntacticStart := strings.Index(query, syntacticSugar)
			if syntacticStart == -1 {
				logutil.DDLLogger().Error("Can't find related PARTITION definition in prepare stmt",
					zap.String("PARTITION definition", syntacticSugar), zap.String("prepare stmt", query))
				return errors.Errorf("Can't find related PARTITION definition in PREPARE STMT")
			}
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
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        meta.ID,
		SchemaName:     schema.Name.L,
		SchemaState:    model.StatePublic,
		TableName:      meta.Name.L,
		Type:           model.ActionDropTablePartition,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	args := &model.TablePartitionArgs{
		PartNames: partNames,
	}

	err = e.doDDLJob2(ctx, job, args)
	if err != nil {
		if dbterror.ErrDropPartitionNonExistent.Equal(err) && spec.IfExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return errors.Trace(err)
	}
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

func (e *executor) ExchangeTablePartition(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	ptSchema, pt, err := e.getSchemaAndTableByIdent(ident)
	if err != nil {
		return errors.Trace(err)
	}

	ptMeta := pt.Meta()

	ntIdent := ast.Ident{Schema: spec.NewTable.Schema, Name: spec.NewTable.Name}

	// We should check local temporary here using session's info schema because the local temporary tables are only stored in session.
	ntLocalTempTable, err := sessiontxn.GetTxnManager(ctx).GetTxnInfoSchema().TableByName(context.Background(), ntIdent.Schema, ntIdent.Name)
	if err == nil && ntLocalTempTable.Meta().TempTableType == model.TempTableLocal {
		return errors.Trace(dbterror.ErrPartitionExchangeTempTable.FastGenByArgs(ntLocalTempTable.Meta().Name))
	}

	ntSchema, nt, err := e.getSchemaAndTableByIdent(ntIdent)
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
		Version:        model.GetJobVerInUse(),
		SchemaID:       ntSchema.ID,
		TableID:        ntMeta.ID,
		SchemaName:     ntSchema.Name.L,
		TableName:      ntMeta.Name.L,
		Type:           model.ActionExchangeTablePartition,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{
			{Database: ptSchema.Name.L, Table: ptMeta.Name.L},
			{Database: ntSchema.Name.L, Table: ntMeta.Name.L},
		},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}
	args := &model.ExchangeTablePartitionArgs{
		PartitionID:    defID,
		PTSchemaID:     ptSchema.ID,
		PTTableID:      ptMeta.ID,
		PartitionName:  partName,
		WithValidation: spec.WithValidation,
	}

	err = e.doDDLJob2(ctx, job, args)
	if err != nil {
		return errors.Trace(err)
	}
	ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError("after the exchange, please analyze related table of the exchange to update statistics"))
	return nil
}

// DropColumn will drop a column from the table, now we don't support drop the column with index covered.
func (e *executor) DropColumn(ctx sessionctx.Context, ti ast.Ident, spec *ast.AlterTableSpec) error {
	schema, t, err := e.getSchemaAndTableByIdent(ti)
	if err != nil {
		return errors.Trace(err)
	}
	failpoint.InjectCall("afterGetSchemaAndTableByIdent", ctx)

	isDropable, err := checkIsDroppableColumn(ctx, e.infoCache.GetLatest(), schema, t, spec)
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
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        t.Meta().ID,
		SchemaName:     schema.Name.L,
		SchemaState:    model.StatePublic,
		TableName:      t.Meta().Name.L,
		Type:           model.ActionDropColumn,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	args := &model.TableColumnArgs{
		Col:                &model.ColumnInfo{Name: colName},
		IgnoreExistenceErr: spec.IfExists,
	}
	err = e.doDDLJob2(ctx, job, args)
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
func checkDropColumnWithPartitionConstraint(t table.Table, colName pmodel.CIStr) error {
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

func (e *executor) getModifiableColumnJob(
	ctx context.Context, sctx sessionctx.Context,
	ident ast.Ident, originalColName pmodel.CIStr, spec *ast.AlterTableSpec,
) (*JobWrapper, error) {
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return nil, errors.Trace(infoschema.ErrDatabaseNotExists)
	}
	t, err := is.TableByName(ctx, ident.Schema, ident.Name)
	if err != nil {
		return nil, errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}

	return GetModifiableColumnJob(ctx, sctx, is, ident, originalColName, schema, t, spec)
}

// ChangeColumn renames an existing column and modifies the column's definition,
// currently we only support limited kind of changes
// that do not need to change or check data on the table.
func (e *executor) ChangeColumn(ctx context.Context, sctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
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

	jobW, err := e.getModifiableColumnJob(ctx, sctx, ident, spec.OldColumnName.Name, spec)
	if err != nil {
		if infoschema.ErrColumnNotExists.Equal(err) && spec.IfExists {
			sctx.GetSessionVars().StmtCtx.AppendNote(infoschema.ErrColumnNotExists.FastGenByArgs(spec.OldColumnName.Name, ident.Name))
			return nil
		}
		return errors.Trace(err)
	}

	err = e.DoDDLJobWrapper(sctx, jobW)
	// column not exists, but if_exists flags is true, so we ignore this error.
	if infoschema.ErrColumnNotExists.Equal(err) && spec.IfExists {
		sctx.GetSessionVars().StmtCtx.AppendNote(err)
		return nil
	}
	return errors.Trace(err)
}

// RenameColumn renames an existing column.
func (e *executor) RenameColumn(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	oldColName := spec.OldColumnName.Name
	newColName := spec.NewColumnName.Name

	schema, tbl, err := e.getSchemaAndTableByIdent(ident)
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
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        tbl.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      tbl.Meta().Name.L,
		Type:           model.ActionModifyColumn,
		BinlogInfo:     &model.HistoryInfo{},
		ReorgMeta:      NewDDLReorgMeta(ctx),
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	args := &model.ModifyColumnArgs{
		Column:        newCol,
		OldColumnName: oldColName,
		Position:      spec.Position,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

// ModifyColumn does modification on an existing column, currently we only support limited kind of changes
// that do not need to change or check data on the table.
func (e *executor) ModifyColumn(ctx context.Context, sctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	specNewColumn := spec.NewColumns[0]
	if len(specNewColumn.Name.Schema.O) != 0 && ident.Schema.L != specNewColumn.Name.Schema.L {
		return dbterror.ErrWrongDBName.GenWithStackByArgs(specNewColumn.Name.Schema.O)
	}
	if len(specNewColumn.Name.Table.O) != 0 && ident.Name.L != specNewColumn.Name.Table.L {
		return dbterror.ErrWrongTableName.GenWithStackByArgs(specNewColumn.Name.Table.O)
	}

	originalColName := specNewColumn.Name.Name
	jobW, err := e.getModifiableColumnJob(ctx, sctx, ident, originalColName, spec)
	if err != nil {
		if infoschema.ErrColumnNotExists.Equal(err) && spec.IfExists {
			sctx.GetSessionVars().StmtCtx.AppendNote(infoschema.ErrColumnNotExists.FastGenByArgs(originalColName, ident.Name))
			return nil
		}
		return errors.Trace(err)
	}

	err = e.DoDDLJobWrapper(sctx, jobW)
	// column not exists, but if_exists flags is true, so we ignore this error.
	if infoschema.ErrColumnNotExists.Equal(err) && spec.IfExists {
		sctx.GetSessionVars().StmtCtx.AppendNote(err)
		return nil
	}
	return errors.Trace(err)
}

func (e *executor) AlterColumn(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	specNewColumn := spec.NewColumns[0]
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name)
	}
	t, err := is.TableByName(e.ctx, ident.Schema, ident.Name)
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
		hasDefaultValue, err := SetDefaultValue(ctx.GetExprCtx(), col, specNewColumn.Options[0])
		if err != nil {
			return errors.Trace(err)
		}
		if err = checkDefaultValue(ctx.GetExprCtx(), col, hasDefaultValue); err != nil {
			return errors.Trace(err)
		}
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        t.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      t.Meta().Name.L,
		Type:           model.ActionSetDefaultValue,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	args := &model.SetDefaultValueArgs{
		Col: col.ColumnInfo,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

// AlterTableComment updates the table comment information.
func (e *executor) AlterTableComment(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}

	tb, err := is.TableByName(e.ctx, ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}
	sessionVars := ctx.GetSessionVars()
	if _, err = validateCommentLength(sessionVars.StmtCtx.ErrCtx(), sessionVars.SQLMode, ident.Name.L, &spec.Comment, dbterror.ErrTooLongTableComment); err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        tb.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      tb.Meta().Name.L,
		Type:           model.ActionModifyTableComment,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	args := &model.ModifyTableCommentArgs{
		Comment: spec.Comment,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

// AlterTableAutoIDCache updates the table comment information.
func (e *executor) AlterTableAutoIDCache(ctx sessionctx.Context, ident ast.Ident, newCache int64) error {
	schema, tb, err := e.getSchemaAndTableByIdent(ident)
	if err != nil {
		return errors.Trace(err)
	}
	tbInfo := tb.Meta()
	if (newCache == 1 && tbInfo.AutoIDCache != 1) ||
		(newCache != 1 && tbInfo.AutoIDCache == 1) {
		return fmt.Errorf("Can't Alter AUTO_ID_CACHE between 1 and non-1, the underlying implementation is different")
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        tb.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      tb.Meta().Name.L,
		Type:           model.ActionModifyTableAutoIDCache,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	args := &model.ModifyTableAutoIDCacheArgs{
		NewCache: newCache,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

// AlterTableCharsetAndCollate changes the table charset and collate.
func (e *executor) AlterTableCharsetAndCollate(ctx sessionctx.Context, ident ast.Ident, toCharset, toCollate string, needsOverwriteCols bool) error {
	// use the last one.
	if toCharset == "" && toCollate == "" {
		return dbterror.ErrUnknownCharacterSet.GenWithStackByArgs(toCharset)
	}

	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}

	tb, err := is.TableByName(e.ctx, ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}

	if toCharset == "" {
		// charset does not change.
		toCharset = tb.Meta().Charset
	}

	if toCollate == "" {
		// Get the default collation of the charset.
		toCollate, err = GetDefaultCollation(toCharset, ctx.GetSessionVars().DefaultCollationForUTF8MB4)
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
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        tb.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      tb.Meta().Name.L,
		Type:           model.ActionModifyTableCharsetAndCollate,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	args := &model.ModifyTableCharsetAndCollateArgs{
		ToCharset:          toCharset,
		ToCollate:          toCollate,
		NeedsOverwriteCols: needsOverwriteCols,
	}
	err = e.doDDLJob2(ctx, job, args)
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
func (*executor) setHypoTiFlashReplica(ctx sessionctx.Context, schemaName, tableName pmodel.CIStr, replicaInfo *ast.TiFlashReplicaSpec) error {
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
func (e *executor) AlterTableSetTiFlashReplica(ctx sessionctx.Context, ident ast.Ident, replicaInfo *ast.TiFlashReplicaSpec) error {
	schema, tb, err := e.getSchemaAndTableByIdent(ident)
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
		return e.setHypoTiFlashReplica(ctx, schema.Name, tb.Meta().Name, replicaInfo)
	}

	err = checkTiFlashReplicaCount(ctx, replicaInfo.Count)
	if err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        tb.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      tb.Meta().Name.L,
		Type:           model.ActionSetTiFlashReplica,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	args := &model.SetTiFlashReplicaArgs{TiflashReplica: *replicaInfo}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

// AlterTableTTLInfoOrEnable submit ddl job to change table info according to the ttlInfo, or ttlEnable
// at least one of the `ttlInfo`, `ttlEnable` or `ttlCronJobSchedule` should be not nil.
// When `ttlInfo` is nil, and `ttlEnable` is not, it will use the original `.TTLInfo` in the table info and modify the
// `.Enable`. If the `.TTLInfo` in the table info is empty, this function will return an error.
// When `ttlInfo` is nil, and `ttlCronJobSchedule` is not, it will use the original `.TTLInfo` in the table info and modify the
// `.JobInterval`. If the `.TTLInfo` in the table info is empty, this function will return an error.
// When `ttlInfo` is not nil, it simply submits the job with the `ttlInfo` and ignore the `ttlEnable`.
func (e *executor) AlterTableTTLInfoOrEnable(ctx sessionctx.Context, ident ast.Ident, ttlInfo *model.TTLInfo, ttlEnable *bool, ttlCronJobSchedule *string) error {
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}

	tb, err := is.TableByName(e.ctx, ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}

	tblInfo := tb.Meta().Clone()
	tableID := tblInfo.ID
	tableName := tblInfo.Name.L

	var job *model.Job
	if ttlInfo != nil {
		tblInfo.TTLInfo = ttlInfo
		err = checkTTLInfoValid(ident.Schema, tblInfo, is)
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
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        tableID,
		SchemaName:     schema.Name.L,
		TableName:      tableName,
		Type:           model.ActionAlterTTLInfo,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	args := &model.AlterTTLInfoArgs{
		TTLInfo:            ttlInfo,
		TTLEnable:          ttlEnable,
		TTLCronJobSchedule: ttlCronJobSchedule,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func (e *executor) AlterTableRemoveTTL(ctx sessionctx.Context, ident ast.Ident) error {
	is := e.infoCache.GetLatest()

	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}

	tb, err := is.TableByName(e.ctx, ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}

	tblInfo := tb.Meta().Clone()
	tableID := tblInfo.ID
	tableName := tblInfo.Name.L

	if tblInfo.TTLInfo != nil {
		job := &model.Job{
			Version:        model.GetJobVerInUse(),
			SchemaID:       schema.ID,
			TableID:        tableID,
			SchemaName:     schema.Name.L,
			TableName:      tableName,
			Type:           model.ActionAlterTTLRemove,
			BinlogInfo:     &model.HistoryInfo{},
			CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
			SQLMode:        ctx.GetSessionVars().SQLMode,
		}
		err = e.doDDLJob2(ctx, job, &model.EmptyArgs{})
		return errors.Trace(err)
	}

	return nil
}

func isTableTiFlashSupported(dbName pmodel.CIStr, tbl *model.TableInfo) error {
	// Memory tables and system tables are not supported by TiFlash
	if util.IsMemOrSysDB(dbName.L) {
		return errors.Trace(dbterror.ErrUnsupportedTiFlashOperationForSysOrMemTable)
	} else if tbl.TempTableType != model.TempTableNone {
		return dbterror.ErrOptOnTemporaryTable.GenWithStackByArgs("set TiFlash replica")
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
	tiflashStoreCnt, err := infoschema.GetTiFlashStoreCount(ctx.GetStore())
	if err != nil {
		return errors.Trace(err)
	}
	if replicaCount > tiflashStoreCnt {
		return errors.Errorf("the tiflash replica count: %d should be less than the total tiflash server count: %d", replicaCount, tiflashStoreCnt)
	}
	return nil
}

// AlterTableAddStatistics registers extended statistics for a table.
func (e *executor) AlterTableAddStatistics(ctx sessionctx.Context, ident ast.Ident, stats *ast.StatisticsSpec, ifNotExists bool) error {
	if !ctx.GetSessionVars().EnableExtendedStats {
		return errors.New("Extended statistics feature is not generally available now, and tidb_enable_extended_stats is OFF")
	}
	// Not support Cardinality and Dependency statistics type for now.
	if stats.StatsType == ast.StatsTypeCardinality || stats.StatsType == ast.StatsTypeDependency {
		return errors.New("Cardinality and Dependency statistics types are not supported now")
	}
	_, tbl, err := e.getSchemaAndTableByIdent(ident)
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
	return e.statsHandle.InsertExtendedStats(stats.StatsName, colIDs, int(stats.StatsType), tblInfo.ID, ifNotExists)
}

// AlterTableDropStatistics logically deletes extended statistics for a table.
func (e *executor) AlterTableDropStatistics(ctx sessionctx.Context, ident ast.Ident, stats *ast.StatisticsSpec, ifExists bool) error {
	if !ctx.GetSessionVars().EnableExtendedStats {
		return errors.New("Extended statistics feature is not generally available now, and tidb_enable_extended_stats is OFF")
	}
	_, tbl, err := e.getSchemaAndTableByIdent(ident)
	if err != nil {
		return err
	}
	tblInfo := tbl.Meta()
	// Call utilities of statistics.Handle to modify system tables instead of doing DML directly,
	// because locking in Handle can guarantee the correctness of `version` in system tables.
	return e.statsHandle.MarkExtendedStatsDeleted(stats.StatsName, tblInfo.ID, ifExists)
}

// UpdateTableReplicaInfo updates the table flash replica infos.
func (e *executor) UpdateTableReplicaInfo(ctx sessionctx.Context, physicalID int64, available bool) error {
	is := e.infoCache.GetLatest()
	tb, ok := is.TableByID(e.ctx, physicalID)
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
		Version:        model.GetJobVerInUse(),
		SchemaID:       db.ID,
		TableID:        tb.Meta().ID,
		SchemaName:     db.Name.L,
		TableName:      tb.Meta().Name.L,
		Type:           model.ActionUpdateTiFlashReplicaStatus,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	args := &model.UpdateTiFlashReplicaStatusArgs{
		Available:  available,
		PhysicalID: physicalID,
	}
	err := e.doDDLJob2(ctx, job, args)
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
	origCharset, origCollate, err = ResolveCharsetCollation([]ast.CharsetOpt{
		{Chs: origCharset, Col: origCollate},
		{Chs: dbInfo.Charset, Col: dbInfo.Collate},
	}, "")
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
func (e *executor) RenameIndex(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}

	tb, err := is.TableByName(e.ctx, ident.Schema, ident.Name)
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
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        tb.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      tb.Meta().Name.L,
		Type:           model.ActionRenameIndex,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	args := &model.ModifyIndexArgs{
		IndexArgs: []*model.IndexArg{
			{IndexName: spec.FromKey},
			{IndexName: spec.ToKey},
		},
	}
	err = e.doDDLJob2(ctx, job, args)
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
	}
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
	for i := 0; i < len(oldIdents); i++ {
		schemas, tableID, err = ExtractTblInfos(is, oldIdents[i], newIdents[i], isAlterTable, tables)
		if err != nil {
			return err
		}

		if t, ok := is.TableByID(e.ctx, tableID); ok {
			if t.Meta().TableCacheStatusType != model.TableCacheStatusDisable {
				return errors.Trace(dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Rename Tables"))
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

func getAnonymousIndexPrefix(isVector bool) string {
	colName := "expression_index"
	if isVector {
		colName = "vector_index"
	}
	return colName
}

// GetName4AnonymousIndex returns a valid name for anonymous index.
func GetName4AnonymousIndex(t table.Table, colName pmodel.CIStr, idxName pmodel.CIStr) pmodel.CIStr {
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
		indexName = pmodel.NewCIStr(fmt.Sprintf("%s_%d", colName.O, id))
		id = 3
	}
	for i := 0; i < l; i++ {
		if t.Indices()[i].Meta().Name.L == indexName.L {
			indexName = pmodel.NewCIStr(fmt.Sprintf("%s_%d", colName.O, id))
			if err := checkTooLongIndex(indexName); err != nil {
				indexName = GetName4AnonymousIndex(t, pmodel.NewCIStr(colName.O[:30]), pmodel.NewCIStr(fmt.Sprintf("%s_%d", colName.O[:30], 2)))
			}
			i = -1
			id++
		}
	}
	return indexName
}

func (e *executor) CreatePrimaryKey(ctx sessionctx.Context, ti ast.Ident, indexName pmodel.CIStr,
	indexPartSpecifications []*ast.IndexPartSpecification, indexOption *ast.IndexOption) error {
	if indexOption != nil && indexOption.PrimaryKeyTp == pmodel.PrimaryKeyTypeClustered {
		return dbterror.ErrUnsupportedModifyPrimaryKey.GenWithStack("Adding clustered primary key is not supported. " +
			"Please consider adding NONCLUSTERED primary key instead")
	}
	schema, t, err := e.getSchemaAndTableByIdent(ti)
	if err != nil {
		return errors.Trace(err)
	}

	if err = checkTooLongIndex(indexName); err != nil {
		return dbterror.ErrTooLongIdent.GenWithStackByArgs(mysql.PrimaryKeyName)
	}

	indexName = pmodel.NewCIStr(mysql.PrimaryKeyName)
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
	// to job queue, the fail path logic is particularly fast.
	// After DDL job is put to the queue, and if the check fail, TiDB will run the DDL cancel logic.
	// The recover step causes DDL wait a few seconds, makes the unit test painfully slow.
	// For same reason, decide whether index is global here.
	indexColumns, _, err := buildIndexColumns(NewMetaBuildContextWithSctx(ctx), tblInfo.Columns, indexPartSpecifications, false)
	if err != nil {
		return errors.Trace(err)
	}
	if _, err = CheckPKOnGeneratedColumn(tblInfo, indexPartSpecifications); err != nil {
		return err
	}

	if tblInfo.GetPartitionInfo() != nil {
		ck, err := checkPartitionKeysConstraint(tblInfo.GetPartitionInfo(), indexColumns, tblInfo)
		if err != nil {
			return err
		}
		if !ck {
			// index columns does not contain all partition columns, must be global
			if indexOption == nil || !indexOption.Global {
				return dbterror.ErrGlobalIndexNotExplicitlySet.GenWithStackByArgs("PRIMARY")
			}
			validateGlobalIndexWithGeneratedColumns(ctx.GetSessionVars().StmtCtx.ErrCtx(), tblInfo, indexName.O, indexColumns)
		}
	}

	// May be truncate comment here, when index comment too long and sql_mode is't strict.
	if indexOption != nil {
		sessionVars := ctx.GetSessionVars()
		if _, err = validateCommentLength(sessionVars.StmtCtx.ErrCtx(), sessionVars.SQLMode, indexName.String(), &indexOption.Comment, dbterror.ErrTooLongIndexComment); err != nil {
			return errors.Trace(err)
		}
	}

	sqlMode := ctx.GetSessionVars().SQLMode
	// global is set to  'false' is just there to be backwards compatible,
	// to avoid unmarshal issues, it is now part of indexOption.
	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        t.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      t.Meta().Name.L,
		Type:           model.ActionAddPrimaryKey,
		BinlogInfo:     &model.HistoryInfo{},
		ReorgMeta:      nil,
		Priority:       ctx.GetSessionVars().DDLReorgPriority,
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	args := &model.ModifyIndexArgs{
		IndexArgs: []*model.IndexArg{{
			Unique:                  true,
			IndexName:               indexName,
			IndexPartSpecifications: indexPartSpecifications,
			IndexOption:             indexOption,
			SQLMode:                 sqlMode,
			Global:                  false,
			IsPK:                    true,
		}},
		OpType: model.OpAddIndex,
	}

	reorgMeta, err := newReorgMetaFromVariables(job, ctx)
	if err != nil {
		return err
	}
	job.ReorgMeta = reorgMeta

	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func checkIndexNameAndColumns(ctx *metabuild.Context, t table.Table, indexName pmodel.CIStr,
	indexPartSpecifications []*ast.IndexPartSpecification, isVector, ifNotExists bool) (pmodel.CIStr, []*model.ColumnInfo, error) {
	// Deal with anonymous index.
	if len(indexName.L) == 0 {
		colName := pmodel.NewCIStr(getAnonymousIndexPrefix(isVector))
		if indexPartSpecifications[0].Column != nil {
			colName = indexPartSpecifications[0].Column.Name
		}
		indexName = GetName4AnonymousIndex(t, colName, pmodel.NewCIStr(""))
	}

	var err error
	if indexInfo := t.Meta().FindIndexByName(indexName.L); indexInfo != nil {
		if indexInfo.State != model.StatePublic {
			// NOTE: explicit error message. See issue #18363.
			err = dbterror.ErrDupKeyName.GenWithStack("index already exist %s; "+
				"a background job is trying to add the same index, "+
				"please check by `ADMIN SHOW DDL JOBS`", indexName)
		} else {
			err = dbterror.ErrDupKeyName.GenWithStackByArgs(indexName)
		}
		if ifNotExists {
			ctx.AppendNote(err)
			return pmodel.CIStr{}, nil, nil
		}
		return pmodel.CIStr{}, nil, err
	}

	if err = checkTooLongIndex(indexName); err != nil {
		return pmodel.CIStr{}, nil, errors.Trace(err)
	}

	// Build hidden columns if necessary.
	var hiddenCols []*model.ColumnInfo
	if !isVector {
		hiddenCols, err = buildHiddenColumnInfoWithCheck(ctx, indexPartSpecifications, indexName, t.Meta(), t.Cols())
		if err != nil {
			return pmodel.CIStr{}, nil, err
		}
	}
	if err = checkAddColumnTooManyColumns(len(t.Cols()) + len(hiddenCols)); err != nil {
		return pmodel.CIStr{}, nil, errors.Trace(err)
	}

	return indexName, hiddenCols, nil
}

func checkTableTypeForVectorIndex(tblInfo *model.TableInfo) error {
	if tblInfo.TableCacheStatusType != model.TableCacheStatusDisable {
		return dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Create Vector Index")
	}
	if tblInfo.TempTableType != model.TempTableNone {
		return dbterror.ErrOptOnTemporaryTable.FastGenByArgs("vector index")
	}
	if tblInfo.GetPartitionInfo() != nil {
		return dbterror.ErrUnsupportedAddVectorIndex.FastGenByArgs("unsupported partition table")
	}
	if tblInfo.TiFlashReplica == nil || tblInfo.TiFlashReplica.Count == 0 {
		return dbterror.ErrUnsupportedAddVectorIndex.FastGenByArgs("unsupported empty TiFlash replica, the replica is nil")
	}

	return nil
}

func (e *executor) createVectorIndex(ctx sessionctx.Context, ti ast.Ident, indexName pmodel.CIStr,
	indexPartSpecifications []*ast.IndexPartSpecification, indexOption *ast.IndexOption, ifNotExists bool) error {
	schema, t, err := e.getSchemaAndTableByIdent(ti)
	if err != nil {
		return errors.Trace(err)
	}

	tblInfo := t.Meta()
	if err := checkTableTypeForVectorIndex(tblInfo); err != nil {
		return errors.Trace(err)
	}

	metaBuildCtx := NewMetaBuildContextWithSctx(ctx)
	indexName, _, err = checkIndexNameAndColumns(metaBuildCtx, t, indexName, indexPartSpecifications, true, ifNotExists)
	if err != nil {
		return errors.Trace(err)
	}
	_, funcExpr, err := buildVectorInfoWithCheck(indexPartSpecifications, tblInfo)
	if err != nil {
		return errors.Trace(err)
	}

	// Check before the job is put to the queue.
	// This check is redundant, but useful. If DDL check fail before the job is put
	// to job queue, the fail path logic is particularly fast.
	// After DDL job is put to the queue, and if the check fail, TiDB will run the DDL cancel logic.
	// The recover step causes DDL wait a few seconds, makes the unit test painfully slow.
	// For same reason, decide whether index is global here.
	_, _, err = buildIndexColumns(metaBuildCtx, tblInfo.Columns, indexPartSpecifications, true)
	if err != nil {
		return errors.Trace(err)
	}

	// May be truncate comment here, when index comment too long and sql_mode it's strict.
	sessionVars := ctx.GetSessionVars()
	if _, err = validateCommentLength(sessionVars.StmtCtx.ErrCtx(), sessionVars.SQLMode, indexName.String(), &indexOption.Comment, dbterror.ErrTooLongTableComment); err != nil {
		return errors.Trace(err)
	}

	job, err := buildAddIndexJobWithoutTypeAndArgs(ctx, schema, t)
	if err != nil {
		return errors.Trace(err)
	}
	job.Version = model.GetJobVerInUse()
	job.Type = model.ActionAddVectorIndex
	indexPartSpecifications[0].Expr = nil

	// TODO: support CDCWriteSource

	args := &model.ModifyIndexArgs{
		IndexArgs: []*model.IndexArg{{
			IndexName:               indexName,
			IndexPartSpecifications: indexPartSpecifications,
			IndexOption:             indexOption,
			FuncExpr:                funcExpr,
			IsVector:                true,
		}},
		OpType: model.OpAddIndex,
	}

	err = e.doDDLJob2(ctx, job, args)
	// key exists, but if_not_exists flags is true, so we ignore this error.
	if dbterror.ErrDupKeyName.Equal(err) && ifNotExists {
		ctx.GetSessionVars().StmtCtx.AppendNote(err)
		return nil
	}
	return errors.Trace(err)
}

func buildAddIndexJobWithoutTypeAndArgs(ctx sessionctx.Context, schema *model.DBInfo, t table.Table) (*model.Job, error) {
	tzName, tzOffset := ddlutil.GetTimeZone(ctx)
	charset, collate := ctx.GetSessionVars().GetCharsetInfo()
	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		SchemaName: schema.Name.L,
		TableName:  t.Meta().Name.L,
		BinlogInfo: &model.HistoryInfo{},
		ReorgMeta: &model.DDLReorgMeta{
			SQLMode:       ctx.GetSessionVars().SQLMode,
			Warnings:      make(map[errors.ErrorID]*terror.Error),
			WarningsCount: make(map[errors.ErrorID]int64),
			Location:      &model.TimeZoneLocation{Name: tzName, Offset: tzOffset},
		},
		Priority: ctx.GetSessionVars().DDLReorgPriority,
		Charset:  charset,
		Collate:  collate,
		SQLMode:  ctx.GetSessionVars().SQLMode,
	}
	reorgMeta, err := newReorgMetaFromVariables(job, ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	job.ReorgMeta = reorgMeta
	return job, nil
}

func (e *executor) CreateIndex(ctx sessionctx.Context, stmt *ast.CreateIndexStmt) error {
	ident := ast.Ident{Schema: stmt.Table.Schema, Name: stmt.Table.Name}
	return e.createIndex(ctx, ident, stmt.KeyType, pmodel.NewCIStr(stmt.IndexName),
		stmt.IndexPartSpecifications, stmt.IndexOption, stmt.IfNotExists)
}

// addHypoIndexIntoCtx adds this index as a hypo-index into this ctx.
func (*executor) addHypoIndexIntoCtx(ctx sessionctx.Context, schemaName, tableName pmodel.CIStr, indexInfo *model.IndexInfo) error {
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

func (e *executor) createIndex(ctx sessionctx.Context, ti ast.Ident, keyType ast.IndexKeyType, indexName pmodel.CIStr,
	indexPartSpecifications []*ast.IndexPartSpecification, indexOption *ast.IndexOption, ifNotExists bool) error {
	// not support Spatial and FullText index
	if keyType == ast.IndexKeyTypeFullText || keyType == ast.IndexKeyTypeSpatial {
		return dbterror.ErrUnsupportedIndexType.GenWithStack("FULLTEXT and SPATIAL index is not supported")
	}
	if keyType == ast.IndexKeyTypeVector {
		return e.createVectorIndex(ctx, ti, indexName, indexPartSpecifications, indexOption, ifNotExists)
	}
	unique := keyType == ast.IndexKeyTypeUnique
	schema, t, err := e.getSchemaAndTableByIdent(ti)
	if err != nil {
		return errors.Trace(err)
	}

	if t.Meta().TableCacheStatusType != model.TableCacheStatusDisable {
		return errors.Trace(dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Create Index"))
	}
	metaBuildCtx := NewMetaBuildContextWithSctx(ctx)
	indexName, hiddenCols, err := checkIndexNameAndColumns(metaBuildCtx, t, indexName, indexPartSpecifications, false, ifNotExists)
	if err != nil {
		return errors.Trace(err)
	}

	tblInfo := t.Meta()
	finalColumns := make([]*model.ColumnInfo, len(tblInfo.Columns), len(tblInfo.Columns)+len(hiddenCols))
	copy(finalColumns, tblInfo.Columns)
	finalColumns = append(finalColumns, hiddenCols...)
	// Check before the job is put to the queue.
	// This check is redundant, but useful. If DDL check fail before the job is put
	// to job queue, the fail path logic is particularly fast.
	// After DDL job is put to the queue, and if the check fail, TiDB will run the DDL cancel logic.
	// The recover step causes DDL wait a few seconds, makes the unit test painfully slow.
	// For same reason, decide whether index is global here.
	indexColumns, _, err := buildIndexColumns(metaBuildCtx, finalColumns, indexPartSpecifications, false)
	if err != nil {
		return errors.Trace(err)
	}

	globalIndex := false
	if indexOption != nil && indexOption.Global {
		globalIndex = true
	}
	if globalIndex {
		if tblInfo.GetPartitionInfo() == nil {
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("Global Index on non-partitioned table")
		}
		if !unique {
			// TODO: remove this limitation
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("Global IndexOption on non-unique index")
		}
	}
	if unique && tblInfo.GetPartitionInfo() != nil {
		ck, err := checkPartitionKeysConstraint(tblInfo.GetPartitionInfo(), indexColumns, tblInfo)
		if err != nil {
			return err
		}
		if !ck {
			// index columns does not contain all partition columns, must be global
			if !globalIndex {
				return dbterror.ErrGlobalIndexNotExplicitlySet.GenWithStackByArgs(indexName.O)
			}
			validateGlobalIndexWithGeneratedColumns(ctx.GetSessionVars().StmtCtx.ErrCtx(), tblInfo, indexName.O, indexColumns)
		} else if globalIndex {
			// TODO: remove this restriction
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("Global IndexOption on index including all columns in the partitioning expression")
		}
	}
	// May be truncate comment here, when index comment too long and sql_mode is't strict.
	if indexOption != nil {
		sessionVars := ctx.GetSessionVars()
		if _, err = validateCommentLength(sessionVars.StmtCtx.ErrCtx(), sessionVars.SQLMode, indexName.String(), &indexOption.Comment, dbterror.ErrTooLongIndexComment); err != nil {
			return errors.Trace(err)
		}
	}

	if indexOption != nil && indexOption.Tp == pmodel.IndexTypeHypo { // for hypo-index
		indexInfo, err := BuildIndexInfo(metaBuildCtx, tblInfo, indexName, false, unique, false,
			indexPartSpecifications, indexOption, model.StatePublic)
		if err != nil {
			return err
		}
		return e.addHypoIndexIntoCtx(ctx, ti.Schema, ti.Name, indexInfo)
	}

	// global is set to  'false' is just there to be backwards compatible,
	// to avoid unmarshal issues, it is now part of indexOption.
	global := false
	job, err := buildAddIndexJobWithoutTypeAndArgs(ctx, schema, t)
	if err != nil {
		return errors.Trace(err)
	}

	job.Version = model.GetJobVerInUse()
	job.Type = model.ActionAddIndex
	job.CDCWriteSource = ctx.GetSessionVars().CDCWriteSource

	args := &model.ModifyIndexArgs{
		IndexArgs: []*model.IndexArg{{
			Unique:                  unique,
			IndexName:               indexName,
			IndexPartSpecifications: indexPartSpecifications,
			IndexOption:             indexOption,
			HiddenCols:              hiddenCols,
			Global:                  global,
		}},
		OpType: model.OpAddIndex,
	}

	err = e.doDDLJob2(ctx, job, args)
	// key exists, but if_not_exists flags is true, so we ignore this error.
	if dbterror.ErrDupKeyName.Equal(err) && ifNotExists {
		ctx.GetSessionVars().StmtCtx.AppendNote(err)
		return nil
	}
	return errors.Trace(err)
}

func newReorgMetaFromVariables(job *model.Job, sctx sessionctx.Context) (*model.DDLReorgMeta, error) {
	reorgMeta := NewDDLReorgMeta(sctx)
	reorgMeta.IsDistReorg = variable.EnableDistTask.Load()
	reorgMeta.IsFastReorg = variable.EnableFastReorg.Load()
	reorgMeta.TargetScope = variable.ServiceScope.Load()
	if sv, ok := sctx.GetSessionVars().GetSystemVar(variable.TiDBDDLReorgWorkerCount); ok {
		reorgMeta.Concurrency = variable.TidbOptInt(sv, 0)
	}
	if sv, ok := sctx.GetSessionVars().GetSystemVar(variable.TiDBDDLReorgBatchSize); ok {
		reorgMeta.BatchSize = variable.TidbOptInt(sv, 0)
	}

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

	logutil.DDLLogger().Info("initialize reorg meta",
		zap.String("jobSchema", job.SchemaName),
		zap.String("jobTable", job.TableName),
		zap.Stringer("jobType", job.Type),
		zap.Bool("enableDistTask", reorgMeta.IsDistReorg),
		zap.Bool("enableFastReorg", reorgMeta.IsFastReorg),
		zap.String("targetScope", reorgMeta.TargetScope),
		zap.Int("concurrency", reorgMeta.Concurrency),
		zap.Int("batchSize", reorgMeta.BatchSize),
	)
	return reorgMeta, nil
}

// LastReorgMetaFastReorgDisabled is used for test.
var LastReorgMetaFastReorgDisabled bool

func buildFKInfo(fkName pmodel.CIStr, keys []*ast.IndexPartSpecification, refer *ast.ReferenceDef, cols []*table.Column) (*model.FKInfo, error) {
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
		Cols:      make([]pmodel.CIStr, len(keys)),
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
				case pmodel.ReferOptionCascade, pmodel.ReferOptionSetNull, pmodel.ReferOptionSetDefault:
					//nolint: gosec
					return nil, dbterror.ErrWrongFKOptionForGeneratedColumn.GenWithStackByArgs("ON UPDATE " + refer.OnUpdate.ReferOpt.String())
				}
				switch refer.OnDelete.ReferOpt {
				case pmodel.ReferOptionSetNull, pmodel.ReferOptionSetDefault:
					//nolint: gosec
					return nil, dbterror.ErrWrongFKOptionForGeneratedColumn.GenWithStackByArgs("ON DELETE " + refer.OnDelete.ReferOpt.String())
				}
				continue
			}
			// Check wrong reference options of foreign key on base columns of stored generated columns
			if _, ok := baseCols[col.Name.L]; ok {
				switch refer.OnUpdate.ReferOpt {
				case pmodel.ReferOptionCascade, pmodel.ReferOptionSetNull, pmodel.ReferOptionSetDefault:
					return nil, infoschema.ErrCannotAddForeign
				}
				switch refer.OnDelete.ReferOpt {
				case pmodel.ReferOptionCascade, pmodel.ReferOptionSetNull, pmodel.ReferOptionSetDefault:
					return nil, infoschema.ErrCannotAddForeign
				}
			}
		}
		col := table.FindCol(cols, key.Column.Name.O)
		if col == nil {
			return nil, dbterror.ErrKeyColumnDoesNotExits.GenWithStackByArgs(key.Column.Name)
		}
		if mysql.HasNotNullFlag(col.GetFlag()) && (refer.OnDelete.ReferOpt == pmodel.ReferOptionSetNull || refer.OnUpdate.ReferOpt == pmodel.ReferOptionSetNull) {
			return nil, infoschema.ErrForeignKeyColumnNotNull.GenWithStackByArgs(col.Name.O, fkName)
		}
		fkInfo.Cols[i] = key.Column.Name
	}

	fkInfo.RefCols = make([]pmodel.CIStr, len(refer.IndexPartSpecifications))
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

func (e *executor) CreateForeignKey(ctx sessionctx.Context, ti ast.Ident, fkName pmodel.CIStr, keys []*ast.IndexPartSpecification, refer *ast.ReferenceDef) error {
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ti.Schema)
	}

	t, err := is.TableByName(context.Background(), ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name))
	}
	if t.Meta().TempTableType != model.TempTableNone {
		return infoschema.ErrCannotAddForeign
	}

	if fkName.L == "" {
		fkName = pmodel.NewCIStr(fmt.Sprintf("fk_%d", t.Meta().MaxForeignKeyID+1))
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
		err = e.createIndex(ctx, ti, ast.IndexKeyTypeNone, fkInfo.Name, indexPartSpecifications, indexOption, false)
		if err != nil {
			return err
		}
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        t.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      t.Meta().Name.L,
		Type:           model.ActionAddForeignKey,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{
			{
				Database: schema.Name.L,
				Table:    t.Meta().Name.L,
			},
			{
				Database: fkInfo.RefSchema.L,
				Table:    fkInfo.RefTable.L,
				Mode:     model.SharedInvolving,
			},
		},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}

	args := &model.AddForeignKeyArgs{
		FkInfo:  fkInfo,
		FkCheck: fkCheck,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func (e *executor) DropForeignKey(ctx sessionctx.Context, ti ast.Ident, fkName pmodel.CIStr) error {
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ti.Schema)
	}

	t, err := is.TableByName(context.Background(), ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name))
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        t.Meta().ID,
		SchemaName:     schema.Name.L,
		SchemaState:    model.StatePublic,
		TableName:      t.Meta().Name.L,
		Type:           model.ActionDropForeignKey,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	args := &model.DropForeignKeyArgs{FkName: fkName}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func (e *executor) DropIndex(ctx sessionctx.Context, stmt *ast.DropIndexStmt) error {
	ti := ast.Ident{Schema: stmt.Table.Schema, Name: stmt.Table.Name}
	err := e.dropIndex(ctx, ti, pmodel.NewCIStr(stmt.IndexName), stmt.IfExists, stmt.IsHypo)
	if (infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableNotExists.Equal(err)) && stmt.IfExists {
		err = nil
	}
	return err
}

// dropHypoIndexFromCtx drops this hypo-index from this ctx.
func (*executor) dropHypoIndexFromCtx(ctx sessionctx.Context, schema, table, index pmodel.CIStr, ifExists bool) error {
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
func (e *executor) dropIndex(ctx sessionctx.Context, ti ast.Ident, indexName pmodel.CIStr, ifExist, isHypo bool) error {
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists)
	}
	t, err := is.TableByName(context.Background(), ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name))
	}
	if t.Meta().TableCacheStatusType != model.TableCacheStatusDisable {
		return errors.Trace(dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Drop Index"))
	}

	if isHypo {
		return e.dropHypoIndexFromCtx(ctx, ti.Schema, ti.Name, indexName, ifExist)
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
		if ifExist {
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
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        t.Meta().ID,
		SchemaName:     schema.Name.L,
		SchemaState:    indexInfo.State,
		TableName:      t.Meta().Name.L,
		Type:           jobTp,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	args := &model.ModifyIndexArgs{
		IndexArgs: []*model.IndexArg{{
			IndexName: indexName,
			IfExist:   ifExist,
		}},
		OpType: model.OpDropIndex,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

// CheckIsDropPrimaryKey checks if we will drop PK, there are many PK implementations so we provide a helper function.
func CheckIsDropPrimaryKey(indexName pmodel.CIStr, indexInfo *model.IndexInfo, t table.Table) (bool, error) {
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

func validateGlobalIndexWithGeneratedColumns(ec errctx.Context, tblInfo *model.TableInfo, indexName string, indexColumns []*model.IndexColumn) {
	// Auto analyze is not effective when a global index contains prefix columns or virtual generated columns.
	for _, col := range indexColumns {
		colInfo := tblInfo.Columns[col.Offset]
		isPrefixCol := col.Length != types.UnspecifiedLength
		if colInfo.IsVirtualGenerated() || isPrefixCol {
			ec.AppendWarning(dbterror.ErrWarnGlobalIndexNeedManuallyAnalyze.FastGenByArgs(indexName))
			return
		}
	}
}

// BuildAddedPartitionInfo build alter table add partition info
func BuildAddedPartitionInfo(ctx expression.BuildContext, meta *model.TableInfo, spec *ast.AlterTableSpec) (*model.PartitionInfo, error) {
	numParts := uint64(0)
	switch meta.Partition.Type {
	case pmodel.PartitionTypeNone:
		// OK
	case pmodel.PartitionTypeList:
		if len(spec.PartDefinitions) == 0 {
			return nil, ast.ErrPartitionsMustBeDefined.GenWithStackByArgs(meta.Partition.Type)
		}
		err := checkListPartitions(spec.PartDefinitions)
		if err != nil {
			return nil, err
		}

	case pmodel.PartitionTypeRange:
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
	case pmodel.PartitionTypeHash, pmodel.PartitionTypeKey:
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

func throwErrIfInMemOrSysDB(ctx sessionctx.Context, dbLowerName string) error {
	if util.IsMemOrSysDB(dbLowerName) {
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

func (e *executor) RepairTable(ctx sessionctx.Context, createStmt *ast.CreateTableStmt) error {
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
	newTableInfo, err := buildTableInfoWithCheck(NewMetaBuildContextWithSctx(ctx), ctx.GetStore(), createStmt,
		oldTableInfo.Charset, oldTableInfo.Collate, oldTableInfo.PlacementPolicyRef)
	if err != nil {
		return errors.Trace(err)
	}
	if err = rewritePartitionQueryString(ctx, createStmt.Partition, newTableInfo); err != nil {
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
		Version:        model.GetJobVerInUse(),
		SchemaID:       oldDBInfo.ID,
		TableID:        newTableInfo.ID,
		SchemaName:     oldDBInfo.Name.L,
		TableName:      newTableInfo.Name.L,
		Type:           model.ActionRepairTable,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	args := &model.RepairTableArgs{TableInfo: newTableInfo}
	err = e.doDDLJob2(ctx, job, args)
	if err == nil {
		// Remove the old TableInfo from repairInfo before domain reload.
		domainutil.RepairInfo.RemoveFromRepairInfo(oldDBInfo.Name.L, oldTableInfo.Name.L)
	}
	return errors.Trace(err)
}

func (e *executor) OrderByColumns(ctx sessionctx.Context, ident ast.Ident) error {
	_, tb, err := e.getSchemaAndTableByIdent(ident)
	if err != nil {
		return errors.Trace(err)
	}
	if tb.Meta().GetPkColInfo() != nil {
		ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("ORDER BY ignored as there is a user-defined clustered index in the table '%s'", ident.Name))
	}
	return nil
}

func (e *executor) CreateSequence(ctx sessionctx.Context, stmt *ast.CreateSequenceStmt) error {
	ident := ast.Ident{Name: stmt.Name.Name, Schema: stmt.Name.Schema}
	sequenceInfo, err := buildSequenceInfo(stmt, ident)
	if err != nil {
		return err
	}
	// TiDB describe the sequence within a tableInfo, as a same-level object of a table and view.
	tbInfo, err := BuildTableInfo(NewMetaBuildContextWithSctx(ctx), ident.Name, nil, nil, "", "")
	if err != nil {
		return err
	}
	tbInfo.Sequence = sequenceInfo

	onExist := OnExistError
	if stmt.IfNotExists {
		onExist = OnExistIgnore
	}

	return e.CreateTableWithInfo(ctx, ident.Schema, tbInfo, nil, WithOnExist(onExist))
}

func (e *executor) AlterSequence(ctx sessionctx.Context, stmt *ast.AlterSequenceStmt) error {
	ident := ast.Ident{Name: stmt.Name.Name, Schema: stmt.Name.Schema}
	is := e.infoCache.GetLatest()
	// Check schema existence.
	db, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}
	// Check table existence.
	tbl, err := is.TableByName(context.Background(), ident.Schema, ident.Name)
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
		Version:        model.GetJobVerInUse(),
		SchemaID:       db.ID,
		TableID:        tbl.Meta().ID,
		SchemaName:     db.Name.L,
		TableName:      tbl.Meta().Name.L,
		Type:           model.ActionAlterSequence,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	args := &model.AlterSequenceArgs{
		Ident:      ident,
		SeqOptions: stmt.SeqOptions,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func (e *executor) DropSequence(ctx sessionctx.Context, stmt *ast.DropSequenceStmt) (err error) {
	return e.dropTableObject(ctx, stmt.Sequences, stmt.IfExists, sequenceObject)
}

func (e *executor) AlterIndexVisibility(ctx sessionctx.Context, ident ast.Ident, indexName pmodel.CIStr, visibility ast.IndexVisibility) error {
	schema, tb, err := e.getSchemaAndTableByIdent(ident)
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
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        tb.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      tb.Meta().Name.L,
		Type:           model.ActionAlterIndexVisibility,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	args := &model.AlterIndexVisibilityArgs{
		IndexName: indexName,
		Invisible: invisible,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func (e *executor) AlterTableAttributes(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	schema, tb, err := e.getSchemaAndTableByIdent(ident)
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
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        meta.ID,
		SchemaName:     schema.Name.L,
		TableName:      meta.Name.L,
		Type:           model.ActionAlterTableAttributes,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	pdLabelRule := (*pdhttp.LabelRule)(rule)
	args := &model.AlterTableAttributesArgs{LabelRule: pdLabelRule}
	err = e.doDDLJob2(ctx, job, args)
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(err)
}

func (e *executor) AlterTablePartitionAttributes(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) (err error) {
	schema, tb, err := e.getSchemaAndTableByIdent(ident)
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

	pdLabelRule := (*pdhttp.LabelRule)(rule)
	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        meta.ID,
		SchemaName:     schema.Name.L,
		TableName:      meta.Name.L,
		Type:           model.ActionAlterTablePartitionAttributes,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	args := &model.AlterTablePartitionArgs{
		PartitionID: partitionID,
		LabelRule:   pdLabelRule,
	}

	err = e.doDDLJob2(ctx, job, args)
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(err)
}

func (e *executor) AlterTablePartitionOptions(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) (err error) {
	var policyRefInfo *model.PolicyRefInfo
	if spec.Options != nil {
		for _, op := range spec.Options {
			switch op.Tp {
			case ast.TableOptionPlacementPolicy:
				policyRefInfo = &model.PolicyRefInfo{
					Name: pmodel.NewCIStr(op.StrValue),
				}
			default:
				return errors.Trace(errors.New("unknown partition option"))
			}
		}
	}

	if policyRefInfo != nil {
		err = e.AlterTablePartitionPlacement(ctx, ident, spec, policyRefInfo)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (e *executor) AlterTablePartitionPlacement(ctx sessionctx.Context, tableIdent ast.Ident, spec *ast.AlterTableSpec, policyRefInfo *model.PolicyRefInfo) (err error) {
	schema, tb, err := e.getSchemaAndTableByIdent(tableIdent)
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

	var involveSchemaInfo []model.InvolvingSchemaInfo
	if policyRefInfo != nil {
		involveSchemaInfo = []model.InvolvingSchemaInfo{
			{
				Database: schema.Name.L,
				Table:    tblInfo.Name.L,
			},
			{
				Policy: policyRefInfo.Name.L,
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
		Type:                model.ActionAlterTablePartitionPlacement,
		BinlogInfo:          &model.HistoryInfo{},
		CDCWriteSource:      ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: involveSchemaInfo,
		SQLMode:             ctx.GetSessionVars().SQLMode,
	}
	args := &model.AlterTablePartitionArgs{
		PartitionID:   partitionID,
		PolicyRefInfo: policyRefInfo,
	}

	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

// AddResourceGroup implements the DDL interface, creates a resource group.
func (e *executor) AddResourceGroup(ctx sessionctx.Context, stmt *ast.CreateResourceGroupStmt) (err error) {
	groupName := stmt.ResourceGroupName
	groupInfo := &model.ResourceGroupInfo{Name: groupName, ResourceGroupSettings: model.NewResourceGroupSettings()}
	groupInfo, err = buildResourceGroup(groupInfo, stmt.ResourceGroupOptionList)
	if err != nil {
		return err
	}

	if _, ok := e.infoCache.GetLatest().ResourceGroupByName(groupName); ok {
		if stmt.IfNotExists {
			err = infoschema.ErrResourceGroupExists.FastGenByArgs(groupName)
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return infoschema.ErrResourceGroupExists.GenWithStackByArgs(groupName)
	}

	if err := checkResourceGroupValidation(groupInfo); err != nil {
		return err
	}

	logutil.DDLLogger().Debug("create resource group", zap.String("name", groupName.O), zap.Stringer("resource group settings", groupInfo.ResourceGroupSettings))

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaName:     groupName.L,
		Type:           model.ActionCreateResourceGroup,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			ResourceGroup: groupInfo.Name.L,
		}},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}
	args := &model.ResourceGroupArgs{RGInfo: groupInfo}
	err = e.doDDLJob2(ctx, job, args)
	return err
}

// DropResourceGroup implements the DDL interface.
func (e *executor) DropResourceGroup(ctx sessionctx.Context, stmt *ast.DropResourceGroupStmt) (err error) {
	groupName := stmt.ResourceGroupName
	if groupName.L == rg.DefaultResourceGroupName {
		return resourcegroup.ErrDroppingInternalResourceGroup
	}
	is := e.infoCache.GetLatest()
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
		Version:        model.GetJobVerInUse(),
		SchemaID:       group.ID,
		SchemaName:     group.Name.L,
		Type:           model.ActionDropResourceGroup,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			ResourceGroup: groupName.L,
		}},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}
	args := &model.ResourceGroupArgs{RGInfo: &model.ResourceGroupInfo{Name: groupName}}
	err = e.doDDLJob2(ctx, job, args)
	return err
}

// AlterResourceGroup implements the DDL interface.
func (e *executor) AlterResourceGroup(ctx sessionctx.Context, stmt *ast.AlterResourceGroupStmt) (err error) {
	groupName := stmt.ResourceGroupName
	is := e.infoCache.GetLatest()
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

	if err := checkResourceGroupValidation(newGroupInfo); err != nil {
		return err
	}

	logutil.DDLLogger().Debug("alter resource group", zap.String("name", groupName.L), zap.Stringer("new resource group settings", newGroupInfo.ResourceGroupSettings))

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       newGroupInfo.ID,
		SchemaName:     newGroupInfo.Name.L,
		Type:           model.ActionAlterResourceGroup,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			ResourceGroup: newGroupInfo.Name.L,
		}},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}
	args := &model.ResourceGroupArgs{RGInfo: newGroupInfo}
	err = e.doDDLJob2(ctx, job, args)
	return err
}

func (e *executor) CreatePlacementPolicy(ctx sessionctx.Context, stmt *ast.CreatePlacementPolicyStmt) (err error) {
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

	return e.CreatePlacementPolicyWithInfo(ctx, policyInfo, onExists)
}

func (e *executor) DropPlacementPolicy(ctx sessionctx.Context, stmt *ast.DropPlacementPolicyStmt) (err error) {
	if checkIgnorePlacementDDL(ctx) {
		return nil
	}
	policyName := stmt.PolicyName
	is := e.infoCache.GetLatest()
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
		Version:        model.GetJobVerInUse(),
		SchemaID:       policy.ID,
		SchemaName:     policy.Name.L,
		Type:           model.ActionDropPlacementPolicy,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			Policy: policyName.L,
		}},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}

	args := &model.PlacementPolicyArgs{
		PolicyName: policyName,
		PolicyID:   policy.ID,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func (e *executor) AlterPlacementPolicy(ctx sessionctx.Context, stmt *ast.AlterPlacementPolicyStmt) (err error) {
	if checkIgnorePlacementDDL(ctx) {
		return nil
	}
	policyName := stmt.PolicyName
	is := e.infoCache.GetLatest()
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
		Version:        model.GetJobVerInUse(),
		SchemaID:       policy.ID,
		SchemaName:     policy.Name.L,
		Type:           model.ActionAlterPlacementPolicy,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			Policy: newPolicyInfo.Name.L,
		}},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}
	args := &model.PlacementPolicyArgs{
		Policy:   newPolicyInfo,
		PolicyID: policy.ID,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func (e *executor) AlterTableCache(sctx sessionctx.Context, ti ast.Ident) (err error) {
	schema, t, err := e.getSchemaAndTableByIdent(ti)
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

	succ, err := checkCacheTableSize(e.store, t.Meta().ID)
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
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		SchemaName:     schema.Name.L,
		TableName:      t.Meta().Name.L,
		TableID:        t.Meta().ID,
		Type:           model.ActionAlterCacheTable,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: sctx.GetSessionVars().CDCWriteSource,
		SQLMode:        sctx.GetSessionVars().SQLMode,
	}

	return e.doDDLJob2(sctx, job, &model.EmptyArgs{})
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

func (e *executor) AlterTableNoCache(ctx sessionctx.Context, ti ast.Ident) (err error) {
	schema, t, err := e.getSchemaAndTableByIdent(ti)
	if err != nil {
		return err
	}
	// if a table is not in cache state, return directly
	if t.Meta().TableCacheStatusType == model.TableCacheStatusDisable {
		return nil
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		SchemaName:     schema.Name.L,
		TableName:      t.Meta().Name.L,
		TableID:        t.Meta().ID,
		Type:           model.ActionAlterNoCacheTable,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	return e.doDDLJob2(ctx, job, &model.EmptyArgs{})
}

func (e *executor) CreateCheckConstraint(ctx sessionctx.Context, ti ast.Ident, constrName pmodel.CIStr, constr *ast.Constraint) error {
	schema, t, err := e.getSchemaAndTableByIdent(ti)
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
	dependedCols := make([]pmodel.CIStr, 0, len(dependedColsMap))
	for k := range dependedColsMap {
		if _, ok := existedColsMap[k]; !ok {
			// The table constraint depended on a non-existed column.
			return dbterror.ErrBadField.GenWithStackByArgs(k, "check constraint "+constr.Name+" expression")
		}
		dependedCols = append(dependedCols, pmodel.NewCIStr(k))
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
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        tblInfo.ID,
		SchemaName:     schema.Name.L,
		TableName:      tblInfo.Name.L,
		Type:           model.ActionAddCheckConstraint,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		Priority:       ctx.GetSessionVars().DDLReorgPriority,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	args := &model.AddCheckConstraintArgs{
		Constraint: constraintInfo,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func (e *executor) DropCheckConstraint(ctx sessionctx.Context, ti ast.Ident, constrName pmodel.CIStr) error {
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists)
	}
	t, err := is.TableByName(context.Background(), ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name))
	}
	tblInfo := t.Meta()

	constraintInfo := tblInfo.FindConstraintInfoByName(constrName.L)
	if constraintInfo == nil {
		return dbterror.ErrConstraintNotFound.GenWithStackByArgs(constrName)
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        tblInfo.ID,
		SchemaName:     schema.Name.L,
		TableName:      tblInfo.Name.L,
		Type:           model.ActionDropCheckConstraint,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	args := &model.CheckConstraintArgs{
		ConstraintName: constrName,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func (e *executor) AlterCheckConstraint(ctx sessionctx.Context, ti ast.Ident, constrName pmodel.CIStr, enforced bool) error {
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists)
	}
	t, err := is.TableByName(context.Background(), ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name))
	}
	tblInfo := t.Meta()

	constraintInfo := tblInfo.FindConstraintInfoByName(constrName.L)
	if constraintInfo == nil {
		return dbterror.ErrConstraintNotFound.GenWithStackByArgs(constrName)
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        tblInfo.ID,
		SchemaName:     schema.Name.L,
		TableName:      tblInfo.Name.L,
		Type:           model.ActionAlterCheckConstraint,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	args := &model.CheckConstraintArgs{
		ConstraintName: constrName,
		Enforced:       enforced,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func (e *executor) genPlacementPolicyID() (int64, error) {
	var ret int64
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	err := kv.RunInNewTxn(ctx, e.store, true, func(_ context.Context, txn kv.Transaction) error {
		m := meta.NewMutator(txn)
		var err error
		ret, err = m.GenPlacementPolicyID()
		return err
	})

	return ret, err
}

// DoDDLJob will return
// - nil: found in history DDL job and no job error
// - context.Cancel: job has been sent to worker, but not found in history DDL job before cancel
// - other: found in history DDL job and return that job error
func (e *executor) DoDDLJob(ctx sessionctx.Context, job *model.Job) error {
	return e.DoDDLJobWrapper(ctx, NewJobWrapper(job, false))
}

func (e *executor) doDDLJob2(ctx sessionctx.Context, job *model.Job, args model.JobArgs) error {
	return e.DoDDLJobWrapper(ctx, NewJobWrapperWithArgs(job, args, false))
}

// DoDDLJobWrapper submit DDL job and wait it finishes.
// When fast create is enabled, we might merge multiple jobs into one, so do not
// depend on job.ID, use JobID from jobSubmitResult.
func (e *executor) DoDDLJobWrapper(ctx sessionctx.Context, jobW *JobWrapper) (resErr error) {
	job := jobW.Job
	job.TraceInfo = &model.TraceInfo{
		ConnectionID: ctx.GetSessionVars().ConnectionID,
		SessionAlias: ctx.GetSessionVars().SessionAlias,
	}
	if mci := ctx.GetSessionVars().StmtCtx.MultiSchemaInfo; mci != nil {
		// In multiple schema change, we don't run the job.
		// Instead, we merge all the jobs into one pending job.
		return appendToSubJobs(mci, jobW)
	}
	// Get a global job ID and put the DDL job in the queue.
	setDDLJobQuery(ctx, job)
	e.deliverJobTask(jobW)

	failpoint.Inject("mockParallelSameDDLJobTwice", func(val failpoint.Value) {
		if val.(bool) {
			<-jobW.ResultCh[0]
			// The same job will be put to the DDL queue twice.
			job = job.Clone()
			newJobW := NewJobWrapperWithArgs(job, jobW.JobArgs, jobW.IDAllocated)
			e.deliverJobTask(newJobW)
			// The second job result is used for test.
			jobW = newJobW
		}
	})

	// worker should restart to continue handling tasks in limitJobCh, and send back through jobW.err
	result := <-jobW.ResultCh[0]
	// job.ID must be allocated after previous channel receive returns nil.
	jobID, err := result.jobID, result.err
	defer e.delJobDoneCh(jobID)
	if err != nil {
		// The transaction of enqueuing job is failed.
		return errors.Trace(err)
	}
	failpoint.InjectCall("waitJobSubmitted")

	sessVars := ctx.GetSessionVars()
	sessVars.StmtCtx.IsDDLJobInQueue = true

	ddlAction := job.Type
	if result.merged {
		logutil.DDLLogger().Info("DDL job submitted", zap.Int64("job_id", jobID), zap.String("query", job.Query), zap.String("merged", "true"))
	} else {
		logutil.DDLLogger().Info("DDL job submitted", zap.Stringer("job", job), zap.String("query", job.Query))
	}

	// lock tables works on table ID, for some DDLs which changes table ID, we need
	// make sure the session still tracks it.
	// we need add it here to avoid this ddl job was executed successfully but the
	// session was killed before return. The session will release all table locks
	// it holds, if we don't add the new locking table id here, the session may forget
	// to release the new locked table id when this ddl job was executed successfully
	// but the session was killed before return.
	if config.TableLockEnabled() {
		HandleLockTablesOnSuccessSubmit(ctx, jobW)
		defer func() {
			HandleLockTablesOnFinish(ctx, jobW, resErr)
		}()
	}

	var historyJob *model.Job

	// Attach the context of the jobId to the calling session so that
	// KILL can cancel this DDL job.
	ctx.GetSessionVars().StmtCtx.DDLJobID = jobID

	// For a job from start to end, the state of it will be none -> delete only -> write only -> reorganization -> public
	// For every state changes, we will wait as lease 2 * lease time, so here the ticker check is 10 * lease.
	// But we use etcd to speed up, normally it takes less than 0.5s now, so we use 0.5s or 1s or 3s as the max value.
	initInterval, _ := getJobCheckInterval(ddlAction, 0)
	ticker := time.NewTicker(chooseLeaseTime(10*e.lease, initInterval))
	startTime := time.Now()
	metrics.JobsGauge.WithLabelValues(ddlAction.String()).Inc()
	defer func() {
		ticker.Stop()
		metrics.JobsGauge.WithLabelValues(ddlAction.String()).Dec()
		metrics.HandleJobHistogram.WithLabelValues(ddlAction.String(), metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
		recordLastDDLInfo(ctx, historyJob)
	}()
	i := 0
	notifyCh, _ := e.getJobDoneCh(jobID)
	for {
		failpoint.InjectCall("storeCloseInLoop")
		select {
		case _, ok := <-notifyCh:
			if !ok {
				// when fast create enabled, jobs might be merged, and we broadcast
				// the result by closing the channel, to avoid this loop keeps running
				// without sleeping on retryable error, we set it to nil.
				notifyCh = nil
			}
		case <-ticker.C:
			i++
			ticker = updateTickerInterval(ticker, 10*e.lease, ddlAction, i)
		case <-e.ctx.Done():
			logutil.DDLLogger().Info("DoDDLJob will quit because context done")
			return context.Canceled
		}

		// If the connection being killed, we need to CANCEL the DDL job.
		if sessVars.SQLKiller.HandleSignal() == exeerrors.ErrQueryInterrupted {
			if atomic.LoadInt32(&sessVars.ConnectionStatus) == variable.ConnStatusShutdown {
				logutil.DDLLogger().Info("DoDDLJob will quit because context done")
				return context.Canceled
			}
			if sessVars.StmtCtx.DDLJobID != 0 {
				se, err := e.sessPool.Get()
				if err != nil {
					logutil.DDLLogger().Error("get session failed, check again", zap.Error(err))
					continue
				}
				sessVars.StmtCtx.DDLJobID = 0 // Avoid repeat.
				errs, err := CancelJobsBySystem(se, []int64{jobID})
				e.sessPool.Put(se)
				if len(errs) > 0 {
					logutil.DDLLogger().Warn("error canceling DDL job", zap.Error(errs[0]))
				}
				if err != nil {
					logutil.DDLLogger().Warn("Kill command could not cancel DDL job", zap.Error(err))
					continue
				}
			}
		}

		se, err := e.sessPool.Get()
		if err != nil {
			logutil.DDLLogger().Error("get session failed, check again", zap.Error(err))
			continue
		}
		historyJob, err = GetHistoryJobByID(se, jobID)
		e.sessPool.Put(se)
		if err != nil {
			logutil.DDLLogger().Error("get history DDL job failed, check again", zap.Error(err))
			continue
		}
		if historyJob == nil {
			logutil.DDLLogger().Debug("DDL job is not in history, maybe not run", zap.Int64("jobID", jobID))
			continue
		}

		e.checkHistoryJobInTest(ctx, historyJob)

		// If a job is a history job, the state must be JobStateSynced or JobStateRollbackDone or JobStateCancelled.
		if historyJob.IsSynced() {
			// Judge whether there are some warnings when executing DDL under the certain SQL mode.
			if historyJob.ReorgMeta != nil && len(historyJob.ReorgMeta.Warnings) != 0 {
				if len(historyJob.ReorgMeta.Warnings) != len(historyJob.ReorgMeta.WarningsCount) {
					logutil.DDLLogger().Info("DDL warnings doesn't match the warnings count", zap.Int64("jobID", jobID))
				} else {
					for key, warning := range historyJob.ReorgMeta.Warnings {
						keyCount := historyJob.ReorgMeta.WarningsCount[key]
						if keyCount == 1 {
							ctx.GetSessionVars().StmtCtx.AppendWarning(warning)
						} else {
							newMsg := fmt.Sprintf("%d warnings with this error code, first warning: "+warning.GetMsg(), keyCount)
							newWarning := dbterror.ClassTypes.Synthesize(terror.ErrCode(warning.Code()), newMsg)
							ctx.GetSessionVars().StmtCtx.AppendWarning(newWarning)
						}
					}
				}
			}
			appendMultiChangeWarningsToOwnerCtx(ctx, historyJob)

			logutil.DDLLogger().Info("DDL job is finished", zap.Int64("jobID", jobID))
			return nil
		}

		if historyJob.Error != nil {
			logutil.DDLLogger().Info("DDL job is failed", zap.Int64("jobID", jobID))
			return errors.Trace(historyJob.Error)
		}
		panic("When the state is JobStateRollbackDone or JobStateCancelled, historyJob.Error should never be nil")
	}
}

func getRenameTableUniqueIDs(jobW *JobWrapper, schema bool) []int64 {
	if !schema {
		return []int64{jobW.TableID}
	}

	oldSchemaID := jobW.JobArgs.(*model.RenameTableArgs).OldSchemaID
	return []int64{oldSchemaID, jobW.SchemaID}
}

// HandleLockTablesOnSuccessSubmit handles the table lock for the job which is submitted
// successfully. exported for testing purpose.
func HandleLockTablesOnSuccessSubmit(ctx sessionctx.Context, jobW *JobWrapper) {
	if jobW.Type == model.ActionTruncateTable {
		if ok, lockTp := ctx.CheckTableLocked(jobW.TableID); ok {
			ctx.AddTableLock([]model.TableLockTpInfo{{
				SchemaID: jobW.SchemaID,
				TableID:  jobW.JobArgs.(*model.TruncateTableArgs).NewTableID,
				Tp:       lockTp,
			}})
		}
	}
}

// HandleLockTablesOnFinish handles the table lock for the job which is finished.
// exported for testing purpose.
func HandleLockTablesOnFinish(ctx sessionctx.Context, jobW *JobWrapper, ddlErr error) {
	if jobW.Type == model.ActionTruncateTable {
		if ddlErr != nil {
			newTableID := jobW.JobArgs.(*model.TruncateTableArgs).NewTableID
			ctx.ReleaseTableLockByTableIDs([]int64{newTableID})
			return
		}
		if ok, _ := ctx.CheckTableLocked(jobW.TableID); ok {
			ctx.ReleaseTableLockByTableIDs([]int64{jobW.TableID})
		}
	}
}

func (e *executor) getJobDoneCh(jobID int64) (chan struct{}, bool) {
	return e.ddlJobDoneChMap.Load(jobID)
}

func (e *executor) delJobDoneCh(jobID int64) {
	e.ddlJobDoneChMap.Delete(jobID)
}

func (e *executor) deliverJobTask(task *JobWrapper) {
	// TODO this might block forever, as the consumer part considers context cancel.
	e.limitJobCh <- task
}

func updateTickerInterval(ticker *time.Ticker, lease time.Duration, action model.ActionType, i int) *time.Ticker {
	interval, changed := getJobCheckInterval(action, i)
	if !changed {
		return ticker
	}
	// For now we should stop old ticker and create a new ticker
	ticker.Stop()
	return time.NewTicker(chooseLeaseTime(lease, interval))
}

func recordLastDDLInfo(ctx sessionctx.Context, job *model.Job) {
	if job == nil {
		return
	}
	ctx.GetSessionVars().LastDDLInfo.Query = job.Query
	ctx.GetSessionVars().LastDDLInfo.SeqNum = job.SeqNum
}

func setDDLJobQuery(ctx sessionctx.Context, job *model.Job) {
	switch job.Type {
	case model.ActionUpdateTiFlashReplicaStatus, model.ActionUnlockTable:
		job.Query = ""
	default:
		job.Query, _ = ctx.Value(sessionctx.QueryString).(string)
	}
}

var (
	fastDDLIntervalPolicy = []time.Duration{
		500 * time.Millisecond,
	}
	normalDDLIntervalPolicy = []time.Duration{
		500 * time.Millisecond,
		500 * time.Millisecond,
		1 * time.Second,
	}
	slowDDLIntervalPolicy = []time.Duration{
		500 * time.Millisecond,
		500 * time.Millisecond,
		1 * time.Second,
		1 * time.Second,
		3 * time.Second,
	}
)

func getIntervalFromPolicy(policy []time.Duration, i int) (time.Duration, bool) {
	plen := len(policy)
	if i < plen {
		return policy[i], true
	}
	return policy[plen-1], false
}

func getJobCheckInterval(action model.ActionType, i int) (time.Duration, bool) {
	switch action {
	case model.ActionAddIndex, model.ActionAddPrimaryKey, model.ActionModifyColumn,
		model.ActionReorganizePartition,
		model.ActionRemovePartitioning,
		model.ActionAlterTablePartitioning:
		return getIntervalFromPolicy(slowDDLIntervalPolicy, i)
	case model.ActionCreateTable, model.ActionCreateSchema:
		return getIntervalFromPolicy(fastDDLIntervalPolicy, i)
	default:
		return getIntervalFromPolicy(normalDDLIntervalPolicy, i)
	}
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
