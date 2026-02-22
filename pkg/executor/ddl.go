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

package executor

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/sessiontxn/staleread"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/temptable"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/gcutil"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

// DDLExec represents a DDL executor.
// It grabs a DDL instance from Domain, calling the DDL methods to do the work.
type DDLExec struct {
	exec.BaseExecutor

	ddlExecutor  ddl.Executor
	stmt         ast.StmtNode
	is           infoschema.InfoSchema
	tempTableDDL temptable.TemporaryTableDDL
	done         bool
}

// toErr converts the error to the ErrInfoSchemaChanged when the schema is outdated.
func (e *DDLExec) toErr(err error) error {
	// The err may be cause by schema changed, here we distinguish the ErrInfoSchemaChanged error from other errors.
	dom := domain.GetDomain(e.Ctx())
	checker := domain.NewSchemaChecker(dom, e.is.SchemaMetaVersion(), nil, true)
	txn, err1 := e.Ctx().Txn(true)
	if err1 != nil {
		logutil.BgLogger().Error("active txn failed", zap.Error(err1))
		return err
	}
	_, schemaInfoErr := checker.Check(txn.StartTS())
	if schemaInfoErr != nil {
		return errors.Trace(schemaInfoErr)
	}
	return err
}

func (e *DDLExec) getLocalTemporaryTable(schema pmodel.CIStr, table pmodel.CIStr) (table.Table, bool) {
	tbl, err := e.Ctx().GetInfoSchema().(infoschema.InfoSchema).TableByName(context.Background(), schema, table)
	if infoschema.ErrTableNotExists.Equal(err) {
		return nil, false
	}

	if tbl.Meta().TempTableType != model.TempTableLocal {
		return nil, false
	}

	return tbl, true
}

// RefreshMaterializedViewExec executes "REFRESH MATERIALIZED VIEW" as a utility-style statement.
type RefreshMaterializedViewExec struct {
	DDLExec
}

// Next implements the Executor Next interface.
func (e *RefreshMaterializedViewExec) Next(ctx context.Context, _ *chunk.Chunk) (err error) {
	if e.done {
		return nil
	}
	e.done = true

	refreshStmt, ok := e.stmt.(*ast.RefreshMaterializedViewStmt)
	if !ok {
		return errors.Errorf("invalid statement type for RefreshMaterializedViewExec: %T", e.stmt)
	}

	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMVMaintenance)
	if err = sessiontxn.NewTxnInStmt(ctx, e.Ctx()); err != nil {
		return err
	}
	defer func() {
		e.Ctx().GetSessionVars().StmtCtx.IsDDLJobInQueue = false
		e.Ctx().GetSessionVars().StmtCtx.DDLJobID = 0
	}()

	err = e.executeRefreshMaterializedView(ctx, refreshStmt)
	if err != nil {
		if (e.Ctx().GetSessionVars().StmtCtx.IsDDLJobInQueue && infoschema.ErrTableNotExists.Equal(err)) ||
			!e.Ctx().GetSessionVars().StmtCtx.IsDDLJobInQueue {
			return e.toErr(err)
		}
		return err
	}

	// Keep the post-exec behavior consistent with DDLExec.
	dom := domain.GetDomain(e.Ctx())
	is := dom.InfoSchema()
	txnCtx := e.Ctx().GetSessionVars().TxnCtx
	txnCtx.InfoSchema = is
	e.Ctx().GetSessionVars().SetInTxn(false)
	return nil
}

func (e *RefreshMaterializedViewExec) executeRefreshMaterializedView(kctx context.Context, s *ast.RefreshMaterializedViewStmt) error {
	refreshType, err := validateRefreshMaterializedViewStmtForUtility(s)
	if err != nil {
		return err
	}

	schemaName, tblInfo, err := e.resolveRefreshMaterializedViewTarget(s)
	if err != nil {
		return err
	}

	refreshSctx, err := e.GetSysSession()
	if err != nil {
		return err
	}
	defer e.ReleaseSysSession(kctx, refreshSctx)
	sqlExec := refreshSctx.GetSQLExecutor()
	sessVars := refreshSctx.GetSessionVars()

	restoreConstraintCheck, err := forceConstraintCheckInPlacePessimisticOnForRefresh(sessVars)
	if err != nil {
		return err
	}
	defer restoreConstraintCheck()

	txnStarted := false
	txnCommitted := false
	defer func() {
		if !txnStarted || txnCommitted {
			return
		}
		_, _ = sqlExec.ExecuteInternal(kctx, "ROLLBACK")
	}()

	// Use a pessimistic txn to ensure `FOR UPDATE NOWAIT` works as a mutex.
	if _, err := sqlExec.ExecuteInternal(kctx, "BEGIN PESSIMISTIC"); err != nil {
		return errors.Trace(err)
	}
	txnStarted = true

	failpoint.InjectCall("refreshMaterializedViewAfterBegin")
	failpoint.Inject("pauseRefreshMaterializedViewAfterBegin", func() {})

	mviewID := tblInfo.ID
	lockedReadTSO, lockedReadTSONull, persistFailureOnErr, err := lockAndValidateRefreshInfoRowForUtility(kctx, sqlExec, mviewID)
	if err != nil {
		if persistFailureOnErr {
			return persistRefreshFailureAndCommitForUtility(kctx, sqlExec, refreshType, mviewID, err, &txnCommitted)
		}
		return err
	}

	var lastSuccessfulRefreshReadTSO int64
	if s.Type == ast.RefreshMaterializedViewTypeFast {
		// LAST_SUCCESSFUL_REFRESH_READ_TSO is BIGINT DEFAULT NULL. FAST refresh requires it to be non-NULL.
		if lockedReadTSONull {
			return dbterror.ErrInvalidDDLJob.GenWithStackByArgs("refresh materialized view fast: LAST_SUCCESSFUL_REFRESH_READ_TSO is NULL")
		}
		lastSuccessfulRefreshReadTSO = lockedReadTSO
	}

	txn, err := refreshSctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	startTS := txn.StartTS()
	if startTS == 0 {
		return dbterror.ErrInvalidDDLJob.GenWithStackByArgs("refresh materialized view: invalid transaction start tso")
	}

	// Use a savepoint so we can keep the mutex lock, rollback MV data changes on failure,
	// but still commit refresh metadata updates (failed reason) as requested.
	const refreshSavepoint = "tidb_mview_refresh_sp"
	if _, err := sqlExec.ExecuteInternal(kctx, "SAVEPOINT "+refreshSavepoint); err != nil {
		return errors.Trace(err)
	}

	if err := executeRefreshMaterializedViewDataChangesForUtility(
		kctx,
		sqlExec,
		sessVars,
		s,
		schemaName,
		tblInfo,
		lastSuccessfulRefreshReadTSO,
	); err != nil {
		if _, rollbackErr := sqlExec.ExecuteInternal(kctx, "ROLLBACK TO SAVEPOINT "+refreshSavepoint); rollbackErr != nil {
			return errors.Annotatef(rollbackErr, "refresh materialized view: failed to rollback MV data changes after error %v", err)
		}
		return persistRefreshFailureAndCommitForUtility(kctx, sqlExec, refreshType, mviewID, err, &txnCommitted)
	}

	refreshReadTSO, err := getRefreshReadTSOForSuccessForUtility(s.Type, sessVars, startTS)
	if err != nil {
		return err
	}
	return persistRefreshSuccessAndCommitForUtility(kctx, sqlExec, refreshType, mviewID, refreshReadTSO, &txnCommitted)
}

func validateRefreshMaterializedViewStmtForUtility(s *ast.RefreshMaterializedViewStmt) (string, error) {
	if s == nil || s.ViewName == nil {
		return "", dbterror.ErrInvalidDDLJob.GenWithStackByArgs("refresh materialized view: missing view name")
	}
	switch s.Type {
	case ast.RefreshMaterializedViewTypeComplete:
		// supported
	case ast.RefreshMaterializedViewTypeFast:
		// Framework is supported; actual execution happens via RefreshMaterializedViewImplementStmt.
	default:
		return "", dbterror.ErrGeneralUnsupportedDDL.GenWithStack("unknown REFRESH MATERIALIZED VIEW type")
	}
	// In MVP, refresh is synchronous by nature. `WITH SYNC MODE` is accepted and behaves the same.
	return strings.ToLower(s.Type.String()), nil
}

func (e *RefreshMaterializedViewExec) resolveRefreshMaterializedViewTarget(
	s *ast.RefreshMaterializedViewStmt,
) (pmodel.CIStr, *model.TableInfo, error) {
	is := e.Ctx().GetInfoSchema().(infoschema.InfoSchema)
	schemaName := s.ViewName.Schema
	if schemaName.O == "" {
		if e.Ctx().GetSessionVars().CurrentDB == "" {
			return pmodel.CIStr{}, nil, errors.Trace(plannererrors.ErrNoDB)
		}
		schemaName = pmodel.NewCIStr(e.Ctx().GetSessionVars().CurrentDB)
		s.ViewName.Schema = schemaName
	}
	if _, ok := is.SchemaByName(schemaName); !ok {
		return pmodel.CIStr{}, nil, infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schemaName)
	}

	tbl, err := is.TableByName(context.Background(), schemaName, s.ViewName.Name)
	if err != nil {
		return pmodel.CIStr{}, nil, err
	}
	tblInfo := tbl.Meta()
	if tblInfo.MaterializedView == nil {
		return pmodel.CIStr{}, nil, dbterror.ErrWrongObject.GenWithStackByArgs(schemaName.O, s.ViewName.Name.O, "MATERIALIZED VIEW")
	}
	if len(tblInfo.MaterializedView.SQLContent) == 0 {
		return pmodel.CIStr{}, nil, dbterror.ErrInvalidDDLJob.GenWithStackByArgs("refresh materialized view: invalid select sql")
	}
	return schemaName, tblInfo, nil
}

func forceConstraintCheckInPlacePessimisticOnForRefresh(sessVars *variable.SessionVars) (func(), error) {
	// Savepoint is required for transactional refresh-with-failure-record (rollback MV data changes but persist failure info).
	// Savepoint is not supported in pessimistic txn when `tidb_constraint_check_in_place_pessimistic` is OFF, so we
	// force it to ON for the duration of this statement and then restore it.
	oldConstraintCheckInPlacePessimistic, err := sessVars.SetSystemVarWithOldValAsRet(variable.TiDBConstraintCheckInPlacePessimistic, variable.On)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return func() {
		_ = sessVars.SetSystemVar(variable.TiDBConstraintCheckInPlacePessimistic, oldConstraintCheckInPlacePessimistic)
	}, nil
}

func lockAndValidateRefreshInfoRowForUtility(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	mviewID int64,
) (lockedReadTSO int64, lockedReadTSONull bool, persistFailureOnErr bool, err error) {
	lockRS, err := sqlExec.ExecuteInternal(
		kctx,
		// Also select LAST_SUCCESSFUL_REFRESH_READ_TSO so FAST refresh can reuse this mutex/metadata load path.
		"SELECT MVIEW_ID, LAST_SUCCESSFUL_REFRESH_READ_TSO FROM mysql.tidb_mview_refresh WHERE MVIEW_ID = %? FOR UPDATE NOWAIT",
		mviewID,
	)
	if infoschema.ErrTableNotExists.Equal(err) {
		return 0, false, false, dbterror.ErrInvalidDDLJob.GenWithStackByArgs("refresh materialized view: required system table mysql.tidb_mview_refresh does not exist")
	}
	if err != nil {
		return 0, false, false, errors.Trace(err)
	}
	if lockRS == nil {
		return 0, false, false, dbterror.ErrInvalidDDLJob.GenWithStackByArgs("refresh materialized view: cannot lock mysql.tidb_mview_refresh row")
	}
	lockRows, drainErr := sqlexec.DrainRecordSet(kctx, lockRS, 1)
	closeErr := lockRS.Close()
	if drainErr != nil {
		return 0, false, false, errors.Trace(drainErr)
	}
	if closeErr != nil {
		return 0, false, false, errors.Trace(closeErr)
	}
	if len(lockRows) == 0 {
		return 0, false, false, dbterror.ErrInvalidDDLJob.GenWithStackByArgs("refresh materialized view: refresh info row missing in mysql.tidb_mview_refresh")
	}

	// In pessimistic txn, `SELECT ... FOR UPDATE` reads at txn's `for_update_ts`, while normal `SELECT`
	// reads at txn's `start_ts`. Re-check LAST_SUCCESSFUL_REFRESH_READ_TSO using a normal SELECT to
	// ensure the refresh info row is consistent between these 2 read timestamps.
	lockedRow := lockRows[0]
	lockedReadTSONull = lockedRow.IsNull(1)
	if !lockedReadTSONull {
		lockedReadTSO = lockedRow.GetInt64(1)
	}

	recheckRS, err := sqlExec.ExecuteInternal(
		kctx,
		"SELECT LAST_SUCCESSFUL_REFRESH_READ_TSO FROM mysql.tidb_mview_refresh WHERE MVIEW_ID = %?",
		mviewID,
	)
	if err != nil {
		return 0, false, false, errors.Trace(err)
	}
	if recheckRS == nil {
		return 0, false, false, dbterror.ErrInvalidDDLJob.GenWithStackByArgs("refresh materialized view: cannot re-check mysql.tidb_mview_refresh row")
	}
	recheckRows, drainErr := sqlexec.DrainRecordSet(kctx, recheckRS, 1)
	closeErr = recheckRS.Close()
	if drainErr != nil {
		return 0, false, false, errors.Trace(drainErr)
	}
	if closeErr != nil {
		return 0, false, false, errors.Trace(closeErr)
	}
	if len(recheckRows) == 0 {
		return 0, false, false, dbterror.ErrInvalidDDLJob.GenWithStackByArgs("refresh materialized view: refresh info row missing in mysql.tidb_mview_refresh")
	}
	recheckRow := recheckRows[0]
	recheckReadTSONull := recheckRow.IsNull(0)
	var recheckReadTSO int64
	if !recheckReadTSONull {
		recheckReadTSO = recheckRow.GetInt64(0)
	}
	if lockedReadTSONull != recheckReadTSONull || (!lockedReadTSONull && lockedReadTSO != recheckReadTSO) {
		return 0, false, true, dbterror.ErrInvalidDDLJob.GenWithStackByArgs("refresh materialized view: inconsistent LAST_SUCCESSFUL_REFRESH_READ_TSO between locking read and snapshot read")
	}
	return lockedReadTSO, lockedReadTSONull, false, nil
}

func executeRefreshMaterializedViewDataChangesForUtility(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	sessVars *variable.SessionVars,
	s *ast.RefreshMaterializedViewStmt,
	schemaName pmodel.CIStr,
	tblInfo *model.TableInfo,
	lastSuccessfulRefreshReadTSO int64,
) error {
	// TiFlash read is blocked for write statements when sql_mode is strict. Refresh prefers TiFlash for the
	// scan part, so we bypass this guard for MV maintenance statements.
	origInMaterializedViewMaintenance := sessVars.InMaterializedViewMaintenance
	sessVars.InMaterializedViewMaintenance = true
	defer func() {
		sessVars.InMaterializedViewMaintenance = origInMaterializedViewMaintenance
	}()

	switch s.Type {
	case ast.RefreshMaterializedViewTypeComplete:
		deleteSQL := sqlescape.MustEscapeSQL("DELETE FROM %n.%n", schemaName.O, s.ViewName.Name.O)
		insertPrefix := sqlescape.MustEscapeSQL("INSERT INTO %n.%n ", schemaName.O, s.ViewName.Name.O)
		/* #nosec G202: SQLContent is restored from AST (single SELECT statement, no user-provided placeholders). */
		insertSQL := insertPrefix + tblInfo.MaterializedView.SQLContent
		if _, err := sqlExec.ExecuteInternal(kctx, deleteSQL); err != nil {
			return err
		}
		if _, err := sqlExec.ExecuteInternal(kctx, insertSQL); err != nil {
			return err
		}
		return nil
	case ast.RefreshMaterializedViewTypeFast:
		implementStmt := &ast.RefreshMaterializedViewImplementStmt{
			RefreshStmt:                  s,
			LastSuccessfulRefreshReadTSO: lastSuccessfulRefreshReadTSO,
		}
		return executeFastRefreshImplementStmtForUtility(kctx, sqlExec, sessVars, implementStmt)
	default:
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStack("unknown REFRESH MATERIALIZED VIEW type")
	}
}

func executeFastRefreshImplementStmtForUtility(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	sessVars *variable.SessionVars,
	implementStmt *ast.RefreshMaterializedViewImplementStmt,
) error {
	if internalExec, ok := sqlExec.(interface {
		ExecuteInternalStmt(context.Context, ast.StmtNode) (sqlexec.RecordSet, error)
	}); ok {
		rs, execErr := internalExec.ExecuteInternalStmt(kctx, implementStmt)
		return drainAndCloseRefreshRecordSetForUtility(kctx, rs, execErr)
	}

	// Fallback: emulate ExecuteInternalStmt by flipping InRestrictedSQL around ExecuteStmt.
	origRestricted := sessVars.InRestrictedSQL
	sessVars.InRestrictedSQL = true
	defer func() {
		sessVars.InRestrictedSQL = origRestricted
	}()
	rs, execErr := sqlExec.ExecuteStmt(kctx, implementStmt)
	return drainAndCloseRefreshRecordSetForUtility(kctx, rs, execErr)
}

func drainAndCloseRefreshRecordSetForUtility(
	kctx context.Context,
	rs sqlexec.RecordSet,
	execErr error,
) error {
	if rs == nil {
		return execErr
	}
	if execErr == nil {
		if drainErr := drainRefreshRecordSetForUtility(kctx, rs); drainErr != nil {
			_ = rs.Close()
			return errors.Trace(drainErr)
		}
	}
	if closeErr := rs.Close(); closeErr != nil && execErr == nil {
		return errors.Trace(closeErr)
	}
	return execErr
}

func drainRefreshRecordSetForUtility(kctx context.Context, rs sqlexec.RecordSet) error {
	chk := rs.NewChunk(nil)
	for {
		chk.Reset()
		if err := rs.Next(kctx, chk); err != nil {
			return err
		}
		if chk.NumRows() == 0 {
			return nil
		}
	}
}

func persistRefreshFailureAndCommitForUtility(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	refreshType string,
	mviewID int64,
	refreshErr error,
	txnCommitted *bool,
) error {
	updateFailedSQL := `UPDATE mysql.tidb_mview_refresh
SET
	LAST_REFRESH_RESULT = 'failed',
	LAST_REFRESH_TYPE = %?,
	LAST_REFRESH_TIME = NOW(6),
	LAST_REFRESH_FAILED_REASON = %?
WHERE MVIEW_ID = %?`
	if _, err := sqlExec.ExecuteInternal(kctx, updateFailedSQL, refreshType, refreshErr.Error(), mviewID); err != nil {
		if infoschema.ErrTableNotExists.Equal(err) {
			return dbterror.ErrInvalidDDLJob.GenWithStackByArgs("refresh materialized view: required system table mysql.tidb_mview_refresh does not exist")
		}
		return errors.Annotatef(err, "refresh materialized view: failed to persist refresh failure info (original error: %v)", refreshErr)
	}
	if _, err := sqlExec.ExecuteInternal(kctx, "COMMIT"); err != nil {
		return errors.Trace(err)
	}
	*txnCommitted = true
	return errors.Trace(refreshErr)
}

func getRefreshReadTSOForSuccessForUtility(
	refreshType ast.RefreshMaterializedViewType,
	sessVars *variable.SessionVars,
	startTS uint64,
) (uint64, error) {
	// COMPLETE refresh uses `DELETE + INSERT INTO ... SELECT ...` and the SELECT part reads at txn's
	// `for_update_ts` in pessimistic txn, so record `for_update_ts` to ensure
	// LAST_SUCCESSFUL_REFRESH_READ_TSO matches the MV data snapshot.
	//
	// For FAST refresh, the actual execution is not implemented yet; keep the original behavior and
	// record txn start_ts when it succeeds in the future.
	refreshReadTSO := startTS
	if refreshType == ast.RefreshMaterializedViewTypeComplete {
		refreshReadTSO = sessVars.TxnCtx.GetForUpdateTS()
		if refreshReadTSO == 0 {
			return 0, dbterror.ErrInvalidDDLJob.GenWithStackByArgs("refresh materialized view: invalid refresh read tso")
		}
	}
	return refreshReadTSO, nil
}

func persistRefreshSuccessAndCommitForUtility(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	refreshType string,
	mviewID int64,
	refreshReadTSO uint64,
	txnCommitted *bool,
) error {
	updateSQL := `UPDATE mysql.tidb_mview_refresh
SET
	LAST_REFRESH_RESULT = 'success',
	LAST_REFRESH_TYPE = %?,
	LAST_REFRESH_TIME = NOW(6),
	LAST_SUCCESSFUL_REFRESH_READ_TSO = %?,
	LAST_REFRESH_FAILED_REASON = NULL
WHERE MVIEW_ID = %?`
	if _, err := sqlExec.ExecuteInternal(kctx, updateSQL, refreshType, refreshReadTSO, mviewID); err != nil {
		if infoschema.ErrTableNotExists.Equal(err) {
			return dbterror.ErrInvalidDDLJob.GenWithStackByArgs("refresh materialized view: required system table mysql.tidb_mview_refresh does not exist")
		}
		return errors.Trace(err)
	}
	if _, err := sqlExec.ExecuteInternal(kctx, "COMMIT"); err != nil {
		return errors.Trace(err)
	}
	*txnCommitted = true
	return nil
}

// Next implements the Executor Next interface.
func (e *DDLExec) Next(ctx context.Context, _ *chunk.Chunk) (err error) {
	if e.done {
		return nil
	}
	e.done = true

	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnDDL)
	// For each DDL, we should commit the previous transaction and create a new transaction.
	// Following cases are exceptions
	var localTempTablesToDrop []*ast.TableName
	switch s := e.stmt.(type) {
	case *ast.CreateTableStmt:
		if s.TemporaryKeyword == ast.TemporaryLocal {
			return e.createSessionTemporaryTable(s)
		}
	case *ast.DropTableStmt:
		if s.IsView {
			break
		}

		for tbIdx := len(s.Tables) - 1; tbIdx >= 0; tbIdx-- {
			if _, ok := e.getLocalTemporaryTable(s.Tables[tbIdx].Schema, s.Tables[tbIdx].Name); ok {
				localTempTablesToDrop = append(localTempTablesToDrop, s.Tables[tbIdx])
				s.Tables = append(s.Tables[:tbIdx], s.Tables[tbIdx+1:]...)
			}
		}

		// Statement `DROP TEMPORARY TABLE ...` should not have non-local temporary tables
		if s.TemporaryKeyword == ast.TemporaryLocal && len(s.Tables) > 0 {
			nonExistsTables := make([]string, 0, len(s.Tables))
			for _, tn := range s.Tables {
				nonExistsTables = append(nonExistsTables, ast.Ident{Schema: tn.Schema, Name: tn.Name}.String())
			}
			// stackless err once used like note.
			err = infoschema.ErrTableDropExists.FastGenByArgs(strings.Join(nonExistsTables, ","))
			if s.IfExists {
				e.Ctx().GetSessionVars().StmtCtx.AppendNote(err)
				return nil
			}
			// complete and trace stack info.
			return errors.Trace(err)
		}

		// if all tables are local temporary, directly drop those tables.
		if len(s.Tables) == 0 {
			return e.dropLocalTemporaryTables(localTempTablesToDrop)
		}
	}

	if err = sessiontxn.NewTxnInStmt(ctx, e.Ctx()); err != nil {
		return err
	}

	defer func() {
		e.Ctx().GetSessionVars().StmtCtx.IsDDLJobInQueue = false
		e.Ctx().GetSessionVars().StmtCtx.DDLJobID = 0
	}()

	switch x := e.stmt.(type) {
	case *ast.AlterDatabaseStmt:
		err = e.executeAlterDatabase(x)
	case *ast.AlterTableStmt:
		err = e.executeAlterTable(ctx, x)
	case *ast.CreateIndexStmt:
		err = e.executeCreateIndex(x)
	case *ast.CreateDatabaseStmt:
		err = e.executeCreateDatabase(x)
	case *ast.FlashBackDatabaseStmt:
		err = e.executeFlashbackDatabase(x)
	case *ast.CreateTableStmt:
		err = e.executeCreateTable(x)
	case *ast.CreateViewStmt:
		err = e.executeCreateView(ctx, x)
	case *ast.CreateMaterializedViewStmt:
		err = e.ddlExecutor.CreateMaterializedView(e.Ctx(), x)
	case *ast.CreateMaterializedViewLogStmt:
		err = e.ddlExecutor.CreateMaterializedViewLog(e.Ctx(), x)
	case *ast.AlterMaterializedViewStmt:
		err = e.ddlExecutor.AlterMaterializedView(e.Ctx(), x)
	case *ast.AlterMaterializedViewLogStmt:
		err = e.ddlExecutor.AlterMaterializedViewLog(e.Ctx(), x)
	case *ast.DropIndexStmt:
		err = e.executeDropIndex(x)
	case *ast.DropDatabaseStmt:
		err = e.executeDropDatabase(x)
	case *ast.DropTableStmt:
		if x.IsView {
			err = e.executeDropView(x)
		} else {
			err = e.executeDropTable(x)
			if err == nil {
				err = e.dropLocalTemporaryTables(localTempTablesToDrop)
			}
		}
	case *ast.RecoverTableStmt:
		err = e.executeRecoverTable(x)
	case *ast.FlashBackTableStmt:
		err = e.executeFlashbackTable(x)
	case *ast.FlashBackToTimestampStmt:
		if len(x.Tables) != 0 {
			err = dbterror.ErrGeneralUnsupportedDDL.GenWithStack("Unsupported FLASHBACK table TO TIMESTAMP")
		} else if x.DBName.O != "" {
			err = dbterror.ErrGeneralUnsupportedDDL.GenWithStack("Unsupported FLASHBACK database TO TIMESTAMP")
		} else {
			err = e.executeFlashBackCluster(x)
		}
	case *ast.RenameTableStmt:
		err = e.executeRenameTable(x)
	case *ast.TruncateTableStmt:
		err = e.executeTruncateTable(x)
	case *ast.LockTablesStmt:
		err = e.executeLockTables(x)
	case *ast.UnlockTablesStmt:
		err = e.executeUnlockTables(x)
	case *ast.CleanupTableLockStmt:
		err = e.executeCleanupTableLock(x)
	case *ast.RepairTableStmt:
		err = e.executeRepairTable(x)
	case *ast.CreateSequenceStmt:
		err = e.executeCreateSequence(x)
	case *ast.DropSequenceStmt:
		err = e.executeDropSequence(x)
	case *ast.DropMaterializedViewStmt:
		err = e.ddlExecutor.DropMaterializedView(e.Ctx(), x)
	case *ast.DropMaterializedViewLogStmt:
		err = e.ddlExecutor.DropMaterializedViewLog(e.Ctx(), x)
	case *ast.AlterSequenceStmt:
		err = e.executeAlterSequence(x)
	case *ast.CreatePlacementPolicyStmt:
		err = e.executeCreatePlacementPolicy(x)
	case *ast.DropPlacementPolicyStmt:
		err = e.executeDropPlacementPolicy(x)
	case *ast.AlterPlacementPolicyStmt:
		err = e.executeAlterPlacementPolicy(x)
	case *ast.CreateResourceGroupStmt:
		err = e.executeCreateResourceGroup(x)
	case *ast.DropResourceGroupStmt:
		err = e.executeDropResourceGroup(x)
	case *ast.AlterResourceGroupStmt:
		err = e.executeAlterResourceGroup(x)
	}
	if err != nil {
		// If the owner return ErrTableNotExists error when running this DDL, it may be caused by schema changed,
		// otherwise, ErrTableNotExists can be returned before putting this DDL job to the job queue.
		if (e.Ctx().GetSessionVars().StmtCtx.IsDDLJobInQueue && infoschema.ErrTableNotExists.Equal(err)) ||
			!e.Ctx().GetSessionVars().StmtCtx.IsDDLJobInQueue {
			return e.toErr(err)
		}
		return err
	}

	dom := domain.GetDomain(e.Ctx())
	// Update InfoSchema in TxnCtx, so it will pass schema check.
	is := dom.InfoSchema()
	txnCtx := e.Ctx().GetSessionVars().TxnCtx
	txnCtx.InfoSchema = is
	// DDL will force commit old transaction, after DDL, in transaction status should be false.
	e.Ctx().GetSessionVars().SetInTxn(false)
	return nil
}

func (e *DDLExec) executeTruncateTable(s *ast.TruncateTableStmt) error {
	ident := ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name}
	if _, exist := e.getLocalTemporaryTable(s.Table.Schema, s.Table.Name); exist {
		return e.tempTableDDL.TruncateLocalTemporaryTable(s.Table.Schema, s.Table.Name)
	}
	err := e.ddlExecutor.TruncateTable(e.Ctx(), ident)
	return err
}

func (e *DDLExec) executeRenameTable(s *ast.RenameTableStmt) error {
	for _, tables := range s.TableToTables {
		if _, ok := e.getLocalTemporaryTable(tables.OldTable.Schema, tables.OldTable.Name); ok {
			return dbterror.ErrUnsupportedLocalTempTableDDL.GenWithStackByArgs("RENAME TABLE")
		}
	}
	return e.ddlExecutor.RenameTable(e.Ctx(), s)
}

func (e *DDLExec) executeCreateDatabase(s *ast.CreateDatabaseStmt) error {
	err := e.ddlExecutor.CreateSchema(e.Ctx(), s)
	return err
}

func (e *DDLExec) executeAlterDatabase(s *ast.AlterDatabaseStmt) error {
	err := e.ddlExecutor.AlterSchema(e.Ctx(), s)
	return err
}

func (e *DDLExec) executeCreateTable(s *ast.CreateTableStmt) error {
	err := e.ddlExecutor.CreateTable(e.Ctx(), s)
	return err
}

func (e *DDLExec) createSessionTemporaryTable(s *ast.CreateTableStmt) error {
	is := e.Ctx().GetInfoSchema().(infoschema.InfoSchema)
	dbInfo, ok := is.SchemaByName(s.Table.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(s.Table.Schema.O)
	}

	_, exists := e.getLocalTemporaryTable(s.Table.Schema, s.Table.Name)
	if exists {
		err := infoschema.ErrTableExists.FastGenByArgs(ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name})
		if s.IfNotExists {
			e.Ctx().GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return errors.Trace(err)
	}

	tbInfo, err := ddl.BuildSessionTemporaryTableInfo(ddl.NewMetaBuildContextWithSctx(e.Ctx()), e.Ctx().GetStore(), is, s,
		dbInfo.Charset, dbInfo.Collate, dbInfo.PlacementPolicyRef)
	if err != nil {
		return err
	}

	if err = e.tempTableDDL.CreateLocalTemporaryTable(dbInfo, tbInfo); err != nil {
		return err
	}

	sessiontxn.GetTxnManager(e.Ctx()).OnLocalTemporaryTableCreated()
	return nil
}

func (e *DDLExec) executeCreateView(ctx context.Context, s *ast.CreateViewStmt) error {
	ret := &core.PreprocessorReturn{}
	nodeW := resolve.NewNodeW(s.Select)
	err := core.Preprocess(ctx, e.Ctx(), nodeW, core.WithPreprocessorReturn(ret))
	if err != nil {
		return errors.Trace(err)
	}
	if ret.IsStaleness {
		return exeerrors.ErrViewInvalid.GenWithStackByArgs(s.ViewName.Schema.L, s.ViewName.Name.L)
	}

	e.Ctx().GetSessionVars().ClearRelatedTableForMDL()
	return e.ddlExecutor.CreateView(e.Ctx(), s)
}

func (e *DDLExec) executeCreateIndex(s *ast.CreateIndexStmt) error {
	if _, ok := e.getLocalTemporaryTable(s.Table.Schema, s.Table.Name); ok {
		return dbterror.ErrUnsupportedLocalTempTableDDL.GenWithStackByArgs("CREATE INDEX")
	}

	return e.ddlExecutor.CreateIndex(e.Ctx(), s)
}

func (e *DDLExec) executeDropDatabase(s *ast.DropDatabaseStmt) error {
	dbName := s.Name

	// Protect important system table from been dropped by a mistake.
	// I can hardly find a case that a user really need to do this.
	if dbName.L == "mysql" {
		return errors.New("Drop 'mysql' database is forbidden")
	}

	err := e.ddlExecutor.DropSchema(e.Ctx(), s)
	sessionVars := e.Ctx().GetSessionVars()
	if err == nil && strings.ToLower(sessionVars.CurrentDB) == dbName.L {
		sessionVars.CurrentDB = ""
		err = sessionVars.SetSystemVar(variable.CharsetDatabase, mysql.DefaultCharset)
		if err != nil {
			return err
		}
		err = sessionVars.SetSystemVar(variable.CollationDatabase, mysql.DefaultCollationName)
		if err != nil {
			return err
		}
	}
	return err
}

func (e *DDLExec) executeDropTable(s *ast.DropTableStmt) error {
	return e.ddlExecutor.DropTable(e.Ctx(), s)
}

func (e *DDLExec) executeDropView(s *ast.DropTableStmt) error {
	return e.ddlExecutor.DropView(e.Ctx(), s)
}

func (e *DDLExec) executeDropSequence(s *ast.DropSequenceStmt) error {
	return e.ddlExecutor.DropSequence(e.Ctx(), s)
}

func (e *DDLExec) dropLocalTemporaryTables(localTempTables []*ast.TableName) error {
	if len(localTempTables) == 0 {
		return nil
	}

	for _, tb := range localTempTables {
		err := e.tempTableDDL.DropLocalTemporaryTable(tb.Schema, tb.Name)
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *DDLExec) executeDropIndex(s *ast.DropIndexStmt) error {
	if _, ok := e.getLocalTemporaryTable(s.Table.Schema, s.Table.Name); ok {
		return dbterror.ErrUnsupportedLocalTempTableDDL.GenWithStackByArgs("DROP INDEX")
	}

	return e.ddlExecutor.DropIndex(e.Ctx(), s)
}

func (e *DDLExec) executeAlterTable(ctx context.Context, s *ast.AlterTableStmt) error {
	if _, ok := e.getLocalTemporaryTable(s.Table.Schema, s.Table.Name); ok {
		return dbterror.ErrUnsupportedLocalTempTableDDL.GenWithStackByArgs("ALTER TABLE")
	}

	return e.ddlExecutor.AlterTable(ctx, e.Ctx(), s)
}

// executeRecoverTable represents a recover table executor.
// It is built from "recover table" statement,
// is used to recover the table that deleted by mistake.
func (e *DDLExec) executeRecoverTable(s *ast.RecoverTableStmt) error {
	dom := domain.GetDomain(e.Ctx())
	var job *model.Job
	var err error
	var tblInfo *model.TableInfo
	// Let check table first. Related isssue #46296.
	if s.Table != nil {
		job, tblInfo, err = e.getRecoverTableByTableName(s.Table)
	} else {
		job, tblInfo, err = e.getRecoverTableByJobID(s, dom)
	}
	if err != nil {
		return err
	}
	// Check the table ID was not exists.
	tbl, ok := dom.InfoSchema().TableByID(context.Background(), tblInfo.ID)
	if ok {
		return infoschema.ErrTableExists.GenWithStack("Table '%-.192s' already been recover to '%-.192s', can't be recover repeatedly", tblInfo.Name.O, tbl.Meta().Name.O)
	}

	m := domain.GetDomain(e.Ctx()).GetSnapshotMeta(job.StartTS)
	autoIDs, err := m.GetAutoIDAccessors(job.SchemaID, job.TableID).Get()
	if err != nil {
		return err
	}

	recoverInfo := &model.RecoverTableInfo{
		SchemaID:      job.SchemaID,
		TableInfo:     tblInfo,
		DropJobID:     job.ID,
		SnapshotTS:    job.StartTS,
		AutoIDs:       autoIDs,
		OldSchemaName: job.SchemaName,
		OldTableName:  tblInfo.Name.L,
	}
	// Call DDL RecoverTable.
	err = e.ddlExecutor.RecoverTable(e.Ctx(), recoverInfo)
	return err
}

func (e *DDLExec) getRecoverTableByJobID(s *ast.RecoverTableStmt, dom *domain.Domain) (*model.Job, *model.TableInfo, error) {
	se, err := e.GetSysSession()
	if err != nil {
		return nil, nil, err
	}
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	defer e.ReleaseSysSession(ctx, se)
	job, err := ddl.GetHistoryJobByID(se, s.JobID)
	if err != nil {
		return nil, nil, err
	}
	if job == nil {
		return nil, nil, dbterror.ErrDDLJobNotFound.GenWithStackByArgs(s.JobID)
	}
	if job.Type != model.ActionDropTable && job.Type != model.ActionTruncateTable {
		return nil, nil, errors.Errorf("Job %v type is %v, not dropped/truncated table", job.ID, job.Type)
	}

	// Check GC safe point for getting snapshot infoSchema.
	err = gcutil.ValidateSnapshot(e.Ctx(), job.StartTS)
	if err != nil {
		return nil, nil, err
	}

	// Get the snapshot infoSchema before drop table.
	snapInfo, err := dom.GetSnapshotInfoSchema(job.StartTS)
	if err != nil {
		return nil, nil, err
	}
	// Get table meta from snapshot infoSchema.
	table, ok := snapInfo.TableByID(ctx, job.TableID)
	if !ok {
		return nil, nil, infoschema.ErrTableNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", job.SchemaID),
			fmt.Sprintf("(Table ID %d)", job.TableID),
		)
	}
	// We can't return the meta directly since it will be modified outside, which may corrupt the infocache.
	// Since only State field is changed, return a shallow copy is enough.
	// see https://github.com/pingcap/tidb/issues/55462
	tblInfo := *table.Meta()
	return job, &tblInfo, nil
}

// GetDropOrTruncateTableInfoFromJobs gets the dropped/truncated table information from DDL jobs,
// it will use the `start_ts` of DDL job as snapshot to get the dropped/truncated table information.
func GetDropOrTruncateTableInfoFromJobs(jobs []*model.Job, gcSafePoint uint64, dom *domain.Domain, fn func(*model.Job, *model.TableInfo) (bool, error)) (bool, error) {
	getTable := func(startTS uint64, schemaID int64, tableID int64) (*model.TableInfo, error) {
		snapMeta := dom.GetSnapshotMeta(startTS)
		tbl, err := snapMeta.GetTable(schemaID, tableID)
		return tbl, err
	}
	return ddl.GetDropOrTruncateTableInfoFromJobsByStore(jobs, gcSafePoint, getTable, fn)
}

func (e *DDLExec) getRecoverTableByTableName(tableName *ast.TableName) (*model.Job, *model.TableInfo, error) {
	txn, err := e.Ctx().Txn(true)
	if err != nil {
		return nil, nil, err
	}
	schemaName := tableName.Schema.L
	if schemaName == "" {
		schemaName = strings.ToLower(e.Ctx().GetSessionVars().CurrentDB)
	}
	if schemaName == "" {
		return nil, nil, errors.Trace(plannererrors.ErrNoDB)
	}
	gcSafePoint, err := gcutil.GetGCSafePoint(e.Ctx())
	if err != nil {
		return nil, nil, err
	}
	var jobInfo *model.Job
	var tableInfo *model.TableInfo
	dom := domain.GetDomain(e.Ctx())
	handleJobAndTableInfo := func(job *model.Job, tblInfo *model.TableInfo) (bool, error) {
		if tblInfo.Name.L != tableName.Name.L {
			return false, nil
		}
		schema, ok := dom.InfoSchema().SchemaByID(job.SchemaID)
		if !ok {
			return false, nil
		}
		if schema.Name.L == schemaName {
			tableInfo = tblInfo
			jobInfo = job
			return true, nil
		}
		return false, nil
	}
	fn := func(jobs []*model.Job) (bool, error) {
		return GetDropOrTruncateTableInfoFromJobs(jobs, gcSafePoint, dom, handleJobAndTableInfo)
	}
	err = ddl.IterHistoryDDLJobs(txn, fn)
	if err != nil {
		if terror.ErrorEqual(variable.ErrSnapshotTooOld, err) {
			return nil, nil, errors.Errorf("Can't find dropped/truncated table '%s' in GC safe point %s", tableName.Name.O, model.TSConvert2Time(gcSafePoint).String())
		}
		return nil, nil, err
	}
	if tableInfo == nil || jobInfo == nil {
		return nil, nil, errors.Errorf("Can't find localTemporary/dropped/truncated table: %v in DDL history jobs", tableName.Name)
	}
	// Dropping local temporary tables won't appear in DDL jobs.
	if tableInfo.TempTableType == model.TempTableGlobal {
		return nil, nil, exeerrors.ErrUnsupportedFlashbackTmpTable
	}

	// We can't return the meta directly since it will be modified outside, which may corrupt the infocache.
	// Since only State field is changed, return a shallow copy is enough.
	// see https://github.com/pingcap/tidb/issues/55462
	tblInfo := *tableInfo
	return jobInfo, &tblInfo, nil
}

func (e *DDLExec) executeFlashBackCluster(s *ast.FlashBackToTimestampStmt) error {
	// Check `TO TSO` clause
	if s.FlashbackTSO > 0 {
		return e.ddlExecutor.FlashbackCluster(e.Ctx(), s.FlashbackTSO)
	}

	// Check `TO TIMESTAMP` clause
	flashbackTS, err := staleread.CalculateAsOfTsExpr(context.Background(), e.Ctx().GetPlanCtx(), s.FlashbackTS)
	if err != nil {
		return err
	}

	return e.ddlExecutor.FlashbackCluster(e.Ctx(), flashbackTS)
}

func (e *DDLExec) executeFlashbackTable(s *ast.FlashBackTableStmt) error {
	job, tblInfo, err := e.getRecoverTableByTableName(s.Table)
	if err != nil {
		return err
	}
	if len(s.NewName) != 0 {
		tblInfo.Name = pmodel.NewCIStr(s.NewName)
	}
	// Check the table ID was not exists.
	is := domain.GetDomain(e.Ctx()).InfoSchema()
	tbl, ok := is.TableByID(context.Background(), tblInfo.ID)
	if ok {
		return infoschema.ErrTableExists.GenWithStack("Table '%-.192s' already been flashback to '%-.192s', can't be flashback repeatedly", s.Table.Name.O, tbl.Meta().Name.O)
	}

	m := domain.GetDomain(e.Ctx()).GetSnapshotMeta(job.StartTS)
	autoIDs, err := m.GetAutoIDAccessors(job.SchemaID, job.TableID).Get()
	if err != nil {
		return err
	}

	recoverInfo := &model.RecoverTableInfo{
		SchemaID:      job.SchemaID,
		TableInfo:     tblInfo,
		DropJobID:     job.ID,
		SnapshotTS:    job.StartTS,
		AutoIDs:       autoIDs,
		OldSchemaName: job.SchemaName,
		OldTableName:  s.Table.Name.L,
	}
	// Call DDL RecoverTable.
	err = e.ddlExecutor.RecoverTable(e.Ctx(), recoverInfo)
	return err
}

// executeFlashbackDatabase represents a restore schema executor.
// It is built from "flashback schema" statement,
// is used to recover the schema that deleted by mistake.
func (e *DDLExec) executeFlashbackDatabase(s *ast.FlashBackDatabaseStmt) error {
	dbName := s.DBName
	if len(s.NewName) > 0 {
		dbName = pmodel.NewCIStr(s.NewName)
	}
	// Check the Schema Name was not exists.
	is := domain.GetDomain(e.Ctx()).InfoSchema()
	if is.SchemaExists(dbName) {
		return infoschema.ErrDatabaseExists.GenWithStackByArgs(dbName)
	}
	recoverSchemaInfo, err := e.getRecoverDBByName(s.DBName)
	if err != nil {
		return err
	}
	// Check the Schema ID was not exists.
	if schema, ok := is.SchemaByID(recoverSchemaInfo.ID); ok {
		return infoschema.ErrDatabaseExists.GenWithStack("Schema '%-.192s' already been recover to '%-.192s', can't be recover repeatedly", s.DBName, schema.Name.O)
	}
	recoverSchemaInfo.Name = dbName
	// Call DDL RecoverSchema.
	err = e.ddlExecutor.RecoverSchema(e.Ctx(), recoverSchemaInfo)
	return err
}

func (e *DDLExec) getRecoverDBByName(schemaName pmodel.CIStr) (recoverSchemaInfo *model.RecoverSchemaInfo, err error) {
	txn, err := e.Ctx().Txn(true)
	if err != nil {
		return nil, err
	}
	gcSafePoint, err := gcutil.GetGCSafePoint(e.Ctx())
	if err != nil {
		return nil, err
	}
	dom := domain.GetDomain(e.Ctx())
	fn := func(jobs []*model.Job) (bool, error) {
		for _, job := range jobs {
			// Check GC safe point for getting snapshot infoSchema.
			err = gcutil.ValidateSnapshotWithGCSafePoint(job.StartTS, gcSafePoint)
			if err != nil {
				return false, err
			}
			if job.Type != model.ActionDropSchema {
				continue
			}
			snapMeta := dom.GetSnapshotMeta(job.StartTS)
			schemaInfo, err := snapMeta.GetDatabase(job.SchemaID)
			if err != nil {
				return false, err
			}
			if schemaInfo == nil {
				// The dropped DDL maybe execute failed that caused by the parallel DDL execution,
				// then can't find the schema from the snapshot info-schema. Should just ignore error here,
				// see more in TestParallelDropSchemaAndDropTable.
				continue
			}
			if schemaInfo.Name.L != schemaName.L {
				continue
			}
			recoverSchemaInfo = &model.RecoverSchemaInfo{
				DBInfo:              schemaInfo,
				LoadTablesOnExecute: true,
				DropJobID:           job.ID,
				SnapshotTS:          job.StartTS,
				OldSchemaName:       schemaName,
			}
			return true, nil
		}
		return false, nil
	}
	err = ddl.IterHistoryDDLJobs(txn, fn)
	if err != nil {
		if terror.ErrorEqual(variable.ErrSnapshotTooOld, err) {
			return nil, errors.Errorf("Can't find dropped database '%s' in GC safe point %s", schemaName.O, model.TSConvert2Time(gcSafePoint).String())
		}
		return nil, err
	}
	if recoverSchemaInfo == nil {
		return nil, errors.Errorf("Can't find dropped database: %v in DDL history jobs", schemaName.O)
	}
	return
}

func (e *DDLExec) executeLockTables(s *ast.LockTablesStmt) error {
	if !config.TableLockEnabled() {
		e.Ctx().GetSessionVars().StmtCtx.AppendWarning(exeerrors.ErrFuncNotEnabled.FastGenByArgs("LOCK TABLES", "enable-table-lock"))
		return nil
	}

	for _, tb := range s.TableLocks {
		if _, ok := e.getLocalTemporaryTable(tb.Table.Schema, tb.Table.Name); ok {
			return dbterror.ErrUnsupportedLocalTempTableDDL.GenWithStackByArgs("LOCK TABLES")
		}
	}

	return e.ddlExecutor.LockTables(e.Ctx(), s)
}

func (e *DDLExec) executeUnlockTables(_ *ast.UnlockTablesStmt) error {
	if !config.TableLockEnabled() {
		e.Ctx().GetSessionVars().StmtCtx.AppendWarning(exeerrors.ErrFuncNotEnabled.FastGenByArgs("UNLOCK TABLES", "enable-table-lock"))
		return nil
	}
	lockedTables := e.Ctx().GetAllTableLocks()
	return e.ddlExecutor.UnlockTables(e.Ctx(), lockedTables)
}

func (e *DDLExec) executeCleanupTableLock(s *ast.CleanupTableLockStmt) error {
	for _, tb := range s.Tables {
		if _, ok := e.getLocalTemporaryTable(tb.Schema, tb.Name); ok {
			return dbterror.ErrUnsupportedLocalTempTableDDL.GenWithStackByArgs("ADMIN CLEANUP TABLE LOCK")
		}
	}
	return e.ddlExecutor.CleanupTableLock(e.Ctx(), s.Tables)
}

func (e *DDLExec) executeRepairTable(s *ast.RepairTableStmt) error {
	return e.ddlExecutor.RepairTable(e.Ctx(), s.CreateStmt)
}

func (e *DDLExec) executeCreateSequence(s *ast.CreateSequenceStmt) error {
	return e.ddlExecutor.CreateSequence(e.Ctx(), s)
}

func (e *DDLExec) executeAlterSequence(s *ast.AlterSequenceStmt) error {
	return e.ddlExecutor.AlterSequence(e.Ctx(), s)
}

func (e *DDLExec) executeCreatePlacementPolicy(s *ast.CreatePlacementPolicyStmt) error {
	return e.ddlExecutor.CreatePlacementPolicy(e.Ctx(), s)
}

func (e *DDLExec) executeDropPlacementPolicy(s *ast.DropPlacementPolicyStmt) error {
	return e.ddlExecutor.DropPlacementPolicy(e.Ctx(), s)
}

func (e *DDLExec) executeAlterPlacementPolicy(s *ast.AlterPlacementPolicyStmt) error {
	return e.ddlExecutor.AlterPlacementPolicy(e.Ctx(), s)
}

func (e *DDLExec) executeCreateResourceGroup(s *ast.CreateResourceGroupStmt) error {
	if !variable.EnableResourceControl.Load() && !e.Ctx().GetSessionVars().InRestrictedSQL {
		return infoschema.ErrResourceGroupSupportDisabled
	}
	return e.ddlExecutor.AddResourceGroup(e.Ctx(), s)
}

func (e *DDLExec) executeAlterResourceGroup(s *ast.AlterResourceGroupStmt) error {
	if !variable.EnableResourceControl.Load() && !e.Ctx().GetSessionVars().InRestrictedSQL {
		return infoschema.ErrResourceGroupSupportDisabled
	}
	return e.ddlExecutor.AlterResourceGroup(e.Ctx(), s)
}

func (e *DDLExec) executeDropResourceGroup(s *ast.DropResourceGroupStmt) error {
	if !variable.EnableResourceControl.Load() && !e.Ctx().GetSessionVars().InRestrictedSQL {
		return infoschema.ErrResourceGroupSupportDisabled
	}
	return e.ddlExecutor.DropResourceGroup(e.Ctx(), s)
}
