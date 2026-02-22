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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	plannererrors "github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

// RefreshMaterializedViewExec executes "REFRESH MATERIALIZED VIEW" as a utility-style statement.
type RefreshMaterializedViewExec struct {
	exec.BaseExecutor
	stmt *ast.RefreshMaterializedViewStmt
	done bool
}

// Next implements the Executor Next interface.
func (e *RefreshMaterializedViewExec) Next(ctx context.Context, _ *chunk.Chunk) (err error) {
	if e.done {
		return nil
	}
	e.done = true

	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMVMaintenance)

	return e.executeRefreshMaterializedView(ctx, e.stmt)
}

func (e *RefreshMaterializedViewExec) executeRefreshMaterializedView(kctx context.Context, s *ast.RefreshMaterializedViewStmt) error {
	refreshType, err := validateRefreshMaterializedViewStmt(s)
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
	lockedReadTSO, lockedReadTSONull, persistFailureOnErr, err := lockAndValidateRefreshInfoRow(kctx, sqlExec, mviewID)
	if err != nil {
		if persistFailureOnErr {
			return persistRefreshFailureAndCommit(kctx, sqlExec, refreshType, mviewID, err, &txnCommitted)
		}
		return err
	}

	var lastSuccessfulRefreshReadTSO int64
	if s.Type == ast.RefreshMaterializedViewTypeFast {
		// LAST_SUCCESSFUL_REFRESH_READ_TSO is BIGINT DEFAULT NULL. FAST refresh requires it to be non-NULL.
		if lockedReadTSONull {
			return errors.New("refresh materialized view fast: LAST_SUCCESSFUL_REFRESH_READ_TSO is NULL")
		}
		lastSuccessfulRefreshReadTSO = lockedReadTSO
	}

	txn, err := refreshSctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	startTS := txn.StartTS()
	if startTS == 0 {
		return errors.New("refresh materialized view: invalid transaction start tso")
	}

	// Use a savepoint so we can keep the mutex lock, rollback MV data changes on failure,
	// but still commit refresh metadata updates (failed reason) as requested.
	const refreshSavepoint = "tidb_mview_refresh_sp"
	if _, err := sqlExec.ExecuteInternal(kctx, "SAVEPOINT "+refreshSavepoint); err != nil {
		return errors.Trace(err)
	}

	if err := executeRefreshMaterializedViewDataChanges(
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
		return persistRefreshFailureAndCommit(kctx, sqlExec, refreshType, mviewID, err, &txnCommitted)
	}

	refreshReadTSO, err := getRefreshReadTSOForSuccess(s.Type, sessVars, startTS)
	if err != nil {
		return err
	}
	return persistRefreshSuccessAndCommit(kctx, sqlExec, refreshType, mviewID, refreshReadTSO, &txnCommitted)
}

func validateRefreshMaterializedViewStmt(s *ast.RefreshMaterializedViewStmt) (string, error) {
	if s == nil || s.ViewName == nil {
		return "", errors.New("refresh materialized view: missing view name")
	}
	switch s.Type {
	case ast.RefreshMaterializedViewTypeComplete:
		// supported
	case ast.RefreshMaterializedViewTypeFast:
		// Framework is supported; actual execution happens via RefreshMaterializedViewImplementStmt.
	default:
		return "", errors.New("unknown REFRESH MATERIALIZED VIEW type")
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
		return pmodel.CIStr{}, nil, errors.New("refresh materialized view: invalid select sql")
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

func lockAndValidateRefreshInfoRow(
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
		return 0, false, false, errors.New("refresh materialized view: required system table mysql.tidb_mview_refresh does not exist")
	}
	if err != nil {
		return 0, false, false, errors.Trace(err)
	}
	if lockRS == nil {
		return 0, false, false, errors.New("refresh materialized view: cannot lock mysql.tidb_mview_refresh row")
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
		return 0, false, false, errors.New("refresh materialized view: refresh info row missing in mysql.tidb_mview_refresh")
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
		return 0, false, false, errors.New("refresh materialized view: cannot re-check mysql.tidb_mview_refresh row")
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
		return 0, false, false, errors.New("refresh materialized view: refresh info row missing in mysql.tidb_mview_refresh")
	}
	recheckRow := recheckRows[0]
	recheckReadTSONull := recheckRow.IsNull(0)
	var recheckReadTSO int64
	if !recheckReadTSONull {
		recheckReadTSO = recheckRow.GetInt64(0)
	}
	if lockedReadTSONull != recheckReadTSONull || (!lockedReadTSONull && lockedReadTSO != recheckReadTSO) {
		return 0, false, true, errors.New("refresh materialized view: inconsistent LAST_SUCCESSFUL_REFRESH_READ_TSO between locking read and snapshot read")
	}
	return lockedReadTSO, lockedReadTSONull, false, nil
}

func executeRefreshMaterializedViewDataChanges(
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
		return executeFastRefreshImplementStmt(kctx, sqlExec, sessVars, implementStmt)
	default:
		return errors.New("unknown REFRESH MATERIALIZED VIEW type")
	}
}

func executeFastRefreshImplementStmt(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	sessVars *variable.SessionVars,
	implementStmt *ast.RefreshMaterializedViewImplementStmt,
) error {
	if internalExec, ok := sqlExec.(interface {
		ExecuteInternalStmt(context.Context, ast.StmtNode) (sqlexec.RecordSet, error)
	}); ok {
		rs, execErr := internalExec.ExecuteInternalStmt(kctx, implementStmt)
		return drainAndCloseRefreshRecordSet(kctx, rs, execErr)
	}

	// Fallback: emulate ExecuteInternalStmt by flipping InRestrictedSQL around ExecuteStmt.
	origRestricted := sessVars.InRestrictedSQL
	sessVars.InRestrictedSQL = true
	defer func() {
		sessVars.InRestrictedSQL = origRestricted
	}()
	rs, execErr := sqlExec.ExecuteStmt(kctx, implementStmt)
	return drainAndCloseRefreshRecordSet(kctx, rs, execErr)
}

func drainAndCloseRefreshRecordSet(
	kctx context.Context,
	rs sqlexec.RecordSet,
	execErr error,
) error {
	if rs == nil {
		return execErr
	}
	if execErr == nil {
		if drainErr := drainRefreshRecordSet(kctx, rs); drainErr != nil {
			_ = rs.Close()
			return errors.Trace(drainErr)
		}
	}
	if closeErr := rs.Close(); closeErr != nil && execErr == nil {
		return errors.Trace(closeErr)
	}
	return execErr
}

func drainRefreshRecordSet(kctx context.Context, rs sqlexec.RecordSet) error {
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

func persistRefreshFailureAndCommit(
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
			return errors.New("refresh materialized view: required system table mysql.tidb_mview_refresh does not exist")
		}
		return errors.Annotatef(err, "refresh materialized view: failed to persist refresh failure info (original error: %v)", refreshErr)
	}
	if _, err := sqlExec.ExecuteInternal(kctx, "COMMIT"); err != nil {
		return errors.Trace(err)
	}
	*txnCommitted = true
	return errors.Trace(refreshErr)
}

func getRefreshReadTSOForSuccess(
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
			return 0, errors.New("refresh materialized view: invalid refresh read tso")
		}
	}
	return refreshReadTSO, nil
}

func persistRefreshSuccessAndCommit(
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
			return errors.New("refresh materialized view: required system table mysql.tidb_mview_refresh does not exist")
		}
		return errors.Trace(err)
	}
	if _, err := sqlExec.ExecuteInternal(kctx, "COMMIT"); err != nil {
		return errors.Trace(err)
	}
	*txnCommitted = true
	return nil
}
