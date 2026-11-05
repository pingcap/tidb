// Copyright 2026 PingCAP, Inc.
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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	tidbmetrics "github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	plannercorebase "github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn/staleread"
	storeerr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	plannererrors "github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

var errMVRefreshAdvisoryLockConflict = errors.NewNoStackError("materialized view refresh advisory lock conflict")

const (
	mvRefreshAdvisoryLockTimeoutSec = int64(1)
	mvRefreshShadowTablePrefix      = "__mv_shadow_"
	mvRefreshImportIntoStoreName    = "TiKV"
)

// RefreshMaterializedViewExec executes "REFRESH MATERIALIZED VIEW" as a utility-style statement.
type RefreshMaterializedViewExec struct {
	exec.BaseExecutor
	stmt                  *ast.RefreshMaterializedViewStmt
	stepObserver          mvRefreshStepObserver
	planFormatForObserver string
	done                  bool
}

func readRefreshHistCancelRequest(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	refreshJobID uint64,
	mviewID int64,
) (bool, string, error) {
	rows, err := sqlexec.ExecSQL(
		kctx,
		sqlExec,
		`SELECT CANCEL_REQUEST_TIME, CANCEL_REQUESTED_BY
FROM mysql.tidb_mview_refresh_hist
WHERE REFRESH_JOB_ID = %?
  AND MVIEW_ID = %?`,
		refreshJobID,
		mviewID,
	)
	if err != nil {
		if infoschema.ErrTableNotExists.Equal(err) {
			return false, "", errors.New("refresh materialized view: required system table mysql.tidb_mview_refresh_hist does not exist")
		}
		return false, "", errors.Trace(err)
	}
	if len(rows) == 0 || rows[0].IsNull(0) {
		return false, "", nil
	}
	if rows[0].IsNull(1) {
		return true, "", nil
	}
	return true, rows[0].GetString(1), nil
}

func requestRefreshHistCancel(
	kctx context.Context,
	sctx sessionctx.Context,
	refreshJobID uint64,
	requester any,
) (bool, error) {
	_, err := sctx.GetSQLExecutor().ExecuteInternal(
		kctx,
		`UPDATE mysql.tidb_mview_refresh_hist
SET CANCEL_REQUEST_TIME = NOW(6),
	CANCEL_REQUESTED_BY = %?
WHERE REFRESH_JOB_ID = %?
  AND REFRESH_STATUS = 'running'
  AND CANCEL_REQUEST_TIME IS NULL`,
		requester,
		refreshJobID,
	)
	if err != nil {
		return false, errors.Trace(err)
	}
	return sctx.GetSessionVars().StmtCtx.AffectedRows() > 0, nil
}

func updateRefreshHistHeartbeat(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	refreshJobID uint64,
	mviewID int64,
) error {
	_, err := sqlExec.ExecuteInternal(
		kctx,
		`UPDATE mysql.tidb_mview_refresh_hist
SET LAST_HEARTBEAT_TIME = NOW(6)
WHERE REFRESH_JOB_ID = %?
  AND MVIEW_ID = %?
  AND REFRESH_STATUS = 'running'`,
		refreshJobID,
		mviewID,
	)
	if err != nil {
		if infoschema.ErrTableNotExists.Equal(err) {
			return errors.New("refresh materialized view: required system table mysql.tidb_mview_refresh_hist does not exist")
		}
		return errors.Trace(err)
	}
	return nil
}

func checkRefreshMaterializedViewBaseTableSelect(
	ctx sessionctx.Context,
	is infoschema.InfoSchema,
	mviewInfo *model.MaterializedViewInfo,
) error {
	sessVars := ctx.GetSessionVars()
	if sessVars.InMViewMaintenance {
		if !sessVars.InRestrictedSQL {
			return plannererrors.ErrInternal.GenWithStack(
				"materialized view maintenance should only run in restricted SQL mode",
			)
		}
		return nil
	}
	if sessVars.InRestrictedSQL {
		return nil
	}

	pm := privilege.GetPrivilegeManager(ctx)
	user := sessVars.User
	if pm == nil || user == nil || mviewInfo == nil {
		return nil
	}
	for _, id := range mviewInfo.BaseTableIDs {
		baseTable, ok := is.TableByID(context.Background(), id)
		if !ok {
			continue
		}
		dbInfo, ok := infoschema.SchemaByTable(is, baseTable.Meta())
		if !ok {
			continue
		}
		baseName := baseTable.Meta().Name.L
		if !pm.RequestVerification(sessVars.ActiveRoles, dbInfo.Name.L, baseName, "", mysql.SelectPriv) {
			return plannererrors.ErrTableaccessDenied.GenWithStackByArgs("SELECT", user.AuthUsername, user.AuthHostname, baseName)
		}
	}
	return nil
}

func resolveCancelRefreshJobPrivilegeTarget(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	is infoschema.InfoSchema,
	refreshJobID uint64,
) (dbName string, tableName string, found bool, err error) {
	rows, err := sqlexec.ExecSQL(
		kctx,
		sqlExec,
		`SELECT MVIEW_ID
FROM mysql.tidb_mview_refresh_hist
WHERE REFRESH_JOB_ID = %?
  AND REFRESH_STATUS = 'running'`,
		refreshJobID,
	)
	if err != nil {
		if infoschema.ErrTableNotExists.Equal(err) {
			return "", "", false, errors.New("refresh materialized view: required system table mysql.tidb_mview_refresh_hist does not exist")
		}
		return "", "", false, errors.Trace(err)
	}
	if len(rows) == 0 {
		return "", "", false, nil
	}
	mviewID := rows[0].GetInt64(0)
	mviewTable, ok := is.TableByID(context.Background(), mviewID)
	if !ok {
		return "", "", false, errors.Errorf("refresh materialized view: cannot resolve target materialized view %d for cancel job %d", mviewID, refreshJobID)
	}
	dbInfo, ok := infoschema.SchemaByTable(is, mviewTable.Meta())
	if !ok {
		return "", "", false, errors.Errorf("refresh materialized view: cannot resolve schema for materialized view %d", mviewID)
	}
	return dbInfo.Name.L, mviewTable.Meta().Name.L, true, nil
}

type mvRefreshStmtResult struct {
	affectedRows uint64
	message      string
}

func newMVRefreshStmtResultFromWriteCounts(insertRows, updateRows, deleteRows int64) mvRefreshStmtResult {
	return mvRefreshStmtResult{
		affectedRows: uint64(insertRows + updateRows + deleteRows),
		message:      formatMVRefreshWriteResultMessage(insertRows, updateRows, deleteRows),
	}
}

func formatMVRefreshWriteResultMessage(insertRows, updateRows, deleteRows int64) string {
	return fmt.Sprintf(
		"Rows inserted: %d  Updated: %d  Deleted: %d",
		insertRows,
		updateRows,
		deleteRows,
	)
}

func captureMVRefreshStmtResult(sessVars *variable.SessionVars) mvRefreshStmtResult {
	if sessVars == nil || sessVars.StmtCtx == nil {
		return mvRefreshStmtResult{}
	}
	return mvRefreshStmtResult{
		affectedRows: sessVars.StmtCtx.AffectedRows(),
		message:      sessVars.StmtCtx.GetMessage(),
	}
}

func applyMVRefreshStmtResult(stmtCtx *stmtctx.StatementContext, result mvRefreshStmtResult) {
	if stmtCtx == nil {
		return
	}
	stmtCtx.SetAffectedRows(result.affectedRows)
	stmtCtx.SetMessage(result.message)
}

// Next implements the Executor Next interface.
func (e *RefreshMaterializedViewExec) Next(ctx context.Context, _ *chunk.Chunk) (err error) {
	if e.done {
		return nil
	}
	e.done = true

	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMViewMaintenance)

	return e.executeRefreshMaterializedView(ctx, e.stmt)
}
func observeMVRefreshStep(
	observer mvRefreshStepObserver,
	step mvRefreshObserveStep,
	fn func() error,
) error {
	if observer == nil {
		return fn()
	}
	startAt := time.Now()
	observer.OnStepStart(step, startAt)
	err := fn()
	observer.OnStepEnd(step, time.Now(), err)
	return err
}

func emitMVRefreshStepPlanRows(
	observer mvRefreshStepObserver,
	step mvRefreshObserveStep,
	sessVars *variable.SessionVars,
	format string,
) {
	if observer == nil || sessVars == nil || sessVars.StmtCtx == nil {
		return
	}
	targetPlanAny := sessVars.StmtCtx.GetPlan()
	targetPlan, ok := targetPlanAny.(plannercorebase.Plan)
	if !ok || targetPlan == nil {
		return
	}

	explain := &plannercore.Explain{
		TargetPlan:       targetPlan,
		Format:           format,
		Analyze:          true,
		RuntimeStatsColl: sessVars.StmtCtx.RuntimeStatsColl,
	}
	explain.SetSCtx(targetPlan.SCtx())
	if err := explain.RenderResult(); err != nil {
		return
	}
	observer.OnStepPlanRows(step, clonePlanRows(explain.Rows))
}

func (e *RefreshMaterializedViewExec) executeRefreshMaterializedView(kctx context.Context, s *ast.RefreshMaterializedViewStmt) (err error) {
	const slowRefreshOverheadThreshold = 5 * time.Second
	refreshStart := time.Now()
	var (
		lockRefreshInfoRowDur time.Duration
		executeDataChangesDur time.Duration
		txnTotalDur           time.Duration
		mviewID               int64
		refreshMode           ast.RefreshMaterializedViewMode
	)
	isInternalSQL := e.Ctx().GetSessionVars().InRestrictedSQL
	defer func() {
		if refreshMode == ast.RefreshMaterializedViewModeCompleteOutOfPlace {
			return
		}
		total := time.Since(refreshStart)
		failpoint.InjectCall("refreshMaterializedViewSlowLogDurations", &total, &executeDataChangesDur)
		// Data execution has its own slow query entries; diagnose refresh orchestration overhead here.
		overhead := total - executeDataChangesDur
		if overhead < slowRefreshOverheadThreshold {
			return
		}

		schemaName, mviewName, refreshType := "", "", ""
		if s != nil {
			refreshType = strings.ToLower(s.Type.String())
			if s.ViewName != nil {
				schemaName = s.ViewName.Schema.O
				mviewName = s.ViewName.Name.O
			}
		}

		fields := []zap.Field{
			zap.Duration("total", total),
			zap.Duration("overhead", overhead),
			zap.Duration("slowOverheadThreshold", slowRefreshOverheadThreshold),
			zap.String("schema", schemaName),
			zap.String("mview", mviewName),
			zap.Int64("mviewID", mviewID),
			zap.String("refreshType", refreshType),
			zap.Bool("internalSQL", isInternalSQL),
			zap.Bool("success", err == nil),
			zap.Duration("lockRefreshInfoRow", lockRefreshInfoRowDur),
			zap.Duration("executeRefreshMaterializedViewDataChanges", executeDataChangesDur),
			zap.Duration("transactionTotal", txnTotalDur),
		}
		if err != nil {
			fields = append(fields, zap.String("error", err.Error()))
		}
		logutil.BgLogger().Info("refresh materialized view is slow", fields...)
	}()

	refreshMode, refreshMethod, err := validateRefreshMaterializedViewStmt(s, isInternalSQL)
	if err != nil {
		return err
	}
	targetRefreshReadTSO, err := evaluateRefreshMaterializedViewTargetTSO(kctx, e.Ctx(), s)
	if err != nil {
		return err
	}
	refreshHistFailedReadTSO := refreshHistReadTSOOnFailure(s, targetRefreshReadTSO)
	stepSet, err := newMVRefreshStepSet(refreshMode)
	if err != nil {
		return err
	}
	releaseCtx := kctx
	taskCancelController := newMVTaskCancelController(kctx)
	defer taskCancelController.cancel()
	kctx = taskCancelController.context()
	finalizeCtx := context.WithoutCancel(kctx)
	refreshJobID := uint64(0)

	schemaName, tblInfo, err := e.resolveRefreshMaterializedViewTarget(s)
	if err != nil {
		return err
	}
	if err := checkRefreshMaterializedViewBaseTableSelect(e.Ctx(), domain.GetDomain(e.Ctx()).InfoSchema(), tblInfo.MaterializedView); err != nil {
		return err
	}
	reportRefreshFailed := tblInfo.MaterializedView != nil && tblInfo.MaterializedView.AlertRefreshFailed
	mviewID = tblInfo.ID
	refreshHistRunningInserted := false
	defer func() {
		if r := recover(); r != nil {
			err = util.GetRecoverError(r)
		}
		if err == nil || mviewID == 0 || refreshMethod == "" || refreshHistRunningInserted {
			return
		}
		err = e.insertRefreshHistFailedFallback(
			finalizeCtx,
			releaseCtx,
			mviewID,
			schemaName.O,
			tblInfo.Name.O,
			refreshMethod,
			refreshHistFailedReadTSO,
			&refreshJobID,
			taskCancelController,
			refreshStart,
			reportRefreshFailed,
			isInternalSQL,
			err,
		)
	}()

	refreshSctx, err := e.GetSysSession()
	if err != nil {
		return err
	}
	defer e.ReleaseSysSession(releaseCtx, refreshSctx)
	if collectorAware, ok := refreshSctx.(interface{ AttachStatsCollectorForInternalSession() func() }); ok {
		// REFRESH MATERIALIZED VIEW runs real maintenance reads/writes against user tables, so
		// reuse the full session collectors here, including index usage collection when enabled.
		defer collectorAware.AttachStatsCollectorForInternalSession()()
	}
	sqlExec := refreshSctx.GetSQLExecutor()
	sessVars := refreshSctx.GetSessionVars()
	refreshExecutionVars := captureRefreshExecutionSessionVars(e.Ctx().GetSessionVars())
	restoreRefreshExecutionVars, err := applyRefreshExecutionSessionVars(sessVars, refreshExecutionVars, isInternalSQL)
	if err != nil {
		return err
	}
	defer restoreRefreshExecutionVars()
	failpoint.InjectCall("mvMaintainMemQuotaAppliedOnRefreshSession", sessVars.MemQuotaQuery, refreshExecutionVars.MaintainMemQuota)
	failpoint.InjectCall(
		"refreshMaterializedViewIsolationReadEnginesApplied",
		variable.GetIsolationReadEnginesString(sessVars),
		refreshExecutionVars.IsolationReadEngines,
	)

	restoreSessVars, err := initRefreshMaterializedViewSession(sessVars, tblInfo.MaterializedView)
	if err != nil {
		return err
	}
	defer restoreSessVars()
	failpoint.InjectCall("refreshMaterializedViewAfterInitSession", sessVars.SQLMode, sessVars.Location().String())

	mviewID = tblInfo.ID
	advisoryLockName, err := acquireMVRefreshAdvisoryLock(refreshSctx, schemaName, tblInfo)
	if err != nil {
		return err
	}
	defer func() {
		releasedCnt := releaseMVRefreshAdvisoryLockFully(refreshSctx, advisoryLockName)
		if releasedCnt == 1 {
			return
		}
		invariantErr := errors.Errorf(
			"refresh materialized view: advisory lock cleanup invariant violated (lock=%s released=%d)",
			advisoryLockName,
			releasedCnt,
		)
		logutil.BgLogger().Error(
			"refresh materialized view advisory lock cleanup invariant violated",
			zap.String("schema", schemaName.O),
			zap.String("mview", tblInfo.Name.O),
			zap.Int64("schemaID", tblInfo.DBID),
			zap.Int64("mviewID", mviewID),
			zap.String("lockName", advisoryLockName),
			zap.Int("releasedCount", releasedCnt),
		)
		if err == nil {
			err = invariantErr
			return
		}
		err = errors.Annotate(err, invariantErr.Error())
	}()
	failpoint.InjectCall("refreshMaterializedViewAfterAcquireAdvisoryLock")
	failpoint.Inject("mockRefreshMaterializedViewErrorBeforeInsertHist", func(val failpoint.Value) {
		if msg, ok := val.(string); ok {
			failpoint.Return(errors.New(msg))
		}
	})

	if refreshMode == ast.RefreshMaterializedViewModeCompleteOutOfPlace {
		expectedRefreshInfo, err := readRefreshInfoSnapshot(kctx, sqlExec, mviewID)
		if err != nil {
			return err
		}
		refreshScheduleDuration := buildRefreshScheduleDuration(isInternalSQL, refreshStart, expectedRefreshInfo)
		refreshJobID, err = allocJobID(e.Ctx().GetStore())
		if err != nil {
			return errors.Annotate(err, "refresh materialized view: failed to allocate refresh job id")
		}
		histSctx, err := e.GetSysSession()
		if err != nil {
			return err
		}
		defer e.ReleaseSysSession(releaseCtx, histSctx)
		histSQLExec := histSctx.GetSQLExecutor()
		histLoc := histSctx.GetSessionVars().Location()

		if err := observeMVRefreshStep(e.stepObserver, stepSet.insertHistRunning, func() error {
			return insertRefreshHistRunning(
				kctx,
				histSQLExec,
				refreshJobID,
				mviewID,
				schemaName.O,
				tblInfo.Name.O,
				refreshMethod,
				histTime(refreshStart, histLoc),
			)
		}); err != nil {
			return err
		}
		refreshHistRunningInserted = true

		finalizeFailure := func(refreshErr error) error {
			refreshFailedReason, finalErr := taskCancelController.normalizeTaskFailure(refreshErr)
			refreshErrMsg := finalErr.Error()
			if refreshFailedReason != nil {
				refreshErrMsg = *refreshFailedReason
			}
			reportMVRefreshFailed(finalizeCtx, histSQLExec, reportRefreshFailed, mviewID, schemaName.O, tblInfo.Name.O, refreshJobID, refreshMethod, isInternalSQL, refreshErrMsg)
			histErr := observeMVRefreshStep(e.stepObserver, stepSet.finalizeHist, func() error {
				refreshEndAt := time.Now()
				return finalizeRefreshHistWithRetry(
					finalizeCtx,
					histSQLExec,
					refreshJobID,
					mviewID,
					refreshHistStatusFailed,
					refreshHistFailedReadTSO,
					nil,
					histTime(refreshStart, histLoc),
					histTime(refreshEndAt, histLoc),
					nil,
					nil,
					&refreshErrMsg,
				)
			})
			if histErr != nil {
				return errors.Annotatef(histErr, "refresh materialized view: failed to finalize refresh history after error %v", finalErr)
			}
			return errors.Trace(finalErr)
		}
		finalizeSuccess := func(buildReadTSO uint64) {
			if err := observeMVRefreshStep(e.stepObserver, stepSet.finalizeHist, func() error {
				refreshEndAt := time.Now()
				return finalizeRefreshHistWithRetry(
					finalizeCtx,
					histSQLExec,
					refreshJobID,
					mviewID,
					refreshHistStatusSuccess,
					&buildReadTSO,
					nil,
					histTime(refreshStart, histLoc),
					histTime(refreshEndAt, histLoc),
					nil,
					refreshScheduleDuration,
					nil,
				)
			}); err != nil {
				e.Ctx().GetSessionVars().StmtCtx.AppendWarning(
					errors.Annotate(err, "refresh materialized view: refresh committed but failed to finalize refresh history"),
				)
			}
			if alertErr := deleteRefreshAlertState(finalizeCtx, histSQLExec, mviewID); alertErr != nil {
				e.Ctx().GetSessionVars().StmtCtx.AppendWarning(
					errors.Annotate(alertErr, "refresh materialized view: refresh committed but failed to delete refresh alert"),
				)
			}
		}
		stopTaskMonitor, err := startMVTaskMonitor(
			kctx,
			e.GetSysSession,
			func(sctx sessionctx.Context) {
				e.ReleaseSysSession(releaseCtx, sctx)
			},
			taskCancelController,
			fmt.Sprintf("refresh-%d", refreshJobID),
			func(watchCtx context.Context, watchSQLExec sqlexec.SQLExecutor) (bool, string, error) {
				return readRefreshHistCancelRequest(watchCtx, watchSQLExec, refreshJobID, mviewID)
			},
			func(watchCtx context.Context, watchSQLExec sqlexec.SQLExecutor) error {
				return updateRefreshHistHeartbeat(watchCtx, watchSQLExec, refreshJobID, mviewID)
			},
		)
		if err != nil {
			return finalizeFailure(err)
		}
		defer stopTaskMonitor()
		failpoint.InjectCall("refreshMaterializedViewAfterInsertRefreshHistRunning")
		failpoint.Inject("pauseRefreshMaterializedViewAfterInsertRefreshHistRunning", func() {})

		buildReadTSO, err := e.executeRefreshMaterializedViewCompleteOutOfPlace(
			kctx,
			releaseCtx,
			s,
			refreshSctx,
			isInternalSQL,
			schemaName,
			tblInfo,
			stepSet,
			expectedRefreshInfo.lastSuccessReadTSO,
			expectedRefreshInfo.lastSuccessReadTSONull,
			refreshExecutionVars,
		)
		if err != nil {
			return finalizeFailure(err)
		}
		refreshStmtResult := captureMVRefreshStmtResult(sessVars)
		observeMVRefreshScheduleDuration(refreshScheduleDuration)
		finalizeSuccess(buildReadTSO)
		applyMVRefreshStmtResult(e.Ctx().GetSessionVars().StmtCtx, refreshStmtResult)
		return nil
	}
	var scheduleEvalSctx sessionctx.Context
	if isInternalSQL {
		scheduleEvalSctx, err = e.GetSysSession()
		if err != nil {
			return err
		}
		defer e.ReleaseSysSession(releaseCtx, scheduleEvalSctx)
	}

	txnStarted := false
	txnFinished := false
	txnCommitTimerStarted := false
	var txnCommitStart time.Time
	defer func() {
		if !txnStarted || txnFinished {
			return
		}
		_, _ = sqlExec.ExecuteInternal(finalizeCtx, "ROLLBACK")
		txnFinished = true
		if txnCommitTimerStarted && txnTotalDur == 0 {
			txnTotalDur = time.Since(txnCommitStart)
		}
	}()

	// Use a pessimistic txn to ensure `FOR UPDATE NOWAIT` works as a mutex.
	txnCommitStart = time.Now()
	if err := observeMVRefreshStep(e.stepObserver, stepSet.txnBegin, func() error {
		if _, err := sqlExec.ExecuteInternal(kctx, "BEGIN PESSIMISTIC"); err != nil {
			return errors.Trace(err)
		}
		txnStarted = true
		txnCommitTimerStarted = true
		return nil
	}); err != nil {
		return err
	}

	failpoint.InjectCall("refreshMaterializedViewAfterBegin")
	failpoint.Inject("pauseRefreshMaterializedViewAfterBegin", func() {})

	mviewID = tblInfo.ID
	var lockedRefreshInfo refreshInfoSnapshot
	if err := observeMVRefreshStep(e.stepObserver, stepSet.lockRefreshInfo, func() error {
		lockRefreshInfoRowStart := time.Now()
		var lockErr error
		lockedRefreshInfo, lockErr = lockRefreshInfoRow(kctx, sqlExec, mviewID)
		lockRefreshInfoRowDur = time.Since(lockRefreshInfoRowStart)
		return lockErr
	}); err != nil {
		return err
	}
	refreshScheduleDuration := buildRefreshScheduleDuration(isInternalSQL, refreshStart, lockedRefreshInfo)
	if refreshMode == ast.RefreshMaterializedViewModeFast && lockedRefreshInfo.lastSuccessReadTSONull {
		return errors.New("refresh materialized view fast: LAST_SUCCESS_READ_TSO is NULL")
	}
	if refreshMode == ast.RefreshMaterializedViewModeFast && targetRefreshReadTSO > 0 && targetRefreshReadTSO == lockedRefreshInfo.lastSuccessReadTSO {
		applyMVRefreshStmtResult(e.Ctx().GetSessionVars().StmtCtx, newMVRefreshStmtResultFromWriteCounts(0, 0, 0))
		return nil
	}
	histSctx, err := e.GetSysSession()
	if err != nil {
		return err
	}
	defer e.ReleaseSysSession(releaseCtx, histSctx)
	histSQLExec := histSctx.GetSQLExecutor()
	histLoc := histSctx.GetSessionVars().Location()

	boundedFastRefresh := refreshMode == ast.RefreshMaterializedViewModeFast &&
		targetRefreshReadTSO > 0 &&
		targetRefreshReadTSO > lockedRefreshInfo.lastSuccessReadTSO
	var mlogRetainedLowerTSO uint64
	if refreshMode == ast.RefreshMaterializedViewModeFast {
		// Read purge metadata through an internal session so this precheck sees latest committed state
		// instead of the refresh transaction's repeatable-read startTS snapshot.
		//
		// This is still a best-effort guard rather than a strict serialization point with future purge.
		// In theory, a concurrently-started purge should still be safe because purge computes safePurgeTSO
		// from persisted LAST_SUCCESS_READ_TSO, which does not advance until this refresh commits.
		is := e.Ctx().GetLatestInfoSchema().(infoschema.InfoSchema)
		mlogIntegrity, err := checkFastRefreshMLogIntegrity(kctx, histSQLExec, is, schemaName, tblInfo, lockedRefreshInfo.lastSuccessReadTSO)
		if err != nil {
			return err
		}
		if mlogIntegrity.hasRetainedLowerTSO {
			mlogRetainedLowerTSO = mlogIntegrity.retainedLowerTSO
		}
	}

	txn, err := refreshSctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	startTS := txn.StartTS()
	if startTS == 0 {
		return errors.New("refresh materialized view: invalid transaction start tso")
	}
	refreshJobID = startTS

	if err := observeMVRefreshStep(e.stepObserver, stepSet.insertHistRunning, func() error {
		return insertRefreshHistRunning(
			kctx,
			histSQLExec,
			refreshJobID,
			mviewID,
			schemaName.O,
			tblInfo.Name.O,
			refreshMethod,
			histTime(refreshStart, histLoc),
		)
	}); err != nil {
		return err
	}
	refreshHistRunningInserted = true

	finalizeFailure := func(refreshErr error) error {
		refreshFailedReason, finalErr := taskCancelController.normalizeTaskFailure(refreshErr)
		refreshErrMsg := finalErr.Error()
		if refreshFailedReason != nil {
			refreshErrMsg = *refreshFailedReason
		}
		var rollbackErr error
		if !txnFinished {
			if _, err := sqlExec.ExecuteInternal(finalizeCtx, "ROLLBACK"); err != nil {
				rollbackErr = errors.Trace(err)
				refreshErrMsg = refreshErrMsg + "; rollback error: " + err.Error()
			}
			txnFinished = true
			if txnCommitTimerStarted && txnTotalDur == 0 {
				txnTotalDur = time.Since(txnCommitStart)
			}
		}
		reportMVRefreshFailed(finalizeCtx, histSQLExec, reportRefreshFailed, mviewID, schemaName.O, tblInfo.Name.O, refreshJobID, refreshMethod, isInternalSQL, refreshErrMsg)
		histErr := observeMVRefreshStep(e.stepObserver, stepSet.finalizeHist, func() error {
			refreshEndAt := time.Now()
			return finalizeRefreshHistWithRetry(
				finalizeCtx,
				histSQLExec,
				refreshJobID,
				mviewID,
				refreshHistStatusFailed,
				refreshHistFailedReadTSO,
				nil,
				histTime(refreshStart, histLoc),
				histTime(refreshEndAt, histLoc),
				nil,
				nil,
				&refreshErrMsg,
			)
		})
		if histErr != nil {
			if rollbackErr != nil {
				return errors.Annotatef(histErr, "refresh materialized view: rollback failed (%v) and failed to finalize refresh history after error %v", rollbackErr, finalErr)
			}
			return errors.Annotatef(histErr, "refresh materialized view: failed to finalize refresh history after error %v", finalErr)
		}
		if rollbackErr != nil {
			return errors.Annotatef(rollbackErr, "refresh materialized view: rollback failed after error %v", finalErr)
		}
		return errors.Trace(finalErr)
	}
	finalizeSuccess := func(refreshReadTSO uint64, refreshCommitTSO *uint64, refreshRows *int64) {
		if err := observeMVRefreshStep(e.stepObserver, stepSet.finalizeHist, func() error {
			refreshEndAt := time.Now()
			return finalizeRefreshHistWithRetry(
				finalizeCtx,
				histSQLExec,
				refreshJobID,
				mviewID,
				refreshHistStatusSuccess,
				&refreshReadTSO,
				refreshCommitTSO,
				histTime(refreshStart, histLoc),
				histTime(refreshEndAt, histLoc),
				refreshRows,
				refreshScheduleDuration,
				nil,
			)
		}); err != nil {
			e.Ctx().GetSessionVars().StmtCtx.AppendWarning(
				errors.Annotate(err, "refresh materialized view: refresh committed but failed to finalize refresh history"),
			)
		}
		if alertErr := deleteRefreshAlertState(finalizeCtx, histSQLExec, mviewID); alertErr != nil {
			e.Ctx().GetSessionVars().StmtCtx.AppendWarning(
				errors.Annotate(alertErr, "refresh materialized view: refresh committed but failed to delete refresh alert"),
			)
		}
	}
	stopTaskMonitor, err := startMVTaskMonitor(
		kctx,
		e.GetSysSession,
		func(sctx sessionctx.Context) {
			e.ReleaseSysSession(releaseCtx, sctx)
		},
		taskCancelController,
		fmt.Sprintf("refresh-%d", refreshJobID),
		func(watchCtx context.Context, watchSQLExec sqlexec.SQLExecutor) (bool, string, error) {
			return readRefreshHistCancelRequest(watchCtx, watchSQLExec, refreshJobID, mviewID)
		},
		func(watchCtx context.Context, watchSQLExec sqlexec.SQLExecutor) error {
			return updateRefreshHistHeartbeat(watchCtx, watchSQLExec, refreshJobID, mviewID)
		},
	)
	if err != nil {
		return finalizeFailure(err)
	}
	defer stopTaskMonitor()

	failpoint.InjectCall("refreshMaterializedViewAfterInsertRefreshHistRunning")
	failpoint.Inject("pauseRefreshMaterializedViewAfterInsertRefreshHistRunning", func() {})

	var lastSuccessfulRefreshReadTSO uint64
	if refreshMode == ast.RefreshMaterializedViewModeFast {
		lastSuccessfulRefreshReadTSO = lockedRefreshInfo.lastSuccessReadTSO
		if targetRefreshReadTSO > 0 {
			if targetRefreshReadTSO < lastSuccessfulRefreshReadTSO {
				return finalizeFailure(errors.Errorf(
					"refresh materialized view fast as of timestamp: target tso %d is older than LAST_SUCCESS_READ_TSO %d",
					targetRefreshReadTSO,
					lastSuccessfulRefreshReadTSO,
				))
			}
		}
	}

	if err := executeRefreshMaterializedViewDataChanges(
		kctx,
		sqlExec,
		sessVars,
		s,
		refreshMode,
		schemaName,
		tblInfo,
		refreshImplementOptions{
			lastSuccessfulRefreshReadTSO: lastSuccessfulRefreshReadTSO,
			targetRefreshReadTSO:         targetRefreshReadTSO,
			mlogRetainedLowerTSO:         mlogRetainedLowerTSO,
		},
		stepSet,
		e.stepObserver,
		e.planFormatForObserver,
		&executeDataChangesDur,
	); err != nil {
		return finalizeFailure(err)
	}
	failpoint.Inject("mockRefreshMaterializedViewErrorAfterDataChanges", func(val failpoint.Value) {
		if msg, ok := val.(string); ok {
			failpoint.Return(finalizeFailure(errors.New(msg)))
		}
		failpoint.Return(finalizeFailure(errors.New("mock refresh materialized view error after data changes")))
	})
	refreshStmtResult := captureMVRefreshStmtResult(sessVars)

	actualRefreshReadTSO, err := getRefreshReadTSOForSuccess(sessVars)
	if err != nil {
		return finalizeFailure(err)
	}
	refreshReadTSO := actualRefreshReadTSO
	if boundedFastRefresh {
		if targetRefreshReadTSO > actualRefreshReadTSO {
			return finalizeFailure(errors.Errorf(
				"refresh materialized view fast as of timestamp: target tso %d is newer than actual refresh read tso %d",
				targetRefreshReadTSO,
				actualRefreshReadTSO,
			))
		}
		refreshReadTSO = targetRefreshReadTSO
	}

	var refreshRows *int64
	if refreshMode == ast.RefreshMaterializedViewModeFast {
		refreshRows = collectFastRefreshMLogScanRows(sessVars)
	}

	nextRefreshUnixSeconds, shouldUpdateNextRefreshUnixSeconds, err := deriveRuntimeMaterializedScheduleNextUnixSeconds(
		kctx,
		scheduleEvalSctx,
		tblInfo.MaterializedView.RefreshNext,
		isInternalSQL,
		tblInfo.MaterializedView.RefreshScheduleSQLMode,
		func() {
			logRuntimeMaterializedViewRefreshNextUnixSecondsUpdateNull(schemaName.O, tblInfo.Name.O, tblInfo.MaterializedView.RefreshNext)
		},
	)
	if err != nil {
		return finalizeFailure(err)
	}

	lastSuccessRefreshEndUnixSeconds := time.Now().Unix()
	if err := observeMVRefreshStep(e.stepObserver, stepSet.persistRefreshInfo, func() error {
		return persistRefreshSuccess(
			kctx,
			sqlExec,
			mviewID,
			lockedRefreshInfo.lastSuccessReadTSO,
			lockedRefreshInfo.lastSuccessReadTSONull,
			refreshReadTSO,
			lastSuccessRefreshEndUnixSeconds,
			nextRefreshUnixSeconds,
			shouldUpdateNextRefreshUnixSeconds,
		)
	}); err != nil {
		return finalizeFailure(err)
	}

	if err := observeMVRefreshStep(e.stepObserver, stepSet.txnCommit, func() error {
		_, commitErr := sqlExec.ExecuteInternal(kctx, "COMMIT")
		return commitErr
	}); err != nil {
		return finalizeFailure(err)
	}
	txnFinished = true
	if txnCommitTimerStarted && txnTotalDur == 0 {
		txnTotalDur = time.Since(txnCommitStart)
	}
	refreshCommitTSO, err := getSessionLastTxnCommitTSO(refreshSctx)
	if err != nil {
		e.Ctx().GetSessionVars().StmtCtx.AppendWarning(
			errors.Annotate(err, "refresh materialized view: refresh committed but failed to capture refresh commit tso"),
		)
		refreshCommitTSO = nil
	}
	applyMVRefreshStmtResult(e.Ctx().GetSessionVars().StmtCtx, refreshStmtResult)
	observeMVRefreshScheduleDuration(refreshScheduleDuration)
	finalizeSuccess(refreshReadTSO, refreshCommitTSO, refreshRows)
	return nil
}

func (e *RefreshMaterializedViewExec) executeRefreshMaterializedViewCompleteOutOfPlace(
	kctx context.Context,
	releaseCtx context.Context,
	s *ast.RefreshMaterializedViewStmt,
	refreshSctx sessionctx.Context,
	isInternalSQL bool,
	schemaName ast.CIStr,
	tblInfo *model.TableInfo,
	stepSet mvRefreshStepSet,
	expectedLastSuccessReadTSO uint64,
	expectedLastSuccessReadTSONull bool,
	targetExecutionVars variable.MViewExecutionSessionVars,
) (buildReadTSO uint64, err error) {
	const slowOverheadThreshold = 5 * time.Second
	refreshStart := time.Now()
	var createShadowDur, loadShadowDur, cutoverDur, cleanupShadowDur time.Duration
	defer func() {
		total := time.Since(refreshStart)
		failpoint.InjectCall("refreshMaterializedViewSlowLogDurations", &total, &loadShadowDur)
		overhead := total - loadShadowDur
		if overhead < slowOverheadThreshold {
			return
		}
		fields := []zap.Field{
			zap.Duration("total", total),
			zap.Duration("overhead", overhead),
			zap.Duration("slowOverheadThreshold", slowOverheadThreshold),
			zap.String("schema", schemaName.O),
			zap.String("mview", tblInfo.Name.O),
			zap.Int64("mviewID", tblInfo.ID),
			zap.Bool("internalSQL", isInternalSQL),
			zap.Bool("success", err == nil),
			zap.Duration("createShadow", createShadowDur),
			zap.Duration("loadShadow", loadShadowDur),
			zap.Duration("cutover", cutoverDur),
			zap.Duration("cleanupShadow", cleanupShadowDur),
		}
		if err != nil {
			fields = append(fields, zap.String("error", err.Error()))
		}
		logutil.BgLogger().Info("refresh materialized view complete out-of-place is slow", fields...)
	}()

	if err := kctx.Err(); err != nil {
		return 0, err
	}
	buildSctx, err := e.GetSysSession()
	if err != nil {
		return 0, err
	}
	defer e.ReleaseSysSession(releaseCtx, buildSctx)

	buildSessVars := buildSctx.GetSessionVars()
	restoreBuildExecutionVars, err := applyRefreshExecutionSessionVars(buildSessVars, targetExecutionVars, isInternalSQL)
	if err != nil {
		return 0, err
	}
	defer restoreBuildExecutionVars()
	failpoint.InjectCall("mvMaintainMemQuotaAppliedOnRefreshOutOfPlaceBuildSession", buildSessVars.MemQuotaQuery, targetExecutionVars.MaintainMemQuota)
	failpoint.InjectCall(
		"refreshMaterializedViewOutOfPlaceBuildIsolationReadEnginesApplied",
		variable.GetIsolationReadEnginesString(buildSessVars),
		targetExecutionVars.IsolationReadEngines,
	)
	failpoint.InjectCall(
		"refreshMaterializedViewOutOfPlaceBuildTiFlashSessionVarsApplied",
		buildSessVars.TiFlashMaxThreads,
		buildSessVars.TiFlashFineGrainedShuffleStreamCount,
		buildSessVars.TiFlashFineGrainedShuffleBatchSize,
	)
	failpoint.InjectCall(
		"refreshMaterializedViewOutOfPlaceBuildTiFlashSpillSessionVarsApplied",
		buildSessVars.TiFlashMaxBytesBeforeExternalJoin,
		buildSessVars.TiFlashMaxBytesBeforeExternalGroupBy,
		buildSessVars.TiFlashMaxBytesBeforeExternalSort,
		buildSessVars.TiFlashMaxQueryMemoryPerNode,
		buildSessVars.TiFlashQuerySpillRatio,
	)
	failpoint.InjectCall(
		"refreshMaterializedViewOutOfPlaceBuildImportSessionVarsApplied",
		buildSessVars.MViewMaintainImportThreads,
		buildSessVars.MViewMaintainImportDiskQuota,
	)

	restoreBuildSessVars, err := initRefreshMaterializedViewSession(buildSessVars, tblInfo.MaterializedView)
	if err != nil {
		return 0, err
	}
	defer restoreBuildSessVars()

	origInMViewMaintenance := buildSessVars.InMViewMaintenance
	buildSessVars.InMViewMaintenance = true
	defer func() {
		buildSessVars.InMViewMaintenance = origInMViewMaintenance
	}()

	if buildSessVars.InTxn() {
		return 0, errors.New("refresh materialized view complete OUT OF PLACE: build session unexpectedly in transaction")
	}

	shadowTableName := buildMVRefreshShadowTableName(tblInfo.ID)
	shadowNeedsCleanup := false
	shadowLoadStmtResult := mvRefreshStmtResult{}
	buildSQLExec := buildSctx.GetSQLExecutor()
	defer func() {
		// err can still be nil while a panic unwinds this function.
		if !shadowNeedsCleanup {
			return
		}
		cleanupShadowStart := time.Now()
		defer func() { cleanupShadowDur = time.Since(cleanupShadowStart) }()
		if dropErr := domain.GetDomain(e.Ctx()).DDLExecutor().DropMaterializedViewShadowTable(
			buildSctx, schemaName, ast.NewCIStr(shadowTableName),
		); dropErr != nil {
			logutil.BgLogger().Warn(
				"failed to cleanup shadow table after out-of-place complete refresh error",
				zap.String("schema", schemaName.O),
				zap.String("mview", s.ViewName.Name.O),
				zap.String("shadowTable", shadowTableName),
				zap.Error(dropErr),
			)
			if err != nil {
				err = errors.Annotatef(err, "cleanup shadow table %s.%s failed: %v", schemaName.O, shadowTableName, dropErr)
			}
		}
	}()

	shadowTableInfo, err := buildMVRefreshOutOfPlaceShadowTableInfo(schemaName, shadowTableName, tblInfo)
	if err != nil {
		return 0, err
	}
	if err := observeMVRefreshStep(e.stepObserver, stepSet.dataChangeOutOfPlaceCreateShadow, func() error {
		createShadowStart := time.Now()
		defer func() { createShadowDur = time.Since(createShadowStart) }()
		if err := kctx.Err(); err != nil {
			return err
		}
		ddlExecutor, ok := domain.GetDomain(e.Ctx()).DDLExecutor().(interface {
			CreateMaterializedViewShadowTable(sessionctx.Context, int64, ast.CIStr, *model.TableInfo) error
		})
		if !ok {
			return errors.New("materialized view complete out-of-place refresh is not supported")
		}
		if execErr := ddlExecutor.CreateMaterializedViewShadowTable(
			refreshSctx,
			tblInfo.DBID,
			schemaName,
			shadowTableInfo,
		); execErr != nil {
			return execErr
		}
		shadowNeedsCleanup = true
		return nil
	}); err != nil {
		return 0, err
	}

	failpoint.InjectCall("refreshMaterializedViewOutOfPlaceAfterCreateShadow")
	failpoint.Inject("pauseRefreshMaterializedViewOutOfPlaceAfterCreateShadow", func() {})
	storeName := e.Ctx().GetStore().Name()
	if err := observeMVRefreshStep(e.stepObserver, stepSet.dataChangeOutOfPlaceLoadShadow, func() error {
		loadShadowStart := time.Now()
		defer func() { loadShadowDur = time.Since(loadShadowStart) }()
		expectedStoreName := mvRefreshImportIntoStoreName
		caseSensitiveEqual := storeName == expectedStoreName
		buildMethod := "insert-into"
		if shouldUseImportIntoForMVRefreshOutOfPlace(storeName) {
			buildMethod = "import-into"
		}
		logutil.BgLogger().Info(
			"refresh materialized view complete out-of-place: choose shadow build method",
			zap.String("schema", schemaName.O),
			zap.String("mview", s.ViewName.Name.O),
			zap.String("storeName", storeName),
			zap.String("expectedStoreName", expectedStoreName),
			zap.Bool("caseSensitiveEqual", caseSensitiveEqual),
			zap.Bool("caseInsensitiveEqual", strings.EqualFold(storeName, expectedStoreName)),
			zap.String("method", buildMethod),
		)

		buildSQL, buildErr := buildMVRefreshOutOfPlaceBuildSQL(
			schemaName.O,
			shadowTableName,
			tblInfo,
			storeName,
			targetExecutionVars.ImportThreads,
			targetExecutionVars.ImportDiskQuota,
		)
		if buildErr != nil {
			return buildErr
		}
		if buildErr = executeRefreshMaterializedViewInternalSQL(kctx, buildSQLExec, buildSQL); buildErr != nil {
			return buildErr
		}
		shadowLoadStmtResult = newMVRefreshStmtResultFromWriteCounts(int64(buildSessVars.StmtCtx.AffectedRows()), 0, 0)
		// Capture profile rows for the real shadow-load statement before any follow-up SQL (for example read tso query)
		// overwrites session statement context.
		emitMVRefreshStepPlanRows(e.stepObserver, stepSet.dataChangeOutOfPlaceLoadShadow, buildSessVars, e.planFormatForObserver)
		buildReadTSO, buildErr = getMVRefreshOutOfPlaceBuildReadTSO(kctx, buildSQLExec)
		return buildErr
	}); err != nil {
		return 0, err
	}
	failpoint.Inject("mockRefreshMaterializedViewOutOfPlaceBuildErrorAfterLoadShadow", func(val failpoint.Value) {
		if msg, ok := val.(string); ok {
			failpoint.Return(uint64(0), errors.New(msg))
		}
		failpoint.Return(uint64(0), errors.New("mock refresh materialized view out-of-place build error after load shadow"))
	})
	failpoint.Inject("mockRefreshMaterializedViewOutOfPlaceBuildReadTSO", func(val failpoint.Value) {
		s, ok := val.(string)
		if !ok {
			return
		}
		overrideTSO, parseErr := strconv.ParseUint(s, 10, 64)
		if parseErr == nil && overrideTSO > 0 {
			buildReadTSO = overrideTSO
		}
	})
	failpoint.InjectCall("refreshMaterializedViewOutOfPlaceAfterBuildDataLoad", buildReadTSO)
	failpoint.Inject("pauseRefreshMaterializedViewOutOfPlaceAfterBuildDataLoad", func() {})
	var shadowTableID int64
	expectedOldMViewRevision := tblInfo.Revision
	if err := observeMVRefreshStep(e.stepObserver, stepSet.dataChangeOutOfPlaceCutover, func() error {
		cutoverStart := time.Now()
		defer func() { cutoverDur = time.Since(cutoverStart) }()
		var lookupErr error
		shadowTableID, lookupErr = getMVRefreshOutOfPlaceShadowTableID(kctx, buildSctx, schemaName, shadowTableName)
		if lookupErr != nil {
			return lookupErr
		}
		var nextRefreshUnixSeconds *int64
		var shouldUpdateNextRefreshUnixSeconds bool
		if isInternalSQL {
			scheduleEvalSctx, scheduleErr := e.GetSysSession()
			if scheduleErr != nil {
				return scheduleErr
			}
			defer e.ReleaseSysSession(releaseCtx, scheduleEvalSctx)
			nextRefreshUnixSeconds, shouldUpdateNextRefreshUnixSeconds, scheduleErr = deriveRuntimeMaterializedScheduleNextUnixSeconds(
				kctx,
				scheduleEvalSctx,
				tblInfo.MaterializedView.RefreshNext,
				isInternalSQL,
				tblInfo.MaterializedView.RefreshScheduleSQLMode,
				func() {
					logRuntimeMaterializedViewRefreshNextUnixSecondsUpdateNull(schemaName.O, tblInfo.Name.O, tblInfo.MaterializedView.RefreshNext)
				},
			)
			if scheduleErr != nil {
				return scheduleErr
			}
		}
		if err := kctx.Err(); err != nil {
			return err
		}
		ddlExecutor, ok := domain.GetDomain(e.Ctx()).DDLExecutor().(interface {
			RefreshMaterializedViewCompleteOutOfPlaceCutover(sessionctx.Context, int64, ast.CIStr, ast.CIStr, int64, int64, uint64, *uint64, uint64, bool, *int64, bool) error
		})
		if !ok {
			return errors.New("materialized view complete out-of-place refresh is not supported")
		}
		if err := ddlExecutor.RefreshMaterializedViewCompleteOutOfPlaceCutover(
			e.Ctx(),
			tblInfo.DBID,
			schemaName,
			s.ViewName.Name,
			tblInfo.ID,
			shadowTableID,
			buildReadTSO,
			&expectedOldMViewRevision,
			expectedLastSuccessReadTSO,
			expectedLastSuccessReadTSONull,
			nextRefreshUnixSeconds,
			shouldUpdateNextRefreshUnixSeconds,
		); err != nil {
			return err
		}
		// Disarm cleanup before observeMVRefreshStep invokes OnStepEnd.
		shadowNeedsCleanup = false
		return nil
	}); err != nil {
		return 0, err
	}
	applyMVRefreshStmtResult(refreshSctx.GetSessionVars().StmtCtx, shadowLoadStmtResult)
	return buildReadTSO, nil
}

func applyMVMaintenanceMemQuota(sessVars *variable.SessionVars, targetMemQuota int64, bestEffort bool) (func(), error) {
	if sessVars == nil {
		return nil, errors.New("mv maintenance: session vars is nil")
	}
	originMemQuota := sessVars.MemQuotaQuery
	if originMemQuota == targetMemQuota {
		return func() {}, nil
	}
	var injectedErr error
	failpoint.Inject("mockMVMaintenanceMemQuotaApplyError", func() {
		injectedErr = errors.New("mock mv maintenance mem quota apply error")
	})
	err := injectedErr
	if err == nil {
		err = sessVars.SetSystemVar(vardef.TiDBMemQuotaQuery, strconv.FormatInt(targetMemQuota, 10))
	}
	if err != nil {
		if !bestEffort {
			return nil, errors.Annotate(err, "mv maintenance: failed to apply tidb_mv_maintain_mem_quota to tidb_mem_quota_query")
		}
		logutil.BgLogger().Warn(
			"mv maintenance: failed to apply tidb_mv_maintain_mem_quota to tidb_mem_quota_query, fallback to current session value",
			zap.Int64("originMemQuota", originMemQuota),
			zap.Int64("targetMemQuota", targetMemQuota),
			zap.Error(err),
		)
		return func() {}, nil
	}
	return func() {
		if err := sessVars.SetSystemVar(vardef.TiDBMemQuotaQuery, strconv.FormatInt(originMemQuota, 10)); err != nil {
			logutil.BgLogger().Warn(
				"mv maintenance: failed to restore tidb_mem_quota_query after using tidb_mv_maintain_mem_quota",
				zap.Int64("originMemQuota", originMemQuota),
				zap.Int64("targetMemQuota", targetMemQuota),
				zap.Error(err),
			)
		}
	}, nil
}

func applyMVMaintenanceSessionVars(
	sessVars *variable.SessionVars,
	targetMemQuota int64,
	targetIsolationReadEngines string,
	bestEffort bool,
) (func(), error) {
	restoreMemQuota, err := applyMVMaintenanceMemQuota(sessVars, targetMemQuota, bestEffort)
	if err != nil {
		return nil, err
	}
	restoreIsolationReadEngines, err := applyMVMaintenanceIsolationReadEngines(sessVars, targetIsolationReadEngines, bestEffort)
	if err != nil {
		restoreMemQuota()
		return nil, err
	}
	return func() {
		restoreIsolationReadEngines()
		restoreMemQuota()
	}, nil
}

func applyMVMaintenanceIsolationReadEngines(
	sessVars *variable.SessionVars,
	targetIsolationReadEngines string,
	bestEffort bool,
) (func(), error) {
	if sessVars == nil {
		return nil, errors.New("mv maintenance: session vars is nil")
	}
	originIsolationReadEngines := variable.GetIsolationReadEnginesString(sessVars)
	if originIsolationReadEngines == targetIsolationReadEngines {
		return func() {}, nil
	}
	if err := sessVars.SetSystemVar(vardef.TiDBIsolationReadEngines, targetIsolationReadEngines); err != nil {
		if !bestEffort {
			return nil, errors.Annotate(
				err,
				"mv maintenance: failed to apply tidb_mv_maintain_isolation_read_engines to tidb_isolation_read_engines",
			)
		}
		logutil.BgLogger().Warn(
			"mv maintenance: failed to apply tidb_mv_maintain_isolation_read_engines to tidb_isolation_read_engines, fallback to current session value",
			zap.String("originIsolationReadEngines", originIsolationReadEngines),
			zap.String("targetIsolationReadEngines", targetIsolationReadEngines),
			zap.Error(err),
		)
		return func() {}, nil
	}
	return func() {
		if err := sessVars.SetSystemVar(vardef.TiDBIsolationReadEngines, originIsolationReadEngines); err != nil {
			logutil.BgLogger().Warn(
				"mv maintenance: failed to restore tidb_isolation_read_engines after maintenance",
				zap.String("originIsolationReadEngines", originIsolationReadEngines),
				zap.String("currentIsolationReadEngines", targetIsolationReadEngines),
				zap.Error(err),
			)
		}
	}, nil
}

func captureRefreshExecutionSessionVars(sessVars *variable.SessionVars) variable.MViewExecutionSessionVars {
	return variable.CaptureMViewExecutionSessionVars(sessVars)
}

func applyRefreshExecutionSessionVars(
	sessVars *variable.SessionVars,
	target variable.MViewExecutionSessionVars,
	bestEffort bool,
) (func(), error) {
	var injectedErr error
	failpoint.Inject("mockRefreshExecutionSessionVarsApplyError", func() {
		injectedErr = errors.New("mock refresh execution session vars apply error")
	})

	var (
		restore func()
		err     error
	)
	if injectedErr != nil {
		err = injectedErr
	} else if bestEffort {
		restore, err = ddl.ApplyMViewExecutionSessionVarsBestEffort(sessVars, target)
	} else {
		restore, err = ddl.ApplyMViewExecutionSessionVars(sessVars, target)
	}
	if err != nil {
		if !bestEffort {
			return nil, err
		}
		logutil.BgLogger().Warn(
			"refresh materialized view: failed to apply execution session vars, fallback to internal session defaults",
			zap.Error(err),
		)
		return func() {}, nil
	}
	failpoint.InjectCall(
		"refreshMaterializedViewTiFlashSessionVarsApplied",
		sessVars.TiFlashMaxThreads,
		sessVars.TiFlashFineGrainedShuffleStreamCount,
		sessVars.TiFlashFineGrainedShuffleBatchSize,
	)
	failpoint.InjectCall(
		"refreshMaterializedViewTiFlashSpillSessionVarsApplied",
		sessVars.TiFlashMaxBytesBeforeExternalJoin,
		sessVars.TiFlashMaxBytesBeforeExternalGroupBy,
		sessVars.TiFlashMaxBytesBeforeExternalSort,
		sessVars.TiFlashMaxQueryMemoryPerNode,
		sessVars.TiFlashQuerySpillRatio,
	)
	return restore, nil
}

func buildMVRefreshShadowTableName(mviewID int64) string {
	return fmt.Sprintf("%s%d_%d", mvRefreshShadowTablePrefix, mviewID, time.Now().UnixNano())
}

func buildMVRefreshOutOfPlaceShadowTableInfo(
	schemaName ast.CIStr,
	shadowTableName string,
	tblInfo *model.TableInfo,
) (*model.TableInfo, error) {
	if tblInfo == nil || tblInfo.MaterializedView == nil {
		return nil, errors.New("refresh materialized view complete OUT OF PLACE: invalid materialized view metadata")
	}
	shadowTableInfo, err := ddl.BuildTableInfoWithLike(
		ast.Ident{Schema: schemaName, Name: ast.NewCIStr(shadowTableName)},
		tblInfo,
		&ast.CreateTableStmt{},
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	shadowTableInfo.MaterializedViewShadow = &model.MaterializedViewShadowInfo{SourceMViewID: tblInfo.ID}
	return shadowTableInfo, nil
}

func buildMVRefreshOutOfPlaceBuildSQL(
	schemaName string,
	shadowTableName string,
	tblInfo *model.TableInfo,
	storeName string,
	importThreads int,
	importDiskQuota string,
) (string, error) {
	if tblInfo.MaterializedView == nil || len(tblInfo.MaterializedView.SQLContent) == 0 {
		return "", errors.New("refresh materialized view: invalid select sql")
	}
	selectSQL := tblInfo.MaterializedView.SQLContent
	if shouldUseImportIntoForMVRefreshOutOfPlace(storeName) {
		prefix := sqlescape.MustEscapeSQL("IMPORT INTO %n.%n FROM ", schemaName, shadowTableName)
		options := ddl.BuildMViewImportIntoOptions(importThreads, importDiskQuota)
		/* #nosec G202: SQLContent is restored from AST (single SELECT statement, no user-provided placeholders). */
		return prefix + "(" + selectSQL + ") WITH " + strings.Join(options, ", "), nil
	}
	prefix := sqlescape.MustEscapeSQL("INSERT INTO %n.%n ", schemaName, shadowTableName)
	/* #nosec G202: SQLContent is restored from AST (single SELECT statement, no user-provided placeholders). */
	return prefix + selectSQL, nil
}

func shouldUseImportIntoForMVRefreshOutOfPlace(storeName string) bool {
	return storeName == mvRefreshImportIntoStoreName
}

func getMVRefreshOutOfPlaceBuildReadTSO(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
) (uint64, error) {
	rs, err := sqlExec.ExecuteInternal(
		kctx,
		"SELECT COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(@@tidb_last_query_info, '$.start_ts')) AS UNSIGNED), CAST(0 AS UNSIGNED))",
	)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if rs == nil {
		return 0, errors.New("refresh materialized view complete OUT OF PLACE: cannot fetch build read tso")
	}
	rows, drainErr := sqlexec.DrainRecordSet(kctx, rs, 1)
	closeErr := rs.Close()
	if drainErr != nil {
		return 0, errors.Trace(drainErr)
	}
	if closeErr != nil {
		return 0, errors.Trace(closeErr)
	}
	if len(rows) == 0 {
		return 0, errors.New("refresh materialized view complete OUT OF PLACE: cannot fetch build read tso")
	}
	buildReadTSO := rows[0].GetUint64(0)
	if buildReadTSO == 0 {
		return 0, errors.New("refresh materialized view complete OUT OF PLACE: invalid build read tso")
	}
	return buildReadTSO, nil
}

func getMVRefreshOutOfPlaceShadowTableID(
	kctx context.Context,
	sctx sessionctx.Context,
	schemaName ast.CIStr,
	shadowTableName string,
) (int64, error) {
	is := sctx.GetLatestInfoSchema().(infoschema.InfoSchema)
	shadowTbl, err := is.TableByName(kctx, schemaName, ast.NewCIStr(shadowTableName))
	if err != nil {
		if infoschema.ErrTableNotExists.Equal(err) {
			return 0, errors.New("refresh materialized view complete OUT OF PLACE: cannot resolve shadow table id")
		}
		return 0, errors.Trace(err)
	}
	shadowTableID := shadowTbl.Meta().ID
	if shadowTableID == 0 {
		return 0, errors.New("refresh materialized view complete OUT OF PLACE: invalid shadow table id")
	}
	return shadowTableID, nil
}

func initRefreshMaterializedViewSession(
	sessVars *variable.SessionVars,
	mviewInfo *model.MaterializedViewInfo,
) (func(), error) {
	if mviewInfo == nil {
		return nil, errors.New("refresh materialized view: invalid materialized view metadata")
	}
	timezone := mviewInfo.DefinitionTimeZone.Clone()
	loc, err := timezone.GetLocation()
	if err != nil {
		return nil, errors.Annotate(err, "refresh materialized view: invalid definition timezone")
	}

	origEnableMView := sessVars.EnableMView
	if err := sessVars.SetSystemVar(vardef.TiDBMViewEnable, "on"); err != nil {
		return nil, errors.Annotate(err, "refresh materialized view: failed to enable materialized view support")
	}
	origSQLMode := sessVars.SQLMode
	origDivPrecisionIncrement := sessVars.DivPrecisionIncrement
	origTimeZone := sessVars.TimeZone
	origMViewMaintenance := sessVars.InMViewMaintenance
	origStmtCtxTimeZone := sessVars.StmtCtx.TimeZone()
	origTypeFlags := sessVars.StmtCtx.TypeFlags()
	origErrLevels := sessVars.StmtCtx.ErrLevels()

	sessVars.SQLMode = mviewInfo.DefinitionSQLMode
	sessVars.DivPrecisionIncrement = mviewInfo.DefinitionDivPrecisionIncrement
	sessVars.InMViewMaintenance = true
	sessVars.SetStatusFlag(mysql.ServerStatusNoBackslashEscaped, sessVars.SQLMode.HasNoBackslashEscapesMode())
	sessVars.TimeZone = loc
	sessVars.StmtCtx.SetTimeZone(loc)
	sessVars.StmtCtx.SetTypeFlags(expression.MaterializedScheduleTypeFlagsWithSQLMode(sessVars.SQLMode))
	sessVars.StmtCtx.SetErrLevels(expression.MaterializedScheduleErrLevelsWithSQLMode(sessVars.SQLMode))

	return func() {
		if err := sessVars.SetSystemVar(vardef.TiDBMViewEnable, variable.BoolToOnOff(origEnableMView)); err != nil {
			logutil.BgLogger().Warn("refresh materialized view: failed to restore materialized view enablement", zap.Error(err))
		}
		sessVars.SQLMode = origSQLMode
		sessVars.DivPrecisionIncrement = origDivPrecisionIncrement
		sessVars.InMViewMaintenance = origMViewMaintenance
		sessVars.SetStatusFlag(mysql.ServerStatusNoBackslashEscaped, origSQLMode.HasNoBackslashEscapesMode())
		sessVars.TimeZone = origTimeZone
		sessVars.StmtCtx.SetTimeZone(origStmtCtxTimeZone)
		sessVars.StmtCtx.SetTypeFlags(origTypeFlags)
		sessVars.StmtCtx.SetErrLevels(origErrLevels)
	}, nil
}

func validateRefreshMaterializedViewStmt(s *ast.RefreshMaterializedViewStmt, isInternalSQL bool) (ast.RefreshMaterializedViewMode, string, error) {
	if s == nil || s.ViewName == nil {
		return 0, "", errors.New("refresh materialized view: missing view name")
	}
	mode, err := s.Mode()
	if err != nil {
		return 0, "", errors.Trace(err)
	}
	methodType := ""
	switch mode {
	case ast.RefreshMaterializedViewModeFast:
		// Framework is supported; actual execution happens via RefreshMaterializedViewImplementStmt.
		methodType = "fast"
		if s.AsOf != nil {
			methodType = "bounded fast"
		}
	case ast.RefreshMaterializedViewModeCompleteDeltaApply:
		methodType = "complete delta apply"
	case ast.RefreshMaterializedViewModeCompleteInPlace:
		methodType = "complete in place"
	case ast.RefreshMaterializedViewModeCompleteOutOfPlace:
		methodType = "complete out of place"
	default:
		return 0, "", errors.New("refresh materialized view: unknown mode")
	}
	methodOrigin := "manual"
	if isInternalSQL {
		methodOrigin = "auto"
	}
	if s.WithAsyncMode {
		return 0, "", errors.New("refresh materialized view: WITH ASYNC MODE is not supported yet")
	}
	if s.AsOf != nil && mode != ast.RefreshMaterializedViewModeFast {
		return 0, "", errors.New("refresh materialized view: AS OF TIMESTAMP is only supported for FAST refresh")
	}
	return mode, methodType + " " + methodOrigin, nil
}

func refreshHistReadTSOOnFailure(s *ast.RefreshMaterializedViewStmt, targetRefreshReadTSO uint64) *uint64 {
	if s == nil || s.AsOf == nil || targetRefreshReadTSO == 0 {
		return nil
	}
	failedReadTSO := targetRefreshReadTSO
	return &failedReadTSO
}

func evaluateRefreshMaterializedViewTargetTSO(
	kctx context.Context,
	sctx sessionctx.Context,
	s *ast.RefreshMaterializedViewStmt,
) (uint64, error) {
	if s == nil || s.AsOf == nil {
		return 0, nil
	}
	targetTSO, err := staleread.CalculateAsOfTsExpr(kctx, sctx.GetPlanCtx(), s.AsOf.TsExpr)
	if err != nil {
		return 0, err
	}
	if err := sessionctx.ValidateSnapshotReadTS(kctx, sctx.GetStore(), targetTSO, true); err != nil {
		return 0, err
	}
	return targetTSO, nil
}

func (e *RefreshMaterializedViewExec) resolveRefreshMaterializedViewTarget(
	s *ast.RefreshMaterializedViewStmt,
) (ast.CIStr, *model.TableInfo, error) {
	is := e.Ctx().GetLatestInfoSchema().(infoschema.InfoSchema)
	schemaName := s.ViewName.Schema
	if schemaName.O == "" {
		if e.Ctx().GetSessionVars().CurrentDB == "" {
			return ast.CIStr{}, nil, errors.Trace(plannererrors.ErrNoDB)
		}
		schemaName = ast.NewCIStr(e.Ctx().GetSessionVars().CurrentDB)
		s.ViewName.Schema = schemaName
	}
	if _, ok := is.SchemaByName(schemaName); !ok {
		return ast.CIStr{}, nil, infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schemaName)
	}

	tbl, err := is.TableByName(context.Background(), schemaName, s.ViewName.Name)
	if err != nil {
		return ast.CIStr{}, nil, err
	}
	tblInfo := tbl.Meta()
	if tblInfo.MaterializedView == nil {
		return ast.CIStr{}, nil, dbterror.ErrWrongObject.GenWithStackByArgs(schemaName.O, s.ViewName.Name.O, "MATERIALIZED VIEW")
	}
	if len(tblInfo.MaterializedView.SQLContent) == 0 {
		return ast.CIStr{}, nil, errors.New("refresh materialized view: invalid select sql")
	}
	if err := checkRefreshMaterializedViewReady(schemaName, tblInfo); err != nil {
		return ast.CIStr{}, nil, err
	}
	return schemaName, tblInfo, nil
}

func resolveRefreshMaterializedViewLogInfo(
	is infoschema.InfoSchema,
	schemaName ast.CIStr,
	tblInfo *model.TableInfo,
) (int64, error) {
	if tblInfo == nil || tblInfo.MaterializedView == nil {
		return 0, errors.New("refresh materialized view: target is not a materialized view")
	}
	if len(tblInfo.MaterializedView.BaseTableIDs) != 1 {
		return 0, errors.New("refresh materialized view: fast refresh requires exactly one base table")
	}

	baseTableID := tblInfo.MaterializedView.BaseTableIDs[0]
	baseTable, ok := is.TableByID(context.Background(), baseTableID)
	if !ok {
		return 0, errors.Errorf("refresh materialized view: cannot resolve base table %d for materialized view %s.%s", baseTableID, schemaName.O, tblInfo.Name.O)
	}
	baseTableInfo := baseTable.Meta()
	if baseTableInfo.MaterializedViewBase == nil {
		return 0, errors.Errorf("refresh materialized view: base table %d is missing materialized view base metadata", baseTableID)
	}
	mlogID := baseTableInfo.MaterializedViewBase.MLogID
	if mlogID == 0 {
		return 0, errors.Errorf(
			"refresh materialized view: materialized view log does not exist for base table %s.%s",
			schemaName.O,
			baseTableInfo.Name.O,
		)
	}
	return mlogID, nil
}

func checkRefreshMaterializedViewReady(schemaName ast.CIStr, tblInfo *model.TableInfo) error {
	if tblInfo == nil || tblInfo.MaterializedView == nil {
		return nil
	}
	initBuildState := tblInfo.MaterializedView.GetInitBuildState()
	if initBuildState.IsReady() {
		return nil
	}
	objectName := tblInfo.Name.O
	if schemaName.O != "" {
		objectName = schemaName.O + "." + objectName
	}
	return errors.New(initBuildState.AccessErrorMessage(objectName))
}

type refreshInfoSnapshot struct {
	lastSuccessReadTSO                   uint64
	lastSuccessReadTSONull               bool
	lastSuccessRefreshEndUnixSeconds     int64
	lastSuccessRefreshEndUnixSecondsNull bool
}

func (s refreshInfoSnapshot) previousSuccessTime() time.Time {
	if !s.lastSuccessRefreshEndUnixSecondsNull {
		return time.Unix(s.lastSuccessRefreshEndUnixSeconds, 0)
	}
	if !s.lastSuccessReadTSONull && s.lastSuccessReadTSO > 0 {
		return time.UnixMilli(oracle.ExtractPhysical(s.lastSuccessReadTSO))
	}
	return time.Time{}
}

func buildRefreshScheduleDuration(isInternalSQL bool, refreshStart time.Time, info refreshInfoSnapshot) *time.Duration {
	if !isInternalSQL {
		return nil
	}
	previousSuccessTime := info.previousSuccessTime()
	if previousSuccessTime.IsZero() {
		return nil
	}
	// Defined as current success time - previous success time - current refresh duration,
	// which is equivalent to current refresh start time - previous success time.
	duration := refreshStart.Sub(previousSuccessTime)
	if duration < 0 {
		return nil
	}
	return &duration
}

func observeMVRefreshScheduleDuration(duration *time.Duration) {
	if duration == nil || *duration < 0 {
		return
	}
	tidbmetrics.MVServiceRefreshScheduleDurationHistogram.Observe(duration.Seconds())
}

func decodeRefreshInfoSnapshot(row chunk.Row, readTSOIdx int, endUnixSecondsIdx int) refreshInfoSnapshot {
	info := refreshInfoSnapshot{
		lastSuccessReadTSONull:               true,
		lastSuccessRefreshEndUnixSecondsNull: true,
	}
	if !row.IsNull(readTSOIdx) {
		info.lastSuccessReadTSO = row.GetUint64(readTSOIdx)
		info.lastSuccessReadTSONull = false
	}
	if !row.IsNull(endUnixSecondsIdx) {
		info.lastSuccessRefreshEndUnixSeconds = row.GetInt64(endUnixSecondsIdx)
		info.lastSuccessRefreshEndUnixSecondsNull = false
	}
	return info
}

func lockRefreshInfoRow(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	mviewID int64,
) (refreshInfoSnapshot, error) {
	lockRS, err := sqlExec.ExecuteInternal(
		kctx,
		// Also select LAST_SUCCESS_READ_TSO so FAST refresh can reuse this mutex/metadata load path.
		"SELECT MVIEW_ID, LAST_SUCCESS_READ_TSO, LAST_SUCCESS_REFRESH_END_UNIX_SECONDS FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID = %? FOR UPDATE NOWAIT",
		mviewID,
	)
	if infoschema.ErrTableNotExists.Equal(err) {
		return refreshInfoSnapshot{}, errors.New("refresh materialized view: required system table mysql.tidb_mview_refresh_info does not exist")
	}
	if err != nil {
		return refreshInfoSnapshot{}, errors.Trace(err)
	}
	if lockRS == nil {
		return refreshInfoSnapshot{}, errors.New("refresh materialized view: cannot lock mysql.tidb_mview_refresh_info row")
	}
	lockRows, drainErr := sqlexec.DrainRecordSet(kctx, lockRS, 1)
	closeErr := lockRS.Close()
	if drainErr != nil {
		return refreshInfoSnapshot{}, errors.Trace(drainErr)
	}
	if closeErr != nil {
		return refreshInfoSnapshot{}, errors.Trace(closeErr)
	}
	if len(lockRows) == 0 {
		return refreshInfoSnapshot{}, errors.New("refresh materialized view: refresh info row missing in mysql.tidb_mview_refresh_info")
	}

	return decodeRefreshInfoSnapshot(lockRows[0], 1, 2), nil
}

func buildMVRefreshAdvisoryLockName(schemaID int64, mviewID int64) string {
	return fmt.Sprintf("mv_refresh_%d_%d", schemaID, mviewID)
}

func acquireMVRefreshAdvisoryLock(
	refreshSctx sessionctx.Context,
	schemaName ast.CIStr,
	tblInfo *model.TableInfo,
) (string, error) {
	lockName := buildMVRefreshAdvisoryLockName(tblInfo.DBID, tblInfo.ID)
	if err := refreshSctx.GetAdvisoryLock(lockName, mvRefreshAdvisoryLockTimeoutSec); err != nil {
		if isMVRefreshAdvisoryLockConflict(err) {
			return lockName, errors.Annotatef(
				errMVRefreshAdvisoryLockConflict,
				"another refresh is running for materialized view %s.%s, please retry later",
				schemaName.O,
				tblInfo.Name.O,
			)
		}
		return lockName, errors.Trace(err)
	}
	return lockName, nil
}

func isMVRefreshAdvisoryLockConflict(err error) bool {
	return err != nil && storeerr.ErrLockWaitTimeout.Equal(err)
}

func releaseMVRefreshAdvisoryLockFully(refreshSctx sessionctx.Context, lockName string) int {
	failpoint.Inject("mockReleaseMVRefreshAdvisoryLockFullyCount", func(val failpoint.Value) {
		switch v := val.(type) {
		case int:
			failpoint.Return(v)
		case int64:
			failpoint.Return(int(v))
		}
	})
	releasedCnt := 0
	for refreshSctx.ReleaseAdvisoryLock(lockName) {
		releasedCnt++
	}
	return releasedCnt
}

func readRefreshInfoSnapshot(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	mviewID int64,
) (refreshInfoSnapshot, error) {
	recheckRS, err := sqlExec.ExecuteInternal(
		kctx,
		"SELECT LAST_SUCCESS_READ_TSO, LAST_SUCCESS_REFRESH_END_UNIX_SECONDS FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID = %?",
		mviewID,
	)
	if err != nil {
		if infoschema.ErrTableNotExists.Equal(err) {
			return refreshInfoSnapshot{}, errors.New("refresh materialized view: required system table mysql.tidb_mview_refresh_info does not exist")
		}
		return refreshInfoSnapshot{}, errors.Trace(err)
	}
	if recheckRS == nil {
		return refreshInfoSnapshot{}, errors.New("refresh materialized view: cannot read mysql.tidb_mview_refresh_info row")
	}
	recheckRows, drainErr := sqlexec.DrainRecordSet(kctx, recheckRS, 1)
	closeErr := recheckRS.Close()
	if drainErr != nil {
		return refreshInfoSnapshot{}, errors.Trace(drainErr)
	}
	if closeErr != nil {
		return refreshInfoSnapshot{}, errors.Trace(closeErr)
	}
	if len(recheckRows) == 0 {
		return refreshInfoSnapshot{}, errors.New("refresh materialized view: refresh info row missing in mysql.tidb_mview_refresh_info")
	}
	return decodeRefreshInfoSnapshot(recheckRows[0], 0, 1), nil
}

func readMLogPurgeInfoLastPurgedTSO(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	mlogID int64,
) (lastPurgedTSO uint64, hasLastPurgedTSO bool, err error) {
	rows, err := sqlexec.ExecSQL(
		kctx,
		sqlExec,
		"SELECT LAST_PURGED_TSO FROM mysql.tidb_mlog_purge_info WHERE MLOG_ID = %?",
		mlogID,
	)
	if err != nil {
		if infoschema.ErrTableNotExists.Equal(err) {
			return 0, false, errors.New("refresh materialized view: required system table mysql.tidb_mlog_purge_info does not exist")
		}
		return 0, false, errors.Trace(err)
	}
	if len(rows) == 0 {
		return 0, false, errors.Errorf("refresh materialized view: mlog purge info row missing for mlog id %d", mlogID)
	}
	if rows[0].IsNull(0) {
		return 0, false, nil
	}
	return rows[0].GetUint64(0), true, nil
}

// readLatestHazardousMLogPurgeCutoffTSO relies on purge-side monotonic cutoff
// enforcement: purge skips before writing history if its newly computed
// safePurgeTSO is lower than the latest recorded non-NULL PURGE_CUTOFF_TSO.
func readLatestHazardousMLogPurgeCutoffTSO(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	mlogID int64,
) (purgeCutoffTSO uint64, hasHazard bool, err error) {
	rows, err := sqlexec.ExecSQL(
		kctx,
		sqlExec,
		`SELECT PURGE_CUTOFF_TSO
FROM mysql.tidb_mlog_purge_hist
WHERE MLOG_ID = %?
  AND (
    PURGE_STATUS = %?
    OR PURGE_STATUS = %?
    OR PURGE_ROWS > 0
  )
ORDER BY PURGE_JOB_ID DESC
LIMIT 1`,
		mlogID,
		mvTaskHistStatusRunning,
		mvTaskHistStatusOrphaned,
	)
	if err != nil {
		if infoschema.ErrTableNotExists.Equal(err) {
			return 0, false, errors.New("refresh materialized view: required system table mysql.tidb_mlog_purge_hist does not exist")
		}
		return 0, false, errors.Trace(err)
	}
	if len(rows) == 0 || rows[0].IsNull(0) {
		return 0, false, nil
	}
	return rows[0].GetUint64(0), true, nil
}

type fastRefreshMLogIntegrity struct {
	hazardFenceTSO      uint64
	retainedLowerTSO    uint64
	hasRetainedLowerTSO bool
}

func checkFastRefreshMLogIntegrity(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	is infoschema.InfoSchema,
	schemaName ast.CIStr,
	tblInfo *model.TableInfo,
	lastSuccessfulRefreshReadTSO uint64,
) (fastRefreshMLogIntegrity, error) {
	mlogID, err := resolveRefreshMaterializedViewLogInfo(is, schemaName, tblInfo)
	if err != nil {
		return fastRefreshMLogIntegrity{}, err
	}

	lastPurgedTSO, hasLastPurgedTSO, err := readMLogPurgeInfoLastPurgedTSO(kctx, sqlExec, mlogID)
	if err != nil {
		return fastRefreshMLogIntegrity{}, err
	}
	latestHazardCutoffTSO, hasLatestHazardCutoffTSO, err := readLatestHazardousMLogPurgeCutoffTSO(kctx, sqlExec, mlogID)
	if err != nil {
		return fastRefreshMLogIntegrity{}, err
	}

	hazardFenceTSO := uint64(0)
	if hasLastPurgedTSO {
		hazardFenceTSO = lastPurgedTSO
	}
	if hasLatestHazardCutoffTSO && latestHazardCutoffTSO > hazardFenceTSO {
		hazardFenceTSO = latestHazardCutoffTSO
	}
	if hazardFenceTSO > lastSuccessfulRefreshReadTSO {
		return fastRefreshMLogIntegrity{}, errors.Errorf(
			"refresh materialized view fast: materialized view log may have been purged beyond LAST_SUCCESS_READ_TSO (hazard tso %d, LAST_SUCCESS_READ_TSO %d)",
			hazardFenceTSO,
			lastSuccessfulRefreshReadTSO,
		)
	}
	return fastRefreshMLogIntegrity{
		hazardFenceTSO:      hazardFenceTSO,
		retainedLowerTSO:    hazardFenceTSO,
		hasRetainedLowerTSO: hazardFenceTSO > 0,
	}, nil
}

type refreshImplementOptions struct {
	lastSuccessfulRefreshReadTSO uint64
	targetRefreshReadTSO         uint64
	mlogRetainedLowerTSO         uint64
}

func executeRefreshMaterializedViewDataChanges(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	sessVars *variable.SessionVars,
	s *ast.RefreshMaterializedViewStmt,
	refreshMode ast.RefreshMaterializedViewMode,
	schemaName ast.CIStr,
	tblInfo *model.TableInfo,
	implementOpts refreshImplementOptions,
	stepSet mvRefreshStepSet,
	stepObserver mvRefreshStepObserver,
	explainFormat string,
	executeDataChangesDur *time.Duration,
) error {
	executeDataChangesStart := time.Now()
	defer func() {
		*executeDataChangesDur = time.Since(executeDataChangesStart)
	}()
	// TiFlash read is blocked for write statements when sql_mode is strict. Refresh prefers TiFlash for the
	// scan part, so we bypass this guard for MV maintenance statements.
	origInMViewMaintenance := sessVars.InMViewMaintenance
	sessVars.InMViewMaintenance = true
	defer func() {
		sessVars.InMViewMaintenance = origInMViewMaintenance
	}()

	switch refreshMode {
	case ast.RefreshMaterializedViewModeCompleteInPlace:
		return executeRefreshMaterializedViewCompleteInPlace(
			kctx,
			sqlExec,
			sessVars,
			s,
			schemaName,
			tblInfo,
			stepSet,
			stepObserver,
			explainFormat,
		)
	case ast.RefreshMaterializedViewModeFast:
		return executeRefreshMaterializedViewFast(
			kctx,
			sqlExec,
			sessVars,
			s,
			implementOpts,
			stepSet,
			stepObserver,
			explainFormat,
		)
	case ast.RefreshMaterializedViewModeCompleteOutOfPlace:
		return errors.New("refresh materialized view: complete OUT OF PLACE should use dedicated execution path")
	case ast.RefreshMaterializedViewModeCompleteDeltaApply:
		return executeRefreshMaterializedViewCompleteDeltaApply(
			kctx,
			sqlExec,
			sessVars,
			s,
			stepSet,
			stepObserver,
			explainFormat,
		)
	default:
		return errors.New("refresh materialized view: unknown mode")
	}
}

func executeRefreshMaterializedViewCompleteInPlace(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	sessVars *variable.SessionVars,
	s *ast.RefreshMaterializedViewStmt,
	schemaName ast.CIStr,
	tblInfo *model.TableInfo,
	stepSet mvRefreshStepSet,
	stepObserver mvRefreshStepObserver,
	explainFormat string,
) error {
	deleteSQL := sqlescape.MustEscapeSQL("DELETE FROM %n.%n", schemaName.O, s.ViewName.Name.O)
	insertPrefix := sqlescape.MustEscapeSQL("INSERT INTO %n.%n ", schemaName.O, s.ViewName.Name.O)
	/* #nosec G202: SQLContent is restored from AST (single SELECT statement, no user-provided placeholders). */
	insertSQL := insertPrefix + tblInfo.MaterializedView.SQLContent
	deleteRows := int64(0)
	if err := observeMVRefreshStep(stepObserver, stepSet.dataChangeCompleteDelete, func() error {
		_, deleteErr := sqlExec.ExecuteInternal(kctx, deleteSQL)
		if deleteErr == nil && sessVars != nil && sessVars.StmtCtx != nil {
			deleteRows = int64(sessVars.StmtCtx.AffectedRows())
		}
		return deleteErr
	}); err != nil {
		return err
	}
	emitMVRefreshStepPlanRows(stepObserver, stepSet.dataChangeCompleteDelete, sessVars, explainFormat)

	insertRows := int64(0)
	if err := observeMVRefreshStep(stepObserver, stepSet.dataChangeCompleteInsert, func() error {
		_, insertErr := sqlExec.ExecuteInternal(kctx, insertSQL)
		if insertErr == nil && sessVars != nil && sessVars.StmtCtx != nil {
			insertRows = int64(sessVars.StmtCtx.AffectedRows())
		}
		return insertErr
	}); err != nil {
		return err
	}
	emitMVRefreshStepPlanRows(stepObserver, stepSet.dataChangeCompleteInsert, sessVars, explainFormat)
	if sessVars != nil {
		applyMVRefreshStmtResult(sessVars.StmtCtx, newMVRefreshStmtResultFromWriteCounts(insertRows, 0, deleteRows))
	}
	return nil
}

func executeRefreshMaterializedViewFast(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	sessVars *variable.SessionVars,
	s *ast.RefreshMaterializedViewStmt,
	implementOpts refreshImplementOptions,
	stepSet mvRefreshStepSet,
	stepObserver mvRefreshStepObserver,
	explainFormat string,
) error {
	origInternalSQLScanUserTable := sessVars.InternalSQLScanUserTable
	sessVars.InternalSQLScanUserTable = true
	defer func() {
		sessVars.InternalSQLScanUserTable = origInternalSQLScanUserTable
	}()

	if err := observeMVRefreshStep(stepObserver, stepSet.dataChangeFastMerge, func() error {
		return executeRefreshMaterializedViewImplement(
			kctx,
			sqlExec,
			sessVars,
			s,
			implementOpts,
		)
	}); err != nil {
		return err
	}
	emitMVRefreshStepPlanRows(stepObserver, stepSet.dataChangeFastMerge, sessVars, explainFormat)
	return nil
}

func executeRefreshMaterializedViewCompleteDeltaApply(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	sessVars *variable.SessionVars,
	s *ast.RefreshMaterializedViewStmt,
	stepSet mvRefreshStepSet,
	stepObserver mvRefreshStepObserver,
	explainFormat string,
) error {
	if err := observeMVRefreshStep(stepObserver, stepSet.dataChangeCompleteDeltaApply, func() error {
		return executeRefreshMaterializedViewImplement(kctx, sqlExec, sessVars, s, refreshImplementOptions{})
	}); err != nil {
		return err
	}
	emitMVRefreshStepPlanRows(stepObserver, stepSet.dataChangeCompleteDeltaApply, sessVars, explainFormat)
	return nil
}

func executeRefreshMaterializedViewImplement(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	sessVars *variable.SessionVars,
	s *ast.RefreshMaterializedViewStmt,
	implementOpts refreshImplementOptions,
) error {
	implementStmt := &ast.RefreshMaterializedViewImplementStmt{
		RefreshStmt:                  s,
		LastSuccessfulRefreshReadTSO: implementOpts.lastSuccessfulRefreshReadTSO,
		TargetRefreshReadTSO:         implementOpts.targetRefreshReadTSO,
		MLogRetainedLowerTSO:         implementOpts.mlogRetainedLowerTSO,
	}

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

func executeRefreshMaterializedViewInternalSQL(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	sql string,
	args ...any,
) error {
	rs, err := sqlExec.ExecuteInternal(kctx, sql, args...)
	return drainAndCloseRefreshRecordSet(kctx, rs, err)
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

func getRefreshReadTSOForSuccess(sessVars *variable.SessionVars) (uint64, error) {
	// MV refresh executes in pessimistic txn and reads data at `for_update_ts`.
	// Persist this timestamp so refresh metadata is aligned with the data snapshot.
	refreshReadTSO := sessVars.TxnCtx.GetForUpdateTS()
	if refreshReadTSO == 0 {
		return 0, errors.New("refresh materialized view: invalid refresh read tso")
	}
	return refreshReadTSO, nil
}

func collectFastRefreshMLogScanRows(sessVars *variable.SessionVars) *int64 {
	if sessVars == nil || sessVars.StmtCtx == nil || sessVars.StmtCtx.RuntimeStatsColl == nil {
		return nil
	}
	mergePlan, ok := sessVars.StmtCtx.GetPlan().(*plannercore.MViewDeltaMerge)
	if !ok || mergePlan.Source == nil || mergePlan.MLogTableID == 0 {
		return nil
	}

	scanPlanIDs := make(map[int]struct{})
	collectMLogScanPlanIDs(mergePlan.Source, mergePlan.MLogTableID, scanPlanIDs)
	if len(scanPlanIDs) != 1 {
		return nil
	}

	runtimeStatsColl := sessVars.StmtCtx.RuntimeStatsColl
	var totalRows int64
	hasRuntimeStats := false
	for scanPlanID := range scanPlanIDs {
		hasCopStats := runtimeStatsColl.ExistsCopStats(scanPlanID)
		if !hasCopStats && !runtimeStatsColl.ExistsRootStats(scanPlanID) {
			continue
		}
		hasRuntimeStats = true
		if hasCopStats {
			_, copRows := runtimeStatsColl.GetCopCountAndRows(scanPlanID)
			totalRows += copRows
			continue
		}
		totalRows += runtimeStatsColl.GetPlanActRows(scanPlanID)
	}
	if !hasRuntimeStats {
		return nil
	}
	return &totalRows
}

func collectMLogScanPlanIDs(plan plannercorebase.PhysicalPlan, mlogTableID int64, target map[int]struct{}) {
	if plan == nil {
		return
	}
	switch p := plan.(type) {
	case *physicalop.PhysicalTableScan:
		if p.Table != nil && p.Table.ID == mlogTableID {
			target[p.ID()] = struct{}{}
		}
	case *physicalop.PhysicalIndexScan:
		if p.Table != nil && p.Table.ID == mlogTableID {
			target[p.ID()] = struct{}{}
		}
	case *physicalop.PhysicalTableReader:
		for _, child := range p.TablePlans {
			collectMLogScanPlanIDs(child, mlogTableID, target)
		}
	case *physicalop.PhysicalIndexReader:
		for _, child := range p.IndexPlans {
			collectMLogScanPlanIDs(child, mlogTableID, target)
		}
	case *physicalop.PhysicalIndexLookUpReader:
		for _, child := range p.IndexPlans {
			collectMLogScanPlanIDs(child, mlogTableID, target)
		}
		for _, child := range p.TablePlans {
			collectMLogScanPlanIDs(child, mlogTableID, target)
		}
	case *physicalop.PhysicalIndexMergeReader:
		for _, partialPlan := range p.PartialPlans {
			for _, child := range partialPlan {
				collectMLogScanPlanIDs(child, mlogTableID, target)
			}
		}
		for _, child := range p.TablePlans {
			collectMLogScanPlanIDs(child, mlogTableID, target)
		}
	}
	for _, child := range plan.Children() {
		collectMLogScanPlanIDs(child, mlogTableID, target)
	}
}

func deriveRuntimeMaterializedScheduleNextUnixSeconds(
	kctx context.Context,
	evalSctx sessionctx.Context,
	nextExpr string,
	isInternalSQL bool,
	scheduleSQLMode mysql.SQLMode,
	logNullUpdate func(),
) (*int64, bool, error) {
	if !isInternalSQL {
		return nil, false, nil
	}
	nextAt, shouldUpdate, err := expression.DeriveMaterializedScheduleNextTime(
		kctx,
		evalSctx,
		nextExpr,
		scheduleSQLMode,
	)
	if err != nil {
		return nil, false, err
	}
	if shouldUpdate && nextAt == nil && logNullUpdate != nil {
		logNullUpdate()
	}
	if nextAt == nil {
		return nil, shouldUpdate, nil
	}
	nextUnixSeconds, err := expression.MaterializedScheduleTimeToUnixSeconds(nextAt)
	return nextUnixSeconds, shouldUpdate, errors.Trace(err)
}

func logRuntimeMaterializedViewRefreshNextUnixSecondsUpdateNull(
	schemaName string,
	mviewName string,
	nextExpr string,
) {
	if strings.TrimSpace(nextExpr) == "" {
		return
	}
	logutil.BgLogger().Error(
		"refresh materialized view: automatic refresh schedule disabled because NEXT expression evaluated to NULL, updating NEXT_REFRESH_UNIX_SECONDS to NULL",
		zap.String("schemaName", schemaName),
		zap.String("tableName", mviewName),
		zap.String("refreshNext", nextExpr),
	)
}
func persistRefreshSuccess(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	mviewID int64,
	lockedReadTSO uint64,
	lockedReadTSONull bool,
	refreshReadTSO uint64,
	lastSuccessRefreshEndUnixSeconds int64,
	nextRefreshUnixSeconds *int64,
	shouldUpdateNextRefreshUnixSeconds bool,
) error {
	setClauses := []string{
		"LAST_SUCCESS_READ_TSO = %?",
		"LAST_SUCCESS_REFRESH_END_UNIX_SECONDS = %?",
	}
	args := []any{refreshReadTSO, lastSuccessRefreshEndUnixSeconds}
	if shouldUpdateNextRefreshUnixSeconds {
		setClauses = append(setClauses, "NEXT_REFRESH_UNIX_SECONDS = %?")
		var nextRefreshUnixSecondsArg any
		if nextRefreshUnixSeconds != nil {
			nextRefreshUnixSecondsArg = *nextRefreshUnixSeconds
		}
		args = append(args, nextRefreshUnixSecondsArg)
	}
	var lockedReadTSOArg any = lockedReadTSO
	if lockedReadTSONull {
		lockedReadTSOArg = nil
	}

	updateSQL := fmt.Sprintf(
		`UPDATE mysql.tidb_mview_refresh_info
SET
	%s
WHERE MVIEW_ID = %%? AND LAST_SUCCESS_READ_TSO <=> %%?`,
		strings.Join(setClauses, ",\n\t"),
	)
	args = append(args, mviewID, lockedReadTSOArg)
	if _, err := sqlExec.ExecuteInternal(kctx, updateSQL, args...); err != nil {
		if infoschema.ErrTableNotExists.Equal(err) {
			return errors.New("refresh materialized view: required system table mysql.tidb_mview_refresh_info does not exist")
		}
		return errors.Trace(err)
	}
	persistedRefreshInfo, err := readRefreshInfoSnapshot(kctx, sqlExec, mviewID)
	if err != nil {
		return err
	}
	if persistedRefreshInfo.lastSuccessReadTSONull || persistedRefreshInfo.lastSuccessReadTSO != refreshReadTSO {
		return errors.New("refresh materialized view: inconsistent LAST_SUCCESS_READ_TSO after success update")
	}
	return nil
}

const (
	refreshHistStatusRunning = "running"
	refreshHistStatusSuccess = "success"
	refreshHistStatusFailed  = "failed"
)

func markRefreshFailedAlertState(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	mviewID int64,
	mviewSchema string,
	mviewName string,
) error {
	_, err := sqlExec.ExecuteInternal(
		kctx,
		`INSERT INTO mysql.tidb_mview_refresh_alert (
	MVIEW_ID,
	MVIEW_SCHEMA,
	MVIEW_NAME,
	REFRESH_FAILED,
	UPDATE_TIME
) VALUES (
	%?,
	%?,
	%?,
	'YES',
	NOW(6)
) ON DUPLICATE KEY UPDATE
MVIEW_SCHEMA = VALUES(MVIEW_SCHEMA),
MVIEW_NAME = VALUES(MVIEW_NAME),
REFRESH_FAILED = VALUES(REFRESH_FAILED),
UPDATE_TIME = VALUES(UPDATE_TIME)`,
		mviewID,
		mviewSchema,
		mviewName,
	)
	if err != nil {
		if infoschema.ErrTableNotExists.Equal(err) {
			return errors.New("refresh materialized view: required system table mysql.tidb_mview_refresh_alert does not exist")
		}
		return errors.Trace(err)
	}
	return nil
}

func reportMVRefreshFailed(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	reportRefreshFailed bool,
	mviewID int64,
	mviewSchema string,
	mviewName string,
	refreshJobID uint64,
	refreshMethod string,
	isInternalSQL bool,
	refreshErrMsg string,
) {
	if !reportRefreshFailed {
		return
	}
	if alertErr := markRefreshFailedAlertState(kctx, sqlExec, mviewID, mviewSchema, mviewName); alertErr != nil {
		logutil.BgLogger().Warn("refresh materialized view: failed to mark refresh_failed alert",
			zap.Int64("mviewID", mviewID),
			zap.String("schema", mviewSchema),
			zap.String("mview", mviewName),
			zap.Uint64("refreshJobID", refreshJobID),
			zap.Error(alertErr),
		)
	}
	logutil.BgLogger().Error("Materialized_view_refresh_failed",
		zap.Int64("mview_id", mviewID),
		zap.String("schema", mviewSchema),
		zap.String("mview", mviewName),
		zap.Uint64("refresh_job_id", refreshJobID),
		zap.String("refresh_method", refreshMethod),
		zap.Bool("internal_sql", isInternalSQL),
		zap.String("error", refreshErrMsg),
	)
}

func deleteRefreshAlertState(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	mviewID int64,
) error {
	if _, err := sqlExec.ExecuteInternal(
		kctx,
		`DELETE FROM mysql.tidb_mview_refresh_alert
WHERE MVIEW_ID = %?`,
		mviewID,
	); err != nil {
		if infoschema.ErrTableNotExists.Equal(err) {
			return errors.New("refresh materialized view: required system table mysql.tidb_mview_refresh_alert does not exist")
		}
		return errors.Trace(err)
	}
	return nil
}

func insertRefreshHistRunning(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	refreshJobID uint64,
	mviewID int64,
	mviewSchema string,
	mviewName string,
	refreshMethod string,
	refreshStartAt time.Time,
) error {
	insertSQL := `INSERT INTO mysql.tidb_mview_refresh_hist (
	REFRESH_JOB_ID,
	MVIEW_ID,
	MVIEW_SCHEMA,
	MVIEW_NAME,
	REFRESH_METHOD,
	REFRESH_START_TIME,
	REFRESH_STATUS,
	LAST_HEARTBEAT_TIME
) VALUES (
	%?,
	%?,
	%?,
	%?,
	%?,
	%?,
	%?,
	%?
)`
	if _, err := sqlExec.ExecuteInternal(
		kctx,
		insertSQL,
		refreshJobID,
		mviewID,
		mviewSchema,
		mviewName,
		refreshMethod,
		refreshStartAt,
		refreshHistStatusRunning,
		refreshStartAt,
	); err != nil {
		if infoschema.ErrTableNotExists.Equal(err) {
			return errors.New("refresh materialized view: required system table mysql.tidb_mview_refresh_hist does not exist")
		}
		return errors.Trace(err)
	}
	return nil
}

func insertRefreshHistFailed(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	refreshJobID uint64,
	mviewID int64,
	mviewSchema string,
	mviewName string,
	refreshMethod string,
	refreshStartAt time.Time,
	refreshEndAt time.Time,
	refreshReadTSO *uint64,
	refreshFailedReason *string,
) error {
	var refreshReadTSOArg any
	if refreshReadTSO != nil {
		refreshReadTSOArg = *refreshReadTSO
	}
	var refreshFailedReasonArg any
	if refreshFailedReason != nil {
		refreshFailedReasonArg = *refreshFailedReason
	}
	failpoint.Inject("mockInsertRefreshHistFailedError", func(val failpoint.Value) {
		if shouldFail, ok := val.(bool); ok && shouldFail {
			failpoint.Return(errors.New("mock insert failed refresh history error"))
		}
	})
	insertSQL := `INSERT INTO mysql.tidb_mview_refresh_hist (
	REFRESH_JOB_ID,
	MVIEW_ID,
	MVIEW_SCHEMA,
	MVIEW_NAME,
	REFRESH_METHOD,
	REFRESH_START_TIME,
	REFRESH_END_TIME,
	REFRESH_STATUS,
	REFRESH_ROWS,
	REFRESH_DURATION_SEC,
	REFRESH_READ_TSO,
	REFRESH_FAILED_REASON
) VALUES (
	%?,
	%?,
	%?,
	%?,
	%?,
	%?,
	%?,
	%?,
	%?,
	%?,
	%?,
	%?
)`
	if _, err := sqlExec.ExecuteInternal(
		kctx,
		insertSQL,
		refreshJobID,
		mviewID,
		mviewSchema,
		mviewName,
		refreshMethod,
		refreshStartAt,
		refreshEndAt,
		refreshHistStatusFailed,
		nil,
		formatDurationSecondsBetween(refreshStartAt, refreshEndAt),
		refreshReadTSOArg,
		refreshFailedReasonArg,
	); err != nil {
		if infoschema.ErrTableNotExists.Equal(err) {
			return errors.New("refresh materialized view: required system table mysql.tidb_mview_refresh_hist does not exist")
		}
		return errors.Trace(err)
	}
	return nil
}

func (e *RefreshMaterializedViewExec) insertRefreshHistFailedFallback(
	kctx context.Context,
	releaseCtx context.Context,
	mviewID int64,
	mviewSchema string,
	mviewName string,
	refreshMethod string,
	refreshReadTSO *uint64,
	refreshJobID *uint64,
	taskCancelController *mvTaskCancelController,
	refreshStart time.Time,
	reportRefreshFailed bool,
	isInternalSQL bool,
	refreshErr error,
) error {
	refreshFailedReason, finalErr := taskCancelController.normalizeTaskFailure(refreshErr)
	histSctx, err := e.GetSysSession()
	if err != nil {
		return errors.Annotatef(err, "refresh materialized view: failed to open history session after error %v", finalErr)
	}
	defer e.ReleaseSysSession(releaseCtx, histSctx)
	histSQLExec := histSctx.GetSQLExecutor()
	histLoc := histSctx.GetSessionVars().Location()

	if *refreshJobID == 0 {
		*refreshJobID, err = allocJobID(e.Ctx().GetStore())
		if err != nil {
			return errors.Annotatef(err, "refresh materialized view: failed to allocate history job id after error %v", finalErr)
		}
	}

	refreshErrMsg := finalErr.Error()
	if refreshFailedReason != nil {
		refreshErrMsg = *refreshFailedReason
	}
	reportMVRefreshFailed(kctx, histSQLExec, reportRefreshFailed, mviewID, mviewSchema, mviewName, *refreshJobID, refreshMethod, isInternalSQL, refreshErrMsg)
	refreshEndAt := time.Now()
	if err := insertRefreshHistFailed(
		kctx,
		histSQLExec,
		*refreshJobID,
		mviewID,
		mviewSchema,
		mviewName,
		refreshMethod,
		histTime(refreshStart, histLoc),
		histTime(refreshEndAt, histLoc),
		refreshReadTSO,
		&refreshErrMsg,
	); err != nil {
		return errors.Annotatef(err, "refresh materialized view: failed to insert failed refresh history after error %v", finalErr)
	}
	return errors.Trace(finalErr)
}

func finalizeRefreshHistWithRetry(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	refreshJobID uint64,
	mviewID int64,
	refreshStatus string,
	refreshReadTSO *uint64,
	refreshCommitTSO *uint64,
	refreshStartAt time.Time,
	refreshEndAt time.Time,
	refreshRows *int64,
	refreshScheduleDuration *time.Duration,
	refreshFailedReason *string,
) error {
	firstErr := finalizeRefreshHist(
		kctx,
		sqlExec,
		refreshJobID,
		mviewID,
		refreshStatus,
		refreshReadTSO,
		refreshCommitTSO,
		refreshStartAt,
		refreshEndAt,
		refreshRows,
		refreshScheduleDuration,
		refreshFailedReason,
	)
	if firstErr == nil {
		return nil
	}
	retryErr := finalizeRefreshHist(
		kctx,
		sqlExec,
		refreshJobID,
		mviewID,
		refreshStatus,
		refreshReadTSO,
		refreshCommitTSO,
		refreshStartAt,
		refreshEndAt,
		refreshRows,
		refreshScheduleDuration,
		refreshFailedReason,
	)
	if retryErr == nil {
		return nil
	}
	logutil.BgLogger().Warn("refresh materialized view: failed to finalize refresh history after retry",
		zap.Uint64("refreshJobID", refreshJobID),
		zap.Int64("mviewID", mviewID),
		zap.String("refreshStatus", refreshStatus),
		zap.NamedError("firstAttemptErr", firstErr),
		zap.NamedError("retryErr", retryErr),
	)
	return errors.Annotatef(retryErr, "first finalize attempt failed: %v", firstErr)
}

func finalizeRefreshHist(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	refreshJobID uint64,
	mviewID int64,
	refreshStatus string,
	refreshReadTSO *uint64,
	refreshCommitTSO *uint64,
	refreshStartAt time.Time,
	refreshEndAt time.Time,
	refreshRows *int64,
	refreshScheduleDuration *time.Duration,
	refreshFailedReason *string,
) error {
	failpoint.Inject("mockFinalizeRefreshHistError", func(val failpoint.Value) {
		if shouldFail, ok := val.(bool); ok && shouldFail {
			failpoint.Return(errors.New("mock finalize refresh history error"))
		}
	})

	var refreshReadTSOArg any
	if refreshReadTSO != nil {
		refreshReadTSOArg = *refreshReadTSO
	}
	var refreshCommitTSOArg any
	if refreshCommitTSO != nil {
		refreshCommitTSOArg = *refreshCommitTSO
	}
	var refreshRowsArg any
	if refreshRows != nil {
		refreshRowsArg = *refreshRows
	}
	var refreshScheduleDurationArg any
	if refreshScheduleDuration != nil {
		refreshScheduleDurationArg = formatDurationSeconds(*refreshScheduleDuration)
	}
	var refreshFailedReasonArg any
	if refreshFailedReason != nil {
		refreshFailedReasonArg = *refreshFailedReason
	}
	updateSQL := `UPDATE mysql.tidb_mview_refresh_hist
SET
	REFRESH_END_TIME = %?,
	REFRESH_STATUS = %?,
	REFRESH_ROWS = %?,
	REFRESH_DURATION_SEC = %?,
	REFRESH_SCHEDULE_DURATION_SEC = %?,
	REFRESH_READ_TSO = %?,
	REFRESH_COMMIT_TSO = %?,
	REFRESH_FAILED_REASON = %?
WHERE REFRESH_JOB_ID = %?
  AND MVIEW_ID = %?`
	if _, err := sqlExec.ExecuteInternal(
		kctx,
		updateSQL,
		refreshEndAt,
		refreshStatus,
		refreshRowsArg,
		formatDurationSecondsBetween(refreshStartAt, refreshEndAt),
		refreshScheduleDurationArg,
		refreshReadTSOArg,
		refreshCommitTSOArg,
		refreshFailedReasonArg,
		refreshJobID,
		mviewID,
	); err != nil {
		if infoschema.ErrTableNotExists.Equal(err) {
			return errors.New("refresh materialized view: required system table mysql.tidb_mview_refresh_hist does not exist")
		}
		return errors.Trace(err)
	}
	return nil
}

type lastTxnCommitTSInfo struct {
	CommitTS uint64 `json:"commit_ts"`
	ErrMsg   string `json:"error,omitempty"`
}

func getSessionLastTxnCommitTSO(sctx sessionctx.Context) (*uint64, error) {
	if sctx == nil || sctx.GetSessionVars() == nil {
		return nil, errors.New("refresh materialized view: session vars are nil")
	}
	lastTxnInfo := sctx.GetSessionVars().LastTxnInfo
	if len(lastTxnInfo) == 0 {
		return nil, errors.New("refresh materialized view: last transaction info is empty")
	}
	var txnInfo lastTxnCommitTSInfo
	if err := json.Unmarshal([]byte(lastTxnInfo), &txnInfo); err != nil {
		return nil, errors.Annotate(err, "refresh materialized view: invalid last transaction info")
	}
	if txnInfo.CommitTS == 0 {
		if txnInfo.ErrMsg != "" {
			return nil, errors.Errorf("refresh materialized view: last transaction info reports error %s", txnInfo.ErrMsg)
		}
		return nil, errors.New("refresh materialized view: last transaction info missing commit tso")
	}
	commitTSO := txnInfo.CommitTS
	return &commitTSO, nil
}
