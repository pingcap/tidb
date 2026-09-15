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
	"fmt"
	"math"
	"math/bits"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	statshandle "github.com/pingcap/tidb/pkg/statistics/handle"
	storeerr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

var errMLogPurgeLockConflict = errors.NewNoStackError("mlog purge lock conflict")
var errMVTaskCanceledManually = errors.NewNoStackError("materialized view task canceled manually")

const (
	purgeHistStatusRunning  = "running"
	purgeHistStatusSuccess  = "success"
	purgeHistStatusFailed   = "failed"
	purgeHistStatusOrphaned = "orphaned"

	mlogPurgeAdaptiveCountTimeout   = 30 * time.Second
	mlogPurgeAdaptiveBatchWindow    = 200 * time.Millisecond
	mlogPurgeAdaptiveMinBatchSize   = int64(8000)
	mlogPurgeAdaptiveMaxRangeCount  = int64(16)
	mlogPurgeAdaptiveDeadlineBuffer = 10 * time.Second
	// Keep the manual purge budget aligned with the source MV service default.
	mlogPurgeAdaptiveMaxBudget  = 10*time.Minute - mlogPurgeAdaptiveDeadlineBuffer
	mvTaskMonitorPollInterval   = 5 * time.Second
	mvTaskHistHeartbeatInterval = 10 * time.Minute
	mvTaskMonitorSQLTimeout     = 5 * time.Second
)

type mvTaskCancelReason uint8

const (
	mvTaskCancelReasonNone mvTaskCancelReason = iota
	mvTaskCancelReasonManual
)

type mvTaskCancelController struct {
	ctx    context.Context
	cancel context.CancelFunc

	mu        sync.Mutex
	reason    mvTaskCancelReason
	requester string
}

func newMVTaskCancelController(parent context.Context) *mvTaskCancelController {
	ctx, cancel := context.WithCancel(parent)
	return &mvTaskCancelController{ctx: ctx, cancel: cancel}
}

func (c *mvTaskCancelController) context() context.Context {
	if c == nil {
		return nil
	}
	return c.ctx
}

func (c *mvTaskCancelController) requestManualCancelByRequester(requester string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	if c.reason == mvTaskCancelReasonNone {
		c.reason = mvTaskCancelReasonManual
	}
	if c.requester == "" && requester != "" {
		c.requester = requester
	}
	cancel := c.cancel
	c.mu.Unlock()
	cancel()
}

func (c *mvTaskCancelController) normalizeTaskFailure(taskErr error) (*string, error) {
	if c == nil {
		return nil, taskErr
	}
	c.mu.Lock()
	reason := c.reason
	requester := c.requester
	c.mu.Unlock()
	if reason != mvTaskCancelReasonManual {
		return nil, taskErr
	}
	failedReason := formatMVManualCancelFailureReason(requester)
	return &failedReason, errMVTaskCanceledManually
}

func (c *mvTaskCancelController) isManualCancelRequested() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reason == mvTaskCancelReasonManual
}

func formatMVManualCancelFailureReason(requester string) string {
	if requester == "" {
		return "cancelled manually"
	}
	return "cancelled manually by " + requester
}

func formatMVManualCancelRequester(user *auth.UserIdentity) string {
	if user == nil {
		return ""
	}
	username := user.AuthUsername
	if username == "" {
		username = user.Username
	}
	hostname := user.AuthHostname
	if hostname == "" {
		hostname = user.Hostname
	}
	if username == "" && hostname == "" {
		return ""
	}
	return "'" + strings.ReplaceAll(username, "'", "''") + "'@'" + strings.ReplaceAll(hostname, "'", "''") + "'"
}

type mvTaskCancelPoller func(context.Context, sqlexec.SQLExecutor) (requested bool, requester string, err error)
type mvTaskHeartbeatWriter func(context.Context, sqlexec.SQLExecutor) error

func startMVTaskMonitor(
	taskCtx context.Context,
	getSysSession func() (sessionctx.Context, error),
	releaseWatchSession func(sessionctx.Context),
	taskCancelController *mvTaskCancelController,
	monitorName string,
	poller mvTaskCancelPoller,
	heartbeatWriter mvTaskHeartbeatWriter,
) (func(), error) {
	if taskCancelController == nil {
		return func() {}, errors.New("mv task monitor: task cancel controller is nil")
	}
	monitorSctx, err := getSysSession()
	if err != nil {
		return func() {}, err
	}
	monitorCtx, stopMonitor := context.WithCancel(taskCtx)
	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		defer releaseWatchSession(monitorSctx)

		sqlExec := monitorSctx.GetSQLExecutor()
		ticker := time.NewTicker(getMVTaskMonitorPollInterval())
		defer ticker.Stop()
		nextHeartbeatAt := time.Now().Add(getMVTaskHistHeartbeatInterval())
		for {
			if heartbeatWriter != nil && !time.Now().Before(nextHeartbeatAt) {
				heartbeatCtx, cancelHeartbeat := context.WithTimeout(monitorCtx, getMVTaskMonitorSQLTimeout())
				err := heartbeatWriter(heartbeatCtx, sqlExec)
				cancelHeartbeat()
				nextHeartbeatAt = time.Now().Add(getMVTaskHistHeartbeatInterval())
				if err != nil {
					if monitorCtx.Err() != nil {
						return
					}
					logutil.BgLogger().Warn("materialized view task heartbeat failed", zap.String("monitor", monitorName), zap.Error(err))
				}
			}

			pollCtx, cancelPoll := context.WithTimeout(monitorCtx, getMVTaskMonitorSQLTimeout())
			requested, requester, err := poller(pollCtx, sqlExec)
			cancelPoll()
			failpoint.InjectCall("mvTaskMonitorPolled", monitorName)
			if err != nil {
				if monitorCtx.Err() != nil {
					return
				}
				logutil.BgLogger().Warn("materialized view task monitor cancel poll failed", zap.String("monitor", monitorName), zap.Error(err))
			} else if requested {
				taskCancelController.requestManualCancelByRequester(requester)
				failpoint.InjectCall("mvTaskCancelWatcherRequested", monitorName)
				return
			}

			select {
			case <-monitorCtx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return func() {
		stopMonitor()
		<-monitorDone
	}, nil
}

func getMVTaskMonitorPollInterval() time.Duration {
	interval := mvTaskMonitorPollInterval
	failpoint.Inject("mockMVTaskMonitorPollInterval", func(val failpoint.Value) {
		switch v := val.(type) {
		case int:
			interval = time.Duration(v) * time.Millisecond
		case int64:
			interval = time.Duration(v) * time.Millisecond
		}
	})
	return interval
}

func getMVTaskHistHeartbeatInterval() time.Duration {
	interval := mvTaskHistHeartbeatInterval
	failpoint.Inject("mockMVTaskHistHeartbeatInterval", func(val failpoint.Value) {
		switch v := val.(type) {
		case int:
			interval = time.Duration(v) * time.Millisecond
		case int64:
			interval = time.Duration(v) * time.Millisecond
		}
	})
	return interval
}

func getMVTaskMonitorSQLTimeout() time.Duration {
	timeout := mvTaskMonitorSQLTimeout
	failpoint.Inject("mockMVTaskMonitorSQLTimeout", func(val failpoint.Value) {
		switch v := val.(type) {
		case int:
			timeout = time.Duration(v) * time.Millisecond
		case int64:
			timeout = time.Duration(v) * time.Millisecond
		}
	})
	return timeout
}

func readPurgeHistCancelRequest(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	purgeJobID uint64,
	mlogID int64,
) (bool, string, error) {
	rows, err := sqlexec.ExecSQL(kctx, sqlExec, `SELECT CANCEL_REQUEST_TIME, CANCEL_REQUESTED_BY
FROM mysql.tidb_mlog_purge_hist
WHERE PURGE_JOB_ID = %? AND MLOG_ID = %?`, purgeJobID, mlogID)
	if err != nil {
		if infoschema.ErrTableNotExists.Equal(err) {
			return false, "", errors.New("required system table mysql.tidb_mlog_purge_hist does not exist")
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

func requestPurgeHistCancel(
	kctx context.Context,
	sctx sessionctx.Context,
	purgeJobID uint64,
	requester any,
) (bool, error) {
	_, err := sctx.GetSQLExecutor().ExecuteInternal(kctx, `UPDATE mysql.tidb_mlog_purge_hist
SET CANCEL_REQUEST_TIME = NOW(6), CANCEL_REQUESTED_BY = %?
WHERE PURGE_JOB_ID = %? AND PURGE_STATUS = 'running' AND CANCEL_REQUEST_TIME IS NULL`, requester, purgeJobID)
	if err != nil {
		return false, errors.Trace(err)
	}
	return sctx.GetSessionVars().StmtCtx.AffectedRows() > 0, nil
}

func updatePurgeHistHeartbeat(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	purgeJobID uint64,
	mlogID int64,
) error {
	_, err := sqlExec.ExecuteInternal(kctx, `UPDATE mysql.tidb_mlog_purge_hist
SET LAST_HEARTBEAT_TIME = NOW(6)
WHERE PURGE_JOB_ID = %? AND MLOG_ID = %? AND PURGE_STATUS = 'running'`, purgeJobID, mlogID)
	if err != nil {
		if infoschema.ErrTableNotExists.Equal(err) {
			return errors.New("required system table mysql.tidb_mlog_purge_hist does not exist")
		}
		return errors.Trace(err)
	}
	return nil
}

func resolveCancelPurgeJobPrivilegeTarget(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	is infoschema.InfoSchema,
	purgeJobID uint64,
) (dbName string, tableName string, found bool, err error) {
	rows, err := sqlexec.ExecSQL(kctx, sqlExec, `SELECT MLOG_ID
FROM mysql.tidb_mlog_purge_hist
WHERE PURGE_JOB_ID = %? AND PURGE_STATUS = 'running'`, purgeJobID)
	if err != nil {
		if infoschema.ErrTableNotExists.Equal(err) {
			return "", "", false, errors.New("required system table mysql.tidb_mlog_purge_hist does not exist")
		}
		return "", "", false, errors.Trace(err)
	}
	if len(rows) == 0 {
		return "", "", false, nil
	}
	mlogID := rows[0].GetInt64(0)
	mlogTable, ok := is.TableByID(context.Background(), mlogID)
	if !ok {
		return "", "", false, errors.Errorf("cannot resolve materialized view log %d for cancel job %d", mlogID, purgeJobID)
	}
	mlogMeta := mlogTable.Meta()
	if mlogMeta.MaterializedViewLog == nil {
		return "", "", false, errors.Errorf("table %d is not a materialized view log", mlogID)
	}
	dbInfo, ok := infoschema.SchemaByTable(is, mlogMeta)
	if !ok {
		return "", "", false, errors.Errorf("cannot resolve schema for materialized view log %d", mlogID)
	}
	return dbInfo.Name.L, mlogMeta.Name.L, true, nil
}

func checkCancelMaterializedViewJobPrivilege(
	kctx context.Context,
	ctx sessionctx.Context,
	sqlExec sqlexec.SQLExecutor,
	stmt *ast.CancelMaterializedViewJobStmt,
) error {
	if err := validateCancelMaterializedViewJobStmt(stmt); err != nil {
		return err
	}
	pm := privilege.GetPrivilegeManager(ctx)
	user := ctx.GetSessionVars().User
	if pm == nil || user == nil {
		return nil
	}
	is, ok := ctx.GetInfoSchema().(infoschema.InfoSchema)
	if !ok {
		return errors.New("cannot resolve current infoschema for materialized view log purge cancellation")
	}
	dbName, tableName, found, err := resolveCancelPurgeJobPrivilegeTarget(kctx, sqlExec, is, uint64(stmt.JobID))
	if err != nil {
		return err
	}
	if !found {
		return cancelMaterializedViewJobUserError(stmt)
	}
	if pm.RequestVerification(ctx.GetSessionVars().ActiveRoles, dbName, tableName, "", mysql.OperateViewPriv) {
		return nil
	}
	return plannererrors.ErrTableaccessDenied.GenWithStackByArgs("OPERATE VIEW", user.AuthUsername, user.AuthHostname, tableName)
}

func validateCancelMaterializedViewJobStmt(stmt *ast.CancelMaterializedViewJobStmt) error {
	if stmt == nil {
		return errors.New("cancel materialized view job: missing statement")
	}
	if stmt.Tp != ast.CancelMaterializedViewJobTypeLogPurge {
		return errors.Errorf("invalid materialized view job cancel type: %d", stmt.Tp)
	}
	return nil
}

func cancelMaterializedViewJobUserError(stmt *ast.CancelMaterializedViewJobStmt) error {
	return errors.NewNoStackErrorf("cannot cancel materialized view log purge job %d", stmt.JobID)
}

// Next implements the Executor Next interface.
func (e *CancelMaterializedViewJobExec) Next(ctx context.Context, _ *chunk.Chunk) error {
	if e.done {
		return nil
	}
	e.done = true
	if err := validateCancelMaterializedViewJobStmt(e.stmt); err != nil {
		return err
	}
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMViewMaintenance)
	requester := formatMVManualCancelRequester(e.Ctx().GetSessionVars().User)
	var requesterArg any
	if requester != "" {
		requesterArg = requester
	}
	sctx, err := e.GetSysSession()
	if err != nil {
		return err
	}
	defer e.ReleaseSysSession(ctx, sctx)
	if err := checkCancelMaterializedViewJobPrivilege(ctx, e.Ctx(), sctx.GetSQLExecutor(), e.stmt); err != nil {
		return err
	}
	applied, err := requestPurgeHistCancel(ctx, sctx, uint64(e.stmt.JobID), requesterArg)
	if err != nil {
		return err
	}
	if !applied {
		return cancelMaterializedViewJobUserError(e.stmt)
	}
	return nil
}

type mlogPurgeThrottleConfig struct {
	minRate     float64
	budgetRatio float64
}

type mlogPurgeThrottlePlan struct {
	targetRate         float64
	pendingRows        int64
	effectiveBatchSize int64
	minRate            float64
	deadline           time.Time
	noWaitStreak       int
}

type mlogPurgePendingRowStats struct {
	pendingRows    int64
	minRowID       int64
	maxRowID       int64
	hasRowIDBounds bool
}

type mlogPurgeDeleteRowIDRange struct {
	startRowID int64
	endRowID   int64
}

type mlogPurgeDeletePlan struct {
	pendingRows  int64
	throttlePlan *mlogPurgeThrottlePlan
	rowIDRanges  []mlogPurgeDeleteRowIDRange
}

// PurgeMaterializedViewLogExec executes "PURGE MATERIALIZED VIEW LOG".
type PurgeMaterializedViewLogExec struct {
	exec.BaseExecutor
	stmt *ast.PurgeMaterializedViewLogStmt
	done bool
}

// CancelMaterializedViewJobExec executes a purge-job cancellation request.
type CancelMaterializedViewJobExec struct {
	exec.BaseExecutor
	stmt *ast.CancelMaterializedViewJobStmt
	done bool
}

// Next implements the Executor Next interface.
func (e *PurgeMaterializedViewLogExec) Next(ctx context.Context, _ *chunk.Chunk) (err error) {
	if e.done {
		return nil
	}
	e.done = true
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMViewMaintenance)
	return e.executePurgeMaterializedViewLog(ctx, e.stmt)
}

func (e *PurgeMaterializedViewLogExec) executePurgeMaterializedViewLog(
	kctx context.Context,
	stmt *ast.PurgeMaterializedViewLogStmt,
) (err error) {
	purgeStart := time.Now()
	vars := e.Ctx().GetSessionVars()
	isInternalSQL := vars.InRestrictedSQL
	purgeMethod, err := validatePurgeMaterializedViewLogStmt(stmt, isInternalSQL)
	if err != nil {
		return err
	}

	schemaName, baseTable, mlogName, mlogID, mlogShardRowIDBits, mlogInfo, err := e.resolvePurgeMaterializedViewLogMeta(stmt)
	if err != nil {
		return err
	}
	if err := checkOperateViewOnMLog(e.Ctx(), schemaName, mlogName); err != nil {
		return err
	}
	releaseCtx := kctx
	taskCancelController := newMVTaskCancelController(kctx)
	defer taskCancelController.cancel()
	kctx = taskCancelController.context()
	finalizeCtx := context.WithoutCancel(kctx)
	batchSize := int64(vars.MLogPurgeBatchSize)
	if batchSize <= 0 {
		batchSize = int64(vardef.DefTiDBMLogPurgeBatchSize)
	}
	totalPurgeRows := int64(0)
	safePurgeTSO := uint64(0)
	lockedLastPurgedTSO := uint64(0)
	lockedLastPurgedTSOReady := false
	var lockedNextTime *time.Time
	purgeJobID := uint64(0)
	purgeHistRunningInserted := false
	txnStarted := false
	txnFinished := false
	var throttlePlan *mlogPurgeThrottlePlan
	effectiveBatchSize := batchSize
	var deleteLoopStart time.Time

	// A failure before the running row is inserted still needs a durable failed
	// history row. Once a running row exists, finalizeFailure updates that row.
	defer func() {
		if r := recover(); r != nil {
			err = errors.Errorf("purge materialized view log: panic: %v", r)
		}
		if err == nil || purgeHistRunningInserted || mlogID == 0 {
			return
		}
		_, finalErr := taskCancelController.normalizeTaskFailure(err)
		if fallbackErr := e.insertMLogPurgeHistFailedFallback(
			finalizeCtx,
			releaseCtx,
			mlogID,
			schemaName.O,
			baseTable.Name.O,
			purgeMethod,
			&purgeJobID,
			taskCancelController,
			purgeStart,
			totalPurgeRows,
			finalErr,
		); fallbackErr != nil {
			err = fallbackErr
		}
	}()

	// The purge-info row lock and checkpoint are kept in this transaction.
	purgeSctx, err := e.GetSysSession()
	if err != nil {
		return err
	}
	defer e.ReleaseSysSession(releaseCtx, purgeSctx)
	purgeSessVars := purgeSctx.GetSessionVars()
	restorePurgeVars, err := applyMLogPurgeMaintenanceSessionVars(
		purgeSessVars,
		vars.MViewMaintainMemQuota,
		vars.MViewMaintainIsolationReadEngines,
		isInternalSQL,
	)
	if err != nil {
		return err
	}
	defer restorePurgeVars()
	failpoint.InjectCall("mvMaintainMemQuotaAppliedOnPurgeSession", purgeSessVars.MemQuotaQuery, vars.MViewMaintainMemQuota)
	failpoint.InjectCall("mvMaintainIsolationReadEnginesAppliedOnPurgeSession", variable.GetIsolationReadEnginesString(purgeSessVars), vars.MViewMaintainIsolationReadEngines)
	purgeSQLExec := purgeSctx.GetSQLExecutor()

	deleteSctx, err := e.GetSysSession()
	if err != nil {
		return err
	}
	defer e.ReleaseSysSession(releaseCtx, deleteSctx)
	deleteVars := deleteSctx.GetSessionVars()
	restoreDeleteVars, err := applyMLogPurgeMaintenanceSessionVars(
		deleteVars,
		vars.MViewMaintainMemQuota,
		vars.MViewMaintainIsolationReadEngines,
		isInternalSQL,
	)
	if err != nil {
		return err
	}
	defer restoreDeleteVars()
	failpoint.InjectCall("mvMaintainMemQuotaAppliedOnPurgeDeleteSession", deleteVars.MemQuotaQuery, vars.MViewMaintainMemQuota)
	failpoint.InjectCall("mvMaintainIsolationReadEnginesAppliedOnPurgeDeleteSession", variable.GetIsolationReadEnginesString(deleteVars), vars.MViewMaintainIsolationReadEngines)
	restoreTiFlashThreads, err := applyMLogPurgeDeleteTiFlashThreads(
		deleteVars,
		vars.MLogPurgeDeleteTiFlashThreads,
		isInternalSQL,
	)
	if err != nil {
		return err
	}
	defer restoreTiFlashThreads()
	deleteSQLExec := statshandle.AttachStatsCollector(deleteSctx.GetSQLExecutor())
	defer statshandle.DetachStatsCollector(deleteSQLExec)

	countSctx, err := e.GetSysSession()
	if err != nil {
		return err
	}
	defer e.ReleaseSysSession(releaseCtx, countSctx)
	countVars := countSctx.GetSessionVars()
	restoreCountVars, err := applyMLogPurgeMaintenanceSessionVars(
		countVars,
		vars.MViewMaintainMemQuota,
		vars.MViewMaintainIsolationReadEngines,
		isInternalSQL,
	)
	if err != nil {
		return err
	}
	defer restoreCountVars()
	countSQLExec := countSctx.GetSQLExecutor()

	histSctx, err := e.GetSysSession()
	if err != nil {
		return err
	}
	defer e.ReleaseSysSession(releaseCtx, histSctx)
	histSQLExec := histSctx.GetSQLExecutor()
	histLocation := histSctx.GetSessionVars().Location()

	var evalSctx sessionctx.Context
	if isInternalSQL {
		evalSctx, err = e.GetSysSession()
		if err != nil {
			return err
		}
		defer e.ReleaseSysSession(releaseCtx, evalSctx)
	}
	stopTaskMonitor := func() {}
	defer func() {
		stopTaskMonitor()
	}()
	finalizeFailure := func(purgeErr error) error {
		purgeFailedReason, finalErr := taskCancelController.normalizeTaskFailure(purgeErr)
		if txnStarted && !txnFinished {
			_, _ = purgeSQLExec.ExecuteInternal(finalizeCtx, "ROLLBACK")
			txnFinished = true
		}
		if !purgeHistRunningInserted {
			return errors.Trace(finalErr)
		}
		purgeErrMsg := finalErr.Error()
		if purgeFailedReason != nil {
			purgeErrMsg = *purgeFailedReason
		}
		purgeEnd := time.Now()
		if historyErr := finalizeMLogPurgeHistWithRetry(
			finalizeCtx,
			histSQLExec,
			purgeJobID,
			mlogID,
			purgeHistStatusFailed,
			historyTime(purgeStart, histLocation),
			historyTime(purgeEnd, histLocation),
			totalPurgeRows,
			&purgeErrMsg,
		); historyErr != nil {
			return errors.Annotatef(historyErr, "purge materialized view log: failed to finalize purge history after error %v", finalErr)
		}
		return errors.Trace(finalErr)
	}
	failpoint.Inject("mockPurgeMaterializedViewLogErrorBeforeInsertHist", func(val failpoint.Value) {
		if msg, ok := val.(string); ok {
			failpoint.Return(errors.New(msg))
		}
	})
	var beginErr error
	failpoint.Inject("mockPurgeMaterializedViewLogBeginErr", func(val failpoint.Value) {
		if v, ok := val.(bool); ok && v {
			beginErr = errors.New("mock purge begin error")
		}
	})
	if beginErr != nil {
		return finalizeFailure(beginErr)
	}

	if _, err = purgeSQLExec.ExecuteInternal(kctx, "BEGIN PESSIMISTIC"); err != nil {
		return finalizeFailure(err)
	}
	txnStarted = true
	defer func() {
		if txnStarted && !txnFinished {
			_, _ = purgeSQLExec.ExecuteInternal(finalizeCtx, "ROLLBACK")
			txnFinished = true
		}
	}()

	lastPurgedTSO, hasLastPurgedTSO, nextTime, err := acquireMaterializedViewLogPurgeLock(
		kctx, purgeSQLExec, schemaName, stmt.Table.Name, mlogID,
	)
	if err != nil {
		return finalizeFailure(err)
	}
	lockedLastPurgedTSO = lastPurgedTSO
	lockedLastPurgedTSOReady = hasLastPurgedTSO
	lockedNextTime = nextTime
	txn, err := purgeSctx.Txn(true)
	if err != nil {
		return finalizeFailure(err)
	}
	purgeStartTSO := txn.StartTS()
	failpoint.Inject("mockPurgeMaterializedViewLogZeroStartTS", func(val failpoint.Value) {
		if v, ok := val.(bool); ok && v {
			purgeStartTSO = 0
		}
	})
	if purgeStartTSO == 0 {
		return finalizeFailure(errors.New("purge materialized view log: invalid transaction start tso"))
	}
	purgeJobID = purgeStartTSO

	publicMVIDs, buildingMVIDs, err := collectDependentMViewIDsForMLogPurge(kctx, purgeSQLExec, baseTable, mlogID)
	if err != nil {
		return finalizeFailure(err)
	}
	safePurgeTSO, err = calcMaterializedViewLogSafePurgeTSO(
		kctx, purgeSQLExec, schemaName.O, stmt.Table.Name.O, purgeStartTSO, publicMVIDs, buildingMVIDs,
	)
	if err != nil {
		return finalizeFailure(err)
	}

	purgeCutoffFenceTSO := uint64(0)
	if lockedLastPurgedTSOReady {
		purgeCutoffFenceTSO = lockedLastPurgedTSO
	}
	latestCutoffTSO, hasLatestCutoffTSO, err := readLatestMLogPurgeCutoffFenceTSO(kctx, histSQLExec, mlogID)
	if err != nil {
		return finalizeFailure(err)
	}
	if hasLatestCutoffTSO && latestCutoffTSO > purgeCutoffFenceTSO {
		purgeCutoffFenceTSO = latestCutoffTSO
	}
	skipPurgeByCutoffFence := safePurgeTSO < purgeCutoffFenceTSO
	if !skipPurgeByCutoffFence {
		if err := insertMLogPurgeHistRunning(
			kctx, histSQLExec, purgeJobID, mlogID, schemaName.O, baseTable.Name.O,
			purgeMethod, safePurgeTSO, historyTime(purgeStart, histLocation),
		); err != nil {
			return finalizeFailure(err)
		}
		purgeHistRunningInserted = true
		stopTaskMonitor, err = startMVTaskMonitor(
			kctx,
			e.GetSysSession,
			func(sctx sessionctx.Context) {
				e.ReleaseSysSession(releaseCtx, sctx)
			},
			taskCancelController,
			fmt.Sprintf("mlog-purge-%d", purgeJobID),
			func(watchCtx context.Context, watchSQLExec sqlexec.SQLExecutor) (bool, string, error) {
				return readPurgeHistCancelRequest(watchCtx, watchSQLExec, purgeJobID, mlogID)
			},
			func(watchCtx context.Context, watchSQLExec sqlexec.SQLExecutor) error {
				return updatePurgeHistHeartbeat(watchCtx, watchSQLExec, purgeJobID, mlogID)
			},
		)
		if err != nil {
			return finalizeFailure(err)
		}
		failpoint.Inject("pausePurgeMaterializedViewLogAfterInsertPurgeHistRunning", func() {})
	}
	skipDeleteByCheckpoint := lockedLastPurgedTSOReady && lockedLastPurgedTSO >= safePurgeTSO
	if !skipPurgeByCutoffFence && !skipDeleteByCheckpoint && safePurgeTSO > 0 {
		deletePlan := tryBuildMLogPurgeDeletePlanBestEffort(
			kctx, vars, evalSctx, countSQLExec, countVars, mlogInfo, isInternalSQL,
			schemaName.O, mlogName.O, mlogShardRowIDBits, lockedLastPurgedTSO,
			lockedLastPurgedTSOReady, safePurgeTSO, lockedNextTime,
		)
		if deletePlan == nil || (deletePlan.pendingRows > 0 && len(deletePlan.rowIDRanges) == 0) {
			effectiveBatchSize = batchSize
			for {
				rows, deleteErr := purgeMaterializedViewLogData(kctx, deleteSQLExec, deleteVars, schemaName.O, mlogName.O, lockedLastPurgedTSO, lockedLastPurgedTSOReady, safePurgeTSO, nil, effectiveBatchSize)
				totalPurgeRows += rows
				if deleteErr != nil {
					return finalizeFailure(deleteErr)
				}
				failpoint.Inject("pausePurgeMaterializedViewLogAfterDeleteBatch", func() {})
				if rows < effectiveBatchSize {
					break
				}
			}
		} else if deletePlan.pendingRows > 0 {
			throttlePlan = deletePlan.throttlePlan
			if throttlePlan != nil {
				effectiveBatchSize = throttlePlan.effectiveDeleteBatchSize(batchSize)
			}
			deleteLoopStart = time.Now()
			for _, rowIDRange := range deletePlan.rowIDRanges {
				for {
					rows, deleteErr := purgeMaterializedViewLogData(kctx, deleteSQLExec, deleteVars, schemaName.O, mlogName.O, lockedLastPurgedTSO, lockedLastPurgedTSOReady, safePurgeTSO, &rowIDRange, effectiveBatchSize)
					totalPurgeRows += rows
					if deleteErr != nil {
						return finalizeFailure(deleteErr)
					}
					failpoint.Inject("pausePurgeMaterializedViewLogAfterDeleteBatch", func() {})
					if rows < effectiveBatchSize {
						break
					}
					if throttlePlan != nil {
						if sleepErr := throttlePlan.maybeSleep(kctx, deleteLoopStart, totalPurgeRows); sleepErr != nil {
							if taskCancelController.isManualCancelRequested() {
								return finalizeFailure(sleepErr)
							}
							logutil.BgLogger().Warn("purge materialized view log: adaptive throttle sleep failed, fallback to unthrottled purge", zap.String("schemaName", schemaName.O), zap.String("tableName", mlogName.O), zap.Error(sleepErr))
							throttlePlan = nil
							effectiveBatchSize = batchSize
						} else {
							effectiveBatchSize = throttlePlan.effectiveDeleteBatchSize(batchSize)
						}
					}
				}
			}
		}
	}

	var nextUnixSeconds *int64
	shouldUpdateNext := false
	if isInternalSQL {
		tz, tzErr := mlogInfo.PurgeScheduleTimeZone.GetLocation()
		if tzErr != nil {
			return finalizeFailure(tzErr)
		}
		nextUnixSeconds, shouldUpdateNext, err = deriveMLogPurgeNextUnixSeconds(kctx, evalSctx, mlogInfo, tz)
		if err != nil {
			return finalizeFailure(err)
		}
	}
	var checkpoint *uint64
	if !skipPurgeByCutoffFence && !skipDeleteByCheckpoint {
		checkpoint = &safePurgeTSO
	}
	if err := updateMaterializedViewLogPurgeInfoOnSuccess(
		kctx, purgeSQLExec, mlogID, checkpoint, nextUnixSeconds, shouldUpdateNext,
	); err != nil {
		return finalizeFailure(err)
	}
	if _, err = purgeSQLExec.ExecuteInternal(kctx, "COMMIT"); err != nil {
		return finalizeFailure(err)
	}
	txnFinished = true

	if purgeHistRunningInserted {
		var historyErr error
		failpoint.Inject("mockPurgeMaterializedViewLogFinalizeSuccessErr", func(val failpoint.Value) {
			if v, ok := val.(bool); ok && v {
				historyErr = errors.New("mock purge finalize success error")
			}
		})
		if historyErr == nil {
			end := time.Now()
			historyErr = finalizeMLogPurgeHistWithRetry(finalizeCtx, histSQLExec, purgeJobID, mlogID, purgeHistStatusSuccess, historyTime(purgeStart, histLocation), historyTime(end, histLocation), totalPurgeRows, nil)
		}
		if historyErr != nil {
			e.Ctx().GetSessionVars().StmtCtx.AppendWarning(errors.Annotate(
				historyErr, "purge materialized view log: purge committed but failed to finalize purge history",
			))
		}
	}
	stmtCtx := e.Ctx().GetSessionVars().StmtCtx
	stmtCtx.AddAffectedRows(uint64(totalPurgeRows))
	stmtCtx.SetMessage(fmt.Sprintf("Rows inserted: 0  Updated: 0  Deleted: %d", totalPurgeRows))
	return nil
}

func purgeMethod(internal bool) string {
	if internal {
		return "auto"
	}
	return "manual"
}

func validatePurgeMaterializedViewLogStmt(stmt *ast.PurgeMaterializedViewLogStmt, isInternalSQL bool) (string, error) {
	if stmt == nil || stmt.Table == nil || stmt.Table.Name.O == "" {
		return "", errors.New("purge materialized view log: missing table name")
	}
	return purgeMethod(isInternalSQL), nil
}

func applyMLogPurgeMaintenanceSessionVars(
	sessVars *variable.SessionVars,
	targetMemQuota int64,
	targetIsolationReadEngines string,
	bestEffort bool,
) (func(), error) {
	if sessVars == nil {
		return nil, errors.New("mv maintenance: session vars is nil")
	}
	originalEnableMView := sessVars.EnableMView
	restoreMViewEnable := func() {
		if sessVars.EnableMView == originalEnableMView {
			return
		}
		if err := sessVars.SetSystemVar(vardef.TiDBMViewEnable, variable.BoolToOnOff(originalEnableMView)); err != nil {
			logutil.BgLogger().Warn("mv maintenance: failed to restore materialized view enablement", zap.Error(err))
		}
	}
	if !originalEnableMView {
		if err := sessVars.SetSystemVar(vardef.TiDBMViewEnable, "on"); err != nil {
			return nil, errors.Annotate(err, "mv maintenance: failed to enable materialized view support")
		}
	}
	originalMemQuota := sessVars.MemQuotaQuery
	originalIsolationReadEngines := variable.GetIsolationReadEnginesString(sessVars)
	if originalMemQuota != targetMemQuota {
		var applyErr error
		failpoint.Inject("mockMVMaintenanceMemQuotaApplyError", func(val failpoint.Value) {
			if v, ok := val.(bool); ok && v {
				applyErr = errors.New("mock mv maintenance mem quota apply error")
			}
		})
		if applyErr == nil {
			applyErr = sessVars.SetSystemVar(vardef.TiDBMemQuotaQuery, strconv.FormatInt(targetMemQuota, 10))
		}
		if applyErr != nil {
			if !bestEffort {
				restoreMViewEnable()
				return nil, errors.Annotate(applyErr, "mv maintenance: failed to apply maintenance memory quota")
			}
			logutil.BgLogger().Warn("mv maintenance: failed to apply maintenance memory quota, using current value", zap.Error(applyErr))
		}
	}
	if originalIsolationReadEngines != targetIsolationReadEngines {
		if err := sessVars.SetSystemVar(vardef.TiDBIsolationReadEngines, targetIsolationReadEngines); err != nil {
			if !bestEffort {
				if sessVars.MemQuotaQuery != originalMemQuota {
					_ = sessVars.SetSystemVar(vardef.TiDBMemQuotaQuery, strconv.FormatInt(originalMemQuota, 10))
				}
				restoreMViewEnable()
				return nil, errors.Annotate(err, "mv maintenance: failed to apply isolation read engines")
			}
			logutil.BgLogger().Warn("mv maintenance: failed to apply isolation read engines, using current value", zap.Error(err))
		}
	}
	return func() {
		restoreMViewEnable()
		if err := sessVars.SetSystemVar(vardef.TiDBIsolationReadEngines, originalIsolationReadEngines); err != nil {
			logutil.BgLogger().Warn("mv maintenance: failed to restore isolation read engines", zap.Error(err))
		}
		if err := sessVars.SetSystemVar(vardef.TiDBMemQuotaQuery, strconv.FormatInt(originalMemQuota, 10)); err != nil {
			logutil.BgLogger().Warn("mv maintenance: failed to restore memory quota", zap.Error(err))
		}
	}, nil
}

func applyMLogPurgeDeleteTiFlashThreads(
	sessVars *variable.SessionVars,
	targetThreads int64,
	bestEffort bool,
) (func(), error) {
	if sessVars == nil {
		return nil, errors.New("mv maintenance: session vars is nil")
	}
	if targetThreads <= 0 || sessVars.TiFlashMaxThreads == targetThreads {
		return func() {}, nil
	}
	originalThreads := sessVars.TiFlashMaxThreads
	if err := sessVars.SetSystemVar(vardef.TiDBMaxTiFlashThreads, strconv.FormatInt(targetThreads, 10)); err != nil {
		if !bestEffort {
			return nil, errors.Annotate(err, "mv maintenance: failed to apply TiFlash thread limit")
		}
		logutil.BgLogger().Warn("mv maintenance: failed to apply TiFlash thread limit, using current value", zap.Error(err))
		return func() {}, nil
	}
	failpoint.InjectCall("mvMLogPurgeDeleteTiFlashThreadsAppliedOnPurgeDeleteSession", sessVars.TiFlashMaxThreads, targetThreads)
	return func() {
		if err := sessVars.SetSystemVar(vardef.TiDBMaxTiFlashThreads, strconv.FormatInt(originalThreads, 10)); err != nil {
			logutil.BgLogger().Warn("mv maintenance: failed to restore TiFlash thread limit", zap.Error(err))
		}
	}, nil
}

func checkOperateViewOnMLog(ctx sessionctx.Context, schemaName, mlogName ast.CIStr) error {
	pm := privilege.GetPrivilegeManager(ctx)
	user := ctx.GetSessionVars().User
	if pm == nil || user == nil || pm.RequestVerification(
		ctx.GetSessionVars().ActiveRoles, schemaName.L, mlogName.L, "", mysql.OperateViewPriv,
	) {
		return nil
	}
	return plannererrors.ErrTableaccessDenied.GenWithStackByArgs(
		"OPERATE VIEW", user.AuthUsername, user.AuthHostname, mlogName.L,
	)
}

func (e *PurgeMaterializedViewLogExec) resolvePurgeMaterializedViewLogMeta(
	stmt *ast.PurgeMaterializedViewLogStmt,
) (schemaName ast.CIStr, baseTableMeta *model.TableInfo, mlogName ast.CIStr, mlogID int64, mlogShardRowIDBits uint64, mlogInfo *model.MaterializedViewLogInfo, _ error) {
	is, ok := e.Ctx().GetInfoSchema().(infoschema.InfoSchema)
	if !ok {
		return schemaName, nil, mlogName, 0, 0, nil, errors.New("current infoschema does not support materialized view log purge")
	}
	schemaName = stmt.Table.Schema
	if schemaName.O == "" {
		if e.Ctx().GetSessionVars().CurrentDB == "" {
			return schemaName, nil, mlogName, 0, 0, nil, plannererrors.ErrNoDB
		}
		schemaName = ast.NewCIStr(e.Ctx().GetSessionVars().CurrentDB)
		stmt.Table.Schema = schemaName
	}
	if _, ok := is.SchemaByName(schemaName); !ok {
		return schemaName, nil, mlogName, 0, 0, nil, infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schemaName)
	}
	baseTable, err := is.TableByName(context.Background(), schemaName, stmt.Table.Name)
	if err != nil {
		return schemaName, nil, mlogName, 0, 0, nil, err
	}
	if baseTable.Meta().IsView() || baseTable.Meta().IsSequence() || baseTable.Meta().TempTableType != model.TempTableNone {
		return schemaName, nil, mlogName, 0, 0, nil, errors.Errorf("table %s.%s is not a base table", schemaName.O, stmt.Table.Name.O)
	}
	baseTableMeta = baseTable.Meta()
	mlogName = model.MaterializedViewLogTableName(baseTableMeta.Name)
	mlogTable, err := is.TableByName(context.Background(), schemaName, mlogName)
	if err != nil {
		if infoschema.ErrTableNotExists.Equal(err) {
			return schemaName, baseTableMeta, mlogName, 0, 0, nil, errors.Errorf(
				"materialized view log does not exist for base table %s.%s", schemaName.O, stmt.Table.Name.O,
			)
		}
		return schemaName, baseTableMeta, mlogName, 0, 0, nil, err
	}
	mlogInfo = mlogTable.Meta().MaterializedViewLog
	if mlogInfo == nil || mlogInfo.BaseTableID != baseTableMeta.ID {
		return schemaName, baseTableMeta, mlogName, 0, 0, nil, errors.Errorf(
			"table %s.%s is not a materialized view log for base table %s.%s",
			schemaName.O, mlogName.O, schemaName.O, stmt.Table.Name.O,
		)
	}
	return schemaName, baseTableMeta, mlogName, mlogTable.Meta().ID, mlogTable.Meta().ShardRowIDBits, mlogInfo, nil
}

func acquireMaterializedViewLogPurgeLock(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	schemaName ast.CIStr,
	baseTableName ast.CIStr,
	mlogID int64,
) (lastPurgedTSO uint64, hasLastPurgedTSO bool, nextTime *time.Time, _ error) {
	forceConflict := false
	failpoint.Inject("mockPurgeMaterializedViewLogLockConflict", func(val failpoint.Value) {
		if v, ok := val.(bool); ok && v {
			forceConflict = true
		}
	})
	if forceConflict {
		return 0, false, nil, errors.Annotatef(
			errMLogPurgeLockConflict,
			"another purge is running for materialized view log on %s.%s, please retry later",
			schemaName.O, baseTableName.O,
		)
	}
	lockSQL := sqlescape.MustEscapeSQL(
		"SELECT LAST_PURGED_TSO, NEXT_PURGE_UNIX_SECONDS FROM mysql.tidb_mlog_purge_info WHERE MLOG_ID = %? FOR UPDATE NOWAIT",
		mlogID,
	)
	rows, err := sqlexec.ExecSQL(kctx, sqlExec, lockSQL)
	if err != nil {
		if storeerr.ErrLockAcquireFailAndNoWaitSet.Equal(err) {
			return 0, false, nil, errors.Annotatef(
				errMLogPurgeLockConflict,
				"another purge is running for materialized view log on %s.%s, please retry later",
				schemaName.O, baseTableName.O,
			)
		}
		if infoschema.ErrTableNotExists.Equal(err) {
			return 0, false, nil, errors.New("required system table mysql.tidb_mlog_purge_info does not exist")
		}
		return 0, false, nil, errors.Trace(err)
	}
	if len(rows) == 0 {
		return 0, false, nil, errors.Errorf("mlog purge lock row does not exist for mlog id %d", mlogID)
	}
	if !rows[0].IsNull(1) {
		next := time.Unix(rows[0].GetInt64(1), 0).UTC()
		nextTime = &next
	}
	if rows[0].IsNull(0) {
		return 0, false, nextTime, nil
	}
	last := rows[0].GetInt64(0)
	if last < 0 {
		return 0, false, nextTime, errors.Errorf("invalid LAST_PURGED_TSO %d for mlog id %d", last, mlogID)
	}
	return uint64(last), true, nextTime, nil
}

func collectDependentMViewIDsForMLogPurge(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	baseTableMeta *model.TableInfo,
	mlogID int64,
) (publicMVIDs, buildingMVIDs map[int64]struct{}, _ error) {
	publicMVIDs = make(map[int64]struct{})
	if baseInfo := baseTableMeta.MaterializedViewBase; baseInfo != nil {
		for _, id := range baseInfo.MViewIDs {
			if id > 0 {
				publicMVIDs[id] = struct{}{}
			}
		}
	}
	buildingMVIDs = make(map[int64]struct{})
	jobSQL := sqlescape.MustEscapeSQL(
		"SELECT job_meta FROM mysql.tidb_ddl_job WHERE type = %? AND FIND_IN_SET(%?, table_ids)",
		model.ActionCreateMaterializedView, mlogID,
	)
	rows, err := sqlexec.ExecSQL(kctx, sqlExec, jobSQL)
	if err != nil {
		if infoschema.ErrTableNotExists.Equal(err) {
			return publicMVIDs, buildingMVIDs, errors.New("required system table mysql.tidb_ddl_job does not exist")
		}
		return publicMVIDs, buildingMVIDs, errors.Trace(err)
	}
	for _, row := range rows {
		jobBytes := row.GetBytes(0)
		if len(jobBytes) == 0 {
			continue
		}
		job := model.Job{}
		if err := job.Decode(jobBytes); err != nil {
			return publicMVIDs, buildingMVIDs, errors.Trace(err)
		}
		if job.TableID > 0 {
			if _, ok := publicMVIDs[job.TableID]; !ok {
				buildingMVIDs[job.TableID] = struct{}{}
			}
		}
	}
	return publicMVIDs, buildingMVIDs, nil
}

func calcMaterializedViewLogSafePurgeTSO(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	baseSchema string,
	baseTable string,
	purgeStartTS uint64,
	publicMVIDs map[int64]struct{},
	buildingMVIDs map[int64]struct{},
) (uint64, error) {
	safePurgeTSO := purgeStartTS
	buildINList := func(ids []int64) string {
		var b strings.Builder
		for i, id := range ids {
			if i > 0 {
				b.WriteString(",")
			}
			b.WriteString(strconv.FormatInt(id, 10))
		}
		return b.String()
	}
	publicIDs := make([]int64, 0, len(publicMVIDs))
	for id := range publicMVIDs {
		publicIDs = append(publicIDs, id)
	}
	if len(publicIDs) > 0 {
		rows, err := sqlexec.ExecSQL(kctx, sqlExec, fmt.Sprintf(
			"SELECT COUNT(1) FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID IN (%s)", buildINList(publicIDs),
		))
		if err != nil {
			if infoschema.ErrTableNotExists.Equal(err) {
				return safePurgeTSO, errors.New("required system table mysql.tidb_mview_refresh_info does not exist")
			}
			return safePurgeTSO, errors.Trace(err)
		}
		count := int64(0)
		if len(rows) > 0 {
			count = rows[0].GetInt64(0)
		}
		if count != int64(len(publicIDs)) {
			return safePurgeTSO, errors.Errorf(
				"materialized view refresh info is missing for some dependent materialized views on base table %s.%s (expected %d, got %d)",
				baseSchema, baseTable, len(publicIDs), count,
			)
		}
	}
	allIDs := make([]int64, 0, len(publicMVIDs)+len(buildingMVIDs))
	seen := make(map[int64]struct{}, len(publicMVIDs)+len(buildingMVIDs))
	for id := range publicMVIDs {
		seen[id] = struct{}{}
		allIDs = append(allIDs, id)
	}
	for id := range buildingMVIDs {
		if _, ok := seen[id]; !ok {
			allIDs = append(allIDs, id)
		}
	}
	if len(allIDs) == 0 {
		return safePurgeTSO, nil
	}
	rows, err := sqlexec.ExecSQL(kctx, sqlExec, fmt.Sprintf(
		"SELECT MIN(COALESCE(LAST_SUCCESS_READ_TSO, CAST(0 AS UNSIGNED))) FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID IN (%s)",
		buildINList(allIDs),
	))
	if err != nil {
		if infoschema.ErrTableNotExists.Equal(err) {
			return safePurgeTSO, errors.New("required system table mysql.tidb_mview_refresh_info does not exist")
		}
		return safePurgeTSO, errors.Trace(err)
	}
	if len(rows) > 0 && !rows[0].IsNull(0) {
		value := rows[0].GetUint64(0)
		if value < safePurgeTSO {
			safePurgeTSO = value
		}
	}
	return safePurgeTSO, nil
}

func purgeMaterializedViewLogData(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	sessVars *variable.SessionVars,
	schemaName, mlogName string,
	lastPurgedTSO uint64,
	hasLastPurgedTSO bool,
	safePurgeTSO uint64,
	rowIDRange *mlogPurgeDeleteRowIDRange,
	batchSize int64,
) (int64, error) {
	failpoint.Inject("mockPurgeMaterializedViewLogDeleteErr", func(val failpoint.Value) {
		if v, ok := val.(bool); ok && v {
			failpoint.Return(int64(0), errors.New("mock purge mlog delete error"))
		}
	})
	failpoint.Inject("mockPurgeMaterializedViewLogDeleteRows", func(val failpoint.Value) {
		switch v := val.(type) {
		case int:
			failpoint.Return(int64(v), nil)
		case int64:
			failpoint.Return(v, nil)
		}
	})
	deleteSQL := buildPurgeMaterializedViewLogDeleteSQL(
		schemaName, mlogName, lastPurgedTSO, hasLastPurgedTSO, safePurgeTSO, rowIDRange, batchSize,
	)
	failpoint.InjectCall("purgeMaterializedViewLogDeleteSQL", deleteSQL)
	if sessVars == nil {
		return 0, errors.New("purge materialized view log: delete session vars is nil")
	}
	originalMaintenance := sessVars.InMViewMaintenance
	originalScanUserTables := sessVars.InternalSQLScanUserTable
	sessVars.InMViewMaintenance = true
	sessVars.InternalSQLScanUserTable = true
	defer func() {
		sessVars.InMViewMaintenance = originalMaintenance
		sessVars.InternalSQLScanUserTable = originalScanUserTables
	}()
	_, err := sqlExec.ExecuteInternal(kctx, deleteSQL)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return int64(sessVars.StmtCtx.AffectedRows()), nil
}

func buildPurgeMaterializedViewLogDeleteSQL(
	schemaName, mlogName string,
	lastPurgedTSO uint64,
	hasLastPurgedTSO bool,
	safePurgeTSO uint64,
	rowIDRange *mlogPurgeDeleteRowIDRange,
	batchSize int64,
) string {
	if rowIDRange != nil {
		if hasLastPurgedTSO {
			return sqlescape.MustEscapeSQL(
				"DELETE /*+ read_from_storage(tiflash[%n.%n]) */ FROM %n.%n WHERE _tidb_rowid >= %? AND _tidb_rowid <= %? AND _tidb_commit_ts > %? AND _tidb_commit_ts <= %? LIMIT %?",
				schemaName, mlogName, schemaName, mlogName, rowIDRange.startRowID, rowIDRange.endRowID, lastPurgedTSO, safePurgeTSO, batchSize,
			)
		}
		return sqlescape.MustEscapeSQL(
			"DELETE /*+ read_from_storage(tiflash[%n.%n]) */ FROM %n.%n WHERE _tidb_rowid >= %? AND _tidb_rowid <= %? AND _tidb_commit_ts <= %? LIMIT %?",
			schemaName, mlogName, schemaName, mlogName, rowIDRange.startRowID, rowIDRange.endRowID, safePurgeTSO, batchSize,
		)
	}
	if hasLastPurgedTSO {
		return sqlescape.MustEscapeSQL(
			"DELETE /*+ read_from_storage(tiflash[%n.%n]) */ FROM %n.%n WHERE _tidb_commit_ts > %? AND _tidb_commit_ts <= %? LIMIT %?",
			schemaName, mlogName, schemaName, mlogName, lastPurgedTSO, safePurgeTSO, batchSize,
		)
	}
	return sqlescape.MustEscapeSQL(
		"DELETE /*+ read_from_storage(tiflash[%n.%n]) */ FROM %n.%n WHERE _tidb_commit_ts <= %? LIMIT %?",
		schemaName, mlogName, schemaName, mlogName, safePurgeTSO, batchSize,
	)
}

func tryBuildMLogPurgeDeletePlanBestEffort(
	kctx context.Context,
	sessVars *variable.SessionVars,
	evalSctx sessionctx.Context,
	sqlExec sqlexec.SQLExecutor,
	countSessVars *variable.SessionVars,
	mlogInfo *model.MaterializedViewLogInfo,
	isInternalSQL bool,
	schemaName, mlogName string,
	mlogShardRowIDBits uint64,
	lastPurgedTSO uint64,
	hasLastPurgedTSO bool,
	safePurgeTSO uint64,
	fallbackNextTime *time.Time,
) *mlogPurgeDeletePlan {
	if safePurgeTSO == 0 {
		return nil
	}
	stats, err := readMLogPurgePendingRowStatsOnTiFlash(
		kctx, sqlExec, countSessVars, schemaName, mlogName, lastPurgedTSO, hasLastPurgedTSO, safePurgeTSO,
	)
	if err != nil {
		logutil.BgLogger().Warn(
			"purge materialized view log: failed to read pending row stats, fallback to unscoped unthrottled purge",
			zap.String("schemaName", schemaName), zap.String("tableName", mlogName), zap.Error(err),
		)
		return nil
	}
	plan := &mlogPurgeDeletePlan{pendingRows: stats.pendingRows}
	if stats.pendingRows <= 0 {
		return plan
	}
	plan.rowIDRanges = buildMLogPurgeDeleteRowIDRanges(stats, mlogShardRowIDBits)
	throttleCfg, err := loadMLogPurgeThrottleConfig(kctx, sessVars)
	if err != nil {
		logutil.BgLogger().Warn("purge materialized view log: failed to load adaptive throttle config, fallback to unthrottled purge", zap.String("schemaName", schemaName), zap.String("tableName", mlogName), zap.Uint64("safePurgeTSO", safePurgeTSO), zap.Error(err))
		return plan
	}
	throttleDeadline, err := deriveMLogPurgeThrottleDeadline(kctx, evalSctx, mlogInfo, isInternalSQL, schemaName, mlogName, fallbackNextTime)
	if err != nil {
		logutil.BgLogger().Warn("purge materialized view log: failed to derive adaptive throttle deadline, fallback to unthrottled purge", zap.String("schemaName", schemaName), zap.String("tableName", mlogName), zap.Uint64("safePurgeTSO", safePurgeTSO), zap.Error(err))
		return plan
	}
	plan.throttlePlan = tryBuildMLogPurgeThrottlePlan(stats, throttleDeadline, throttleCfg)
	return plan
}

func loadMLogPurgeThrottleConfig(kctx context.Context, sessVars *variable.SessionVars) (mlogPurgeThrottleConfig, error) {
	if sessVars == nil {
		return mlogPurgeThrottleConfig{}, errors.New("purge materialized view log: session vars is nil")
	}
	minRateStr, err := sessVars.GetSessionOrGlobalSystemVar(kctx, vardef.TiDBMLogPurgeMinRate)
	if err != nil {
		return mlogPurgeThrottleConfig{}, errors.Trace(err)
	}
	minRate, err := strconv.ParseFloat(minRateStr, 64)
	if err != nil {
		return mlogPurgeThrottleConfig{}, errors.Trace(err)
	}
	ratioStr, err := sessVars.GetSessionOrGlobalSystemVar(kctx, vardef.TiDBMLogPurgeRateBudgetRatio)
	if err != nil {
		return mlogPurgeThrottleConfig{}, errors.Trace(err)
	}
	ratio, err := strconv.ParseFloat(ratioStr, 64)
	if err != nil {
		return mlogPurgeThrottleConfig{}, errors.Trace(err)
	}
	return mlogPurgeThrottleConfig{minRate: minRate, budgetRatio: ratio}, nil
}

func deriveMLogPurgeThrottleDeadline(
	kctx context.Context,
	evalSctx sessionctx.Context,
	mlogInfo *model.MaterializedViewLogInfo,
	isInternalSQL bool,
	schemaName, mlogName string,
	fallbackNextTime *time.Time,
) (*time.Time, error) {
	failpoint.Inject("mockMLogPurgeAdaptiveDeadlineErr", func(val failpoint.Value) {
		if v, ok := val.(bool); ok && v {
			failpoint.Return(nil, errors.New("mock adaptive purge deadline error"))
		}
	})
	deadline := time.Now().UTC().Add(mlogPurgeAdaptiveMaxBudget)
	if isInternalSQL {
		if mlogInfo == nil || evalSctx == nil {
			return nil, errors.New("purge materialized view log: schedule evaluation metadata is unavailable")
		}
		tz, err := mlogInfo.PurgeScheduleTimeZone.GetLocation()
		if err != nil {
			return nil, errors.Trace(err)
		}
		next, shouldUpdate, err := deriveMLogPurgeNextUnixSeconds(kctx, evalSctx, mlogInfo, tz)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if shouldUpdate && next != nil {
			nextTime := time.Unix(*next, 0).UTC()
			if nextTime.Before(deadline) {
				deadline = nextTime
			}
		}
	} else if fallbackNextTime != nil && fallbackNextTime.Before(deadline) {
		deadline = *fallbackNextTime
	}
	return &deadline, nil
}

func tryBuildMLogPurgeThrottlePlan(stats mlogPurgePendingRowStats, deadline *time.Time, cfg mlogPurgeThrottleConfig) *mlogPurgeThrottlePlan {
	if stats.pendingRows <= 0 || deadline == nil {
		return nil
	}
	now := time.Now().UTC()
	if !deadline.After(now) || cfg.budgetRatio <= 0 {
		return nil
	}
	budget := time.Duration(float64(deadline.Sub(now)) * cfg.budgetRatio)
	if budget <= 0 {
		return nil
	}
	targetRate := float64(stats.pendingRows) / budget.Seconds()
	if targetRate < cfg.minRate {
		targetRate = cfg.minRate
	}
	return &mlogPurgeThrottlePlan{targetRate: targetRate, pendingRows: stats.pendingRows, effectiveBatchSize: calcMLogPurgeAdaptiveBatchSize(targetRate), minRate: cfg.minRate, deadline: *deadline}
}

func calcMLogPurgeAdaptiveBatchSize(targetRate float64) int64 {
	if targetRate <= 0 {
		return mlogPurgeAdaptiveMinBatchSize
	}
	batchSize := int64(math.Ceil(targetRate * mlogPurgeAdaptiveBatchWindow.Seconds()))
	if batchSize < mlogPurgeAdaptiveMinBatchSize {
		return mlogPurgeAdaptiveMinBatchSize
	}
	return batchSize
}

func readMLogPurgePendingRowStatsOnTiFlash(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	sessVars *variable.SessionVars,
	schemaName, mlogName string,
	lastPurgedTSO uint64,
	hasLastPurgedTSO bool,
	safePurgeTSO uint64,
) (mlogPurgePendingRowStats, error) {
	failpoint.Inject("mockMLogPurgeAdaptiveCountErr", func(val failpoint.Value) {
		if v, ok := val.(bool); ok && v {
			failpoint.Return(mlogPurgePendingRowStats{}, errors.New("mock adaptive purge count error"))
		}
	})
	if sessVars == nil {
		return mlogPurgePendingRowStats{}, errors.New("purge materialized view log: count session vars is nil")
	}
	restoreIsolation, err := setSessionVarWithRestore(sessVars, vardef.TiDBIsolationReadEngines, kv.TiFlash.Name())
	if err != nil {
		return mlogPurgePendingRowStats{}, err
	}
	defer restoreIsolation()
	restoreFallback, err := setSessionVarWithRestore(sessVars, vardef.TiDBAllowFallbackToTiKV, "")
	if err != nil {
		return mlogPurgePendingRowStats{}, err
	}
	defer restoreFallback()
	originalMaintenance := sessVars.InMViewMaintenance
	originalScanUserTables := sessVars.InternalSQLScanUserTable
	sessVars.InMViewMaintenance = true
	sessVars.InternalSQLScanUserTable = true
	defer func() {
		sessVars.InMViewMaintenance = originalMaintenance
		sessVars.InternalSQLScanUserTable = originalScanUserTables
	}()
	countCtx, cancel := context.WithTimeout(kctx, mlogPurgeAdaptiveCountTimeout)
	defer cancel()
	var countSQL string
	if hasLastPurgedTSO {
		countSQL = sqlescape.MustEscapeSQL(
			"SELECT /*+ read_from_storage(tiflash[%n.%n]) */ COUNT(*), MIN(_tidb_rowid), MAX(_tidb_rowid) FROM %n.%n WHERE _tidb_commit_ts > %? AND _tidb_commit_ts <= %?",
			schemaName, mlogName, schemaName, mlogName, lastPurgedTSO, safePurgeTSO,
		)
	} else {
		countSQL = sqlescape.MustEscapeSQL(
			"SELECT /*+ read_from_storage(tiflash[%n.%n]) */ COUNT(*), MIN(_tidb_rowid), MAX(_tidb_rowid) FROM %n.%n WHERE _tidb_commit_ts <= %?",
			schemaName, mlogName, schemaName, mlogName, safePurgeTSO,
		)
	}
	rows, err := sqlexec.ExecSQL(countCtx, sqlExec, countSQL)
	if err != nil {
		return mlogPurgePendingRowStats{}, err
	}
	if len(rows) == 0 || rows[0].IsNull(0) {
		return mlogPurgePendingRowStats{}, nil
	}
	stats := mlogPurgePendingRowStats{pendingRows: rows[0].GetInt64(0)}
	if stats.pendingRows <= 0 || rows[0].IsNull(1) || rows[0].IsNull(2) {
		return stats, nil
	}
	stats.minRowID = rows[0].GetInt64(1)
	stats.maxRowID = rows[0].GetInt64(2)
	stats.hasRowIDBounds = true
	return stats, nil
}

func buildMLogPurgeDeleteRowIDRanges(stats mlogPurgePendingRowStats, shardRowIDBits uint64) []mlogPurgeDeleteRowIDRange {
	if stats.pendingRows <= 0 || !stats.hasRowIDBounds || stats.minRowID > stats.maxRowID {
		return nil
	}
	rangeCount := stats.pendingRows / mlogPurgeAdaptiveMinBatchSize
	if rangeCount < 1 {
		rangeCount = 1
	}
	if rangeCount > mlogPurgeAdaptiveMaxRangeCount {
		rangeCount = mlogPurgeAdaptiveMaxRangeCount
	}
	if shardRowIDBits > 0 {
		shardBucketCount := int64(1) << shardRowIDBits
		if shardBucketCount > 0 && rangeCount > shardBucketCount {
			rangeCount = shardBucketCount
		}
		rangeCount = floorPowerOfTwo(rangeCount)
		return buildShardedMLogPurgeDeleteRowIDRanges(stats.minRowID, stats.maxRowID, shardRowIDBits, rangeCount)
	}
	return buildLinearMLogPurgeDeleteRowIDRanges(stats.minRowID, stats.maxRowID, rangeCount)
}

func buildLinearMLogPurgeDeleteRowIDRanges(minRowID, maxRowID, rangeCount int64) []mlogPurgeDeleteRowIDRange {
	if rangeCount <= 1 || minRowID >= maxRowID {
		return []mlogPurgeDeleteRowIDRange{{startRowID: minRowID, endRowID: maxRowID}}
	}
	span := maxRowID - minRowID + 1
	if span <= rangeCount {
		return []mlogPurgeDeleteRowIDRange{{startRowID: minRowID, endRowID: maxRowID}}
	}
	step := span / rangeCount
	ranges := make([]mlogPurgeDeleteRowIDRange, 0, int(rangeCount))
	for i := int64(0); i < rangeCount; i++ {
		start := minRowID + i*step
		end := maxRowID
		if i < rangeCount-1 {
			end = minRowID + (i+1)*step - 1
		}
		if start <= end {
			ranges = append(ranges, mlogPurgeDeleteRowIDRange{startRowID: start, endRowID: end})
		}
	}
	return ranges
}

func buildShardedMLogPurgeDeleteRowIDRanges(minRowID, maxRowID int64, shardRowIDBits uint64, rangeCount int64) []mlogPurgeDeleteRowIDRange {
	if rangeCount <= 1 || minRowID >= maxRowID {
		return []mlogPurgeDeleteRowIDRange{{startRowID: minRowID, endRowID: maxRowID}}
	}
	shardBucketCount := int64(1) << shardRowIDBits
	if shardBucketCount <= 0 {
		return buildLinearMLogPurgeDeleteRowIDRanges(minRowID, maxRowID, rangeCount)
	}
	rangeCount = floorPowerOfTwo(rangeCount)
	if rangeCount <= 1 {
		return []mlogPurgeDeleteRowIDRange{{startRowID: minRowID, endRowID: maxRowID}}
	}
	shardFmt := autoid.NewShardIDFormat(types.NewFieldType(mysql.TypeLonglong), uint64(shardRowIDBits), autoid.RowIDBitLength)
	bucketsPerRange := shardBucketCount / rangeCount
	if bucketsPerRange <= 0 {
		return []mlogPurgeDeleteRowIDRange{{startRowID: minRowID, endRowID: maxRowID}}
	}
	ranges := make([]mlogPurgeDeleteRowIDRange, 0, int(rangeCount))
	for i := int64(0); i < rangeCount; i++ {
		start := int64(uint64(i*bucketsPerRange) << shardFmt.IncrementalBits)
		end := int64((uint64((i+1)*bucketsPerRange) << shardFmt.IncrementalBits) - 1)
		if i == rangeCount-1 {
			end = maxRowID
		}
		if start < minRowID {
			start = minRowID
		}
		if end > maxRowID {
			end = maxRowID
		}
		if start <= end {
			ranges = append(ranges, mlogPurgeDeleteRowIDRange{startRowID: start, endRowID: end})
		}
	}
	return ranges
}

func floorPowerOfTwo(value int64) int64 {
	if value <= 0 {
		return 0
	}
	return 1 << (bits.Len64(uint64(value)) - 1)
}

func setSessionVarWithRestore(sessVars *variable.SessionVars, varName, value string) (func(), error) {
	original, err := sessVars.GetSessionOrGlobalSystemVar(context.Background(), varName)
	if err != nil {
		return nil, err
	}
	if original == value {
		return func() {}, nil
	}
	if err := sessVars.SetSystemVar(varName, value); err != nil {
		return nil, err
	}
	return func() {
		if err := sessVars.SetSystemVar(varName, original); err != nil {
			logutil.BgLogger().Warn("purge materialized view log: failed to restore session variable", zap.String("var", varName), zap.Error(err))
		}
	}, nil
}

func (p *mlogPurgeThrottlePlan) maybeSleep(kctx context.Context, start time.Time, totalDeletedRows int64) error {
	failpoint.Inject("mockMLogPurgeAdaptiveSleepErr", func(val failpoint.Value) {
		if v, ok := val.(bool); ok && v {
			failpoint.Return(errors.New("mock adaptive purge sleep error"))
		}
	})
	if p == nil || p.targetRate <= 0 || totalDeletedRows <= 0 {
		return nil
	}
	expectedElapsed := time.Duration(float64(totalDeletedRows) / p.targetRate * float64(time.Second))
	actualElapsed := time.Since(start)
	sleepFor := expectedElapsed - actualElapsed
	failpoint.InjectCall("mvPurgeAdaptiveThrottleSleepComputed", totalDeletedRows, sleepFor)
	if sleepFor <= 0 {
		p.noWaitStreak++
		return p.recalculateBatchSizeOnNoWait(totalDeletedRows)
	}
	p.noWaitStreak = 0
	timer := time.NewTimer(sleepFor)
	defer timer.Stop()
	select {
	case <-kctx.Done():
		return kctx.Err()
	case <-timer.C:
		return nil
	}
}

func (p *mlogPurgeThrottlePlan) recalculateBatchSizeOnNoWait(totalDeletedRows int64) error {
	if p == nil || p.noWaitStreak < 2 || p.pendingRows <= 0 || p.deadline.IsZero() {
		return nil
	}
	remainingRows := p.pendingRows - totalDeletedRows
	remainingBudget := time.Until(p.deadline)
	if remainingRows <= 0 || remainingBudget <= 0 {
		return nil
	}
	newRate := float64(remainingRows) / remainingBudget.Seconds()
	if newRate < p.minRate {
		newRate = p.minRate
	}
	p.targetRate = newRate
	p.effectiveBatchSize = calcMLogPurgeAdaptiveBatchSize(newRate)
	p.noWaitStreak = 0
	return nil
}

func (p *mlogPurgeThrottlePlan) effectiveDeleteBatchSize(configuredBatchSize int64) int64 {
	if p == nil || configuredBatchSize <= 0 {
		return configuredBatchSize
	}
	if p.effectiveBatchSize <= 0 {
		p.effectiveBatchSize = calcMLogPurgeAdaptiveBatchSize(p.targetRate)
	}
	if p.effectiveBatchSize > configuredBatchSize {
		p.effectiveBatchSize = configuredBatchSize
	}
	failpoint.InjectCall("mvPurgeAdaptiveBatchSizeComputed", configuredBatchSize, p.effectiveBatchSize)
	return p.effectiveBatchSize
}

func deriveMLogPurgeNextUnixSeconds(
	kctx context.Context,
	evalSctx sessionctx.Context,
	mlogInfo *model.MaterializedViewLogInfo,
	scheduleTimeZone *time.Location,
) (*int64, bool, error) {
	if strings.TrimSpace(mlogInfo.PurgeNext) == "" {
		return nil, true, nil
	}
	nextAt, shouldUpdate, err := expression.DeriveMaterializedScheduleNextTime(
		kctx, evalSctx, mlogInfo.PurgeStartWith, mlogInfo.PurgeNext,
		mlogInfo.DefinitionSQLMode, scheduleTimeZone,
	)
	if err != nil {
		return nil, false, err
	}
	if nextAt == nil {
		return nil, shouldUpdate, nil
	}
	nextUnixSeconds, err := expression.MaterializedScheduleTimeToUnixSeconds(nextAt, scheduleTimeZone)
	return nextUnixSeconds, shouldUpdate, errors.Trace(err)
}

func updateMaterializedViewLogPurgeInfoOnSuccess(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	mlogID int64,
	lastPurgedTSO *uint64,
	nextUnixSeconds *int64,
	shouldUpdateNext bool,
) error {
	if lastPurgedTSO != nil {
		_, err := sqlExec.ExecuteInternal(kctx, `UPDATE mysql.tidb_mlog_purge_info
SET LAST_PURGED_TSO = %?
WHERE MLOG_ID = %? AND (LAST_PURGED_TSO IS NULL OR LAST_PURGED_TSO < %?)`,
			*lastPurgedTSO, mlogID, *lastPurgedTSO)
		if err != nil {
			if infoschema.ErrTableNotExists.Equal(err) {
				return errors.New("required system table mysql.tidb_mlog_purge_info does not exist")
			}
			return errors.Trace(err)
		}
	}
	if !shouldUpdateNext {
		return nil
	}
	var value any
	if nextUnixSeconds != nil {
		value = *nextUnixSeconds
	}
	_, err := sqlExec.ExecuteInternal(kctx, `UPDATE mysql.tidb_mlog_purge_info
SET NEXT_PURGE_UNIX_SECONDS = %? WHERE MLOG_ID = %?`, value, mlogID)
	if err != nil {
		if infoschema.ErrTableNotExists.Equal(err) {
			return errors.New("required system table mysql.tidb_mlog_purge_info does not exist")
		}
		return errors.Trace(err)
	}
	return nil
}

func readLatestMLogPurgeCutoffFenceTSO(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	mlogID int64,
) (uint64, bool, error) {
	rows, err := sqlexec.ExecSQL(kctx, sqlExec, `SELECT PURGE_CUTOFF_TSO
FROM mysql.tidb_mlog_purge_hist
WHERE MLOG_ID = %? AND PURGE_CUTOFF_TSO IS NOT NULL
ORDER BY PURGE_JOB_ID DESC LIMIT 1`, mlogID)
	if err != nil {
		if infoschema.ErrTableNotExists.Equal(err) {
			return 0, false, errors.New("purge materialized view log: required system table mysql.tidb_mlog_purge_hist does not exist")
		}
		return 0, false, errors.Trace(err)
	}
	if len(rows) == 0 || rows[0].IsNull(0) {
		return 0, false, nil
	}
	return rows[0].GetUint64(0), true, nil
}

func insertMLogPurgeHistRunning(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	purgeJobID uint64,
	mlogID int64,
	baseSchema, baseTable, method string,
	cutoffTSO uint64,
	startAt time.Time,
) error {
	_, err := sqlExec.ExecuteInternal(kctx, `INSERT INTO mysql.tidb_mlog_purge_hist (
PURGE_JOB_ID, MLOG_ID, BASE_TABLE_SCHEMA, BASE_TABLE_NAME, PURGE_METHOD,
PURGE_START_TIME, PURGE_ROWS, PURGE_STATUS, PURGE_CUTOFF_TSO, LAST_HEARTBEAT_TIME)
VALUES (%?, %?, %?, %?, %?, %?, %?, %?, %?, %?)`,
		purgeJobID, mlogID, baseSchema, baseTable, method, startAt, int64(0), purgeHistStatusRunning, cutoffTSO, startAt,
	)
	if err != nil {
		if infoschema.ErrTableNotExists.Equal(err) {
			return errors.New("required system table mysql.tidb_mlog_purge_hist does not exist")
		}
		return errors.Trace(err)
	}
	return nil
}

func insertMLogPurgeHistFailed(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	purgeJobID uint64,
	mlogID int64,
	baseSchema, baseTable, method string,
	startAt, endAt time.Time,
	rows int64,
	reason *string,
) error {
	var reasonValue any
	if reason != nil {
		reasonValue = *reason
	}
	_, err := sqlExec.ExecuteInternal(kctx, `INSERT INTO mysql.tidb_mlog_purge_hist (
PURGE_JOB_ID, MLOG_ID, BASE_TABLE_SCHEMA, BASE_TABLE_NAME, PURGE_METHOD,
PURGE_START_TIME, PURGE_END_TIME, PURGE_ROWS, PURGE_DURATION_SEC, PURGE_STATUS, PURGE_FAILED_REASON)
VALUES (%?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?)`,
		purgeJobID, mlogID, baseSchema, baseTable, method, startAt, endAt, rows,
		formatPurgeDuration(startAt, endAt), purgeHistStatusFailed, reasonValue,
	)
	if err != nil {
		if infoschema.ErrTableNotExists.Equal(err) {
			return errors.New("required system table mysql.tidb_mlog_purge_hist does not exist")
		}
		return errors.Trace(err)
	}
	return nil
}

func allocJobID(store kv.Storage) (uint64, error) {
	if store == nil {
		return 0, errors.New("invalid store")
	}
	ver, err := store.CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if ver.Ver == 0 {
		return 0, errors.New("invalid job id")
	}
	return ver.Ver, nil
}

func (e *PurgeMaterializedViewLogExec) insertMLogPurgeHistFailedFallback(
	kctx context.Context,
	releaseCtx context.Context,
	mlogID int64,
	baseSchema, baseTable, method string,
	purgeJobID *uint64,
	taskCancelController *mvTaskCancelController,
	purgeStart time.Time,
	purgeRows int64,
	purgeErr error,
) error {
	purgeFailedReason, finalErr := taskCancelController.normalizeTaskFailure(purgeErr)
	histSctx, err := e.GetSysSession()
	if err != nil {
		return errors.Annotatef(err, "purge materialized view log: failed to open history session after error %v", finalErr)
	}
	defer e.ReleaseSysSession(releaseCtx, histSctx)
	histLoc := histSctx.GetSessionVars().Location()
	if *purgeJobID == 0 {
		*purgeJobID, err = allocJobID(e.Ctx().GetStore())
		if err != nil {
			return errors.Annotatef(err, "purge materialized view log: failed to allocate history job id after error %v", finalErr)
		}
	}
	purgeErrMsg := finalErr.Error()
	if purgeFailedReason != nil {
		purgeErrMsg = *purgeFailedReason
	}
	endAt := time.Now()
	if err := insertMLogPurgeHistFailed(
		kctx,
		histSctx.GetSQLExecutor(),
		*purgeJobID,
		mlogID,
		baseSchema,
		baseTable,
		method,
		historyTime(purgeStart, histLoc),
		historyTime(endAt, histLoc),
		purgeRows,
		&purgeErrMsg,
	); err != nil {
		return errors.Annotatef(err, "purge materialized view log: failed to insert failed purge history after error %v", finalErr)
	}
	return errors.Trace(finalErr)
}

func finalizeMLogPurgeHist(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	purgeJobID uint64,
	status string,
	startAt, endAt time.Time,
	rows int64,
	reason *string,
) error {
	var reasonValue any
	if reason != nil {
		reasonValue = *reason
	}
	_, err := sqlExec.ExecuteInternal(kctx, `UPDATE mysql.tidb_mlog_purge_hist
SET PURGE_END_TIME = %?, PURGE_ROWS = %?, PURGE_DURATION_SEC = %?, PURGE_STATUS = %?, PURGE_FAILED_REASON = %?
WHERE PURGE_JOB_ID = %?`,
		endAt, rows, formatPurgeDuration(startAt, endAt), status, reasonValue, purgeJobID,
	)
	failpoint.Inject("mockUpdateMaterializedViewLogPurgeStateErr", func(val failpoint.Value) {
		if v, ok := val.(bool); ok && v {
			err = errors.New("mock update mlog purge state error")
		}
	})
	if err != nil {
		if infoschema.ErrTableNotExists.Equal(err) {
			return errors.New("required system table mysql.tidb_mlog_purge_hist does not exist")
		}
		return errors.Trace(err)
	}
	return nil
}

func finalizeMLogPurgeHistWithRetry(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	purgeJobID uint64,
	mlogID int64,
	status string,
	startAt, endAt time.Time,
	rows int64,
	reason *string,
) error {
	firstErr := finalizeMLogPurgeHist(kctx, sqlExec, purgeJobID, status, startAt, endAt, rows, reason)
	if firstErr == nil {
		return nil
	}
	secondErr := finalizeMLogPurgeHist(kctx, sqlExec, purgeJobID, status, startAt, endAt, rows, reason)
	if secondErr == nil {
		return nil
	}
	logutil.BgLogger().Warn("purge materialized view log: failed to finalize purge history after retry",
		zap.Uint64("purgeJobID", purgeJobID), zap.Int64("mlogID", mlogID), zap.String("status", status),
		zap.NamedError("firstAttemptErr", firstErr), zap.NamedError("retryErr", secondErr))
	return errors.Annotatef(secondErr, "first finalize attempt failed: %v", firstErr)
}

func formatPurgeDuration(startAt, endAt time.Time) string {
	d := endAt.Sub(startAt)
	if d <= 0 {
		return "0.000000"
	}
	return fmt.Sprintf("%d.%06d", d.Microseconds()/1_000_000, d.Microseconds()%1_000_000)
}

func historyTime(t time.Time, loc *time.Location) time.Time {
	if loc != nil {
		t = t.In(loc)
	}
	return t.Truncate(time.Microsecond)
}
