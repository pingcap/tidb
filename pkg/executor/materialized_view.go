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
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	storeerr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	plannererrors "github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/generatedexpr"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

// RefreshMaterializedViewExec executes "REFRESH MATERIALIZED VIEW" as a utility-style statement.
type RefreshMaterializedViewExec struct {
	exec.BaseExecutor
	stmt *ast.RefreshMaterializedViewStmt
	done bool
}

var errMLogPurgeLockConflict = errors.NewNoStackError("mlog purge lock conflict")

const (
	purgeHistStatusRunning = "running"
	purgeHistStatusSuccess = "success"
	purgeHistStatusFailed  = "failed"
)

// PurgeMaterializedViewLogExec executes "PURGE MATERIALIZED VIEW LOG" as a utility-style statement.
type PurgeMaterializedViewLogExec struct {
	exec.BaseExecutor
	stmt *ast.PurgeMaterializedViewLogStmt
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

// Next implements the Executor Next interface.
func (e *PurgeMaterializedViewLogExec) Next(ctx context.Context, _ *chunk.Chunk) (err error) {
	if e.done {
		return nil
	}
	e.done = true

	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMVMaintenance)

	return e.executePurgeMaterializedViewLog(ctx, e.stmt)
}

func (e *PurgeMaterializedViewLogExec) executePurgeMaterializedViewLog(
	kctx context.Context,
	s *ast.PurgeMaterializedViewLogStmt,
) (err error) {
	schemaName, baseTableMeta, mlogName, mlogID, mlogInfo, err := e.resolvePurgeMaterializedViewLogMeta(s)
	if err != nil {
		return err
	}
	isInternalSQL := e.Ctx().GetSessionVars().InRestrictedSQL
	batchSize := int64(e.Ctx().GetSessionVars().MLogPurgeBatchSize)
	if batchSize <= 0 {
		batchSize = int64(variable.DefTiDBMLogPurgeBatchSize)
	}

	purgeSctx, err := e.GetSysSession()
	if err != nil {
		return err
	}
	defer e.ReleaseSysSession(kctx, purgeSctx)
	sqlExec := purgeSctx.GetSQLExecutor()

	histSctx, err := e.GetSysSession()
	if err != nil {
		return err
	}
	defer e.ReleaseSysSession(kctx, histSctx)
	histSQLExec := histSctx.GetSQLExecutor()

	var scheduleEvalSctx sessionctx.Context
	if isInternalSQL {
		scheduleEvalSctx, err = e.GetSysSession()
		if err != nil {
			return err
		}
		defer e.ReleaseSysSession(kctx, scheduleEvalSctx)
	}

	totalPurgeRows := int64(0)
	safePurgeTSOReady := false
	safePurgeTSO := uint64(0)
	lockedLastPurgedTSO := uint64(0)
	lockedLastPurgedTSOReady := false
	purgeJobID := uint64(0)
	purgeHistRunningInserted := false

	finalizeFailure := func(purgeErr error) error {
		if !purgeHistRunningInserted {
			return errors.Trace(purgeErr)
		}
		if histErr := finalizeMLogPurgeHist(
			kctx,
			histSQLExec,
			purgeJobID,
			purgeHistStatusFailed,
			totalPurgeRows,
		); histErr != nil {
			return errors.Annotatef(histErr, "purge materialized view log: failed to finalize purge history after error %v", purgeErr)
		}
		return errors.Trace(purgeErr)
	}
	finalizeSuccess := func() error {
		if !purgeHistRunningInserted {
			return nil
		}
		return finalizeMLogPurgeHist(
			kctx,
			histSQLExec,
			purgeJobID,
			purgeHistStatusSuccess,
			totalPurgeRows,
		)
	}

	for {
		if _, err = sqlExec.ExecuteInternal(kctx, "BEGIN PESSIMISTIC"); err != nil {
			return errors.Trace(err)
		}

		lastPurgedTSO, hasLastPurgedTSO, err := acquireMaterializedViewLogPurgeLock(kctx, sqlExec, schemaName, s.Table.Name, mlogID)
		if err != nil {
			_, _ = sqlExec.ExecuteInternal(kctx, "ROLLBACK")
			if isMLogPurgeLockConflict(err) && totalPurgeRows > 0 {
				if histErr := finalizeSuccess(); histErr != nil {
					return errors.Trace(histErr)
				}
				e.Ctx().GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf(
					"purge materialized view log on %s.%s stopped before deleting all eligible rows due to lock conflict after deleting %d rows; please retry later",
					schemaName.O,
					s.Table.Name.O,
					totalPurgeRows,
				))
				return nil
			}
			return err
		}

		var batchErr error
		batchPurgeRows := int64(0)

		// Calculate safe purge tso once at the first successful lock acquisition.
		if !safePurgeTSOReady {
			lockedLastPurgedTSO = lastPurgedTSO
			lockedLastPurgedTSOReady = hasLastPurgedTSO

			txn, err := purgeSctx.Txn(true)
			if err != nil {
				_, _ = sqlExec.ExecuteInternal(kctx, "ROLLBACK")
				return errors.Trace(err)
			}
			purgeStartTS := txn.StartTS()
			safePurgeTSO = purgeStartTS

			if !purgeHistRunningInserted {
				purgeJobID = purgeStartTS
				if err := insertMLogPurgeHistRunning(kctx, histSQLExec, purgeJobID, mlogID); err != nil {
					_, _ = sqlExec.ExecuteInternal(kctx, "ROLLBACK")
					return errors.Trace(err)
				}
				purgeHistRunningInserted = true
			}

			// Collect all dependent MV IDs (Public + in-building CREATE MATERIALIZED VIEW jobs).
			publicMVIDs, buildingMVIDs, collectErr := collectDependentMViewIDsForMLogPurge(kctx, sqlExec, baseTableMeta, mlogID)
			if collectErr != nil {
				batchErr = collectErr
			} else {
				// If there are no dependent MVs, it is safe to purge up to the start tso of this transaction.
				safePurgeTSO, batchErr = calcMaterializedViewLogSafePurgeTSO(
					kctx,
					sqlExec,
					schemaName.O,
					s.Table.Name.O,
					purgeStartTS,
					publicMVIDs,
					buildingMVIDs,
				)
			}
			safePurgeTSOReady = true
		}

		skipDeleteByCheckpoint := batchErr == nil && lockedLastPurgedTSOReady && lockedLastPurgedTSO >= safePurgeTSO
		if batchErr == nil && !skipDeleteByCheckpoint && safePurgeTSO > 0 {
			batchPurgeRows, batchErr = purgeMaterializedViewLogData(
				kctx,
				sqlExec,
				purgeSctx.GetSessionVars(),
				schemaName.O,
				mlogName.O,
				safePurgeTSO,
				batchSize,
			)
		}
		if batchErr == nil && batchPurgeRows < batchSize {
			nextTime, shouldUpdateNextTime, deriveErr := deriveRuntimeMaterializedScheduleNextTime(
				kctx,
				scheduleEvalSctx,
				purgeSctx,
				mlogInfo.PurgeStartWith,
				mlogInfo.PurgeNext,
				isInternalSQL,
			)
			if deriveErr != nil {
				batchErr = deriveErr
			} else {
				var lastPurgedTSOToPersist *uint64
				if !skipDeleteByCheckpoint {
					lastPurgedTSOToPersist = &safePurgeTSO
				}
				batchErr = updateMaterializedViewLogPurgeInfoOnSuccess(
					kctx,
					sqlExec,
					mlogID,
					lastPurgedTSOToPersist,
					nextTime,
					shouldUpdateNextTime,
				)
			}
		}
		if batchErr != nil {
			_, _ = sqlExec.ExecuteInternal(kctx, "ROLLBACK")
			return finalizeFailure(batchErr)
		}
		if _, err = sqlExec.ExecuteInternal(kctx, "COMMIT"); err != nil {
			return finalizeFailure(err)
		}

		totalPurgeRows += batchPurgeRows
		if batchPurgeRows < batchSize {
			if err := finalizeSuccess(); err != nil {
				return errors.Trace(err)
			}
			return nil
		}
	}
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
	// If there are no dependent MVs, it is safe to purge up to the start tso of this transaction.
	safePurgeTSO := purgeStartTS
	buildINList := func(ids []int64) string {
		var sb strings.Builder
		for i, id := range ids {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString(strconv.FormatInt(id, 10))
		}
		return sb.String()
	}

	publicIDs := make([]int64, 0, len(publicMVIDs))
	for mvID := range publicMVIDs {
		publicIDs = append(publicIDs, mvID)
	}
	if len(publicIDs) > 0 {
		// Public MVs should always have a refresh record. If not, treat it as metadata inconsistency and abort.
		countSQL := fmt.Sprintf(
			"SELECT COUNT(1) FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID IN (%s)",
			buildINList(publicIDs),
		)
		countRows, err := sqlexec.ExecSQL(kctx, sqlExec, countSQL)
		if err != nil {
			if infoschema.ErrTableNotExists.Equal(err) {
				return safePurgeTSO, errors.New("required system table mysql.tidb_mview_refresh_info does not exist")
			}
			return safePurgeTSO, errors.Trace(err)
		}

		var cnt int64
		if len(countRows) > 0 {
			cnt = countRows[0].GetInt64(0)
		}
		if cnt != int64(len(publicIDs)) {
			return safePurgeTSO, errors.Errorf(
				"materialized view refresh info is missing for some dependent materialized views on base table %s.%s (expected %d, got %d)",
				baseSchema,
				baseTable,
				len(publicIDs),
				cnt,
			)
		}
	}

	allMVIDs := make(map[int64]struct{}, len(publicMVIDs)+len(buildingMVIDs))
	for mvID := range publicMVIDs {
		allMVIDs[mvID] = struct{}{}
	}
	for mvID := range buildingMVIDs {
		allMVIDs[mvID] = struct{}{}
	}
	allIDs := make([]int64, 0, len(allMVIDs))
	for mvID := range allMVIDs {
		allIDs = append(allIDs, mvID)
	}
	if len(allIDs) > 0 {
		minSQL := fmt.Sprintf(
			"SELECT MIN(COALESCE(LAST_SUCCESS_READ_TSO, 0)) FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID IN (%s)",
			buildINList(allIDs),
		)
		minRows, err := sqlexec.ExecSQL(kctx, sqlExec, minSQL)
		if err != nil {
			if infoschema.ErrTableNotExists.Equal(err) {
				return safePurgeTSO, errors.New("required system table mysql.tidb_mview_refresh_info does not exist")
			}
			return safePurgeTSO, errors.Trace(err)
		}

		if len(minRows) > 0 && !minRows[0].IsNull(0) {
			v := minRows[0].GetInt64(0)
			if v <= 0 {
				safePurgeTSO = 0
			} else {
				safePurgeTSO = uint64(v)
				if safePurgeTSO > purgeStartTS {
					safePurgeTSO = purgeStartTS
				}
			}
		}
	}

	return safePurgeTSO, nil
}
func (e *PurgeMaterializedViewLogExec) resolvePurgeMaterializedViewLogMeta(
	s *ast.PurgeMaterializedViewLogStmt,
) (schemaName pmodel.CIStr, baseTableMeta *model.TableInfo, mlogName pmodel.CIStr, mlogID int64, mlogInfo *model.MaterializedViewLogInfo, _ error) {
	is := e.Ctx().GetDomainInfoSchema().(infoschema.InfoSchema)
	schemaName = s.Table.Schema
	if schemaName.O == "" {
		if e.Ctx().GetSessionVars().CurrentDB == "" {
			return schemaName, nil, mlogName, 0, nil, errors.Trace(plannererrors.ErrNoDB)
		}
		schemaName = pmodel.NewCIStr(e.Ctx().GetSessionVars().CurrentDB)
		s.Table.Schema = schemaName
	}
	if _, ok := is.SchemaByName(schemaName); !ok {
		return schemaName, nil, mlogName, 0, nil, infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schemaName)
	}
	baseTable, err := is.TableByName(context.Background(), schemaName, s.Table.Name)
	if err != nil {
		return schemaName, nil, mlogName, 0, nil, err
	}
	if baseTable.Meta().IsView() || baseTable.Meta().IsSequence() || baseTable.Meta().TempTableType != model.TempTableNone {
		return schemaName, nil, mlogName, 0, nil, dbterror.ErrWrongObject.GenWithStackByArgs(schemaName, s.Table.Name, "BASE TABLE")
	}
	baseTableMeta = baseTable.Meta()
	baseTableID := baseTableMeta.ID

	mlogName = pmodel.NewCIStr("$mlog$" + baseTableMeta.Name.O)
	mlogTable, err := is.TableByName(context.Background(), schemaName, mlogName)
	if err != nil {
		if infoschema.ErrTableNotExists.Equal(err) {
			return schemaName, baseTableMeta, mlogName, 0, nil, errors.Errorf(
				"materialized view log does not exist for base table %s.%s",
				schemaName.O,
				s.Table.Name.O,
			)
		}
		return schemaName, baseTableMeta, mlogName, 0, nil, err
	}
	if mlogTable.Meta().MaterializedViewLog == nil || mlogTable.Meta().MaterializedViewLog.BaseTableID != baseTableID {
		return schemaName, baseTableMeta, mlogName, 0, nil, errors.Errorf(
			"table %s.%s is not a materialized view log for base table %s.%s",
			schemaName.O,
			mlogName.O,
			schemaName.O,
			s.Table.Name.O,
		)
	}
	mlogID = mlogTable.Meta().ID
	mlogInfo = mlogTable.Meta().MaterializedViewLog

	return schemaName, baseTableMeta, mlogName, mlogID, mlogInfo, nil
}

func acquireMaterializedViewLogPurgeLock(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	schemaName pmodel.CIStr,
	baseTableName pmodel.CIStr,
	mlogID int64,
) (lastPurgedTSO uint64, hasLastPurgedTSO bool, _ error) {
	forceConflict := false
	failpoint.Inject("mockPurgeMaterializedViewLogLockConflict", func(val failpoint.Value) {
		if v, ok := val.(bool); ok && v {
			forceConflict = true
		}
	})
	if forceConflict {
		return 0, false, errors.Annotatef(
			errMLogPurgeLockConflict,
			"another purge is running for materialized view log on %s.%s, please retry later",
			schemaName.O,
			baseTableName.O,
		)
	}

	// Acquire the mutual exclusion lock row for this MLOG_ID. NOWAIT ensures we fail fast if another purge is running.
	lockSQL := sqlescape.MustEscapeSQL("SELECT LAST_PURGED_TSO FROM mysql.tidb_mlog_purge_info WHERE MLOG_ID = %? FOR UPDATE NOWAIT", mlogID)
	rows, err := sqlexec.ExecSQL(kctx, sqlExec, lockSQL)
	if err != nil {
		if storeerr.ErrLockAcquireFailAndNoWaitSet.Equal(err) {
			return 0, false, errors.Annotatef(
				errMLogPurgeLockConflict,
				"another purge is running for materialized view log on %s.%s, please retry later",
				schemaName.O,
				baseTableName.O,
			)
		}
		if infoschema.ErrTableNotExists.Equal(err) {
			return 0, false, errors.New("required system table mysql.tidb_mlog_purge_info does not exist")
		}
		return 0, false, errors.Trace(err)
	}
	if len(rows) == 0 {
		return 0, false, errors.Errorf("mlog purge lock row does not exist for mlog id %d", mlogID)
	}
	if rows[0].IsNull(0) {
		return 0, false, nil
	}
	v := rows[0].GetInt64(0)
	if v < 0 {
		return 0, false, errors.Errorf("invalid LAST_PURGED_TSO %d for mlog id %d", v, mlogID)
	}
	return uint64(v), true, nil
}

func isMLogPurgeLockConflict(err error) bool {
	return err != nil && errors.ErrorEqual(err, errMLogPurgeLockConflict)
}

func collectDependentMViewIDsForMLogPurge(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	baseTableMeta *model.TableInfo,
	mlogID int64,
) (publicMVIDs, buildingMVIDs map[int64]struct{}, _ error) {
	publicMVIDs = make(map[int64]struct{})
	if baseMeta := baseTableMeta.MaterializedViewBase; baseMeta != nil {
		for _, id := range baseMeta.MViewIDs {
			if id > 0 {
				publicMVIDs[id] = struct{}{}
			}
		}
	}

	buildingMVIDs = make(map[int64]struct{})
	jobSQL := sqlescape.MustEscapeSQL(
		"SELECT job_meta FROM mysql.tidb_ddl_job WHERE type = %? AND FIND_IN_SET(%?, table_ids)",
		model.ActionCreateMaterializedView,
		mlogID,
	)
	jobRows, err := sqlexec.ExecSQL(kctx, sqlExec, jobSQL)
	if err != nil {
		if infoschema.ErrTableNotExists.Equal(err) {
			return publicMVIDs, buildingMVIDs, errors.New("required system table mysql.tidb_ddl_job does not exist")
		}
		return publicMVIDs, buildingMVIDs, errors.Trace(err)
	}
	for _, row := range jobRows {
		jobBytes := row.GetBytes(0)
		if len(jobBytes) == 0 {
			continue
		}
		job := model.Job{}
		if err := job.Decode(jobBytes); err != nil {
			return publicMVIDs, buildingMVIDs, errors.Trace(err)
		}
		if job.TableID > 0 {
			// `MaterializedViewBase.MViewIDs` may already include the MV ID when the job enters later phases.
			// Prefer the semantics of Public MVs (missing refresh record blocks purge) for overlapped IDs.
			if _, ok := publicMVIDs[job.TableID]; !ok {
				buildingMVIDs[job.TableID] = struct{}{}
			}
		}
	}
	return publicMVIDs, buildingMVIDs, nil
}

func purgeMaterializedViewLogData(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	sessVars *variable.SessionVars,
	schemaName string,
	mlogName string,
	safePurgeTSO uint64,
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

	const mlogAlias = "mlog"
	deleteSQL := sqlescape.MustEscapeSQL(
		"DELETE /*+ read_from_storage(tiflash[%n]) */ FROM %n.%n AS %n WHERE _tidb_commit_ts <= %? LIMIT %?",
		mlogAlias,
		schemaName,
		mlogName,
		mlogAlias,
		safePurgeTSO,
		batchSize,
	)
	origInMaterializedViewMaintenance := sessVars.InMaterializedViewMaintenance
	sessVars.InMaterializedViewMaintenance = true
	defer func() {
		sessVars.InMaterializedViewMaintenance = origInMaterializedViewMaintenance
	}()

	_, err := sqlExec.ExecuteInternal(kctx, deleteSQL)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return int64(sessVars.StmtCtx.AffectedRows()), nil
}

func updateMaterializedViewLogPurgeInfoOnSuccess(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	mlogID int64,
	lastPurgedTSO *uint64,
	nextTime *string,
	shouldUpdateNextTime bool,
) error {
	setClauses := make([]string, 0, 2)
	args := make([]any, 0, 3)
	if lastPurgedTSO != nil {
		setClauses = append(setClauses, "LAST_PURGED_TSO = %?")
		args = append(args, *lastPurgedTSO)
	}
	if shouldUpdateNextTime {
		setClauses = append(setClauses, "NEXT_TIME = %?")
		var nextTimeArg any
		if nextTime != nil {
			nextTimeArg = *nextTime
		}
		args = append(args, nextTimeArg)
	}
	if len(setClauses) == 0 {
		return nil
	}

	updateSQL := fmt.Sprintf(
		`UPDATE mysql.tidb_mlog_purge_info
SET
	%s
WHERE MLOG_ID = %%?`,
		strings.Join(setClauses, ",\n\t"),
	)
	args = append(args, mlogID)
	_, err := sqlExec.ExecuteInternal(kctx, updateSQL, args...)
	if err != nil {
		if infoschema.ErrTableNotExists.Equal(err) {
			return errors.New("required system table mysql.tidb_mlog_purge_info does not exist")
		}
		return errors.Trace(err)
	}
	return nil
}

func insertMLogPurgeHistRunning(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	purgeJobID uint64,
	mlogID int64,
) error {
	insertSQL := `INSERT INTO mysql.tidb_mlog_purge_hist (
		PURGE_JOB_ID,
		MLOG_ID,
		PURGE_TIME,
		PURGE_ROWS,
		PURGE_STATUS
	) VALUES (
		%?,
		%?,
		NOW(6),
		%?,
		%?
	)`
	_, err := sqlExec.ExecuteInternal(kctx, insertSQL, purgeJobID, mlogID, int64(0), purgeHistStatusRunning)
	if err != nil {
		if infoschema.ErrTableNotExists.Equal(err) {
			return errors.New("required system table mysql.tidb_mlog_purge_hist does not exist")
		}
		return errors.Trace(err)
	}
	return nil
}

func finalizeMLogPurgeHist(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	purgeJobID uint64,
	purgeStatus string,
	purgeRows int64,
) error {
	updateSQL := `UPDATE mysql.tidb_mlog_purge_hist
	SET
		PURGE_ENDTIME = NOW(6),
		PURGE_ROWS = %?,
		PURGE_STATUS = %?
	WHERE PURGE_JOB_ID = %?`
	_, err := sqlExec.ExecuteInternal(kctx, updateSQL, purgeRows, purgeStatus, purgeJobID)
	failpoint.Inject("mockUpdateMaterializedViewLogPurgeStateErr", func(val failpoint.Value) {
		if val.(bool) {
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

func (e *RefreshMaterializedViewExec) executeRefreshMaterializedView(kctx context.Context, s *ast.RefreshMaterializedViewStmt) error {
	refreshMethod, err := validateRefreshMaterializedViewStmt(s)
	if err != nil {
		return err
	}
	isInternalSQL := e.Ctx().GetSessionVars().InRestrictedSQL
	finalizeCtx := context.WithoutCancel(kctx)

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
	restoreSessVars, err := initRefreshMaterializedViewSession(sessVars, tblInfo.MaterializedView)
	if err != nil {
		return err
	}
	defer restoreSessVars()
	failpoint.InjectCall("refreshMaterializedViewAfterInitSession", sessVars.SQLMode, sessVars.Location().String())

	var scheduleEvalSctx sessionctx.Context
	if isInternalSQL {
		scheduleEvalSctx, err = e.GetSysSession()
		if err != nil {
			return err
		}
		defer e.ReleaseSysSession(kctx, scheduleEvalSctx)
	}

	txnStarted := false
	txnFinished := false
	defer func() {
		if !txnStarted || txnFinished {
			return
		}
		_, _ = sqlExec.ExecuteInternal(finalizeCtx, "ROLLBACK")
	}()

	// Use a pessimistic txn to ensure `FOR UPDATE NOWAIT` works as a mutex.
	if _, err := sqlExec.ExecuteInternal(kctx, "BEGIN PESSIMISTIC"); err != nil {
		return errors.Trace(err)
	}
	txnStarted = true

	failpoint.InjectCall("refreshMaterializedViewAfterBegin")
	failpoint.Inject("pauseRefreshMaterializedViewAfterBegin", func() {})

	mviewID := tblInfo.ID
	lockedReadTSO, lockedReadTSONull, err := lockRefreshInfoRow(kctx, sqlExec, mviewID)
	if err != nil {
		return err
	}
	txn, err := refreshSctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	startTS := txn.StartTS()
	if startTS == 0 {
		return errors.New("refresh materialized view: invalid transaction start tso")
	}
	refreshJobID := startTS

	histSctx, err := e.GetSysSession()
	if err != nil {
		return err
	}
	defer e.ReleaseSysSession(kctx, histSctx)
	histSQLExec := histSctx.GetSQLExecutor()

	if err := insertRefreshHistRunning(kctx, histSQLExec, refreshJobID, mviewID, refreshMethod); err != nil {
		return err
	}

	finalizeFailure := func(refreshErr error) error {
		refreshErrMsg := refreshErr.Error()
		var rollbackErr error
		if !txnFinished {
			if _, err := sqlExec.ExecuteInternal(finalizeCtx, "ROLLBACK"); err != nil {
				rollbackErr = errors.Trace(err)
				refreshErrMsg = refreshErrMsg + "; rollback error: " + err.Error()
			}
			txnFinished = true
		}
		if histErr := finalizeRefreshHistWithRetry(
			finalizeCtx,
			histSQLExec,
			refreshJobID,
			mviewID,
			refreshHistStatusFailed,
			nil,
			&refreshErrMsg,
		); histErr != nil {
			if rollbackErr != nil {
				return errors.Annotatef(histErr, "refresh materialized view: rollback failed (%v) and failed to finalize refresh history after error %v", rollbackErr, refreshErr)
			}
			return errors.Annotatef(histErr, "refresh materialized view: failed to finalize refresh history after error %v", refreshErr)
		}
		if rollbackErr != nil {
			return errors.Annotatef(rollbackErr, "refresh materialized view: rollback failed after error %v", refreshErr)
		}
		return errors.Trace(refreshErr)
	}

	failpoint.InjectCall("refreshMaterializedViewAfterInsertRefreshHistRunning")
	failpoint.Inject("pauseRefreshMaterializedViewAfterInsertRefreshHistRunning", func() {})

	var lastSuccessfulRefreshReadTSO int64
	if s.Type == ast.RefreshMaterializedViewTypeFast {
		// LAST_SUCCESS_READ_TSO is BIGINT DEFAULT NULL. FAST refresh requires it to be non-NULL.
		if lockedReadTSONull {
			return finalizeFailure(errors.New("refresh materialized view fast: LAST_SUCCESS_READ_TSO is NULL"))
		}
		lastSuccessfulRefreshReadTSO = lockedReadTSO
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
		return finalizeFailure(err)
	}

	refreshReadTSO, err := getRefreshReadTSOForSuccess(sessVars)
	if err != nil {
		return finalizeFailure(err)
	}
	nextTime, shouldUpdateNextTime, err := deriveRuntimeMaterializedScheduleNextTime(
		kctx,
		scheduleEvalSctx,
		refreshSctx,
		tblInfo.MaterializedView.RefreshStartWith,
		tblInfo.MaterializedView.RefreshNext,
		isInternalSQL,
	)
	if err != nil {
		return finalizeFailure(err)
	}
	if err := persistRefreshSuccess(
		kctx,
		sqlExec,
		mviewID,
		lockedReadTSO,
		lockedReadTSONull,
		refreshReadTSO,
		nextTime,
		shouldUpdateNextTime,
	); err != nil {
		return finalizeFailure(err)
	}
	if _, err := sqlExec.ExecuteInternal(kctx, "COMMIT"); err != nil {
		return finalizeFailure(err)
	}
	txnFinished = true
	if err := finalizeRefreshHistWithRetry(
		finalizeCtx,
		histSQLExec,
		refreshJobID,
		mviewID,
		refreshHistStatusSuccess,
		&refreshReadTSO,
		nil,
	); err != nil {
		e.Ctx().GetSessionVars().StmtCtx.AppendWarning(
			errors.Annotate(err, "refresh materialized view: refresh committed but failed to finalize refresh history"),
		)
	}
	return nil
}

func initRefreshMaterializedViewSession(
	sessVars *variable.SessionVars,
	mviewInfo *model.MaterializedViewInfo,
) (func(), error) {
	if mviewInfo == nil {
		return nil, errors.New("refresh materialized view: invalid materialized view metadata")
	}
	timezone := mviewInfo.DefinitionTimeZone
	loc, err := timezone.GetLocation()
	if err != nil {
		return nil, errors.Annotate(err, "refresh materialized view: invalid definition timezone")
	}

	origSQLMode := sessVars.SQLMode
	origTimeZone := sessVars.TimeZone
	origStmtCtxTimeZone := sessVars.StmtCtx.TimeZone()
	origTypeFlags := sessVars.StmtCtx.TypeFlags()
	origErrLevels := sessVars.StmtCtx.ErrLevels()

	sessVars.SQLMode = mviewInfo.DefinitionSQLMode
	sessVars.SetStatusFlag(mysql.ServerStatusNoBackslashEscaped, sessVars.SQLMode.HasNoBackslashEscapesMode())
	sessVars.TimeZone = loc
	sessVars.StmtCtx.SetTimeZone(loc)
	sessVars.StmtCtx.SetTypeFlags(refreshTypeFlagsWithSQLMode(sessVars.SQLMode))
	sessVars.StmtCtx.SetErrLevels(refreshErrLevelsWithSQLMode(sessVars.SQLMode))

	return func() {
		sessVars.SQLMode = origSQLMode
		sessVars.SetStatusFlag(mysql.ServerStatusNoBackslashEscaped, origSQLMode.HasNoBackslashEscapesMode())
		sessVars.TimeZone = origTimeZone
		sessVars.StmtCtx.SetTimeZone(origStmtCtxTimeZone)
		sessVars.StmtCtx.SetTypeFlags(origTypeFlags)
		sessVars.StmtCtx.SetErrLevels(origErrLevels)
	}, nil
}

func refreshTypeFlagsWithSQLMode(mode mysql.SQLMode) types.Flags {
	return types.StrictFlags.
		WithTruncateAsWarning(!mode.HasStrictMode()).
		WithIgnoreInvalidDateErr(mode.HasAllowInvalidDatesMode()).
		WithIgnoreZeroInDate(!mode.HasStrictMode() || mode.HasAllowInvalidDatesMode()).
		WithCastTimeToYearThroughConcat(true)
}

func refreshErrLevelsWithSQLMode(mode mysql.SQLMode) errctx.LevelMap {
	return errctx.LevelMap{
		errctx.ErrGroupTruncate:  errctx.ResolveErrLevel(false, !mode.HasStrictMode()),
		errctx.ErrGroupBadNull:   errctx.ResolveErrLevel(false, !mode.HasStrictMode()),
		errctx.ErrGroupNoDefault: errctx.ResolveErrLevel(false, !mode.HasStrictMode()),
		errctx.ErrGroupDividedByZero: errctx.ResolveErrLevel(
			!mode.HasErrorForDivisionByZeroMode(),
			!mode.HasStrictMode(),
		),
	}
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
	is := e.Ctx().GetDomainInfoSchema().(infoschema.InfoSchema)
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

func lockRefreshInfoRow(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	mviewID int64,
) (lockedReadTSO int64, lockedReadTSONull bool, err error) {
	lockRS, err := sqlExec.ExecuteInternal(
		kctx,
		// Also select LAST_SUCCESS_READ_TSO so FAST refresh can reuse this mutex/metadata load path.
		"SELECT MVIEW_ID, LAST_SUCCESS_READ_TSO FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID = %? FOR UPDATE NOWAIT",
		mviewID,
	)
	if infoschema.ErrTableNotExists.Equal(err) {
		return 0, false, errors.New("refresh materialized view: required system table mysql.tidb_mview_refresh_info does not exist")
	}
	if err != nil {
		return 0, false, errors.Trace(err)
	}
	if lockRS == nil {
		return 0, false, errors.New("refresh materialized view: cannot lock mysql.tidb_mview_refresh_info row")
	}
	lockRows, drainErr := sqlexec.DrainRecordSet(kctx, lockRS, 1)
	closeErr := lockRS.Close()
	if drainErr != nil {
		return 0, false, errors.Trace(drainErr)
	}
	if closeErr != nil {
		return 0, false, errors.Trace(closeErr)
	}
	if len(lockRows) == 0 {
		return 0, false, errors.New("refresh materialized view: refresh info row missing in mysql.tidb_mview_refresh_info")
	}

	lockedRow := lockRows[0]
	lockedReadTSONull = lockedRow.IsNull(1)
	if !lockedReadTSONull {
		lockedReadTSO = lockedRow.GetInt64(1)
	}
	return lockedReadTSO, lockedReadTSONull, nil
}

func readRefreshInfoReadTSO(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	mviewID int64,
) (readTSO int64, readTSONull bool, err error) {
	recheckRS, err := sqlExec.ExecuteInternal(
		kctx,
		"SELECT LAST_SUCCESS_READ_TSO FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID = %?",
		mviewID,
	)
	if err != nil {
		if infoschema.ErrTableNotExists.Equal(err) {
			return 0, false, errors.New("refresh materialized view: required system table mysql.tidb_mview_refresh_info does not exist")
		}
		return 0, false, errors.Trace(err)
	}
	if recheckRS == nil {
		return 0, false, errors.New("refresh materialized view: cannot read mysql.tidb_mview_refresh_info row")
	}
	recheckRows, drainErr := sqlexec.DrainRecordSet(kctx, recheckRS, 1)
	closeErr := recheckRS.Close()
	if drainErr != nil {
		return 0, false, errors.Trace(drainErr)
	}
	if closeErr != nil {
		return 0, false, errors.Trace(closeErr)
	}
	if len(recheckRows) == 0 {
		return 0, false, errors.New("refresh materialized view: refresh info row missing in mysql.tidb_mview_refresh_info")
	}
	recheckRow := recheckRows[0]
	readTSONull = recheckRow.IsNull(0)
	if !readTSONull {
		readTSO = recheckRow.GetInt64(0)
	}
	return readTSO, readTSONull, nil
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

func getRefreshReadTSOForSuccess(sessVars *variable.SessionVars) (uint64, error) {
	// MV refresh executes in pessimistic txn and reads data at `for_update_ts`.
	// Persist this timestamp so refresh metadata is aligned with the data snapshot.
	refreshReadTSO := sessVars.TxnCtx.GetForUpdateTS()
	if refreshReadTSO == 0 {
		return 0, errors.New("refresh materialized view: invalid refresh read tso")
	}
	return refreshReadTSO, nil
}

func deriveRuntimeMaterializedScheduleNextTime(
	kctx context.Context,
	evalSctx sessionctx.Context,
	templateSctx sessionctx.Context,
	startExpr string,
	nextExpr string,
	isInternalSQL bool,
) (*string, bool, error) {
	if !isInternalSQL {
		return nil, false, nil
	}
	if evalSctx == nil || templateSctx == nil {
		return nil, false, errors.New("runtime materialized schedule eval session is unavailable")
	}
	startExpr = strings.TrimSpace(startExpr)
	nextExpr = strings.TrimSpace(nextExpr)

	if nextExpr != "" {
		nextAt, err := evalMaterializedScheduleExprToDatetimeUTC(kctx, evalSctx, templateSctx, nextExpr)
		if err != nil {
			return nil, true, err
		}
		if nextAt == nil {
			return nil, true, nil
		}
		nextAtStr := nextAt.String()
		return &nextAtStr, true, nil
	}
	if startExpr != "" {
		return nil, true, nil
	}
	return nil, false, nil
}

func evalMaterializedScheduleExprToDatetimeUTC(
	kctx context.Context,
	evalSctx sessionctx.Context,
	templateSctx sessionctx.Context,
	exprSQL string,
) (*types.Time, error) {
	sessVars := evalSctx.GetSessionVars()
	templateVars := templateSctx.GetSessionVars()
	origSQLMode := sessVars.SQLMode
	origTypeFlags := sessVars.StmtCtx.TypeFlags()
	origErrLevels := sessVars.StmtCtx.ErrLevels()
	origTimeZone := sessVars.TimeZone
	origStmtTimeZone := sessVars.StmtCtx.TimeZone()
	sessVars.SQLMode = templateVars.SQLMode
	sessVars.SetStatusFlag(mysql.ServerStatusNoBackslashEscaped, sessVars.SQLMode.HasNoBackslashEscapesMode())
	sessVars.StmtCtx.SetTypeFlags(templateVars.StmtCtx.TypeFlags())
	sessVars.StmtCtx.SetErrLevels(templateVars.StmtCtx.ErrLevels())
	sessVars.TimeZone = time.UTC
	sessVars.StmtCtx.SetTimeZone(time.UTC)
	defer func() {
		sessVars.SQLMode = origSQLMode
		sessVars.SetStatusFlag(mysql.ServerStatusNoBackslashEscaped, origSQLMode.HasNoBackslashEscapesMode())
		sessVars.StmtCtx.SetTypeFlags(origTypeFlags)
		sessVars.StmtCtx.SetErrLevels(origErrLevels)
		sessVars.TimeZone = origTimeZone
		if origStmtTimeZone != nil {
			sessVars.StmtCtx.SetTimeZone(origStmtTimeZone)
			return
		}
		sessVars.StmtCtx.SetTimeZone(sessVars.Location())
	}()

	exprNode, err := generatedexpr.ParseExpression(exprSQL)
	if err != nil {
		return nil, errors.Trace(err)
	}
	builtExpr, err := expression.BuildSimpleExpr(evalSctx.GetExprCtx(), exprNode)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Refresh statement timestamp before evaluating expressions that may contain NOW.
	if _, err := sqlexec.ExecSQL(kctx, evalSctx.GetSQLExecutor(), "SELECT NOW(6)"); err != nil {
		return nil, errors.Trace(err)
	}

	evalCtx := evalSctx.GetExprCtx().GetEvalCtx()
	v, err := builtExpr.Eval(evalCtx, chunk.Row{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	if v.IsNull() {
		return nil, nil
	}

	targetTp := types.NewFieldType(mysql.TypeDatetime)
	targetTp.SetDecimal(types.MaxFsp)
	datetimeV, err := v.ConvertTo(evalCtx.TypeCtx(), targetTp)
	if err != nil {
		return nil, errors.Trace(err)
	}
	t := datetimeV.GetMysqlTime()
	return &t, nil
}

func persistRefreshSuccess(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	mviewID int64,
	lockedReadTSO int64,
	lockedReadTSONull bool,
	refreshReadTSO uint64,
	nextTime *string,
	shouldUpdateNextTime bool,
) error {
	setClauses := []string{"LAST_SUCCESS_READ_TSO = %?"}
	args := []any{refreshReadTSO}
	if shouldUpdateNextTime {
		setClauses = append(setClauses, "NEXT_TIME = %?")
		var nextTimeArg any
		if nextTime != nil {
			nextTimeArg = *nextTime
		}
		args = append(args, nextTimeArg)
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
	persistedReadTSO, persistedReadTSONull, err := readRefreshInfoReadTSO(kctx, sqlExec, mviewID)
	if err != nil {
		return err
	}
	if persistedReadTSONull || persistedReadTSO < 0 || uint64(persistedReadTSO) != refreshReadTSO {
		return errors.New("refresh materialized view: inconsistent LAST_SUCCESS_READ_TSO after success update")
	}
	return nil
}

const (
	refreshHistStatusRunning = "running"
	refreshHistStatusSuccess = "success"
	refreshHistStatusFailed  = "failed"
)

func insertRefreshHistRunning(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	refreshJobID uint64,
	mviewID int64,
	refreshMethod string,
) error {
	insertSQL := `INSERT INTO mysql.tidb_mview_refresh_hist (
	REFRESH_JOB_ID,
	MVIEW_ID,
	REFRESH_METHOD,
	REFRESH_TIME,
	REFRESH_STATUS
) VALUES (
	%?,
	%?,
	%?,
	NOW(6),
	%?
)`
	if _, err := sqlExec.ExecuteInternal(kctx, insertSQL, refreshJobID, mviewID, refreshMethod, refreshHistStatusRunning); err != nil {
		if infoschema.ErrTableNotExists.Equal(err) {
			return errors.New("refresh materialized view: required system table mysql.tidb_mview_refresh_hist does not exist")
		}
		return errors.Trace(err)
	}
	return nil
}

func finalizeRefreshHistWithRetry(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	refreshJobID uint64,
	mviewID int64,
	refreshStatus string,
	refreshReadTSO *uint64,
	refreshFailedReason *string,
) error {
	firstErr := finalizeRefreshHist(
		kctx,
		sqlExec,
		refreshJobID,
		mviewID,
		refreshStatus,
		refreshReadTSO,
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
	var refreshFailedReasonArg any
	if refreshFailedReason != nil {
		refreshFailedReasonArg = *refreshFailedReason
	}
	updateSQL := `UPDATE mysql.tidb_mview_refresh_hist
SET
	REFRESH_ENDTIME = NOW(6),
	REFRESH_STATUS = %?,
	REFRESH_READ_TSO = %?,
	REFRESH_FAILED_REASON = %?
WHERE REFRESH_JOB_ID = %?
  AND MVIEW_ID = %?`
	if _, err := sqlExec.ExecuteInternal(
		kctx,
		updateSQL,
		refreshStatus,
		refreshReadTSOArg,
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
