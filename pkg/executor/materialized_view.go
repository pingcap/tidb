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
	"bytes"
	"context"
	"fmt"
	"math/bits"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	plannercorebase "github.com/pingcap/tidb/pkg/planner/core/base"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	storeerr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	plannererrors "github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

// RefreshMaterializedViewExec executes "REFRESH MATERIALIZED VIEW" as a utility-style statement.
type RefreshMaterializedViewExec struct {
	exec.BaseExecutor
	stmt                  *ast.RefreshMaterializedViewStmt
	stepObserver          mvRefreshStepObserver
	planFormatForObserver string
	done                  bool
}

// CancelMaterializedViewJobExec executes "CANCEL MATERIALIZED VIEW ... JOB" as a utility-style statement.
type CancelMaterializedViewJobExec struct {
	exec.BaseExecutor
	stmt *ast.CancelMaterializedViewJobStmt
	done bool
}

var errMLogPurgeLockConflict = errors.NewNoStackError("mlog purge lock conflict")
var errMVRefreshAdvisoryLockConflict = errors.NewNoStackError("materialized view refresh advisory lock conflict")
var errMVTaskCanceledManually = errors.NewNoStackError("materialized view task canceled manually")

const (
	purgeHistStatusRunning          = "running"
	purgeHistStatusSuccess          = "success"
	purgeHistStatusFailed           = "failed"
	mvRefreshAdvisoryLockTimeoutSec = int64(1)
	mvRefreshShadowTablePrefix      = "__mv_shadow_"
	mvRefreshImportIntoStoreName    = "TiKV"
	mvTaskCancelWatchPollInterval   = 5 * time.Second
)

// PurgeMaterializedViewLogExec executes "PURGE MATERIALIZED VIEW LOG" as a utility-style statement.
type PurgeMaterializedViewLogExec struct {
	exec.BaseExecutor
	stmt *ast.PurgeMaterializedViewLogStmt
	done bool
}

const (
	mvCompleteDeltaDiffOpInsert = int64(1)
	mvCompleteDeltaDiffOpDelete = int64(2)
	mvCompleteDeltaDiffOpUpdate = int64(3)
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
	return &mvTaskCancelController{
		ctx:    ctx,
		cancel: cancel,
	}
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

func startMVTaskCancelWatcher(
	taskCtx context.Context,
	getSysSession func() (sessionctx.Context, error),
	releaseWatchSession func(sessionctx.Context),
	taskCancelController *mvTaskCancelController,
	watchName string,
	poller mvTaskCancelPoller,
) (func(), error) {
	if taskCancelController == nil {
		return func() {}, errors.New("mv task cancel watcher: task cancel controller is nil")
	}

	watchSctx, err := getSysSession()
	if err != nil {
		return func() {}, err
	}

	watcherCtx, stopWatcher := context.WithCancel(taskCtx)
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		defer releaseWatchSession(watchSctx)

		sqlExec := watchSctx.GetSQLExecutor()
		ticker := time.NewTicker(getMVTaskCancelWatchPollInterval())
		defer ticker.Stop()

		for {
			requested, requester, err := poller(watcherCtx, sqlExec)
			failpoint.InjectCall("mvTaskCancelWatcherPolled", watchName)
			if err != nil {
				if watcherCtx.Err() != nil {
					return
				}
				logutil.BgLogger().Warn("materialized view task cancel watcher poll failed",
					zap.String("watch", watchName),
					zap.Error(err),
				)
			} else if requested {
				taskCancelController.requestManualCancelByRequester(requester)
				return
			}

			select {
			case <-watcherCtx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	return func() {
		stopWatcher()
		<-watcherDone
	}, nil
}

func getMVTaskCancelWatchPollInterval() time.Duration {
	interval := mvTaskCancelWatchPollInterval
	failpoint.Inject("mockMVTaskCancelWatchPollInterval", func(val failpoint.Value) {
		switch v := val.(type) {
		case int:
			interval = time.Duration(v) * time.Millisecond
		case int64:
			interval = time.Duration(v) * time.Millisecond
		}
	})
	return interval
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
		`SELECT CANCEL_REQUESTED_AT, CANCEL_REQUESTED_BY
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

func readPurgeHistCancelRequest(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	purgeJobID uint64,
	mlogID int64,
) (bool, string, error) {
	rows, err := sqlexec.ExecSQL(
		kctx,
		sqlExec,
		`SELECT CANCEL_REQUESTED_AT, CANCEL_REQUESTED_BY
FROM mysql.tidb_mlog_purge_hist
WHERE PURGE_JOB_ID = %?
  AND MLOG_ID = %?`,
		purgeJobID,
		mlogID,
	)
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

func requestRefreshHistCancel(
	kctx context.Context,
	sctx sessionctx.Context,
	refreshJobID uint64,
	requester any,
) (bool, error) {
	_, err := sctx.GetSQLExecutor().ExecuteInternal(
		kctx,
		`UPDATE mysql.tidb_mview_refresh_hist
SET CANCEL_REQUESTED_AT = NOW(6),
	CANCEL_REQUESTED_BY = %?
WHERE REFRESH_JOB_ID = %?
  AND REFRESH_STATUS = 'running'
  AND CANCEL_REQUESTED_AT IS NULL`,
		requester,
		refreshJobID,
	)
	if err != nil {
		return false, errors.Trace(err)
	}
	return sctx.GetSessionVars().StmtCtx.AffectedRows() > 0, nil
}

func requestPurgeHistCancel(
	kctx context.Context,
	sctx sessionctx.Context,
	purgeJobID uint64,
	requester any,
) (bool, error) {
	_, err := sctx.GetSQLExecutor().ExecuteInternal(
		kctx,
		`UPDATE mysql.tidb_mlog_purge_hist
SET CANCEL_REQUESTED_AT = NOW(6),
	CANCEL_REQUESTED_BY = %?
WHERE PURGE_JOB_ID = %?
  AND PURGE_STATUS = 'running'
  AND CANCEL_REQUESTED_AT IS NULL`,
		requester,
		purgeJobID,
	)
	if err != nil {
		return false, errors.Trace(err)
	}
	return sctx.GetSessionVars().StmtCtx.AffectedRows() > 0, nil
}

func checkCancelMaterializedViewJobPrivilege(
	kctx context.Context,
	ctx sessionctx.Context,
	sqlExec sqlexec.SQLExecutor,
	stmt *ast.CancelMaterializedViewJobStmt,
) error {
	if stmt == nil {
		return errors.New("cancel materialized view job: missing statement")
	}
	pm := privilege.GetPrivilegeManager(ctx)
	user := ctx.GetSessionVars().User
	if pm == nil || user == nil {
		return nil
	}

	is := domain.GetDomain(ctx).InfoSchema()
	var dbName string
	var tableName string
	var found bool
	var err error
	switch stmt.Tp {
	case ast.CancelMaterializedViewJobTypeRefresh:
		dbName, tableName, found, err = resolveCancelRefreshJobPrivilegeTarget(kctx, sqlExec, is, uint64(stmt.JobID))
	case ast.CancelMaterializedViewJobTypeLogPurge:
		dbName, tableName, found, err = resolveCancelPurgeJobPrivilegeTarget(kctx, sqlExec, is, uint64(stmt.JobID))
	default:
		return errors.Errorf("invalid materialized view job cancel type: %d", stmt.Tp)
	}
	if err != nil || !found {
		return err
	}
	if pm.RequestVerification(ctx.GetSessionVars().ActiveRoles, dbName, tableName, "", mysql.AlterPriv) {
		return nil
	}
	return plannererrors.ErrTableaccessDenied.GenWithStackByArgs("ALTER", user.AuthUsername, user.AuthHostname, tableName)
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
	mvTable, ok := is.TableByID(context.Background(), mviewID)
	if !ok {
		return "", "", false, errors.Errorf("refresh materialized view: cannot resolve target materialized view %d for cancel job %d", mviewID, refreshJobID)
	}
	dbInfo, ok := infoschema.SchemaByTable(is, mvTable.Meta())
	if !ok {
		return "", "", false, errors.Errorf("refresh materialized view: cannot resolve schema for materialized view %d", mviewID)
	}
	return dbInfo.Name.L, mvTable.Meta().Name.L, true, nil
}

func resolveCancelPurgeJobPrivilegeTarget(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	is infoschema.InfoSchema,
	purgeJobID uint64,
) (dbName string, tableName string, found bool, err error) {
	rows, err := sqlexec.ExecSQL(
		kctx,
		sqlExec,
		`SELECT MLOG_ID
FROM mysql.tidb_mlog_purge_hist
WHERE PURGE_JOB_ID = %?
  AND PURGE_STATUS = 'running'`,
		purgeJobID,
	)
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
	mlogInfo := mlogTable.Meta().MaterializedViewLog
	if mlogInfo == nil {
		return "", "", false, errors.Errorf("table %d is not a materialized view log", mlogID)
	}
	baseTable, ok := is.TableByID(context.Background(), mlogInfo.BaseTableID)
	if !ok {
		return "", "", false, errors.Errorf("cannot resolve base table %d for materialized view log %d", mlogInfo.BaseTableID, mlogID)
	}
	dbInfo, ok := infoschema.SchemaByTable(is, baseTable.Meta())
	if !ok {
		return "", "", false, errors.Errorf("cannot resolve schema for base table %d", mlogInfo.BaseTableID)
	}
	return dbInfo.Name.L, baseTable.Meta().Name.L, true, nil
}

// MVCompleteDeltaApplyExec applies COMPLETE DELTA APPLY diff rows to the target MV table.
// It keeps the runtime single-threaded and only batches the UPDATE old/new comparison at chunk granularity.
type MVCompleteDeltaApplyExec struct {
	exec.BaseExecutor

	TargetTable      table.Table
	TargetHandleCols plannerutil.HandleCols
	OpColID          int

	MWritableInputColIDs []int
	QWritableInputColIDs []int

	CompareWritableIdxes []int
	MCompareInputColIDs  []int
	QCompareInputColIDs  []int

	writableFieldTypes []*types.FieldType
	compareColumns     []mvCompleteDeltaCompareColumn
	oldRow             []types.Datum
	newRow             []types.Datum
	touched            []bool
	// currTouchedIdxes caches writable-column indexes touched by the current UPDATE row.
	// It lets us clear only previously-set bits in `touched` and patch only changed columns in `newRow`.
	currTouchedIdxes        []int
	updateTouchedSingleByte bool

	childChunk          *chunk.Chunk
	updateRows          []int
	updateTouchedBitmap []uint8
	updateTouchedStride int
	executed            bool
	runtimeStats        *mvCompleteDeltaApplyRuntimeStats
}

type mvCompleteDeltaCompareColumn struct {
	writableIdx      int
	mInputColID      int
	qInputColID      int
	fieldType        *types.FieldType
	notNull          bool
	touchedBitMask   uint8
	touchedByteIndex int
}

type mvCompleteDeltaApplyWriterStats struct {
	chunks int64
	rowOps int64

	insertRows int64
	updateRows int64
	deleteRows int64
}

func (s *mvCompleteDeltaApplyWriterStats) merge(other mvCompleteDeltaApplyWriterStats) {
	s.chunks += other.chunks
	s.rowOps += other.rowOps
	s.insertRows += other.insertRows
	s.updateRows += other.updateRows
	s.deleteRows += other.deleteRows
}

type mvCompleteDeltaApplyRuntimeStats struct {
	writerTime   time.Duration
	writerDetail mvCompleteDeltaApplyWriterStats
}

func (s *mvCompleteDeltaApplyRuntimeStats) reset() {
	if s == nil {
		return
	}
	s.writerTime = 0
	s.writerDetail = mvCompleteDeltaApplyWriterStats{}
}

func (s *mvCompleteDeltaApplyRuntimeStats) String() string {
	if s == nil {
		return ""
	}
	var buf bytes.Buffer
	buf.WriteString("mv_complete_delta_apply:{writer:{time:")
	buf.WriteString(execdetails.FormatDuration(s.writerTime))
	buf.WriteString(", chunks:")
	buf.WriteString(strconv.FormatInt(s.writerDetail.chunks, 10))
	buf.WriteString(", row_ops:")
	buf.WriteString(strconv.FormatInt(s.writerDetail.rowOps, 10))
	buf.WriteString(", rows:{insert:")
	buf.WriteString(strconv.FormatInt(s.writerDetail.insertRows, 10))
	buf.WriteString(", update:")
	buf.WriteString(strconv.FormatInt(s.writerDetail.updateRows, 10))
	buf.WriteString(", delete:")
	buf.WriteString(strconv.FormatInt(s.writerDetail.deleteRows, 10))
	buf.WriteString("}}}")
	return buf.String()
}

func (s *mvCompleteDeltaApplyRuntimeStats) Clone() execdetails.RuntimeStats {
	if s == nil {
		return &mvCompleteDeltaApplyRuntimeStats{}
	}
	return &mvCompleteDeltaApplyRuntimeStats{
		writerTime:   s.writerTime,
		writerDetail: s.writerDetail,
	}
}

func (s *mvCompleteDeltaApplyRuntimeStats) Merge(other execdetails.RuntimeStats) {
	tmp, ok := other.(*mvCompleteDeltaApplyRuntimeStats)
	if !ok || tmp == nil {
		return
	}
	s.writerTime += tmp.writerTime
	s.writerDetail.merge(tmp.writerDetail)
}

func (*mvCompleteDeltaApplyRuntimeStats) Tp() int {
	return execdetails.TpMVCompleteDeltaApplyRuntimeStats
}

// Open implements the Executor interface.
func (e *MVCompleteDeltaApplyExec) Open(ctx context.Context) error {
	e.executed = false
	e.childChunk = nil
	e.updateRows = e.updateRows[:0]
	e.updateTouchedBitmap = e.updateTouchedBitmap[:0]
	e.updateTouchedStride = 0
	e.updateTouchedSingleByte = false
	e.currTouchedIdxes = e.currTouchedIdxes[:0]
	clear(e.touched)

	if err := e.BaseExecutor.Open(ctx); err != nil {
		return err
	}

	if e.TargetTable == nil {
		return errors.New("MVCompleteDeltaApply target table is nil")
	}
	if e.TargetHandleCols == nil {
		return errors.New("MVCompleteDeltaApply target handle cols is nil")
	}
	child := e.Children(0)
	if child == nil {
		return errors.New("MVCompleteDeltaApply child executor is nil")
	}

	writableCols := e.TargetTable.WritableCols()
	e.writableFieldTypes = make([]*types.FieldType, len(writableCols))
	for i := range writableCols {
		e.writableFieldTypes[i] = &writableCols[i].FieldType
	}
	e.oldRow = make([]types.Datum, len(writableCols))
	e.newRow = make([]types.Datum, len(writableCols))
	e.touched = make([]bool, len(writableCols))
	if err := e.initCompareColumns(len(child.RetFieldTypes())); err != nil {
		return err
	}
	e.currTouchedIdxes = make([]int, 0, len(e.compareColumns))
	e.updateTouchedStride = (len(e.compareColumns) + 7) >> 3
	e.updateTouchedSingleByte = e.updateTouchedStride == 1
	e.childChunk = exec.NewFirstChunk(child)
	return nil
}

// Next implements the Executor interface.
func (e *MVCompleteDeltaApplyExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.MaxChunkSize())
	if e.executed {
		return nil
	}
	e.executed = true
	if e.BaseExecutor.RuntimeStats() != nil {
		if e.runtimeStats == nil {
			e.runtimeStats = &mvCompleteDeltaApplyRuntimeStats{}
		} else {
			e.runtimeStats.reset()
		}
		defer e.Ctx().GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.ID(), e.runtimeStats)
	}

	child := e.Children(0)
	if child == nil {
		return errors.New("MVCompleteDeltaApply child executor is nil")
	}
	txn, err := e.Ctx().Txn(true)
	if err != nil {
		return err
	}
	tableCtx := e.Ctx().GetTableCtx()
	stmtCtx := e.Ctx().GetSessionVars().StmtCtx
	insertSizeHintStep := int(e.Ctx().GetSessionVars().ShardAllocateStep)
	if insertSizeHintStep <= 0 {
		insertSizeHintStep = 1
	}

	for {
		e.childChunk.Reset()
		if err := exec.Next(ctx, child, e.childChunk); err != nil {
			return err
		}
		if e.childChunk.NumRows() == 0 {
			return nil
		}
		writeStart := time.Time{}
		if e.runtimeStats != nil {
			writeStart = time.Now()
		}
		if err := e.applyChunk(txn, tableCtx, stmtCtx, insertSizeHintStep, e.childChunk); err != nil {
			return err
		}
		if e.runtimeStats != nil {
			e.runtimeStats.writerTime += time.Since(writeStart)
		}
	}
}

// Close implements the Executor interface.
func (e *MVCompleteDeltaApplyExec) Close() error {
	e.writableFieldTypes = nil
	e.compareColumns = nil
	e.oldRow = nil
	e.newRow = nil
	e.touched = nil
	e.currTouchedIdxes = nil
	e.childChunk = nil
	e.updateRows = nil
	e.updateTouchedBitmap = nil
	e.updateTouchedStride = 0
	e.updateTouchedSingleByte = false
	e.executed = false
	e.runtimeStats = nil
	return e.BaseExecutor.Close()
}

func (e *MVCompleteDeltaApplyExec) applyChunk(
	txn kv.Transaction,
	tableCtx table.MutateContext,
	stmtCtx *stmtctx.StatementContext,
	insertSizeHintStep int,
	input *chunk.Chunk,
) error {
	ops := input.Column(e.OpColID).Int64s()[:input.NumRows()]
	insertRemain, err := e.collectChunkUpdateRows(ops)
	if err != nil {
		return err
	}
	if err := e.markChunkUpdateTouchedColumns(input); err != nil {
		return err
	}
	var writerStats *mvCompleteDeltaApplyWriterStats
	var writerStatsDelta mvCompleteDeltaApplyWriterStats
	if e.runtimeStats != nil {
		writerStats = &e.runtimeStats.writerDetail
		writerStatsDelta.chunks = 1
		writerStatsDelta.rowOps = int64(input.NumRows())
		defer func() {
			writerStats.merge(writerStatsDelta)
		}()
	}

	insertOrdinal := 0
	updateOrdinal := 0
	for rowIdx := 0; rowIdx < input.NumRows(); rowIdx++ {
		row := input.GetRow(rowIdx)
		op := ops[rowIdx]
		switch op {
		case mvCompleteDeltaDiffOpInsert:
			writerStatsDelta.insertRows++
			e.buildInsertRow(row)

			sizeHint := 0
			if insertOrdinal%insertSizeHintStep == 0 {
				sizeHint = min(insertSizeHintStep, insertRemain)
			}
			insertOrdinal++
			insertRemain--
			if sizeHint > 0 {
				_, err = e.TargetTable.AddRecord(
					tableCtx,
					txn,
					e.newRow,
					table.WithReserveAutoIDHint(sizeHint),
					table.DupKeyCheckLazy,
				)
			} else {
				_, err = e.TargetTable.AddRecord(tableCtx, txn, e.newRow, table.DupKeyCheckLazy)
			}
			if err != nil {
				return err
			}
		case mvCompleteDeltaDiffOpDelete:
			writerStatsDelta.deleteRows++
			e.buildDeleteRow(row)
			handle, err := e.TargetHandleCols.BuildHandle(stmtCtx, row)
			if err != nil {
				return err
			}
			if err := e.TargetTable.RemoveRecord(tableCtx, txn, handle, e.oldRow); err != nil {
				return err
			}
		case mvCompleteDeltaDiffOpUpdate:
			changed, err := e.buildTouchedFromBitmap(updateOrdinal)
			if err != nil {
				return err
			}
			if changed {
				writerStatsDelta.updateRows++
				e.buildUpdateRows(row)
				handle, err := e.TargetHandleCols.BuildHandle(stmtCtx, row)
				if err != nil {
					return err
				}
				if err := e.TargetTable.UpdateRecord(tableCtx, txn, handle, e.oldRow, e.newRow, e.touched); err != nil {
					return err
				}
			}
			updateOrdinal++
		default:
			return errors.Errorf("MVCompleteDeltaApply invalid diff op %d at row %d", op, rowIdx)
		}
	}
	return nil
}

func (e *MVCompleteDeltaApplyExec) collectChunkUpdateRows(ops []int64) (int, error) {
	if cap(e.updateRows) >= len(ops) {
		e.updateRows = e.updateRows[:0]
	} else {
		e.updateRows = make([]int, 0, len(ops))
	}
	insertRemain := 0
	for rowIdx, op := range ops {
		switch op {
		case mvCompleteDeltaDiffOpInsert:
			insertRemain++
		case mvCompleteDeltaDiffOpDelete:
		case mvCompleteDeltaDiffOpUpdate:
			e.updateRows = append(e.updateRows, rowIdx)
		default:
			return 0, errors.Errorf("MVCompleteDeltaApply invalid diff op %d at row %d", op, rowIdx)
		}
	}
	return insertRemain, nil
}

func (e *MVCompleteDeltaApplyExec) initCompareColumns(inputColCount int) error {
	if len(e.MCompareInputColIDs) != len(e.CompareWritableIdxes) || len(e.QCompareInputColIDs) != len(e.CompareWritableIdxes) {
		return errors.Errorf(
			"MVCompleteDeltaApply compare mapping length mismatch (compare=%d, M=%d, Q=%d)",
			len(e.CompareWritableIdxes),
			len(e.MCompareInputColIDs),
			len(e.QCompareInputColIDs),
		)
	}
	if cap(e.compareColumns) >= len(e.CompareWritableIdxes) {
		e.compareColumns = e.compareColumns[:len(e.CompareWritableIdxes)]
	} else {
		e.compareColumns = make([]mvCompleteDeltaCompareColumn, len(e.CompareWritableIdxes))
	}
	for compareIdx, writableIdx := range e.CompareWritableIdxes {
		if writableIdx < 0 || writableIdx >= len(e.writableFieldTypes) {
			return errors.Errorf(
				"MVCompleteDeltaApply writable compare index %d out of field type range [0,%d)",
				writableIdx,
				len(e.writableFieldTypes),
			)
		}
		mInputColID := e.MCompareInputColIDs[compareIdx]
		if mInputColID < 0 || mInputColID >= inputColCount {
			return errors.Errorf(
				"MVCompleteDeltaApply M compare input col id %d out of source range [0,%d)",
				mInputColID,
				inputColCount,
			)
		}
		qInputColID := e.QCompareInputColIDs[compareIdx]
		if qInputColID < 0 || qInputColID >= inputColCount {
			return errors.Errorf(
				"MVCompleteDeltaApply Q compare input col id %d out of source range [0,%d)",
				qInputColID,
				inputColCount,
			)
		}
		fieldType := e.writableFieldTypes[writableIdx]
		e.compareColumns[compareIdx] = mvCompleteDeltaCompareColumn{
			writableIdx:      writableIdx,
			mInputColID:      mInputColID,
			qInputColID:      qInputColID,
			fieldType:        fieldType,
			notNull:          mysql.HasNotNullFlag(fieldType.GetFlag()),
			touchedBitMask:   uint8(1 << (compareIdx & 7)),
			touchedByteIndex: compareIdx >> 3,
		}
	}
	return nil
}

func (e *MVCompleteDeltaApplyExec) markChunkUpdateTouchedColumns(input *chunk.Chunk) error {
	updateCnt := len(e.updateRows)
	if updateCnt == 0 || e.updateTouchedStride == 0 {
		e.updateTouchedBitmap = e.updateTouchedBitmap[:0]
		return nil
	}

	requiredLen := updateCnt * e.updateTouchedStride
	if cap(e.updateTouchedBitmap) < requiredLen {
		e.updateTouchedBitmap = make([]uint8, requiredLen)
	} else {
		e.updateTouchedBitmap = e.updateTouchedBitmap[:requiredLen]
		clear(e.updateTouchedBitmap)
	}

	for _, compareCol := range e.compareColumns {
		if err := markMVCompleteDeltaTouchedRowsByColumn(
			e.updateRows,
			e.updateTouchedBitmap,
			e.updateTouchedStride,
			e.updateTouchedSingleByte,
			compareCol,
			input.Column(compareCol.mInputColID),
			input.Column(compareCol.qInputColID),
		); err != nil {
			return err
		}
	}
	return nil
}

func (e *MVCompleteDeltaApplyExec) buildDeleteRow(row chunk.Row) {
	for writableIdx, colID := range e.MWritableInputColIDs {
		row.DatumWithBuffer(colID, e.writableFieldTypes[writableIdx], &e.oldRow[writableIdx])
	}
}

func (e *MVCompleteDeltaApplyExec) buildInsertRow(row chunk.Row) {
	for writableIdx, colID := range e.QWritableInputColIDs {
		row.DatumWithBuffer(colID, e.writableFieldTypes[writableIdx], &e.newRow[writableIdx])
	}
}

func (e *MVCompleteDeltaApplyExec) buildUpdateRows(row chunk.Row) {
	for writableIdx, colID := range e.MWritableInputColIDs {
		row.DatumWithBuffer(colID, e.writableFieldTypes[writableIdx], &e.oldRow[writableIdx])
	}
	copy(e.newRow, e.oldRow)
	// `newRow` starts from the old row image and only patches columns marked touched for this UPDATE row.
	for _, writableIdx := range e.currTouchedIdxes {
		row.DatumWithBuffer(e.QWritableInputColIDs[writableIdx], e.writableFieldTypes[writableIdx], &e.newRow[writableIdx])
	}
}

func (e *MVCompleteDeltaApplyExec) buildTouchedFromBitmap(updateOrdinal int) (bool, error) {
	if e.updateTouchedStride == 0 {
		return false, nil
	}
	for _, idx := range e.currTouchedIdxes {
		e.touched[idx] = false
	}
	e.currTouchedIdxes = e.currTouchedIdxes[:0]

	offset := updateOrdinal * e.updateTouchedStride
	rowBits := e.updateTouchedBitmap[offset : offset+e.updateTouchedStride]
	changed := false
	for byteIdx, b := range rowBits {
		for b != 0 {
			bitInByte := bits.TrailingZeros8(b)
			bitPos := (byteIdx << 3) + bitInByte
			writableIdx := e.compareColumns[bitPos].writableIdx
			e.touched[writableIdx] = true
			e.currTouchedIdxes = append(e.currTouchedIdxes, writableIdx)
			changed = true
			b &= b - 1
		}
	}
	return changed, nil
}

func markMVCompleteDeltaTouchedRowsByColumn(
	updateRows []int,
	updateTouchedBitmap []uint8,
	updateTouchedStride int,
	updateTouchedSingleByte bool,
	compareCol mvCompleteDeltaCompareColumn,
	oldCol *chunk.Column,
	newCol *chunk.Column,
) error {
	setTouched := func(updateOrdinal int) {
		if updateTouchedSingleByte {
			updateTouchedBitmap[updateOrdinal] |= compareCol.touchedBitMask
			return
		}
		updateTouchedBitmap[updateOrdinal*updateTouchedStride+compareCol.touchedByteIndex] |= compareCol.touchedBitMask
	}

	switch compareCol.fieldType.EvalType() {
	case types.ETInt:
		if mysql.HasUnsignedFlag(compareCol.fieldType.GetFlag()) {
			oldVals := oldCol.Uint64s()
			newVals := newCol.Uint64s()
			if compareCol.notNull {
				for updateOrdinal, rowIdx := range updateRows {
					if oldVals[rowIdx] != newVals[rowIdx] {
						setTouched(updateOrdinal)
					}
				}
				return nil
			}
			for updateOrdinal, rowIdx := range updateRows {
				oldIsNull := oldCol.IsNull(rowIdx)
				newIsNull := newCol.IsNull(rowIdx)
				if oldIsNull || newIsNull {
					if oldIsNull != newIsNull {
						setTouched(updateOrdinal)
					}
					continue
				}
				if oldVals[rowIdx] != newVals[rowIdx] {
					setTouched(updateOrdinal)
				}
			}
			return nil
		}
		oldVals := oldCol.Int64s()
		newVals := newCol.Int64s()
		if compareCol.notNull {
			for updateOrdinal, rowIdx := range updateRows {
				if oldVals[rowIdx] != newVals[rowIdx] {
					setTouched(updateOrdinal)
				}
			}
			return nil
		}
		for updateOrdinal, rowIdx := range updateRows {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					setTouched(updateOrdinal)
				}
				continue
			}
			if oldVals[rowIdx] != newVals[rowIdx] {
				setTouched(updateOrdinal)
			}
		}
		return nil
	case types.ETReal:
		if compareCol.fieldType.GetType() == mysql.TypeFloat {
			oldVals := oldCol.Float32s()
			newVals := newCol.Float32s()
			if compareCol.notNull {
				for updateOrdinal, rowIdx := range updateRows {
					if oldVals[rowIdx] != newVals[rowIdx] {
						setTouched(updateOrdinal)
					}
				}
				return nil
			}
			for updateOrdinal, rowIdx := range updateRows {
				oldIsNull := oldCol.IsNull(rowIdx)
				newIsNull := newCol.IsNull(rowIdx)
				if oldIsNull || newIsNull {
					if oldIsNull != newIsNull {
						setTouched(updateOrdinal)
					}
					continue
				}
				if oldVals[rowIdx] != newVals[rowIdx] {
					setTouched(updateOrdinal)
				}
			}
			return nil
		}
		oldVals := oldCol.Float64s()
		newVals := newCol.Float64s()
		if compareCol.notNull {
			for updateOrdinal, rowIdx := range updateRows {
				if oldVals[rowIdx] != newVals[rowIdx] {
					setTouched(updateOrdinal)
				}
			}
			return nil
		}
		for updateOrdinal, rowIdx := range updateRows {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					setTouched(updateOrdinal)
				}
				continue
			}
			if oldVals[rowIdx] != newVals[rowIdx] {
				setTouched(updateOrdinal)
			}
		}
		return nil
	case types.ETDecimal:
		oldVals := oldCol.Decimals()
		newVals := newCol.Decimals()
		if compareCol.notNull {
			for updateOrdinal, rowIdx := range updateRows {
				if oldVals[rowIdx].Compare(&newVals[rowIdx]) != 0 {
					setTouched(updateOrdinal)
				}
			}
			return nil
		}
		for updateOrdinal, rowIdx := range updateRows {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					setTouched(updateOrdinal)
				}
				continue
			}
			if oldVals[rowIdx].Compare(&newVals[rowIdx]) != 0 {
				setTouched(updateOrdinal)
			}
		}
		return nil
	case types.ETString:
		binaryCollator := collate.GetBinaryCollator()
		switch compareCol.fieldType.GetType() {
		case mysql.TypeEnum:
			if compareCol.notNull {
				for updateOrdinal, rowIdx := range updateRows {
					if binaryCollator.Compare(oldCol.GetEnum(rowIdx).Name, newCol.GetEnum(rowIdx).Name) != 0 {
						setTouched(updateOrdinal)
					}
				}
				return nil
			}
			for updateOrdinal, rowIdx := range updateRows {
				oldIsNull := oldCol.IsNull(rowIdx)
				newIsNull := newCol.IsNull(rowIdx)
				if oldIsNull || newIsNull {
					if oldIsNull != newIsNull {
						setTouched(updateOrdinal)
					}
					continue
				}
				if binaryCollator.Compare(oldCol.GetEnum(rowIdx).Name, newCol.GetEnum(rowIdx).Name) != 0 {
					setTouched(updateOrdinal)
				}
			}
			return nil
		case mysql.TypeSet:
			if compareCol.notNull {
				for updateOrdinal, rowIdx := range updateRows {
					if binaryCollator.Compare(oldCol.GetSet(rowIdx).Name, newCol.GetSet(rowIdx).Name) != 0 {
						setTouched(updateOrdinal)
					}
				}
				return nil
			}
			for updateOrdinal, rowIdx := range updateRows {
				oldIsNull := oldCol.IsNull(rowIdx)
				newIsNull := newCol.IsNull(rowIdx)
				if oldIsNull || newIsNull {
					if oldIsNull != newIsNull {
						setTouched(updateOrdinal)
					}
					continue
				}
				if binaryCollator.Compare(oldCol.GetSet(rowIdx).Name, newCol.GetSet(rowIdx).Name) != 0 {
					setTouched(updateOrdinal)
				}
			}
			return nil
		}
		if compareCol.notNull {
			for updateOrdinal, rowIdx := range updateRows {
				if binaryCollator.Compare(oldCol.GetString(rowIdx), newCol.GetString(rowIdx)) != 0 {
					setTouched(updateOrdinal)
				}
			}
			return nil
		}
		for updateOrdinal, rowIdx := range updateRows {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					setTouched(updateOrdinal)
				}
				continue
			}
			if binaryCollator.Compare(oldCol.GetString(rowIdx), newCol.GetString(rowIdx)) != 0 {
				setTouched(updateOrdinal)
			}
		}
		return nil
	case types.ETDatetime, types.ETTimestamp:
		oldVals := oldCol.Times()
		newVals := newCol.Times()
		if compareCol.notNull {
			for updateOrdinal, rowIdx := range updateRows {
				if oldVals[rowIdx] != newVals[rowIdx] {
					setTouched(updateOrdinal)
				}
			}
			return nil
		}
		for updateOrdinal, rowIdx := range updateRows {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					setTouched(updateOrdinal)
				}
				continue
			}
			if oldVals[rowIdx] != newVals[rowIdx] {
				setTouched(updateOrdinal)
			}
		}
		return nil
	case types.ETDuration:
		oldVals := oldCol.GoDurations()
		newVals := newCol.GoDurations()
		if compareCol.notNull {
			for updateOrdinal, rowIdx := range updateRows {
				if oldVals[rowIdx] != newVals[rowIdx] {
					setTouched(updateOrdinal)
				}
			}
			return nil
		}
		for updateOrdinal, rowIdx := range updateRows {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					setTouched(updateOrdinal)
				}
				continue
			}
			if oldVals[rowIdx] != newVals[rowIdx] {
				setTouched(updateOrdinal)
			}
		}
		return nil
	case types.ETJson, types.ETVectorFloat32:
		if compareCol.notNull {
			for updateOrdinal, rowIdx := range updateRows {
				if !bytes.Equal(oldCol.GetRaw(rowIdx), newCol.GetRaw(rowIdx)) {
					setTouched(updateOrdinal)
				}
			}
			return nil
		}
		for updateOrdinal, rowIdx := range updateRows {
			oldIsNull := oldCol.IsNull(rowIdx)
			newIsNull := newCol.IsNull(rowIdx)
			if oldIsNull || newIsNull {
				if oldIsNull != newIsNull {
					setTouched(updateOrdinal)
				}
				continue
			}
			if !bytes.Equal(oldCol.GetRaw(rowIdx), newCol.GetRaw(rowIdx)) {
				setTouched(updateOrdinal)
			}
		}
		return nil
	default:
		return errors.Errorf("unsupported eval type %d in COMPLETE DELTA APPLY comparison", compareCol.fieldType.EvalType())
	}
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
func (e *CancelMaterializedViewJobExec) Next(ctx context.Context, _ *chunk.Chunk) error {
	if e.done {
		return nil
	}
	e.done = true

	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMVMaintenance)
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

	var applied bool
	switch e.stmt.Tp {
	case ast.CancelMaterializedViewJobTypeRefresh:
		applied, err = requestRefreshHistCancel(ctx, sctx, uint64(e.stmt.JobID), requesterArg)
		if err != nil {
			return err
		}
		if !applied {
			return errors.NewNoStackErrorf(
				"cannot cancel materialized view refresh job %d: job not running, not found, or cancel already requested",
				e.stmt.JobID,
			)
		}
	case ast.CancelMaterializedViewJobTypeLogPurge:
		applied, err = requestPurgeHistCancel(ctx, sctx, uint64(e.stmt.JobID), requesterArg)
		if err != nil {
			return err
		}
		if !applied {
			return errors.NewNoStackErrorf(
				"cannot cancel materialized view log purge job %d: job not running, not found, or cancel already requested",
				e.stmt.JobID,
			)
		}
	default:
		return errors.Errorf("cancel materialized view job: unsupported type %d", e.stmt.Tp)
	}
	return nil
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
	isInternalSQL := e.Ctx().GetSessionVars().InRestrictedSQL
	purgeMethod, err := validatePurgeMaterializedViewLogStmt(s, isInternalSQL)
	if err != nil {
		return err
	}
	schemaName, baseTableMeta, mlogName, mlogID, mlogInfo, err := e.resolvePurgeMaterializedViewLogMeta(s)
	if err != nil {
		return err
	}
	releaseCtx := kctx
	taskCancelController := newMVTaskCancelController(kctx)
	defer taskCancelController.cancel()
	kctx = taskCancelController.context()
	finalizeCtx := context.WithoutCancel(kctx)
	batchSize := int64(e.Ctx().GetSessionVars().MLogPurgeBatchSize)
	if batchSize <= 0 {
		batchSize = int64(variable.DefTiDBMLogPurgeBatchSize)
	}

	purgeSctx, err := e.GetSysSession()
	if err != nil {
		return err
	}
	defer e.ReleaseSysSession(releaseCtx, purgeSctx)
	purgeSessVars := purgeSctx.GetSessionVars()
	targetMaintainMemQuota := e.Ctx().GetSessionVars().MVMaintainMemQuota
	restorePurgeMemQuota, err := applyMVMaintenanceMemQuota(purgeSessVars, targetMaintainMemQuota)
	if err != nil {
		return err
	}
	defer restorePurgeMemQuota()
	failpoint.InjectCall("mvMaintainMemQuotaAppliedOnPurgeSession", purgeSessVars.MemQuotaQuery, targetMaintainMemQuota)
	sqlExec := purgeSctx.GetSQLExecutor()

	histSctx, err := e.GetSysSession()
	if err != nil {
		return err
	}
	defer e.ReleaseSysSession(releaseCtx, histSctx)
	histSQLExec := histSctx.GetSQLExecutor()

	var scheduleEvalSctx sessionctx.Context
	if isInternalSQL {
		scheduleEvalSctx, err = e.GetSysSession()
		if err != nil {
			return err
		}
		defer e.ReleaseSysSession(releaseCtx, scheduleEvalSctx)
	}

	totalPurgeRows := int64(0)
	safePurgeTSOReady := false
	safePurgeTSO := uint64(0)
	lockedLastPurgedTSO := uint64(0)
	lockedLastPurgedTSOReady := false
	purgeJobID := uint64(0)
	purgeHistRunningInserted := false
	stopCancelWatcher := func() {}
	defer func() {
		stopCancelWatcher()
	}()

	finalizeFailure := func(purgeErr error) error {
		purgeFailedReason, finalErr := taskCancelController.normalizeTaskFailure(purgeErr)
		if !purgeHistRunningInserted {
			return errors.Trace(finalErr)
		}
		purgeErrMsg := finalErr.Error()
		if purgeFailedReason != nil {
			purgeErrMsg = *purgeFailedReason
		}
		if histErr := finalizeMLogPurgeHistWithRetry(
			finalizeCtx,
			histSQLExec,
			purgeJobID,
			mlogID,
			purgeHistStatusFailed,
			totalPurgeRows,
			&purgeErrMsg,
		); histErr != nil {
			return errors.Annotatef(histErr, "purge materialized view log: failed to finalize purge history after error %v", finalErr)
		}
		return errors.Trace(finalErr)
	}
	finalizeSuccess := func() error {
		if !purgeHistRunningInserted {
			return nil
		}
		failpoint.Inject("mockPurgeMaterializedViewLogFinalizeSuccessErr", func(val failpoint.Value) {
			if v, ok := val.(bool); ok && v {
				failpoint.Return(errors.New("mock purge finalize success error"))
			}
		})
		return finalizeMLogPurgeHistWithRetry(
			finalizeCtx,
			histSQLExec,
			purgeJobID,
			mlogID,
			purgeHistStatusSuccess,
			totalPurgeRows,
			nil,
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
					e.Ctx().GetSessionVars().StmtCtx.AppendWarning(
						errors.Annotate(histErr, "purge materialized view log: purge committed but failed to finalize purge history"),
					)
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
				if err := insertMLogPurgeHistRunning(kctx, histSQLExec, purgeJobID, mlogID, purgeMethod); err != nil {
					_, _ = sqlExec.ExecuteInternal(kctx, "ROLLBACK")
					return errors.Trace(err)
				}
				purgeHistRunningInserted = true
				stopCancelWatcher, err = startMVTaskCancelWatcher(
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
				)
				if err != nil {
					_, _ = sqlExec.ExecuteInternal(kctx, "ROLLBACK")
					return finalizeFailure(err)
				}
				failpoint.Inject("pausePurgeMaterializedViewLogAfterInsertPurgeHistRunning", func() {})
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
				mlogInfo.DefinitionSQLMode,
				func() {
					logRuntimeMaterializedViewLogPurgeNextTimeUpdateNull(schemaName.O, mlogName.O, mlogInfo.PurgeNext)
				},
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
				e.Ctx().GetSessionVars().StmtCtx.AppendWarning(
					errors.Annotate(err, "purge materialized view log: purge committed but failed to finalize purge history"),
				)
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
			"SELECT MIN(COALESCE(LAST_SUCCESS_READ_TSO, CAST(0 AS UNSIGNED))) FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID IN (%s)",
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
			safePurgeTSO = minRows[0].GetUint64(0)
			if safePurgeTSO > purgeStartTS {
				safePurgeTSO = purgeStartTS
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
	return rows[0].GetUint64(0), true, nil
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

	deleteSQL := sqlescape.MustEscapeSQL(
		"DELETE /*+ read_from_storage(tiflash[%n.%n]) */ FROM %n.%n WHERE _tidb_commit_ts <= %? LIMIT %?",
		schemaName,
		mlogName,
		schemaName,
		mlogName,
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
	if lastPurgedTSO != nil {
		// Keep LAST_PURGED_TSO monotonic even if different purge transactions interleave.
		updateLastPurgedTSOSQL := `UPDATE mysql.tidb_mlog_purge_info
SET
	LAST_PURGED_TSO = %?
WHERE MLOG_ID = %?
	AND (LAST_PURGED_TSO IS NULL OR LAST_PURGED_TSO < %?)`
		_, err := sqlExec.ExecuteInternal(kctx, updateLastPurgedTSOSQL, *lastPurgedTSO, mlogID, *lastPurgedTSO)
		if err != nil {
			if infoschema.ErrTableNotExists.Equal(err) {
				return errors.New("required system table mysql.tidb_mlog_purge_info does not exist")
			}
			return errors.Trace(err)
		}
	}

	if shouldUpdateNextTime {
		var nextTimeArg any
		if nextTime != nil {
			nextTimeArg = *nextTime
		}
		updateNextTimeSQL := `UPDATE mysql.tidb_mlog_purge_info
SET
		NEXT_TIME = %?
WHERE MLOG_ID = %?`
		_, err := sqlExec.ExecuteInternal(kctx, updateNextTimeSQL, nextTimeArg, mlogID)
		if err != nil {
			if infoschema.ErrTableNotExists.Equal(err) {
				return errors.New("required system table mysql.tidb_mlog_purge_info does not exist")
			}
			return errors.Trace(err)
		}
	}
	return nil
}

func validatePurgeMaterializedViewLogStmt(s *ast.PurgeMaterializedViewLogStmt, isInternalSQL bool) (string, error) {
	if s == nil || s.Table == nil {
		return "", errors.New("purge materialized view log: missing table name")
	}
	if isInternalSQL {
		return "auto", nil
	}
	return "manual", nil
}

func insertMLogPurgeHistRunning(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	purgeJobID uint64,
	mlogID int64,
	purgeMethod string,
) error {
	insertSQL := `INSERT INTO mysql.tidb_mlog_purge_hist (
		PURGE_JOB_ID,
		MLOG_ID,
		PURGE_METHOD,
		PURGE_TIME,
		PURGE_ROWS,
		PURGE_STATUS
	) VALUES (
		%?,
		%?,
		%?,
		NOW(6),
		%?,
		%?
	)`
	_, err := sqlExec.ExecuteInternal(kctx, insertSQL, purgeJobID, mlogID, purgeMethod, int64(0), purgeHistStatusRunning)
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
	purgeFailedReason *string,
) error {
	var purgeFailedReasonArg any
	if purgeFailedReason != nil {
		purgeFailedReasonArg = *purgeFailedReason
	}
	updateSQL := `UPDATE mysql.tidb_mlog_purge_hist
	SET
		PURGE_ENDTIME = NOW(6),
		PURGE_ROWS = %?,
		PURGE_STATUS = %?,
		PURGE_FAILED_REASON = %?
	WHERE PURGE_JOB_ID = %?`
	_, err := sqlExec.ExecuteInternal(kctx, updateSQL, purgeRows, purgeStatus, purgeFailedReasonArg, purgeJobID)
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

func finalizeMLogPurgeHistWithRetry(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	purgeJobID uint64,
	mlogID int64,
	purgeStatus string,
	purgeRows int64,
	purgeFailedReason *string,
) error {
	firstErr := finalizeMLogPurgeHist(
		kctx,
		sqlExec,
		purgeJobID,
		purgeStatus,
		purgeRows,
		purgeFailedReason,
	)
	if firstErr == nil {
		return nil
	}
	retryErr := finalizeMLogPurgeHist(
		kctx,
		sqlExec,
		purgeJobID,
		purgeStatus,
		purgeRows,
		purgeFailedReason,
	)
	if retryErr == nil {
		return nil
	}
	logutil.BgLogger().Warn("purge materialized view log: failed to finalize purge history after retry",
		zap.Uint64("purgeJobID", purgeJobID),
		zap.Int64("mlogID", mlogID),
		zap.String("purgeStatus", purgeStatus),
		zap.NamedError("firstAttemptErr", firstErr),
		zap.NamedError("retryErr", retryErr),
	)
	return errors.Annotatef(retryErr, "first finalize attempt failed: %v", firstErr)
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
	const slowRefreshThreshold = 30 * time.Second
	refreshStart := time.Now()
	var (
		lockRefreshInfoRowDur time.Duration
		executeDataChangesDur time.Duration
		txnTotalDur           time.Duration
		mviewID               int64
	)
	isInternalSQL := e.Ctx().GetSessionVars().InRestrictedSQL
	defer func() {
		total := time.Since(refreshStart)
		if total < slowRefreshThreshold {
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
			zap.Duration("slowThreshold", slowRefreshThreshold),
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
	stepSet, err := newMVRefreshStepSet(refreshMode)
	if err != nil {
		return err
	}
	releaseCtx := kctx
	taskCancelController := newMVTaskCancelController(kctx)
	defer taskCancelController.cancel()
	kctx = taskCancelController.context()
	finalizeCtx := context.WithoutCancel(kctx)

	schemaName, tblInfo, err := e.resolveRefreshMaterializedViewTarget(s)
	if err != nil {
		return err
	}

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
	restoreRefreshExecutionVars, err := applyRefreshExecutionSessionVars(sessVars, refreshExecutionVars)
	if err != nil {
		return err
	}
	defer restoreRefreshExecutionVars()
	failpoint.InjectCall("mvMaintainMemQuotaAppliedOnRefreshSession", sessVars.MemQuotaQuery, refreshExecutionVars.maintainMemQuota)

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

	if refreshMode == ast.RefreshMaterializedViewModeCompleteOutOfPlace {
		expectedLastSuccessReadTSO, expectedLastSuccessReadTSONull, err := readRefreshInfoReadTSO(kctx, sqlExec, mviewID)
		if err != nil {
			return err
		}
		refreshJobID, err := allocRefreshMaterializedViewJobID(e.Ctx().GetStore())
		if err != nil {
			return err
		}
		histSctx, err := e.GetSysSession()
		if err != nil {
			return err
		}
		defer e.ReleaseSysSession(releaseCtx, histSctx)
		histSQLExec := histSctx.GetSQLExecutor()

		if err := observeMVRefreshStep(e.stepObserver, stepSet.insertHistRunning, func() error {
			return insertRefreshHistRunning(kctx, histSQLExec, refreshJobID, mviewID, refreshMethod)
		}); err != nil {
			return err
		}

		finalizeFailure := func(refreshErr error) error {
			refreshFailedReason, finalErr := taskCancelController.normalizeTaskFailure(refreshErr)
			refreshErrMsg := finalErr.Error()
			if refreshFailedReason != nil {
				refreshErrMsg = *refreshFailedReason
			}
			histErr := observeMVRefreshStep(e.stepObserver, stepSet.finalizeHist, func() error {
				return finalizeRefreshHistWithRetry(
					finalizeCtx,
					histSQLExec,
					refreshJobID,
					mviewID,
					refreshHistStatusFailed,
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
		stopCancelWatcher, err := startMVTaskCancelWatcher(
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
		)
		if err != nil {
			return finalizeFailure(err)
		}
		defer stopCancelWatcher()
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
			expectedLastSuccessReadTSO,
			expectedLastSuccessReadTSONull,
			refreshExecutionVars.maintainMemQuota,
		)
		if err != nil {
			return finalizeFailure(err)
		}
		if err := observeMVRefreshStep(e.stepObserver, stepSet.finalizeHist, func() error {
			return finalizeRefreshHistWithRetry(
				finalizeCtx,
				histSQLExec,
				refreshJobID,
				mviewID,
				refreshHistStatusSuccess,
				&buildReadTSO,
				nil,
				nil,
			)
		}); err != nil {
			e.Ctx().GetSessionVars().StmtCtx.AppendWarning(
				errors.Annotate(err, "refresh materialized view: refresh committed but failed to finalize refresh history"),
			)
		}
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
	var lockedReadTSO uint64
	var lockedReadTSONull bool
	if err := observeMVRefreshStep(e.stepObserver, stepSet.lockRefreshInfo, func() error {
		lockRefreshInfoRowStart := time.Now()
		var lockErr error
		lockedReadTSO, lockedReadTSONull, lockErr = lockRefreshInfoRow(kctx, sqlExec, mviewID)
		lockRefreshInfoRowDur = time.Since(lockRefreshInfoRowStart)
		return lockErr
	}); err != nil {
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
	defer e.ReleaseSysSession(releaseCtx, histSctx)
	histSQLExec := histSctx.GetSQLExecutor()

	if err := observeMVRefreshStep(e.stepObserver, stepSet.insertHistRunning, func() error {
		return insertRefreshHistRunning(kctx, histSQLExec, refreshJobID, mviewID, refreshMethod)
	}); err != nil {
		return err
	}

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
		histErr := observeMVRefreshStep(e.stepObserver, stepSet.finalizeHist, func() error {
			return finalizeRefreshHistWithRetry(
				finalizeCtx,
				histSQLExec,
				refreshJobID,
				mviewID,
				refreshHistStatusFailed,
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
	stopCancelWatcher, err := startMVTaskCancelWatcher(
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
	)
	if err != nil {
		return finalizeFailure(err)
	}
	defer stopCancelWatcher()

	failpoint.InjectCall("refreshMaterializedViewAfterInsertRefreshHistRunning")
	failpoint.Inject("pauseRefreshMaterializedViewAfterInsertRefreshHistRunning", func() {})

	var lastSuccessfulRefreshReadTSO uint64
	if refreshMode == ast.RefreshMaterializedViewModeFast {
		// LAST_SUCCESS_READ_TSO is BIGINT UNSIGNED DEFAULT NULL. FAST refresh requires it to be non-NULL.
		if lockedReadTSONull {
			return finalizeFailure(errors.New("refresh materialized view fast: LAST_SUCCESS_READ_TSO is NULL"))
		}
		lastSuccessfulRefreshReadTSO = lockedReadTSO
	}

	executeDataChangesStart := time.Now()
	if err := executeRefreshMaterializedViewDataChanges(
		kctx,
		sqlExec,
		sessVars,
		s,
		refreshMode,
		schemaName,
		tblInfo,
		lastSuccessfulRefreshReadTSO,
		stepSet,
		e.stepObserver,
		e.planFormatForObserver,
	); err != nil {
		executeDataChangesDur = time.Since(executeDataChangesStart)
		return finalizeFailure(err)
	}
	executeDataChangesDur = time.Since(executeDataChangesStart)

	refreshReadTSO, err := getRefreshReadTSOForSuccess(sessVars)
	if err != nil {
		return finalizeFailure(err)
	}

	var refreshRows *int64
	if refreshMode == ast.RefreshMaterializedViewModeFast {
		refreshRows = collectFastRefreshMLogScanRows(sessVars)
	}

	nextTime, shouldUpdateNextTime, err := deriveRuntimeMaterializedScheduleNextTime(
		kctx,
		scheduleEvalSctx,
		refreshSctx,
		tblInfo.MaterializedView.RefreshStartWith,
		tblInfo.MaterializedView.RefreshNext,
		isInternalSQL,
		tblInfo.MaterializedView.DefinitionSQLMode,
		func() {
			logRuntimeMaterializedViewRefreshNextTimeUpdateNull(schemaName.O, tblInfo.Name.O, tblInfo.MaterializedView.RefreshNext)
		},
	)
	if err != nil {
		return finalizeFailure(err)
	}

	if err := observeMVRefreshStep(e.stepObserver, stepSet.persistRefreshInfo, func() error {
		return persistRefreshSuccess(
			kctx,
			sqlExec,
			mviewID,
			lockedReadTSO,
			lockedReadTSONull,
			refreshReadTSO,
			nextTime,
			shouldUpdateNextTime,
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

	if err := observeMVRefreshStep(e.stepObserver, stepSet.finalizeHist, func() error {
		return finalizeRefreshHistWithRetry(
			finalizeCtx,
			histSQLExec,
			refreshJobID,
			mviewID,
			refreshHistStatusSuccess,
			&refreshReadTSO,
			refreshRows,
			nil,
		)
	}); err != nil {
		e.Ctx().GetSessionVars().StmtCtx.AppendWarning(
			errors.Annotate(err, "refresh materialized view: refresh committed but failed to finalize refresh history"),
		)
	}
	return nil
}

func (e *RefreshMaterializedViewExec) executeRefreshMaterializedViewCompleteOutOfPlace(
	kctx context.Context,
	releaseCtx context.Context,
	s *ast.RefreshMaterializedViewStmt,
	refreshSctx sessionctx.Context,
	isInternalSQL bool,
	schemaName pmodel.CIStr,
	tblInfo *model.TableInfo,
	stepSet mvRefreshStepSet,
	expectedLastSuccessReadTSO uint64,
	expectedLastSuccessReadTSONull bool,
	targetMaintainMemQuota int64,
) (buildReadTSO uint64, err error) {
	if err := kctx.Err(); err != nil {
		return 0, err
	}
	buildSctx, err := e.GetSysSession()
	if err != nil {
		return 0, err
	}
	defer e.ReleaseSysSession(releaseCtx, buildSctx)

	buildSessVars := buildSctx.GetSessionVars()
	restoreBuildMemQuota, err := applyMVMaintenanceMemQuota(buildSessVars, targetMaintainMemQuota)
	if err != nil {
		return 0, err
	}
	defer restoreBuildMemQuota()
	failpoint.InjectCall("mvMaintainMemQuotaAppliedOnRefreshOutOfPlaceBuildSession", buildSessVars.MemQuotaQuery, targetMaintainMemQuota)

	restoreBuildSessVars, err := initRefreshMaterializedViewSession(buildSessVars, tblInfo.MaterializedView)
	if err != nil {
		return 0, err
	}
	defer restoreBuildSessVars()

	origInMaterializedViewMaintenance := buildSessVars.InMaterializedViewMaintenance
	buildSessVars.InMaterializedViewMaintenance = true
	defer func() {
		buildSessVars.InMaterializedViewMaintenance = origInMaterializedViewMaintenance
	}()

	if buildSessVars.InTxn() {
		return 0, errors.New("refresh materialized view complete OUT OF PLACE: build session unexpectedly in transaction")
	}

	shadowTableName := buildMVRefreshShadowTableName(tblInfo.ID)
	shadowCreated := false
	buildSQLExec := buildSctx.GetSQLExecutor()
	defer func() {
		if err == nil || !shadowCreated {
			return
		}
		dropShadowSQL := sqlescape.MustEscapeSQL("DROP TABLE IF EXISTS %n.%n", schemaName.O, shadowTableName)
		if dropErr := executeRefreshMaterializedViewInternalSQL(context.WithoutCancel(kctx), buildSQLExec, dropShadowSQL); dropErr != nil {
			logutil.BgLogger().Warn(
				"failed to cleanup shadow table after out-of-place complete refresh error",
				zap.String("schema", schemaName.O),
				zap.String("mview", s.ViewName.Name.O),
				zap.String("shadowTable", shadowTableName),
				zap.Error(dropErr),
			)
			err = errors.Annotatef(err, "cleanup shadow table %s.%s failed: %v", schemaName.O, shadowTableName, dropErr)
		}
	}()

	shadowTableInfo, err := buildMVRefreshOutOfPlaceShadowTableInfo(schemaName, shadowTableName, tblInfo)
	if err != nil {
		return 0, err
	}
	if err := observeMVRefreshStep(e.stepObserver, stepSet.dataChangeOutOfPlaceCreateShadow, func() error {
		if err := kctx.Err(); err != nil {
			return err
		}
		if execErr := domain.GetDomain(e.Ctx()).DDLExecutor().CreateMaterializedViewShadowTable(
			refreshSctx,
			tblInfo.DBID,
			schemaName,
			shadowTableInfo,
		); execErr != nil {
			return execErr
		}
		shadowCreated = true
		return nil
	}); err != nil {
		return 0, err
	}

	failpoint.InjectCall("refreshMaterializedViewOutOfPlaceAfterCreateShadow")
	failpoint.Inject("pauseRefreshMaterializedViewOutOfPlaceAfterCreateShadow", func() {})
	storeName := e.Ctx().GetStore().Name()
	if err := observeMVRefreshStep(e.stepObserver, stepSet.dataChangeOutOfPlaceLoadShadow, func() error {
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

		buildSQL, buildErr := buildMVRefreshOutOfPlaceBuildSQL(schemaName.O, shadowTableName, tblInfo, storeName)
		if buildErr != nil {
			return buildErr
		}
		if buildErr = executeRefreshMaterializedViewInternalSQL(kctx, buildSQLExec, buildSQL); buildErr != nil {
			return buildErr
		}
		// Capture profile rows for the real shadow-load statement before any follow-up SQL (for example read tso query)
		// overwrites session statement context.
		emitMVRefreshStepPlanRows(e.stepObserver, stepSet.dataChangeOutOfPlaceLoadShadow, buildSessVars, e.planFormatForObserver)
		buildReadTSO, buildErr = getMVRefreshOutOfPlaceBuildReadTSO(kctx, buildSQLExec)
		return buildErr
	}); err != nil {
		return 0, err
	}
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
	if err := observeMVRefreshStep(e.stepObserver, stepSet.dataChangeOutOfPlaceCutover, func() error {
		var lookupErr error
		shadowTableID, lookupErr = getMVRefreshOutOfPlaceShadowTableID(kctx, buildSctx, schemaName, shadowTableName)
		if lookupErr != nil {
			return lookupErr
		}
		var nextTime *string
		var shouldUpdateNextTime bool
		if isInternalSQL {
			scheduleEvalSctx, scheduleErr := e.GetSysSession()
			if scheduleErr != nil {
				return scheduleErr
			}
			defer e.ReleaseSysSession(releaseCtx, scheduleEvalSctx)
			nextTime, shouldUpdateNextTime, scheduleErr = deriveRuntimeMaterializedScheduleNextTime(
				kctx,
				scheduleEvalSctx,
				refreshSctx,
				tblInfo.MaterializedView.RefreshStartWith,
				tblInfo.MaterializedView.RefreshNext,
				isInternalSQL,
				tblInfo.MaterializedView.DefinitionSQLMode,
				func() {
					logRuntimeMaterializedViewRefreshNextTimeUpdateNull(schemaName.O, tblInfo.Name.O, tblInfo.MaterializedView.RefreshNext)
				},
			)
			if scheduleErr != nil {
				return scheduleErr
			}
		}
		if err := kctx.Err(); err != nil {
			return err
		}
		return domain.GetDomain(e.Ctx()).DDLExecutor().RefreshMaterializedViewCompleteOutOfPlaceCutover(
			e.Ctx(),
			tblInfo.DBID,
			schemaName,
			s.ViewName.Name,
			tblInfo.ID,
			shadowTableID,
			buildReadTSO,
			expectedLastSuccessReadTSO,
			expectedLastSuccessReadTSONull,
			nextTime,
			shouldUpdateNextTime,
		)
	}); err != nil {
		return 0, err
	}
	return buildReadTSO, nil
}

func applyMVMaintenanceMemQuota(sessVars *variable.SessionVars, targetMemQuota int64) (func(), error) {
	if sessVars == nil {
		return nil, errors.New("mv maintenance: session vars is nil")
	}
	originMemQuota := sessVars.MemQuotaQuery
	if originMemQuota == targetMemQuota {
		return func() {}, nil
	}
	if err := sessVars.SetSystemVar(variable.TiDBMemQuotaQuery, strconv.FormatInt(targetMemQuota, 10)); err != nil {
		return nil, errors.Annotate(err, "mv maintenance: failed to apply tidb_mv_maintain_mem_quota to tidb_mem_quota_query")
	}
	return func() {
		if err := sessVars.SetSystemVar(variable.TiDBMemQuotaQuery, strconv.FormatInt(originMemQuota, 10)); err != nil {
			logutil.BgLogger().Warn(
				"mv maintenance: failed to restore tidb_mem_quota_query after using tidb_mv_maintain_mem_quota",
				zap.Int64("originMemQuota", originMemQuota),
				zap.Int64("targetMemQuota", targetMemQuota),
				zap.Error(err),
			)
		}
	}, nil
}

type refreshExecutionSessionVars struct {
	maintainMemQuota       int64
	tiFlashMaxThreads      int64
	fineGrainedStreamCount int64
	fineGrainedBatchSize   uint64
}

func captureRefreshExecutionSessionVars(sessVars *variable.SessionVars) refreshExecutionSessionVars {
	return refreshExecutionSessionVars{
		maintainMemQuota:       sessVars.MVMaintainMemQuota,
		tiFlashMaxThreads:      sessVars.TiFlashMaxThreads,
		fineGrainedStreamCount: sessVars.TiFlashFineGrainedShuffleStreamCount,
		fineGrainedBatchSize:   sessVars.TiFlashFineGrainedShuffleBatchSize,
	}
}

func captureAppliedRefreshExecutionSessionVars(sessVars *variable.SessionVars) refreshExecutionSessionVars {
	return refreshExecutionSessionVars{
		maintainMemQuota:       sessVars.MemQuotaQuery,
		tiFlashMaxThreads:      sessVars.TiFlashMaxThreads,
		fineGrainedStreamCount: sessVars.TiFlashFineGrainedShuffleStreamCount,
		fineGrainedBatchSize:   sessVars.TiFlashFineGrainedShuffleBatchSize,
	}
}

func applyRefreshExecutionSessionVars(sessVars *variable.SessionVars, target refreshExecutionSessionVars) (func(), error) {
	if sessVars == nil {
		return nil, errors.New("mv maintenance: session vars is nil")
	}

	origin := captureAppliedRefreshExecutionSessionVars(sessVars)
	if origin != target {
		if err := sessVars.SetSystemVar(variable.TiDBMemQuotaQuery, strconv.FormatInt(target.maintainMemQuota, 10)); err != nil {
			return nil, errors.Annotate(err, "mv maintenance: failed to apply tidb_mv_maintain_mem_quota to tidb_mem_quota_query")
		}
		if err := sessVars.SetSystemVar(variable.TiDBMaxTiFlashThreads, strconv.FormatInt(target.tiFlashMaxThreads, 10)); err != nil {
			restoreRefreshExecutionSessionVars(sessVars, origin, captureAppliedRefreshExecutionSessionVars(sessVars))
			return nil, errors.Annotate(err, "mv maintenance: failed to apply tidb_max_tiflash_threads on refresh session")
		}
		if err := sessVars.SetSystemVar(variable.TiFlashFineGrainedShuffleStreamCount, strconv.FormatInt(target.fineGrainedStreamCount, 10)); err != nil {
			restoreRefreshExecutionSessionVars(sessVars, origin, captureAppliedRefreshExecutionSessionVars(sessVars))
			return nil, errors.Annotate(err, "mv maintenance: failed to apply tiflash_fine_grained_shuffle_stream_count on refresh session")
		}
		if err := sessVars.SetSystemVar(variable.TiFlashFineGrainedShuffleBatchSize, strconv.FormatUint(target.fineGrainedBatchSize, 10)); err != nil {
			restoreRefreshExecutionSessionVars(sessVars, origin, captureAppliedRefreshExecutionSessionVars(sessVars))
			return nil, errors.Annotate(err, "mv maintenance: failed to apply tiflash_fine_grained_shuffle_batch_size on refresh session")
		}
	}
	failpoint.InjectCall(
		"refreshMaterializedViewTiFlashSessionVarsApplied",
		target.tiFlashMaxThreads,
		target.fineGrainedStreamCount,
		target.fineGrainedBatchSize,
	)

	return func() {
		if origin == target {
			return
		}
		restoreRefreshExecutionSessionVars(sessVars, origin, target)
	}, nil
}

func restoreRefreshExecutionSessionVars(sessVars *variable.SessionVars, origin, current refreshExecutionSessionVars) {
	if err := sessVars.SetSystemVar(variable.TiDBMemQuotaQuery, strconv.FormatInt(origin.maintainMemQuota, 10)); err != nil {
		logutil.BgLogger().Warn(
			"mv maintenance: failed to restore tidb_mem_quota_query on refresh session",
			zap.Int64("originMemQuotaQuery", origin.maintainMemQuota),
			zap.Int64("currentMemQuotaQuery", current.maintainMemQuota),
			zap.Error(err),
		)
	}
	if err := sessVars.SetSystemVar(variable.TiDBMaxTiFlashThreads, strconv.FormatInt(origin.tiFlashMaxThreads, 10)); err != nil {
		logutil.BgLogger().Warn(
			"mv maintenance: failed to restore tidb_max_tiflash_threads on refresh session",
			zap.Int64("originMaxThreads", origin.tiFlashMaxThreads),
			zap.Int64("currentMaxThreads", current.tiFlashMaxThreads),
			zap.Error(err),
		)
	}
	if err := sessVars.SetSystemVar(variable.TiFlashFineGrainedShuffleStreamCount, strconv.FormatInt(origin.fineGrainedStreamCount, 10)); err != nil {
		logutil.BgLogger().Warn(
			"mv maintenance: failed to restore tiflash_fine_grained_shuffle_stream_count on refresh session",
			zap.Int64("originFineGrainedStreamCount", origin.fineGrainedStreamCount),
			zap.Int64("currentFineGrainedStreamCount", current.fineGrainedStreamCount),
			zap.Error(err),
		)
	}
	if err := sessVars.SetSystemVar(variable.TiFlashFineGrainedShuffleBatchSize, strconv.FormatUint(origin.fineGrainedBatchSize, 10)); err != nil {
		logutil.BgLogger().Warn(
			"mv maintenance: failed to restore tiflash_fine_grained_shuffle_batch_size on refresh session",
			zap.Uint64("originFineGrainedBatchSize", origin.fineGrainedBatchSize),
			zap.Uint64("currentFineGrainedBatchSize", current.fineGrainedBatchSize),
			zap.Error(err),
		)
	}
}

func buildMVRefreshShadowTableName(mviewID int64) string {
	return fmt.Sprintf("%s%d_%d", mvRefreshShadowTablePrefix, mviewID, time.Now().UnixNano())
}

func buildMVRefreshOutOfPlaceShadowTableInfo(
	schemaName pmodel.CIStr,
	shadowTableName string,
	tblInfo *model.TableInfo,
) (*model.TableInfo, error) {
	if tblInfo == nil || tblInfo.MaterializedView == nil {
		return nil, errors.New("refresh materialized view complete OUT OF PLACE: invalid materialized view metadata")
	}
	shadowTableInfo, err := ddl.BuildTableInfoWithLike(
		ast.Ident{Schema: schemaName, Name: pmodel.NewCIStr(shadowTableName)},
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
) (string, error) {
	if tblInfo.MaterializedView == nil || len(tblInfo.MaterializedView.SQLContent) == 0 {
		return "", errors.New("refresh materialized view: invalid select sql")
	}
	selectSQL := tblInfo.MaterializedView.SQLContent
	if shouldUseImportIntoForMVRefreshOutOfPlace(storeName) {
		prefix := sqlescape.MustEscapeSQL("IMPORT INTO %n.%n FROM ", schemaName, shadowTableName)
		return prefix + "(" + selectSQL + ") WITH disable_precheck", nil
	}
	prefix := sqlescape.MustEscapeSQL("INSERT INTO %n.%n ", schemaName, shadowTableName)
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
	schemaName pmodel.CIStr,
	shadowTableName string,
) (int64, error) {
	is := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
	shadowTbl, err := is.TableByName(kctx, schemaName, pmodel.NewCIStr(shadowTableName))
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
	return mode, methodType + " " + methodOrigin, nil
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
) (lockedReadTSO uint64, lockedReadTSONull bool, err error) {
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
		lockedReadTSO = lockedRow.GetUint64(1)
	}
	return lockedReadTSO, lockedReadTSONull, nil
}

func buildMVRefreshAdvisoryLockName(schemaID int64, mviewID int64) string {
	return fmt.Sprintf("mv_refresh_%d_%d", schemaID, mviewID)
}

func acquireMVRefreshAdvisoryLock(
	refreshSctx sessionctx.Context,
	schemaName pmodel.CIStr,
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
	releasedCnt := 0
	for refreshSctx.ReleaseAdvisoryLock(lockName) {
		releasedCnt++
	}
	return releasedCnt
}

func readRefreshInfoReadTSO(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	mviewID int64,
) (readTSO uint64, readTSONull bool, err error) {
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
		readTSO = recheckRow.GetUint64(0)
	}
	return readTSO, readTSONull, nil
}

func executeRefreshMaterializedViewDataChanges(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	sessVars *variable.SessionVars,
	s *ast.RefreshMaterializedViewStmt,
	refreshMode ast.RefreshMaterializedViewMode,
	schemaName pmodel.CIStr,
	tblInfo *model.TableInfo,
	lastSuccessfulRefreshReadTSO uint64,
	stepSet mvRefreshStepSet,
	stepObserver mvRefreshStepObserver,
	explainFormat string,
) error {
	// TiFlash read is blocked for write statements when sql_mode is strict. Refresh prefers TiFlash for the
	// scan part, so we bypass this guard for MV maintenance statements.
	origInMaterializedViewMaintenance := sessVars.InMaterializedViewMaintenance
	sessVars.InMaterializedViewMaintenance = true
	defer func() {
		sessVars.InMaterializedViewMaintenance = origInMaterializedViewMaintenance
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
			lastSuccessfulRefreshReadTSO,
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
	schemaName pmodel.CIStr,
	tblInfo *model.TableInfo,
	stepSet mvRefreshStepSet,
	stepObserver mvRefreshStepObserver,
	explainFormat string,
) error {
	deleteSQL := sqlescape.MustEscapeSQL("DELETE FROM %n.%n", schemaName.O, s.ViewName.Name.O)
	insertPrefix := sqlescape.MustEscapeSQL("INSERT INTO %n.%n ", schemaName.O, s.ViewName.Name.O)
	/* #nosec G202: SQLContent is restored from AST (single SELECT statement, no user-provided placeholders). */
	insertSQL := insertPrefix + tblInfo.MaterializedView.SQLContent
	if err := observeMVRefreshStep(stepObserver, stepSet.dataChangeCompleteDelete, func() error {
		_, deleteErr := sqlExec.ExecuteInternal(kctx, deleteSQL)
		return deleteErr
	}); err != nil {
		return err
	}
	emitMVRefreshStepPlanRows(stepObserver, stepSet.dataChangeCompleteDelete, sessVars, explainFormat)

	if err := observeMVRefreshStep(stepObserver, stepSet.dataChangeCompleteInsert, func() error {
		_, insertErr := sqlExec.ExecuteInternal(kctx, insertSQL)
		return insertErr
	}); err != nil {
		return err
	}
	emitMVRefreshStepPlanRows(stepObserver, stepSet.dataChangeCompleteInsert, sessVars, explainFormat)
	return nil
}

func executeRefreshMaterializedViewFast(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	sessVars *variable.SessionVars,
	s *ast.RefreshMaterializedViewStmt,
	lastSuccessfulRefreshReadTSO uint64,
	stepSet mvRefreshStepSet,
	stepObserver mvRefreshStepObserver,
	explainFormat string,
) error {
	if err := observeMVRefreshStep(stepObserver, stepSet.dataChangeFastMerge, func() error {
		return executeRefreshMaterializedViewImplement(
			kctx,
			sqlExec,
			sessVars,
			s,
			lastSuccessfulRefreshReadTSO,
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
		return executeRefreshMaterializedViewImplement(kctx, sqlExec, sessVars, s, 0)
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
	lastSuccessfulRefreshReadTSO uint64,
) error {
	implementStmt := &ast.RefreshMaterializedViewImplementStmt{
		RefreshStmt:                  s,
		LastSuccessfulRefreshReadTSO: lastSuccessfulRefreshReadTSO,
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

func allocRefreshMaterializedViewJobID(store kv.Storage) (uint64, error) {
	if store == nil {
		return 0, errors.New("refresh materialized view: invalid store")
	}
	ver, err := store.CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if ver.Ver == 0 {
		return 0, errors.New("refresh materialized view: invalid refresh job id")
	}
	return ver.Ver, nil
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
	mergePlan, ok := sessVars.StmtCtx.GetPlan().(*plannercore.MVDeltaMerge)
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
	case *plannercore.PhysicalTableScan:
		if p.Table != nil && p.Table.ID == mlogTableID {
			target[p.ID()] = struct{}{}
		}
	case *plannercore.PhysicalIndexScan:
		if p.Table != nil && p.Table.ID == mlogTableID {
			target[p.ID()] = struct{}{}
		}
	case *plannercore.PhysicalTableReader:
		for _, child := range p.TablePlans {
			collectMLogScanPlanIDs(child, mlogTableID, target)
		}
	case *plannercore.PhysicalIndexReader:
		for _, child := range p.IndexPlans {
			collectMLogScanPlanIDs(child, mlogTableID, target)
		}
	case *plannercore.PhysicalIndexLookUpReader:
		for _, child := range p.IndexPlans {
			collectMLogScanPlanIDs(child, mlogTableID, target)
		}
		for _, child := range p.TablePlans {
			collectMLogScanPlanIDs(child, mlogTableID, target)
		}
	case *plannercore.PhysicalIndexMergeReader:
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

func deriveRuntimeMaterializedScheduleNextTime(
	kctx context.Context,
	evalSctx sessionctx.Context,
	templateSctx sessionctx.Context,
	startExpr string,
	nextExpr string,
	isInternalSQL bool,
	scheduleSQLMode mysql.SQLMode,
	logNullUpdate func(),
) (*string, bool, error) {
	if !isInternalSQL {
		return nil, false, nil
	}
	nextAt, shouldUpdate, err := expression.DeriveMaterializedScheduleNextTimeUTC(
		kctx,
		evalSctx,
		templateSctx,
		startExpr,
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
	nextAtStr := nextAt.String()
	return &nextAtStr, shouldUpdate, nil
}

func logRuntimeMaterializedViewRefreshNextTimeUpdateNull(
	schemaName string,
	mvName string,
	nextExpr string,
) {
	if strings.TrimSpace(nextExpr) == "" {
		return
	}
	logutil.BgLogger().Error(
		"refresh materialized view: automatic refresh schedule disabled because NEXT expression evaluated to NULL, updating NEXT_TIME to NULL",
		zap.String("schemaName", schemaName),
		zap.String("tableName", mvName),
		zap.String("refreshNext", nextExpr),
	)
}

func logRuntimeMaterializedViewLogPurgeNextTimeUpdateNull(
	schemaName string,
	mlogName string,
	nextExpr string,
) {
	if strings.TrimSpace(nextExpr) == "" {
		return
	}
	logutil.BgLogger().Error(
		"purge materialized view log: automatic purge schedule disabled because NEXT expression evaluated to NULL, updating NEXT_TIME to NULL",
		zap.String("schemaName", schemaName),
		zap.String("tableName", mlogName),
		zap.String("purgeNext", nextExpr),
	)
}

func persistRefreshSuccess(
	kctx context.Context,
	sqlExec sqlexec.SQLExecutor,
	mviewID int64,
	lockedReadTSO uint64,
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
	if persistedReadTSONull || persistedReadTSO != refreshReadTSO {
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
	refreshRows *int64,
	refreshFailedReason *string,
) error {
	firstErr := finalizeRefreshHist(
		kctx,
		sqlExec,
		refreshJobID,
		mviewID,
		refreshStatus,
		refreshReadTSO,
		refreshRows,
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
		refreshRows,
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
	refreshRows *int64,
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
	var refreshRowsArg any
	if refreshRows != nil {
		refreshRowsArg = *refreshRows
	}
	var refreshFailedReasonArg any
	if refreshFailedReason != nil {
		refreshFailedReasonArg = *refreshFailedReason
	}
	updateSQL := `UPDATE mysql.tidb_mview_refresh_hist
SET
	REFRESH_ENDTIME = NOW(6),
	REFRESH_STATUS = %?,
	REFRESH_ROWS = %?,
	REFRESH_READ_TSO = %?,
	REFRESH_FAILED_REASON = %?
WHERE REFRESH_JOB_ID = %?
  AND MVIEW_ID = %?`
	if _, err := sqlExec.ExecuteInternal(
		kctx,
		updateSQL,
		refreshStatus,
		refreshRowsArg,
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
