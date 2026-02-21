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

package server

import (
	"context"
	"fmt"
	"io"
	"runtime/trace"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/plugin"
	servererr "github.com/pingcap/tidb/pkg/server/err"
	"github.com/pingcap/tidb/pkg/server/internal/resultset"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	storeerr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/tablecodec"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/topsql"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"go.uber.org/zap"
)

// handleLoadStats does the additional work after processing the 'load stats' query.
// It sends client a file path, then reads the file content from client, loads it into the storage.
func (cc *clientConn) handleLoadStats(ctx context.Context, loadStatsInfo *executor.LoadStatsInfo) error {
	// If the server handles the load data request, the client has to set the ClientLocalFiles capability.
	if cc.capability&mysql.ClientLocalFiles == 0 {
		return servererr.ErrNotAllowedCommand
	}
	if loadStatsInfo == nil {
		return errors.New("load stats: info is empty")
	}
	data, err := cc.getDataFromPath(ctx, loadStatsInfo.Path)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	return loadStatsInfo.Update(data)
}

func (cc *clientConn) handlePlanReplayerLoad(ctx context.Context, planReplayerLoadInfo *executor.PlanReplayerLoadInfo) error {
	if cc.capability&mysql.ClientLocalFiles == 0 {
		return servererr.ErrNotAllowedCommand
	}
	if planReplayerLoadInfo == nil {
		return errors.New("plan replayer load: info is empty")
	}
	data, err := cc.getDataFromPath(ctx, planReplayerLoadInfo.Path)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	return planReplayerLoadInfo.Update(data)
}

func (cc *clientConn) handlePlanReplayerDump(ctx context.Context, e *executor.PlanReplayerDumpInfo) error {
	if cc.capability&mysql.ClientLocalFiles == 0 {
		return servererr.ErrNotAllowedCommand
	}
	if e == nil {
		return errors.New("plan replayer dump: executor is empty")
	}
	data, err := cc.getDataFromPath(ctx, e.Path)
	if err != nil {
		logutil.BgLogger().Error(err.Error())
		return err
	}
	if len(data) == 0 {
		return nil
	}
	return e.DumpSQLsFromFile(ctx, data)
}

func (cc *clientConn) audit(ctx context.Context, eventType plugin.GeneralEvent) {
	err := plugin.ForeachPlugin(plugin.Audit, func(p *plugin.Plugin) error {
		audit := plugin.DeclareAuditManifest(p.Manifest)
		if audit.OnGeneralEvent != nil {
			cmd := mysql.Command2Str[byte(atomic.LoadUint32(&cc.ctx.GetSessionVars().CommandValue))]
			ctx := context.WithValue(ctx, plugin.ExecStartTimeCtxKey, cc.ctx.GetSessionVars().StartTime)
			audit.OnGeneralEvent(ctx, cc.ctx.GetSessionVars(), eventType, cmd)
		}
		return nil
	})
	if err != nil {
		terror.Log(err)
	}
}

// handleQuery executes the sql query string and writes result set or result ok to the client.
// As the execution time of this function represents the performance of TiDB, we do time log and metrics here.
// Some special queries like `load data` that does not return result, which is handled in handleFileTransInConn.
func (cc *clientConn) handleQuery(ctx context.Context, sql string) (err error) {
	defer trace.StartRegion(ctx, "handleQuery").End()
	sessVars := cc.ctx.GetSessionVars()
	sc := sessVars.StmtCtx
	prevWarns := sc.GetWarnings()
	var stmts []ast.StmtNode
	cc.ctx.GetSessionVars().SetAlloc(cc.chunkAlloc)

	warnCountBeforeParse := len(sc.GetWarnings())
	if stmts, err = cc.ctx.Parse(ctx, sql); err != nil {
		cc.onExtensionSQLParseFailed(sql, err)

		// If an error happened, we'll need to remove the warnings in previous execution because the `ResetContextOfStmt` will not be called.
		// Ref https://github.com/pingcap/tidb/issues/59132
		sc.SetWarnings(sc.GetWarnings()[warnCountBeforeParse:])
		return err
	}

	if len(stmts) == 0 {
		return cc.writeOK(ctx)
	}

	warns := sc.GetWarnings()
	parserWarns := warns[len(prevWarns):]

	var pointPlans []base.Plan
	cc.ctx.GetSessionVars().InMultiStmts = false
	if len(stmts) > 1 {
		// The client gets to choose if it allows multi-statements, and
		// probably defaults OFF. This helps prevent against SQL injection attacks
		// by early terminating the first statement, and then running an entirely
		// new statement.

		capabilities := cc.ctx.GetSessionVars().ClientCapability
		if capabilities&mysql.ClientMultiStatements < 1 {
			// The client does not have multi-statement enabled. We now need to determine
			// how to handle an unsafe situation based on the multiStmt sysvar.
			switch cc.ctx.GetSessionVars().MultiStatementMode {
			case variable.OffInt:
				err = servererr.ErrMultiStatementDisabled
				return err
			case variable.OnInt:
				// multi statement is fully permitted, do nothing
			default:
				warn := contextutil.SQLWarn{Level: contextutil.WarnLevelWarning, Err: servererr.ErrMultiStatementDisabled}
				parserWarns = append(parserWarns, warn)
			}
		}
		cc.ctx.GetSessionVars().InMultiStmts = true

		// Only pre-build point plans for multi-statement query
		pointPlans, err = cc.prefetchPointPlanKeys(ctx, stmts, sql)
		if err != nil {
			for _, stmt := range stmts {
				cc.onExtensionStmtEnd(stmt, false, err)
			}
			return err
		}
		metrics.NumOfMultiQueryHistogram.Observe(float64(len(stmts)))
	}
	if len(pointPlans) > 0 {
		defer cc.ctx.ClearValue(plannercore.PointPlanKey)
	}
	var retryable bool
	var lastStmt ast.StmtNode
	var expiredStmtTaskID uint64
	for i, stmt := range stmts {
		if lastStmt != nil {
			cc.onExtensionStmtEnd(lastStmt, true, nil)
		}
		lastStmt = stmt

		// expiredTaskID is the task ID of the previous statement. When executing a stmt,
		// the StmtCtx will be reinit and the TaskID will change. We can compare the StmtCtx.TaskID
		// with the previous one to determine whether StmtCtx has been inited for the current stmt.
		expiredStmtTaskID = sessVars.StmtCtx.TaskID

		if len(pointPlans) > 0 {
			// Save the point plan in Session, so we don't need to build the point plan again.
			cc.ctx.SetValue(plannercore.PointPlanKey, plannercore.PointPlanVal{Plan: pointPlans[i]})
		}
		retryable, err = cc.handleStmt(ctx, stmt, parserWarns, i == len(stmts)-1)
		if err != nil {
			action, txnErr := sessiontxn.GetTxnManager(&cc.ctx).OnStmtErrorForNextAction(ctx, sessiontxn.StmtErrAfterQuery, err)
			if txnErr != nil {
				err = txnErr
				break
			}

			if retryable && action == sessiontxn.StmtActionRetryReady {
				cc.ctx.GetSessionVars().RetryInfo.Retrying = true
				_, err = cc.handleStmt(ctx, stmt, parserWarns, i == len(stmts)-1)
				cc.ctx.GetSessionVars().RetryInfo.Retrying = false
				if err != nil {
					break
				}
				continue
			}
			if !retryable || !errors.ErrorEqual(err, storeerr.ErrTiFlashServerTimeout) {
				break
			}
			_, allowTiFlashFallback := cc.ctx.GetSessionVars().AllowFallbackToTiKV[kv.TiFlash]
			if !allowTiFlashFallback {
				break
			}
			// When the TiFlash server seems down, we append a warning to remind the user to check the status of the TiFlash
			// server and fallback to TiKV.
			warns := append(parserWarns, contextutil.SQLWarn{Level: contextutil.WarnLevelError, Err: err})
			delete(cc.ctx.GetSessionVars().IsolationReadEngines, kv.TiFlash)
			_, err = cc.handleStmt(ctx, stmt, warns, i == len(stmts)-1)
			cc.ctx.GetSessionVars().IsolationReadEngines[kv.TiFlash] = struct{}{}
			if err != nil {
				break
			}
		}
	}

	if lastStmt != nil {
		cc.onExtensionStmtEnd(lastStmt, sessVars.StmtCtx.TaskID != expiredStmtTaskID, err)
	}

	return err
}

// prefetchPointPlanKeys extracts the point keys in multi-statement query,
// use BatchGet to get the keys, so the values will be cached in the snapshot cache, save RPC call cost.
// For pessimistic transaction, the keys will be batch locked.
func (cc *clientConn) prefetchPointPlanKeys(ctx context.Context, stmts []ast.StmtNode, sqls string) ([]base.Plan, error) {
	txn, err := cc.ctx.Txn(false)
	if err != nil {
		return nil, err
	}
	if !txn.Valid() {
		// Only prefetch in-transaction query for simplicity.
		// Later we can support out-transaction multi-statement query.
		return nil, nil
	}
	vars := cc.ctx.GetSessionVars()
	if vars.TxnCtx.IsPessimistic {
		if vars.IsIsolation(ast.ReadCommitted) {
			// TODO: to support READ-COMMITTED, we need to avoid getting new TS for each statement in the query.
			return nil, nil
		}
		if vars.TxnCtx.GetForUpdateTS() != vars.TxnCtx.StartTS {
			// Do not handle the case that ForUpdateTS is changed for simplicity.
			return nil, nil
		}
	}
	pointPlans := make([]base.Plan, len(stmts))
	var idxKeys []kv.Key //nolint: prealloc
	var rowKeys []kv.Key //nolint: prealloc
	isCommonHandle := make(map[string]bool, 0)

	handlePlan := func(sctx sessionctx.Context, p base.PhysicalPlan, resetStmtCtxFn func()) error {
		var tableID int64
		switch v := p.(type) {
		case *physicalop.PointGetPlan:
			isTableDual, err0 := v.PrunePartitions(sctx)
			if err0 != nil || isTableDual {
				return err0
			}
			tableID = executor.GetPhysID(v.TblInfo, v.PartitionIdx)
			if v.IndexInfo != nil {
				resetStmtCtxFn()
				idxKey, err1 := physicalop.EncodeUniqueIndexKey(cc.getCtx(), v.TblInfo, v.IndexInfo, v.IndexValues, tableID)
				if err1 != nil {
					return err1
				}
				idxKeys = append(idxKeys, idxKey)
				isCommonHandle[string(hack.String(idxKey))] = v.TblInfo.IsCommonHandle
			} else {
				rowKeys = append(rowKeys, tablecodec.EncodeRowKeyWithHandle(tableID, v.Handle))
			}
		case *physicalop.BatchPointGetPlan:
			_, isTableDual, err1 := v.PrunePartitionsAndValues(sctx)
			if err1 != nil {
				return err1
			}
			if isTableDual {
				return nil
			}
			pi := v.TblInfo.GetPartitionInfo()
			getPhysID := func(i int) int64 {
				if pi == nil || i >= len(v.PartitionIdxs) {
					return v.TblInfo.ID
				}
				return executor.GetPhysID(v.TblInfo, &v.PartitionIdxs[i])
			}
			if v.IndexInfo != nil {
				resetStmtCtxFn()
				for i, idxVals := range v.IndexValues {
					idxKey, err1 := physicalop.EncodeUniqueIndexKey(cc.getCtx(), v.TblInfo, v.IndexInfo, idxVals, getPhysID(i))
					if err1 != nil {
						return err1
					}
					idxKeys = append(idxKeys, idxKey)
					isCommonHandle[string(hack.String(idxKey))] = v.TblInfo.IsCommonHandle
				}
			} else {
				for i, handle := range v.Handles {
					rowKeys = append(rowKeys, tablecodec.EncodeRowKeyWithHandle(getPhysID(i), handle))
				}
			}
		}
		return nil
	}

	sc := vars.StmtCtx
	for i, stmt := range stmts {
		if _, ok := stmt.(*ast.UseStmt); ok {
			// If there is a "use db" statement, we shouldn't cache even if it's possible.
			// Consider the scenario where there are statements that could execute on multiple
			// schemas, but the schema is actually different.
			return nil, nil
		}
		// TODO: the preprocess is run twice, we should find some way to avoid do it again.
		nodeW := resolve.NewNodeW(stmt)
		if err = plannercore.Preprocess(ctx, cc.getCtx(), nodeW); err != nil {
			// error might happen, see https://github.com/pingcap/tidb/issues/39664
			return nil, nil
		}
		p := plannercore.TryFastPlan(cc.ctx.Session.GetPlanCtx(), nodeW)
		pointPlans[i] = p
		if p == nil {
			continue
		}
		// Only support Update and Delete for now.
		// TODO: support other point plans.
		switch x := p.(type) {
		case *physicalop.Update:
			//nolint:forcetypeassert
			updateStmt, ok := stmt.(*ast.UpdateStmt)
			if !ok {
				logutil.BgLogger().Warn("unexpected statement type for Update plan",
					zap.String("type", fmt.Sprintf("%T", stmt)))
				continue
			}
			err = handlePlan(cc.ctx.Session, x.SelectPlan, func() {
				executor.ResetUpdateStmtCtx(sc, updateStmt, vars)
			})
			if err != nil {
				return nil, err
			}
		case *physicalop.Delete:
			deleteStmt, ok := stmt.(*ast.DeleteStmt)
			if !ok {
				logutil.BgLogger().Warn("unexpected statement type for Delete plan",
					zap.String("type", fmt.Sprintf("%T", stmt)))
				continue
			}
			err = handlePlan(cc.ctx.Session, x.SelectPlan, func() {
				executor.ResetDeleteStmtCtx(sc, deleteStmt, vars)
			})
			if err != nil {
				return nil, err
			}
		}
	}
	if len(idxKeys) == 0 && len(rowKeys) == 0 {
		return pointPlans, nil
	}
	snapshot := txn.GetSnapshot()
	setResourceGroupTaggerForMultiStmtPrefetch(snapshot, sqls)
	idxVals, err1 := snapshot.BatchGet(ctx, idxKeys)
	if err1 != nil {
		return nil, err1
	}
	for idxKey, idxVal := range idxVals {
		h, err2 := tablecodec.DecodeHandleInIndexValue(idxVal.Value)
		if err2 != nil {
			return nil, err2
		}
		tblID := tablecodec.DecodeTableID(hack.Slice(idxKey))
		rowKeys = append(rowKeys, tablecodec.EncodeRowKeyWithHandle(tblID, h))
	}
	if vars.TxnCtx.IsPessimistic {
		allKeys := append(rowKeys, idxKeys...)
		err = executor.LockKeys(ctx, cc.getCtx(), vars.LockWaitTimeout, allKeys...)
		if err != nil {
			// suppress the lock error, we are not going to handle it here for simplicity.
			err = nil
			logutil.BgLogger().Warn("lock keys error on prefetch", zap.Error(err))
		}
	} else {
		_, err = snapshot.BatchGet(ctx, rowKeys)
		if err != nil {
			return nil, err
		}
	}
	return pointPlans, nil
}

func setResourceGroupTaggerForMultiStmtPrefetch(snapshot kv.Snapshot, sqls string) {
	if !topsqlstate.TopProfilingEnabled() {
		return
	}
	normalized, digest := parser.NormalizeDigest(sqls)
	topsql.AttachAndRegisterSQLInfo(context.Background(), normalized, digest, false)
	if len(normalized) != 0 {
		snapshot.SetOption(kv.ResourceGroupTagger, kv.NewResourceGroupTagBuilder(keyspace.GetKeyspaceNameBytesBySettings()).SetSQLDigest(digest))
	}
}

// The first return value indicates whether the call of handleStmt has no side effect and can be retried.
// Currently, the first return value is used to fall back to TiKV when TiFlash is down.
func (cc *clientConn) handleStmt(
	ctx context.Context, stmt ast.StmtNode,
	warns []contextutil.SQLWarn, lastStmt bool,
) (bool, error) {
	ctx = execdetails.ContextWithInitializedExecDetails(ctx)
	reg := trace.StartRegion(ctx, "ExecuteStmt")
	cc.audit(context.Background(), plugin.Starting)

	// if stmt is load data stmt, store the channel that reads from the conn
	// into the ctx for executor to use
	if s, ok := stmt.(*ast.LoadDataStmt); ok {
		if s.FileLocRef == ast.FileLocClient {
			err := cc.preprocessLoadDataLocal(ctx)
			defer cc.postprocessLoadDataLocal()
			if err != nil {
				return false, err
			}
		}
	}

	rs, err := cc.ctx.ExecuteStmt(ctx, stmt)
	reg.End()
	// - If rs is not nil, the statement tracker detachment from session tracker
	//   is done in the `rs.Close` in most cases.
	// - If the rs is nil and err is not nil, the detachment will be done in
	//   the `handleNoDelay`.
	if rs != nil {
		defer rs.Close()
	}

	if err != nil {
		// If error is returned during the planner phase or the executor.Open
		// phase, the rs will be nil, and StmtCtx.MemTracker StmtCtx.DiskTracker
		// will not be detached. We need to detach them manually.
		if sv := cc.ctx.GetSessionVars(); sv != nil && sv.StmtCtx != nil {
			sv.StmtCtx.DetachMemDiskTracker()
		}
		return true, err
	}

	status := cc.ctx.Status()
	if lastStmt {
		cc.ctx.GetSessionVars().StmtCtx.AppendWarnings(warns)
	} else {
		status |= mysql.ServerMoreResultsExists
	}

	if rs != nil {
		if cc.getStatus() == connStatusShutdown {
			return false, exeerrors.ErrQueryInterrupted
		}
		cc.ctx.GetSessionVars().SQLKiller.SetFinishFunc(
			func() {
				//nolint: errcheck
				rs.Finish()
			})
		fn := func() bool {
			if cc.bufReadConn != nil {
				return cc.bufReadConn.IsAlive() != 0
			}
			return true
		}
		cc.ctx.GetSessionVars().SQLKiller.IsConnectionAlive.Store(&fn)
		cc.ctx.GetSessionVars().SQLKiller.InWriteResultSet.Store(true)
		defer cc.ctx.GetSessionVars().SQLKiller.InWriteResultSet.Store(false)
		defer cc.ctx.GetSessionVars().SQLKiller.ClearFinishFunc()
		defer cc.ctx.GetSessionVars().SQLKiller.IsConnectionAlive.Store(nil)
		if retryable, err := cc.writeResultSet(ctx, rs, false, status, 0); err != nil {
			return retryable, err
		}
		return false, nil
	}

	handled, err := cc.handleFileTransInConn(ctx, status)
	if handled {
		if execStmt := cc.ctx.Value(session.ExecStmtVarKey); execStmt != nil {
			//nolint:forcetypeassert
			execStmt.(*executor.ExecStmt).FinishExecuteStmt(0, err, false)
		}
	}
	return false, err
}

// Preprocess LOAD DATA. Load data from a local file requires reading from the connection.
// The function pass a builder to build the connection reader to the context,
// which will be used in LoadDataExec.
func (cc *clientConn) preprocessLoadDataLocal(ctx context.Context) error {
	if cc.capability&mysql.ClientLocalFiles == 0 {
		return servererr.ErrNotAllowedCommand
	}

	wg := &sync.WaitGroup{}
	builderFunc := func(filepath string) (
		io.ReadCloser, error,
	) {
		err := cc.writeReq(ctx, filepath)
		if err != nil {
			return nil, err
		}

		drained := false
		r, w := io.Pipe()

		wg.Add(1)
		go func() {
			defer wg.Done()

			var errOccurred error

			defer func() {
				if errOccurred != nil {
					// Continue reading packets to drain the connection
					for !drained {
						data, err := cc.readPacket()
						if err != nil {
							logutil.Logger(ctx).Error(
								"drain connection failed in load data",
								zap.Error(err),
							)
							break
						}
						if len(data) == 0 {
							drained = true
						}
					}
				}
				err := w.CloseWithError(errOccurred)
				if err != nil {
					logutil.Logger(ctx).Error(
						"close pipe failed in `load data`",
						zap.Error(err),
					)
				}
			}()

			for {
				data, err := cc.readPacket()
				if err != nil {
					errOccurred = err
					return
				}

				if len(data) == 0 {
					drained = true
					return
				}

				// Write all content in `data`
				for len(data) > 0 {
					n, err := w.Write(data)
					if err != nil {
						errOccurred = err
						return
					}
					data = data[n:]
				}
			}
		}()

		return r, nil
	}

	var readerBuilder executor.LoadDataReaderBuilder = executor.LoadDataReaderBuilder{
		Build: builderFunc,
		Wg:    wg,
	}

	cc.ctx.SetValue(executor.LoadDataReaderBuilderKey, readerBuilder)

	return nil
}

func (cc *clientConn) postprocessLoadDataLocal() {
	builder := cc.ctx.Value(executor.LoadDataReaderBuilderKey)
	if builder != nil {
		builder, ok := builder.(executor.LoadDataReaderBuilder)
		if !ok {
			intest.Assert(false, "LoadDataReaderBuilder should be of type executor.LoadDataReaderBuilder")
			return
		}
		builder.Wg.Wait()
		cc.ctx.ClearValue(executor.LoadDataReaderBuilderKey)
	}
}

func (cc *clientConn) handleFileTransInConn(ctx context.Context, status uint16) (bool, error) {
	handled := false

	loadStats := cc.ctx.Value(executor.LoadStatsVarKey)
	if loadStats != nil {
		handled = true
		defer cc.ctx.SetValue(executor.LoadStatsVarKey, nil)
		//nolint:forcetypeassert
		if err := cc.handleLoadStats(ctx, loadStats.(*executor.LoadStatsInfo)); err != nil {
			return handled, err
		}
	}

	planReplayerLoad := cc.ctx.Value(executor.PlanReplayerLoadVarKey)
	if planReplayerLoad != nil {
		handled = true
		defer cc.ctx.SetValue(executor.PlanReplayerLoadVarKey, nil)
		//nolint:forcetypeassert
		if err := cc.handlePlanReplayerLoad(ctx, planReplayerLoad.(*executor.PlanReplayerLoadInfo)); err != nil {
			return handled, err
		}
	}

	planReplayerDump := cc.ctx.Value(executor.PlanReplayerDumpVarKey)
	if planReplayerDump != nil {
		handled = true
		defer cc.ctx.SetValue(executor.PlanReplayerDumpVarKey, nil)
		//nolint:forcetypeassert
		if err := cc.handlePlanReplayerDump(ctx, planReplayerDump.(*executor.PlanReplayerDumpInfo)); err != nil {
			return handled, err
		}
	}
	return handled, cc.writeOkWith(ctx, mysql.OKHeader, true, status)
}

// handleFieldList returns the field list for a table.
// The sql string is composed of a table name and a terminating character \x00.
func (cc *clientConn) handleFieldList(ctx context.Context, sql string) (err error) {
	parts := strings.Split(sql, "\x00")
	columns, err := cc.ctx.FieldList(parts[0])
	if err != nil {
		return err
	}
	data := cc.alloc.AllocWithLen(4, 1024)
	cc.initResultEncoder(ctx)
	defer cc.rsEncoder.Clean()
	for _, column := range columns {
		data = data[0:4]
		data = column.DumpWithDefault(data, cc.rsEncoder)
		if err := cc.writePacket(data); err != nil {
			return err
		}
	}
	if err := cc.writeEOF(ctx, cc.ctx.Status()); err != nil {
		return err
	}
	return cc.flush(ctx)
}

// writeResultSet writes data into a result set and uses rs.Next to get row data back.
// If binary is true, the data would be encoded in BINARY format.
// serverStatus, a flag bit represents server information.
// fetchSize, the desired number of rows to be fetched each time when client uses cursor.
// retryable indicates whether the call of writeResultSet has no side effect and can be retried to correct error. The call
// has side effect in cursor mode or once data has been sent to client. Currently retryable is used to fallback to TiKV when
// TiFlash is down.
func (cc *clientConn) writeResultSet(ctx context.Context, rs resultset.ResultSet, binary bool, serverStatus uint16, fetchSize int) (retryable bool, runErr error) {
	defer func() {
		// close ResultSet when cursor doesn't exist
		r := recover()
		if r == nil {
			return
		}
		recoverdErr, ok := r.(error)
		if !ok || !(exeerrors.ErrMemoryExceedForQuery.Equal(recoverdErr) ||
			exeerrors.ErrMemoryExceedForInstance.Equal(recoverdErr) ||
			exeerrors.ErrQueryInterrupted.Equal(recoverdErr) ||
			exeerrors.ErrMaxExecTimeExceeded.Equal(recoverdErr)) {
			panic(r)
		}
		runErr = recoverdErr
		// TODO(jianzhang.zj: add metrics here)
		logutil.Logger(ctx).Error("write query result panic", zap.Stringer("lastSQL", getLastStmtInConn{cc}), zap.Stack("stack"), zap.Any("recover", r))
	}()
	cc.initResultEncoder(ctx)
	defer cc.rsEncoder.Clean()
	if mysql.HasCursorExistsFlag(serverStatus) {
		crs, ok := rs.(resultset.CursorResultSet)
		if !ok {
			// this branch is actually unreachable
			return false, errors.New("this cursor is not a resultSet")
		}
		if err := cc.writeChunksWithFetchSize(ctx, crs, serverStatus, fetchSize); err != nil {
			return false, err
		}
		return false, cc.flush(ctx)
	}
	if retryable, err := cc.writeChunks(ctx, rs, binary, serverStatus); err != nil {
		return retryable, err
	}

	return false, cc.flush(ctx)
}

