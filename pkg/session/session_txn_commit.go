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

package session

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	session_metrics "github.com/pingcap/tidb/pkg/session/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/redact"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/traceevent"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	tikvutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

func (s *session) CommitTxn(ctx context.Context) error {
	r, ctx := tracing.StartRegionEx(ctx, "session.CommitTxn")
	defer r.End()

	s.setLastTxnInfoBeforeTxnEnd()
	var commitDetail *tikvutil.CommitDetails
	ctx = context.WithValue(ctx, tikvutil.CommitDetailCtxKey, &commitDetail)
	err := s.doCommitWithRetry(ctx)
	if commitDetail != nil {
		s.sessionVars.StmtCtx.MergeExecDetails(commitDetail)
	}

	if err == nil && s.txn.lastCommitTS > 0 {
		// lastCommitTS could be the same, e.g. when the txn is considered readonly
		if s.txn.lastCommitTS < s.sessionVars.LastCommitTS {
			logutil.BgLogger().Error("check lastCommitTS failed",
				zap.Uint64("sessionLastCommitTS", s.sessionVars.LastCommitTS),
				zap.Uint64("txnLastCommitTS", s.txn.lastCommitTS),
				zap.String("sql", redact.String(s.sessionVars.EnableRedactLog, s.sessionVars.StmtCtx.OriginalSQL)),
			)
			return fmt.Errorf("txn commit_ts:%d is before session last_commit_ts:%d",
				s.txn.lastCommitTS, s.sessionVars.LastCommitTS)
		}
		s.sessionVars.LastCommitTS = s.txn.lastCommitTS
	}

	// record the TTLInsertRows in the metric
	metrics.TTLInsertRowsCount.Add(float64(s.sessionVars.TxnCtx.InsertTTLRowsCount))
	metrics.DDLCommitTempIndexWrite(s.sessionVars.ConnectionID)

	failpoint.Inject("keepHistory", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(err)
		}
	})
	s.sessionVars.TxnCtx.Cleanup()
	s.sessionVars.CleanupTxnReadTSIfUsed()
	return err
}

func (s *session) RollbackTxn(ctx context.Context) {
	r, ctx := tracing.StartRegionEx(ctx, "session.RollbackTxn")
	defer r.End()

	// Emit txn.rollback trace event
	if traceevent.IsEnabled(traceevent.TxnLifecycle) {
		startTS := s.sessionVars.TxnCtx.StartTS
		stmtCount := uint64(s.sessionVars.TxnCtx.StatementCount)
		traceevent.TraceEvent(ctx, traceevent.TxnLifecycle, "txn.rollback",
			zap.Uint64("start_ts", startTS),
			zap.Uint64("stmt_count", stmtCount),
		)
	}

	s.setLastTxnInfoBeforeTxnEnd()
	if s.txn.Valid() {
		terror.Log(s.txn.Rollback())
	}
	if ctx.Value(inCloseSession{}) == nil {
		s.cleanRetryInfo()
	}
	s.txn.changeToInvalid()
	s.sessionVars.TxnCtx.Cleanup()
	s.sessionVars.CleanupTxnReadTSIfUsed()
	s.sessionVars.SetInTxn(false)
	sessiontxn.GetTxnManager(s).OnTxnEnd()
	metrics.DDLRollbackTempIndexWrite(s.sessionVars.ConnectionID)
}

// setLastTxnInfoBeforeTxnEnd sets the @@last_txn_info variable before commit/rollback the transaction.
// The `LastTxnInfo` updated with a JSON string that contains start_ts, for_update_ts, etc.
// The `LastTxnInfo` is updated without the `commit_ts` fields because it is unknown
// until the commit is done (or do not need to commit for readonly or a rollback transaction).
// The non-readonly transaction will overwrite the `LastTxnInfo` again after commit to update the `commit_ts` field.
func (s *session) setLastTxnInfoBeforeTxnEnd() {
	txnCtx := s.GetSessionVars().TxnCtx
	if txnCtx.StartTS == 0 {
		// If the txn is not active, for example, executing "SELECT 1", skip setting the last txn info.
		return
	}

	lastTxnInfo, err := json.Marshal(transaction.TxnInfo{
		TxnScope: txnCtx.TxnScope,
		StartTS:  txnCtx.StartTS,
	})
	terror.Log(err)
	s.GetSessionVars().LastTxnInfo = string(lastTxnInfo)
}

func (*session) isTxnRetryableError(err error) bool {
	if atomic.LoadUint32(&SchemaChangedWithoutRetry) == 1 {
		return kv.IsTxnRetryableError(err)
	}
	return kv.IsTxnRetryableError(err) || domain.ErrInfoSchemaChanged.Equal(err)
}

func isEndTxnStmt(stmt ast.StmtNode, vars *variable.SessionVars) (bool, error) {
	switch n := stmt.(type) {
	case *ast.RollbackStmt, *ast.CommitStmt:
		return true, nil
	case *ast.ExecuteStmt:
		ps, err := plannercore.GetPreparedStmt(n, vars)
		if err != nil {
			return false, err
		}
		return isEndTxnStmt(ps.PreparedAst.Stmt, vars)
	}
	return false, nil
}

func (s *session) checkTxnAborted(stmt sqlexec.Statement) error {
	if atomic.LoadUint32(&s.GetSessionVars().TxnCtx.LockExpire) == 0 {
		return nil
	}
	// If the transaction is aborted, the following statements do not need to execute, except `commit` and `rollback`,
	// because they are used to finish the aborted transaction.
	if ok, err := isEndTxnStmt(stmt.(*executor.ExecStmt).StmtNode, s.sessionVars); err == nil && ok {
		return nil
	} else if err != nil {
		return err
	}
	return kv.ErrLockExpire
}

func (s *session) retry(ctx context.Context, maxCnt uint) (err error) {
	var retryCnt uint
	defer func() {
		s.sessionVars.RetryInfo.Retrying = false
		// retryCnt only increments on retryable error, so +1 here.
		if s.sessionVars.InRestrictedSQL {
			session_metrics.TransactionRetryInternal.Observe(float64(retryCnt + 1))
		} else {
			session_metrics.TransactionRetryGeneral.Observe(float64(retryCnt + 1))
		}
		s.sessionVars.SetInTxn(false)
		if err != nil {
			s.RollbackTxn(ctx)
		}
		s.txn.changeToInvalid()
	}()

	connID := s.sessionVars.ConnectionID
	s.sessionVars.RetryInfo.Retrying = true
	if atomic.LoadUint32(&s.sessionVars.TxnCtx.ForUpdate) == 1 {
		err = ErrForUpdateCantRetry.GenWithStackByArgs(connID)
		return err
	}

	nh := GetHistory(s)
	var schemaVersion int64
	sessVars := s.GetSessionVars()
	orgStartTS := sessVars.TxnCtx.StartTS
	label := s.GetSQLLabel()
	for {
		if err = s.PrepareTxnCtx(ctx, nil); err != nil {
			return err
		}
		s.sessionVars.RetryInfo.ResetOffset()
		for i, sr := range nh.history {
			st := sr.st
			s.sessionVars.StmtCtx = sr.stmtCtx
			s.sessionVars.StmtCtx.CTEStorageMap = map[int]*executor.CTEStorages{}
			s.sessionVars.StmtCtx.ResetForRetry()
			s.sessionVars.PlanCacheParams.Reset()
			schemaVersion, err = st.RebuildPlan(ctx)
			if err != nil {
				return err
			}

			if retryCnt == 0 {
				// We do not have to log the query every time.
				// We print the queries at the first try only.
				sql := sqlForLog(st.GetTextToLog(false))
				if sessVars.EnableRedactLog != errors.RedactLogEnable {
					sql += redact.String(sessVars.EnableRedactLog, sessVars.PlanCacheParams.String())
				}
				logutil.Logger(ctx).Warn("retrying",
					zap.Int64("schemaVersion", schemaVersion),
					zap.Uint("retryCnt", retryCnt),
					zap.Int("queryNum", i),
					zap.String("sql", sql))
			} else {
				logutil.Logger(ctx).Warn("retrying",
					zap.Int64("schemaVersion", schemaVersion),
					zap.Uint("retryCnt", retryCnt),
					zap.Int("queryNum", i))
			}
			_, digest := s.sessionVars.StmtCtx.SQLDigest()
			s.txn.onStmtStart(digest.String())
			if err = sessiontxn.GetTxnManager(s).OnStmtStart(ctx, st.GetStmtNode()); err == nil {
				_, err = st.Exec(ctx)
			}
			s.txn.onStmtEnd()
			if err != nil {
				s.StmtRollback(ctx, false)
				break
			}
			s.StmtCommit(ctx)
		}
		logutil.Logger(ctx).Warn("transaction association",
			zap.Uint64("retrying txnStartTS", s.GetSessionVars().TxnCtx.StartTS),
			zap.Uint64("original txnStartTS", orgStartTS))
		failpoint.Inject("preCommitHook", func() {
			hook, ok := ctx.Value("__preCommitHook").(func())
			if ok {
				hook()
			}
		})
		if err == nil {
			err = s.doCommit(ctx)
			if err == nil {
				break
			}
		}
		if !s.isTxnRetryableError(err) {
			logutil.Logger(ctx).Warn("sql",
				zap.String("label", label),
				zap.Stringer("session", s),
				zap.Error(err))
			metrics.SessionRetryErrorCounter.WithLabelValues(label, metrics.LblUnretryable).Inc()
			return err
		}
		retryCnt++
		if retryCnt >= maxCnt {
			logutil.Logger(ctx).Warn("sql",
				zap.String("label", label),
				zap.Uint("retry reached max count", retryCnt))
			metrics.SessionRetryErrorCounter.WithLabelValues(label, metrics.LblReachMax).Inc()
			return err
		}
		logutil.Logger(ctx).Warn("sql",
			zap.String("label", label),
			zap.Error(err),
			zap.String("txn", s.txn.GoString()))
		kv.BackOff(retryCnt)
		s.txn.changeToInvalid()
		s.sessionVars.SetInTxn(false)
	}
	return err
}

func sqlForLog(sql string) string {
	if len(sql) > sqlLogMaxLen {
		sql = sql[:sqlLogMaxLen] + fmt.Sprintf("(len:%d)", len(sql))
	}
	return executor.QueryReplacer.Replace(sql)
}
const quoteCommaQuote = "', '"

// loadCommonGlobalVariablesIfNeeded loads and applies commonly used global variables for the session.
func (s *session) loadCommonGlobalVariablesIfNeeded() error {
	vars := s.sessionVars
	if vars.CommonGlobalLoaded {
		return nil
	}
	if s.Value(sessionctx.Initing) != nil {
		// When running bootstrap or upgrade, we should not access global storage.
		// But we need to init max_allowed_packet to use concat function during bootstrap or upgrade.
		err := vars.SetSystemVar(vardef.MaxAllowedPacket, strconv.FormatUint(vardef.DefMaxAllowedPacket, 10))
		if err != nil {
			logutil.BgLogger().Error("set system variable max_allowed_packet error", zap.Error(err))
		}
		return nil
	}

	vars.CommonGlobalLoaded = true

	// Deep copy sessionvar cache
	sessionCache, err := domain.GetDomain(s).GetSessionCache()
	if err != nil {
		return err
	}
	for varName, varVal := range sessionCache {
		if _, ok := vars.GetSystemVar(varName); !ok {
			err = vars.SetSystemVarWithRelaxedValidation(varName, varVal)
			if err != nil {
				if variable.ErrUnknownSystemVar.Equal(err) {
					continue // sessionCache is stale; sysvar has likely been unregistered
				}
				return err
			}
		}
	}
	// when client set Capability Flags CLIENT_INTERACTIVE, init wait_timeout with interactive_timeout
	if vars.ClientCapability&mysql.ClientInteractive > 0 {
		if varVal, ok := vars.GetSystemVar(vardef.InteractiveTimeout); ok {
			if err := vars.SetSystemVar(vardef.WaitTimeout, varVal); err != nil {
				return err
			}
		}
	}
	return nil
}


// GetDBNames gets the sql layer database names from the session.
