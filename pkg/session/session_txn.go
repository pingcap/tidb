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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package session

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"bytes"
	stderrs "errors"
	"encoding/hex"
	"slices"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/privilege"
	session_metrics "github.com/pingcap/tidb/pkg/session/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	storeerr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/logutil/consistency"
	"github.com/pingcap/tidb/pkg/util/redact"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/traceevent"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"github.com/tikv/client-go/v2/oracle"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	tikvutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

func (s *session) doCommit(ctx context.Context) error {
	if !s.txn.Valid() {
		return nil
	}

	defer func() {
		s.txn.changeToInvalid()
		s.sessionVars.SetInTxn(false)
		s.sessionVars.ClearDiskFullOpt()
	}()
	// check if the transaction is read-only
	if s.txn.IsReadOnly() {
		return nil
	}
	// check if the cluster is read-only
	if !s.sessionVars.InRestrictedSQL && (vardef.RestrictedReadOnly.Load() || vardef.VarTiDBSuperReadOnly.Load()) {
		// It is not internal SQL, and the cluster has one of RestrictedReadOnly or SuperReadOnly
		// We need to privilege check again: a privilege check occurred during planning, but we need
		// to prevent the case that a long running auto-commit statement is now trying to commit.
		pm := privilege.GetPrivilegeManager(s)
		roles := s.sessionVars.ActiveRoles
		if pm != nil && !pm.HasExplicitlyGrantedDynamicPrivilege(roles, "RESTRICTED_REPLICA_WRITER_ADMIN", false) {
			s.RollbackTxn(ctx)
			return plannererrors.ErrSQLInReadOnlyMode
		}
	}
	err := s.checkPlacementPolicyBeforeCommit(ctx)
	if err != nil {
		return err
	}
	// mockCommitError and mockGetTSErrorInRetry use to test PR #8743.
	failpoint.Inject("mockCommitError", func(val failpoint.Value) {
		if val.(bool) {
			if _, err := failpoint.Eval("tikvclient/mockCommitErrorOpt"); err == nil {
				failpoint.Return(kv.ErrTxnRetryable)
			}
		}
	})

	sessVars := s.GetSessionVars()

	var commitTSChecker func(uint64) bool
	if tables := sessVars.TxnCtx.CachedTables; len(tables) > 0 {
		c := cachedTableRenewLease{tables: tables}
		now := time.Now()
		err := c.start(ctx)
		defer c.stop(ctx)
		sessVars.StmtCtx.WaitLockLeaseTime += time.Since(now)
		if err != nil {
			return errors.Trace(err)
		}
		commitTSChecker = c.commitTSCheck
	}
	if err = sessiontxn.GetTxnManager(s).SetOptionsBeforeCommit(s.txn.Transaction, commitTSChecker); err != nil {
		return err
	}

	err = s.commitTxnWithTemporaryData(tikvutil.SetSessionID(ctx, sessVars.ConnectionID), &s.txn)
	if err != nil {
		err = s.handleAssertionFailure(ctx, err)
	}
	return err
}

type cachedTableRenewLease struct {
	tables map[int64]any
	lease  []uint64 // Lease for each visited cached tables.
	exit   chan struct{}
}

func (c *cachedTableRenewLease) start(ctx context.Context) error {
	c.exit = make(chan struct{})
	c.lease = make([]uint64, len(c.tables))
	wg := make(chan error, len(c.tables))
	ith := 0
	for _, raw := range c.tables {
		tbl := raw.(table.CachedTable)
		go tbl.WriteLockAndKeepAlive(ctx, c.exit, &c.lease[ith], wg)
		ith++
	}

	// Wait for all LockForWrite() return, this function can return.
	var err error
	for ; ith > 0; ith-- {
		tmp := <-wg
		if tmp != nil {
			err = tmp
		}
	}
	return err
}

func (c *cachedTableRenewLease) stop(_ context.Context) {
	close(c.exit)
}

func (c *cachedTableRenewLease) commitTSCheck(commitTS uint64) bool {
	for i := range c.lease {
		lease := atomic.LoadUint64(&c.lease[i])
		if commitTS >= lease {
			// Txn fails to commit because the write lease is expired.
			return false
		}
	}
	return true
}

// handleAssertionFailure extracts the possible underlying assertionFailed error,
// gets the corresponding MVCC history and logs it.
// If it's not an assertion failure, returns the original error.
func (s *session) handleAssertionFailure(ctx context.Context, err error) error {
	var assertionFailure *tikverr.ErrAssertionFailed
	if !stderrs.As(err, &assertionFailure) {
		return err
	}
	key := assertionFailure.Key
	newErr := kv.ErrAssertionFailed.GenWithStackByArgs(
		hex.EncodeToString(key), assertionFailure.Assertion.String(), assertionFailure.StartTs,
		assertionFailure.ExistingStartTs, assertionFailure.ExistingCommitTs,
	)

	rmode := s.GetSessionVars().EnableRedactLog
	if rmode == errors.RedactLogEnable {
		return newErr
	}

	var decodeFunc func(kv.Key, *kvrpcpb.MvccGetByKeyResponse, map[string]any)
	// if it's a record key or an index key, decode it
	if infoSchema, ok := s.sessionVars.TxnCtx.InfoSchema.(infoschema.InfoSchema); ok &&
		infoSchema != nil && (tablecodec.IsRecordKey(key) || tablecodec.IsIndexKey(key)) {
		tableOrPartitionID := tablecodec.DecodeTableID(key)
		tbl, ok := infoSchema.TableByID(ctx, tableOrPartitionID)
		if !ok {
			tbl, _, _ = infoSchema.FindTableByPartitionID(tableOrPartitionID)
		}
		if tbl == nil {
			logutil.Logger(ctx).Warn("cannot find table by id", zap.Int64("tableID", tableOrPartitionID), zap.String("key", hex.EncodeToString(key)))
			return newErr
		}

		if tablecodec.IsRecordKey(key) {
			decodeFunc = consistency.DecodeRowMvccData(tbl.Meta())
		} else {
			tableInfo := tbl.Meta()
			_, indexID, _, e := tablecodec.DecodeIndexKey(key)
			if e != nil {
				logutil.Logger(ctx).Error("assertion failed but cannot decode index key", zap.Error(e))
				return newErr
			}
			var indexInfo *model.IndexInfo
			for _, idx := range tableInfo.Indices {
				if idx.ID == indexID {
					indexInfo = idx
					break
				}
			}
			if indexInfo == nil {
				return newErr
			}
			decodeFunc = consistency.DecodeIndexMvccData(indexInfo)
		}
	}
	if store, ok := s.store.(helper.Storage); ok {
		content := consistency.GetMvccByKey(store, key, decodeFunc)
		logutil.Logger(ctx).Error("assertion failed", zap.String("message", newErr.Error()), zap.String("mvcc history", redact.String(rmode, content)))
	}
	return newErr
}

func (s *session) commitTxnWithTemporaryData(ctx context.Context, txn kv.Transaction) error {
	sessVars := s.sessionVars
	txnTempTables := sessVars.TxnCtx.TemporaryTables
	if len(txnTempTables) == 0 {
		failpoint.Inject("mockSleepBeforeTxnCommit", func(v failpoint.Value) {
			ms := v.(int)
			time.Sleep(time.Millisecond * time.Duration(ms))
		})
		return txn.Commit(ctx)
	}

	sessionData := sessVars.TemporaryTableData
	var (
		stage           kv.StagingHandle
		localTempTables *infoschema.SessionTables
	)

	if sessVars.LocalTemporaryTables != nil {
		localTempTables = sessVars.LocalTemporaryTables.(*infoschema.SessionTables)
	} else {
		localTempTables = new(infoschema.SessionTables)
	}

	defer func() {
		// stage != kv.InvalidStagingHandle means error occurs, we need to cleanup sessionData
		if stage != kv.InvalidStagingHandle {
			sessionData.Cleanup(stage)
		}
	}()

	for tblID, tbl := range txnTempTables {
		if !tbl.GetModified() {
			continue
		}

		if tbl.GetMeta().TempTableType != model.TempTableLocal {
			continue
		}
		if _, ok := localTempTables.TableByID(tblID); !ok {
			continue
		}

		if stage == kv.InvalidStagingHandle {
			stage = sessionData.Staging()
		}

		tblPrefix := tablecodec.EncodeTablePrefix(tblID)
		endKey := tablecodec.EncodeTablePrefix(tblID + 1)

		txnMemBuffer := s.txn.GetMemBuffer()
		iter, err := txnMemBuffer.Iter(tblPrefix, endKey)
		if err != nil {
			return err
		}

		for iter.Valid() {
			key := iter.Key()
			if !bytes.HasPrefix(key, tblPrefix) {
				break
			}

			value := iter.Value()
			if len(value) == 0 {
				err = sessionData.DeleteTableKey(tblID, key)
			} else {
				err = sessionData.SetTableKey(tblID, key, iter.Value())
			}

			if err != nil {
				return err
			}

			err = iter.Next()
			if err != nil {
				return err
			}
		}
	}

	err := txn.Commit(ctx)
	if err != nil {
		return err
	}

	if stage != kv.InvalidStagingHandle {
		sessionData.Release(stage)
		stage = kv.InvalidStagingHandle
	}

	return nil
}

// errIsNoisy is used to filter DUPLICATE KEY errors.
// These can observed by users in INFORMATION_SCHEMA.CLIENT_ERRORS_SUMMARY_GLOBAL instead.
//
// The rationale for filtering these errors is because they are "client generated errors". i.e.
// of the errors defined in kv/error.go, these look to be clearly related to a client-inflicted issue,
// and the server is only responsible for handling the error correctly. It does not need to log.
func errIsNoisy(err error) bool {
	if kv.ErrKeyExists.Equal(err) {
		return true
	}
	if storeerr.ErrLockAcquireFailAndNoWaitSet.Equal(err) {
		return true
	}
	return false
}

func (s *session) doCommitWithRetry(ctx context.Context) error {
	defer func() {
		s.GetSessionVars().SetTxnIsolationLevelOneShotStateForNextTxn()
		s.txn.changeToInvalid()
		s.cleanRetryInfo()
		sessiontxn.GetTxnManager(s).OnTxnEnd()
	}()
	if !s.txn.Valid() {
		// If the transaction is invalid, maybe it has already been rolled back by the client.
		return nil
	}
	isInternalTxn := false
	if internal := s.txn.GetOption(kv.RequestSourceInternal); internal != nil && internal.(bool) {
		isInternalTxn = true
	}
	var err error
	txnSize := s.txn.Size()
	isPessimistic := s.sessionVars.TxnCtx.IsPessimistic
	isPipelined := s.txn.IsPipelined()
	r, ctx := tracing.StartRegionEx(ctx, "session.doCommitWithRetry")
	defer r.End()

	// Emit txn.commit.start trace event
	startTS := s.sessionVars.TxnCtx.StartTS
	if traceevent.IsEnabled(traceevent.TxnLifecycle) {
		traceevent.TraceEvent(ctx, traceevent.TxnLifecycle, "txn.commit.start",
			zap.Uint64("start_ts", startTS),
			zap.Bool("pessimistic", isPessimistic),
			zap.Bool("pipelined", isPipelined),
			zap.Int("txn_size", txnSize),
			zap.Uint64("conn_id", s.sessionVars.ConnectionID),
		)
	}

	// Defer txn.commit.finish to capture final result
	defer func() {
		if traceevent.IsEnabled(traceevent.TxnLifecycle) {
			fields := []zap.Field{
				zap.Uint64("start_ts", startTS),
				zap.Bool("pessimistic", isPessimistic),
				zap.Bool("pipelined", isPipelined),
				zap.Uint64("conn_id", s.sessionVars.ConnectionID),
			}
			if s.txn.lastCommitTS > 0 {
				fields = append(fields, zap.Uint64("commit_ts", s.txn.lastCommitTS))
			}
			if err != nil {
				fields = append(fields, zap.Error(err))
			}
			traceevent.TraceEvent(ctx, traceevent.TxnLifecycle, "txn.commit.finish", fields...)
		}
	}()

	err = s.doCommit(ctx)
	if err != nil {
		// polish the Write Conflict error message
		newErr := s.tryReplaceWriteConflictError(ctx, err)
		if newErr != nil {
			err = newErr
		}

		commitRetryLimit := s.sessionVars.RetryLimit
		if !s.sessionVars.TxnCtx.CouldRetry {
			commitRetryLimit = 0
		}
		// Don't retry in BatchInsert mode. As a counter-example, insert into t1 select * from t2,
		// BatchInsert already commit the first batch 1000 rows, then it commit 1000-2000 and retry the statement,
		// Finally t1 will have more data than t2, with no errors return to user!
		if s.isTxnRetryableError(err) && !s.sessionVars.BatchInsert && commitRetryLimit > 0 && !isPessimistic && !isPipelined {
			logutil.Logger(ctx).Warn("sql",
				zap.String("label", s.GetSQLLabel()),
				zap.Error(err),
				zap.String("txn", s.txn.GoString()))
			// Transactions will retry 2 ~ commitRetryLimit times.
			// We make larger transactions retry less times to prevent cluster resource outage.
			txnSizeRate := float64(txnSize) / float64(kv.TxnTotalSizeLimit.Load())
			maxRetryCount := commitRetryLimit - int64(float64(commitRetryLimit-1)*txnSizeRate)
			err = s.retry(ctx, uint(maxRetryCount))
		} else if !errIsNoisy(err) {
			logutil.Logger(ctx).Warn("can not retry txn",
				zap.String("label", s.GetSQLLabel()),
				zap.Error(err),
				zap.Bool("IsBatchInsert", s.sessionVars.BatchInsert),
				zap.Bool("IsPessimistic", isPessimistic),
				zap.Bool("InRestrictedSQL", s.sessionVars.InRestrictedSQL),
				zap.Int64("tidb_retry_limit", s.sessionVars.RetryLimit),
				zap.Bool("tidb_disable_txn_auto_retry", s.sessionVars.DisableTxnAutoRetry))
		}
	}
	counter := s.sessionVars.TxnCtx.StatementCount
	duration := time.Since(s.GetSessionVars().TxnCtx.CreateTime).Seconds()
	s.recordOnTransactionExecution(err, counter, duration, isInternalTxn)

	if err != nil {
		if !errIsNoisy(err) {
			logutil.Logger(ctx).Warn("commit failed",
				zap.String("finished txn", s.txn.GoString()),
				zap.Error(err))
		}
		return err
	}
	s.updateStatsDeltaToCollector()
	return nil
}

// adds more information about the table in the error message
// precondition: oldErr is a 9007:WriteConflict Error
func (s *session) tryReplaceWriteConflictError(ctx context.Context, oldErr error) (newErr error) {
	if !kv.ErrWriteConflict.Equal(oldErr) {
		return nil
	}
	if errors.RedactLogEnabled.Load() == errors.RedactLogEnable {
		return nil
	}
	originErr := errors.Cause(oldErr)
	inErr, _ := originErr.(*errors.Error)
	// we don't want to modify the oldErr, so copy the args list
	oldArgs := inErr.Args()
	args := slices.Clone(oldArgs)
	is := sessiontxn.GetTxnManager(s).GetTxnInfoSchema()
	if is == nil {
		return nil
	}
	newKeyTableField, ok := addTableNameInTableIDField(ctx, args[3], is)
	if ok {
		args[3] = newKeyTableField
	}
	newPrimaryKeyTableField, ok := addTableNameInTableIDField(ctx, args[5], is)
	if ok {
		args[5] = newPrimaryKeyTableField
	}
	return kv.ErrWriteConflict.FastGenByArgs(args...)
}

// precondition: is != nil
func addTableNameInTableIDField(ctx context.Context, tableIDField any, is infoschema.InfoSchema) (enhancedMsg string, done bool) {
	keyTableID, ok := tableIDField.(string)
	if !ok {
		return "", false
	}
	stringsInTableIDField := strings.Split(keyTableID, "=")
	if len(stringsInTableIDField) == 0 {
		return "", false
	}
	tableIDStr := stringsInTableIDField[len(stringsInTableIDField)-1]
	tableID, err := strconv.ParseInt(tableIDStr, 10, 64)
	if err != nil {
		return "", false
	}
	var tableName string
	tbl, ok := is.TableByID(ctx, tableID)
	if !ok {
		tableName = "unknown"
	} else {
		dbInfo, ok := infoschema.SchemaByTable(is, tbl.Meta())
		if !ok {
			tableName = "unknown." + tbl.Meta().Name.String()
		} else {
			tableName = dbInfo.Name.String() + "." + tbl.Meta().Name.String()
		}
	}
	enhancedMsg = keyTableID + ", tableName=" + tableName
	return enhancedMsg, true
}

func (s *session) updateStatsDeltaToCollector() {
	mapper := s.GetSessionVars().TxnCtx.TableDeltaMap
	if s.statsCollector != nil && mapper != nil {
		for tableID, item := range mapper {
			if tableID > 0 {
				s.statsCollector.Update(tableID, item.Delta, item.Count)
			}
		}
	}
}

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

// PrepareTxnCtx begins a transaction, and creates a new transaction context.
// When stmt is provided, it determines transaction mode based on the statement.
// When stmt is nil, it uses the session's default transaction mode.
func (s *session) PrepareTxnCtx(ctx context.Context, stmt ast.StmtNode) error {
	s.currentCtx = ctx
	if s.txn.validOrPending() {
		return nil
	}

	txnMode := s.decideTxnMode(stmt)

	return sessiontxn.GetTxnManager(s).EnterNewTxn(ctx, &sessiontxn.EnterNewTxnRequest{
		Type:    sessiontxn.EnterNewTxnBeforeStmt,
		TxnMode: txnMode,
	})
}

// decideTxnMode determines whether to use pessimistic or optimistic transaction mode
// based on the current session state, configuration, and the statement being executed.
// When stmt is nil, it uses the session's default transaction mode.
func (s *session) decideTxnMode(stmt ast.StmtNode) string {
	if s.sessionVars.RetryInfo.Retrying {
		return ast.Pessimistic
	}

	if s.sessionVars.TxnMode != ast.Pessimistic {
		return ast.Optimistic
	}

	if !s.sessionVars.IsAutocommit() {
		return s.sessionVars.TxnMode
	}

	if stmt != nil && s.shouldUsePessimisticAutoCommit(stmt) {
		return ast.Pessimistic
	}

	return ast.Optimistic
}

// shouldUsePessimisticAutoCommit checks if pessimistic-auto-commit should be applied
// for the current statement.
func (s *session) shouldUsePessimisticAutoCommit(stmtNode ast.StmtNode) bool {
	// Check if pessimistic-auto-commit is enabled globally
	if !config.GetGlobalConfig().PessimisticTxn.PessimisticAutoCommit.Load() {
		return false
	}

	// Disabled for bulk DML operations
	if s.GetSessionVars().BulkDMLEnabled {
		return false
	}

	if s.isInternal() {
		return false
	}

	// Use direct AST inspection to determine if this is a DML statement
	return s.isDMLStatement(stmtNode)
}

// isDMLStatement checks if the given statement should use pessimistic-auto-commit.
// It handles EXECUTE unwrapping and properly handles EXPLAIN statements by checking their inner statement.
func (s *session) isDMLStatement(stmtNode ast.StmtNode) bool {
	if stmtNode == nil {
		return false
	}

	// Handle EXECUTE statements - unwrap to get the actual prepared statement
	actualStmt := stmtNode
	if execStmt, ok := stmtNode.(*ast.ExecuteStmt); ok {
		prepareStmt, err := plannercore.GetPreparedStmt(execStmt, s.GetSessionVars())
		if err != nil || prepareStmt == nil {
			return false
		}
		actualStmt = prepareStmt.PreparedAst.Stmt
	}

	// For EXPLAIN statements, check the underlying statement
	// This ensures EXPLAIN shows the correct plan that would be used if the statement were executed
	if explainStmt, ok := actualStmt.(*ast.ExplainStmt); ok {
		return s.isDMLStatement(explainStmt.Stmt)
	}

	// Only these DML statements should use pessimistic-auto-commit
	// Note: LOAD DATA and IMPORT are intentionally excluded
	switch actualStmt.(type) {
	case *ast.InsertStmt, *ast.UpdateStmt, *ast.DeleteStmt:
		return true
	default:
		return false
	}
}

// PrepareTSFuture uses to try to get ts future.
func (s *session) PrepareTSFuture(ctx context.Context, future oracle.Future, scope string) error {
	if s.txn.Valid() {
		return errors.New("cannot prepare ts future when txn is valid")
	}

	failpoint.Inject("assertTSONotRequest", func() {
		if _, ok := future.(sessiontxn.ConstantFuture); !ok && !s.isInternal() {
			panic("tso shouldn't be requested")
		}
	})

	failpoint.InjectContext(ctx, "mockGetTSFail", func() {
		future = txnFailFuture{}
	})

	s.txn.changeToPending(&txnFuture{
		future:                          future,
		store:                           s.store,
		txnScope:                        scope,
		pipelined:                       s.usePipelinedDmlOrWarn(ctx),
		pipelinedFlushConcurrency:       s.GetSessionVars().PipelinedFlushConcurrency,
		pipelinedResolveLockConcurrency: s.GetSessionVars().PipelinedResolveLockConcurrency,
		pipelinedWriteThrottleRatio:     s.GetSessionVars().PipelinedWriteThrottleRatio,
	})
	return nil
}

// GetPreparedTxnFuture returns the TxnFuture if it is valid or pending.
// It returns nil otherwise.
func (s *session) GetPreparedTxnFuture() sessionctx.TxnFuture {
	if !s.txn.validOrPending() {
		return nil
	}
	return &s.txn
}

// RefreshTxnCtx implements context.RefreshTxnCtx interface.
func (s *session) RefreshTxnCtx(ctx context.Context) error {
	var commitDetail *tikvutil.CommitDetails
	ctx = context.WithValue(ctx, tikvutil.CommitDetailCtxKey, &commitDetail)
	err := s.doCommit(ctx)
	if commitDetail != nil {
		s.GetSessionVars().StmtCtx.MergeExecDetails(commitDetail)
	}
	if err != nil {
		return err
	}

	s.updateStatsDeltaToCollector()

	return sessiontxn.NewTxn(ctx, s)
}

// GetStore gets the store of session.
func (s *session) GetStore() kv.Storage {
	return s.store
}

// usePipelinedDmlOrWarn returns the current statement can be executed as a pipelined DML.
func (s *session) usePipelinedDmlOrWarn(ctx context.Context) bool {
	if !s.sessionVars.BulkDMLEnabled {
		return false
	}
	stmtCtx := s.sessionVars.StmtCtx
	if stmtCtx == nil {
		return false
	}
	if stmtCtx.IsReadOnly {
		return false
	}
	vars := s.GetSessionVars()
	if !vars.TxnCtx.EnableMDL {
		stmtCtx.AppendWarning(
			errors.New(
				"Pipelined DML can not be used without Metadata Lock. Fallback to standard mode",
			),
		)
		return false
	}
	if (vars.BatchCommit || vars.BatchInsert || vars.BatchDelete) && vars.DMLBatchSize > 0 && vardef.EnableBatchDML.Load() {
		stmtCtx.AppendWarning(errors.New("Pipelined DML can not be used with the deprecated Batch DML. Fallback to standard mode"))
		return false
	}
	if !(stmtCtx.InInsertStmt || stmtCtx.InDeleteStmt || stmtCtx.InUpdateStmt) {
		if !stmtCtx.IsReadOnly {
			stmtCtx.AppendWarning(errors.New("Pipelined DML can only be used for auto-commit INSERT, REPLACE, UPDATE or DELETE. Fallback to standard mode"))
		}
		return false
	}
	if s.isInternal() {
		stmtCtx.AppendWarning(errors.New("Pipelined DML can not be used for internal SQL. Fallback to standard mode"))
		return false
	}
	if vars.InTxn() {
		stmtCtx.AppendWarning(errors.New("Pipelined DML can not be used in transaction. Fallback to standard mode"))
		return false
	}
	if !vars.IsAutocommit() {
		stmtCtx.AppendWarning(errors.New("Pipelined DML can only be used in autocommit mode. Fallback to standard mode"))
		return false
	}
	if s.GetSessionVars().ConstraintCheckInPlace {
		// we enforce that pipelined DML must lazily check key.
		stmtCtx.AppendWarning(
			errors.New(
				"Pipelined DML can not be used when tidb_constraint_check_in_place=ON. " +
					"Fallback to standard mode",
			),
		)
		return false
	}
	is, ok := s.GetLatestInfoSchema().(infoschema.InfoSchema)
	if !ok {
		stmtCtx.AppendWarning(errors.New("Pipelined DML failed to get latest InfoSchema. Fallback to standard mode"))
		return false
	}
	for _, t := range stmtCtx.Tables {
		// get table schema from current infoschema
		tbl, err := is.TableByName(ctx, ast.NewCIStr(t.DB), ast.NewCIStr(t.Table))
		if err != nil {
			stmtCtx.AppendWarning(errors.New("Pipelined DML failed to get table schema. Fallback to standard mode"))
			return false
		}
		if tbl.Meta().IsView() {
			stmtCtx.AppendWarning(errors.New("Pipelined DML can not be used on view. Fallback to standard mode"))
			return false
		}
		if tbl.Meta().IsSequence() {
			stmtCtx.AppendWarning(errors.New("Pipelined DML can not be used on sequence. Fallback to standard mode"))
			return false
		}
		if vars.ForeignKeyChecks && (len(tbl.Meta().ForeignKeys) > 0 || len(is.GetTableReferredForeignKeys(t.DB, t.Table)) > 0) {
			stmtCtx.AppendWarning(
				errors.New(
					"Pipelined DML can not be used on table with foreign keys when foreign_key_checks = ON. Fallback to standard mode",
				),
			)
			return false
		}
		if tbl.Meta().TempTableType != model.TempTableNone {
			stmtCtx.AppendWarning(
				errors.New(
					"Pipelined DML can not be used on temporary tables. " +
						"Fallback to standard mode",
				),
			)
			return false
		}
		if tbl.Meta().TableCacheStatusType != model.TableCacheStatusDisable {
			stmtCtx.AppendWarning(
				errors.New(
					"Pipelined DML can not be used on cached tables. " +
						"Fallback to standard mode",
				),
			)
			return false
		}
	}

	// tidb_dml_type=bulk will invalidate the config pessimistic-auto-commit.
	// The behavior is as if the config is set to false. But we generate a warning for it.
	if config.GetGlobalConfig().PessimisticTxn.PessimisticAutoCommit.Load() {
		stmtCtx.AppendWarning(
			errors.New(
				"pessimistic-auto-commit config is ignored in favor of Pipelined DML",
			),
		)
	}
	return true
}

// GetDBNames gets the sql layer database names from the session.
