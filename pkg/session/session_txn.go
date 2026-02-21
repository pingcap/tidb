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
	"bytes"
	"context"
	"encoding/hex"
	stderrs "errors"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	storeerr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/logutil/consistency"
	"github.com/pingcap/tidb/pkg/util/redact"
	"github.com/pingcap/tidb/pkg/util/traceevent"
	"github.com/pingcap/tidb/pkg/util/tracing"
	tikverr "github.com/tikv/client-go/v2/error"
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

