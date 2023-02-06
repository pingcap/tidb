// Copyright 2023 PingCAP, Inc.
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

package sessiontxn

import (
	"bytes"
	"context"
	"fmt"
	"runtime/trace"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/sessiontxn/txninfo"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sli"
	"github.com/pingcap/tidb/util/syncutil"
	"github.com/pingcap/tipb/go-binlog"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

var hasMockAutoIncIDRetry = int64(0)

func enableMockAutoIncIDRetry() {
	atomic.StoreInt64(&hasMockAutoIncIDRetry, 1)
}

func mockAutoIncIDRetry() bool {
	return atomic.LoadInt64(&hasMockAutoIncIDRetry) == 1
}

var mockAutoRandIDRetryCount = int64(0)

func needMockAutoRandIDRetry() bool {
	return atomic.LoadInt64(&mockAutoRandIDRetryCount) > 0
}

func decreaseMockAutoRandIDRetryCount() {
	atomic.AddInt64(&mockAutoRandIDRetryCount, -1)
}

// ResetMockAutoRandIDRetryCount set the number of occurrences of
// `kv.ErrTxnRetryable` when calling TxnState.Commit().
func ResetMockAutoRandIDRetryCount(failTimes int64) {
	atomic.StoreInt64(&mockAutoRandIDRetryCount, failTimes)
}

// LazyTxn wraps kv.Transaction to provide a new kv.Transaction.
// 1. It holds all statement related modification in the buffer before flush to the txn,
// so if execute statement meets error, the txn won't be made dirty.
// 2. It's a lazy transaction, that means it's a txnFuture before StartTS() is really need.
type LazyTxn struct {
	// States of a LazyTxn should be one of the followings:
	// Invalid: kv.Transaction == nil && txnFuture == nil
	// Pending: kv.Transaction == nil && txnFuture != nil
	// Valid:	kv.Transaction != nil && txnFuture == nil
	kv.Transaction
	txnFuture *TxnFuture

	initCnt       int
	stagingHandle kv.StagingHandle
	mutations     map[int64]*binlog.TableMutation
	WriteSLI      sli.TxnWriteThroughputSLI

	enterAggressiveLockingOnValid bool

	// TxnInfo is added for the lock view feature, the data is frequent modified but
	// rarely read (just in query select * from information_schema.tidb_trx).
	// The data in this session would be query by other sessions, so Mutex is necessary.
	// Since read is rare, the reader can copy-on-read to get a data snapshot.
	mu struct {
		syncutil.RWMutex
		txninfo.TxnInfo
	}

	// mark the txn enables lazy uniqueness check in pessimistic transactions.
	lazyUniquenessCheckEnabled bool
}

// GetTableInfo returns the cached index name.
func (txn *LazyTxn) GetTableInfo(id int64) *model.TableInfo {
	return txn.Transaction.GetTableInfo(id)
}

// CacheTableInfo caches the index name.
func (txn *LazyTxn) CacheTableInfo(id int64, info *model.TableInfo) {
	txn.Transaction.CacheTableInfo(id, info)
}

// Init creates the internal TxnInfo object.
func (txn *LazyTxn) Init() {
	txn.mutations = make(map[int64]*binlog.TableMutation)
	txn.mu.Lock()
	defer txn.mu.Unlock()
	txn.mu.TxnInfo = txninfo.TxnInfo{}
}

// call this under lock!
func (txn *LazyTxn) updateState(state txninfo.TxnRunningState) {
	if txn.mu.TxnInfo.State != state {
		lastState := txn.mu.TxnInfo.State
		lastStateChangeTime := txn.mu.TxnInfo.LastStateChangeTime
		txn.mu.TxnInfo.State = state
		txn.mu.TxnInfo.LastStateChangeTime = time.Now()
		if !lastStateChangeTime.IsZero() {
			hasLockLbl := !txn.mu.TxnInfo.BlockStartTime.IsZero()
			txninfo.TxnDurationHistogram(lastState, hasLockLbl).Observe(time.Since(lastStateChangeTime).Seconds())
		}
		txninfo.TxnStatusEnteringCounter(state).Inc()
	}
}

func (txn *LazyTxn) initStmtBuf() {
	if txn.Transaction == nil {
		return
	}
	buf := txn.Transaction.GetMemBuffer()
	txn.initCnt = buf.Len()
	txn.stagingHandle = buf.Staging()
}

// countHint is estimated count of mutations.
func (txn *LazyTxn) countHint() int {
	if txn.stagingHandle == kv.InvalidStagingHandle {
		return 0
	}
	return txn.Transaction.GetMemBuffer().Len() - txn.initCnt
}

func (txn *LazyTxn) flushStmtBuf() {
	if txn.stagingHandle == kv.InvalidStagingHandle {
		return
	}
	buf := txn.Transaction.GetMemBuffer()

	if txn.lazyUniquenessCheckEnabled {
		keysNeedSetPersistentPNE := kv.FindKeysInStage(buf, txn.stagingHandle, func(k kv.Key, flags kv.KeyFlags, v []byte) bool {
			return flags.HasPresumeKeyNotExists()
		})
		for _, key := range keysNeedSetPersistentPNE {
			buf.UpdateFlags(key, kv.SetPreviousPresumeKeyNotExists)
		}
	}

	buf.Release(txn.stagingHandle)
	txn.initCnt = buf.Len()
}

func (txn *LazyTxn) cleanupStmtBuf() {
	if txn.stagingHandle == kv.InvalidStagingHandle {
		return
	}
	buf := txn.Transaction.GetMemBuffer()
	buf.Cleanup(txn.stagingHandle)
	txn.initCnt = buf.Len()

	txn.mu.Lock()
	defer txn.mu.Unlock()
	txn.mu.TxnInfo.EntriesCount = uint64(txn.Transaction.Len())
}

// resetTxnInfo resets the transaction info.
// Note: call it under lock!
func (txn *LazyTxn) resetTxnInfo(
	startTS uint64,
	state txninfo.TxnRunningState,
	entriesCount uint64,
	currentSQLDigest string,
	allSQLDigests []string,
	activated bool,
) {
	if !txn.mu.LastStateChangeTime.IsZero() {
		lastState := txn.mu.State
		hasLockLbl := !txn.mu.BlockStartTime.IsZero()
		txninfo.TxnDurationHistogram(lastState, hasLockLbl).Observe(time.Since(txn.mu.TxnInfo.LastStateChangeTime).Seconds())
	}
	if txn.mu.TxnInfo.StartTS != 0 {
		txninfo.Recorder.OnTrxEnd(&txn.mu.TxnInfo)
	}
	txn.mu.TxnInfo = txninfo.TxnInfo{}
	txn.mu.TxnInfo.StartTS = startTS
	txn.mu.TxnInfo.State = state
	txninfo.TxnStatusEnteringCounter(state).Inc()
	txn.mu.TxnInfo.LastStateChangeTime = time.Now()
	txn.mu.TxnInfo.EntriesCount = entriesCount

	txn.mu.TxnInfo.CurrentSQLDigest = currentSQLDigest
	txn.mu.TxnInfo.AllSQLDigests = allSQLDigests
	txn.mu.TxnInfo.Activated = activated
}

// Size implements the MemBuffer interface.
func (txn *LazyTxn) Size() int {
	if txn.Transaction == nil {
		return 0
	}
	return txn.Transaction.Size()
}

// Mem implements the MemBuffer interface.
func (txn *LazyTxn) Mem() uint64 {
	if txn.Transaction == nil {
		return 0
	}
	return txn.Transaction.Mem()
}

// SetMemoryFootprintChangeHook sets the hook to be called when the memory footprint of this transaction changes.
func (txn *LazyTxn) SetMemoryFootprintChangeHook(hook func(uint64)) {
	if txn.Transaction == nil {
		return
	}
	txn.Transaction.SetMemoryFootprintChangeHook(hook)
}

// Valid implements the kv.Transaction interface.
func (txn *LazyTxn) Valid() bool {
	return txn.Transaction != nil && txn.Transaction.Valid()
}

// Pending returns if the transaction state is pending.
func (txn *LazyTxn) Pending() bool {
	return txn.Transaction == nil && txn.txnFuture != nil
}

// ValidOrPending returns if the transaction state is in valid or pending.
func (txn *LazyTxn) ValidOrPending() bool {
	return txn.txnFuture != nil || txn.Valid()
}

func (txn *LazyTxn) String() string {
	if txn.Transaction != nil {
		return txn.Transaction.String()
	}
	if txn.txnFuture != nil {
		res := "txnFuture"
		if txn.enterAggressiveLockingOnValid {
			res += " (pending aggressive locking)"
		}
		return res
	}
	return "invalid transaction"
}

// GoString implements the "%#v" format for fmt.Printf.
func (txn *LazyTxn) GoString() string {
	var s strings.Builder
	s.WriteString("Txn{")
	if txn.Pending() {
		s.WriteString("state=pending")
	} else if txn.Valid() {
		s.WriteString("state=valid")
		fmt.Fprintf(&s, ", txnStartTS=%d", txn.Transaction.StartTS())
		if len(txn.mutations) > 0 {
			fmt.Fprintf(&s, ", len(mutations)=%d, %#v", len(txn.mutations), txn.mutations)
		}
	} else {
		s.WriteString("state=invalid")
	}

	s.WriteString("}")
	return s.String()
}

// GetOption implements the GetOption
func (txn *LazyTxn) GetOption(opt int) interface{} {
	if txn.Transaction == nil {
		switch opt {
		case kv.TxnScope:
			return ""
		}
		return nil
	}
	return txn.Transaction.GetOption(opt)
}

// ChangeToPending changes the transaction state to pending.
func (txn *LazyTxn) ChangeToPending(future *TxnFuture) {
	txn.Transaction = nil
	txn.txnFuture = future
}

// ChangePendingToValid changes the transaction state to valie from pending.
func (txn *LazyTxn) ChangePendingToValid(ctx context.Context) error {
	if txn.txnFuture == nil {
		return errors.New("transaction future is not set")
	}

	future := txn.txnFuture
	txn.txnFuture = nil

	defer trace.StartRegion(ctx, "WaitTsoFuture").End()
	t, err := future.wait()
	if err != nil {
		txn.Transaction = nil
		return err
	}
	txn.Transaction = t
	txn.initStmtBuf()

	if txn.enterAggressiveLockingOnValid {
		txn.enterAggressiveLockingOnValid = false
		err = txn.Transaction.StartAggressiveLocking()
		if err != nil {
			return err
		}
	}

	// The txnInfo may already recorded the first statement (usually "begin") when it's pending, so keep them.
	txn.mu.Lock()
	defer txn.mu.Unlock()
	txn.resetTxnInfo(
		t.StartTS(),
		txninfo.TxnIdle,
		uint64(txn.Transaction.Len()),
		txn.mu.TxnInfo.CurrentSQLDigest,
		txn.mu.TxnInfo.AllSQLDigests,
		true)

	return nil
}

// ChangeToInvalid changes the transaction state to invalid.
func (txn *LazyTxn) ChangeToInvalid() {
	if txn.stagingHandle != kv.InvalidStagingHandle {
		txn.Transaction.GetMemBuffer().Cleanup(txn.stagingHandle)
	}
	txn.stagingHandle = kv.InvalidStagingHandle
	txn.Transaction = nil
	txn.txnFuture = nil

	txn.enterAggressiveLockingOnValid = false

	txn.mu.Lock()
	lastState := txn.mu.TxnInfo.State
	lastStateChangeTime := txn.mu.TxnInfo.LastStateChangeTime
	hasLock := !txn.mu.TxnInfo.BlockStartTime.IsZero()
	if txn.mu.TxnInfo.StartTS != 0 {
		txninfo.Recorder.OnTrxEnd(&txn.mu.TxnInfo)
	}
	txn.mu.TxnInfo = txninfo.TxnInfo{}
	txn.mu.Unlock()
	if !lastStateChangeTime.IsZero() {
		txninfo.TxnDurationHistogram(lastState, hasLock).Observe(time.Since(lastStateChangeTime).Seconds())
	}
}

// OnStmtStart is used to record TxnInfo information when the statement is started. By now it's a bit duplicated
// with the TxnManager.OnStmtStart hook.
func (txn *LazyTxn) OnStmtStart(currentSQLDigest string) {
	if len(currentSQLDigest) == 0 {
		return
	}

	txn.mu.Lock()
	defer txn.mu.Unlock()
	txn.updateState(txninfo.TxnRunning)
	txn.mu.TxnInfo.CurrentSQLDigest = currentSQLDigest
	// Keeps at most 50 history sqls to avoid consuming too much memory.
	const maxTransactionStmtHistory int = 50
	if len(txn.mu.TxnInfo.AllSQLDigests) < maxTransactionStmtHistory {
		txn.mu.TxnInfo.AllSQLDigests = append(txn.mu.TxnInfo.AllSQLDigests, currentSQLDigest)
	}
}

// OnStmtEnd is used to record TxnInfo information when the statement is finished.
func (txn *LazyTxn) OnStmtEnd() {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	txn.mu.TxnInfo.CurrentSQLDigest = ""
	txn.updateState(txninfo.TxnIdle)
}

// StmtCommit commits the current execution statement.
func (txn *LazyTxn) StmtCommit(ctx context.Context, sctx sessionctx.Context) error {
	defer func() {
		txn.cleanup()
	}()

	txnManager := GetTxnManager(sctx)
	err := txnManager.OnStmtCommit(ctx)
	if err != nil {
		logutil.Logger(ctx).Error("txnManager failed to handle OnStmtCommit", zap.Error(err))
	}

	txn.flushStmtBuf()

	// Need to flush binlog.
	for tableID, delta := range txn.mutations {
		mutation := getBinlogMutation(sctx, tableID)
		mergeToMutation(mutation, delta)
	}
	return nil
}

// StmtRollback rolls back the current execution statement.
func (txn *LazyTxn) StmtRollback(ctx context.Context, sctx sessionctx.Context, isForcePessimisticRetry bool) error {
	txnManager := GetTxnManager(sctx)
	err := txnManager.OnStmtRollback(ctx, isForcePessimisticRetry)
	if err != nil {
		logutil.Logger(ctx).Error("txnManager failed to handle OnStmtRollback", zap.Error(err))
	}
	txn.cleanup()
	return nil
}

// StmtGetMutation returns the binlog mutation in current statement.
func (txn *LazyTxn) StmtGetMutation(tableID int64) *binlog.TableMutation {
	if _, ok := txn.mutations[tableID]; !ok {
		txn.mutations[tableID] = &binlog.TableMutation{TableId: tableID}
	}
	return txn.mutations[tableID]
}

// HasDirtyContent checks whether the transaction is dirty.
func (txn *LazyTxn) HasDirtyContent(tableID int64) bool {
	if txn.Transaction == nil {
		return false
	}
	seekKey := tablecodec.EncodeTablePrefix(tableID)
	it, err := txn.GetMemBuffer().Iter(seekKey, nil)
	terror.Log(err)
	return it.Valid() && bytes.HasPrefix(it.Key(), seekKey)
}

// Commit overrides the Transaction interface.
func (txn *LazyTxn) Commit(ctx context.Context) error {
	defer txn.reset()
	if len(txn.mutations) != 0 || txn.countHint() != 0 {
		logutil.BgLogger().Error("the code should never run here",
			zap.String("TxnState", txn.GoString()),
			zap.Int("staging handler", int(txn.stagingHandle)),
			zap.Stack("something must be wrong"))
		return errors.Trace(kv.ErrInvalidTxn)
	}

	txn.mu.Lock()
	txn.updateState(txninfo.TxnCommitting)
	txn.mu.Unlock()

	failpoint.Inject("mockSlowCommit", func(_ failpoint.Value) {})

	// mockCommitError8942 is used for PR #8942.
	failpoint.Inject("mockCommitError8942", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(kv.ErrTxnRetryable)
		}
	})

	// mockCommitRetryForAutoIncID is used to mock an commit retry for adjustAutoIncrementDatum.
	failpoint.Inject("mockCommitRetryForAutoIncID", func(val failpoint.Value) {
		if val.(bool) && !mockAutoIncIDRetry() {
			enableMockAutoIncIDRetry()
			failpoint.Return(kv.ErrTxnRetryable)
		}
	})

	failpoint.Inject("mockCommitRetryForAutoRandID", func(val failpoint.Value) {
		if val.(bool) && needMockAutoRandIDRetry() {
			decreaseMockAutoRandIDRetryCount()
			failpoint.Return(kv.ErrTxnRetryable)
		}
	})

	return txn.Transaction.Commit(ctx)
}

// Rollback overrides the Transaction interface.
func (txn *LazyTxn) Rollback() error {
	defer txn.reset()
	txn.mu.Lock()
	txn.updateState(txninfo.TxnRollingBack)
	txn.mu.Unlock()
	// mockSlowRollback is used to mock a rollback which takes a long time
	failpoint.Inject("mockSlowRollback", func(_ failpoint.Value) {})
	return txn.Transaction.Rollback()
}

// RollbackMemDBToCheckpoint overrides the Transaction interface.
func (txn *LazyTxn) RollbackMemDBToCheckpoint(savepoint *tikv.MemDBCheckpoint) {
	txn.flushStmtBuf()
	txn.Transaction.RollbackMemDBToCheckpoint(savepoint)
	txn.cleanup()
}

// LockKeys wraps the inner transaction's `LockKeys` to record the status
func (txn *LazyTxn) LockKeys(ctx context.Context, lockCtx *kv.LockCtx, keys ...kv.Key) error {
	return txn.LockKeysFunc(ctx, lockCtx, nil, keys...)
}

// LockKeysFunc Wrap the inner transaction's `LockKeys` to record the status
func (txn *LazyTxn) LockKeysFunc(ctx context.Context, lockCtx *kv.LockCtx, fn func(), keys ...kv.Key) error {
	failpoint.Inject("beforeLockKeys", func() {})
	t := time.Now()

	var originState txninfo.TxnRunningState
	txn.mu.Lock()
	originState = txn.mu.TxnInfo.State
	txn.updateState(txninfo.TxnLockAcquiring)
	txn.mu.TxnInfo.BlockStartTime.Valid = true
	txn.mu.TxnInfo.BlockStartTime.Time = t
	txn.mu.Unlock()
	lockFunc := func() {
		if fn != nil {
			fn()
		}
		txn.mu.Lock()
		defer txn.mu.Unlock()
		txn.updateState(originState)
		txn.mu.TxnInfo.BlockStartTime.Valid = false
		txn.mu.TxnInfo.EntriesCount = uint64(txn.Transaction.Len())
	}
	return txn.Transaction.LockKeysFunc(ctx, lockCtx, lockFunc, keys...)
}

// StartAggressiveLocking wraps the inner transaction to support using aggressive locking with lazy initialization.
func (txn *LazyTxn) StartAggressiveLocking() error {
	if txn.Valid() {
		return txn.Transaction.StartAggressiveLocking()
	} else if txn.Pending() {
		txn.enterAggressiveLockingOnValid = true
	} else {
		err := errors.New("trying to start aggressive locking on a transaction in invalid state")
		logutil.BgLogger().Error("unexpected error when starting aggressive locking", zap.Error(err), zap.Stringer("txn", txn))
		return err
	}
	return nil
}

// RetryAggressiveLocking wraps the inner transaction to support using aggressive locking with lazy initialization.
func (txn *LazyTxn) RetryAggressiveLocking(ctx context.Context) error {
	if txn.Valid() {
		return txn.Transaction.RetryAggressiveLocking(ctx)
	} else if !txn.Pending() {
		err := errors.New("trying to retry aggressive locking on a transaction in invalid state")
		logutil.BgLogger().Error("unexpected error when retrying aggressive locking", zap.Error(err), zap.Stringer("txnStartTS", txn))
		return err
	}
	return nil
}

// CancelAggressiveLocking wraps the inner transaction to support using aggressive locking with lazy initialization.
func (txn *LazyTxn) CancelAggressiveLocking(ctx context.Context) error {
	if txn.Valid() {
		return txn.Transaction.CancelAggressiveLocking(ctx)
	} else if txn.Pending() {
		if txn.enterAggressiveLockingOnValid {
			txn.enterAggressiveLockingOnValid = false
		} else {
			err := errors.New("trying to cancel aggressive locking when it's not started")
			logutil.BgLogger().Error("unexpected error when cancelling aggressive locking", zap.Error(err), zap.Stringer("txnStartTS", txn))
			return err
		}
	} else {
		err := errors.New("trying to cancel aggressive locking on a transaction in invalid state")
		logutil.BgLogger().Error("unexpected error when cancelling aggressive locking", zap.Error(err), zap.Stringer("txnStartTS", txn))
		return err
	}
	return nil
}

// DoneAggressiveLocking wraps the inner transaction to support using aggressive locking with lazy initialization.
func (txn *LazyTxn) DoneAggressiveLocking(ctx context.Context) error {
	if txn.Valid() {
		return txn.Transaction.DoneAggressiveLocking(ctx)
	} else if txn.Pending() {
		if txn.enterAggressiveLockingOnValid {
			txn.enterAggressiveLockingOnValid = false
		} else {
			err := errors.New("trying to finish aggressive locking when it's not started")
			logutil.BgLogger().Error("unexpected error when finishing aggressive locking")
			return err
		}
	} else {
		err := errors.New("trying to cancel aggressive locking on a transaction in invalid state")
		logutil.BgLogger().Error("unexpected error when finishing aggressive locking")
		return err
	}
	return nil
}

// IsInAggressiveLockingMode wraps the inner transaction to support using aggressive locking with lazy initialization.
func (txn *LazyTxn) IsInAggressiveLockingMode() bool {
	if txn.Valid() {
		return txn.Transaction.IsInAggressiveLockingMode()
	} else if txn.Pending() {
		return txn.enterAggressiveLockingOnValid
	} else {
		return false
	}
}

func (txn *LazyTxn) reset() {
	txn.cleanup()
	txn.ChangeToInvalid()
}

func (txn *LazyTxn) cleanup() {
	txn.cleanupStmtBuf()
	txn.initStmtBuf()
	for key := range txn.mutations {
		delete(txn.mutations, key)
	}
}

// KeysNeedToLock returns the keys need to be locked.
func (txn *LazyTxn) KeysNeedToLock() ([]kv.Key, error) {
	if txn.stagingHandle == kv.InvalidStagingHandle {
		return nil, nil
	}
	keys := make([]kv.Key, 0, txn.countHint())
	buf := txn.Transaction.GetMemBuffer()
	buf.InspectStage(txn.stagingHandle, func(k kv.Key, flags kv.KeyFlags, v []byte) {
		if !KeyNeedToLock(k, v, flags) {
			return
		}
		keys = append(keys, k)
	})

	return keys, nil
}

// Wait converts pending txn to valid
func (txn *LazyTxn) Wait(ctx context.Context, sctx sessionctx.Context) (kv.Transaction, error) {
	if !txn.ValidOrPending() {
		return txn, errors.AddStack(kv.ErrInvalidTxn)
	}
	if txn.Pending() {
		defer func(begin time.Time) {
			sctx.GetSessionVars().DurationWaitTS = time.Since(begin)
		}(time.Now())

		// Transaction is lazy initialized.
		// PrepareTxnCtx is called to get a tso future, makes s.txn a pending txn,
		// If Txn() is called later, wait for the future to get a valid txn.
		if err := txn.ChangePendingToValid(ctx); err != nil {
			logutil.BgLogger().Error("active transaction fail",
				zap.Error(err))
			txn.cleanup()
			sctx.GetSessionVars().TxnCtx.StartTS = 0
			return txn, err
		}
		txn.lazyUniquenessCheckEnabled = !sctx.GetSessionVars().ConstraintCheckInPlacePessimistic
	}
	return txn, nil
}

// TxnInfo returns information about current transaction.
func (txn *LazyTxn) TxnInfo(sctx sessionctx.Context) *txninfo.TxnInfo {
	txn.mu.RLock()
	// Copy on read to get a snapshot, this API shouldn't be frequently called.
	txnInfo := txn.mu.TxnInfo
	txn.mu.RUnlock()

	processInfo := sctx.ShowProcess()
	txnInfo.ConnectionID = processInfo.ID
	txnInfo.Username = processInfo.User
	txnInfo.CurrentDB = processInfo.DB
	txnInfo.RelatedTableIDs = make(map[int64]struct{})
	sctx.GetSessionVars().GetRelatedTableForMDL().Range(func(key, value interface{}) bool {
		txnInfo.RelatedTableIDs[key.(int64)] = struct{}{}
		return true
	})
	return &txnInfo
}

// PrepareTSFuture replaces the transaction future using the input one.
func (txn *LazyTxn) PrepareTSFuture(ctx context.Context, future oracle.Future, scope string, store kv.Storage) error {
	if txn.Valid() {
		return errors.New("cannot prepare ts future when txn is valid")
	}

	failpoint.Inject("assertTSONotRequest", func() {
		if _, ok := future.(ConstantFuture); !ok {
			panic("tso shouldn't be requested")
		}
	})

	failpoint.InjectContext(ctx, "mockGetTSFail", func() {
		future = TxnFailFuture{}
	})

	txn.ChangeToPending(&TxnFuture{
		Future:   future,
		Store:    store,
		TxnScope: scope,
	})
	return nil
}

// GetPreparedFuture returns transaction future already prepared.
func (txn *LazyTxn) GetPreparedFuture() sessionctx.TxnFuture {
	if !txn.ValidOrPending() {
		return nil
	}
	return txn
}

// KeyNeedToLock checks whether the current key should be pessimistically locked.
func KeyNeedToLock(k, v []byte, flags kv.KeyFlags) bool {
	isTableKey := bytes.HasPrefix(k, tablecodec.TablePrefix())
	if !isTableKey {
		// meta key always need to lock.
		return true
	}

	// a pessimistic locking is skipped, perform the conflict check and
	// constraint check (more accurately, PresumeKeyNotExist) in prewrite (or later pessimistic locking)
	if flags.HasNeedConstraintCheckInPrewrite() {
		return false
	}

	if flags.HasPresumeKeyNotExists() {
		return true
	}

	// lock row key, primary key and unique index for delete operation,
	if len(v) == 0 {
		return flags.HasNeedLocked() || tablecodec.IsRecordKey(k)
	}

	if tablecodec.IsUntouchedIndexKValue(k, v) {
		return false
	}

	if !tablecodec.IsIndexKey(k) {
		return true
	}

	return tablecodec.IndexKVIsUnique(v)
}

func getBinlogMutation(ctx sessionctx.Context, tableID int64) *binlog.TableMutation {
	bin := binloginfo.GetPrewriteValue(ctx, true)
	for i := range bin.Mutations {
		if bin.Mutations[i].TableId == tableID {
			return &bin.Mutations[i]
		}
	}
	idx := len(bin.Mutations)
	bin.Mutations = append(bin.Mutations, binlog.TableMutation{TableId: tableID})
	return &bin.Mutations[idx]
}

func mergeToMutation(m1, m2 *binlog.TableMutation) {
	m1.InsertedRows = append(m1.InsertedRows, m2.InsertedRows...)
	m1.UpdatedRows = append(m1.UpdatedRows, m2.UpdatedRows...)
	m1.DeletedIds = append(m1.DeletedIds, m2.DeletedIds...)
	m1.DeletedPks = append(m1.DeletedPks, m2.DeletedPks...)
	m1.DeletedRows = append(m1.DeletedRows, m2.DeletedRows...)
	m1.Sequence = append(m1.Sequence, m2.Sequence...)
}

// TxnFailFuture is used to simulate future wait failure.
type TxnFailFuture struct{}

// Wait implements the future wait interface.
func (TxnFailFuture) Wait() (uint64, error) {
	return 0, errors.New("mock get timestamp fail")
}

// TxnFuture is a promise, which promises to return a txn in future.
type TxnFuture struct {
	Future   oracle.Future
	Store    kv.Storage
	TxnScope string
}

func (tf *TxnFuture) wait() (kv.Transaction, error) {
	startTS, err := tf.Future.Wait()
	failpoint.Inject("txnFutureWait", func() {})
	if err == nil {
		return tf.Store.Begin(tikv.WithTxnScope(tf.TxnScope), tikv.WithStartTS(startTS))
	} else if config.GetGlobalConfig().Store == "unistore" {
		return nil, err
	}

	logutil.BgLogger().Warn("wait tso failed", zap.Error(err))
	// It would retry get timestamp.
	return tf.Store.Begin(tikv.WithTxnScope(tf.TxnScope))
}
