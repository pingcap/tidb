// Copyright 2018 PingCAP, Inc.

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
	"fmt"
	"runtime/trace"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/session/txninfo"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/sli"
	"github.com/pingcap/tidb/pkg/util/syncutil"
)

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
	txnFuture *txnFuture

	initCnt       int
	stagingHandle kv.StagingHandle
	writeSLI      sli.TxnWriteThroughputSLI

	enterFairLockingOnValid bool

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

	// commit ts of the last successful transaction, to ensure ordering of TS
	lastCommitTS uint64
}

// GetTableInfo returns the cached index name.
func (txn *LazyTxn) GetTableInfo(id int64) *model.TableInfo {
	return txn.Transaction.GetTableInfo(id)
}

// CacheTableInfo caches the index name.
func (txn *LazyTxn) CacheTableInfo(id int64, info *model.TableInfo) {
	txn.Transaction.CacheTableInfo(id, info)
}

func (txn *LazyTxn) init() {
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
	if !txn.IsPipelined() {
		txn.stagingHandle = buf.Staging()
	}
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
		keysNeedSetPersistentPNE := kv.FindKeysInStage(buf, txn.stagingHandle, func(_ kv.Key, flags kv.KeyFlags, _ []byte) bool {
			return flags.HasPresumeKeyNotExists()
		})
		for _, key := range keysNeedSetPersistentPNE {
			buf.UpdateFlags(key, kv.SetPreviousPresumeKeyNotExists)
		}
	}

	if !txn.IsPipelined() {
		buf.Release(txn.stagingHandle)
	}
	txn.initCnt = buf.Len()
}

func (txn *LazyTxn) cleanupStmtBuf() {
	if txn.stagingHandle == kv.InvalidStagingHandle {
		return
	}
	buf := txn.Transaction.GetMemBuffer()
	if !txn.IsPipelined() {
		buf.Cleanup(txn.stagingHandle)
	}
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

// MemHookSet returns whether the memory footprint change hook is set.
func (txn *LazyTxn) MemHookSet() bool {
	if txn.Transaction == nil {
		return false
	}
	return txn.Transaction.MemHookSet()
}

// Valid implements the kv.Transaction interface.
func (txn *LazyTxn) Valid() bool {
	return txn.Transaction != nil && txn.Transaction.Valid()
}

func (txn *LazyTxn) pending() bool {
	return txn.Transaction == nil && txn.txnFuture != nil
}

func (txn *LazyTxn) validOrPending() bool {
	return txn.txnFuture != nil || txn.Valid()
}

func (txn *LazyTxn) String() string {
	if txn.Transaction != nil {
		return txn.Transaction.String()
	}
	if txn.txnFuture != nil {
		res := "txnFuture"
		if txn.enterFairLockingOnValid {
			res += " (pending fair locking)"
		}
		return res
	}
	return "invalid transaction"
}

// GoString implements the "%#v" format for fmt.Printf.
func (txn *LazyTxn) GoString() string {
	var s strings.Builder
	s.WriteString("Txn{")
	if txn.pending() {
		s.WriteString("state=pending")
	} else if txn.Valid() {
		s.WriteString("state=valid")
		fmt.Fprintf(&s, ", txnStartTS=%d", txn.Transaction.StartTS())
	} else {
		s.WriteString("state=invalid")
	}

	s.WriteString("}")
	return s.String()
}

// GetOption implements the GetOption
func (txn *LazyTxn) GetOption(opt int) any {
	if txn.Transaction == nil {
		if opt == kv.TxnScope {
			return ""
		}
		return nil
	}
	return txn.Transaction.GetOption(opt)
}

func (txn *LazyTxn) changeToPending(future *txnFuture) {
	txn.Transaction = nil
	txn.txnFuture = future
}

func (txn *LazyTxn) changePendingToValid(ctx context.Context, sctx sessionctx.Context) error {
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

	if txn.enterFairLockingOnValid {
		txn.enterFairLockingOnValid = false
		err = txn.Transaction.StartFairLocking()
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
		txn.mu.TxnInfo.AllSQLDigests)

	// set resource group name for kv request such as lock pessimistic keys.
	kv.SetTxnResourceGroup(txn, sctx.GetSessionVars().StmtCtx.ResourceGroupName)
	// overwrite entry size limit by sys var.
	if entrySizeLimit := sctx.GetSessionVars().TxnEntrySizeLimit; entrySizeLimit > 0 {
		txn.SetOption(kv.SizeLimits, kv.TxnSizeLimits{
			Entry: entrySizeLimit,
			Total: kv.TxnTotalSizeLimit.Load(),
		})
	}

	return nil
}

func (txn *LazyTxn) changeToInvalid() {
	if txn.stagingHandle != kv.InvalidStagingHandle && !txn.IsPipelined() {
		txn.Transaction.GetMemBuffer().Cleanup(txn.stagingHandle)
	}
	txn.stagingHandle = kv.InvalidStagingHandle
	txn.Transaction = nil
	txn.txnFuture = nil

	txn.enterFairLockingOnValid = false

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

func (txn *LazyTxn) onStmtStart(currentSQLDigest string) {
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

func (txn *LazyTxn) onStmtEnd() {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	txn.mu.TxnInfo.CurrentSQLDigest = ""
	txn.updateState(txninfo.TxnIdle)
}

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

