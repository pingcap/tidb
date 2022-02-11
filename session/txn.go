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
	"bytes"
	"context"
	"fmt"
	"runtime/trace"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/session/txninfo"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sli"
	"github.com/pingcap/tipb/go-binlog"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
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
	mutations     map[int64]*binlog.TableMutation
	writeSLI      sli.TxnWriteThroughputSLI

	// TxnInfo is added for the lock view feature, the data is frequent modified but
	// rarely read (just in query select * from information_schema.tidb_trx).
	// The data in this session would be query by other sessions, so Mutex is necessary.
	// Since read is rare, the reader can copy-on-read to get a data snapshot.
	mu struct {
		sync.RWMutex
		txninfo.TxnInfo
	}
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
	txn.mutations = make(map[int64]*binlog.TableMutation)
	txn.mu.Lock()
	txn.mu.TxnInfo.State = txninfo.TxnIdle
	txn.mu.Unlock()
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
	txn.mu.TxnInfo.EntriesSize = uint64(txn.Transaction.Size())
}

// resetTxnInfo resets the transaction info.
// Note: call it under lock!
func (txn *LazyTxn) resetTxnInfo(
	startTS uint64,
	state txninfo.TxnRunningState,
	entriesCount,
	entriesSize uint64,
	currentSQLDigest string,
	allSQLDigests []string,
) {
	txn.mu.TxnInfo = txninfo.TxnInfo{}
	txn.mu.TxnInfo.StartTS = startTS
	txn.mu.TxnInfo.State = state
	txn.mu.TxnInfo.EntriesCount = entriesCount
	txn.mu.TxnInfo.EntriesSize = entriesSize
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
		return "txnFuture"
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

func (txn *LazyTxn) changeInvalidToValid(kvTxn kv.Transaction) {
	txn.Transaction = kvTxn
	txn.initStmtBuf()
	txn.txnFuture = nil

	txn.mu.Lock()
	defer txn.mu.Unlock()
	txn.resetTxnInfo(
		kvTxn.StartTS(),
		txninfo.TxnIdle,
		uint64(txn.Transaction.Len()),
		uint64(txn.Transaction.Size()),
		"",
		nil)
}

func (txn *LazyTxn) changeInvalidToPending(future *txnFuture) {
	txn.Transaction = nil
	txn.txnFuture = future
}

func (txn *LazyTxn) changePendingToValid(ctx context.Context) error {
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

	// The txnInfo may already recorded the first statement (usually "begin") when it's pending, so keep them.
	txn.mu.Lock()
	defer txn.mu.Unlock()
	txn.resetTxnInfo(
		t.StartTS(),
		txninfo.TxnIdle,
		uint64(txn.Transaction.Len()),
		uint64(txn.Transaction.Size()),
		txn.mu.TxnInfo.CurrentSQLDigest,
		txn.mu.TxnInfo.AllSQLDigests)

	return nil
}

func (txn *LazyTxn) changeToInvalid() {
	if txn.stagingHandle != kv.InvalidStagingHandle {
		txn.Transaction.GetMemBuffer().Cleanup(txn.stagingHandle)
	}
	txn.stagingHandle = kv.InvalidStagingHandle
	txn.Transaction = nil
	txn.txnFuture = nil

	txn.mu.Lock()
	defer txn.mu.Unlock()
	txn.mu.TxnInfo = txninfo.TxnInfo{}
}

func (txn *LazyTxn) onStmtStart(currentSQLDigest string) {
	if len(currentSQLDigest) == 0 {
		return
	}

	txn.mu.Lock()
	defer txn.mu.Unlock()
	txn.mu.TxnInfo.State = txninfo.TxnRunning
	txn.mu.TxnInfo.CurrentSQLDigest = currentSQLDigest
	// Keeps at most 50 history sqls to avoid consuming too much memory.
	const maxTransactionStmtHistory int = 50
	if len(txn.mu.TxnInfo.AllSQLDigests) < maxTransactionStmtHistory {
		txn.mu.TxnInfo.AllSQLDigests = append(txn.mu.TxnInfo.AllSQLDigests, currentSQLDigest)
	}
}

func (txn *LazyTxn) onStmtEnd() {
	txn.mu.Lock()
	txn.mu.TxnInfo.CurrentSQLDigest = ""
	txn.mu.TxnInfo.State = txninfo.TxnIdle
	txn.mu.Unlock()
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
	txn.mu.TxnInfo.State = txninfo.TxnCommitting
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
	txn.mu.TxnInfo.State = txninfo.TxnRollingBack
	txn.mu.Unlock()
	// mockSlowRollback is used to mock a rollback which takes a long time
	failpoint.Inject("mockSlowRollback", func(_ failpoint.Value) {})
	return txn.Transaction.Rollback()
}

// LockKeys Wrap the inner transaction's `LockKeys` to record the status
func (txn *LazyTxn) LockKeys(ctx context.Context, lockCtx *kv.LockCtx, keys ...kv.Key) error {
	failpoint.Inject("beforeLockKeys", func() {})
	t := time.Now()

	var originState txninfo.TxnRunningState
	txn.mu.Lock()
	originState = txn.mu.TxnInfo.State
	txn.mu.TxnInfo.State = txninfo.TxnLockWaiting
	txn.mu.TxnInfo.BlockStartTime.Valid = true
	txn.mu.TxnInfo.BlockStartTime.Time = t
	txn.mu.Unlock()

	err := txn.Transaction.LockKeys(ctx, lockCtx, keys...)

	txn.mu.Lock()
	defer txn.mu.Unlock()
	txn.mu.TxnInfo.State = originState
	txn.mu.TxnInfo.BlockStartTime.Valid = false
	txn.mu.TxnInfo.EntriesCount = uint64(txn.Transaction.Len())
	txn.mu.TxnInfo.EntriesSize = uint64(txn.Transaction.Size())
	return err
}

func (txn *LazyTxn) reset() {
	txn.cleanup()
	txn.changeToInvalid()
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
		if !keyNeedToLock(k, v, flags) {
			return
		}
		keys = append(keys, k)
	})
	return keys, nil
}

func keyNeedToLock(k, v []byte, flags kv.KeyFlags) bool {
	isTableKey := bytes.HasPrefix(k, tablecodec.TablePrefix())
	if !isTableKey {
		// meta key always need to lock.
		return true
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
	isNonUniqueIndex := tablecodec.IsIndexKey(k) && len(v) == 1
	// Put row key and unique index need to lock.
	return !isNonUniqueIndex
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

type txnFailFuture struct{}

func (txnFailFuture) Wait() (uint64, error) {
	return 0, errors.New("mock get timestamp fail")
}

// txnFuture is a promise, which promises to return a txn in future.
type txnFuture struct {
	future   oracle.Future
	store    kv.Storage
	txnScope string
}

func (tf *txnFuture) wait() (kv.Transaction, error) {
	startTS, err := tf.future.Wait()
	if err == nil {
		return tf.store.Begin(tikv.WithTxnScope(tf.txnScope), tikv.WithStartTS(startTS))
	} else if config.GetGlobalConfig().Store == "unistore" {
		return nil, err
	}

	logutil.BgLogger().Warn("wait tso failed", zap.Error(err))
	failpoint.Inject("mockGetTSFail", func() {
		tf.store.Begin(tikv.WithTxnScope(tf.txnScope))
		failpoint.Return(nil, errors.New("mock get timestamp fail"))
	})
	// It would retry get timestamp.
	return tf.store.Begin(tikv.WithTxnScope(tf.txnScope))
}

func (s *session) getTxnFuture(ctx context.Context) *txnFuture {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("session.getTxnFuture", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	oracleStore := s.store.GetOracle()
	var tsFuture oracle.Future
	if s.sessionVars.LowResolutionTSO {
		tsFuture = oracleStore.GetLowResolutionTimestampAsync(ctx, &oracle.Option{TxnScope: s.sessionVars.CheckAndGetTxnScope()})
	} else {
		tsFuture = oracleStore.GetTimestampAsync(ctx, &oracle.Option{TxnScope: s.sessionVars.CheckAndGetTxnScope()})
	}
	ret := &txnFuture{future: tsFuture, store: s.store, txnScope: s.sessionVars.CheckAndGetTxnScope()}
	failpoint.InjectContext(ctx, "mockGetTSFail", func() {
		ret.future = txnFailFuture{}
	})
	return ret
}

// HasDirtyContent checks whether there's dirty update on the given table.
// Put this function here is to avoid cycle import.
func (s *session) HasDirtyContent(tid int64) bool {
	if s.txn.Transaction == nil {
		return false
	}
	seekKey := tablecodec.EncodeTablePrefix(tid)
	it, err := s.txn.GetMemBuffer().Iter(seekKey, nil)
	terror.Log(err)
	return it.Valid() && bytes.HasPrefix(it.Key(), seekKey)
}

// StmtCommit implements the sessionctx.Context interface.
func (s *session) StmtCommit() {
	defer func() {
		s.txn.cleanup()
	}()

	st := &s.txn
	st.flushStmtBuf()

	// Need to flush binlog.
	for tableID, delta := range st.mutations {
		mutation := getBinlogMutation(s, tableID)
		mergeToMutation(mutation, delta)
	}
}

// StmtRollback implements the sessionctx.Context interface.
func (s *session) StmtRollback() {
	s.txn.cleanup()
}

// StmtGetMutation implements the sessionctx.Context interface.
func (s *session) StmtGetMutation(tableID int64) *binlog.TableMutation {
	st := &s.txn
	if _, ok := st.mutations[tableID]; !ok {
		st.mutations[tableID] = &binlog.TableMutation{TableId: tableID}
	}
	return st.mutations[tableID]
}
