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
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	_ kv.Transaction = (*tikvTxn)(nil)
)

var (
	tikvTxnCmdHistogramWithCommit   = metrics.TiKVTxnCmdHistogram.WithLabelValues(metrics.LblCommit)
	tikvTxnCmdHistogramWithRollback = metrics.TiKVTxnCmdHistogram.WithLabelValues(metrics.LblRollback)
)

// tikvTxn implements kv.Transaction.
type tikvTxn struct {
	snapshot  *tikvSnapshot
	us        kv.UnionStore
	store     *tikvStore // for connection to region.
	startTS   uint64
	startTime time.Time // Monotonic timestamp for recording txn time consuming.
	commitTS  uint64
	lockKeys  [][]byte
	lockedMap map[string]struct{}
	mu        sync.Mutex // For thread-safe LockKeys function.
	setCnt    int64
	vars      *kv.Variables
	committer *twoPhaseCommitter

	// For data consistency check.
	// assertions[:confirmed] is the assertion of current transaction.
	// assertions[confirmed:len(assertions)] is the assertions of current statement.
	// StmtCommit/StmtRollback may change the confirmed position.
	assertions []assertionPair
	confirmed  int

	valid bool
	dirty bool
}

func newTiKVTxn(store *tikvStore) (*tikvTxn, error) {
	bo := NewBackoffer(context.Background(), tsoMaxBackoff)
	startTS, err := store.getTimestampWithRetry(bo)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return newTikvTxnWithStartTS(store, startTS, store.nextReplicaReadSeed())
}

// newTikvTxnWithStartTS creates a txn with startTS.
func newTikvTxnWithStartTS(store *tikvStore, startTS uint64, replicaReadSeed uint32) (*tikvTxn, error) {
	ver := kv.NewVersion(startTS)
	snapshot := newTiKVSnapshot(store, ver, replicaReadSeed)
	return &tikvTxn{
		snapshot:  snapshot,
		us:        kv.NewUnionStore(snapshot),
		lockedMap: map[string]struct{}{},
		store:     store,
		startTS:   startTS,
		startTime: time.Now(),
		valid:     true,
		vars:      kv.DefaultVars,
	}, nil
}

type assertionPair struct {
	key       kv.Key
	assertion kv.AssertionType
}

func (a assertionPair) String() string {
	return fmt.Sprintf("key: %s, assertion type: %d", a.key, a.assertion)
}

func (txn *tikvTxn) SetVars(vars *kv.Variables) {
	txn.vars = vars
	txn.snapshot.vars = vars
}

// SetCap sets the transaction's MemBuffer capability, to reduce memory allocations.
func (txn *tikvTxn) SetCap(cap int) {
	txn.us.SetCap(cap)
}

// Reset reset tikvTxn's membuf.
func (txn *tikvTxn) Reset() {
	txn.us.Reset()
}

// Get implements transaction interface.
func (txn *tikvTxn) Get(ctx context.Context, k kv.Key) ([]byte, error) {
	ret, err := txn.us.Get(ctx, k)
	if kv.IsErrNotFound(err) {
		return nil, err
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	return ret, nil
}

func (txn *tikvTxn) BatchGet(ctx context.Context, keys []kv.Key) (map[string][]byte, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("tikvTxn.BatchGet", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	return kv.NewBufferBatchGetter(txn.GetMemBuffer(), nil, txn.snapshot).BatchGet(ctx, keys)
}

func (txn *tikvTxn) Set(k kv.Key, v []byte) error {
	txn.setCnt++

	txn.dirty = true
	return txn.us.Set(k, v)
}

func (txn *tikvTxn) String() string {
	return fmt.Sprintf("%d", txn.StartTS())
}

func (txn *tikvTxn) Iter(k kv.Key, upperBound kv.Key) (kv.Iterator, error) {
	return txn.us.Iter(k, upperBound)
}

// IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
func (txn *tikvTxn) IterReverse(k kv.Key) (kv.Iterator, error) {
	return txn.us.IterReverse(k)
}

func (txn *tikvTxn) Delete(k kv.Key) error {
	txn.dirty = true
	return txn.us.Delete(k)
}

func (txn *tikvTxn) SetOption(opt kv.Option, val interface{}) {
	txn.us.SetOption(opt, val)
	switch opt {
	case kv.Priority:
		txn.snapshot.priority = kvPriorityToCommandPri(val.(int))
	case kv.NotFillCache:
		txn.snapshot.notFillCache = val.(bool)
	case kv.SyncLog:
		txn.snapshot.syncLog = val.(bool)
	case kv.KeyOnly:
		txn.snapshot.keyOnly = val.(bool)
	case kv.SnapshotTS:
		txn.snapshot.setSnapshotTS(val.(uint64))
	}
}

func (txn *tikvTxn) DelOption(opt kv.Option) {
	txn.us.DelOption(opt)
}

func (txn *tikvTxn) IsPessimistic() bool {
	return txn.us.GetOption(kv.Pessimistic) != nil
}

func (txn *tikvTxn) Commit(ctx context.Context) error {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("tikvTxn.Commit", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	if !txn.valid {
		return kv.ErrInvalidTxn
	}
	defer txn.close()

	failpoint.Inject("mockCommitError", func(val failpoint.Value) {
		if val.(bool) && kv.IsMockCommitErrorEnable() {
			kv.MockCommitErrorDisable()
			failpoint.Return(errors.New("mock commit error"))
		}
	})

	start := time.Now()
	defer func() { tikvTxnCmdHistogramWithCommit.Observe(time.Since(start).Seconds()) }()

	// connID is used for log.
	var connID uint64
	val := ctx.Value(sessionctx.ConnID)
	if val != nil {
		connID = val.(uint64)
	}

	var err error
	// If the txn use pessimistic lock, committer is initialized.
	committer := txn.committer
	if committer == nil {
		committer, err = newTwoPhaseCommitter(txn, connID)
		if err != nil {
			return errors.Trace(err)
		}
	}
	defer committer.ttlManager.close()
	if err := committer.initKeysAndMutations(); err != nil {
		return errors.Trace(err)
	}
	if committer.mutations.len() == 0 {
		return nil
	}

	defer func() {
		ctxValue := ctx.Value(execdetails.CommitDetailCtxKey)
		if ctxValue != nil {
			commitDetail := ctxValue.(**execdetails.CommitDetails)
			if *commitDetail != nil {
				(*commitDetail).TxnRetry++
			} else {
				*commitDetail = committer.getDetail()
			}
		}
	}()
	// latches disabled
	// pessimistic transaction should also bypass latch.
	if txn.store.txnLatches == nil || txn.IsPessimistic() {
		err = committer.execute(ctx)
		logutil.Logger(ctx).Debug("[kv] txnLatches disabled, 2pc directly", zap.Error(err))
		return errors.Trace(err)
	}

	// latches enabled
	// for transactions which need to acquire latches
	start = time.Now()
	lock := txn.store.txnLatches.Lock(committer.startTS, committer.mutations.keys)
	commitDetail := committer.getDetail()
	commitDetail.LocalLatchTime = time.Since(start)
	if commitDetail.LocalLatchTime > 0 {
		metrics.TiKVLocalLatchWaitTimeHistogram.Observe(commitDetail.LocalLatchTime.Seconds())
	}
	defer txn.store.txnLatches.UnLock(lock)
	if lock.IsStale() {
		return kv.ErrWriteConflictInTiDB.FastGenByArgs(txn.startTS)
	}
	err = committer.execute(ctx)
	if err == nil {
		lock.SetCommitTS(committer.commitTS)
	}
	logutil.Logger(ctx).Debug("[kv] txnLatches enabled while txn retryable", zap.Error(err))
	return errors.Trace(err)
}

func (txn *tikvTxn) close() {
	txn.valid = false
}

func (txn *tikvTxn) Rollback() error {
	if !txn.valid {
		return kv.ErrInvalidTxn
	}
	start := time.Now()
	// Clean up pessimistic lock.
	if txn.IsPessimistic() && txn.committer != nil {
		err := txn.rollbackPessimisticLocks()
		txn.committer.ttlManager.close()
		if err != nil {
			logutil.BgLogger().Error(err.Error())
		}
	}
	txn.close()
	logutil.BgLogger().Debug("[kv] rollback txn", zap.Uint64("txnStartTS", txn.StartTS()))
	tikvTxnCmdHistogramWithRollback.Observe(time.Since(start).Seconds())
	return nil
}

func (txn *tikvTxn) rollbackPessimisticLocks() error {
	if len(txn.lockKeys) == 0 {
		return nil
	}
	return txn.committer.pessimisticRollbackMutations(NewBackoffer(context.Background(), cleanupMaxBackoff), committerMutations{keys: txn.lockKeys})
}

// lockWaitTime in ms, except that kv.LockAlwaysWait(0) means always wait lock, kv.LockNowait(-1) means nowait lock
func (txn *tikvTxn) LockKeys(ctx context.Context, lockCtx *kv.LockCtx, keysInput ...kv.Key) error {
	// Exclude keys that are already locked.
	var err error
	keys := make([][]byte, 0, len(keysInput))
	defer func() {
		if err == nil {
			if lockCtx.PessimisticLockWaited != nil {
				if atomic.LoadInt32(lockCtx.PessimisticLockWaited) > 0 {
					timeWaited := time.Since(lockCtx.WaitStartTime)
					*lockCtx.LockKeysDuration = timeWaited
					metrics.TiKVPessimisticLockKeysDuration.Observe(timeWaited.Seconds())
				}
			}
		}
		if lockCtx.LockKeysCount != nil {
			*lockCtx.LockKeysCount += int32(len(keys))
		}
	}()
	txn.mu.Lock()
	for _, key := range keysInput {
		if _, ok := txn.lockedMap[string(key)]; !ok {
			keys = append(keys, key)
		} else if lockCtx.ReturnValues {
			// An already locked key can not return values, we add an entry to let the caller get the value
			// in other ways.
			lockCtx.Values[string(key)] = kv.ReturnedValue{AlreadyLocked: true}
		}
	}
	txn.mu.Unlock()
	if len(keys) == 0 {
		lockCtx.AlreadyLocked = true
		return nil
	}
	if txn.IsPessimistic() && lockCtx.ForUpdateTS > 0 {
		if txn.committer == nil {
			// connID is used for log.
			var connID uint64
			var err error
			val := ctx.Value(sessionctx.ConnID)
			if val != nil {
				connID = val.(uint64)
			}
			txn.committer, err = newTwoPhaseCommitter(txn, connID)
			if err != nil {
				return err
			}
		}
		var assignedPrimaryKey bool
		if txn.committer.primaryKey == nil {
			txn.committer.primaryKey = keys[0]
			assignedPrimaryKey = true
		}

		bo := NewBackoffer(ctx, pessimisticLockMaxBackoff).WithVars(txn.vars)
		txn.committer.forUpdateTS = lockCtx.ForUpdateTS
		// If the number of keys greater than 1, it can be on different region,
		// concurrently execute on multiple regions may lead to deadlock.
		txn.committer.isFirstLock = len(txn.lockKeys) == 0 && len(keys) == 1
		err = txn.committer.pessimisticLockMutations(bo, lockCtx, committerMutations{keys: keys})
		if lockCtx.Killed != nil {
			// If the kill signal is received during waiting for pessimisticLock,
			// pessimisticLockKeys would handle the error but it doesn't reset the flag.
			// We need to reset the killed flag here.
			atomic.CompareAndSwapUint32(lockCtx.Killed, 1, 0)
		}
		if err != nil {
			for _, key := range keys {
				txn.us.DeleteKeyExistErrInfo(key)
			}
			keyMayBeLocked := terror.ErrorNotEqual(kv.ErrWriteConflict, err) && terror.ErrorNotEqual(kv.ErrKeyExists, err)
			// If there is only 1 key and lock fails, no need to do pessimistic rollback.
			if len(keys) > 1 || keyMayBeLocked {
				wg := txn.asyncPessimisticRollback(ctx, keys)
				if dl, ok := errors.Cause(err).(*ErrDeadlock); ok && hashInKeys(dl.DeadlockKeyHash, keys) {
					dl.IsRetryable = true
					// Wait for the pessimistic rollback to finish before we retry the statement.
					wg.Wait()
					// Sleep a little, wait for the other transaction that blocked by this transaction to acquire the lock.
					time.Sleep(time.Millisecond * 5)
					failpoint.Inject("SingleStmtDeadLockRetrySleep", func() {
						time.Sleep(300 * time.Millisecond)
					})
				}
			}
			if assignedPrimaryKey {
				// unset the primary key if we assigned primary key when failed to lock it.
				txn.committer.primaryKey = nil
			}
			return err
		}
		if assignedPrimaryKey {
			txn.committer.ttlManager.run(txn.committer, lockCtx)
		}
	}
	txn.mu.Lock()
	txn.lockKeys = append(txn.lockKeys, keys...)
	for _, key := range keys {
		txn.lockedMap[string(key)] = struct{}{}
	}
	txn.dirty = true
	txn.mu.Unlock()
	return nil
}

func (txn *tikvTxn) asyncPessimisticRollback(ctx context.Context, keys [][]byte) *sync.WaitGroup {
	// Clone a new committer for execute in background.
	committer := &twoPhaseCommitter{
		store:       txn.committer.store,
		connID:      txn.committer.connID,
		startTS:     txn.committer.startTS,
		forUpdateTS: txn.committer.forUpdateTS,
		primaryKey:  txn.committer.primaryKey,
	}
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		failpoint.Inject("AsyncRollBackSleep", func() {
			time.Sleep(100 * time.Millisecond)
		})
		err := committer.pessimisticRollbackMutations(NewBackoffer(ctx, pessimisticRollbackMaxBackoff).WithVars(txn.vars), committerMutations{keys: keys})
		if err != nil {
			logutil.Logger(ctx).Warn("[kv] pessimisticRollback failed.", zap.Error(err))
		}
		wg.Done()
	}()
	return wg
}

func hashInKeys(deadlockKeyHash uint64, keys [][]byte) bool {
	for _, key := range keys {
		if farm.Fingerprint64(key) == deadlockKeyHash {
			return true
		}
	}
	return false
}

func (txn *tikvTxn) IsReadOnly() bool {
	return !txn.dirty
}

func (txn *tikvTxn) StartTS() uint64 {
	return txn.startTS
}

func (txn *tikvTxn) Valid() bool {
	return txn.valid
}

func (txn *tikvTxn) Len() int {
	return txn.us.Len()
}

func (txn *tikvTxn) Size() int {
	return txn.us.Size()
}

func (txn *tikvTxn) GetMemBuffer() kv.MemBuffer {
	return txn.us.GetMemBuffer()
}

func (txn *tikvTxn) GetSnapshot() kv.Snapshot {
	return txn.snapshot
}
