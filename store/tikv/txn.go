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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"runtime/trace"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	tikverr "github.com/pingcap/tidb/store/tikv/error"
	tikv "github.com/pingcap/tidb/store/tikv/kv"
	"github.com/pingcap/tidb/store/tikv/logutil"
	"github.com/pingcap/tidb/store/tikv/metrics"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/retry"
	"github.com/pingcap/tidb/store/tikv/unionstore"
	"github.com/pingcap/tidb/store/tikv/util"
	"go.uber.org/zap"
)

// MaxTxnTimeUse is the max time a Txn may use (in ms) from its begin to commit.
// We use it to abort the transaction to guarantee GC worker will not influence it.
const MaxTxnTimeUse = 24 * 60 * 60 * 1000

// SchemaAmender is used by pessimistic transactions to amend commit mutations for schema change during 2pc.
type SchemaAmender interface {
	// AmendTxn is the amend entry, new mutations will be generated based on input mutations using schema change info.
	// The returned results are mutations need to prewrite and mutations need to cleanup.
	AmendTxn(ctx context.Context, startInfoSchema SchemaVer, change *RelatedSchemaChange, mutations CommitterMutations) (CommitterMutations, error)
}

// StartTSOption indicates the option when beginning a transaction
// `TxnScope` must be set for each object
// Every other fields are optional, but currently at most one of them can be set
type StartTSOption struct {
	TxnScope string
	StartTS  *uint64
}

// DefaultStartTSOption creates a default StartTSOption, ie. Work in GlobalTxnScope and get start ts when got used
func DefaultStartTSOption() StartTSOption {
	return StartTSOption{TxnScope: oracle.GlobalTxnScope}
}

// SetStartTS returns a new StartTSOption with StartTS set to the given startTS
func (to StartTSOption) SetStartTS(startTS uint64) StartTSOption {
	to.StartTS = &startTS
	return to
}

// SetTxnScope returns a new StartTSOption with TxnScope set to txnScope
func (to StartTSOption) SetTxnScope(txnScope string) StartTSOption {
	to.TxnScope = txnScope
	return to
}

// KVTxn contains methods to interact with a TiKV transaction.
type KVTxn struct {
	snapshot  *KVSnapshot
	us        *unionstore.KVUnionStore
	store     *KVStore // for connection to region.
	startTS   uint64
	startTime time.Time // Monotonic timestamp for recording txn time consuming.
	commitTS  uint64
	mu        sync.Mutex // For thread-safe LockKeys function.
	setCnt    int64
	vars      *tikv.Variables
	committer *twoPhaseCommitter
	lockedCnt int

	valid bool

	// schemaVer is the infoSchema fetched at startTS.
	schemaVer SchemaVer
	// SchemaAmender is used amend pessimistic txn commit mutations for schema change
	schemaAmender SchemaAmender
	// commitCallback is called after current transaction gets committed
	commitCallback func(info string, err error)

	binlog             BinlogExecutor
	schemaLeaseChecker SchemaLeaseChecker
	syncLog            bool
	priority           Priority
	isPessimistic      bool
	enableAsyncCommit  bool
	enable1PC          bool
	causalConsistency  bool
	scope              string
	kvFilter           KVFilter
	resourceGroupTag   []byte
}

// ExtractStartTS use `option` to get the proper startTS for a transaction.
func ExtractStartTS(store *KVStore, option StartTSOption) (uint64, error) {
	if option.StartTS != nil {
		return *option.StartTS, nil
	}
	bo := NewBackofferWithVars(context.Background(), tsoMaxBackoff, nil)
	return store.getTimestampWithRetry(bo, option.TxnScope)
}

func newTiKVTxnWithOptions(store *KVStore, options StartTSOption) (*KVTxn, error) {
	if options.TxnScope == "" {
		options.TxnScope = oracle.GlobalTxnScope
	}
	startTS, err := ExtractStartTS(store, options)
	if err != nil {
		return nil, errors.Trace(err)
	}
	snapshot := newTiKVSnapshot(store, startTS, store.nextReplicaReadSeed())
	newTiKVTxn := &KVTxn{
		snapshot:  snapshot,
		us:        unionstore.NewUnionStore(snapshot),
		store:     store,
		startTS:   startTS,
		startTime: time.Now(),
		valid:     true,
		vars:      tikv.DefaultVars,
		scope:     options.TxnScope,
	}
	return newTiKVTxn, nil
}

// SetSuccess is used to probe if kv variables are set or not. It is ONLY used in test cases.
var SetSuccess = false

// SetVars sets variables to the transaction.
func (txn *KVTxn) SetVars(vars *tikv.Variables) {
	txn.vars = vars
	txn.snapshot.vars = vars
	failpoint.Inject("probeSetVars", func(val failpoint.Value) {
		if val.(bool) {
			SetSuccess = true
		}
	})
}

// GetVars gets variables from the transaction.
func (txn *KVTxn) GetVars() *tikv.Variables {
	return txn.vars
}

// Get implements transaction interface.
func (txn *KVTxn) Get(ctx context.Context, k []byte) ([]byte, error) {
	ret, err := txn.us.Get(ctx, k)
	if tikverr.IsErrNotFound(err) {
		return nil, err
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	return ret, nil
}

// Set sets the value for key k as v into kv store.
// v must NOT be nil or empty, otherwise it returns ErrCannotSetNilValue.
func (txn *KVTxn) Set(k []byte, v []byte) error {
	txn.setCnt++
	return txn.us.GetMemBuffer().Set(k, v)
}

// String implements fmt.Stringer interface.
func (txn *KVTxn) String() string {
	return fmt.Sprintf("%d", txn.StartTS())
}

// Iter creates an Iterator positioned on the first entry that k <= entry's key.
// If such entry is not found, it returns an invalid Iterator with no error.
// It yields only keys that < upperBound. If upperBound is nil, it means the upperBound is unbounded.
// The Iterator must be Closed after use.
func (txn *KVTxn) Iter(k []byte, upperBound []byte) (unionstore.Iterator, error) {
	return txn.us.Iter(k, upperBound)
}

// IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
func (txn *KVTxn) IterReverse(k []byte) (unionstore.Iterator, error) {
	return txn.us.IterReverse(k)
}

// Delete removes the entry for key k from kv store.
func (txn *KVTxn) Delete(k []byte) error {
	return txn.us.GetMemBuffer().Delete(k)
}

// SetSchemaLeaseChecker sets a hook to check schema version.
func (txn *KVTxn) SetSchemaLeaseChecker(checker SchemaLeaseChecker) {
	txn.schemaLeaseChecker = checker
}

// EnableForceSyncLog indicates tikv to always sync log for the transaction.
func (txn *KVTxn) EnableForceSyncLog() {
	txn.syncLog = true
}

// SetPessimistic indicates if the transaction should use pessimictic lock.
func (txn *KVTxn) SetPessimistic(b bool) {
	txn.isPessimistic = b
}

// SetSchemaVer updates schema version to validate transaction.
func (txn *KVTxn) SetSchemaVer(schemaVer SchemaVer) {
	txn.schemaVer = schemaVer
}

// SetPriority sets the priority for both write and read.
func (txn *KVTxn) SetPriority(pri Priority) {
	txn.priority = pri
	txn.GetSnapshot().SetPriority(pri)
}

// SetResourceGroupTag sets the resource tag for both write and read.
func (txn *KVTxn) SetResourceGroupTag(tag []byte) {
	txn.resourceGroupTag = tag
	txn.GetSnapshot().SetResourceGroupTag(tag)
}

// SetSchemaAmender sets an amender to update mutations after schema change.
func (txn *KVTxn) SetSchemaAmender(sa SchemaAmender) {
	txn.schemaAmender = sa
}

// SetCommitCallback sets up a function that will be called when the transaction
// is finished.
func (txn *KVTxn) SetCommitCallback(f func(string, error)) {
	txn.commitCallback = f
}

// SetEnableAsyncCommit indicates if the transaction will try to use async commit.
func (txn *KVTxn) SetEnableAsyncCommit(b bool) {
	txn.enableAsyncCommit = b
}

// SetEnable1PC indicates if the transaction will try to use 1 phase commit.
func (txn *KVTxn) SetEnable1PC(b bool) {
	txn.enable1PC = b
}

// SetCausalConsistency indicates if the transaction does not need to
// guarantee linearizability. Default value is false which means
// linearizability is guaranteed.
func (txn *KVTxn) SetCausalConsistency(b bool) {
	txn.causalConsistency = b
}

// SetScope sets the geographical scope of the transaction.
func (txn *KVTxn) SetScope(scope string) {
	txn.scope = scope
}

// SetKVFilter sets the filter to ignore key-values in memory buffer.
func (txn *KVTxn) SetKVFilter(filter KVFilter) {
	txn.kvFilter = filter
}

// IsPessimistic returns true if it is pessimistic.
func (txn *KVTxn) IsPessimistic() bool {
	return txn.isPessimistic
}

// IsCasualConsistency returns if the transaction allows linearizability
// inconsistency.
func (txn *KVTxn) IsCasualConsistency() bool {
	return txn.causalConsistency
}

// GetScope returns the geographical scope of the transaction.
func (txn *KVTxn) GetScope() string {
	return txn.scope
}

// Commit commits the transaction operations to KV store.
func (txn *KVTxn) Commit(ctx context.Context) error {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("tikvTxn.Commit", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	defer trace.StartRegion(ctx, "CommitTxn").End()

	if !txn.valid {
		return tikverr.ErrInvalidTxn
	}
	defer txn.close()

	failpoint.Inject("mockCommitError", func(val failpoint.Value) {
		if val.(bool) && IsMockCommitErrorEnable() {
			MockCommitErrorDisable()
			failpoint.Return(errors.New("mock commit error"))
		}
	})

	start := time.Now()
	defer func() { metrics.TxnCmdHistogramWithCommit.Observe(time.Since(start).Seconds()) }()

	// sessionID is used for log.
	var sessionID uint64
	val := ctx.Value(util.SessionID)
	if val != nil {
		sessionID = val.(uint64)
	}

	var err error
	// If the txn use pessimistic lock, committer is initialized.
	committer := txn.committer
	if committer == nil {
		committer, err = newTwoPhaseCommitter(txn, sessionID)
		if err != nil {
			return errors.Trace(err)
		}
		txn.committer = committer
	}
	defer committer.ttlManager.close()

	initRegion := trace.StartRegion(ctx, "InitKeys")
	err = committer.initKeysAndMutations()
	initRegion.End()
	if err != nil {
		return errors.Trace(err)
	}
	if committer.mutations.Len() == 0 {
		return nil
	}

	defer func() {
		detail := committer.getDetail()
		detail.Mu.Lock()
		metrics.TiKVTxnCommitBackoffSeconds.Observe(float64(detail.Mu.CommitBackoffTime) / float64(time.Second))
		metrics.TiKVTxnCommitBackoffCount.Observe(float64(len(detail.Mu.BackoffTypes)))
		detail.Mu.Unlock()

		ctxValue := ctx.Value(util.CommitDetailCtxKey)
		if ctxValue != nil {
			commitDetail := ctxValue.(**util.CommitDetails)
			if *commitDetail != nil {
				(*commitDetail).TxnRetry++
			} else {
				*commitDetail = detail
			}
		}
	}()
	// latches disabled
	// pessimistic transaction should also bypass latch.
	if txn.store.txnLatches == nil || txn.IsPessimistic() {
		err = committer.execute(ctx)
		if val == nil || sessionID > 0 {
			txn.onCommitted(err)
		}
		logutil.Logger(ctx).Debug("[kv] txnLatches disabled, 2pc directly", zap.Error(err))
		return errors.Trace(err)
	}

	// latches enabled
	// for transactions which need to acquire latches
	start = time.Now()
	lock := txn.store.txnLatches.Lock(committer.startTS, committer.mutations.GetKeys())
	commitDetail := committer.getDetail()
	commitDetail.LocalLatchTime = time.Since(start)
	if commitDetail.LocalLatchTime > 0 {
		metrics.TiKVLocalLatchWaitTimeHistogram.Observe(commitDetail.LocalLatchTime.Seconds())
	}
	defer txn.store.txnLatches.UnLock(lock)
	if lock.IsStale() {
		return &tikverr.ErrWriteConflictInLatch{StartTS: txn.startTS}
	}
	err = committer.execute(ctx)
	if val == nil || sessionID > 0 {
		txn.onCommitted(err)
	}
	if err == nil {
		lock.SetCommitTS(committer.commitTS)
	}
	logutil.Logger(ctx).Debug("[kv] txnLatches enabled while txn retryable", zap.Error(err))
	return errors.Trace(err)
}

func (txn *KVTxn) close() {
	txn.valid = false
}

// Rollback undoes the transaction operations to KV store.
func (txn *KVTxn) Rollback() error {
	if !txn.valid {
		return tikverr.ErrInvalidTxn
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
	metrics.TxnCmdHistogramWithRollback.Observe(time.Since(start).Seconds())
	return nil
}

func (txn *KVTxn) rollbackPessimisticLocks() error {
	if txn.lockedCnt == 0 {
		return nil
	}
	bo := retry.NewBackofferWithVars(context.Background(), cleanupMaxBackoff, txn.vars)
	keys := txn.collectLockedKeys()
	return txn.committer.pessimisticRollbackMutations(bo, &PlainMutations{keys: keys})
}

func (txn *KVTxn) collectLockedKeys() [][]byte {
	keys := make([][]byte, 0, txn.lockedCnt)
	buf := txn.GetMemBuffer()
	var err error
	for it := buf.IterWithFlags(nil, nil); it.Valid(); err = it.Next() {
		_ = err
		if it.Flags().HasLocked() {
			keys = append(keys, it.Key())
		}
	}
	return keys
}

// TxnInfo is used to keep track the info of a committed transaction (mainly for diagnosis and testing)
type TxnInfo struct {
	TxnScope            string `json:"txn_scope"`
	StartTS             uint64 `json:"start_ts"`
	CommitTS            uint64 `json:"commit_ts"`
	TxnCommitMode       string `json:"txn_commit_mode"`
	AsyncCommitFallback bool   `json:"async_commit_fallback"`
	OnePCFallback       bool   `json:"one_pc_fallback"`
	ErrMsg              string `json:"error,omitempty"`
}

func (txn *KVTxn) onCommitted(err error) {
	if txn.commitCallback != nil {
		isAsyncCommit := txn.committer.isAsyncCommit()
		isOnePC := txn.committer.isOnePC()

		commitMode := "2pc"
		if isOnePC {
			commitMode = "1pc"
		} else if isAsyncCommit {
			commitMode = "async_commit"
		}

		info := TxnInfo{
			TxnScope:            txn.GetScope(),
			StartTS:             txn.startTS,
			CommitTS:            txn.commitTS,
			TxnCommitMode:       commitMode,
			AsyncCommitFallback: txn.committer.hasTriedAsyncCommit && !isAsyncCommit,
			OnePCFallback:       txn.committer.hasTriedOnePC && !isOnePC,
		}
		if err != nil {
			info.ErrMsg = err.Error()
		}
		infoStr, err2 := json.Marshal(info)
		_ = err2
		txn.commitCallback(string(infoStr), err)
	}
}

// LockKeys tries to lock the entries with the keys in KV store.
// lockWaitTime in ms, except that kv.LockAlwaysWait(0) means always wait lock, kv.LockNowait(-1) means nowait lock
func (txn *KVTxn) LockKeys(ctx context.Context, lockCtx *tikv.LockCtx, keysInput ...[]byte) error {
	// Exclude keys that are already locked.
	var err error
	keys := make([][]byte, 0, len(keysInput))
	startTime := time.Now()
	txn.mu.Lock()
	defer txn.mu.Unlock()
	defer func() {
		metrics.TxnCmdHistogramWithLockKeys.Observe(time.Since(startTime).Seconds())
		if err == nil {
			if lockCtx.PessimisticLockWaited != nil {
				if atomic.LoadInt32(lockCtx.PessimisticLockWaited) > 0 {
					timeWaited := time.Since(lockCtx.WaitStartTime)
					atomic.StoreInt64(lockCtx.LockKeysDuration, int64(timeWaited))
					metrics.TiKVPessimisticLockKeysDuration.Observe(timeWaited.Seconds())
				}
			}
		}
		if lockCtx.LockKeysCount != nil {
			*lockCtx.LockKeysCount += int32(len(keys))
		}
		if lockCtx.Stats != nil {
			lockCtx.Stats.TotalTime = time.Since(startTime)
			ctxValue := ctx.Value(util.LockKeysDetailCtxKey)
			if ctxValue != nil {
				lockKeysDetail := ctxValue.(**util.LockKeysDetails)
				*lockKeysDetail = lockCtx.Stats
			}
		}
	}()
	memBuf := txn.us.GetMemBuffer()
	for _, key := range keysInput {
		// The value of lockedMap is only used by pessimistic transactions.
		var valueExist, locked, checkKeyExists bool
		if flags, err := memBuf.GetFlags(key); err == nil {
			locked = flags.HasLocked()
			valueExist = flags.HasLockedValueExists()
			checkKeyExists = flags.HasNeedCheckExists()
		}
		if !locked {
			keys = append(keys, key)
		} else if txn.IsPessimistic() {
			if checkKeyExists && valueExist {
				alreadyExist := kvrpcpb.AlreadyExist{Key: key}
				e := &tikverr.ErrKeyExist{AlreadyExist: &alreadyExist}
				return txn.committer.extractKeyExistsErr(e)
			}
		}
		if lockCtx.ReturnValues && locked {
			// An already locked key can not return values, we add an entry to let the caller get the value
			// in other ways.
			lockCtx.Values[string(key)] = tikv.ReturnedValue{AlreadyLocked: true}
		}
	}
	if len(keys) == 0 {
		return nil
	}
	keys = deduplicateKeys(keys)
	if txn.IsPessimistic() && lockCtx.ForUpdateTS > 0 {
		if txn.committer == nil {
			// sessionID is used for log.
			var sessionID uint64
			var err error
			val := ctx.Value(util.SessionID)
			if val != nil {
				sessionID = val.(uint64)
			}
			txn.committer, err = newTwoPhaseCommitter(txn, sessionID)
			if err != nil {
				return err
			}
		}
		var assignedPrimaryKey bool
		if txn.committer.primaryKey == nil {
			txn.committer.primaryKey = keys[0]
			assignedPrimaryKey = true
		}

		lockCtx.Stats = &util.LockKeysDetails{
			LockKeys: int32(len(keys)),
		}
		bo := retry.NewBackofferWithVars(ctx, pessimisticLockMaxBackoff, txn.vars)
		txn.committer.forUpdateTS = lockCtx.ForUpdateTS
		// If the number of keys greater than 1, it can be on different region,
		// concurrently execute on multiple regions may lead to deadlock.
		txn.committer.isFirstLock = txn.lockedCnt == 0 && len(keys) == 1
		err = txn.committer.pessimisticLockMutations(bo, lockCtx, &PlainMutations{keys: keys})
		if bo.GetTotalSleep() > 0 {
			atomic.AddInt64(&lockCtx.Stats.BackoffTime, int64(bo.GetTotalSleep())*int64(time.Millisecond))
			lockCtx.Stats.Mu.Lock()
			lockCtx.Stats.Mu.BackoffTypes = append(lockCtx.Stats.Mu.BackoffTypes, bo.GetTypes()...)
			lockCtx.Stats.Mu.Unlock()
		}
		if lockCtx.Killed != nil {
			// If the kill signal is received during waiting for pessimisticLock,
			// pessimisticLockKeys would handle the error but it doesn't reset the flag.
			// We need to reset the killed flag here.
			atomic.CompareAndSwapUint32(lockCtx.Killed, 1, 0)
		}
		if err != nil {
			for _, key := range keys {
				if txn.us.HasPresumeKeyNotExists(key) {
					txn.us.UnmarkPresumeKeyNotExists(key)
				}
			}
			keyMayBeLocked := !(tikverr.IsErrWriteConflict(err) || tikverr.IsErrKeyExist(err))
			// If there is only 1 key and lock fails, no need to do pessimistic rollback.
			if len(keys) > 1 || keyMayBeLocked {
				dl, isDeadlock := errors.Cause(err).(*tikverr.ErrDeadlock)
				if isDeadlock {
					if hashInKeys(dl.DeadlockKeyHash, keys) {
						dl.IsRetryable = true
					}
					if lockCtx.OnDeadlock != nil {
						// Call OnDeadlock before pessimistic rollback.
						lockCtx.OnDeadlock(dl)
					}
				}
				wg := txn.asyncPessimisticRollback(ctx, keys)
				if isDeadlock {
					logutil.Logger(ctx).Debug("deadlock error received", zap.Uint64("startTS", txn.startTS), zap.Stringer("deadlockInfo", dl))
					if dl.IsRetryable {
						// Wait for the pessimistic rollback to finish before we retry the statement.
						wg.Wait()
						// Sleep a little, wait for the other transaction that blocked by this transaction to acquire the lock.
						time.Sleep(time.Millisecond * 5)
						failpoint.Inject("SingleStmtDeadLockRetrySleep", func() {
							time.Sleep(300 * time.Millisecond)
						})
					}
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
	for _, key := range keys {
		valExists := tikv.SetKeyLockedValueExists
		// PointGet and BatchPointGet will return value in pessimistic lock response, the value may not exist.
		// For other lock modes, the locked key values always exist.
		if lockCtx.ReturnValues {
			val := lockCtx.Values[string(key)]
			if len(val.Value) == 0 {
				valExists = tikv.SetKeyLockedValueNotExists
			}
		}
		memBuf.UpdateFlags(key, tikv.SetKeyLocked, tikv.DelNeedCheckExists, valExists)
	}
	txn.lockedCnt += len(keys)
	return nil
}

// deduplicateKeys deduplicate the keys, it use sort instead of map to avoid memory allocation.
func deduplicateKeys(keys [][]byte) [][]byte {
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})
	deduped := keys[:1]
	for i := 1; i < len(keys); i++ {
		if !bytes.Equal(deduped[len(deduped)-1], keys[i]) {
			deduped = append(deduped, keys[i])
		}
	}
	return deduped
}

const pessimisticRollbackMaxBackoff = 20000

func (txn *KVTxn) asyncPessimisticRollback(ctx context.Context, keys [][]byte) *sync.WaitGroup {
	// Clone a new committer for execute in background.
	committer := &twoPhaseCommitter{
		store:       txn.committer.store,
		sessionID:   txn.committer.sessionID,
		startTS:     txn.committer.startTS,
		forUpdateTS: txn.committer.forUpdateTS,
		primaryKey:  txn.committer.primaryKey,
	}
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		failpoint.Inject("beforeAsyncPessimisticRollback", func(val failpoint.Value) {
			if s, ok := val.(string); ok {
				if s == "skip" {
					logutil.Logger(ctx).Info("[failpoint] injected skip async pessimistic rollback",
						zap.Uint64("txnStartTS", txn.startTS))
					wg.Done()
					failpoint.Return()
				} else if s == "delay" {
					duration := time.Duration(rand.Int63n(int64(time.Second) * 2))
					logutil.Logger(ctx).Info("[failpoint] injected delay before async pessimistic rollback",
						zap.Uint64("txnStartTS", txn.startTS), zap.Duration("duration", duration))
					time.Sleep(duration)
				}
			}
		})

		err := committer.pessimisticRollbackMutations(retry.NewBackofferWithVars(ctx, pessimisticRollbackMaxBackoff, txn.vars), &PlainMutations{keys: keys})
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

// IsReadOnly checks if the transaction has only performed read operations.
func (txn *KVTxn) IsReadOnly() bool {
	return !txn.us.GetMemBuffer().Dirty()
}

// StartTS returns the transaction start timestamp.
func (txn *KVTxn) StartTS() uint64 {
	return txn.startTS
}

// Valid returns if the transaction is valid.
// A transaction become invalid after commit or rollback.
func (txn *KVTxn) Valid() bool {
	return txn.valid
}

// Len returns the number of entries in the DB.
func (txn *KVTxn) Len() int {
	return txn.us.GetMemBuffer().Len()
}

// Size returns sum of keys and values length.
func (txn *KVTxn) Size() int {
	return txn.us.GetMemBuffer().Size()
}

// Reset reset the Transaction to initial states.
func (txn *KVTxn) Reset() {
	txn.us.GetMemBuffer().Reset()
}

// GetUnionStore returns the UnionStore binding to this transaction.
func (txn *KVTxn) GetUnionStore() *unionstore.KVUnionStore {
	return txn.us
}

// GetMemBuffer return the MemBuffer binding to this transaction.
func (txn *KVTxn) GetMemBuffer() *unionstore.MemDB {
	return txn.us.GetMemBuffer()
}

// GetSnapshot returns the Snapshot binding to this transaction.
func (txn *KVTxn) GetSnapshot() *KVSnapshot {
	return txn.snapshot
}

// SetBinlogExecutor sets the method to perform binlong synchronization.
func (txn *KVTxn) SetBinlogExecutor(binlog BinlogExecutor) {
	txn.binlog = binlog
	if txn.committer != nil {
		txn.committer.binlog = binlog
	}
}

// GetClusterID returns store's cluster id.
func (txn *KVTxn) GetClusterID() uint64 {
	return txn.store.clusterID
}
