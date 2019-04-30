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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	_ kv.Transaction = (*tikvTxn)(nil)
)

var (
	tikvTxnCmdCountWithGet             = metrics.TiKVTxnCmdCounter.WithLabelValues("get")
	tikvTxnCmdHistogramWithGet         = metrics.TiKVTxnCmdHistogram.WithLabelValues("get")
	tikvTxnCmdCountWithSeek            = metrics.TiKVTxnCmdCounter.WithLabelValues("seek")
	tikvTxnCmdHistogramWithSeek        = metrics.TiKVTxnCmdHistogram.WithLabelValues("seek")
	tikvTxnCmdCountWithSeekReverse     = metrics.TiKVTxnCmdCounter.WithLabelValues("seek_reverse")
	tikvTxnCmdHistogramWithSeekReverse = metrics.TiKVTxnCmdHistogram.WithLabelValues("seek_reverse")
	tikvTxnCmdCountWithDelete          = metrics.TiKVTxnCmdCounter.WithLabelValues("delete")
	tikvTxnCmdCountWithSet             = metrics.TiKVTxnCmdCounter.WithLabelValues("set")
	tikvTxnCmdCountWithCommit          = metrics.TiKVTxnCmdCounter.WithLabelValues("commit")
	tikvTxnCmdHistogramWithCommit      = metrics.TiKVTxnCmdHistogram.WithLabelValues("commit")
	tikvTxnCmdCountWithRollback        = metrics.TiKVTxnCmdCounter.WithLabelValues("rollback")
	tikvTxnCmdHistogramWithLockKeys    = metrics.TiKVTxnCmdCounter.WithLabelValues("lock_keys")
)

// tikvTxn implements kv.Transaction.
type tikvTxn struct {
	snapshot  *tikvSnapshot
	us        kv.UnionStore
	store     *tikvStore // for connection to region.
	startTS   uint64
	startTime time.Time // Monotonic timestamp for recording txn time consuming.
	commitTS  uint64
	valid     bool
	lockKeys  [][]byte
	mu        sync.Mutex // For thread-safe LockKeys function.
	dirty     bool
	setCnt    int64
	vars      *kv.Variables

	// For data consistency check.
	assertions []assertionPair
}

func newTiKVTxn(store *tikvStore) (*tikvTxn, error) {
	bo := NewBackoffer(context.Background(), tsoMaxBackoff)
	startTS, err := store.getTimestampWithRetry(bo)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return newTikvTxnWithStartTS(store, startTS)
}

// newTikvTxnWithStartTS creates a txn with startTS.
func newTikvTxnWithStartTS(store *tikvStore, startTS uint64) (*tikvTxn, error) {
	ver := kv.NewVersion(startTS)
	snapshot := newTiKVSnapshot(store, ver)
	return &tikvTxn{
		snapshot:  snapshot,
		us:        kv.NewUnionStore(snapshot),
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

// SetAssertion sets a assertion for the key operation.
func (txn *tikvTxn) SetAssertion(key kv.Key, assertion kv.AssertionType) {
	txn.assertions = append(txn.assertions, assertionPair{key, assertion})
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
func (txn *tikvTxn) Get(k kv.Key) ([]byte, error) {
	tikvTxnCmdCountWithGet.Inc()
	start := time.Now()
	defer func() { tikvTxnCmdHistogramWithGet.Observe(time.Since(start).Seconds()) }()

	ret, err := txn.us.Get(k)
	if kv.IsErrNotFound(err) {
		return nil, err
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = txn.store.CheckVisibility(txn.startTS)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return ret, nil
}

func (txn *tikvTxn) BatchGet(keys []kv.Key) (map[string][]byte, error) {
	if txn.IsReadOnly() {
		return txn.snapshot.BatchGet(keys)
	}
	bufferValues := make([][]byte, len(keys))
	shrinkKeys := make([]kv.Key, 0, len(keys))
	for i, key := range keys {
		val, err := txn.GetMemBuffer().Get(key)
		if kv.IsErrNotFound(err) {
			shrinkKeys = append(shrinkKeys, key)
			continue
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		if len(val) != 0 {
			bufferValues[i] = val
		}
	}
	storageValues, err := txn.snapshot.BatchGet(shrinkKeys)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for i, key := range keys {
		if bufferValues[i] == nil {
			continue
		}
		storageValues[string(key)] = bufferValues[i]
	}
	return storageValues, nil
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
	tikvTxnCmdCountWithSeek.Inc()
	start := time.Now()
	defer func() { tikvTxnCmdHistogramWithSeek.Observe(time.Since(start).Seconds()) }()

	return txn.us.Iter(k, upperBound)
}

// IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
func (txn *tikvTxn) IterReverse(k kv.Key) (kv.Iterator, error) {
	tikvTxnCmdCountWithSeekReverse.Inc()
	start := time.Now()
	defer func() {
		tikvTxnCmdHistogramWithSeekReverse.Observe(time.Since(start).Seconds())
	}()

	return txn.us.IterReverse(k)
}

func (txn *tikvTxn) Delete(k kv.Key) error {
	tikvTxnCmdCountWithDelete.Inc()

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
	}
}

func (txn *tikvTxn) DelOption(opt kv.Option) {
	txn.us.DelOption(opt)
}

func (txn *tikvTxn) Commit(ctx context.Context) error {
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

	tikvTxnCmdCountWithSet.Add(float64(txn.setCnt))
	tikvTxnCmdCountWithCommit.Inc()
	start := time.Now()
	defer func() { tikvTxnCmdHistogramWithCommit.Observe(time.Since(start).Seconds()) }()

	// connID is used for log.
	var connID uint64
	val := ctx.Value(sessionctx.ConnID)
	if val != nil {
		connID = val.(uint64)
	}
	committer, err := newTwoPhaseCommitter(txn, connID)
	if err != nil || committer == nil {
		return errors.Trace(err)
	}
	defer func() {
		ctxValue := ctx.Value(execdetails.CommitDetailCtxKey)
		if ctxValue != nil {
			commitDetail := ctxValue.(**execdetails.CommitDetails)
			if *commitDetail != nil {
				(*commitDetail).TxnRetry += 1
			} else {
				*commitDetail = committer.detail
			}
		}
	}()
	// latches disabled
	if txn.store.txnLatches == nil {
		err = committer.executeAndWriteFinishBinlog(ctx)
		logutil.Logger(ctx).Debug("[kv] txnLatches disabled, 2pc directly", zap.Error(err))
		return errors.Trace(err)
	}

	// latches enabled
	// for transactions which need to acquire latches
	start = time.Now()
	lock := txn.store.txnLatches.Lock(committer.startTS, committer.keys)
	committer.detail.LocalLatchTime = time.Since(start)
	if committer.detail.LocalLatchTime > 0 {
		metrics.TiKVLocalLatchWaitTimeHistogram.Observe(committer.detail.LocalLatchTime.Seconds())
	}
	defer txn.store.txnLatches.UnLock(lock)
	if lock.IsStale() {
		err = errors.Errorf("%s txnStartTS %d is stale", util.WriteConflictMarker, txn.startTS)
		return errors.Annotate(err, txnRetryableMark)
	}
	err = committer.executeAndWriteFinishBinlog(ctx)
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
	txn.close()
	logutil.Logger(context.Background()).Debug("[kv] rollback txn", zap.Uint64("txnStartTS", txn.StartTS()))
	tikvTxnCmdCountWithRollback.Inc()

	return nil
}

func (txn *tikvTxn) LockKeys(keys ...kv.Key) error {
	tikvTxnCmdHistogramWithLockKeys.Inc()
	txn.mu.Lock()
	for _, key := range keys {
		txn.lockKeys = append(txn.lockKeys, key)
	}
	txn.dirty = true
	txn.mu.Unlock()
	return nil
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
