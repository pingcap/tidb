// Copyright 2018 PingCAP, Inc.

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

package txnstate

import (
	"bytes"
	"context"
	"fmt"
	"github.com/pingcap/tidb/util/logutil"
	"strings"
	"sync/atomic"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tipb/go-binlog"
	"go.uber.org/zap"
)

// TxnState wraps kv.Transaction to provide a new kv.Transaction.
// 1. It holds all statement related modification in the buffer before flush to the txn,
// so if execute statement meets error, the txn won't be made dirty.
// 2. It's a lazy transaction, that means it's a TxnFuture before StartTS() is really need.
type TxnState struct {
	// States of a TxnState should be one of the followings:
	// Invalid: kv.Transaction == nil && txnFuture == nil
	// Pending: kv.Transaction == nil && txnFuture != nil
	// Valid:	kv.Transaction != nil && txnFuture == nil
	kv.Transaction
	txnFuture *TxnFuture

	buf          kv.MemBuffer
	mutations    map[int64]*binlog.TableMutation
	dirtyTableOP []dirtyTableOperation

	// If DoNotCommit is not nil, Commit() will not commit the transaction.
	// DoNotCommit flag may be set when StmtCommit fail.
	DoNotCommit error
}

// Default Initializes a `TxnState` value with the default memory buffer size.
func (st *TxnState) Default() {
	st.buf = kv.NewMemDbBuffer(kv.DefaultTxnMembufCap)
	st.mutations = make(map[int64]*binlog.TableMutation)
}

// Size implements the MemBuffer interface.
func (st *TxnState) Size() int {
	return st.buf.Size()
}

// Valid implements the kv.Transaction interface.
func (st *TxnState) Valid() bool {
	return st.Transaction != nil && st.Transaction.Valid()
}

// Pending checks the transaction is pending or not.
func (st *TxnState) Pending() bool {
	return st.Transaction == nil && st.txnFuture != nil
}

// ValidOrPending checks whether the transaction is valid or pending.
func (st *TxnState) ValidOrPending() bool {
	return st.txnFuture != nil || st.Valid()
}

func (st *TxnState) String() string {
	if st.Transaction != nil {
		return st.Transaction.String()
	}
	if st.txnFuture != nil {
		return "txnFuture"
	}
	return "invalid transaction"
}

// GoString implements the "%#v" format for fmt.Printf.
func (st *TxnState) GoString() string {
	var s strings.Builder
	s.WriteString("Txn{")
	if st.Pending() {
		s.WriteString("state=pending")
	} else if st.Valid() {
		s.WriteString("state=valid")
		fmt.Fprintf(&s, ", txnStartTS=%d", st.Transaction.StartTS())
		if len(st.dirtyTableOP) > 0 {
			fmt.Fprintf(&s, ", len(dirtyTable)=%d, %#v", len(st.dirtyTableOP), st.dirtyTableOP)
		}
		if len(st.mutations) > 0 {
			fmt.Fprintf(&s, ", len(mutations)=%d, %#v", len(st.mutations), st.mutations)
		}
		if st.buf != nil && st.buf.Len() != 0 {
			fmt.Fprintf(&s, ", buf.length: %d, buf.size: %d", st.buf.Len(), st.buf.Size())
		}
	} else {
		s.WriteString("state=invalid")
	}

	s.WriteString("}")
	return s.String()
}

// ChangeInvalidToValid changes state from invalid to valid.
func (st *TxnState) ChangeInvalidToValid(txn kv.Transaction) {
	if st.ValidOrPending() {
		panic("ChangeInvalidToValid must be called when invalid")
	}
	st.Transaction = txn
	st.txnFuture = nil
}

// ChangeInvalidToPending changes state from invalid to pending.
func (st *TxnState) ChangeInvalidToPending(future *TxnFuture) {
	if st.ValidOrPending() {
		panic("ChangeInvalidToPending must be called when invalid")
	}
	st.Transaction = nil
	st.txnFuture = future
}

// ChangePendingToValid changes state from pending to valid.
func (st *TxnState) ChangePendingToValid(txnCap int) error {
	if !st.ValidOrPending() {
		panic("ChangePendingToValid must be called when pending or valid")
	} else if st.Valid() {
		// Do nothing if the transaction is already valid.
		return nil
	}

	future := st.txnFuture
	st.txnFuture = nil

	txn, err := future.wait()
	if err != nil {
		return err
	}
	txn.SetCap(txnCap)
	st.Transaction = txn
	return nil
}

// ChangeToInvalid marks the transaction as invalid.
func (st *TxnState) ChangeToInvalid() {
	st.Transaction = nil
	st.txnFuture = nil
}

func (st *TxnState) reset() {
	st.DoNotCommit = nil
	st.Cleanup()
	st.ChangeToInvalid()
}

// Cleanup clears the internal buf and dirtyTableOP.
func (st *TxnState) Cleanup() {
	const sz4M = 4 << 20
	if st.buf.Size() > sz4M {
		// The memory footprint for the large transaction could be huge here.
		// Each active session has its own buffer, we should free the buffer to
		// avoid memory leak.
		st.buf = kv.NewMemDbBuffer(kv.DefaultTxnMembufCap)
	} else {
		st.buf.Reset()
	}
	for key := range st.mutations {
		delete(st.mutations, key)
	}
	if st.dirtyTableOP != nil {
		empty := dirtyTableOperation{}
		for i := 0; i < len(st.dirtyTableOP); i++ {
			st.dirtyTableOP[i] = empty
		}
		if len(st.dirtyTableOP) > 256 {
			// Reduce memory footprint for the large transaction.
			st.dirtyTableOP = nil
		} else {
			st.dirtyTableOP = st.dirtyTableOP[:0]
		}
	}
}

// Commit overrides the Transaction interface.
func (st *TxnState) Commit(ctx context.Context) error {
	defer st.reset()
	if len(st.mutations) != 0 || len(st.dirtyTableOP) != 0 || st.buf.Len() != 0 {
		logutil.BgLogger().Error("the code should never run here",
			zap.String("TxnState", st.GoString()),
			zap.Stack("something must be wrong"))
		return errors.New("invalid transaction")
	}
	if st.DoNotCommit != nil {
		if err1 := st.Transaction.Rollback(); err1 != nil {
			logutil.BgLogger().Error("rollback error", zap.Error(err1))
		}
		return errors.Trace(st.DoNotCommit)
	}

	// mockCommitError8942 is used for PR #8942.
	failpoint.Inject("mockCommitError8942", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(kv.ErrTxnRetryable)
		}
	})

	// mockCommitRetryForAutoID is used to mock an commit retry for adjustAutoIncrementDatum.
	failpoint.Inject("mockCommitRetryForAutoID", func(val failpoint.Value) {
		if val.(bool) && !mockAutoIDRetry() {
			enableMockAutoIDRetry()
			failpoint.Return(kv.ErrTxnRetryable)
		}
	})

	return st.Transaction.Commit(ctx)
}

// Rollback overrides the Transaction interface.
func (st *TxnState) Rollback() error {
	defer st.reset()
	return st.Transaction.Rollback()
}

// Get overrides the Transaction interface.
func (st *TxnState) Get(ctx context.Context, k kv.Key) ([]byte, error) {
	val, err := st.buf.Get(ctx, k)
	if kv.IsErrNotFound(err) {
		val, err = st.Transaction.Get(ctx, k)
		if kv.IsErrNotFound(err) {
			return nil, err
		}
	}
	if err != nil {
		return nil, err
	}
	if len(val) == 0 {
		return nil, kv.ErrNotExist
	}
	return val, nil
}

// BatchGet overrides the Transaction interface.
func (st *TxnState) BatchGet(ctx context.Context, keys []kv.Key) (map[string][]byte, error) {
	bufferValues := make([][]byte, len(keys))
	shrinkKeys := make([]kv.Key, 0, len(keys))
	for i, key := range keys {
		val, err := st.buf.Get(ctx, key)
		if kv.IsErrNotFound(err) {
			shrinkKeys = append(shrinkKeys, key)
			continue
		}
		if err != nil {
			return nil, err
		}
		if len(val) != 0 {
			bufferValues[i] = val
		}
	}
	storageValues, err := st.Transaction.BatchGet(ctx, shrinkKeys)
	if err != nil {
		return nil, err
	}
	for i, key := range keys {
		if bufferValues[i] == nil {
			continue
		}
		storageValues[string(key)] = bufferValues[i]
	}
	return storageValues, nil
}

// Set overrides the Transaction interface.
func (st *TxnState) Set(k kv.Key, v []byte) error {
	return st.buf.Set(k, v)
}

// Delete overrides the Transaction interface.
func (st *TxnState) Delete(k kv.Key) error {
	return st.buf.Delete(k)
}

// Iter overrides the Transaction interface.
func (st *TxnState) Iter(k kv.Key, upperBound kv.Key) (kv.Iterator, error) {
	bufferIt, err := st.buf.Iter(k, upperBound)
	if err != nil {
		return nil, err
	}
	retrieverIt, err := st.Transaction.Iter(k, upperBound)
	if err != nil {
		return nil, err
	}
	return kv.NewUnionIter(bufferIt, retrieverIt, false)
}

// IterReverse overrides the Transaction interface.
func (st *TxnState) IterReverse(k kv.Key) (kv.Iterator, error) {
	bufferIt, err := st.buf.IterReverse(k)
	if err != nil {
		return nil, err
	}
	retrieverIt, err := st.Transaction.IterReverse(k)
	if err != nil {
		return nil, err
	}
	return kv.NewUnionIter(bufferIt, retrieverIt, true)
}

// KeysNeedToLock returns the keys need to be locked.
func (st *TxnState) KeysNeedToLock() ([]kv.Key, error) {
	keys := make([]kv.Key, 0, st.buf.Len())
	if err := kv.WalkMemBuffer(st.buf, func(k kv.Key, v []byte) error {
		if !keyNeedToLock(k, v) {
			return nil
		}
		// If the key is already locked, it will be deduplicated in LockKeys method later.
		// The statement MemBuffer will be reused, so we must copy the key here.
		keys = append(keys, append([]byte{}, k...))
		return nil
	}); err != nil {
		return nil, err
	}
	return keys, nil
}

func keyNeedToLock(k, v []byte) bool {
	isTableKey := bytes.HasPrefix(k, tablecodec.TablePrefix())
	if !isTableKey {
		// meta key always need to lock.
		return true
	}
	isDelete := len(v) == 0
	if isDelete {
		// only need to delete row key.
		return k[10] == 'r'
	}
	if tablecodec.IsUntouchedIndexKValue(k, v) {
		return false
	}
	isNonUniqueIndex := tablecodec.IsIndexKey(k) && len(v) == 1
	// Put row key and unique index need to lock.
	return !isNonUniqueIndex
}

// StmtCommit commmits the current statement.
func (st *TxnState) StmtCommit(ctx sessionctx.Context, memTracker *memory.Tracker) error {
	defer func() {
		// If StmtCommit is called in batch mode, we need to clear the txn size
		// in memTracker to avoid double-counting. If it's not batch mode, this
		// work has no effect because that no more data will be appended into
		// s.txn.
		if memTracker != nil {
			memTracker.Consume(int64(-st.Size()))
		}
		st.Cleanup()
	}()

	txnSize := st.Transaction.Size()
	var count int
	err := kv.WalkMemBuffer(st.buf, func(k kv.Key, v []byte) error {
		failpoint.Inject("mockStmtCommitError", func(val failpoint.Value) {
			if val.(bool) {
				count++
			}
		})

		if count > 3 {
			return errors.New("mock stmt commit error")
		}

		if len(v) == 0 {
			return st.Transaction.Delete(k)
		}
		return st.Transaction.Set(k, v)
	})
	if err != nil {
		st.DoNotCommit = err
		return err
	}
	if memTracker != nil {
		memTracker.Consume(int64(st.Transaction.Size() - txnSize))
	}

	// Need to flush binlog.
	for tableID, delta := range st.mutations {
		mutation := getBinlogMutation(ctx, tableID)
		mergeToMutation(mutation, delta)
	}

	if len(st.dirtyTableOP) > 0 {
		dirtyDB := executor.GetDirtyDB(ctx)
		for _, op := range st.dirtyTableOP {
			mergeToDirtyDB(dirtyDB, op)
		}
	}
	return nil
}

// StmtGetMutation gets mutation from st.
func (st *TxnState) StmtGetMutation(tableID int64) *binlog.TableMutation {
	if _, ok := st.mutations[tableID]; !ok {
		st.mutations[tableID] = &binlog.TableMutation{TableId: tableID}
	}
	return st.mutations[tableID]
}

// StmtAddDirtyTableOP adds op into st.
func (st *TxnState) StmtAddDirtyTableOP(op int, tid int64, handle int64) {
	st.dirtyTableOP = append(st.dirtyTableOP, dirtyTableOperation{op, tid, handle})
}

// TxnFuture is a promise, which promises to return a txn in future.
type TxnFuture struct {
	future oracle.Future
	store  kv.Storage
}

// NewTxnFuture creates a new TxnFuture.
func NewTxnFuture(ctx context.Context, store kv.Storage, lowResolution bool) *TxnFuture {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("session.getTxnFuture", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	oracleStore := store.GetOracle()
	var tsFuture oracle.Future
	if lowResolution {
		tsFuture = oracleStore.GetLowResolutionTimestampAsync(ctx)
	} else {
		tsFuture = oracleStore.GetTimestampAsync(ctx)
	}
	ret := &TxnFuture{future: tsFuture, store: store}
	failpoint.InjectContext(ctx, "mockGetTSFail", func() {
		ret.future = txnFailFuture{}
	})
	return ret
}

// GetFuture returns the internal future of st.
func (tf *TxnFuture) GetFuture() oracle.Future {
	return tf.future
}

// TODO: is it possible to move all get timestamp operation into `TxnFuture` from `Transaction`?
// So that `kv.Transaction` must have a resolved start ts, maybe it's helpful to simplify code.
func (tf *TxnFuture) wait() (kv.Transaction, error) {
	startTS, err := tf.future.Wait()
	if err == nil {
		return tf.store.BeginWithStartTS(startTS)
	} else if _, ok := tf.future.(txnFailFuture); ok {
		return nil, err
	}

	// It would retry get timestamp.
	return tf.store.Begin()
}

// dirtyTableOperation represents an operation to dirtyTable, we log the operation
// first and apply the operation log when statement commit.
type dirtyTableOperation struct {
	kind   int
	tid    int64
	handle int64
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

func mergeToDirtyDB(dirtyDB *executor.DirtyDB, op dirtyTableOperation) {
	dt := dirtyDB.GetDirtyTable(op.tid)
	switch op.kind {
	case table.DirtyTableAddRow:
		dt.AddRow(op.handle)
	case table.DirtyTableDeleteRow:
		dt.DeleteRow(op.handle)
	}
}

// hasMockAutoIDRetry is for failpoint injection in TxnState::Commit.
var hasMockAutoIDRetry = int64(0)

func enableMockAutoIDRetry() {
	atomic.StoreInt64(&hasMockAutoIDRetry, 1)
}

func mockAutoIDRetry() bool {
	return atomic.LoadInt64(&hasMockAutoIDRetry) == 1
}

// txnFailFuture is for tests.
type txnFailFuture struct{}

func (txnFailFuture) Wait() (uint64, error) {
	return 0, errors.New("mock get timestamp fail")
}
