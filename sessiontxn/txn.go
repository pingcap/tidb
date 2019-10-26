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

package sessiontxn

import (
	"bytes"
	"context"
	"fmt"
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
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tipb/go-binlog"
	"go.uber.org/zap"

	"github.com/petermattis/goid"
	"github.com/pingcap/tidb/util/futures"
	"sync"
)

// TxnState wraps kv.Transaction to provide a new kv.Transaction.
// 1. It holds all statement related modification in the buffer before flush to the txn,
// so if execute statement meets error, the txn won't be made dirty.
// 2. It's a lazy transaction, that means it's a txnFuture before StartTS() is really need.
type TxnState struct {
	// Protect from pending to valid.
	m sync.Mutex

	// States of a TxnState should be one of the followings:
	// Invalid: kv.Transaction == nil && txnFuture == nil
	// Pending: kv.Transaction == nil && txnFuture != nil
	// Valid:	kv.Transaction != nil && txnFuture == nil
	kv.Transaction
	txnFuture *txnFuture

	buf          kv.MemBuffer
	mutations    map[int64]*binlog.TableMutation
	dirtyTableOP []dirtyTableOperation

	// If doNotCommit is not nil, Commit() will not commit the transaction.
	// doNotCommit flag may be set when StmtCommit fail.
	doNotCommit error
}

// Init a new TxnState.
func (st *TxnState) Init() {
	st.m.Lock()
	defer st.m.Unlock()
	st.buf = kv.NewMemDbBuffer(kv.DefaultTxnMembufCap)
	st.mutations = make(map[int64]*binlog.TableMutation)
}

func (st *TxnState) DoNotCommit() error {
	st.m.Lock()
	defer st.m.Unlock()
	return st.doNotCommit
}

// StartTSFuture implements the kv.Transaction interface.
func (st *TxnState) StartTSFuture() (futures.TSFuture, error) {
	return st.txnFuture, nil
}

// Valid implements the kv.Transaction interface.
func (st *TxnState) Valid() bool {
	st.m.Lock()
	defer st.m.Unlock()
	return st.valid()
}

func (st *TxnState) valid() bool {
	return st.Transaction != nil && st.Transaction.Valid()
}

func (st *TxnState) Pending() bool {
	st.m.Lock()
	defer st.m.Unlock()
	return st.pending()
}

func (st *TxnState) pending() bool {
	return st.Transaction == nil && st.txnFuture != nil
}

func (st *TxnState) ValidOrPending() bool {
	st.m.Lock()
	defer st.m.Unlock()
	return st.valid() || st.pending()
}

func (st *TxnState) String() string {
	st.m.Lock()
	defer st.m.Unlock()
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

// AttachValid binds a valid transaction an invalid `TxnState`.
func (st *TxnState) AttachValid(txn kv.Transaction) {
	st.m.Lock()
	defer st.m.Unlock()

	if st.valid() || st.pending() {
		logutil.QPLogger().Warn(
			"TxnState::AttachValid",
			zap.Int64("goid", goid.Get()),
			zap.Bool("valid_or_pending", st.pending() || st.valid()),
		)
	} else {
		logutil.QPLogger().Info(
			"TxnState::AttachValid",
			zap.Int64("goid", goid.Get()),
			zap.Bool("valid_or_pending", st.pending() || st.valid()),
		)
	}

	st.Transaction = txn
}

func (st *TxnState) ChangeInvalidToPendingIfNeed(ctx context.Context, store kv.Storage, lowResolution bool) {
	st.m.Lock()
	defer st.m.Unlock()

	logutil.QPLogger().Info(
		"TxnState::ChangeInvalidToPendingIfNeed",
		zap.Int64("goid", goid.Get()),
		zap.Bool("valid", st.valid()),
	)

	if st.valid() || st.pending() {
		return
	}
	st.txnFuture = getTxnFuture(ctx, store, st, lowResolution)
}

func (st *TxnState) ChangePendingToValidIfNeed(txnCap int) error {
	st.m.Lock()
	defer st.m.Unlock()

	logutil.QPLogger().Info(
		"TxnState::ChangePendingToValidIfNeed",
		zap.Int64("goid", goid.Get()),
		zap.Bool("invalid", !st.valid() && !st.pending()),
		// zap.Stack("stack"),
	)

	if st.valid() {
		return nil
	} else if st.Transaction != nil {
		return errors.New("Transaction is invalid")
	} else if st.txnFuture == nil {
		return errors.New("txnFuture is nil")
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

func (st *TxnState) ChangeToInvalid() {
	st.m.Lock()
	defer st.m.Unlock()
	st.changeToInvalid()
}

func (st *TxnState) changeToInvalid() {
	logutil.QPLogger().Info(
		"TxnState::changeToInvalid",
		zap.Int64("goid", goid.Get()),
		zap.Bool("pending", st.pending()),
	)
	if st.pending() {
		panic("TxnState::changeToInvalid during pending")
	}
	st.Transaction = nil
	st.txnFuture = nil
}

// dirtyTableOperation represents an operation to dirtyTable, we log the operation
// first and apply the operation log when statement commit.
type dirtyTableOperation struct {
	kind   int
	tid    int64
	handle int64
}

var hasMockAutoIDRetry = int64(0)

func enableMockAutoIDRetry() {
	atomic.StoreInt64(&hasMockAutoIDRetry, 1)
}

func mockAutoIDRetry() bool {
	return atomic.LoadInt64(&hasMockAutoIDRetry) == 1
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
	if st.doNotCommit != nil {
		if err1 := st.Transaction.Rollback(); err1 != nil {
			logutil.BgLogger().Error("rollback error", zap.Error(err1))
		}
		return errors.Trace(st.doNotCommit)
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

func (st *TxnState) reset() {
	st.doNotCommit = nil
	st.cleanup()
	st.changeToInvalid()
}

// Get overrides the Transaction interface.
func (st *TxnState) Get(ctx context.Context, k kv.Key) ([]byte, error) {
	val, err := st.buf.Get(ctx, k)
	if kv.IsErrNotFound(err) {
		if err := st.ChangePendingToValidIfNeed(kv.DefaultTxnMembufCap); err != nil {
			return nil, err
		}
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

	var storageValues map[string][]byte = make(map[string][]byte)
	if len(shrinkKeys) > 0 {
		if err := st.ChangePendingToValidIfNeed(kv.DefaultTxnMembufCap); err != nil {
			return nil, err
		}
		svs, err := st.Transaction.BatchGet(ctx, shrinkKeys)
		if err != nil {
			return nil, err
		}
		storageValues = svs
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
	if err := st.ChangePendingToValidIfNeed(kv.DefaultTxnMembufCap); err != nil {
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
	if err := st.ChangePendingToValidIfNeed(kv.DefaultTxnMembufCap); err != nil {
		return nil, err
	}
	retrieverIt, err := st.Transaction.IterReverse(k)
	if err != nil {
		return nil, err
	}
	return kv.NewUnionIter(bufferIt, retrieverIt, true)
}

func (st *TxnState) Cleanup() {
	st.m.Lock()
	st.m.Unlock()
	st.cleanup()
}

func (st *TxnState) cleanup() {
	st.buf.Reset()
	for key := range st.mutations {
		delete(st.mutations, key)
	}
	if st.dirtyTableOP != nil {
		empty := dirtyTableOperation{}
		for i := 0; i < len(st.dirtyTableOP); i++ {
			st.dirtyTableOP[i] = empty
		}
		st.dirtyTableOP = st.dirtyTableOP[:0]
	}
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
	isNonUniqueIndex := len(v) == 1
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

func mergeToDirtyDB(dirtyDB *executor.DirtyDB, op dirtyTableOperation) {
	dt := dirtyDB.GetDirtyTable(op.tid)
	switch op.kind {
	case table.DirtyTableAddRow:
		dt.AddRow(op.handle)
	case table.DirtyTableDeleteRow:
		dt.DeleteRow(op.handle)
	}
}

type txnFailFuture struct{}

func (txnFailFuture) Wait() (uint64, error) {
	return 0, errors.New("mock get timestamp fail")
}

// txnFuture is a promise, which promises to return a txn in future.
type txnFuture struct {
	store    kv.Storage
	txnState *TxnState

	f  oracle.Future
	ts uint64
	e  error
}

func (tf *txnFuture) wait() (kv.Transaction, error) {
	tf.ts, tf.e = tf.f.Wait()
	if tf.e == nil {
		return tf.store.BeginWithStartTS(tf.ts)
	} else if _, ok := tf.f.(txnFailFuture); ok {
		return nil, tf.e
	}

	// It would retry get timestamp.
	var txn kv.Transaction
	txn, tf.e = tf.store.Begin()
	if tf.e == nil {
		tf.ts = txn.StartTS()
	}
	return txn, tf.e
}

// Wait implements futures.TSFuture interface.
func (tf *txnFuture) Wait() (uint64, error) {
	tf.txnState.m.Lock()
	defer tf.txnState.m.Unlock()

	if tf.ts != 0 || tf.e != nil {
		return tf.ts, tf.e
	}

	txn, err := tf.wait()
	if err != nil {
		return 0, err
	}
	txn.SetCap(kv.DefaultTxnMembufCap)
	tf.txnState.Transaction = txn
	tf.txnState.txnFuture = nil

	return tf.ts, tf.e
}

func getTxnFuture(ctx context.Context, store kv.Storage, ts *TxnState, lowResolution bool) *txnFuture {
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

	ret := &txnFuture{f: tsFuture, txnState: ts, store: store}
	failpoint.InjectContext(ctx, "mockGetTSFail", func() {
		ret.f = txnFailFuture{}
	})
	return ret
}

func (ts *TxnState) StmtCommit(s sessionctx.Context) error {
	defer ts.cleanup()
	var count int
	err := kv.WalkMemBuffer(ts.buf, func(k kv.Key, v []byte) error {
		failpoint.Inject("mockStmtCommitError", func(val failpoint.Value) {
			if val.(bool) {
				count++
			}
		})

		if count > 3 {
			return errors.New("mock stmt commit error")
		}

		if len(v) == 0 {
			return ts.Transaction.Delete(k)
		}
		return ts.Transaction.Set(k, v)
	})
	if err != nil {
		ts.doNotCommit = err
		ts.ConfirmAssertions(false)
		return err
	}

	// Need to flush binlog.
	for tableID, delta := range ts.mutations {
		mutation := getBinlogMutation(s, tableID)
		mergeToMutation(mutation, delta)
	}

	if len(ts.dirtyTableOP) > 0 {
		dirtyDB := executor.GetDirtyDB(s)
		for _, op := range ts.dirtyTableOP {
			mergeToDirtyDB(dirtyDB, op)
		}
	}
	ts.ConfirmAssertions(true)
	return nil
}

func (ts *TxnState) StmtRollback() {
	ts.cleanup()
	if ts.valid() {
		ts.ConfirmAssertions(false)
	}
	return
}

func (ts *TxnState) StmtGetMutation(tableID int64) *binlog.TableMutation {
	if _, ok := ts.mutations[tableID]; !ok {
		ts.mutations[tableID] = &binlog.TableMutation{TableId: tableID}
	}
	return ts.mutations[tableID]
}

func (ts *TxnState) AppendDirtyTableOP(op int, tid int64, handle int64) {
	ts.m.Lock()
	defer ts.m.Unlock()
	ts.dirtyTableOP = append(ts.dirtyTableOP, dirtyTableOperation{op, tid, handle})
}
