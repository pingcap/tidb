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

package session

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
)

// TxnState wraps kv.Transaction to provide a new kv.Transaction.
// 1. It holds all statement related modification in the buffer before flush to the txn,
// so if execute statement meets error, the txn won't be made dirty.
// 2. It's a lazy transaction, that means it's a txnFuture before StartTS() is really need.
type TxnState struct {
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

func (st *TxnState) init() {
	st.buf = kv.NewMemDbBuffer(kv.DefaultTxnMembufCap)
	st.mutations = make(map[int64]*binlog.TableMutation)
}

// Valid implements the kv.Transaction interface.
func (st *TxnState) Valid() bool {
	return st.Transaction != nil && st.Transaction.Valid()
}

func (st *TxnState) pending() bool {
	return st.Transaction == nil && st.txnFuture != nil
}

func (st *TxnState) validOrPending() bool {
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
	if st.pending() {
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

func (st *TxnState) changeInvalidToValid(txn kv.Transaction) {
	st.Transaction = txn
	st.txnFuture = nil
}

func (st *TxnState) changeInvalidToPending(future *txnFuture) {
	st.Transaction = nil
	st.txnFuture = future
}

func (st *TxnState) changePendingToValid(txnCap int) error {
	if st.txnFuture == nil {
		return errors.New("transaction future is not set")
	}

	future := st.txnFuture
	st.txnFuture = nil

	txn, err := future.wait()
	if err != nil {
		st.Transaction = nil
		return err
	}
	txn.SetCap(txnCap)
	st.Transaction = txn
	return nil
}

func (st *TxnState) changeToInvalid() {
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
		logutil.Logger(context.Background()).Error("the code should never run here",
			zap.String("TxnState", st.GoString()),
			zap.Stack("something must be wrong"))
		return errors.New("invalid transaction")
	}
	if st.doNotCommit != nil {
		if err1 := st.Transaction.Rollback(); err1 != nil {
			logutil.Logger(context.Background()).Error("rollback error", zap.Error(err1))
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
func (st *TxnState) Get(k kv.Key) ([]byte, error) {
	val, err := st.buf.Get(k)
	if kv.IsErrNotFound(err) {
		val, err = st.Transaction.Get(k)
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

// GetMemBuffer overrides the Transaction interface.
func (st *TxnState) GetMemBuffer() kv.MemBuffer {
	if st.buf == nil || st.buf.Size() == 0 {
		return st.Transaction.GetMemBuffer()
	}
	return kv.NewBufferStoreFrom(st.Transaction.GetMemBuffer(), st.buf)
}

// BatchGet overrides the Transaction interface.
func (st *TxnState) BatchGet(keys []kv.Key) (map[string][]byte, error) {
	bufferValues := make([][]byte, len(keys))
	shrinkKeys := make([]kv.Key, 0, len(keys))
	for i, key := range keys {
		val, err := st.buf.Get(key)
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
	storageValues, err := st.Transaction.BatchGet(shrinkKeys)
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

func mergeToDirtyDB(dirtyDB *executor.DirtyDB, op dirtyTableOperation) {
	dt := dirtyDB.GetDirtyTable(op.tid)
	switch op.kind {
	case table.DirtyTableAddRow:
		dt.AddRow(op.handle)
	case table.DirtyTableDeleteRow:
		dt.DeleteRow(op.handle)
	}
}

// txnFuture is a promise, which promises to return a txn in future.
type txnFuture struct {
	future oracle.Future
	store  kv.Storage

	mockFail bool
}

func (tf *txnFuture) wait() (kv.Transaction, error) {
	if tf.mockFail {
		return nil, errors.New("mock get timestamp fail")
	}

	startTS, err := tf.future.Wait()
	if err == nil {
		return tf.store.BeginWithStartTS(startTS)
	}

	// It would retry get timestamp.
	return tf.store.Begin()
}

func (s *session) getTxnFuture(ctx context.Context) *txnFuture {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("session.getTxnFuture", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}

	oracleStore := s.store.GetOracle()
	var tsFuture oracle.Future
	if s.sessionVars.LowResolutionTSO {
		tsFuture = oracleStore.GetLowResolutionTimestampAsync(ctx)
	} else {
		tsFuture = oracleStore.GetTimestampAsync(ctx)
	}
	ret := &txnFuture{future: tsFuture, store: s.store}
	if x := ctx.Value("mockGetTSFail"); x != nil {
		ret.mockFail = true
	}
	return ret
}

// HasDirtyContent checks whether there's dirty update on the given table.
// Put this function here is to avoid cycle import.
func (s *session) HasDirtyContent(tid int64) bool {
	x := s.GetSessionVars().TxnCtx.DirtyDB
	if x == nil {
		return false
	}
	return !x.(*executor.DirtyDB).GetDirtyTable(tid).IsEmpty()
}

// StmtCommit implements the sessionctx.Context interface.
func (s *session) StmtCommit() error {
	defer s.txn.cleanup()
	st := &s.txn
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
		st.doNotCommit = err
		return err
	}

	// Need to flush binlog.
	for tableID, delta := range st.mutations {
		mutation := getBinlogMutation(s, tableID)
		mergeToMutation(mutation, delta)
	}

	if len(st.dirtyTableOP) > 0 {
		dirtyDB := executor.GetDirtyDB(s)
		for _, op := range st.dirtyTableOP {
			mergeToDirtyDB(dirtyDB, op)
		}
	}
	return nil
}

// StmtRollback implements the sessionctx.Context interface.
func (s *session) StmtRollback() {
	s.txn.cleanup()
	return
}

// StmtGetMutation implements the sessionctx.Context interface.
func (s *session) StmtGetMutation(tableID int64) *binlog.TableMutation {
	st := &s.txn
	if _, ok := st.mutations[tableID]; !ok {
		st.mutations[tableID] = &binlog.TableMutation{TableId: tableID}
	}
	return st.mutations[tableID]
}

func (s *session) StmtAddDirtyTableOP(op int, tid int64, handle int64) {
	s.txn.dirtyTableOP = append(s.txn.dirtyTableOP, dirtyTableOperation{op, tid, handle})
}
