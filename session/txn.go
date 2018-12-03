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
	"fmt"
	"strings"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	binlog "github.com/pingcap/tipb/go-binlog"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
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

	// If StmtCommit meets error (which should not happen, just in case), mark it.
	// And rollback the whole transaction when it commit.
	fail error
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
		fmt.Fprintf(&s, ", startTS=%d", st.Transaction.StartTS())
		if len(st.dirtyTableOP) > 0 {
			fmt.Fprintf(&s, ", len(dirtyTable)=%d", len(st.dirtyTableOP))
		}
		if len(st.mutations) > 0 {
			fmt.Fprintf(&s, ", len(mutations)=%d", len(st.mutations))
		}
	} else {
		s.WriteString("state=invalid")
	}

	if st.fail != nil {
		s.WriteString(", fail=")
		s.WriteString(st.fail.Error())
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
		return errors.Trace(err)
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
	row    []types.Datum
}

// Commit overrides the Transaction interface.
func (st *TxnState) Commit(ctx context.Context) error {
	if st.fail != nil {
		// If any error happen during StmtCommit, don't commit this transaction.
		err := st.fail
		st.fail = nil
		return errors.Trace(err)
	}
	return errors.Trace(st.Transaction.Commit(ctx))
}

// Rollback overrides the Transaction interface.
func (st *TxnState) Rollback() error {
	if st.fail != nil {
		log.Error(errors.Trace(st.fail))
		st.fail = nil
	}
	return errors.Trace(st.Transaction.Rollback())
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
		return nil, errors.Trace(err)
	}
	if len(val) == 0 {
		return nil, kv.ErrNotExist
	}
	return val, nil
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
		return nil, errors.Trace(err)
	}
	retrieverIt, err := st.Transaction.Iter(k, upperBound)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return kv.NewUnionIter(bufferIt, retrieverIt, false)
}

// IterReverse overrides the Transaction interface.
func (st *TxnState) IterReverse(k kv.Key) (kv.Iterator, error) {
	bufferIt, err := st.buf.IterReverse(k)
	if err != nil {
		return nil, errors.Trace(err)
	}
	retrieverIt, err := st.Transaction.IterReverse(k)
	if err != nil {
		return nil, errors.Trace(err)
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
	switch op.kind {
	case table.DirtyTableAddRow:
		dirtyDB.AddRow(op.tid, op.handle, op.row)
	case table.DirtyTableDeleteRow:
		dirtyDB.DeleteRow(op.tid, op.handle)
	case table.DirtyTableTruncate:
		dirtyDB.TruncateTable(op.tid)
	}
}

// txnFuture is a promise, which promises to return a txn in future.
type txnFuture struct {
	future oracle.Future
	store  kv.Storage
}

func (tf *txnFuture) wait() (kv.Transaction, error) {
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
	tsFuture := oracleStore.GetTimestampAsync(ctx)
	return &txnFuture{tsFuture, s.store}
}

// StmtCommit implements the sessionctx.Context interface.
func (s *session) StmtCommit() {
	if s.txn.fail != nil {
		return
	}

	defer s.txn.cleanup()
	st := &s.txn
	err := kv.WalkMemBuffer(st.buf, func(k kv.Key, v []byte) error {
		// gofail: var mockStmtCommitError bool
		// if mockStmtCommitError {
		// 	return errors.New("mock stmt commit error")
		// }
		if len(v) == 0 {
			return errors.Trace(st.Transaction.Delete(k))
		}
		return errors.Trace(st.Transaction.Set(k, v))
	})
	if err != nil {
		s.txn.fail = errors.Trace(err)
		return
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

func (s *session) StmtAddDirtyTableOP(op int, tid int64, handle int64, row []types.Datum) {
	s.txn.dirtyTableOP = append(s.txn.dirtyTableOP, dirtyTableOperation{op, tid, handle, row})
}
