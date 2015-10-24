// Copyright 2015 PingCAP, Inc.
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

package structure

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
)

// ErrTxDone is the error return after transaction has already been committed or rolled back.
var ErrTxDone = errors.Errorf("Transaction has already been committed or rolled back")

// TStore is the storage for data structure.
type TStore struct {
	store  kv.Storage
	prefix []byte
}

// NewStore creates a TStore with kv storage and special key prefix.
func NewStore(store kv.Storage, prefix []byte) *TStore {
	s := &TStore{
		store:  store,
		prefix: prefix,
	}
	return s
}

// Begin creates a TxStructure for calling structure APIs in a transaction later.
func (s *TStore) Begin() (*TxStructure, error) {
	t := &TxStructure{done: false, prefix: s.prefix}

	var err error
	t.txn, err = s.store.Begin()
	if err != nil {
		return nil, errors.Trace(err)
	}

	return t, nil
}

// RunInNewTxn runs f in a new transaction
func (s *TStore) RunInNewTxn(retryable bool, f func(t *TxStructure) error) error {
	fn := func(txn kv.Transaction) error {
		t := &TxStructure{done: false, prefix: s.prefix, txn: txn}
		return errors.Trace(f(t))
	}
	err := kv.RunInNewTxn(s.store, retryable, fn)
	return errors.Trace(err)
}

// TxStructure supports some simple data structure like string, hash, list, etc... and
// you can use these in a transaction.
type TxStructure struct {
	txn    kv.Transaction
	done   bool
	prefix []byte
}

// Commit commits the transaction.
func (t *TxStructure) Commit() error {
	if t.done {
		return ErrTxDone
	}

	t.done = true

	return errors.Trace(t.txn.Commit())
}

// Rollback rolls back the transaction.
func (t *TxStructure) Rollback() error {
	if t.done {
		return ErrTxDone
	}

	t.done = true

	return errors.Trace(t.txn.Rollback())
}
