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

// TStructure supports some simple data structure like string, hash, list, etc... and
// you can use these in a transaction.
type TStructure struct {
	txn  kv.Transaction
	done bool
}

// Begin starts a new transaction.
func Begin(store kv.Storage) (*TStructure, error) {
	t := &TStructure{done: false}

	var err error
	t.txn, err = store.Begin()
	if err != nil {
		return nil, errors.Trace(err)
	}

	return t, nil
}

// Commit commits the transaction.
func (t *TStructure) Commit() error {
	if t.done {
		return ErrTxDone
	}

	t.done = true

	return errors.Trace(t.txn.Commit())
}

// Rollback rolls back the transaction.
func (t *TStructure) Rollback() error {
	if t.done {
		return ErrTxDone
	}

	t.done = true

	return errors.Trace(t.txn.Rollback())
}
