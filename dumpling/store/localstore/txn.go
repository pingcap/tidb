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

package localstore

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv"
)

var (
	_ kv.Transaction = (*dbTxn)(nil)
)

// dbTxn is not thread safe
type dbTxn struct {
	kv.UnionStore
	store      *dbStore // for commit
	tid        uint64
	valid      bool
	version    kv.Version          // commit version
	lockedKeys map[string]struct{} // origin version in snapshot
}

// Implement transaction interface

func (txn *dbTxn) Get(k kv.Key) ([]byte, error) {
	log.Debugf("[kv] get key:%q, txn:%d", k, txn.tid)
	val, err := txn.UnionStore.Get(k)
	if err != nil {
		return nil, errors.Trace(err)
	}
	txn.store.compactor.OnGet(k)
	return val, nil
}

func (txn *dbTxn) Set(k kv.Key, data []byte) error {
	log.Debugf("[kv] set key:%q, txn:%d", k, txn.tid)
	err := txn.UnionStore.Set(k, data)
	if err != nil {
		return errors.Trace(err)
	}
	txn.store.compactor.OnSet(k)
	return nil
}

func (txn *dbTxn) Inc(k kv.Key, step int64) (int64, error) {
	log.Debugf("[kv] Inc %q, step %d txn:%d", k, step, txn.tid)
	val, err := txn.UnionStore.Inc(k, step)
	if err != nil {
		return 0, errors.Trace(err)
	}
	txn.store.compactor.OnSet(k)
	return val, nil
}

func (txn *dbTxn) GetInt64(k kv.Key) (int64, error) {
	log.Debugf("[kv] GetInt64 %q, txn:%d", k, txn.tid)
	val, err := txn.UnionStore.GetInt64(k)
	if err != nil {
		return 0, errors.Trace(err)
	}
	txn.store.compactor.OnGet(k)
	return val, nil
}

func (txn *dbTxn) String() string {
	return fmt.Sprintf("%d", txn.tid)
}

func (txn *dbTxn) Seek(k kv.Key) (kv.Iterator, error) {
	log.Debugf("[kv] seek key:%q, txn:%d", k, txn.tid)
	iter, err := txn.UnionStore.Seek(k)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !iter.Valid() {
		return &kv.UnionIter{}, nil
	}

	return iter, nil
}

func (txn *dbTxn) Delete(k kv.Key) error {
	log.Debugf("[kv] delete key:%q, txn:%d", k, txn.tid)
	err := txn.UnionStore.Delete(k)
	if err != nil {
		return errors.Trace(err)
	}
	txn.store.compactor.OnDelete(k)
	return nil
}

func (txn *dbTxn) doCommit() error {
	// check lazy condition pairs
	if err := txn.CheckLazyConditionPairs(); err != nil {
		return errors.Trace(err)
	}

	txn.ReleaseSnapshot()

	err := txn.WalkBuffer(func(k kv.Key, v []byte) error {
		e := txn.LockKeys(k)
		return errors.Trace(e)
	})
	if err != nil {
		return errors.Trace(err)
	}

	return txn.store.CommitTxn(txn)
}

func (txn *dbTxn) Commit() error {
	if !txn.valid {
		return errors.Trace(kv.ErrInvalidTxn)
	}
	log.Debugf("[kv] commit txn %d", txn.tid)
	defer func() {
		txn.close()
	}()

	return errors.Trace(txn.doCommit())
}

func (txn *dbTxn) close() error {
	txn.UnionStore.Release()
	txn.lockedKeys = nil
	txn.valid = false
	return nil
}

func (txn *dbTxn) Rollback() error {
	if !txn.valid {
		return errors.Trace(kv.ErrInvalidTxn)
	}
	log.Warnf("[kv] Rollback txn %d", txn.tid)
	return txn.close()
}

func (txn *dbTxn) LockKeys(keys ...kv.Key) error {
	for _, key := range keys {
		txn.lockedKeys[string(key)] = struct{}{}
	}
	return nil
}
