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
	"github.com/pingcap/tidb/util/codec"
)

var (
	_ kv.Transaction = (*dbTxn)(nil)
)

// dbTxn is not thread safe
type dbTxn struct {
	kv.UnionStore
	store        *dbStore // for commit
	tid          uint64
	valid        bool
	version      kv.Version          // commit version
	snapshotVals map[string]struct{} // origin version in snapshot
}

func (txn *dbTxn) markOrigin(k []byte) {
	keystr := string(k)

	// Already exist, do nothing.
	if _, ok := txn.snapshotVals[keystr]; ok {
		return
	}

	txn.snapshotVals[keystr] = struct{}{}
}

// Implement transaction interface

func (txn *dbTxn) Get(k kv.Key) ([]byte, error) {
	log.Debugf("get key:%q, txn:%d", k, txn.tid)
	val, err := txn.UnionStore.Get(k)
	if err != nil {
		return nil, errors.Trace(err)
	}
	txn.store.compactor.OnGet(k)
	return val, nil
}

func (txn *dbTxn) Set(k kv.Key, data []byte) error {
	log.Debugf("set key:%q, txn:%d", k, txn.tid)
	err := txn.UnionStore.Set(k, data)
	if err != nil {
		return errors.Trace(err)
	}
	txn.markOrigin(k)
	txn.store.compactor.OnSet(k)
	return nil
}

func (txn *dbTxn) Inc(k kv.Key, step int64) (int64, error) {
	log.Debugf("Inc %q, step %d txn:%d", k, step, txn.tid)

	txn.markOrigin(k)
	val, err := txn.UnionStore.Inc(k, step)
	if err != nil {
		return 0, errors.Trace(err)
	}
	txn.store.compactor.OnSet(k)
	return val, nil
}

func (txn *dbTxn) GetInt64(k kv.Key) (int64, error) {
	log.Debugf("GetInt64 %q, txn:%d", k, txn.tid)
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
	log.Debugf("seek key:%q, txn:%d", k, txn.tid)
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
	log.Debugf("delete key:%q, txn:%d", k, txn.tid)
	err := txn.UnionStore.Delete(k)
	if err != nil {
		return errors.Trace(err)
	}
	txn.markOrigin(k)
	txn.store.compactor.OnDelete(k)
	return nil
}

func (txn *dbTxn) doCommit() error {
	b := txn.store.newBatch()
	keysLocked := make([]string, 0, len(txn.snapshotVals))
	defer func() {
		for _, key := range keysLocked {
			txn.store.unLockKeys(key)
		}
	}()

	// check lazy condition pairs
	if err := txn.CheckLazyConditionPairs(); err != nil {
		return errors.Trace(err)
	}

	txn.ReleaseSnapshot()

	// Check locked keys
	for k := range txn.snapshotVals {
		err := txn.store.tryConditionLockKey(txn.tid, k)
		if err != nil {
			return errors.Trace(err)
		}
		keysLocked = append(keysLocked, k)
	}

	// disable version provider temporarily
	providerMu.Lock()
	defer providerMu.Unlock()

	curVer, err := globalVersionProvider.CurrentVersion()
	if err != nil {
		return errors.Trace(err)
	}
	err = txn.WalkWriteBuffer(func(iter kv.Iterator) error {
		metaKey := codec.EncodeBytes(nil, []byte(iter.Key()))
		// put dummy meta key, write current version
		b.Put(metaKey, codec.EncodeUint(nil, curVer.Ver))
		mvccKey := MvccEncodeVersionKey(kv.Key(iter.Key()), curVer)
		if len(iter.Value()) == 0 { // Deleted marker
			b.Put(mvccKey, nil)
		} else {
			b.Put(mvccKey, iter.Value())
		}
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	// Update commit version.
	txn.version = curVer
	return txn.store.writeBatch(b)
}

func (txn *dbTxn) Commit() error {
	if !txn.valid {
		return errors.Trace(kv.ErrInvalidTxn)
	}
	log.Infof("commit txn %d", txn.tid)
	defer func() {
		txn.close()
	}()

	return errors.Trace(txn.doCommit())
}

func (txn *dbTxn) CommittedVersion() (kv.Version, error) {
	// Check if this transaction is not committed.
	if txn.version.Cmp(kv.MinVersion) == 0 {
		return kv.MinVersion, kv.ErrNotCommitted
	}
	return txn.version, nil
}

func (txn *dbTxn) close() error {
	txn.UnionStore.Close()
	txn.snapshotVals = nil
	txn.valid = false
	return nil
}

func (txn *dbTxn) Rollback() error {
	if !txn.valid {
		return errors.Trace(kv.ErrInvalidTxn)
	}
	log.Warnf("Rollback txn %d", txn.tid)
	return txn.close()
}

func (txn *dbTxn) LockKeys(keys ...kv.Key) error {
	for _, key := range keys {
		txn.markOrigin(key)
	}
	return nil
}
