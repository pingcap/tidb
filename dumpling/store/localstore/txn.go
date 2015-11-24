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
	"runtime/debug"
	"strconv"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/codec"
)

var (
	_ kv.Transaction = (*dbTxn)(nil)
	// ErrInvalidTxn is the error when commits or rollbacks in an invalid transaction.
	ErrInvalidTxn = errors.New("invalid transaction")
	// ErrCannotSetNilValue is the error when sets an empty value.
	ErrCannotSetNilValue = errors.New("can not set nil value")
)

// dbTxn is not thread safe
type dbTxn struct {
	kv.UnionStore
	store        *dbStore // for commit
	tid          uint64
	valid        bool
	version      kv.Version          // commit version
	snapshotVals map[string]struct{} // origin version in snapshot
	opts         map[kv.Option]interface{}
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

func (txn *dbTxn) Inc(k kv.Key, step int64) (int64, error) {
	log.Debugf("Inc %q, step %d txn:%d", k, step, txn.tid)
	k = kv.EncodeKey(k)

	txn.markOrigin(k)
	val, err := txn.UnionStore.Get(k)
	if kv.IsErrNotFound(err) {
		err = txn.UnionStore.Set(k, []byte(strconv.FormatInt(step, 10)))
		if err != nil {
			return 0, errors.Trace(err)
		}

		return step, nil
	}
	if err != nil {
		return 0, errors.Trace(err)
	}

	intVal, err := strconv.ParseInt(string(val), 10, 0)
	if err != nil {
		return intVal, errors.Trace(err)
	}

	intVal += step
	err = txn.UnionStore.Set(k, []byte(strconv.FormatInt(intVal, 10)))
	if err != nil {
		return 0, errors.Trace(err)
	}
	txn.store.compactor.OnSet(k)
	return intVal, nil
}

func (txn *dbTxn) GetInt64(k kv.Key) (int64, error) {
	k = kv.EncodeKey(k)
	val, err := txn.UnionStore.Get(k)
	if kv.IsErrNotFound(err) {
		return 0, nil
	}
	if err != nil {
		return 0, errors.Trace(err)
	}
	intVal, err := strconv.ParseInt(string(val), 10, 0)
	return intVal, errors.Trace(err)
}

func (txn *dbTxn) String() string {
	return fmt.Sprintf("%d", txn.tid)
}

func (txn *dbTxn) Get(k kv.Key) ([]byte, error) {
	log.Debugf("get key:%q, txn:%d", k, txn.tid)
	k = kv.EncodeKey(k)
	val, err := txn.UnionStore.Get(k)
	if kv.IsErrNotFound(err) {
		return nil, errors.Trace(kv.ErrNotExist)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(val) == 0 {
		return nil, errors.Trace(kv.ErrNotExist)
	}
	txn.store.compactor.OnGet(k)
	return val, nil
}

func (txn *dbTxn) BatchPrefetch(keys []kv.Key) error {
	encodedKeys := make([]kv.Key, len(keys))
	for i, k := range keys {
		encodedKeys[i] = kv.EncodeKey(k)
	}
	_, err := txn.UnionStore.Snapshot.BatchGet(encodedKeys)
	return err
}

func (txn *dbTxn) RangePrefetch(start, end kv.Key, limit int) error {
	_, err := txn.UnionStore.Snapshot.RangeGet(kv.EncodeKey(start), kv.EncodeKey(end), limit)
	return err
}

func (txn *dbTxn) Set(k kv.Key, data []byte) error {
	if len(data) == 0 {
		// Incase someone use it in the wrong way, we can figure it out immediately.
		debug.PrintStack()
		return errors.Trace(ErrCannotSetNilValue)
	}

	log.Debugf("set key:%q, txn:%d", k, txn.tid)
	k = kv.EncodeKey(k)
	err := txn.UnionStore.Set(k, data)
	if err != nil {
		return errors.Trace(err)
	}
	txn.markOrigin(k)
	txn.store.compactor.OnSet(k)
	return nil
}

func (txn *dbTxn) Seek(k kv.Key) (kv.Iterator, error) {
	log.Debugf("seek key:%q, txn:%d", k, txn.tid)
	k = kv.EncodeKey(k)

	iter, err := txn.UnionStore.Seek(k, txn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !iter.Valid() {
		return &kv.UnionIter{}, nil
	}

	return kv.NewDecodeKeyIter(iter), nil
}

func (txn *dbTxn) Delete(k kv.Key) error {
	log.Debugf("delete key:%q, txn:%d", k, txn.tid)
	k = kv.EncodeKey(k)
	err := txn.UnionStore.Delete(k)
	if err != nil {
		return errors.Trace(err)
	}
	txn.markOrigin(k)
	txn.store.compactor.OnDelete(k)
	return nil
}

func (txn *dbTxn) each(f func(kv.Iterator) error) error {
	iter := txn.UnionStore.WBuffer.NewIterator(nil)
	defer iter.Close()
	for ; iter.Valid(); iter.Next() {
		if err := f(iter); err != nil {
			return errors.Trace(err)
		}
	}
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
	if err := txn.UnionStore.CheckLazyConditionPairs(); err != nil {
		return errors.Trace(err)
	}

	txn.Snapshot.Release()

	// Check locked keys
	for k := range txn.snapshotVals {
		err := txn.store.tryConditionLockKey(txn.tid, k)
		if err != nil {
			return errors.Trace(err)
		}
		keysLocked = append(keysLocked, k)
	}

	// disable version provider temporarily
	lockVersionProvider()
	defer unlockVersionProvider()

	curVer, err := globalVersionProvider.CurrentVersion()
	if err != nil {
		return errors.Trace(err)
	}
	err = txn.each(func(iter kv.Iterator) error {
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
		return errors.Trace(ErrInvalidTxn)
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
		return errors.Trace(ErrInvalidTxn)
	}
	log.Warnf("Rollback txn %d", txn.tid)
	return txn.close()
}

func (txn *dbTxn) LockKeys(keys ...kv.Key) error {
	for _, key := range keys {
		key = kv.EncodeKey(key)
		txn.markOrigin(key)
	}
	return nil
}

func (txn *dbTxn) SetOption(opt kv.Option, val interface{}) {
	txn.opts[opt] = val
}

func (txn *dbTxn) DelOption(opt kv.Option) {
	delete(txn.opts, opt)
}

type options map[kv.Option]interface{}

func (opts options) Get(opt kv.Option) (interface{}, bool) {
	v, ok := opts[opt]
	return v, ok
}
