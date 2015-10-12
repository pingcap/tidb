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

package hbasekv

import (
	"fmt"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/c4pt0r/go-hbase"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/go-themis"
	"github.com/pingcap/tidb/kv"
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

var (
	_ kv.Transaction = (*hbaseTxn)(nil)
	// ErrInvalidTxn is the error when commits or rollbacks in an invalid transaction.
	ErrInvalidTxn = errors.New("invalid transaction")
	// ErrCannotSetNilValue is the error when sets an empty value
	ErrCannotSetNilValue = errors.New("can not set nil value")
)

// dbTxn is not thread safe
type hbaseTxn struct {
	kv.UnionStore
	*themis.Txn
	store        *hbaseStore // for commit
	storeName    string
	startTs      time.Time
	tID          uint64
	valid        bool
	version      kv.Version        // commit version
}

// Implement transaction interface

func (txn *hbaseTxn) Inc(k kv.Key, step int64) (int64, error) {
	log.Debugf("Inc %q, step %d txn:%d", k, step, txn.tID)
	k = kv.EncodeKey(k)

	val, err := txn.UnionStore.Get(k)
	if kv.IsErrNotFound(err) {
		err = txn.UnionStore.Set(k, []byte(strconv.FormatInt(step, 10)))
		if err != nil {
			return 0, err
		}

		return step, nil
	}

	if err != nil {
		return 0, err
	}

	intVal, err := strconv.ParseInt(string(val), 10, 0)
	if err != nil {
		return intVal, err
	}

	intVal += step
	err = txn.UnionStore.Set(k, []byte(strconv.FormatInt(intVal, 10)))
	if err != nil {
		return 0, err
	}

	return intVal, nil
}

func (txn *hbaseTxn) String() string {
	return fmt.Sprintf("%d", txn.tID)
}

func (txn *hbaseTxn) Get(k kv.Key) ([]byte, error) {
	log.Debugf("get key:%q, txn:%d", k, txn.tID)
	k = kv.EncodeKey(k)
	val, err := txn.UnionStore.Get(k)
	if kv.IsErrNotFound(err) {
		return nil, kv.ErrNotExist
	}
	if err != nil {
		return nil, err
	}

	if len(val) == 0 {
		return nil, kv.ErrNotExist
	}

	return val, nil
}

// GetInt64 get int64 which created by Inc method.
func (txn *hbaseTxn) GetInt64(k kv.Key) (int64, error) {
	k = kv.EncodeKey(k)
	val, err := txn.UnionStore.Get(k)
	if kv.IsErrNotFound(err) {
		return 0, nil
	}

	if err != nil {
		return 0, err
	}

	intVal, err := strconv.ParseInt(string(val), 10, 0)
	return intVal, errors.Trace(err)
}

func (txn *hbaseTxn) Set(k kv.Key, data []byte) error {
	if len(data) == 0 {
		// Incase someone use it in the wrong way, we can figure it out immediately
		debug.PrintStack()
		return ErrCannotSetNilValue
	}

	log.Debugf("set key:%q, txn:%d", k, txn.tID)
	k = kv.EncodeKey(k)
	return txn.UnionStore.Set(k, data)
}

func (txn *hbaseTxn) Seek(k kv.Key, fnKeyCmp func(kv.Key) bool) (kv.Iterator, error) {
	log.Debugf("seek %q txn:%d", k, txn.tID)
	k = kv.EncodeKey(k)

	iter, err := txn.UnionStore.Seek(k, txn)
	if err != nil {
		return nil, err
	}

	if !iter.Valid() {
		return &kv.UnionIter{}, nil
	}

	if fnKeyCmp != nil {
		if fnKeyCmp([]byte(iter.Key())[:1]) {
			return &kv.UnionIter{}, nil
		}
	}

	return iter, nil
}

func (txn *hbaseTxn) Delete(k kv.Key) error {
	log.Debugf("delete %q txn:%d", k, txn.tID)
	k = kv.EncodeKey(k)
	return txn.UnionStore.Delete(k)
}

func (txn *hbaseTxn) each(f func(iterator.Iterator) error) error {
	iter := txn.UnionStore.Dirty.NewIterator(nil)
	defer iter.Release()
	for iter.Next() {
		if err := f(iter); err != nil {
			return err
		}
	}
	return nil
}

func (txn *hbaseTxn) doCommit() error {
	err := txn.each(func(iter iterator.Iterator) error {
		var row, val []byte
		row = make([]byte, len(iter.Key()))
		if len(iter.Value()) == 0 { // Deleted marker
			copy(row, iter.Key())
			d := hbase.NewDelete(row)
			d.AddStringColumn(ColFamily, Qualifier)
			txn.Txn.Delete(txn.storeName, d)
		} else {
			val = make([]byte, len(iter.Value()))
			copy(row, iter.Key())
			copy(val, iter.Value())
			p := hbase.NewPut(row)
			p.AddValue([]byte(ColFamily), []byte(Qualifier), val)
			txn.Txn.Put(txn.storeName, p)
		}
		return nil
	})

	defer txn.close()

	if err != nil {
		return err
	}

	err = txn.Txn.Commit()
	if err != nil {
		log.Error(err)
		return err
	}
	return nil
}

func (txn *hbaseTxn) Commit() error {
	if !txn.valid {
		return ErrInvalidTxn
	}
	log.Infof("commit txn %d", txn.tID)
	defer func() {
		txn.close()
	}()

	return txn.doCommit()
}

func (txn *hbaseTxn) CommittedVersion() (kv.Version, error) {
	// Check if this transaction is not committed.
	if txn.version.Cmp(kv.MinVersion) == 0 {
		return kv.MinVersion, kv.ErrNotCommitted
	}
	return txn.version, nil
}

func (txn *hbaseTxn) close() error {
	txn.UnionStore.Close()
	txn.valid = false
	return nil
}

func (txn *hbaseTxn) Rollback() error {
	if !txn.valid {
		return ErrInvalidTxn
	}
	log.Warnf("Rollback txn %d", txn.tID)
	return txn.close()
}

func (txn *hbaseTxn) LockKeys(keys ...kv.Key) error {
//	for _, key := range keys {
//		key = kv.EncodeKey(key)
//		if err := txn.markOrigin(key); err != nil {
//			return err
//		}
//	}
	return nil
}
