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

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/go-hbase"
	"github.com/pingcap/go-themis"
	"github.com/pingcap/tidb/kv"
)

var (
	_ kv.Transaction = (*hbaseTxn)(nil)
	// ErrInvalidTxn is the error when commits or rollbacks in an invalid transaction.
	ErrInvalidTxn = errors.New("invalid transaction")
	// ErrCannotSetNilValue is the error when sets an empty value
	ErrCannotSetNilValue = errors.New("can not set nil value")
)

var (
	// default values for txn.SetOption
	optionDefaultVals = map[kv.Option]interface{}{
		kv.RangePrefetchOnCacheMiss: 1024,
	}
)

func getOptionDefaultVal(opt kv.Option) interface{} {
	if v, ok := optionDefaultVals[opt]; ok {
		return v
	}
	return nil
}

// dbTxn is not thread safe
type hbaseTxn struct {
	kv.UnionStore
	*themis.Txn
	store     *hbaseStore // for commit
	storeName string
	tid       uint64
	valid     bool
	version   kv.Version // commit version
	opts      map[kv.Option]interface{}
}

func newHbaseTxn(t *themis.Txn, storeName string) *hbaseTxn {
	opts := make(map[kv.Option]interface{})

	return &hbaseTxn{
		Txn:       t,
		valid:     true,
		storeName: storeName,
		tid:       t.GetStartTS(),

		UnionStore: kv.NewUnionStore(newHbaseSnapshot(t, storeName), options(opts)),
		opts:       opts,
	}
}

// Implement transaction interface

func (txn *hbaseTxn) Inc(k kv.Key, step int64) (int64, error) {
	log.Debugf("Inc %q, step %d txn:%d", k, step, txn.tid)
	k = kv.EncodeKey(k)
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

	intVal, err := strconv.ParseInt(string(val), 10, 64)
	if err != nil {
		return intVal, errors.Trace(err)
	}

	intVal += step
	err = txn.UnionStore.Set(k, []byte(strconv.FormatInt(intVal, 10)))
	if err != nil {
		return 0, errors.Trace(err)
	}
	return intVal, nil
}

func (txn *hbaseTxn) String() string {
	return fmt.Sprintf("%d", txn.tid)
}

func (txn *hbaseTxn) Get(k kv.Key) ([]byte, error) {
	log.Debugf("get key:%q, txn:%d", k, txn.tid)
	k = kv.EncodeKey(k)
	val, err := txn.UnionStore.Get(k)
	if kv.IsErrNotFound(err) || len(val) == 0 {
		return nil, errors.Trace(kv.ErrNotExist)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	return val, nil
}

func (txn *hbaseTxn) BatchPrefetch(keys []kv.Key) error {
	encodedKeys := make([]kv.Key, len(keys))
	for i, k := range keys {
		encodedKeys[i] = kv.EncodeKey(k)
	}
	_, err := txn.UnionStore.Snapshot.BatchGet(encodedKeys)
	return err
}

func (txn *hbaseTxn) RangePrefetch(start, end kv.Key, limit int) error {
	_, err := txn.UnionStore.Snapshot.RangeGet(kv.EncodeKey(start), kv.EncodeKey(end), limit)
	return err
}

// GetInt64 get int64 which created by Inc method.
func (txn *hbaseTxn) GetInt64(k kv.Key) (int64, error) {
	k = kv.EncodeKey(k)
	val, err := txn.UnionStore.Get(k)
	if kv.IsErrNotFound(err) {
		return 0, nil
	}

	if err != nil {
		return 0, errors.Trace(err)
	}

	intVal, err := strconv.ParseInt(string(val), 10, 64)
	return intVal, errors.Trace(err)
}

func (txn *hbaseTxn) Set(k kv.Key, data []byte) error {
	if len(data) == 0 {
		// Incase someone use it in the wrong way, we can figure it out immediately
		debug.PrintStack()
		return errors.Trace(ErrCannotSetNilValue)
	}

	log.Debugf("set key:%q, txn:%d", k, txn.tid)
	k = kv.EncodeKey(k)
	return txn.UnionStore.Set(k, data)
}

func (txn *hbaseTxn) Seek(k kv.Key) (kv.Iterator, error) {
	log.Debugf("seek %q txn:%d", k, txn.tid)
	k = kv.EncodeKey(k)
	iter, err := txn.UnionStore.Seek(k, txn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return kv.NewDecodeKeyIter(iter), nil
}

func (txn *hbaseTxn) Delete(k kv.Key) error {
	log.Debugf("delete %q txn:%d", k, txn.tid)
	k = kv.EncodeKey(k)
	err := txn.UnionStore.Delete(k)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (txn *hbaseTxn) each(f func(kv.Iterator) error) error {
	iter := txn.UnionStore.WBuffer.NewIterator(nil)
	defer iter.Close()
	for ; iter.Valid(); iter.Next() {
		if err := f(iter); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (txn *hbaseTxn) doCommit() error {
	if err := txn.UnionStore.CheckLazyConditionPairs(); err != nil {
		return errors.Trace(err)
	}

	err := txn.each(func(iter kv.Iterator) error {
		var row, val []byte
		row = make([]byte, len(iter.Key()))
		if len(iter.Value()) == 0 { // Deleted marker
			copy(row, iter.Key())
			d := hbase.NewDelete(row)
			d.AddStringColumn(hbaseColFamily, hbaseQualifier)
			err := txn.Txn.Delete(txn.storeName, d)
			if err != nil {
				return errors.Trace(err)
			}
		} else {
			val = make([]byte, len(iter.Value()))
			copy(row, iter.Key())
			copy(val, iter.Value())
			p := hbase.NewPut(row)
			p.AddValue(hbaseColFamilyBytes, hbaseQualifierBytes, val)
			txn.Txn.Put(txn.storeName, p)
		}
		return nil
	})

	if err != nil {
		return errors.Trace(err)
	}

	err = txn.Txn.Commit()
	if err != nil {
		log.Error(err)
		return errors.Trace(err)
	}

	txn.version = kv.NewVersion(txn.Txn.GetCommitTS())
	log.Debugf("commit successfully, txn.version:%d", txn.version.Ver)
	return nil
}

func (txn *hbaseTxn) Commit() error {
	if !txn.valid {
		return ErrInvalidTxn
	}
	log.Debugf("start to commit txn %d", txn.tid)
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

//if fail, themis auto rollback
func (txn *hbaseTxn) Rollback() error {
	if !txn.valid {
		return ErrInvalidTxn
	}
	log.Warnf("Rollback txn %d", txn.tid)
	return txn.close()
}

func (txn *hbaseTxn) LockKeys(keys ...kv.Key) error {
	for _, key := range keys {
		key = kv.EncodeKey(key)
		if err := txn.Txn.LockRow(txn.storeName, key); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (txn *hbaseTxn) SetOption(opt kv.Option, val interface{}) {
	if val == nil {
		txn.opts[opt] = getOptionDefaultVal(opt)
	} else {
		txn.opts[opt] = val
	}
}

func (txn *hbaseTxn) DelOption(opt kv.Option) {
	delete(txn.opts, opt)
}

type options map[kv.Option]interface{}

func (opts options) Get(opt kv.Option) (interface{}, bool) {
	v, ok := opts[opt]
	return v, ok
}
