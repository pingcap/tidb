// Copyright 2016 PingCAP, Inc.
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

package tikv

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv"
)

var (
	_ kv.Transaction = (*tikvTxn)(nil)
)

// tikvTxn implements kv.Transaction.
type tikvTxn struct {
	us       kv.UnionStore
	store    *tikvStore // for connection to region.
	startTS  uint64
	commitTS uint64
	valid    bool
	lockKeys [][]byte
	dirty    bool
	// FIXME: only doPrewrite, this variable only for lock key test.
	// If find better way to test lock then delete it.
	DONOTCOMMIT bool
}

func newTiKVTxn(store *tikvStore) (*tikvTxn, error) {
	startTS, err := store.oracle.GetTimestamp()
	if err != nil {
		return nil, errors.Trace(err)
	}
	ver := kv.NewVersion(startTS)
	return &tikvTxn{
		us:      kv.NewUnionStore(newTiKVSnapshot(store, ver)),
		store:   store,
		startTS: startTS,
		valid:   true,
	}, nil
}

// Implement transaction interface.
func (txn *tikvTxn) Get(k kv.Key) ([]byte, error) {
	log.Debugf("Get key[%q] txn[%d]", k, txn.StartTS())
	ret, err := txn.us.Get(k)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return ret, nil
}

func (txn *tikvTxn) Set(k kv.Key, v []byte) error {
	log.Debugf("Set key[%q] txn[%d]", k, txn.StartTS())
	txn.dirty = true
	return txn.us.Set(k, v)
}

func (txn *tikvTxn) String() string {
	return fmt.Sprintf("%d", txn.StartTS())
}

func (txn *tikvTxn) Seek(k kv.Key) (kv.Iterator, error) {
	log.Debugf("Seek key[%q] txn[%d]", k, txn.StartTS())
	return txn.us.Seek(k)
}

// SeekReverse creates a reversed Iterator positioned on the first entry which key is less than k.
func (txn *tikvTxn) SeekReverse(k kv.Key) (kv.Iterator, error) {
	log.Debugf("SeekReverse key[%q] txn[%d]", k, txn.StartTS())
	return txn.us.SeekReverse(k)
}

func (txn *tikvTxn) Delete(k kv.Key) error {
	log.Debugf("Delete key[%q] txn[%d]", k, txn.StartTS())
	txn.dirty = true
	return txn.us.Delete(k)
}

func (txn *tikvTxn) SetOption(opt kv.Option, val interface{}) {
	txn.us.SetOption(opt, val)
}

func (txn *tikvTxn) DelOption(opt kv.Option) {
	txn.us.DelOption(opt)
}

func (txn *tikvTxn) Commit() error {
	if !txn.valid {
		return kv.ErrInvalidTxn
	}
	defer txn.close()

	log.Debugf("[kv] start to commit txn %d", txn.StartTS())
	if err := txn.us.CheckLazyConditionPairs(); err != nil {
		return errors.Trace(err)
	}

	committer, err := newTxnCommitter(txn)
	if err != nil {
		return errors.Trace(err)
	}
	if committer == nil {
		return nil
	}
	err = committer.Commit()
	if err != nil {
		return errors.Trace(err)
	}
	log.Debugf("[kv] finish commit txn %d", txn.StartTS())
	return nil
}

func (txn *tikvTxn) close() error {
	txn.us.Release()
	txn.valid = false
	return nil
}

func (txn *tikvTxn) Rollback() error {
	if !txn.valid {
		return kv.ErrInvalidTxn
	}
	log.Warnf("[kv] Rollback txn %d", txn.StartTS())
	return nil
}

func (txn *tikvTxn) LockKeys(keys ...kv.Key) error {
	for _, key := range keys {
		txn.lockKeys = append(txn.lockKeys, key)
	}
	return nil
}

func (txn *tikvTxn) GetClient() kv.Client {
	return &CopClient{
		store: txn.store,
	}
}

func (txn *tikvTxn) IsReadOnly() bool {
	return !txn.dirty
}

func (txn *tikvTxn) StartTS() uint64 {
	return txn.startTS
}
