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

	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv"
)

var (
	_ kv.Transaction = (*tikvTxn)(nil)
)

// tikvTxn implements kv.Transacton. It is not thread safe.
type tikvTxn struct {
	us        kv.UnionStore
	store     *tikvStore // for commit
	storeName string
	tid       uint64
	valid     bool
	version   kv.Version // commit version
}

func newTiKVTxn(store *tikvStore, storeName string) *tikvTxn {
	return &tikvTxn{
		us:        kv.NewUnionStore(newTiKVSnapshot(storeName)),
		store:     store,
		storeName: storeName,
		tid:       0,
		valid:     true,
	}
}

// Implement transaction interface
func (txn *tikvTxn) Get(k kv.Key) ([]byte, error) {
	log.Debugf("[kv] get key:%q, txn:%d", k, txn.tid)
	return txn.us.Get(k)
}

func (txn *tikvTxn) Set(k kv.Key, v []byte) error {
	log.Debugf("[kv] seek %q txn:%d", k, txn.tid)
	return txn.us.Set(k, v)
}

func (txn *tikvTxn) String() string {
	return fmt.Sprintf("%d", txn.tid)
}

func (txn *tikvTxn) Seek(k kv.Key) (kv.Iterator, error) {
	log.Debugf("[kv] seek %q txn:%d", k, txn.tid)
	return txn.us.Seek(k)
}

func (txn *tikvTxn) Delete(k kv.Key) error {
	log.Debugf("[kv] delete %q txn:%d", k, txn.tid)
	return txn.us.Delete(k)
}

func (txn *tikvTxn) SetOption(opt kv.Option, val interface{}) {
	txn.us.SetOption(opt, val)
}

func (txn *tikvTxn) DelOption(opt kv.Option) {
	txn.us.DelOption(opt)
}

func (txn *tikvTxn) doCommit() error {
	// TODO: impl this
	return nil
	//if err := txn.us.CheckLazyConditionPairs(); err != nil {
	//return errors.Trace(err)
	//}

	//err := txn.us.WalkBuffer(func(k kv.Key, v []byte) error {
	//row := append([]byte(nil), k...)
	//if len(v) == 0 { // Deleted marker
	//d := tikv.NewDelete(row)
	//d.AddStringColumn(tikvColFamily, tikvQualifier)
	//err := txn.txn.Delete(txn.storeName, d)
	//if err != nil {
	//return errors.Trace(err)
	//}
	//} else {
	//val := append([]byte(nil), v...)
	//p := tikv.NewPut(row)
	//p.AddValue(tikvColFamilyBytes, tikvQualifierBytes, val)
	//txn.txn.Put(txn.storeName, p)
	//}
	//return nil
	//})

	//if err != nil {
	//return errors.Trace(err)
	//}

	//err = txn.txn.Commit()
	//if err != nil {
	//log.Error(err)
	//return errors.Trace(err)
	//}

	//txn.version = kv.NewVersion(txn.txn.GetCommitTS())
	//log.Debugf("[kv] commit successfully, txn.version:%d", txn.version.Ver)
	//return nil
}

func (txn *tikvTxn) Commit() error {
	if !txn.valid {
		return kv.ErrInvalidTxn
	}
	log.Debugf("[kv] start to commit txn %d", txn.tid)
	defer func() {
		txn.close()
	}()
	return txn.doCommit()
}

func (txn *tikvTxn) close() error {
	txn.us.Release()
	txn.valid = false
	return nil
}

func (txn *tikvTxn) Rollback() error {
	// TODO: impl this
	return nil
	//if !txn.valid {
	//return kv.ErrInvalidTxn
	//}
	//log.Warnf("[kv] Rollback txn %d", txn.tid)
	//return txn.close()
}

func (txn *tikvTxn) LockKeys(keys ...kv.Key) error {
	// TODO: impl this
	//for _, key := range keys {
	//if err := txn.txn.LockRow(txn.storeName, key); err != nil {
	//return errors.Trace(err)
	//}
	//}
	return nil
}
