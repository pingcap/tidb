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
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tipb/go-binlog"
	"golang.org/x/net/context"
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
}

func newTiKVTxn(store *tikvStore) (*tikvTxn, error) {
	bo := NewBackoffer(tsoMaxBackoff, context.Background())
	startTS, err := store.getTimestampWithRetry(bo)
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
	txnCmdCounter.WithLabelValues("get").Inc()
	start := time.Now()
	defer func() { txnCmdHistogram.WithLabelValues("get").Observe(time.Since(start).Seconds()) }()

	ret, err := txn.us.Get(k)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return ret, nil
}

func (txn *tikvTxn) Set(k kv.Key, v []byte) error {
	txnCmdCounter.WithLabelValues("set").Inc()

	txn.dirty = true
	return txn.us.Set(k, v)
}

func (txn *tikvTxn) String() string {
	return fmt.Sprintf("%d", txn.StartTS())
}

func (txn *tikvTxn) Seek(k kv.Key) (kv.Iterator, error) {
	txnCmdCounter.WithLabelValues("seek").Inc()
	start := time.Now()
	defer func() { txnCmdHistogram.WithLabelValues("seek").Observe(time.Since(start).Seconds()) }()

	return txn.us.Seek(k)
}

// SeekReverse creates a reversed Iterator positioned on the first entry which key is less than k.
func (txn *tikvTxn) SeekReverse(k kv.Key) (kv.Iterator, error) {
	txnCmdCounter.WithLabelValues("seek_reverse").Inc()
	start := time.Now()
	defer func() { txnCmdHistogram.WithLabelValues("seek_reverse").Observe(time.Since(start).Seconds()) }()

	return txn.us.SeekReverse(k)
}

func (txn *tikvTxn) Delete(k kv.Key) error {
	txnCmdCounter.WithLabelValues("delete").Inc()

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

	txnCmdCounter.WithLabelValues("commit").Inc()
	start := time.Now()
	defer func() { txnCmdHistogram.WithLabelValues("commit").Observe(time.Since(start).Seconds()) }()

	if err := txn.us.CheckLazyConditionPairs(); err != nil {
		return errors.Trace(err)
	}

	committer, err := newTwoPhaseCommitter(txn)
	if err != nil {
		return errors.Trace(err)
	}
	if committer == nil {
		return nil
	}
	err = committer.execute()
	if err != nil {
		committer.writeFinishBinlog(binlog.BinlogType_Rollback, 0)
		return errors.Trace(err)
	}
	committer.writeFinishBinlog(binlog.BinlogType_Commit, int64(committer.commitTS))
	txn.commitTS = committer.commitTS
	return nil
}

func (txn *tikvTxn) close() error {
	txn.valid = false
	return nil
}

func (txn *tikvTxn) Rollback() error {
	if !txn.valid {
		return kv.ErrInvalidTxn
	}
	txn.close()
	log.Infof("[kv] Rollback txn %d", txn.StartTS())
	txnCmdCounter.WithLabelValues("rollback").Inc()

	return nil
}

func (txn *tikvTxn) LockKeys(keys ...kv.Key) error {
	txnCmdCounter.WithLabelValues("lock_keys").Inc()
	for _, key := range keys {
		txn.lockKeys = append(txn.lockKeys, key)
	}
	return nil
}

func (txn *tikvTxn) IsReadOnly() bool {
	return !txn.dirty
}

func (txn *tikvTxn) StartTS() uint64 {
	return txn.startTS
}

func (txn *tikvTxn) Valid() bool {
	return txn.valid
}
