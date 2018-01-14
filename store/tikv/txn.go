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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tipb/go-binlog"
	log "github.com/sirupsen/logrus"
	goctx "golang.org/x/net/context"
)

var (
	_ kv.Transaction = (*tikvTxn)(nil)
)

// tikvTxn implements kv.Transaction.
type tikvTxn struct {
	snapshot  *tikvSnapshot
	us        kv.UnionStore
	store     *tikvStore // for connection to region.
	startTS   uint64
	startTime time.Time // Monotonic timestamp for recording txn time consuming.
	commitTS  uint64
	valid     bool
	lockKeys  [][]byte
	dirty     bool
	setCnt    int64
}

func newTiKVTxn(store *tikvStore) (*tikvTxn, error) {
	bo := NewBackoffer(tsoMaxBackoff, goctx.Background())
	startTS, err := store.getTimestampWithRetry(bo)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return newTikvTxnWithStartTS(store, startTS)
}

// newTikvTxnWithStartTS creates a txn with startTS.
func newTikvTxnWithStartTS(store *tikvStore, startTS uint64) (*tikvTxn, error) {
	ver := kv.NewVersion(startTS)
	snapshot := newTiKVSnapshot(store, ver)
	return &tikvTxn{
		snapshot:  snapshot,
		us:        kv.NewUnionStore(snapshot),
		store:     store,
		startTS:   startTS,
		startTime: time.Now(),
		valid:     true,
	}, nil
}

// SetMemBufCap sets the transaction's MemBuffer capability, to reduce memory allocations.
func (txn *tikvTxn) SetCap(cap int) {
	txn.us.SetCap(cap)
}

// Reset reset tikvTxn's membuf.
func (txn *tikvTxn) Reset() {
	txn.us.Reset()
}

// Get implements transaction interface.
func (txn *tikvTxn) Get(k kv.Key) ([]byte, error) {
	txnCmdCounter.WithLabelValues("get").Inc()
	start := time.Now()
	defer func() { txnCmdHistogram.WithLabelValues("get").Observe(time.Since(start).Seconds()) }()

	ret, err := txn.us.Get(k)
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = txn.store.CheckVisibility(txn.startTS)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return ret, nil
}

func (txn *tikvTxn) Set(k kv.Key, v []byte) error {
	txn.setCnt++

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
	switch opt {
	case kv.IsolationLevel:
		txn.snapshot.isolationLevel = val.(kv.IsoLevel)
	case kv.Priority:
		txn.snapshot.priority = kvPriorityToCommandPri(val.(int))
	case kv.NotFillCache:
		txn.snapshot.notFillCache = val.(bool)
	case kv.SyncLog:
		txn.snapshot.syncLog = val.(bool)
	}
}

func (txn *tikvTxn) DelOption(opt kv.Option) {
	txn.us.DelOption(opt)
	if opt == kv.IsolationLevel {
		txn.snapshot.isolationLevel = kv.SI
	}
}

func (txn *tikvTxn) Commit(ctx goctx.Context) error {
	if !txn.valid {
		return kv.ErrInvalidTxn
	}
	defer txn.close()

	txnCmdCounter.WithLabelValues("set").Add(float64(txn.setCnt))
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
	err = committer.execute(ctx)
	if err != nil {
		committer.writeFinishBinlog(binlog.BinlogType_Rollback, 0)
		return errors.Trace(err)
	}
	committer.writeFinishBinlog(binlog.BinlogType_Commit, int64(committer.commitTS))
	txn.commitTS = committer.commitTS
	return nil
}

func (txn *tikvTxn) close() {
	txn.valid = false
}

func (txn *tikvTxn) Rollback() error {
	if !txn.valid {
		return kv.ErrInvalidTxn
	}
	txn.close()
	logMsg := fmt.Sprintf("[kv] Rollback txn %d", txn.StartTS())
	if txn.store.mock {
		log.Debug(logMsg)
	} else {
		log.Info(logMsg)
	}
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

func (txn *tikvTxn) Len() int {
	return txn.us.Len()
}

func (txn *tikvTxn) Size() int {
	return txn.us.Size()
}

func (txn *tikvTxn) GetMemBuffer() kv.MemBuffer {
	return txn.us.GetMemBuffer()
}

func (txn *tikvTxn) GetSnapshot() kv.Snapshot {
	return txn.snapshot
}
