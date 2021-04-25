// Copyright 2021 PingCAP, Inc.
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

package txn

import (
	"context"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/store/tikv"
	tikvstore "github.com/pingcap/tidb/store/tikv/kv"
	"github.com/pingcap/tidb/tablecodec"
)

type tikvTxn struct {
	*tikv.KVTxn
	idxNameCache map[int64]*model.TableInfo
}

// NewTiKVTxn returns a new Transaction.
func NewTiKVTxn(txn *tikv.KVTxn) kv.Transaction {
	txn.SetOption(tikvstore.KVFilter, TiDBKVFilter{})

	entryLimit := atomic.LoadUint64(&kv.TxnEntrySizeLimit)
	totalLimit := atomic.LoadUint64(&kv.TxnTotalSizeLimit)
	txn.GetUnionStore().SetEntrySizeLimit(entryLimit, totalLimit)

	return &tikvTxn{txn, make(map[int64]*model.TableInfo)}
}

func (txn *tikvTxn) GetTableInfo(id int64) *model.TableInfo {
	return txn.idxNameCache[id]
}

func (txn *tikvTxn) CacheTableInfo(id int64, info *model.TableInfo) {
	txn.idxNameCache[id] = info
}

// lockWaitTime in ms, except that kv.LockAlwaysWait(0) means always wait lock, kv.LockNowait(-1) means nowait lock
func (txn *tikvTxn) LockKeys(ctx context.Context, lockCtx *kv.LockCtx, keysInput ...kv.Key) error {
	keys := toTiKVKeys(keysInput)
	err := txn.KVTxn.LockKeys(ctx, lockCtx, keys...)
	return txn.extractKeyErr(err)
}

func (txn *tikvTxn) Commit(ctx context.Context) error {
	err := txn.KVTxn.Commit(ctx)
	return txn.extractKeyErr(err)
}

// GetSnapshot returns the Snapshot binding to this transaction.
func (txn *tikvTxn) GetSnapshot() kv.Snapshot {
	return &tikvSnapshot{txn.KVTxn.GetSnapshot()}
}

// Iter creates an Iterator positioned on the first entry that k <= entry's key.
// If such entry is not found, it returns an invalid Iterator with no error.
// It yields only keys that < upperBound. If upperBound is nil, it means the upperBound is unbounded.
// The Iterator must be Closed after use.
func (txn *tikvTxn) Iter(k kv.Key, upperBound kv.Key) (kv.Iterator, error) {
	it, err := txn.KVTxn.Iter(k, upperBound)
	return newKVIterator(it), errors.Trace(err)
}

// IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
// The returned iterator will iterate from greater key to smaller key.
// If k is nil, the returned iterator will be positioned at the last key.
// TODO: Add lower bound limit
func (txn *tikvTxn) IterReverse(k kv.Key) (kv.Iterator, error) {
	it, err := txn.KVTxn.IterReverse(k)
	return newKVIterator(it), errors.Trace(err)
}

func (txn *tikvTxn) BatchGet(ctx context.Context, keys []kv.Key) (map[string][]byte, error) {
	return txn.KVTxn.BatchGet(ctx, toTiKVKeys(keys))
}

func (txn *tikvTxn) Delete(k kv.Key) error {
	return txn.KVTxn.Delete(k)
}

func (txn *tikvTxn) Get(ctx context.Context, k kv.Key) ([]byte, error) {
	return txn.KVTxn.Get(ctx, k)
}

func (txn *tikvTxn) Set(k kv.Key, v []byte) error {
	return txn.KVTxn.Set(k, v)
}

func (txn *tikvTxn) GetMemBuffer() kv.MemBuffer {
	return newMemBuffer(txn.KVTxn.GetMemBuffer())
}

func (txn *tikvTxn) GetUnionStore() kv.UnionStore {
	return &tikvUnionStore{txn.KVTxn.GetUnionStore()}
}

func (txn *tikvTxn) SetOption(opt int, val interface{}) {
	switch opt {
	case tikvstore.BinlogInfo:
		txn.SetBinlogExecutor(&binlogExecutor{
			txn:     txn.KVTxn,
			binInfo: val.(*binloginfo.BinlogInfo), // val cannot be other type.
		})
	default:
		txn.KVTxn.SetOption(opt, val)
	}
}

// SetVars sets variables to the transaction.
func (txn *tikvTxn) SetVars(vars interface{}) {
	if vs, ok := vars.(*tikv.Variables); ok {
		txn.KVTxn.SetVars(vs)
	}
}

func (txn *tikvTxn) GetVars() interface{} {
	return txn.KVTxn.GetVars()
}

func (txn *tikvTxn) extractKeyErr(err error) error {
	if e, ok := errors.Cause(err).(*tikvstore.ErrKeyExist); ok {
		return txn.extractKeyExistsErr(e.GetKey())
	}
	return extractKeyErr(err)
}

func (txn *tikvTxn) extractKeyExistsErr(key kv.Key) error {
	tableID, indexID, isRecord, err := tablecodec.DecodeKeyHead(key)
	if err != nil {
		return genKeyExistsError("UNKNOWN", key.String(), err)
	}

	tblInfo := txn.GetTableInfo(tableID)
	if tblInfo == nil {
		return genKeyExistsError("UNKNOWN", key.String(), errors.New("cannot find table info"))
	}
	value, err := txn.KVTxn.GetUnionStore().GetMemBuffer().SelectValueHistory(key, func(value []byte) bool { return len(value) != 0 })
	if err != nil {
		return genKeyExistsError("UNKNOWN", key.String(), err)
	}

	if isRecord {
		return extractKeyExistsErrFromHandle(key, value, tblInfo)
	}
	return extractKeyExistsErrFromIndex(key, value, tblInfo, indexID)
}

// TiDBKVFilter is the filter specific to TiDB to filter out KV pairs that needn't be committed.
type TiDBKVFilter struct{}

// IsUnnecessaryKeyValue defines which kinds of KV pairs from TiDB needn't be committed.
func (f TiDBKVFilter) IsUnnecessaryKeyValue(key, value []byte, flags tikvstore.KeyFlags) bool {
	return tablecodec.IsUntouchedIndexKValue(key, value)
}
