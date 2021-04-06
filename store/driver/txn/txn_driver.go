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

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	tikvstore "github.com/pingcap/tidb/store/tikv/kv"
	"github.com/pingcap/tidb/store/tikv/unionstore"
	"github.com/pingcap/tidb/tablecodec"
)

type tikvTxn struct {
	*tikv.KVTxn
	idxNameCache map[int64]*model.TableInfo
}

// NewTiKVTxn returns a new Transaction.
func NewTiKVTxn(txn *tikv.KVTxn) kv.Transaction {
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
	err := txn.KVTxn.LockKeys(ctx, lockCtx, keysInput...)
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
	return txn.KVTxn.Iter(k, upperBound)
}

// IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
// The returned iterator will iterate from greater key to smaller key.
// If k is nil, the returned iterator will be positioned at the last key.
// TODO: Add lower bound limit
func (txn *tikvTxn) IterReverse(k kv.Key) (kv.Iterator, error) {
	return txn.KVTxn.IterReverse(k)
}

type memBuffer struct {
	*unionstore.MemDB
}

func (txn *tikvTxn) GetMemBuffer() kv.MemBuffer {
	return &memBuffer{txn.KVTxn.GetMemBuffer()}
}

// Iter creates an Iterator positioned on the first entry that k <= entry's key.
// If such entry is not found, it returns an invalid Iterator with no error.
// It yields only keys that < upperBound. If upperBound is nil, it means the upperBound is unbounded.
// The Iterator must be Closed after use.
func (m *memBuffer) Iter(k kv.Key, upperBound kv.Key) (kv.Iterator, error) {
	return m.MemDB.Iter(k, upperBound)
}

// IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
// The returned iterator will iterate from greater key to smaller key.
// If k is nil, the returned iterator will be positioned at the last key.
// TODO: Add lower bound limit
func (m *memBuffer) IterReverse(k kv.Key) (kv.Iterator, error) {
	return m.MemDB.IterReverse(k)
}

// SnapshotIter returns a Iterator for a snapshot of MemBuffer.
func (m *memBuffer) SnapshotIter(k, upperbound kv.Key) kv.Iterator {
	return m.MemDB.SnapshotIter(k, upperbound)
}

func (txn *tikvTxn) GetUnionStore() kv.UnionStore {
	return &tikvUnionStore{txn.KVTxn.GetUnionStore()}
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

//tikvUnionStore implements kv.UnionStore
type tikvUnionStore struct {
	*unionstore.KVUnionStore
}

func (u *tikvUnionStore) GetMemBuffer() kv.MemBuffer {
	return &memBuffer{u.KVUnionStore.GetMemBuffer()}
}

func (u *tikvUnionStore) Iter(k kv.Key, upperBound kv.Key) (kv.Iterator, error) {
	return u.KVUnionStore.Iter(k, upperBound)
}

// IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
// The returned iterator will iterate from greater key to smaller key.
// If k is nil, the returned iterator will be positioned at the last key.
// TODO: Add lower bound limit
func (u *tikvUnionStore) IterReverse(k kv.Key) (kv.Iterator, error) {
	return u.KVUnionStore.IterReverse(k)
}
