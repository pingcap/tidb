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

func (txn *tikvTxn) GetMemBuffer() kv.MemBuffer {
	return txn.KVTxn.GetMemBuffer()
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
	return u.KVUnionStore.GetMemBuffer()
}
