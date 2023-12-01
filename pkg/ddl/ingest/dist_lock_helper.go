// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ingest

import (
	"context"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/structure"
)

const mDistributedLock = "distLock"

var _ meta.DataStore = (*distLockDataStore)(nil)

type distLockDataStore struct {
	store kv.Storage
}

func (s *distLockDataStore) Begin(ctx context.Context) (meta.DataTxn, error) {
	txn, err := s.store.Begin()
	if err != nil {
		return nil, err
	}
	metaTxn := meta.NewMetaStructure(txn)
	ret := &distLockDataTxn{ctx: ctx, txn: txn, metaTxn: metaTxn}
	return ret, nil
}

func newDistLockDataStore(store kv.Storage) *distLockDataStore {
	return &distLockDataStore{store: store}
}

var _ meta.DataTxn = (*distLockDataTxn)(nil)

type distLockDataTxn struct {
	ctx     context.Context
	txn     kv.Transaction
	metaTxn *structure.TxStructure
}

func (t *distLockDataTxn) StartTS() uint64 {
	return t.txn.StartTS()
}

func (t *distLockDataTxn) Rollback() {
	_ = t.txn.Rollback()
}

func (t *distLockDataTxn) Commit() error {
	return t.txn.Commit(t.ctx)
}

func (t *distLockDataTxn) Set(key, value []byte) error {
	mKey := distLockKey(key)
	return t.metaTxn.Set(mKey, value)
}

func (t *distLockDataTxn) Get(key []byte) ([]byte, error) {
	mKey := distLockKey(key)
	val, err := t.metaTxn.Get(mKey)
	if kv.ErrNotExist.Equal(err) {
		err = nil
	}
	return val, err
}

func (t *distLockDataTxn) Del(key []byte) error {
	mKey := distLockKey(key)
	return t.metaTxn.Clear(mKey)
}

func distLockKey(key []byte) []byte {
	ret := make([]byte, len(mDistributedLock)+1+len(key))
	copy(ret, mDistributedLock)
	ret[len(mDistributedLock)] = '/'
	copy(ret[len(mDistributedLock)+1:], key)
	return ret
}
