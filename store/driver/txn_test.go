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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package driver

import (
	"context"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/stretchr/testify/require"
)

type mockErrInterceptor struct {
	err error
}

func (m *mockErrInterceptor) OnGet(_ context.Context, _ kv.Snapshot, _ kv.Key) ([]byte, error) {
	return nil, m.err
}

func (m *mockErrInterceptor) OnBatchGet(_ context.Context, _ kv.Snapshot, _ []kv.Key) (map[string][]byte, error) {
	return nil, m.err
}

func (m *mockErrInterceptor) OnIter(_ kv.Snapshot, _ kv.Key, _ kv.Key) (kv.Iterator, error) {
	return nil, m.err
}

func (m *mockErrInterceptor) OnIterReverse(_ kv.Snapshot, _ kv.Key) (kv.Iterator, error) {
	return nil, m.err
}

func TestTxnGet(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()
	clearStoreData(t, store)

	prepareSnapshot(t, store, [][]interface{}{{"k1", "v1"}})
	txn, err := store.Begin()
	require.NoError(t, err)
	require.NotNil(t, txn)

	// should return snapshot value if no dirty data
	v, err := txn.Get(context.Background(), kv.Key("k1"))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), v)

	// insert but not commit
	err = txn.Set(kv.Key("k1"), kv.Key("v1+"))
	require.NoError(t, err)

	// should return dirty data if dirty data exists
	v, err = txn.Get(context.Background(), kv.Key("k1"))
	require.NoError(t, err)
	require.Equal(t, []byte("v1+"), v)

	err = txn.Set(kv.Key("k2"), []byte("v2+"))
	require.NoError(t, err)

	// should return dirty data if dirty data exists
	v, err = txn.Get(context.Background(), kv.Key("k2"))
	require.NoError(t, err)
	require.Equal(t, []byte("v2+"), v)

	// delete but not commit
	err = txn.Delete(kv.Key("k1"))
	require.NoError(t, err)

	// should return kv.ErrNotExist if deleted
	v, err = txn.Get(context.Background(), kv.Key("k1"))
	require.Nil(t, v)
	require.True(t, kv.ErrNotExist.Equal(err))

	// should return kv.ErrNotExist if not exist
	v, err = txn.Get(context.Background(), kv.Key("kn"))
	require.Nil(t, v)
	require.True(t, kv.ErrNotExist.Equal(err))

	// make snapshot returns error
	errInterceptor := &mockErrInterceptor{err: errors.New("error")}
	txn.SetOption(kv.SnapInterceptor, errInterceptor)

	// should return kv.ErrNotExist because k1 is deleted in memBuff
	v, err = txn.Get(context.Background(), kv.Key("k1"))
	require.Nil(t, v)
	require.True(t, kv.ErrNotExist.Equal(err))

	// should return dirty data because k2 is in memBuff
	v, err = txn.Get(context.Background(), kv.Key("k2"))
	require.NoError(t, err)
	require.Equal(t, []byte("v2+"), v)

	// should return error because kn is read from snapshot
	v, err = txn.Get(context.Background(), kv.Key("kn"))
	require.Nil(t, v)
	require.Equal(t, errInterceptor.err, err)
}

func TestTxnBatchGet(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()
	clearStoreData(t, store)

	prepareSnapshot(t, store, [][]interface{}{{"k1", "v1"}, {"k2", "v2"}, {"k3", "v3"}, {"k4", "v4"}})
	txn, err := store.Begin()
	require.NoError(t, err)

	result, err := txn.BatchGet(context.Background(), []kv.Key{kv.Key("k1"), kv.Key("k2"), kv.Key("k3"), kv.Key("kn")})
	require.NoError(t, err)
	require.Equal(t, 3, len(result))
	require.Equal(t, []byte("v1"), result["k1"])
	require.Equal(t, []byte("v2"), result["k2"])
	require.Equal(t, []byte("v3"), result["k3"])

	// make some dirty data
	err = txn.Set(kv.Key("k1"), []byte("v1+"))
	require.NoError(t, err)
	err = txn.Set(kv.Key("k4"), []byte("v4+"))
	require.NoError(t, err)
	err = txn.Delete(kv.Key("k2"))
	require.NoError(t, err)

	result, err = txn.BatchGet(context.Background(), []kv.Key{kv.Key("k1"), kv.Key("k2"), kv.Key("k3"), kv.Key("k4"), kv.Key("kn")})
	require.NoError(t, err)
	require.Equal(t, 3, len(result))
	require.Equal(t, []byte("v1+"), result["k1"])
	require.Equal(t, []byte("v3"), result["k3"])
	require.Equal(t, []byte("v4+"), result["k4"])

	// return data if not read from snapshot
	result, err = txn.BatchGet(context.Background(), []kv.Key{kv.Key("k1"), kv.Key("k4")})
	require.NoError(t, err)
	require.Equal(t, 2, len(result))
	require.Equal(t, []byte("v1+"), result["k1"])
	require.Equal(t, []byte("v4+"), result["k4"])

	// make snapshot returns error
	errInterceptor := &mockErrInterceptor{err: errors.New("error")}
	txn.SetOption(kv.SnapInterceptor, errInterceptor)

	// fails if read from snapshot
	result, err = txn.BatchGet(context.Background(), []kv.Key{kv.Key("k3")})
	require.Nil(t, result)
	require.Equal(t, errInterceptor.err, err)
	result, err = txn.BatchGet(context.Background(), []kv.Key{kv.Key("k1"), kv.Key("k3"), kv.Key("k4")})
	require.Nil(t, result)
	require.Equal(t, errInterceptor.err, err)
	result, err = txn.BatchGet(context.Background(), []kv.Key{kv.Key("k1"), kv.Key("k4"), kv.Key("kn")})
	require.Nil(t, result)
	require.Equal(t, errInterceptor.err, err)
}

func TestTxnScan(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()
	clearStoreData(t, store)

	prepareSnapshot(t, store, [][]interface{}{{"k1", "v1"}, {"k3", "v3"}, {"k5", "v5"}, {"k7", "v7"}, {"k9", "v9"}})
	txn, err := store.Begin()
	require.NoError(t, err)

	iter, err := txn.Iter(kv.Key("k3"), kv.Key("k9"))
	require.NoError(t, err)
	checkIter(t, iter, [][]interface{}{{"k3", "v3"}, {"k5", "v5"}, {"k7", "v7"}})

	iter, err = txn.IterReverse(kv.Key("k9"))
	require.NoError(t, err)
	checkIter(t, iter, [][]interface{}{{"k7", "v7"}, {"k5", "v5"}, {"k3", "v3"}, {"k1", "v1"}})

	// make some dirty data
	err = txn.Set(kv.Key("k1"), []byte("v1+"))
	require.NoError(t, err)
	err = txn.Set(kv.Key("k3"), []byte("v3+"))
	require.NoError(t, err)
	err = txn.Set(kv.Key("k31"), []byte("v31+"))
	require.NoError(t, err)
	err = txn.Delete(kv.Key("k5"))
	require.NoError(t, err)

	iter, err = txn.Iter(kv.Key("k3"), kv.Key("k9"))
	require.NoError(t, err)
	checkIter(t, iter, [][]interface{}{{"k3", "v3+"}, {"k31", "v31+"}, {"k7", "v7"}})

	iter, err = txn.IterReverse(kv.Key("k9"))
	require.NoError(t, err)
	checkIter(t, iter, [][]interface{}{{"k7", "v7"}, {"k31", "v31+"}, {"k3", "v3+"}, {"k1", "v1+"}})

	// make snapshot returns error
	errInterceptor := &mockErrInterceptor{err: errors.New("error")}
	txn.SetOption(kv.SnapInterceptor, errInterceptor)

	iter, err = txn.Iter(kv.Key("k1"), kv.Key("k2"))
	require.Equal(t, errInterceptor.err, err)
	require.Nil(t, iter)
}
