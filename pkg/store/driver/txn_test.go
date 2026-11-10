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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

type mockErrInterceptor struct {
	err error
}

func (m *mockErrInterceptor) OnGet(_ context.Context, _ kv.Snapshot, k kv.Key, _ ...kv.GetOption) (kv.ValueEntry, error) {
	return kv.ValueEntry{}, m.err
}

func (m *mockErrInterceptor) OnBatchGet(_ context.Context, _ kv.Snapshot, _ []kv.Key, _ ...kv.BatchGetOption) (map[string]kv.ValueEntry, error) {
	return nil, m.err
}

func (m *mockErrInterceptor) OnIter(_ kv.Snapshot, _ kv.Key, _ kv.Key) (kv.Iterator, error) {
	return nil, m.err
}

func (m *mockErrInterceptor) OnIterReverse(_ kv.Snapshot, _ kv.Key, _ kv.Key) (kv.Iterator, error) {
	return nil, m.err
}

func validCommitTS(t *testing.T, commitTS uint64) {
	require.Greater(t, commitTS, uint64(0))
	now := time.Now()
	tm := oracle.GetTimeFromTS(commitTS)
	require.InDelta(t, now.Unix(), tm.Unix(), 10)
}

func TestTxnGet(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()
	clearStoreData(t, store)

	prepareSnapshot(t, store, [][]any{{"k1", "v1"}})
	txn, err := store.Begin()
	require.NoError(t, err)
	require.NotNil(t, txn)

	// should return snapshot value if no dirty data
	entry, err := txn.Get(context.Background(), kv.Key("k1"))
	require.NoError(t, err)
	require.Equal(t, kv.NewValueEntry([]byte("v1"), 0), entry)
	// should return the CommitTS for option WithReturnCommitTS
	entry, err = txn.Get(context.Background(), kv.Key("k1"), kv.WithReturnCommitTS())
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), entry.Value)
	validCommitTS(t, entry.CommitTS)

	// insert but not commit
	err = txn.Set(kv.Key("k1"), kv.Key("v1+"))
	require.NoError(t, err)

	// should return dirty data if dirty data exists
	entry, err = txn.Get(context.Background(), kv.Key("k1"))
	require.NoError(t, err)
	require.Equal(t, kv.NewValueEntry([]byte("v1+"), 0), entry)
	// dirty data's commitTS should be 0
	entry, err = txn.Get(context.Background(), kv.Key("k1"), kv.WithReturnCommitTS())
	require.NoError(t, err)
	require.Equal(t, kv.NewValueEntry([]byte("v1+"), 0), entry)

	err = txn.Set(kv.Key("k2"), []byte("v2+"))
	require.NoError(t, err)

	// should return dirty data if dirty data exists
	entry, err = txn.Get(context.Background(), kv.Key("k2"))
	require.NoError(t, err)
	require.Equal(t, kv.NewValueEntry([]byte("v2+"), 0), entry)

	// delete but not commit
	err = txn.Delete(kv.Key("k1"))
	require.NoError(t, err)

	// should return kv.ErrNotExist if deleted
	entry, err = txn.Get(context.Background(), kv.Key("k1"))
	require.Equal(t, kv.ValueEntry{}, entry)
	require.True(t, kv.ErrNotExist.Equal(err))

	// should return kv.ErrNotExist if not exist
	entry, err = txn.Get(context.Background(), kv.Key("kn"))
	require.Equal(t, kv.ValueEntry{}, entry)
	require.True(t, kv.ErrNotExist.Equal(err))

	// should return kv.ErrNotExist if not exist (WithReturnCommitTS specified)
	entry, err = txn.Get(context.Background(), kv.Key("k1"), kv.WithReturnCommitTS())
	require.Equal(t, kv.ValueEntry{}, entry)
	require.True(t, kv.ErrNotExist.Equal(err))

	// make snapshot returns error
	errInterceptor := &mockErrInterceptor{err: errors.New("error")}
	txn.SetOption(kv.SnapInterceptor, errInterceptor)

	// should return kv.ErrNotExist because k1 is deleted in memBuff
	entry, err = txn.Get(context.Background(), kv.Key("k1"))
	require.Equal(t, kv.ValueEntry{}, entry)
	require.True(t, kv.ErrNotExist.Equal(err))

	// should return dirty data because k2 is in memBuff
	entry, err = txn.Get(context.Background(), kv.Key("k2"))
	require.NoError(t, err)
	require.Equal(t, kv.NewValueEntry([]byte("v2+"), 0), entry)

	// should return error because kn is read from snapshot
	entry, err = txn.Get(context.Background(), kv.Key("kn"))
	require.Equal(t, kv.ValueEntry{}, entry)
	require.Equal(t, errInterceptor.err, err)
}

func TestTxnBatchGet(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()
	clearStoreData(t, store)

	prepareSnapshot(t, store, [][]any{{"k1", "v1"}, {"k2", "v2"}, {"k3", "v3"}, {"k4", "v4"}})
	txn, err := store.Begin()
	require.NoError(t, err)

	// Get the snapshot commit ts
	entry, err := txn.Get(context.Background(), kv.Key("k1"), kv.WithReturnCommitTS())
	require.NoError(t, err)
	validCommitTS(t, entry.CommitTS)
	commitTS := entry.CommitTS

	// Test BatchGet from snapshot
	result, err := txn.BatchGet(context.Background(), []kv.Key{kv.Key("k1"), kv.Key("k2"), kv.Key("k3"), kv.Key("kn")})
	require.NoError(t, err)
	require.Equal(t, 3, len(result))
	require.Equal(t, kv.NewValueEntry([]byte("v1"), 0), result["k1"])
	require.Equal(t, kv.NewValueEntry([]byte("v2"), 0), result["k2"])
	require.Equal(t, kv.NewValueEntry([]byte("v3"), 0), result["k3"])

	// Test BatchGet from snapshot with WithReturnCommitTS option
	result, err = txn.BatchGet(context.Background(), []kv.Key{kv.Key("k1"), kv.Key("k2"), kv.Key("k3"), kv.Key("kn")}, kv.WithReturnCommitTS())
	require.NoError(t, err)
	require.Equal(t, 3, len(result))
	require.Equal(t, kv.NewValueEntry([]byte("v1"), commitTS), result["k1"])
	require.Equal(t, kv.NewValueEntry([]byte("v2"), commitTS), result["k2"])
	require.Equal(t, kv.NewValueEntry([]byte("v3"), commitTS), result["k3"])

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
	require.Equal(t, kv.NewValueEntry([]byte("v1+"), 0), result["k1"])
	require.Equal(t, kv.NewValueEntry([]byte("v3"), 0), result["k3"])
	require.Equal(t, kv.NewValueEntry([]byte("v4+"), 0), result["k4"])

	// return data if not read from snapshot
	result, err = txn.BatchGet(context.Background(), []kv.Key{kv.Key("k1"), kv.Key("k4")})
	require.NoError(t, err)
	require.Equal(t, 2, len(result))
	require.Equal(t, kv.NewValueEntry([]byte("v1+"), 0), result["k1"])
	require.Equal(t, kv.NewValueEntry([]byte("v4+"), 0), result["k4"])

	// test WithReturnCommitTS option
	result, err = txn.BatchGet(context.Background(), []kv.Key{kv.Key("k1"), kv.Key("k2"), kv.Key("k3"), kv.Key("k4")}, kv.WithReturnCommitTS())
	require.Equal(t, 3, len(result))
	require.Equal(t, kv.NewValueEntry([]byte("v1+"), 0), result["k1"])
	require.Equal(t, kv.NewValueEntry([]byte("v3"), commitTS), result["k3"])
	require.Equal(t, kv.NewValueEntry([]byte("v4+"), 0), result["k4"])

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

	prepareSnapshot(t, store, [][]any{{"k1", "v1"}, {"k3", "v3"}, {"k5", "v5"}, {"k7", "v7"}, {"k9", "v9"}})
	txn, err := store.Begin()
	require.NoError(t, err)

	iter, err := txn.Iter(kv.Key("k3"), kv.Key("k9"))
	require.NoError(t, err)
	checkIter(t, iter, [][]any{{"k3", "v3"}, {"k5", "v5"}, {"k7", "v7"}})

	iter, err = txn.IterReverse(kv.Key("k9"), nil)
	require.NoError(t, err)
	checkIter(t, iter, [][]any{{"k7", "v7"}, {"k5", "v5"}, {"k3", "v3"}, {"k1", "v1"}})

	iter, err = txn.IterReverse(kv.Key("k9"), kv.Key("k3"))
	require.NoError(t, err)
	checkIter(t, iter, [][]any{{"k7", "v7"}, {"k5", "v5"}, {"k3", "v3"}})

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
	checkIter(t, iter, [][]any{{"k3", "v3+"}, {"k31", "v31+"}, {"k7", "v7"}})

	iter, err = txn.IterReverse(kv.Key("k9"), nil)
	require.NoError(t, err)
	checkIter(t, iter, [][]any{{"k7", "v7"}, {"k31", "v31+"}, {"k3", "v3+"}, {"k1", "v1+"}})

	// make snapshot returns error
	errInterceptor := &mockErrInterceptor{err: errors.New("error")}
	txn.SetOption(kv.SnapInterceptor, errInterceptor)

	iter, err = txn.Iter(kv.Key("k1"), kv.Key("k2"))
	require.Equal(t, errInterceptor.err, err)
	require.Nil(t, iter)
}
