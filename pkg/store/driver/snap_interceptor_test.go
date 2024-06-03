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
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/stretchr/testify/require"
)

func TestSnapshotWithoutInterceptor(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()
	clearStoreData(t, store)

	snap := prepareSnapshot(t, store, [][]any{
		{"k1", "v1"},
		{"k2", "v2"},
		{"k3", "v3"},
	})

	ctx := context.Background()
	// Test for Get
	val, err := snap.Get(ctx, kv.Key("k1"))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), val)

	val, err = snap.Get(ctx, kv.Key("k2"))
	require.NoError(t, err)
	require.Equal(t, []byte("v2"), val)

	val, err = snap.Get(ctx, kv.Key("kn"))
	require.True(t, kv.ErrNotExist.Equal(err))
	require.Nil(t, val)

	// Test for BatchGet
	result, err := snap.BatchGet(ctx, []kv.Key{kv.Key("k1"), kv.Key("k3")})
	require.NoError(t, err)
	require.Equal(t, map[string][]byte{"k1": []byte("v1"), "k3": []byte("v3")}, result)

	result, err = snap.BatchGet(ctx, []kv.Key{kv.Key("k3"), kv.Key("kn")})
	require.NoError(t, err)
	require.Equal(t, map[string][]byte{"k3": []byte("v3")}, result)

	result, err = snap.BatchGet(ctx, []kv.Key{kv.Key("kn"), kv.Key("kn2")})
	require.NoError(t, err)
	require.Equal(t, map[string][]byte{}, result)

	result, err = snap.BatchGet(ctx, []kv.Key{})
	require.NoError(t, err)
	require.Equal(t, map[string][]byte{}, result)

	// Test for Iter
	iter, err := snap.Iter(nil, nil)
	require.NoError(t, err)
	checkIter(t, iter, [][]any{{"k1", "v1"}, {"k2", "v2"}, {"k3", "v3"}})

	iter, err = snap.Iter(nil, kv.Key("k3"))
	require.NoError(t, err)
	checkIter(t, iter, [][]any{{"k1", "v1"}, {"k2", "v2"}})

	iter, err = snap.Iter(kv.Key("k2"), kv.Key("k3"))
	require.NoError(t, err)
	checkIter(t, iter, [][]any{{"k2", "v2"}})

	iter, err = snap.Iter(kv.Key("k2"), nil)
	require.NoError(t, err)
	checkIter(t, iter, [][]any{{"k2", "v2"}, {"k3", "v3"}})

	iter, err = snap.Iter(kv.Key("k4"), nil)
	require.NoError(t, err)
	checkIter(t, iter, [][]any{})

	// Test for IterReverse
	iter, err = snap.IterReverse(kv.Key("k5"), nil)
	require.NoError(t, err)
	checkIter(t, iter, [][]any{{"k3", "v3"}, {"k2", "v2"}, {"k1", "v1"}})

	iter, err = snap.IterReverse(kv.Key("k3"), nil)
	require.NoError(t, err)
	checkIter(t, iter, [][]any{{"k2", "v2"}, {"k1", "v1"}})

	iter, err = snap.IterReverse(kv.Key("k4"), kv.Key("k2"))
	require.NoError(t, err)
	checkIter(t, iter, [][]any{{"k3", "v3"}, {"k2", "v2"}})

	iter, err = snap.IterReverse(kv.Key("k1"), nil)
	require.NoError(t, err)
	checkIter(t, iter, [][]any{})

	iter, err = snap.IterReverse(kv.Key("k0"), nil)
	require.NoError(t, err)
	checkIter(t, iter, [][]any{})
}

type mockSnapshotInterceptor struct {
	spy []any
}

func (m *mockSnapshotInterceptor) OnGet(ctx context.Context, snap kv.Snapshot, k kv.Key) ([]byte, error) {
	m.spy = []any{"OnGet", ctx, k}
	if len(k) == 0 {
		return nil, fmt.Errorf("MockErr%s", m.spy[0])
	}
	return snap.Get(ctx, k)
}

func (m *mockSnapshotInterceptor) OnBatchGet(ctx context.Context, snap kv.Snapshot, keys []kv.Key) (map[string][]byte, error) {
	m.spy = []any{"OnBatchGet", ctx, keys}
	if len(keys) == 0 {
		return nil, fmt.Errorf("MockErr%s", m.spy[0])
	}
	return snap.BatchGet(ctx, keys)
}

func (m *mockSnapshotInterceptor) OnIter(snap kv.Snapshot, k kv.Key, upperBound kv.Key) (kv.Iterator, error) {
	m.spy = []any{"OnIter", k, upperBound}
	if len(k) == 0 {
		return nil, fmt.Errorf("MockErr%s", m.spy[0])
	}
	return snap.Iter(k, upperBound)
}

func (m *mockSnapshotInterceptor) OnIterReverse(snap kv.Snapshot, k kv.Key, lowerBound kv.Key) (kv.Iterator, error) {
	m.spy = []any{"OnIterReverse", k, lowerBound}
	if len(k) == 0 {
		return nil, fmt.Errorf("MockErr%s", m.spy[0])
	}
	return snap.IterReverse(k, lowerBound)
}

func TestSnapshotWitInterceptor(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()
	clearStoreData(t, store)

	snap := prepareSnapshot(t, store, [][]any{
		{"k1", "v1"},
		{"k2", "v2"},
		{"k3", "v3"},
	})
	mockInterceptor := &mockSnapshotInterceptor{}
	snap.SetOption(kv.SnapInterceptor, mockInterceptor)

	ctx := context.Background()

	// Test for Get
	k := kv.Key("k1")
	v, err := snap.Get(ctx, k)
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), v)
	require.Equal(t, []any{"OnGet", ctx, k}, mockInterceptor.spy)

	v, err = snap.Get(ctx, kv.Key{})
	require.Equal(t, "MockErrOnGet", err.Error())
	require.Nil(t, v)
	require.Equal(t, []any{"OnGet", ctx, kv.Key{}}, mockInterceptor.spy)

	// Test for BatchGet
	keys := []kv.Key{kv.Key("k2"), kv.Key("k3")}
	result, err := snap.BatchGet(ctx, keys)
	require.NoError(t, err)
	require.Equal(t, map[string][]byte{"k2": []byte("v2"), "k3": []byte("v3")}, result)
	require.Equal(t, []any{"OnBatchGet", ctx, keys}, mockInterceptor.spy)

	result, err = snap.BatchGet(ctx, []kv.Key{})
	require.Equal(t, "MockErrOnBatchGet", err.Error())
	require.Nil(t, result)
	require.Equal(t, []any{"OnBatchGet", ctx, []kv.Key{}}, mockInterceptor.spy)

	// Test for Iter
	k1 := kv.Key("k1")
	k2 := kv.Key("k3")
	iter, err := snap.Iter(k1, k2)
	require.NoError(t, err)
	checkIter(t, iter, [][]any{{"k1", "v1"}, {"k2", "v2"}})
	require.Equal(t, []any{"OnIter", k1, k2}, mockInterceptor.spy)

	iter, err = snap.Iter(kv.Key{}, k2)
	require.Equal(t, "MockErrOnIter", err.Error())
	require.Nil(t, iter)
	require.Equal(t, []any{"OnIter", kv.Key{}, k2}, mockInterceptor.spy)

	// Test for IterReverse
	k = kv.Key("k3")
	iter, err = snap.IterReverse(k, nil)
	require.NoError(t, err)
	checkIter(t, iter, [][]any{{"k2", "v2"}, {"k1", "v1"}})
	require.Equal(t, []any{"OnIterReverse", k, kv.Key(nil)}, mockInterceptor.spy)

	iter, err = snap.IterReverse(k, kv.Key("k2"))
	require.NoError(t, err)
	checkIter(t, iter, [][]any{{"k2", "v2"}})
	require.Equal(t, []any{"OnIterReverse", k, kv.Key("k2")}, mockInterceptor.spy)

	iter, err = snap.IterReverse(kv.Key{}, nil)
	require.Equal(t, "MockErrOnIterReverse", err.Error())
	require.Nil(t, iter)
	require.Equal(t, []any{"OnIterReverse", kv.Key{}, kv.Key(nil)}, mockInterceptor.spy)

	snap.SetOption(kv.TiKVClientReadTimeout, uint64(10))
}

func checkIter(t *testing.T, iter kv.Iterator, expected [][]any) {
	for i, item := range expected {
		require.True(t, iter.Valid(), "%dst loop: invalid iter", i)
		key := item[0]
		val := item[1]
		require.Equal(t, kv.Key(makeBytes(key)), iter.Key(), "loop %d: %s != %s ", i, key, string(iter.Key()))
		require.Equal(t, makeBytes(val), iter.Value(), "loop %d: %s != %s ", i, val, string(iter.Value()))
		require.NoError(t, iter.Next())
	}
	require.False(t, iter.Valid())
}
