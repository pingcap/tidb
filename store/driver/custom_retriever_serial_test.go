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
	"bytes"
	"context"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	txn2 "github.com/pingcap/tidb/store/driver/txn"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/txnkv/transaction"
)

var prefix = kv.Key("t_c_retriever_")

func TestSnapshotGetWithCustomRetrievers(t *testing.T) {
	store, _, clean := createTestStore(t)
	defer clean()
	clearData(t, store)

	snap := prepareSnapshot(t, store, [][]interface{}{
		{"a0", "s0"},
		{"a01", "s01"},
		{"a1", "s1"},
		{"a12", "s12"},
		{"a2", "s2"},
	})

	snap.SetOption(kv.SortedCustomRetrievers, []*txn2.RangedKVRetriever{
		newMemBufferRetriever(t, "a1", "a2", [][]interface{}{
			{"a0", "v0"},
			{"a02", "v02"},
			{"a1", "v1"},
			{"a11", "v11"},
			{"a1x", ""},
		}),
		newMemBufferRetriever(t, "a3", "a4", [][]interface{}{
			{"a1", "vx"},
		}),
	})

	cases := [][]interface{}{
		{"a0", "s0"},
		{"a01", "s01"},
		{"a02", kv.ErrNotExist},
		{"a03", kv.ErrNotExist},
		{"a1", "v1"},
		{"a11", "v11"},
		{"a12", kv.ErrNotExist},
		{"a13", kv.ErrNotExist},
		{"a1x", kv.ErrNotExist},
	}

	ctx := context.Background()
	for _, ca := range cases {
		val, err := snap.Get(ctx, genKey(ca[0]))
		if expectedErr, ok := ca[1].(error); ok {
			require.True(t, errors.ErrorEqual(expectedErr, err))
			require.Nil(t, val)
		} else {
			require.NoError(t, err)
			require.Equal(t, val, makeBytes(ca[1]))
		}
	}
}

func TestSnapshotBatchGetWithCustomRetrievers(t *testing.T) {
	store, _, clean := createTestStore(t)
	defer clean()
	clearData(t, store)

	snap := prepareSnapshot(t, store, [][]interface{}{
		{"a0", "s0"},
		{"a01", "s01"},
		{"a1", "s1"},
		{"a12", "s12"},
		{"a2", "s2"},
	})

	snap.SetOption(kv.SortedCustomRetrievers, []*txn2.RangedKVRetriever{
		newMemBufferRetriever(t, "a1", "a2", [][]interface{}{
			{"a0", "v0"},
			{"a02", "v02"},
			{"a1", "v1"},
			{"a11", "v11"},
			{"a1x", ""},
		}),
		newMemBufferRetriever(t, "a3", "a4", [][]interface{}{
			{"a1", "vx"},
		}),
	})

	cases := []struct {
		keys   []interface{}
		result map[string]string
	}{
		{
			keys:   []interface{}{},
			result: map[string]string{},
		},
		{
			keys:   []interface{}{"a0"},
			result: map[string]string{"a0": "s0"},
		},
		{
			keys:   []interface{}{"a02"},
			result: map[string]string{},
		},
		{
			keys:   []interface{}{"a1"},
			result: map[string]string{"a1": "v1"},
		},
		{
			keys:   []interface{}{"a0", "a01", "a02", "a03", "a1", "a11", "a12", "a13", "a1x", "a3", "a5"},
			result: map[string]string{"a0": "s0", "a01": "s01", "a1": "v1", "a11": "v11"},
		},
	}

	ctx := context.Background()
	for _, ca := range cases {
		keys := make([]kv.Key, 0)
		for _, k := range ca.keys {
			keys = append(keys, genKey(k))
		}

		m, err := snap.BatchGet(ctx, keys)
		require.NoError(t, err)
		require.NotNil(t, m)
		require.Equal(t, len(ca.result), len(m))

		for k, expectedVal := range ca.result {
			val, ok := m[string(genKey(k))]
			require.True(t, ok)
			require.Equal(t, makeBytes(expectedVal), val)
		}
	}
}

func TestSnapshotIterWithCustomRetrievers(t *testing.T) {
	store, _, clean := createTestStore(t)
	defer clean()
	clearData(t, store)

	snap := prepareSnapshot(t, store, [][]interface{}{
		{"a0", "s0"},
		{"a01", "s01"},
		{"a1", "s1"},
		{"a12", "s12"},
		{"a2", "s2"},
		{"a4", "s4"},
		{"a7", "s7"},
		{"a9", "s9"},
	})

	snap.SetOption(kv.SortedCustomRetrievers, []*txn2.RangedKVRetriever{
		newMemBufferRetriever(t, "a1", "a2", [][]interface{}{
			{"a0", "v0"},
			{"a02", "v02"},
			{"a1", "v1"},
			{"a11", "v11"},
			{"a1x", ""},
		}),
		newMemBufferRetriever(t, "a3", "a4", [][]interface{}{
			{"a1", "vx"},
			{"a31", "v31"},
		}),
		txn2.NewRangeRetriever(&kv.EmptyRetriever{}, genKey("a5"), genKey("a6")),
	})

	cases := []struct {
		query   []interface{}
		reverse bool
		result  [][]interface{}
	}{
		{
			query: []interface{}{nil, "a1"},
			result: [][]interface{}{
				{"a0", "s0"},
				{"a01", "s01"},
			},
		},
		{
			query: []interface{}{"a1", "a2"},
			result: [][]interface{}{
				{"a1", "v1"},
				{"a11", "v11"},
			},
		},
		{
			query: []interface{}{"a01", "a9"},
			result: [][]interface{}{
				{"a01", "s01"},
				{"a1", "v1"},
				{"a11", "v11"},
				{"a2", "s2"},
				{"a31", "v31"},
				{"a4", "s4"},
				{"a7", "s7"},
			},
		},
		{
			query:   []interface{}{"a9"},
			reverse: true,
			result: [][]interface{}{
				{"a7", "s7"},
				{"a4", "s4"},
				{"a31", "v31"},
				{"a2", "s2"},
				{"a11", "v11"},
				{"a1", "v1"},
				{"a01", "s01"},
				{"a0", "s0"},
			},
		},
	}

	for _, ca := range cases {
		var iter kv.Iterator
		var err error
		if ca.reverse {
			iter, err = snap.IterReverse(genKey(ca.query[0]))
			require.NoError(t, err)
		} else {
			iter, err = snap.Iter(genKey(ca.query[0]), genKey(ca.query[1]))
			require.NoError(t, err)
		}

		for i := range ca.result {
			require.True(t, iter.Valid())
			gotKey := iter.Key()
			gotValue := iter.Value()
			expectedKey := genKey(ca.result[i][0])
			expectedValue := makeBytes(ca.result[i][1])
			require.Equal(t, []byte(expectedKey), []byte(gotKey))
			require.Equal(t, expectedValue, gotValue)
			err = iter.Next()
			require.NoError(t, err)
		}

		if ca.reverse && iter.Valid() {
			k := iter.Key()
			require.False(t, bytes.HasPrefix(k, prefix))
		} else {
			require.False(t, iter.Valid())
		}
	}
}

func clearData(t *testing.T, store kv.Storage) {
	txn, err := store.Begin()
	require.NoError(t, err)
	defer func() {
		if txn.Valid() {
			require.NoError(t, txn.Rollback())
		}
	}()

	iter, err := txn.Iter(prefix, nil)
	require.NoError(t, err)
	defer iter.Close()

	for iter.Valid() && bytes.HasPrefix(iter.Key(), prefix) {
		require.NoError(t, txn.Delete(iter.Key()))
		require.NoError(t, iter.Next())
	}

	require.NoError(t, txn.Commit(context.Background()))
}

func prepareSnapshot(t *testing.T, store kv.Storage, data [][]interface{}) kv.Snapshot {
	txn, err := store.Begin()
	require.NoError(t, err)
	defer func() {
		if txn.Valid() {
			require.NoError(t, txn.Rollback())
		}
	}()

	for _, d := range data {
		err = txn.Set(genKey(d[0]), makeBytes(d[1]))
		require.NoError(t, err)
	}

	err = txn.Commit(context.Background())
	require.NoError(t, err)

	return store.GetSnapshot(kv.MaxVersion)
}

func genKey(k interface{}) kv.Key {
	key := makeBytes(k)
	if len(key) == 0 {
		return prefix
	}

	ret := make([]byte, 0)
	ret = append(ret, prefix...)
	return append(ret, key...)
}

func newMemBufferRetriever(t *testing.T, start interface{}, end interface{}, data [][]interface{}) *txn2.RangedKVRetriever {
	tmpTxn, err := transaction.NewTiKVTxn(nil, nil, 0, "")
	require.NoError(t, err)
	memBuffer := txn2.NewTiKVTxn(tmpTxn).GetMemBuffer()
	for _, d := range data {
		k := genKey(d[0])
		val := makeBytes(d[1])
		if len(val) == 0 {
			// to test delete case
			err := memBuffer.Set(k, []byte("12345"))
			require.NoError(t, err)
			err = memBuffer.Delete(k)
			require.NoError(t, err)
		} else {
			err := memBuffer.Set(k, makeBytes(d[1]))
			require.NoError(t, err)
		}
	}

	return txn2.NewRangeRetriever(memBuffer, genKey(start), genKey(end))
}

func makeBytes(s interface{}) []byte {
	if s == nil {
		return nil
	}

	switch key := s.(type) {
	case string:
		return []byte(key)
	default:
		return key.([]byte)
	}
}
