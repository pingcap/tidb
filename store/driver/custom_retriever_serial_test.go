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
	txn2 "github.com/pingcap/tidb/store/driver/txn"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/txnkv/transaction"
)

type mockErrRetriever struct {
	err error
}

// Get gets the value for key k from kv store. Always return nil for this retriever
func (r *mockErrRetriever) Get(_ context.Context, _ kv.Key) ([]byte, error) {
	return nil, r.err
}

// Iter creates an Iterator. Always return EmptyIterator for this retriever
func (r *mockErrRetriever) Iter(_ kv.Key, _ kv.Key) (kv.Iterator, error) { return nil, r.err }

// IterReverse creates a reversed Iterator. Always return EmptyIterator for this retriever
func (r *mockErrRetriever) IterReverse(_ kv.Key) (kv.Iterator, error) {
	return nil, r.err
}

func TestSnapshotGetWithCustomRetrievers(t *testing.T) {
	store, clean := createEmptyTestStore(t)
	defer clean()
	clearData(t, store)

	snap := prepareSnapshot(t, store, [][]interface{}{
		{"a0", "s0"},
		{"a01", "s01"},
		{"a1", "s1"},
		{"a12", "s12"},
		{"a2", "s2"},
	})

	errRetriever := &mockErrRetriever{
		err: errors.New("error"),
	}

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
		txn2.NewRangeRetriever(errRetriever, kv.Key("a6"), kv.Key("a7")),
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
		{"a6", errRetriever.err},
		{"a7", kv.ErrNotExist},
	}

	ctx := context.Background()
	for _, ca := range cases {
		val, err := snap.Get(ctx, makeBytes(ca[0]))
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
	store, clean := createEmptyTestStore(t)
	defer clean()
	clearData(t, store)

	snap := prepareSnapshot(t, store, [][]interface{}{
		{"a0", "s0"},
		{"a01", "s01"},
		{"a1", "s1"},
		{"a12", "s12"},
		{"a2", "s2"},
	})

	errRetriever := &mockErrRetriever{
		err: errors.New("error"),
	}

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
		txn2.NewRangeRetriever(errRetriever, kv.Key("a6"), kv.Key("a7")),
	})

	cases := []struct {
		keys   []interface{}
		result map[string]string
		err    error
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
		{
			keys: []interface{}{"a6"},
			err:  errRetriever.err,
		},
		{
			keys: []interface{}{"a0", "a6"},
			err:  errRetriever.err,
		},
	}

	ctx := context.Background()
	for _, ca := range cases {
		keys := make([]kv.Key, 0)
		for _, k := range ca.keys {
			keys = append(keys, makeBytes(k))
		}

		m, err := snap.BatchGet(ctx, keys)
		if ca.err == nil {
			require.NoError(t, err)
		} else {
			require.True(t, errors.ErrorEqual(err, ca.err))
		}

		if ca.result == nil {
			require.Nil(t, m)
		} else {
			require.NotNil(t, m)
			require.Equal(t, len(ca.result), len(m))
			for k, expectedVal := range ca.result {
				val, ok := m[k]
				require.True(t, ok)
				require.Equal(t, makeBytes(expectedVal), val)
			}
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
		txn2.NewRangeRetriever(&kv.EmptyRetriever{}, makeBytes("a5"), makeBytes("a6")),
	})

	cases := []struct {
		query   []interface{}
		reverse bool
		result  [][]interface{}
		err     error
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
			iter, err = snap.IterReverse(makeBytes(ca.query[0]))
			require.NoError(t, err)
		} else {
			iter, err = snap.Iter(makeBytes(ca.query[0]), makeBytes(ca.query[1]))
			require.NoError(t, err)
		}

		for i := range ca.result {
			require.True(t, iter.Valid())
			gotKey := iter.Key()
			gotValue := iter.Value()
			expectedKey := makeBytes(ca.result[i][0])
			expectedValue := makeBytes(ca.result[i][1])
			require.Equal(t, expectedKey, []byte(gotKey))
			require.Equal(t, expectedValue, gotValue)
			err = iter.Next()
			require.NoError(t, err)
		}

		require.False(t, iter.Valid())
	}

	// test return error for Iter/IterReverse
	errRetriever := &mockErrRetriever{err: errors.New("error")}
	snap.SetOption(kv.SortedCustomRetrievers, []*txn2.RangedKVRetriever{txn2.NewRangeRetriever(errRetriever, nil, kv.Key("k1"))})
	iter, err := snap.Iter(nil, nil)
	require.Nil(t, iter)
	require.Equal(t, errRetriever.err, err)
	snap.SetOption(kv.SortedCustomRetrievers, []*txn2.RangedKVRetriever{txn2.NewRangeRetriever(errRetriever, kv.Key("k1"), nil)})
	iter, err = snap.IterReverse(nil)
	require.Nil(t, iter)
	require.Equal(t, errRetriever.err, err)
}

func clearData(t *testing.T, store kv.Storage) {
	txn, err := store.Begin()
	require.NoError(t, err)
	defer func() {
		if txn.Valid() {
			require.NoError(t, txn.Rollback())
		}
	}()

	iter, err := txn.Iter(nil, nil)
	require.NoError(t, err)
	defer iter.Close()

	for iter.Valid() {
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
		err = txn.Set(makeBytes(d[0]), makeBytes(d[1]))
		require.NoError(t, err)
	}

	err = txn.Commit(context.Background())
	require.NoError(t, err)

	return store.GetSnapshot(kv.MaxVersion)
}

func newMemBufferRetriever(t *testing.T, start interface{}, end interface{}, data [][]interface{}) *txn2.RangedKVRetriever {
	tmpTxn, err := transaction.NewTiKVTxn(nil, nil, 0, "")
	require.NoError(t, err)
	memBuffer := txn2.NewTiKVTxn(tmpTxn).GetMemBuffer()
	for _, d := range data {
		k := makeBytes(d[0])
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

	return txn2.NewRangeRetriever(memBuffer, makeBytes(start), makeBytes(end))
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
