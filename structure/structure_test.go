// Copyright 2015 PingCAP, Inc.
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

package structure_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/structure"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestString(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	txn, err := store.Begin()
	require.NoError(t, err)

	tx := structure.NewStructure(txn, txn, []byte{0x00})

	key := []byte("a")
	value := []byte("1")
	err = tx.Set(key, value)
	require.NoError(t, err)

	v, err := tx.Get(key)
	require.NoError(t, err)
	require.Equal(t, value, v)

	n, err := tx.Inc(key, 1)
	require.NoError(t, err)
	require.Equal(t, int64(2), n)

	v, err = tx.Get(key)
	require.NoError(t, err)
	require.Equal(t, []byte("2"), v)

	n, err = tx.GetInt64(key)
	require.NoError(t, err)
	require.Equal(t, int64(2), n)

	err = tx.Clear(key)
	require.NoError(t, err)

	v, err = tx.Get(key)
	require.NoError(t, err)
	require.Nil(t, v)

	tx1 := structure.NewStructure(txn, nil, []byte{0x01})
	err = tx1.Set(key, value)
	require.NotNil(t, err)

	_, err = tx1.Inc(key, 1)
	require.NotNil(t, err)

	err = tx1.Clear(key)
	require.NotNil(t, err)

	err = txn.Commit(context.Background())
	require.NoError(t, err)
}

func TestList(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	txn, err := store.Begin()
	require.NoError(t, err)

	tx := structure.NewStructure(txn, txn, []byte{0x00})

	key := []byte("a")
	err = tx.LPush(key, []byte("3"), []byte("2"), []byte("1"))
	require.NoError(t, err)

	// Test LGetAll.
	err = tx.LPush(key, []byte("11"))
	require.NoError(t, err)
	values, err := tx.LGetAll(key)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("3"), []byte("2"), []byte("1"), []byte("11")}, values)
	value, err := tx.LPop(key)
	require.NoError(t, err)
	require.Equal(t, []byte("11"), value)

	l, err := tx.LLen(key)
	require.NoError(t, err)
	require.Equal(t, int64(3), l)

	value, err = tx.LIndex(key, 1)
	require.NoError(t, err)
	require.Equal(t, []byte("2"), value)

	err = tx.LSet(key, 1, []byte("4"))
	require.NoError(t, err)

	value, err = tx.LIndex(key, 1)
	require.NoError(t, err)
	require.Equal(t, []byte("4"), value)

	err = tx.LSet(key, 1, []byte("2"))
	require.NoError(t, err)

	err = tx.LSet(key, 100, []byte("2"))
	require.NotNil(t, err)

	value, err = tx.LIndex(key, -1)
	require.NoError(t, err)
	require.Equal(t, []byte("3"), value)

	value, err = tx.LPop(key)
	require.NoError(t, err)
	require.Equal(t, []byte("1"), value)

	l, err = tx.LLen(key)
	require.NoError(t, err)
	require.Equal(t, int64(2), l)

	err = tx.RPush(key, []byte("4"))
	require.NoError(t, err)

	l, err = tx.LLen(key)
	require.NoError(t, err)
	require.Equal(t, int64(3), l)

	value, err = tx.LIndex(key, -1)
	require.NoError(t, err)
	require.Equal(t, []byte("4"), value)

	value, err = tx.RPop(key)
	require.NoError(t, err)
	require.Equal(t, []byte("4"), value)

	value, err = tx.RPop(key)
	require.NoError(t, err)
	require.Equal(t, []byte("3"), value)

	value, err = tx.RPop(key)
	require.NoError(t, err)
	require.Equal(t, []byte("2"), value)

	l, err = tx.LLen(key)
	require.NoError(t, err)
	require.Equal(t, int64(0), l)

	err = tx.LPush(key, []byte("1"))
	require.NoError(t, err)

	err = tx.LClear(key)
	require.NoError(t, err)

	l, err = tx.LLen(key)
	require.NoError(t, err)
	require.Equal(t, int64(0), l)

	tx1 := structure.NewStructure(txn, nil, []byte{0x01})
	err = tx1.LPush(key, []byte("1"))
	require.NotNil(t, err)
	require.NotNil(t, err)

	_, err = tx1.RPop(key)
	require.NotNil(t, err)

	err = tx1.LSet(key, 1, []byte("2"))
	require.NotNil(t, err)

	err = tx1.LClear(key)
	require.NotNil(t, err)

	err = txn.Commit(context.Background())
	require.NoError(t, err)
}

func TestHash(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	txn, err := store.Begin()
	require.NoError(t, err)

	tx := structure.NewStructure(txn, txn, []byte{0x00})

	key := []byte("a")

	tx.EncodeHashAutoIDKeyValue(key, key, 5)

	err = tx.HSet(key, []byte("1"), []byte("1"))
	require.NoError(t, err)

	err = tx.HSet(key, []byte("2"), []byte("2"))
	require.NoError(t, err)

	value, err := tx.HGet(key, []byte("1"))
	require.NoError(t, err)
	require.Equal(t, []byte("1"), value)

	value, err = tx.HGet(key, []byte("fake"))
	require.NoError(t, err)
	require.Nil(t, value)

	keys, err := tx.HKeys(key)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("1"), []byte("2")}, keys)

	res, err := tx.HGetAll(key)
	require.NoError(t, err)
	require.Equal(t, []structure.HashPair{
		{Field: []byte("1"), Value: []byte("1")},
		{Field: []byte("2"), Value: []byte("2")}}, res)

	res, err = tx.HGetLastN(key, 1)
	require.NoError(t, err)
	require.Equal(t, []structure.HashPair{
		{Field: []byte("2"), Value: []byte("2")}}, res)

	res, err = tx.HGetLastN(key, 2)
	require.NoError(t, err)
	require.Equal(t, []structure.HashPair{
		{Field: []byte("2"), Value: []byte("2")},
		{Field: []byte("1"), Value: []byte("1")}}, res)

	err = tx.HDel(key, []byte("1"))
	require.NoError(t, err)

	value, err = tx.HGet(key, []byte("1"))
	require.NoError(t, err)
	require.Nil(t, value)

	n, err := tx.HInc(key, []byte("1"), 1)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	// Test set new value which equals to old value.
	value, err = tx.HGet(key, []byte("1"))
	require.NoError(t, err)
	require.Equal(t, []byte("1"), value)

	err = tx.HSet(key, []byte("1"), []byte("1"))
	require.NoError(t, err)

	value, err = tx.HGet(key, []byte("1"))
	require.NoError(t, err)
	require.Equal(t, []byte("1"), value)

	n, err = tx.HInc(key, []byte("1"), 1)
	require.NoError(t, err)
	require.Equal(t, int64(2), n)

	n, err = tx.HInc(key, []byte("1"), 1)
	require.NoError(t, err)
	require.Equal(t, int64(3), n)

	n, err = tx.HGetInt64(key, []byte("1"))
	require.NoError(t, err)
	require.Equal(t, int64(3), n)

	err = tx.HClear(key)
	require.NoError(t, err)

	err = tx.HDel(key, []byte("fake_key"))
	require.NoError(t, err)

	// Test set nil value.
	value, err = tx.HGet(key, []byte("nil_key"))
	require.NoError(t, err)
	require.Nil(t, value)

	err = tx.HSet(key, []byte("nil_key"), nil)
	require.NoError(t, err)

	err = tx.HSet(key, []byte("nil_key"), []byte("1"))
	require.NoError(t, err)

	value, err = tx.HGet(key, []byte("nil_key"))
	require.NoError(t, err)
	require.Equal(t, []byte("1"), value)

	err = tx.HSet(key, []byte("nil_key"), nil)
	require.NotNil(t, err)

	value, err = tx.HGet(key, []byte("nil_key"))
	require.NoError(t, err)
	require.Equal(t, []byte("1"), value)

	err = tx.HSet(key, []byte("nil_key"), []byte("2"))
	require.NoError(t, err)

	value, err = tx.HGet(key, []byte("nil_key"))
	require.NoError(t, err)
	require.Equal(t, []byte("2"), value)

	tx1 := structure.NewStructure(txn, nil, []byte{0x01})
	_, err = tx1.HInc(key, []byte("1"), 1)
	require.NotNil(t, err)

	err = tx1.HDel(key, []byte("1"))
	require.NotNil(t, err)

	err = txn.Commit(context.Background())
	require.NoError(t, err)

	err = kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		newTxn := structure.NewStructure(txn, txn, []byte{0x00})
		err = newTxn.Set(key, []byte("abc"))
		require.NoError(t, err)

		value, err = newTxn.Get(key)
		require.NoError(t, err)
		require.Equal(t, []byte("abc"), value)
		return nil
	})
	require.NoError(t, err)
}

func TestError(t *testing.T) {
	kvErrs := []*terror.Error{
		structure.ErrInvalidHashKeyFlag,
		structure.ErrInvalidListIndex,
		structure.ErrInvalidListMetaData,
		structure.ErrWriteOnSnapshot,
	}
	for _, err := range kvErrs {
		code := terror.ToSQLError(err).Code
		require.NotEqual(t, mysql.ErrUnknown, code)
		require.Equal(t, uint16(err.Code()), code, "err: %v", err)
	}
}
