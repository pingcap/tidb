// Copyright 2016 PingCAP, Inc.
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

package kv

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInterface(t *testing.T) {
	storage := newMockStorage()
	storage.GetClient()
	storage.UUID()
	version, err := storage.CurrentVersion(GlobalTxnScope)
	require.Nil(t, err)

	snapshot := storage.GetSnapshot(version)
	_, err = snapshot.BatchGet(context.Background(), []Key{Key("abc"), Key("def")})
	require.Nil(t, err)

	snapshot.SetOption(Priority, PriorityNormal)
	transaction, err := storage.Begin()
	require.Nil(t, err)
	require.NotNil(t, transaction)

	err = transaction.LockKeys(context.Background(), new(LockCtx), Key("lock"))
	require.Nil(t, err)

	transaction.SetOption(23, struct{}{})
	if mock, ok := transaction.(*mockTxn); ok {
		mock.GetOption(23)
	}
	transaction.StartTS()
	if transaction.IsReadOnly() {
		_, err = transaction.Get(context.TODO(), Key("lock"))
		require.Nil(t, err)
		err = transaction.Set(Key("lock"), []byte{})
		require.Nil(t, err)
		_, err = transaction.Iter(Key("lock"), nil)
		require.Nil(t, err)
		_, err = transaction.IterReverse(Key("lock"))
		require.Nil(t, err)
	}
	_ = transaction.Commit(context.Background())

	transaction, err = storage.Begin()
	require.Nil(t, err)

	// Test for mockTxn interface.
	require.Equal(t, "", transaction.String())
	require.True(t, transaction.Valid())
	require.Equal(t, 0, transaction.Len())
	require.Equal(t, 0, transaction.Size())
	require.Nil(t, transaction.GetMemBuffer())

	transaction.Reset()
	err = transaction.Rollback()
	require.Nil(t, err)
	require.False(t, transaction.Valid())
	require.False(t, transaction.IsPessimistic())
	require.Nil(t, transaction.Delete(nil))

	require.Nil(t, storage.GetOracle())
	require.Equal(t, "KVMockStorage", storage.Name())
	require.Equal(t, "KVMockStorage is a mock Store implementation, only for unittests in KV package", storage.Describe())
	require.False(t, storage.SupportDeleteRange())

	status, err := storage.ShowStatus(context.Background(), "")
	require.Nil(t, status)
	require.Nil(t, err)

	err = storage.Close()
	require.Nil(t, err)
}
