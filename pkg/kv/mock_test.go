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

	"github.com/stretchr/testify/assert"
)

func TestInterface(t *testing.T) {
	storage := newMockStorage()
	storage.GetClient()
	storage.UUID()
	version, err := storage.CurrentVersion(GlobalTxnScope)
	assert.Nil(t, err)

	snapshot := storage.GetSnapshot(version)
	_, err = snapshot.BatchGet(context.Background(), []Key{Key("abc"), Key("def")})
	assert.Nil(t, err)

	snapshot.SetOption(Priority, PriorityNormal)
	transaction, err := storage.Begin()
	assert.Nil(t, err)
	assert.NotNil(t, transaction)

	err = transaction.LockKeys(context.Background(), new(LockCtx), Key("lock"))
	assert.Nil(t, err)

	transaction.SetOption(23, struct{}{})
	if mock, ok := transaction.(*mockTxn); ok {
		mock.GetOption(23)
	}
	transaction.StartTS()
	if transaction.IsReadOnly() {
		_, err = transaction.Get(context.TODO(), Key("lock"))
		assert.Nil(t, err)
		err = transaction.Set(Key("lock"), []byte{})
		assert.Nil(t, err)
		_, err = transaction.Iter(Key("lock"), nil)
		assert.Nil(t, err)
		_, err = transaction.IterReverse(Key("lock"), nil)
		assert.Nil(t, err)
	}
	_ = transaction.Commit(context.Background())

	transaction, err = storage.Begin()
	assert.Nil(t, err)

	// Test for mockTxn interface.
	assert.Equal(t, "", transaction.String())
	assert.True(t, transaction.Valid())
	assert.Equal(t, 0, transaction.Len())
	assert.Equal(t, 0, transaction.Size())
	assert.Nil(t, transaction.GetMemBuffer())

	transaction.(*mockTxn).Reset()
	err = transaction.Rollback()
	assert.Nil(t, err)
	assert.False(t, transaction.Valid())
	assert.False(t, transaction.IsPessimistic())
	assert.Nil(t, transaction.Delete(nil))

	assert.Nil(t, storage.GetOracle())
	assert.Equal(t, "KVMockStorage", storage.Name())
	assert.Equal(t, "KVMockStorage is a mock Store implementation, only for unittests in KV package", storage.Describe())
	assert.False(t, storage.SupportDeleteRange())

	status, err := storage.ShowStatus(context.Background(), "")
	assert.Nil(t, status)
	assert.Nil(t, err)

	err = storage.Close()
	assert.Nil(t, err)
}
