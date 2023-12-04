// Copyright 2023 PingCAP, Inc.
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

package meta_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestDistLockAcquisitionAndRelease(t *testing.T) {
	store := testkit.CreateMockStore(t)
	ctx := context.Background()
	lock := meta.NewDistributedLockBuilder().
		SetBackoff(1*time.Microsecond).
		SetLease(0).
		Build(ctx, &mockDataStore{store}, "owner1", "testLock")

	// Test basic lock and unlock functions.
	require.NoError(t, lock.Lock())
	require.NoError(t, lock.Unlock())
	require.NoError(t, lock.Lock())
	require.NoError(t, lock.Unlock())

	// Test no data race like plain lock.
	total := 0
	wg := sync.WaitGroup{}
	conc, cnt := 3, 10000
	wg.Add(conc)
	for i := 0; i < conc; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < cnt; j++ {
				require.NoError(t, lock.Lock())
				total++
				require.NoError(t, lock.Unlock())
			}
		}()
	}
	wg.Wait()
	require.Equal(t, conc*cnt, total)
}

func TestDistLockKeyAndOwner(t *testing.T) {
	store := testkit.CreateMockStore(t)
	ctx := context.Background()
	mockStore := &mockDataStore{store}
	backoff := 1 * time.Microsecond

	checkNotRelate := func(lockA, lockB *meta.DistributedLock) {
		require.NoError(t, lockA.Lock())
		require.NoError(t, lockB.Lock())
		require.NoError(t, lockA.Unlock())
		require.NoError(t, lockB.Unlock())
	}

	// Test different keys.
	lock00 := meta.NewDistributedLockBuilder().SetBackoff(backoff).SetLease(0).Build(ctx, mockStore, "owner-0", "000")
	lock01 := meta.NewDistributedLockBuilder().SetBackoff(backoff).SetLease(0).Build(ctx, mockStore, "owner-0", "111")
	checkNotRelate(lock00, lock01)

	checkMutualExclusive := func(lockA, lockB *meta.DistributedLock, cancelLockB context.CancelFunc) {
		require.NoError(t, lockA.Lock())
		wg := sync.WaitGroup{}
		wg.Add(1)
		var checkErr error
		go func() {
			defer wg.Done()
			// Should be blocked until the context is canceled.
			checkErr = lockB.Lock()
		}()
		time.Sleep(300 * time.Millisecond)
		cancelLockB()
		wg.Wait()
		require.ErrorIs(t, checkErr, context.Canceled)
		require.NoError(t, lockA.Unlock())
	}

	// Test same keys.
	ctx10, cancel10 := context.WithCancel(ctx)
	lock10 := meta.NewDistributedLockBuilder().SetBackoff(backoff).SetLease(0).Build(ctx10, mockStore, "owner-1", "000")
	checkMutualExclusive(lock00, lock10, cancel10)

	// Test same keys with same owner.
	ctx00Fake, cancel00Fake := context.WithCancel(ctx)
	lock00Fake := meta.NewDistributedLockBuilder().SetBackoff(backoff).SetLease(0).Build(ctx00Fake, mockStore, "owner-0", "000")
	require.NoError(t, lock00.Lock())
	require.NoError(t, lock00Fake.Unlock())
	require.NoError(t, lock00Fake.Lock())
	require.NoError(t, lock00.Unlock())
	checkMutualExclusive(lock00, lock00Fake, cancel00Fake)
}

func TestDistributedLockExpire(t *testing.T) {
	store := testkit.CreateMockStore(t)
	ctx := context.Background()
	lock := meta.NewDistributedLockBuilder().
		SetBackoff(1*time.Microsecond).
		SetLease(500*time.Millisecond).
		Build(ctx, &mockDataStore{store}, "owner1", "testLock")

	require.NoError(t, lock.Lock())
	require.Eventually(t, func())
}

type mockDataStore struct {
	store kv.Storage
}

func (m *mockDataStore) Begin(_ context.Context) (meta.DataTxn, error) {
	txn, err := m.store.Begin()
	if err != nil {
		return nil, err
	}
	return &mockDataTxn{txn: txn}, nil
}

type mockDataTxn struct {
	txn kv.Transaction
}

func (m *mockDataTxn) Rollback() {
	_ = m.txn.Rollback()
}

func (m *mockDataTxn) Commit() error {
	return m.txn.Commit(context.Background())
}

func (m *mockDataTxn) StartTS() uint64 {
	return m.txn.StartTS()
}

func (m *mockDataTxn) Set(key, value []byte) error {
	return m.txn.Set(key, value)
}

func (m *mockDataTxn) Get(key []byte) ([]byte, error) {
	val, err := m.txn.Get(context.Background(), key)
	if kv.ErrNotExist.Equal(err) {
		err = nil
	}
	return val, err
}

func (m *mockDataTxn) Del(key []byte) error {
	return m.txn.Delete(key)
}
