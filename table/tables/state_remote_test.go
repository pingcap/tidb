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

package tables_test

import (
	"testing"

	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestStateRemote(t *testing.T) {
	t.Parallel()
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	h := tables.NewStateRemoteHandle(5)
	state, err := h.Load(5)
	require.NoError(t, err)
	require.Equal(t, tables.CachedTableLockNone, state.LockType())

	// Check read lock.
	err = h.LockForRead(5, 8, 8)
	require.NoError(t, err)
	stateLocal, err := h.Load(5)
	require.NoError(t, err)
	require.Equal(t, stateLocal.LockType(), tables.CachedTableLockRead)
	require.Equal(t, stateLocal.LockType().String(), "READ")
	require.Equal(t, stateLocal.Lease(), uint64(8))
	// LockForRead when read lock is hold.
	// This operation equals to renew lease.
	err = h.LockForRead(5,  9, 9)
	require.NoError(t, err)
	stateLocal, err = h.Load(5)
	require.NoError(t, err)
	require.Equal(t, stateLocal.LockType(), tables.CachedTableLockRead)
	require.Equal(t, stateLocal.LockType().String(), "READ")
	require.Equal(t, stateLocal.Lease(), uint64(9))

	// Renew read lock lease operation.
	err = h.RenewLease(5, 12)
	require.NoError(t, err)
	stateLocal, err = h.Load(5)
	require.NoError(t, err)
	require.Equal(t, stateLocal.LockType(), tables.CachedTableLockRead)
	require.Equal(t, stateLocal.LockType().String(), "READ")
	require.Equal(t, stateLocal.Lease(), uint64(12))

	// Check write lock.
	_, err = h.PreLock(5, 14, 14)
	require.NoError(t, err)
	require.NoError(t, h.LockForWrite(5, 15))
	stateLocal, err = h.Load( 5)
	require.NoError(t, err)
	require.Equal(t,  stateLocal.LockType(), tables.CachedTableLockWrite)
	require.Equal(t,  stateLocal.LockType().String(), "WRITE")
	require.Equal(t,  stateLocal.Lease(), uint64(15))

	// Renew read lock lease should fail when the write lock is hold.
	err = h.RenewLease(5, 19)
	require.Error(t, err)

	// Acquire read lock should also fail when the write lock is hold.
	err = h.LockForRead(5,12, 12)
	require.Error(t, err)

	// But clear orphan write lock should success.
	err = h.LockForRead(5, 19, 19)
	require.NoError(t, err)
}
