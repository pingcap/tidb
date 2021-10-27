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
	"context"
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
	se := tk.Session()

	ctx := context.Background()
	h := tables.NewStateRemote(se)
	err := tables.CreateMetaLockForCachedTable(se)
	require.NoError(t, err)
	require.Equal(t, tables.CachedTableLockNone, tables.CachedTableLockType(0))

	// Check the initial value.
	require.NoError(t, tables.InitRow(ctx, se, 5))
	lockType, lease, err := h.Load(ctx, 5)
	require.NoError(t, err)
	require.Equal(t, lockType, tables.CachedTableLockNone)
	require.Equal(t, lockType.String(), "NONE")
	require.Equal(t, lease, uint64(0))

	// Check read lock.
	succ, err := h.LockForRead(ctx, 5, 1234, 1234)
	require.NoError(t, err)
	require.True(t, succ)
	lockType, lease, err = h.Load(ctx, 5)
	require.NoError(t, err)
	require.Equal(t, lockType, tables.CachedTableLockRead)
	require.Equal(t, lockType.String(), "READ")
	require.Equal(t, lease, uint64(1234))

	// LockForRead when read lock is hold.
	// This operation equals to renew lease.
	succ, err = h.LockForRead(ctx, 5, 1235, 1235)
	require.NoError(t, err)
	require.True(t, succ)
	lockType, lease, err = h.Load(ctx, 5)
	require.NoError(t, err)
	require.Equal(t, lockType, tables.CachedTableLockRead)
	require.Equal(t, lockType.String(), "READ")
	require.Equal(t, lease, uint64(1235))

	// Renew read lock lease operation.
	succ, err = h.RenewLease(ctx, 5, 1264)
	require.NoError(t, err)
	require.True(t, succ)
	lockType, lease, err = h.Load(ctx, 5)
	require.NoError(t, err)
	require.Equal(t, lockType, tables.CachedTableLockRead)
	require.Equal(t, lockType.String(), "READ")
	require.Equal(t, lease, uint64(1264))

	// Check write lock.
	require.NoError(t, h.LockForWrite(ctx, 5, 2234, 2234))
	lockType, lease, err = h.Load(ctx, 5)
	require.NoError(t, err)
	require.Equal(t, lockType, tables.CachedTableLockWrite)
	require.Equal(t, lockType.String(), "WRITE")
	require.Equal(t, lease, uint64(2234))

	// Lock for write again
	require.NoError(t, h.LockForWrite(ctx, 5, 3234, 3234))
	lockType, lease, err = h.Load(ctx, 5)
	require.NoError(t, err)
	require.Equal(t, lockType, tables.CachedTableLockWrite)
	require.Equal(t, lockType.String(), "WRITE")
	require.Equal(t, lease, uint64(3234))

	// Renew read lock lease should fail when the write lock is hold.
	succ, err = h.RenewLease(ctx, 5, 1264)
	require.NoError(t, err)
	require.False(t, succ)

	// Acquire read lock should also fail when the write lock is hold.
	succ, err = h.LockForRead(ctx, 5, 1264, 1264)
	require.NoError(t, err)
	require.False(t, succ)

	// But clear orphan write lock should success.
	succ, err = h.LockForRead(ctx, 5, 4234, 4234)
	require.NoError(t, err)
	require.True(t, succ)
}
