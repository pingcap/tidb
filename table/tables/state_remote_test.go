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
	"time"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

// CreateMetaLockForCachedTable initializes the cached table meta lock information.
func createMetaLockForCachedTable(h session.Session) error {
	createTable := "CREATE TABLE IF NOT EXISTS `mysql`.`table_cache_meta` (" +
		"`tid` int(11) NOT NULL DEFAULT 0," +
		"`lock_type` enum('NONE','READ', 'INTEND', 'WRITE') NOT NULL DEFAULT 'NONE'," +
		"`lease` bigint(20) NOT NULL DEFAULT 0," +
		"`oldReadLease` bigint(20) NOT NULL DEFAULT 0," +
		"PRIMARY KEY (`tid`))"
	_, err := h.ExecuteInternal(context.Background(), createTable)
	return err
}

// InitRow add a new record into the cached table meta lock table.
func initRow(ctx context.Context, exec session.Session, tid int) error {
	_, err := exec.ExecuteInternal(ctx, "insert ignore into mysql.table_cache_meta values (%?, 'NONE', 0, 0)", tid)
	return err
}

func TestStateRemote(t *testing.T) {
	t.Parallel()
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	se := tk.Session()

	ctx := context.Background()
	h := tables.NewStateRemote(se)
	err := createMetaLockForCachedTable(se)
	require.NoError(t, err)
	require.Equal(t, tables.CachedTableLockNone, tables.CachedTableLockType(0))

	// Check the initial value.
	require.NoError(t, initRow(ctx, se, 5))
	lockType, lease, err := h.Load(ctx, 5)
	require.NoError(t, err)
	require.Equal(t, lockType, tables.CachedTableLockNone)
	require.Equal(t, lockType.String(), "NONE")
	require.Equal(t, lease, uint64(0))

	ts, err := se.GetStore().GetOracle().GetTimestamp(ctx, &oracle.Option{TxnScope: kv.GlobalTxnScope})
	require.NoError(t, err)
	physicalTime := oracle.GetTimeFromTS(ts)
	leaseVal := oracle.GoTimeToTS(physicalTime.Add(200 * time.Millisecond))

	// Check read lock.
	succ, err := h.LockForRead(ctx, 5, leaseVal)
	require.NoError(t, err)
	require.True(t, succ)
	lockType, lease, err = h.Load(ctx, 5)
	require.NoError(t, err)
	require.Equal(t, lockType, tables.CachedTableLockRead)
	require.Equal(t, lockType.String(), "READ")
	require.Equal(t, lease, leaseVal)

	// LockForRead when read lock is hold.
	// This operation equals to renew lease.
	succ, err = h.LockForRead(ctx, 5, leaseVal+1)
	require.NoError(t, err)
	require.True(t, succ)
	lockType, lease, err = h.Load(ctx, 5)
	require.NoError(t, err)
	require.Equal(t, lockType, tables.CachedTableLockRead)
	require.Equal(t, lockType.String(), "READ")
	require.Equal(t, lease, leaseVal+1)

	// Renew read lock lease operation.
	leaseVal = oracle.GoTimeToTS(physicalTime.Add(400 * time.Millisecond))
	succ, err = h.RenewLease(ctx, 5, leaseVal, tables.RenewReadLease)
	require.NoError(t, err)
	require.True(t, succ)
	lockType, lease, err = h.Load(ctx, 5)
	require.NoError(t, err)
	require.Equal(t, lockType, tables.CachedTableLockRead)
	require.Equal(t, lockType.String(), "READ")
	require.Equal(t, lease, leaseVal)

	// Check write lock.
	leaseVal = oracle.GoTimeToTS(physicalTime.Add(700 * time.Millisecond))
	require.NoError(t, h.LockForWrite(ctx, 5, leaseVal))
	lockType, lease, err = h.Load(ctx, 5)
	require.NoError(t, err)
	require.Equal(t, lockType, tables.CachedTableLockWrite)
	require.Equal(t, lockType.String(), "WRITE")
	require.Equal(t, lease, leaseVal)

	// Lock for write again
	leaseVal = oracle.GoTimeToTS(physicalTime.Add(800 * time.Millisecond))
	require.NoError(t, h.LockForWrite(ctx, 5, leaseVal))
	lockType, _, err = h.Load(ctx, 5)
	require.NoError(t, err)
	require.Equal(t, lockType, tables.CachedTableLockWrite)
	require.Equal(t, lockType.String(), "WRITE")

	// Renew read lock lease should fail when the write lock is hold.
	succ, err = h.RenewLease(ctx, 5, leaseVal, tables.RenewReadLease)
	require.NoError(t, err)
	require.False(t, succ)

	// Acquire read lock should also fail when the write lock is hold.
	succ, err = h.LockForRead(ctx, 5, leaseVal)
	require.NoError(t, err)
	require.False(t, succ)

	// But clear orphan write lock should success.
	time.Sleep(time.Second)
	leaseVal = oracle.GoTimeToTS(physicalTime.Add(2 * time.Second))
	succ, err = h.LockForRead(ctx, 5, leaseVal)
	require.NoError(t, err)
	require.True(t, succ)
}
