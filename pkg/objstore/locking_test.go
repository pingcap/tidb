// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objstore_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	backup "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
)

func createMockStorage(t *testing.T) (storeapi.Storage, string) {
	tempdir := t.TempDir()
	storage, err := objstore.New(context.Background(), &backup.StorageBackend{
		Backend: &backup.StorageBackend_Local{
			Local: &backup.Local{
				Path: tempdir,
			},
		},
	}, nil)
	require.NoError(t, err)
	return storage, tempdir
}

func requireFileExists(t *testing.T, path string) {
	_, err := os.Stat(path)
	require.NoError(t, err)
}

func requireFileNotExists(t *testing.T, path string) {
	_, err := os.Stat(path)
	require.True(t, os.IsNotExist(err))
}

func TestTryLockRemote(t *testing.T) {
	ctx := context.Background()
	strg, pth := createMockStorage(t)
	lock, err := objstore.TryLockRemote(ctx, strg, "test.lock", "This file is mine!")
	require.NoError(t, err)
	requireFileExists(t, filepath.Join(pth, "test.lock"))
	err = lock.Unlock(ctx)
	require.NoError(t, err)
	requireFileNotExists(t, filepath.Join(pth, "test.lock"))
}

func TestConflictLock(t *testing.T) {
	ctx := context.Background()
	strg, pth := createMockStorage(t)
	lock, err := objstore.TryLockRemote(ctx, strg, "test.lock", "This file is mine!")
	require.NoError(t, err)
	_, err = objstore.TryLockRemote(ctx, strg, "test.lock", "This file is mine!")
	require.ErrorContains(t, err, "conflict file test.lock")
	requireFileExists(t, filepath.Join(pth, "test.lock"))
	err = lock.Unlock(ctx)
	require.NoError(t, err)
	requireFileNotExists(t, filepath.Join(pth, "test.lock"))
}

func TestRWLock(t *testing.T) {
	ctx := context.Background()
	strg, path := createMockStorage(t)
	lock, err := objstore.TryLockRemoteRead(ctx, strg, "test.lock", "I wanna read it!")
	require.NoError(t, err)
	lock2, err := objstore.TryLockRemoteRead(ctx, strg, "test.lock", "I wanna read it too!")
	require.NoError(t, err)
	_, err = objstore.TryLockRemoteWrite(ctx, strg, "test.lock", "I wanna write it, you get out!")
	require.Error(t, err)
	require.NoError(t, lock.Unlock(ctx))
	require.NoError(t, lock2.Unlock(ctx))
	l, err := objstore.TryLockRemoteWrite(ctx, strg, "test.lock", "Can I have a write lock?")
	require.NoError(t, err)
	requireFileExists(t, filepath.Join(path, "test.lock.WRIT"))
	require.NoError(t, l.Unlock(ctx))
	requireFileNotExists(t, filepath.Join(path, "test.lock.WRIT"))
}

func TestConcurrentLock(t *testing.T) {
	ctx := context.Background()
	strg, path := createMockStorage(t)

	errChA := make(chan error, 1)
	errChB := make(chan error, 1)

	waitRecvTwice := func(ch chan<- struct{}) func() {
		return func() {
			ch <- struct{}{}
			ch <- struct{}{}
		}
	}

	asyncOnceFunc := func(f func()) func() {
		run := new(atomic.Bool)
		return func() {
			if run.CompareAndSwap(false, true) {
				f()
			}
		}
	}
	chA := make(chan struct{})
	onceA := asyncOnceFunc(waitRecvTwice(chA))
	chB := make(chan struct{})
	onceB := asyncOnceFunc(waitRecvTwice(chB))

	require.NoError(t, failpoint.EnableCall("github.com/pingcap/tidb/pkg/objstore/exclusive-write-commit-to-1", onceA))
	require.NoError(t, failpoint.EnableCall("github.com/pingcap/tidb/pkg/objstore/exclusive-write-commit-to-2", onceB))

	go func() {
		_, err := objstore.TryLockRemote(ctx, strg, "test.lock", "I wanna read it, but I hesitated before send my intention!")
		errChA <- err
	}()

	go func() {
		_, err := objstore.TryLockRemote(ctx, strg, "test.lock", "I wanna read it too, but I hesitated before committing!")
		errChB <- err
	}()

	<-chA
	<-chB

	<-chB
	<-chA

	// There is exactly one error.
	errA := <-errChA
	errB := <-errChB
	if errA == nil {
		require.Error(t, errB)
	} else {
		require.NoError(t, errB, "%s", errA)
	}

	requireFileExists(t, filepath.Join(path, "test.lock"))
}

func TestUnlockOnCleanUp(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	strg, pth := createMockStorage(t)
	lock, err := objstore.TryLockRemote(ctx, strg, "test.lock", "This file is mine!")
	require.NoError(t, err)
	requireFileExists(t, filepath.Join(pth, "test.lock"))

	cancel()
	lock.UnlockOnCleanUp(ctx)
	requireFileNotExists(t, filepath.Join(pth, "test.lock"))
}

func TestMakeLockMetaHasExpireAt(t *testing.T) {
	before := time.Now()
	meta := objstore.MakeLockMeta("test hint")
	after := time.Now()

	require.False(t, meta.ExpireAt.IsZero(), "ExpireAt must be set on new locks")
	require.Equal(t, meta.LockedAt.Add(objstore.LeaseTTL), meta.ExpireAt,
		"ExpireAt must equal LockedAt + LeaseTTL")
	require.False(t, meta.LockedAt.Before(before) || meta.LockedAt.After(after),
		"LockedAt must be within test window")
}

func TestTESTSetLeaseConstantsRestores(t *testing.T) {
	origTTL := objstore.LeaseTTL
	restore := objstore.TEST_SetLeaseConstants(100*time.Millisecond, 30*time.Millisecond, 3, 5*time.Millisecond)
	require.Equal(t, 100*time.Millisecond, objstore.LeaseTTL, "override must take effect")
	restore()
	require.Equal(t, origTTL, objstore.LeaseTTL, "restore must revert the override")
}

func TestTESTSetNowRestores(t *testing.T) {
	fixed := time.Date(2000, 1, 2, 3, 4, 5, 0, time.UTC)
	restore := objstore.TEST_SetNow(func() time.Time { return fixed })
	m1 := objstore.MakeLockMeta("t")
	require.Equal(t, fixed, m1.LockedAt)
	require.Equal(t, fixed.Add(objstore.LeaseTTL), m1.ExpireAt)
	restore()
	m2 := objstore.MakeLockMeta("t")
	require.True(t, m2.LockedAt.After(fixed), "after restore, time must advance past the injected past value")
}

func TestTryRenewSuccess(t *testing.T) {
	ctx := context.Background()
	strg, _ := createMockStorage(t)
	lock, err := objstore.TryLockRemote(ctx, strg, "test.lock", "owner")
	require.NoError(t, err)

	// Read back original ExpireAt.
	origData, err := strg.ReadFile(ctx, "test.lock")
	require.NoError(t, err)
	var origMeta objstore.LockMeta
	require.NoError(t, json.Unmarshal(origData, &origMeta))

	// Advance clock and renew.
	advanced := origMeta.LockedAt.Add(time.Minute)
	restoreNow := objstore.TEST_SetNow(func() time.Time { return advanced })
	defer restoreNow()

	require.NoError(t, objstore.TEST_TryRenew(lock, ctx))

	newData, err := strg.ReadFile(ctx, "test.lock")
	require.NoError(t, err)
	var newMeta objstore.LockMeta
	require.NoError(t, json.Unmarshal(newData, &newMeta))
	require.Equal(t, advanced.Add(objstore.LeaseTTL), newMeta.ExpireAt,
		"renewed ExpireAt must be nowFunc + LeaseTTL")
	require.Equal(t, origMeta.TxnID, newMeta.TxnID, "TxnID must stay unchanged across renewal")
}

func TestTryRenewTxnIDMismatch(t *testing.T) {
	ctx := context.Background()
	strg, _ := createMockStorage(t)
	lock, err := objstore.TryLockRemote(ctx, strg, "test.lock", "owner")
	require.NoError(t, err)

	// Overwrite the lock file with a different TxnID, simulating that another
	// process has reclaimed and re-acquired the lock.
	hijacked := objstore.MakeLockMeta("hijacker")
	hijacked.TxnID = []byte{99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99}
	hijackedData, err := json.Marshal(hijacked)
	require.NoError(t, err)
	require.NoError(t, strg.WriteFile(ctx, "test.lock", hijackedData))

	err = objstore.TEST_TryRenew(lock, ctx)
	require.ErrorIs(t, err, objstore.TEST_ErrRenewTxnIDMismatch)
}

func TestTryRenewLeaseExpired(t *testing.T) {
	ctx := context.Background()
	strg, _ := createMockStorage(t)
	lock, err := objstore.TryLockRemote(ctx, strg, "test.lock", "owner")
	require.NoError(t, err)

	// Advance the clock far past the lease expiry.
	origData, err := strg.ReadFile(ctx, "test.lock")
	require.NoError(t, err)
	var origMeta objstore.LockMeta
	require.NoError(t, json.Unmarshal(origData, &origMeta))

	wayLate := origMeta.ExpireAt.Add(time.Hour)
	restoreNow := objstore.TEST_SetNow(func() time.Time { return wayLate })
	defer restoreNow()

	err = objstore.TEST_TryRenew(lock, ctx)
	require.ErrorIs(t, err, objstore.TEST_ErrRenewLeaseExpired)
}

func TestTryRenewWriteError(t *testing.T) {
	ctx := context.Background()
	strg, _ := createMockStorage(t)
	lock, err := objstore.TryLockRemote(ctx, strg, "test.lock", "owner")
	require.NoError(t, err)

	require.NoError(t, failpoint.Enable(
		"github.com/pingcap/tidb/pkg/objstore/local_write_file_err",
		`return("simulated S3 outage")`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/objstore/local_write_file_err"))
	}()

	err = objstore.TEST_TryRenew(lock, ctx)
	require.Error(t, err)
	require.NotErrorIs(t, err, objstore.TEST_ErrRenewTxnIDMismatch)
	require.NotErrorIs(t, err, objstore.TEST_ErrRenewLeaseExpired)
	require.Contains(t, err.Error(), "simulated S3 outage")
}

func TestStartRenewalRefreshesLeasePeriodically(t *testing.T) {
	ctx := context.Background()
	strg, _ := createMockStorage(t)
	defer objstore.TEST_SetLeaseConstants(200*time.Millisecond, 30*time.Millisecond, 3, 5*time.Millisecond)()

	lock, err := objstore.TryLockRemote(ctx, strg, "test.lock", "owner")
	require.NoError(t, err)
	defer objstore.TEST_StopRenewal(lock)

	// Capture original ExpireAt.
	getExpireAt := func() time.Time {
		data, err := strg.ReadFile(ctx, "test.lock")
		require.NoError(t, err)
		var meta objstore.LockMeta
		require.NoError(t, json.Unmarshal(data, &meta))
		return meta.ExpireAt
	}
	orig := getExpireAt()

	lock.StartRenewal(ctx, nil)

	// Wait for at least two renewal ticks (interval=30ms → 100ms is plenty).
	require.Eventually(t, func() bool {
		return getExpireAt().After(orig)
	}, 500*time.Millisecond, 10*time.Millisecond,
		"ExpireAt should advance past the original within a few renewal ticks")
}

func TestStartRenewalCallsOnLeaseLostOnTxnIDMismatch(t *testing.T) {
	ctx := context.Background()
	strg, _ := createMockStorage(t)
	defer objstore.TEST_SetLeaseConstants(200*time.Millisecond, 20*time.Millisecond, 3, 5*time.Millisecond)()

	lock, err := objstore.TryLockRemote(ctx, strg, "test.lock", "owner")
	require.NoError(t, err)
	defer objstore.TEST_StopRenewal(lock)

	lostCh := make(chan struct{}, 1)
	lock.StartRenewal(ctx, func() { lostCh <- struct{}{} })

	// Hijack the lock with a different TxnID.
	hijacked := objstore.MakeLockMeta("hijacker")
	hijacked.TxnID = []byte{99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99}
	hijackedData, err := json.Marshal(hijacked)
	require.NoError(t, err)
	require.NoError(t, strg.WriteFile(ctx, "test.lock", hijackedData))

	select {
	case <-lostCh:
		// expected
	case <-time.After(500 * time.Millisecond):
		t.Fatal("onLeaseLost was not invoked within the expected window after TxnID mismatch")
	}
}

func TestStartRenewalCallsOnLeaseLostAfterRetryExhaustion(t *testing.T) {
	ctx := context.Background()
	strg, _ := createMockStorage(t)
	// Tight timing: 5 retries × ~5ms backoff = ~155ms tail. TTL = 1s gives margin.
	defer objstore.TEST_SetLeaseConstants(1*time.Second, 20*time.Millisecond, 5, 5*time.Millisecond)()

	lock, err := objstore.TryLockRemote(ctx, strg, "test.lock", "owner")
	require.NoError(t, err)
	defer objstore.TEST_StopRenewal(lock)

	require.NoError(t, failpoint.Enable(
		"github.com/pingcap/tidb/pkg/objstore/local_write_file_err",
		`return("simulated outage")`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/objstore/local_write_file_err"))
	}()

	lostCh := make(chan struct{}, 1)
	lock.StartRenewal(ctx, func() { lostCh <- struct{}{} })

	select {
	case <-lostCh:
		// expected
	case <-time.After(2 * time.Second):
		t.Fatal("onLeaseLost was not invoked after persistent renewal failures")
	}
}

func TestStartRenewalPanicsOnDoubleCall(t *testing.T) {
	ctx := context.Background()
	strg, _ := createMockStorage(t)
	defer objstore.TEST_SetLeaseConstants(1*time.Second, 100*time.Millisecond, 3, 5*time.Millisecond)()

	lock, err := objstore.TryLockRemote(ctx, strg, "test.lock", "owner")
	require.NoError(t, err)
	defer objstore.TEST_StopRenewal(lock)

	lock.StartRenewal(ctx, nil)
	require.Panics(t, func() {
		lock.StartRenewal(ctx, nil)
	}, "second StartRenewal must panic")
}

func TestUnlockWithoutRenewalIsUnchanged(t *testing.T) {
	ctx := context.Background()
	strg, pth := createMockStorage(t)
	lock, err := objstore.TryLockRemote(ctx, strg, "test.lock", "owner")
	require.NoError(t, err)
	requireFileExists(t, filepath.Join(pth, "test.lock"))

	// No StartRenewal called; Unlock should still work.
	require.NoError(t, lock.Unlock(ctx))
	requireFileNotExists(t, filepath.Join(pth, "test.lock"))
}

func TestUnlockWaitsForRenewalGoroutine(t *testing.T) {
	ctx := context.Background()
	strg, pth := createMockStorage(t)
	defer objstore.TEST_SetLeaseConstants(1*time.Second, 30*time.Millisecond, 3, 5*time.Millisecond)()

	lock, err := objstore.TryLockRemote(ctx, strg, "test.lock", "owner")
	require.NoError(t, err)
	lock.StartRenewal(ctx, nil)

	// Let renewal run for a few ticks so a WriteFile is likely in flight.
	time.Sleep(60 * time.Millisecond)

	// Unlock must wait for the renewal goroutine to exit before deleting.
	// After Unlock returns, no further WriteFile can happen, so the lock
	// file stays deleted.
	require.NoError(t, lock.Unlock(ctx))
	requireFileNotExists(t, filepath.Join(pth, "test.lock"))

	// Give any (incorrectly-still-running) goroutine a moment to misbehave.
	time.Sleep(60 * time.Millisecond)
	requireFileNotExists(t, filepath.Join(pth, "test.lock"))

	require.NotPanics(t, func() {
		err = lock.Unlock(ctx)
	})
	require.Error(t, err)
}

// writeLockMeta is a small test helper that places a custom LockMeta JSON at a
// given path on the storage, simulating what a (possibly long-dead) holder
// would have written. Used by CleanUpStaleLock tests to plant scenarios.
func writeLockMeta(t *testing.T, strg storeapi.Storage, path string, meta objstore.LockMeta) {
	t.Helper()
	data, err := json.Marshal(meta)
	require.NoError(t, err)
	require.NoError(t, strg.WriteFile(context.Background(), path, data))
}

type deleteRaceStorage struct {
	storeapi.Storage
	beforeDelete func(context.Context, string)
}

func (s *deleteRaceStorage) DeleteFile(ctx context.Context, name string) error {
	if s.beforeDelete != nil {
		s.beforeDelete(ctx, name)
	}
	return s.Storage.DeleteFile(ctx, name)
}

func TestCleanUpStaleLockOverdueWithinTTL(t *testing.T) {
	ctx := context.Background()
	strg, pth := createMockStorage(t)
	defer objstore.TEST_SetLeaseConstants(100*time.Millisecond, 30*time.Millisecond, 3, 5*time.Millisecond)()

	t.Run("missing lock is no-op", func(t *testing.T) {
		reclaimed, err := objstore.CleanUpStaleLock(ctx, strg, "missing.lock")
		require.NoError(t, err)
		require.False(t, reclaimed, "missing lock means there is nothing to clean up")
	})

	// Plant a lock that expired 30ms ago. With LeaseTTL=100ms, the lock is
	// not reclaimable until ExpireAt+LeaseTTL.
	now := time.Now()
	defer objstore.TEST_SetNow(func() time.Time { return now })()
	writeLockMeta(t, strg, "stale.lock", objstore.LockMeta{
		LockedAt: now.Add(-2 * time.Minute),
		ExpireAt: now.Add(-30 * time.Millisecond),
		TxnID:    []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		Hint:     "stale-overdue-within-ttl",
	})
	requireFileExists(t, filepath.Join(pth, "stale.lock"))

	start := time.Now()
	reclaimed, err := objstore.CleanUpStaleLock(ctx, strg, "stale.lock")
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.False(t, reclaimed, "lock must not be reclaimed until ExpireAt+LeaseTTL")
	requireFileExists(t, filepath.Join(pth, "stale.lock"))
	require.Less(t, elapsed, 30*time.Millisecond, "cleanup should not wait for the grace window")
}

func TestCleanUpStaleLockOverduePastTTL(t *testing.T) {
	ctx := context.Background()
	strg, pth := createMockStorage(t)
	defer objstore.TEST_SetLeaseConstants(100*time.Millisecond, 30*time.Millisecond, 3, 5*time.Millisecond)()

	// Lock expired well over a full TTL ago — no wait needed.
	now := time.Now()
	writeLockMeta(t, strg, "stale.lock", objstore.LockMeta{
		LockedAt: now.Add(-time.Hour),
		ExpireAt: now.Add(-time.Hour),
		TxnID:    []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		Hint:     "stale-overdue-past-ttl",
	})

	start := time.Now()
	reclaimed, err := objstore.CleanUpStaleLock(ctx, strg, "stale.lock")
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.True(t, reclaimed)
	requireFileNotExists(t, filepath.Join(pth, "stale.lock"))
	require.Less(t, elapsed, 30*time.Millisecond, "deeply overdue locks should reclaim immediately")
}

func TestCleanUpStaleLockDoesNotDeleteFreshFixedPathLock(t *testing.T) {
	ctx := context.Background()
	strg, _ := createMockStorage(t)
	defer objstore.TEST_SetLeaseConstants(100*time.Millisecond, 30*time.Millisecond, 3, 5*time.Millisecond)()

	now := time.Now()
	defer objstore.TEST_SetNow(func() time.Time { return now })()
	writeLockMeta(t, strg, "fixed.lock", objstore.LockMeta{
		LockedAt: now.Add(-time.Hour),
		ExpireAt: now.Add(-time.Hour),
		TxnID:    []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		Hint:     "stale-holder",
	})

	raceStrg := &deleteRaceStorage{Storage: strg}
	var freshLock *objstore.RemoteLock
	raceStrg.beforeDelete = func(ctx context.Context, name string) {
		require.Equal(t, "fixed.lock", name)
		raceStrg.beforeDelete = nil

		require.NoError(t, strg.DeleteFile(ctx, name))
		var err error
		freshLock, err = objstore.TryLockRemote(ctx, strg, name, "fresh-holder")
		require.NoError(t, err)
	}

	reclaimed, err := objstore.CleanUpStaleLock(ctx, raceStrg, "fixed.lock")
	require.NoError(t, err)
	require.NotNil(t, freshLock)

	meta, err := getLockMetaForTest(t, strg, "fixed.lock")
	require.NoError(t, err)
	require.Equal(t, "fresh-holder", meta.Hint, "cleanup must not delete a freshly acquired fixed-path lock")
	require.False(t, reclaimed, "cleanup must not report reclaiming a lock that was replaced during cleanup")
	require.NoError(t, freshLock.Unlock(ctx))
}

func TestCleanUpStaleLockAlive(t *testing.T) {
	ctx := context.Background()
	strg, pth := createMockStorage(t)
	defer objstore.TEST_SetLeaseConstants(100*time.Millisecond, 30*time.Millisecond, 3, 5*time.Millisecond)()

	// Fresh lock with ExpireAt comfortably in the future.
	now := time.Now()
	writeLockMeta(t, strg, "alive.lock", objstore.LockMeta{
		LockedAt: now,
		ExpireAt: now.Add(50 * time.Millisecond),
		TxnID:    []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		Hint:     "alive",
	})

	reclaimed, err := objstore.CleanUpStaleLock(ctx, strg, "alive.lock")
	require.NoError(t, err)
	require.False(t, reclaimed, "alive lock must NOT be reclaimed")
	requireFileExists(t, filepath.Join(pth, "alive.lock"))
}

func TestCleanUpStaleLockZeroExpireAt(t *testing.T) {
	ctx := context.Background()
	strg, pth := createMockStorage(t)

	// Old-format lock without ExpireAt.
	writeLockMeta(t, strg, "old.lock", objstore.LockMeta{
		LockedAt: time.Now().Add(-time.Hour),
		TxnID:    []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		Hint:     "old-format",
	})

	reclaimed, err := objstore.CleanUpStaleLock(ctx, strg, "old.lock")
	require.NoError(t, err)
	require.False(t, reclaimed, "zero ExpireAt locks must NOT be auto-reclaimed (backward compat)")
	requireFileExists(t, filepath.Join(pth, "old.lock"))
}

func TestCleanUpStaleLockRefreshedDuringWait(t *testing.T) {
	ctx := context.Background()
	strg, pth := createMockStorage(t)
	defer objstore.TEST_SetLeaseConstants(100*time.Millisecond, 30*time.Millisecond, 3, 5*time.Millisecond)()

	// Plant a lock that expired 20ms ago. It is past ExpireAt but still
	// inside the extra LeaseTTL grace window, so cleanup must not delete it.
	now := time.Now()
	writeLockMeta(t, strg, "racing.lock", objstore.LockMeta{
		LockedAt: now.Add(-time.Minute),
		ExpireAt: now.Add(-20 * time.Millisecond),
		TxnID:    []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		Hint:     "to-be-refreshed",
	})

	reclaimed, err := objstore.CleanUpStaleLock(ctx, strg, "racing.lock")
	require.NoError(t, err)
	require.False(t, reclaimed, "reclaim must wait until ExpireAt+LeaseTTL has passed")
	requireFileExists(t, filepath.Join(pth, "racing.lock"))
}

func TestLockWithRetryReclaimsStaleWriteLock(t *testing.T) {
	ctx := context.Background()
	strg, pth := createMockStorage(t)
	defer objstore.TEST_SetLeaseConstants(100*time.Millisecond, 30*time.Millisecond, 3, 5*time.Millisecond)()

	// Plant a stale write lock at v1/LOCK.WRIT.
	now := time.Now()
	writeLockMeta(t, strg, "v1/LOCK.WRIT", objstore.LockMeta{
		LockedAt: now.Add(-time.Hour),
		ExpireAt: now.Add(-time.Hour), // long past
		TxnID:    []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		Hint:     "crashed-prior-truncate",
	})
	requireFileExists(t, filepath.Join(pth, "v1/LOCK.WRIT"))

	shortCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	lock, err := objstore.LockWithRetry(shortCtx, objstore.TryLockRemoteWrite, strg, "v1/LOCK", "new truncate")
	require.Error(t, err)
	require.Nil(t, lock)
	requireFileNotExists(t, filepath.Join(pth, "v1/LOCK.WRIT"))
}

func TestLockWithRetryReclaimsStaleReadLockBeforeWrite(t *testing.T) {
	ctx := context.Background()
	strg, pth := createMockStorage(t)
	defer objstore.TEST_SetLeaseConstants(100*time.Millisecond, 30*time.Millisecond, 3, 5*time.Millisecond)()

	// Plant a stale READ lock under v1/LOCK.READ.{xxxx}.
	now := time.Now()
	writeLockMeta(t, strg, "v1/LOCK.READ.cafef00d12345678", objstore.LockMeta{
		LockedAt: now.Add(-time.Hour),
		ExpireAt: now.Add(-time.Hour),
		TxnID:    []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		Hint:     "crashed-prior-restore",
	})
	writeLockMeta(t, strg, "v1/LOCK.READ.deadbeef12345678", objstore.LockMeta{
		LockedAt: now.Add(-time.Hour),
		ExpireAt: now.Add(-time.Hour),
		TxnID:    []byte{16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1},
		Hint:     "another-crashed-prior-restore",
	})

	// Without reclaim the write would fail (stale read blocks write). The
	// cleanup pass should delete it, then fall through to ordinary backoff.
	shortCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	lock, err := objstore.LockWithRetry(shortCtx, objstore.TryLockRemoteWrite, strg, "v1/LOCK", "new compaction")
	require.Error(t, err)
	require.Nil(t, lock)
	requireFileNotExists(t, filepath.Join(pth, "v1/LOCK.READ.cafef00d12345678"))
	requireFileNotExists(t, filepath.Join(pth, "v1/LOCK.READ.deadbeef12345678"))

	t.Run("cleanup does not bypass retry backoff", func(t *testing.T) {
		strg, _ := createMockStorage(t)
		now := time.Now()
		writeLockMeta(t, strg, "v2/LOCK.READ.cafef00d12345678", objstore.LockMeta{
			LockedAt: now.Add(-time.Hour),
			ExpireAt: now.Add(-time.Hour),
			TxnID:    []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			Hint:     "unrelated stale reader",
		})

		var attempts atomic.Int32
		shortCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		lock, err := objstore.LockWithRetry(shortCtx, func(
			context.Context,
			storeapi.Storage,
			string,
			string,
		) (*objstore.RemoteLock, error) {
			attempts.Add(1)
			return nil, context.Canceled
		}, strg, "v2/LOCK", "still blocked")
		require.Error(t, err)
		require.Nil(t, lock)
		require.Equal(t, int32(1), attempts.Load())
	})
}

func TestLockWithRetryDoesNotReclaimAliveLock(t *testing.T) {
	ctx := context.Background()
	strg, _ := createMockStorage(t)
	defer objstore.TEST_SetLeaseConstants(100*time.Millisecond, 30*time.Millisecond, 3, 5*time.Millisecond)()

	// Acquire a real read lock (alive, ExpireAt in the future).
	aliveLock, err := objstore.TryLockRemoteRead(ctx, strg, "v1/LOCK", "alive reader")
	require.NoError(t, err)
	defer func() { _ = aliveLock.Unlock(ctx) }()

	// Try to acquire a write lock with a tight retry budget. Since the alive
	// reader's lock is not stale, reclaim must NOT delete it. LockWithRetry
	// should backoff-retry until exhausted and return error.
	type result struct {
		lock *objstore.RemoteLock
		err  error
	}
	resCh := make(chan result, 1)
	go func() {
		// Use a short context to bound the test if backoff somehow becomes infinite.
		shortCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()
		l, err := objstore.LockWithRetry(shortCtx, objstore.TryLockRemoteWrite, strg, "v1/LOCK", "wants write")
		resCh <- result{l, err}
	}()
	res := <-resCh
	require.Error(t, res.err, "alive reader must keep the writer blocked")
	require.Nil(t, res.lock)

	// And the alive lock file is still there.
	meta, err := getLockMetaForTest(t, strg, aliveLockPath(t, strg, "v1/LOCK"))
	require.NoError(t, err)
	require.False(t, meta.ExpireAt.IsZero(), "alive lock should still have its ExpireAt")
}

func TestLockWithRetryBackwardCompatOldFormatLock(t *testing.T) {
	ctx := context.Background()
	strg, pth := createMockStorage(t)
	defer objstore.TEST_SetLeaseConstants(100*time.Millisecond, 30*time.Millisecond, 3, 5*time.Millisecond)()

	// Plant an old-format lock (no ExpireAt) — must not be auto-reclaimed.
	writeLockMeta(t, strg, "v1/LOCK.WRIT", objstore.LockMeta{
		LockedAt: time.Now().Add(-time.Hour),
		TxnID:    []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		Hint:     "old-binary",
	})

	type result struct {
		lock *objstore.RemoteLock
		err  error
	}
	resCh := make(chan result, 1)
	go func() {
		shortCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()
		l, err := objstore.LockWithRetry(shortCtx, objstore.TryLockRemoteWrite, strg, "v1/LOCK", "wants write")
		resCh <- result{l, err}
	}()
	res := <-resCh
	require.Error(t, res.err, "old-format lock must NOT be auto-reclaimed; must fall through to backoff retry then fail")
	require.Nil(t, res.lock)
	requireFileExists(t, filepath.Join(pth, "v1/LOCK.WRIT"))
}

// getLockMetaForTest is a small helper that reads and unmarshals a LockMeta
// from a known path. Used by alive-lock test to inspect post-state.
func getLockMetaForTest(t *testing.T, strg storeapi.Storage, p string) (objstore.LockMeta, error) {
	t.Helper()
	data, err := strg.ReadFile(context.Background(), p)
	if err != nil {
		return objstore.LockMeta{}, err
	}
	var meta objstore.LockMeta
	err = json.Unmarshal(data, &meta)
	return meta, err
}

// aliveLockPath finds the single READ lock file under base path. Test helper.
func aliveLockPath(t *testing.T, strg storeapi.Storage, base string) string {
	t.Helper()
	var found string
	require.NoError(t, strg.WalkDir(context.Background(), &storeapi.WalkOption{
		SubDir:    "v1",
		ObjPrefix: "LOCK.READ.",
	}, func(p string, _ int64) error {
		found = p
		return nil
	}))
	require.NotEmpty(t, found, "expected exactly one read lock under %s", base)
	return found
}

func TestLockMetaBackwardCompatZeroExpireAt(t *testing.T) {
	oldJSON := `{
		"locked_at": "2024-01-01T00:00:00Z",
		"locker_host": "oldhost",
		"locker_pid": 123,
		"txn_id": "AAECAwQFBgcICQoLDA0ODw==",
		"hint": "old"
	}`
	var meta objstore.LockMeta
	require.NoError(t, json.Unmarshal([]byte(oldJSON), &meta))
	require.True(t, meta.ExpireAt.IsZero(),
		"old-format LockMeta (no expire_at key) must deserialize with zero ExpireAt")
	require.Equal(t, "oldhost", meta.LockerHost, "other fields deserialize as usual")
}
