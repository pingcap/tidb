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
