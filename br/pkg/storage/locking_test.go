// Copyright 2023 PingCAP, Inc. Licensed under Apache-2.0.

package storage_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pingcaperrors "github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	backup "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func createMockStorage(t *testing.T) (storage.ExternalStorage, string) {
	tempdir := t.TempDir()
	storage, err := storage.New(context.Background(), &backup.StorageBackend{
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

type walkDirErrorStorage struct {
	storage.ExternalStorage
	objPrefix string
	err       error
}

func (s walkDirErrorStorage) WalkDir(ctx context.Context, opt *storage.WalkOption, fn func(path string, size int64) error) error {
	if opt.ObjPrefix == s.objPrefix {
		return s.err
	}
	return s.ExternalStorage.WalkDir(ctx, opt, fn)
}

type causeAndLockError struct {
	cause  error
	locked storage.ErrLocked
}

func (e causeAndLockError) Error() string {
	return e.cause.Error() + ": " + e.locked.Error()
}

func (e causeAndLockError) Unwrap() []error {
	return []error{e.locked, e.cause}
}

func (e causeAndLockError) Cause() error {
	return e.cause
}

func TestTryLockRemote(t *testing.T) {
	ctx := context.Background()
	strg, pth := createMockStorage(t)
	lock, err := storage.TryLockRemote(ctx, strg, "test.lock", storage.LockMetaInput{Hint: "This file is mine!"})
	require.NoError(t, err)
	requireFileExists(t, filepath.Join(pth, "test.lock"))
	err = lock.Unlock(ctx)
	require.NoError(t, err)
	requireFileNotExists(t, filepath.Join(pth, "test.lock"))
}

func TestConflictLock(t *testing.T) {
	ctx := context.Background()
	strg, pth := createMockStorage(t)
	lock, err := storage.TryLockRemote(ctx, strg, "test.lock", storage.LockMetaInput{Hint: "This file is mine!"})
	require.NoError(t, err)
	_, err = storage.TryLockRemote(ctx, strg, "test.lock", storage.LockMetaInput{Hint: "This file is mine!"})
	require.ErrorContains(t, err, "conflict file test.lock")
	requireFileExists(t, filepath.Join(pth, "test.lock"))
	err = lock.Unlock(ctx)
	require.NoError(t, err)
	requireFileNotExists(t, filepath.Join(pth, "test.lock"))
}

func TestRWLock(t *testing.T) {
	ctx := context.Background()
	strg, path := createMockStorage(t)
	lock, err := storage.TryLockRemoteRead(ctx, strg, "test.lock", storage.LockMetaInput{Hint: "I wanna read it!"})
	require.NoError(t, err)
	lock2, err := storage.TryLockRemoteRead(ctx, strg, "test.lock", storage.LockMetaInput{Hint: "I wanna read it too!"})
	require.NoError(t, err)
	_, err = storage.TryLockRemoteWrite(ctx, strg, "test.lock", storage.LockMetaInput{Hint: "I wanna write it, you get out!"})
	require.Error(t, err)
	require.NoError(t, lock.Unlock(ctx))
	require.NoError(t, lock2.Unlock(ctx))
	l, err := storage.TryLockRemoteWrite(ctx, strg, "test.lock", storage.LockMetaInput{Hint: "Can I have a write lock?"})
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

	require.NoError(t, failpoint.EnableCall("github.com/pingcap/tidb/br/pkg/storage/exclusive-write-commit-to-1", onceA))
	require.NoError(t, failpoint.EnableCall("github.com/pingcap/tidb/br/pkg/storage/exclusive-write-commit-to-2", onceB))

	go func() {
		_, err := storage.TryLockRemote(ctx, strg, "test.lock", storage.LockMetaInput{Hint: "I wanna read it, but I hesitated before send my intention!"})
		errChA <- err
	}()

	go func() {
		_, err := storage.TryLockRemote(ctx, strg, "test.lock", storage.LockMetaInput{Hint: "I wanna read it too, but I hesitated before committing!"})
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
	lock, err := storage.TryLockRemote(ctx, strg, "test.lock", storage.LockMetaInput{Hint: "This file is mine!"})
	require.NoError(t, err)
	requireFileExists(t, filepath.Join(pth, "test.lock"))

	cancel()
	lock.UnlockOnCleanUp(ctx)
	requireFileNotExists(t, filepath.Join(pth, "test.lock"))
}

func TestMakeLockMeta(t *testing.T) {
	cases := []struct {
		name             string
		input            storage.LockMetaInput
		expectedOwnerID  string
		expectedLockType string
		expectedHint     string
	}{
		{
			name: "owner input",
			input: storage.LockMetaInput{
				OwnerID:  "op-1",
				LockType: "migration-read",
				Hint:     "holder",
			},
			expectedOwnerID:  "op-1",
			expectedLockType: "migration-read",
			expectedHint:     "holder",
		},
		{
			name:         "minimal input",
			input:        storage.LockMetaInput{Hint: "minimal"},
			expectedHint: "minimal",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			meta := storage.MakeLockMeta(c.input)

			require.Equal(t, c.expectedOwnerID, meta.OwnerID)
			require.Equal(t, c.expectedLockType, meta.LockType)
			require.Equal(t, c.expectedHint, meta.Hint)
			require.NotZero(t, meta.LockedAt)
			require.NotEmpty(t, meta.LockerHost)
			require.Equal(t, os.Getpid(), meta.LockerPID)

			data, err := json.Marshal(meta)
			require.NoError(t, err)
			require.NotContains(t, string(data), "operation_started_at")
			require.NotContains(t, string(data), "restore_id")
			require.NotContains(t, string(data), "resource_type")
		})
	}
}

func TestLockMetaOldJSONCompatibility(t *testing.T) {
	oldJSON := []byte(`{"locked_at":"2026-06-15T01:02:03Z","locker_host":"host-a","locker_pid":123,"txn_id":"dHhu","hint":"old"}`)
	var meta storage.LockMeta
	require.NoError(t, json.Unmarshal(oldJSON, &meta))

	require.Equal(t, "host-a", meta.LockerHost)
	require.Equal(t, 123, meta.LockerPID)
	require.Equal(t, "old", meta.Hint)
	require.Empty(t, meta.OwnerID)
	require.Empty(t, meta.LockType)
}

func TestLockMetaStringIncludesOwnerFields(t *testing.T) {
	meta := storage.LockMeta{
		LockedAt:   time.Date(2026, 6, 15, 4, 5, 6, 0, time.UTC),
		LockerHost: "host-a",
		LockerPID:  123,
		TxnID:      []byte("txn"),
		Hint:       `restore_id=456 detail="hint-a"`,
		OwnerID:    "op-1",
		LockType:   "migration-write",
	}

	got := meta.String()
	require.Contains(t, got, "2026-06-15 04:05:06")
	require.Contains(t, got, "host-a")
	require.Contains(t, got, "123")
	require.Contains(t, got, "hint-a")
	require.Contains(t, got, "op-1")
	require.Contains(t, got, "456")
	require.Contains(t, got, "migration-write")
	require.NotContains(t, strings.ToLower(got), "txn_id")
	require.NotContains(t, strings.ToLower(got), "txn")
}

func TestErrLockedErrorLimitsBlockerOutput(t *testing.T) {
	err := storage.ErrLocked{
		Path: "test.lock.WRIT",
		Blockers: []storage.LockBlocker{
			{Path: "test.lock.READ.0", Meta: storage.LockMeta{OwnerID: "op-0"}},
			{Path: "test.lock.READ.1", Meta: storage.LockMeta{OwnerID: "op-1"}},
			{Path: "test.lock.READ.2", Meta: storage.LockMeta{OwnerID: "op-2"}},
			{Path: "test.lock.READ.3", Meta: storage.LockMeta{OwnerID: "op-3"}},
		},
	}

	got := err.Error()
	require.Contains(t, got, "test.lock.READ.0")
	require.Contains(t, got, "test.lock.READ.1")
	require.Contains(t, got, "test.lock.READ.2")
	require.NotContains(t, got, "test.lock.READ.3")
	require.Contains(t, got, "omitted_conflict_files = 1")
}

func TestConflictLockReportsRemotePathAndMeta(t *testing.T) {
	ctx := context.Background()
	strg, pth := createMockStorage(t)
	remoteInput := storage.LockMetaInput{
		OwnerID:  "remote-op",
		LockType: "migration-read",
		Hint:     "remote",
	}
	lock, err := storage.TryLockRemote(ctx, strg, "test.lock", remoteInput)
	require.NoError(t, err)
	defer func() { require.NoError(t, lock.Unlock(ctx)) }()

	localInput := storage.LockMetaInput{OwnerID: "local-op", LockType: "migration-write", Hint: "local"}
	_, err = storage.TryLockRemote(ctx, strg, "test.lock", localInput)
	require.Error(t, err)
	require.ErrorContains(t, err, "test.lock")
	require.ErrorContains(t, err, "remote-op")
	require.ErrorContains(t, err, "migration-read")

	var locked storage.ErrLocked
	require.True(t, errors.As(err, &locked))
	require.Equal(t, "test.lock", locked.Path)
	require.Equal(t, localInput, locked.Local)
	require.Equal(t, "remote-op", locked.Meta.OwnerID)
	require.Equal(t, "migration-read", locked.Meta.LockType)
	requireFileExists(t, filepath.Join(pth, "test.lock"))
}

func TestWriteLockConflictReportsReadLockBlocker(t *testing.T) {
	ctx := context.Background()
	readInput := storage.LockMetaInput{
		OwnerID:  "read-op",
		LockType: "migration-read",
		Hint:     "reader",
	}
	localInput := storage.LockMetaInput{OwnerID: "write-op", LockType: "migration-write", Hint: "writer"}
	_, err := createReadWriteConflict(ctx, t, readInput, localInput)

	var locked storage.ErrLocked
	require.True(t, errors.As(err, &locked))
	require.Equal(t, "test.lock.WRIT", locked.Path)
	require.Equal(t, localInput, locked.Local)
	require.Len(t, locked.Blockers, 1)
	require.Contains(t, locked.Blockers[0].Path, "test.lock.READ.")
	require.Equal(t, "read-op", locked.Blockers[0].Meta.OwnerID)
	require.Equal(t, "migration-read", locked.Blockers[0].Meta.LockType)
	require.NoError(t, locked.Blockers[0].Err)
	require.ErrorContains(t, err, "test.lock.READ.")
	require.ErrorContains(t, err, "read-op")
}

func TestWriteLockConflictReportsMultipleReadLockBlockers(t *testing.T) {
	ctx := context.Background()
	strg, _ := createMockStorage(t)
	readInput := storage.LockMetaInput{
		OwnerID:  "read-op",
		LockType: "migration-read",
		Hint:     "reader",
	}
	lock1, err := storage.TryLockRemoteRead(ctx, strg, "test.lock", readInput)
	require.NoError(t, err)
	defer func() { require.NoError(t, lock1.Unlock(ctx)) }()
	lock2, err := storage.TryLockRemoteRead(ctx, strg, "test.lock", readInput)
	require.NoError(t, err)
	defer func() { require.NoError(t, lock2.Unlock(ctx)) }()

	_, err = storage.TryLockRemoteWrite(ctx, strg, "test.lock", storage.LockMetaInput{
		OwnerID:  "write-op",
		LockType: "migration-write",
		Hint:     "writer",
	})
	require.Error(t, err)

	var locked storage.ErrLocked
	require.True(t, errors.As(err, &locked))
	require.Equal(t, 2, locked.BlockerCount)
	require.Len(t, locked.Blockers, 2)
	for _, blocker := range locked.Blockers {
		require.Contains(t, blocker.Path, "test.lock.READ.")
		require.Equal(t, "read-op", blocker.Meta.OwnerID)
		require.Equal(t, "migration-read", blocker.Meta.LockType)
		require.NoError(t, blocker.Err)
	}
}

func TestWriteLockConflictSamplesReadLockBlockers(t *testing.T) {
	ctx := context.Background()
	strg, _ := createMockStorage(t)
	readInput := storage.LockMetaInput{
		OwnerID:  "read-op",
		LockType: "migration-read",
		Hint:     "reader",
	}
	readLocks := make([]storage.RemoteLock, 0, 5)
	for range 5 {
		lock, err := storage.TryLockRemoteRead(ctx, strg, "test.lock", readInput)
		require.NoError(t, err)
		readLocks = append(readLocks, lock)
	}
	t.Cleanup(func() {
		for _, lock := range readLocks {
			require.NoError(t, lock.Unlock(context.Background()))
		}
	})

	localInput := storage.LockMetaInput{OwnerID: "write-op", LockType: "migration-write", Hint: "writer"}
	_, err := storage.TryLockRemoteWrite(ctx, strg, "test.lock", localInput)
	require.Error(t, err)

	var locked storage.ErrLocked
	require.True(t, errors.As(err, &locked))
	require.Equal(t, 5, locked.BlockerCount)
	require.Len(t, locked.Blockers, 3)
	for _, blocker := range locked.Blockers {
		require.Contains(t, blocker.Path, "test.lock.READ.")
		require.Equal(t, "read-op", blocker.Meta.OwnerID)
		require.Equal(t, "migration-read", blocker.Meta.LockType)
		require.NoError(t, blocker.Err)
	}
	require.ErrorContains(t, err, "omitted_conflict_files = 2")

	fields := storage.LockConflictLogFields("test.lock", localInput, err)
	requireZapIntField(t, fields, "remote_blocker_count", 5)
	requireZapStringField(t, fields, "remote_blocker_0_owner_id", "read-op")
	requireZapStringField(t, fields, "remote_blocker_1_owner_id", "read-op")
	requireZapStringField(t, fields, "remote_blocker_2_owner_id", "read-op")
}

func TestLockWithRetryCarriesLocalAndRemoteMetadata(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	remoteInput := storage.LockMetaInput{
		OwnerID:  "remote-op",
		LockType: "migration-read",
		Hint:     "reader",
	}
	localInput := storage.LockMetaInput{OwnerID: "local-op", LockType: "migration-write", Hint: "writer"}
	strg, _ := createReadWriteConflict(context.Background(), t, remoteInput, localInput)
	_, err := storage.LockWithRetry(ctx, storage.TryLockRemoteWrite, strg, "test.lock", localInput)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))
	require.Equal(t, context.Canceled, pingcaperrors.Cause(err))

	var locked storage.ErrLocked
	require.True(t, errors.As(err, &locked))
	require.Equal(t, localInput, locked.Local)
	require.Len(t, locked.Blockers, 1)
	require.Contains(t, locked.Blockers[0].Path, "test.lock.READ.")
	require.Equal(t, "remote-op", locked.Blockers[0].Meta.OwnerID)
	require.Equal(t, "migration-read", locked.Blockers[0].Meta.LockType)
}

func TestLockConflictLogFieldsCarriesLocalAndRemoteMetadata(t *testing.T) {
	ctx := context.Background()
	remoteInput := storage.LockMetaInput{
		OwnerID:  "remote-op",
		LockType: "migration-read",
		Hint:     "reader",
	}
	localInput := storage.LockMetaInput{OwnerID: "local-op", LockType: "migration-write", Hint: "writer"}
	_, err := createReadWriteConflict(ctx, t, remoteInput, localInput)

	fields := storage.LockConflictLogFields("test.lock", localInput, err)
	requireZapStringField(t, fields, "path", "test.lock")
	requireZapStringField(t, fields, "local_owner_id", "local-op")
	requireZapStringField(t, fields, "local_lock_type", "migration-write")
	requireZapStringField(t, fields, "remote_blocker_0_owner_id", "remote-op")
	requireZapStringField(t, fields, "remote_blocker_0_lock_type", "migration-read")
	requireZapStringField(t, fields, "remote_blocker_0_hint", "reader")
}

func TestTryLockRemoteWritePreservesOriginalErrorWhenEnrichingErrLocked(t *testing.T) {
	ctx := context.Background()
	strg, _ := createMockStorage(t)
	sentinelErr := errors.New("intent walk failed")
	originalErr := causeAndLockError{
		cause: sentinelErr,
		locked: storage.ErrLocked{
			Blockers: []storage.LockBlocker{
				{
					Path: "test.lock.READ.remote",
					Meta: storage.LockMeta{
						OwnerID:  "remote-op",
						LockType: "migration-read",
						Hint:     "reader",
					},
				},
			},
		},
	}
	wrappedStorage := walkDirErrorStorage{
		ExternalStorage: strg,
		objPrefix:       "test.lock",
		err:             originalErr,
	}
	_, err := storage.TryLockRemoteWrite(ctx, wrappedStorage, "test.lock", storage.LockMetaInput{
		OwnerID:  "local-op",
		LockType: "migration-write",
		Hint:     "writer",
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, sentinelErr))
	require.Equal(t, sentinelErr, pingcaperrors.Cause(err))

	var locked storage.ErrLocked
	require.True(t, errors.As(err, &locked))
	require.Equal(t, "test.lock.WRIT", locked.Path)
	require.Len(t, locked.Blockers, 1)
	require.Equal(t, "test.lock.READ.remote", locked.Blockers[0].Path)
	require.Equal(t, "remote-op", locked.Blockers[0].Meta.OwnerID)
}

func createReadWriteConflict(
	ctx context.Context,
	t *testing.T,
	remoteInput storage.LockMetaInput,
	localInput storage.LockMetaInput,
) (storage.ExternalStorage, error) {
	t.Helper()

	strg, _ := createMockStorage(t)
	lock, err := storage.TryLockRemoteRead(ctx, strg, "test.lock", remoteInput)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, lock.Unlock(context.Background())) })

	_, err = storage.TryLockRemoteWrite(ctx, strg, "test.lock", localInput)
	require.Error(t, err)
	return strg, err
}

func requireZapStringField(t *testing.T, fields []zap.Field, key string, value string) {
	t.Helper()
	for _, field := range fields {
		if field.Key == key {
			require.Equal(t, value, field.String)
			return
		}
	}
	require.Failf(t, "missing zap field", "field %q not found in %#v", key, fields)
}

func requireZapIntField(t *testing.T, fields []zap.Field, key string, value int64) {
	t.Helper()
	for _, field := range fields {
		if field.Key == key {
			require.Equal(t, value, field.Integer)
			return
		}
	}
	require.Failf(t, "missing zap field", "field %q not found in %#v", key, fields)
}
