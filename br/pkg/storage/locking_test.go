// Copyright 2023 PingCAP, Inc. Licensed under Apache-2.0.

package storage_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	backup "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"
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

func TestTryLockRemote(t *testing.T) {
	ctx := context.Background()
	strg, pth := createMockStorage(t)
	lock, err := storage.TryLockRemote(ctx, strg, "test.lock", "This file is mine!")
	require.NoError(t, err)
	requireFileExists(t, filepath.Join(pth, "test.lock"))
	err = lock.Unlock(ctx)
	require.NoError(t, err)
	requireFileNotExists(t, filepath.Join(pth, "test.lock"))
}

func TestConflictLock(t *testing.T) {
	ctx := context.Background()
	strg, pth := createMockStorage(t)
	lock, err := storage.TryLockRemote(ctx, strg, "test.lock", "This file is mine!")
	require.NoError(t, err)
	_, err = storage.TryLockRemote(ctx, strg, "test.lock", "This file is mine!")
	require.ErrorContains(t, err, "conflict file test.lock")
	requireFileExists(t, filepath.Join(pth, "test.lock"))
	err = lock.Unlock(ctx)
	require.NoError(t, err)
	requireFileNotExists(t, filepath.Join(pth, "test.lock"))
}

func TestRWLock(t *testing.T) {
	ctx := context.Background()
	strg, path := createMockStorage(t)
	lock, err := storage.TryLockRemoteRead(ctx, strg, "test.lock", "I wanna read it!")
	require.NoError(t, err)
	lock2, err := storage.TryLockRemoteRead(ctx, strg, "test.lock", "I wanna read it too!")
	require.NoError(t, err)
	_, err = storage.TryLockRemoteWrite(ctx, strg, "test.lock", "I wanna write it, you gotta out!")
	require.Error(t, err)
	require.NoError(t, lock.Unlock(ctx))
	require.NoError(t, lock2.Unlock(ctx))
	l, err := storage.TryLockRemoteWrite(ctx, strg, "test.lock", "Can I have a write lock?")
	require.NoError(t, err)
	requireFileExists(t, filepath.Join(path, "test.lock.WRIT"))
	require.NoError(t, l.Unlock(ctx))
	requireFileNotExists(t, filepath.Join(path, "test.lock.WRIT"))
}
