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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

func requireListedPathsWithPrefix(t *testing.T, strg storeapi.Storage, prefix string) []string {
	t.Helper()
	dirName := ""
	fileName := prefix
	if idx := strings.LastIndex(prefix, "/"); idx >= 0 {
		dirName = prefix[:idx]
		fileName = prefix[idx+1:]
	}

	var paths []string
	require.NoError(t, strg.WalkDir(context.Background(), &storeapi.WalkOption{
		SubDir:    dirName,
		ObjPrefix: fileName,
	}, func(p string, _ int64) error {
		paths = append(paths, p)
		return nil
	}))
	return paths
}

func requireSinglePathWithPrefix(t *testing.T, strg storeapi.Storage, prefix string) string {
	t.Helper()
	paths := requireListedPathsWithPrefix(t, strg, prefix)
	require.Len(t, paths, 1)
	return paths[0]
}

func require32HexSuffix(t *testing.T, p, prefix string) {
	t.Helper()
	require.True(t, strings.HasPrefix(p, prefix), "path %q must start with %q", p, prefix)
	suffix := strings.TrimPrefix(p, prefix)
	require.Len(t, suffix, 32)
	_, err := hex.DecodeString(suffix)
	require.NoError(t, err)
}

type sequenceLeaseClock struct {
	times []time.Time
	err   error
	errs  []error
	idx   int
}

func (c *sequenceLeaseClock) Now(context.Context) (time.Time, error) {
	if c.err != nil {
		return time.Time{}, c.err
	}
	if c.idx < len(c.errs) && c.errs[c.idx] != nil {
		err := c.errs[c.idx]
		c.idx++
		return time.Time{}, err
	}
	now := c.times[c.idx]
	c.idx++
	return now, nil
}

type cancelingProofLeaseClock struct {
	leaseNow time.Time
	cancel   context.CancelFunc
	called   bool
}

func (c *cancelingProofLeaseClock) Now(context.Context) (time.Time, error) {
	if !c.called {
		c.called = true
		return c.leaseNow, nil
	}
	c.cancel()
	return time.Time{}, context.Canceled
}

type cancelAwareDeleteStorage struct {
	storeapi.Storage
}

func (s cancelAwareDeleteStorage) DeleteFile(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.Storage.DeleteFile(ctx, name)
}

func TestTryLockRemoteTruncate(t *testing.T) {
	ctx := context.Background()
	strg, pth := createMockStorage(t)
	lock, err := objstore.TryLockRemoteTruncate(ctx, strg, "This file is mine!")
	require.NoError(t, err)
	lockPath := requireSinglePathWithPrefix(t, strg, "truncating.lock.")
	requireFileExists(t, filepath.Join(pth, lockPath))
	err = lock.Unlock(ctx)
	require.NoError(t, err)
	requireFileNotExists(t, filepath.Join(pth, lockPath))
}

func TestConflictLock(t *testing.T) {
	ctx := context.Background()
	strg, pth := createMockStorage(t)
	lock, err := objstore.TryLockRemoteTruncate(ctx, strg, "This file is mine!")
	require.NoError(t, err)
	lockPath := requireSinglePathWithPrefix(t, strg, "truncating.lock.")
	_, err = objstore.TryLockRemoteTruncate(ctx, strg, "This file is mine!")
	require.ErrorContains(t, err, "conflict file")
	requireFileExists(t, filepath.Join(pth, lockPath))
	err = lock.Unlock(ctx)
	require.NoError(t, err)
	requireFileNotExists(t, filepath.Join(pth, lockPath))
}

func TestRWLock(t *testing.T) {
	ctx := context.Background()
	strg, path := createMockStorage(t)
	lock, err := objstore.TryLockRemoteRead(ctx, strg, "v1/LOCK", "I wanna read it!")
	require.NoError(t, err)
	lock2, err := objstore.TryLockRemoteRead(ctx, strg, "v1/LOCK", "I wanna read it too!")
	require.NoError(t, err)
	_, err = objstore.TryLockRemoteWrite(ctx, strg, "v1/LOCK", "I wanna write it, you get out!")
	require.Error(t, err)
	require.NoError(t, lock.Unlock(ctx))
	require.NoError(t, lock2.Unlock(ctx))
	l, err := objstore.TryLockRemoteWrite(ctx, strg, "v1/LOCK", "Can I have a write lock?")
	require.NoError(t, err)
	writePath := requireSinglePathWithPrefix(t, strg, "v1/LOCK.WRIT.")
	require32HexSuffix(t, writePath, "v1/LOCK.WRIT.")
	requireFileExists(t, filepath.Join(path, writePath))
	readWhileWriting, err := objstore.TryLockRemoteRead(ctx, strg, "v1/LOCK", "Can I read while you write?")
	require.ErrorContains(t, err, "conflict file")
	require.Nil(t, readWhileWriting)
	require.NoError(t, l.Unlock(ctx))
	requireFileNotExists(t, filepath.Join(path, writePath))
}

func TestTryLockRemoteTruncateCreatesInstancePath(t *testing.T) {
	ctx := context.Background()
	strg, pth := createMockStorage(t)

	lock, err := objstore.TryLockRemoteTruncate(ctx, strg, "truncate")
	require.NoError(t, err)

	lockPath := requireSinglePathWithPrefix(t, strg, "truncating.lock.")
	require32HexSuffix(t, lockPath, "truncating.lock.")
	requireFileExists(t, filepath.Join(pth, lockPath))
	require.NoError(t, lock.Unlock(ctx))
	requireFileNotExists(t, filepath.Join(pth, lockPath))
}

func TestTryLockRemoteReadWriteCreateInstancePaths(t *testing.T) {
	ctx := context.Background()
	strg, pth := createMockStorage(t)

	readLock, err := objstore.TryLockRemoteRead(ctx, strg, "v1/LOCK", "reader")
	require.NoError(t, err)
	readPath := requireSinglePathWithPrefix(t, strg, "v1/LOCK.READ.")
	require32HexSuffix(t, readPath, "v1/LOCK.READ.")
	requireFileExists(t, filepath.Join(pth, readPath))

	readLock2, err := objstore.TryLockRemoteRead(ctx, strg, "v1/LOCK", "reader 2")
	require.NoError(t, err)
	require.Len(t, requireListedPathsWithPrefix(t, strg, "v1/LOCK.READ."), 2)

	_, err = objstore.TryLockRemoteWrite(ctx, strg, "v1/LOCK", "writer")
	require.ErrorContains(t, err, "conflict file")

	require.NoError(t, readLock.Unlock(ctx))
	require.NoError(t, readLock2.Unlock(ctx))

	writeLock, err := objstore.TryLockRemoteWrite(ctx, strg, "v1/LOCK", "writer")
	require.NoError(t, err)
	writePath := requireSinglePathWithPrefix(t, strg, "v1/LOCK.WRIT.")
	require32HexSuffix(t, writePath, "v1/LOCK.WRIT.")
	requireFileExists(t, filepath.Join(pth, writePath))
	require.NoError(t, writeLock.Unlock(ctx))
	requireFileNotExists(t, filepath.Join(pth, writePath))
}

func TestTryLockRemoteWriteWithLeaseClockUsesClockForMetaAndGeneration(t *testing.T) {
	ctx := context.Background()
	strg, _ := createMockStorage(t)
	leaseNow := time.Date(2026, 5, 28, 10, 11, 12, 123456789, time.UTC)
	clock := &sequenceLeaseClock{
		times: []time.Time{
			leaseNow,
			leaseNow.Add(time.Millisecond),
		},
	}

	lock, err := objstore.TryLockRemoteWriteWithLeaseClock(ctx, strg, "v1/LOCK", "clocked", clock)
	require.NoError(t, err)
	require.NotNil(t, lock)
	t.Cleanup(func() {
		require.NoError(t, lock.Unlock(ctx))
	})

	lockPath := requireSinglePathWithPrefix(t, strg, "v1/LOCK.WRIT.")
	require.True(t, strings.HasPrefix(lockPath, fmt.Sprintf("v1/LOCK.WRIT.%016x", leaseNow.UnixNano())))
	require32HexSuffix(t, lockPath, "v1/LOCK.WRIT.")

	data, err := strg.ReadFile(ctx, lockPath)
	require.NoError(t, err)
	var meta objstore.LockMeta
	require.NoError(t, json.Unmarshal(data, &meta))
	require.Equal(t, leaseNow, meta.LockedAt)
	require.Equal(t, leaseNow.Add(objstore.LeaseTTL), meta.ExpireAt)
}

func TestTryLockRemoteWriteWithLeaseClockFailsWhenInitialTimeFails(t *testing.T) {
	ctx := context.Background()
	strg, _ := createMockStorage(t)
	expectedErr := errors.New("pd tso unavailable")
	clock := &sequenceLeaseClock{err: expectedErr}

	lock, err := objstore.TryLockRemoteWriteWithLeaseClock(ctx, strg, "v1/LOCK", "clocked", clock)
	require.Nil(t, lock)
	require.ErrorIs(t, err, expectedErr)
	require.Empty(t, requireListedPathsWithPrefix(t, strg, "v1/LOCK.WRIT."))
}

func TestTryLockRemoteWriteWithLeaseClockFailsIfAcquireReturnsAfterExpireAt(t *testing.T) {
	ctx := context.Background()
	strg, _ := createMockStorage(t)
	leaseNow := time.Date(2026, 5, 28, 10, 11, 12, 123456789, time.UTC)
	clock := &sequenceLeaseClock{
		times: []time.Time{
			leaseNow,
			leaseNow.Add(objstore.LeaseTTL).Add(time.Nanosecond),
		},
	}

	lock, err := objstore.TryLockRemoteWriteWithLeaseClock(ctx, strg, "v1/LOCK", "clocked", clock)
	require.Nil(t, lock)
	require.ErrorContains(t, err, "lease expired before acquire returned")
	require.Empty(t, requireListedPathsWithPrefix(t, strg, "v1/LOCK.WRIT."))
}

func TestTryLockRemoteWriteWithLeaseClockCleansUpWhenPostAcquireClockFailsWithCanceledContext(t *testing.T) {
	baseCtx := context.Background()
	ctx, cancel := context.WithCancel(baseCtx)
	strg, _ := createMockStorage(t)
	leaseNow := time.Date(2026, 5, 28, 10, 11, 12, 123456789, time.UTC)
	clock := &cancelingProofLeaseClock{
		leaseNow: leaseNow,
		cancel:   cancel,
	}

	lock, err := objstore.TryLockRemoteWriteWithLeaseClock(ctx, cancelAwareDeleteStorage{Storage: strg}, "v1/LOCK", "clocked", clock)
	require.Nil(t, lock)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, requireListedPathsWithPrefix(t, strg, "v1/LOCK.WRIT."))
}

func TestTryLockRemoteReadAndTruncateWithLeaseClockUseClockForMetaAndGeneration(t *testing.T) {
	ctx := context.Background()
	leaseNow := time.Date(2026, 5, 28, 10, 11, 12, 123456789, time.UTC)

	t.Run("read", func(t *testing.T) {
		strg, _ := createMockStorage(t)
		clock := &sequenceLeaseClock{
			times: []time.Time{
				leaseNow,
				leaseNow.Add(time.Millisecond),
			},
		}

		lock, err := objstore.TryLockRemoteReadWithLeaseClock(ctx, strg, "v1/LOCK", "clocked read", clock)
		require.NoError(t, err)
		require.NotNil(t, lock)
		t.Cleanup(func() {
			require.NoError(t, lock.Unlock(ctx))
		})

		lockPath := requireSinglePathWithPrefix(t, strg, "v1/LOCK.READ.")
		require.True(t, strings.HasPrefix(lockPath, fmt.Sprintf("v1/LOCK.READ.%016x", leaseNow.UnixNano())))
		data, err := strg.ReadFile(ctx, lockPath)
		require.NoError(t, err)
		var meta objstore.LockMeta
		require.NoError(t, json.Unmarshal(data, &meta))
		require.Equal(t, leaseNow, meta.LockedAt)
		require.Equal(t, leaseNow.Add(objstore.LeaseTTL), meta.ExpireAt)
	})

	t.Run("truncate", func(t *testing.T) {
		strg, _ := createMockStorage(t)
		clock := &sequenceLeaseClock{
			times: []time.Time{
				leaseNow,
				leaseNow.Add(time.Millisecond),
			},
		}

		lock, err := objstore.TryLockRemoteTruncateWithLeaseClock(ctx, strg, "clocked truncate", clock)
		require.NoError(t, err)
		require.NotNil(t, lock)
		t.Cleanup(func() {
			require.NoError(t, lock.Unlock(ctx))
		})

		lockPath := requireSinglePathWithPrefix(t, strg, "truncating.lock.")
		require.True(t, strings.HasPrefix(lockPath, fmt.Sprintf("truncating.lock.%016x", leaseNow.UnixNano())))
		data, err := strg.ReadFile(ctx, lockPath)
		require.NoError(t, err)
		var meta objstore.LockMeta
		require.NoError(t, json.Unmarshal(data, &meta))
		require.Equal(t, leaseNow, meta.LockedAt)
		require.Equal(t, leaseNow.Add(objstore.LeaseTTL), meta.ExpireAt)
	})
}

func TestTryLockRemoteAppendWriteCreatesInstancePath(t *testing.T) {
	ctx := context.Background()
	strg, pth := createMockStorage(t)

	lock, err := objstore.TryLockRemoteWrite(ctx, strg, "v1/APPEND_LOCK", "append writer")
	require.NoError(t, err)

	lockPath := requireSinglePathWithPrefix(t, strg, "v1/APPEND_LOCK.WRIT.")
	require32HexSuffix(t, lockPath, "v1/APPEND_LOCK.WRIT.")
	requireFileExists(t, filepath.Join(pth, lockPath))
	lock2, err := objstore.TryLockRemoteWrite(ctx, strg, "v1/APPEND_LOCK", "append writer 2")
	require.ErrorContains(t, err, "conflict file")
	require.Nil(t, lock2)
	require.NoError(t, lock.Unlock(ctx))
	requireFileNotExists(t, filepath.Join(pth, lockPath))
}

func TestTryLockRemoteRejectsUnknownLogicalFamily(t *testing.T) {
	ctx := context.Background()
	strg, _ := createMockStorage(t)

	_, err := objstore.TryLockRemoteRead(ctx, strg, "v1/APPEND_LOCK", "bad read")
	require.ErrorContains(t, err, "unknown lock family")

	_, err = objstore.TryLockRemoteWrite(ctx, strg, "v2/LOCK", "bad write")
	require.ErrorContains(t, err, "unknown lock family")
}

func TestTryLockRemoteIgnoresOwnIntent(t *testing.T) {
	ctx := context.Background()
	strg, _ := createMockStorage(t)

	lock, err := objstore.TryLockRemoteWrite(ctx, strg, "v1/LOCK", "writer")
	require.NoError(t, err)
	require.NoError(t, lock.Unlock(ctx))
}

func TestTryLockRemoteReadWriteIntentCompatibility(t *testing.T) {
	ctx := context.Background()
	strg, _ := createMockStorage(t)

	require.NoError(t, strg.WriteFile(ctx, "v1/LOCK.READ.0123456789abcdef0123456789abcdef.INTENT.reader", []byte{}))
	writeLock, err := objstore.TryLockRemoteWrite(ctx, strg, "v1/LOCK", "writer blocked by read intent")
	require.ErrorContains(t, err, "conflict file")
	require.Nil(t, writeLock)

	readLock, err := objstore.TryLockRemoteRead(ctx, strg, "v1/LOCK", "reader compatible with read intent")
	require.NoError(t, err)
	require.NoError(t, readLock.Unlock(ctx))

	require.NoError(t, strg.DeleteFile(ctx, "v1/LOCK.READ.0123456789abcdef0123456789abcdef.INTENT.reader"))
	require.NoError(t, strg.WriteFile(ctx, "v1/LOCK.WRIT.0123456789abcdef0123456789abcdef.INTENT.writer", []byte{}))
	_, err = objstore.TryLockRemoteRead(ctx, strg, "v1/LOCK", "reader blocked by write intent")
	require.ErrorContains(t, err, "conflict file")
}

func TestTryLockRemoteAcquireBlockedByLegacyAndUnknownProtectedMembers(t *testing.T) {
	ctx := context.Background()

	t.Run("legacy read blocks write", func(t *testing.T) {
		strg, _ := createMockStorage(t)
		require.NoError(t, strg.WriteFile(ctx, "v1/LOCK.READ.cafef00d12345678", []byte("{}")))
		lock, err := objstore.TryLockRemoteWrite(ctx, strg, "v1/LOCK", "writer")
		require.ErrorContains(t, err, "conflict file")
		require.Nil(t, lock)
	})

	t.Run("unknown migration object blocks acquire", func(t *testing.T) {
		strg, _ := createMockStorage(t)
		require.NoError(t, strg.WriteFile(ctx, "v1/LOCK.WRITER_NOTES", []byte("{}")))
		lock, err := objstore.TryLockRemoteRead(ctx, strg, "v1/LOCK", "reader")
		require.ErrorContains(t, err, "conflict file")
		require.Nil(t, lock)
	})

	t.Run("legacy write blocks read", func(t *testing.T) {
		strg, _ := createMockStorage(t)
		require.NoError(t, strg.WriteFile(ctx, "v1/LOCK.WRIT", []byte("{}")))
		lock, err := objstore.TryLockRemoteRead(ctx, strg, "v1/LOCK", "reader")
		require.ErrorContains(t, err, "conflict file")
		require.Nil(t, lock)
	})

	t.Run("legacy append write blocks append writer", func(t *testing.T) {
		strg, _ := createMockStorage(t)
		require.NoError(t, strg.WriteFile(ctx, "v1/APPEND_LOCK.WRIT", []byte("{}")))
		lock, err := objstore.TryLockRemoteWrite(ctx, strg, "v1/APPEND_LOCK", "append writer")
		require.ErrorContains(t, err, "conflict file")
		require.Nil(t, lock)
	})

	t.Run("legacy fixed truncate blocks truncate", func(t *testing.T) {
		strg, _ := createMockStorage(t)
		require.NoError(t, strg.WriteFile(ctx, "truncating.lock", []byte("{}")))
		lock, err := objstore.TryLockRemoteTruncate(ctx, strg, "truncate")
		require.ErrorContains(t, err, "conflict file")
		require.Nil(t, lock)
	})

	t.Run("unknown truncate object blocks acquire", func(t *testing.T) {
		strg, _ := createMockStorage(t)
		require.NoError(t, strg.WriteFile(ctx, "truncating.lock.backup", []byte("{}")))
		lock, err := objstore.TryLockRemoteTruncate(ctx, strg, "truncate")
		require.ErrorContains(t, err, "conflict file")
		require.Nil(t, lock)
	})
}

func TestConcurrentLock(t *testing.T) {
	ctx := context.Background()

	type lockResult struct {
		lock *objstore.RemoteLock
		err  error
	}

	runConcurrentAcquire := func(
		t *testing.T,
		lockerA func() (*objstore.RemoteLock, error),
		lockerB func() (*objstore.RemoteLock, error),
		assertWinner func(t *testing.T),
	) {
		t.Helper()
		errChA := make(chan lockResult, 1)
		errChB := make(chan lockResult, 1)

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
		t.Cleanup(func() {
			require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/objstore/exclusive-write-commit-to-1"))
		})
		require.NoError(t, failpoint.EnableCall("github.com/pingcap/tidb/pkg/objstore/exclusive-write-commit-to-2", onceB))
		t.Cleanup(func() {
			require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/objstore/exclusive-write-commit-to-2"))
		})

		go func() {
			lock, err := lockerA()
			errChA <- lockResult{lock: lock, err: err}
		}()

		go func() {
			lock, err := lockerB()
			errChB <- lockResult{lock: lock, err: err}
		}()

		<-chA
		<-chB

		<-chB
		<-chA

		resA := <-errChA
		resB := <-errChB
		if resA.err == nil {
			require.Error(t, resB.err)
			require.NotNil(t, resA.lock)
		} else {
			require.NoError(t, resB.err, "%s", resA.err)
			require.NotNil(t, resB.lock)
		}
		assertWinner(t)
	}

	t.Run("truncate writers", func(t *testing.T) {
		strg, pth := createMockStorage(t)
		runConcurrentAcquire(t,
			func() (*objstore.RemoteLock, error) {
				return objstore.TryLockRemoteTruncate(ctx, strg, "I wanna truncate, but I hesitated before send my intention!")
			},
			func() (*objstore.RemoteLock, error) {
				return objstore.TryLockRemoteTruncate(ctx, strg, "I wanna truncate too, but I hesitated before committing!")
			},
			func(t *testing.T) {
				lockPath := requireSinglePathWithPrefix(t, strg, "truncating.lock.")
				requireFileExists(t, filepath.Join(pth, lockPath))
			},
		)
	})

	t.Run("migration read and write", func(t *testing.T) {
		strg, pth := createMockStorage(t)
		runConcurrentAcquire(t,
			func() (*objstore.RemoteLock, error) {
				return objstore.TryLockRemoteRead(ctx, strg, "v1/LOCK", "reader hesitated before send intention")
			},
			func() (*objstore.RemoteLock, error) {
				return objstore.TryLockRemoteWrite(ctx, strg, "v1/LOCK", "writer hesitated before committing")
			},
			func(t *testing.T) {
				readPaths := requireListedPathsWithPrefix(t, strg, "v1/LOCK.READ.")
				writePaths := requireListedPathsWithPrefix(t, strg, "v1/LOCK.WRIT.")
				require.Equal(t, 1, len(readPaths)+len(writePaths))
				for _, lockPath := range append(readPaths, writePaths...) {
					requireFileExists(t, filepath.Join(pth, lockPath))
				}
			},
		)
	})

	t.Run("append writers", func(t *testing.T) {
		strg, pth := createMockStorage(t)
		runConcurrentAcquire(t,
			func() (*objstore.RemoteLock, error) {
				return objstore.TryLockRemoteWrite(ctx, strg, "v1/APPEND_LOCK", "append writer A")
			},
			func() (*objstore.RemoteLock, error) {
				return objstore.TryLockRemoteWrite(ctx, strg, "v1/APPEND_LOCK", "append writer B")
			},
			func(t *testing.T) {
				writePaths := requireListedPathsWithPrefix(t, strg, "v1/APPEND_LOCK.WRIT.")
				require.Len(t, writePaths, 1)
				requireFileExists(t, filepath.Join(pth, writePaths[0]))
			},
		)
	})

	t.Run("same physical target", func(t *testing.T) {
		strg, pth := createMockStorage(t)
		const physicalPath = "v1/LOCK.WRIT.0123456789abcdef0123456789abcdef"
		runConcurrentAcquire(t,
			func() (*objstore.RemoteLock, error) {
				return objstore.TESTTryLockRemoteExact(ctx, strg, physicalPath, "same target A")
			},
			func() (*objstore.RemoteLock, error) {
				return objstore.TESTTryLockRemoteExact(ctx, strg, physicalPath, "same target B")
			},
			func(t *testing.T) {
				requireFileExists(t, filepath.Join(pth, physicalPath))
				paths := requireListedPathsWithPrefix(t, strg, physicalPath)
				require.Equal(t, []string{physicalPath}, paths)
			},
		)
	})
}

func TestUnlockOnCleanUp(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	strg, pth := createMockStorage(t)
	lock, err := objstore.TryLockRemoteTruncate(ctx, strg, "This file is mine!")
	require.NoError(t, err)
	lockPath := requireSinglePathWithPrefix(t, strg, "truncating.lock.")
	requireFileExists(t, filepath.Join(pth, lockPath))

	cancel()
	lock.UnlockOnCleanUp(ctx)
	requireFileNotExists(t, filepath.Join(pth, lockPath))
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
	restore := objstore.TESTSetLeaseConstants(100*time.Millisecond, 30*time.Millisecond, 3, 5*time.Millisecond)
	require.Equal(t, 100*time.Millisecond, objstore.LeaseTTL, "override must take effect")
	restore()
	require.Equal(t, origTTL, objstore.LeaseTTL, "restore must revert the override")
}

func TestTESTSetNowRestores(t *testing.T) {
	fixed := time.Date(2000, 1, 2, 3, 4, 5, 0, time.UTC)
	restore := objstore.TESTSetNow(func() time.Time { return fixed })
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
	lock, err := objstore.TryLockRemoteWrite(ctx, strg, "v1/LOCK", "owner")
	require.NoError(t, err)
	physicalPath := requireSinglePathWithPrefix(t, strg, "v1/LOCK.WRIT.")

	// Read back original ExpireAt.
	origData, err := strg.ReadFile(ctx, physicalPath)
	require.NoError(t, err)
	var origMeta objstore.LockMeta
	require.NoError(t, json.Unmarshal(origData, &origMeta))

	// Advance clock and renew.
	advanced := origMeta.LockedAt.Add(time.Minute)
	restoreNow := objstore.TESTSetNow(func() time.Time { return advanced })
	defer restoreNow()

	require.NoError(t, objstore.TESTTryRenew(ctx, lock))

	afterPath := requireSinglePathWithPrefix(t, strg, "v1/LOCK.WRIT.")
	require.Equal(t, physicalPath, afterPath, "renewal must refresh the existing physical instance path")
	newData, err := strg.ReadFile(ctx, afterPath)
	require.NoError(t, err)
	var newMeta objstore.LockMeta
	require.NoError(t, json.Unmarshal(newData, &newMeta))
	require.Equal(t, advanced.Add(objstore.LeaseTTL), newMeta.ExpireAt,
		"renewed ExpireAt must be nowFunc + LeaseTTL")
	require.Equal(t, origMeta.TxnID, newMeta.TxnID, "TxnID must stay unchanged across renewal")
}

func TestTryRenewWithLeaseClockUsesOneTimeForExpiryAndRefresh(t *testing.T) {
	ctx := context.Background()
	strg, _ := createMockStorage(t)
	leaseNow := time.Date(2026, 5, 28, 10, 11, 12, 123456789, time.UTC)
	renewNow := leaseNow.Add(time.Minute)
	clock := &sequenceLeaseClock{
		times: []time.Time{
			leaseNow,
			leaseNow.Add(time.Millisecond),
			renewNow,
		},
	}

	lock, err := objstore.TryLockRemoteWriteWithLeaseClock(ctx, strg, "v1/LOCK", "owner", clock)
	require.NoError(t, err)
	physicalPath := requireSinglePathWithPrefix(t, strg, "v1/LOCK.WRIT.")

	restoreNow := objstore.TESTSetNow(func() time.Time { return leaseNow.Add(30 * time.Second) })
	defer restoreNow()

	require.NoError(t, objstore.TESTTryRenew(ctx, lock))

	data, err := strg.ReadFile(ctx, physicalPath)
	require.NoError(t, err)
	var meta objstore.LockMeta
	require.NoError(t, json.Unmarshal(data, &meta))
	require.Equal(t, renewNow.Add(objstore.LeaseTTL), meta.ExpireAt)
	require.Equal(t, 3, clock.idx)
}

func TestTryRenewWithLeaseClockErrorIsTransient(t *testing.T) {
	ctx := context.Background()
	strg, _ := createMockStorage(t)
	leaseNow := time.Date(2026, 5, 28, 10, 11, 12, 123456789, time.UTC)
	expectedErr := errors.New("pd tso unavailable during renewal")
	clock := &sequenceLeaseClock{
		times: []time.Time{
			leaseNow,
			leaseNow.Add(time.Millisecond),
		},
		errs: []error{
			nil,
			nil,
			expectedErr,
		},
	}

	lock, err := objstore.TryLockRemoteWriteWithLeaseClock(ctx, strg, "v1/LOCK", "owner", clock)
	require.NoError(t, err)
	physicalPath := requireSinglePathWithPrefix(t, strg, "v1/LOCK.WRIT.")

	origData, err := strg.ReadFile(ctx, physicalPath)
	require.NoError(t, err)
	var origMeta objstore.LockMeta
	require.NoError(t, json.Unmarshal(origData, &origMeta))

	err = objstore.TESTTryRenew(ctx, lock)
	require.ErrorIs(t, err, expectedErr)
	require.NotErrorIs(t, err, objstore.TESTRenewTxnIDMismatch)
	require.NotErrorIs(t, err, objstore.TESTRenewLeaseExpired)

	newData, err := strg.ReadFile(ctx, physicalPath)
	require.NoError(t, err)
	var newMeta objstore.LockMeta
	require.NoError(t, json.Unmarshal(newData, &newMeta))
	require.Equal(t, origMeta.ExpireAt, newMeta.ExpireAt)
}

func TestTryRenewWithoutLeaseClockFails(t *testing.T) {
	ctx := context.Background()
	strg, _ := createMockStorage(t)
	leaseNow := time.Date(2026, 5, 28, 10, 11, 12, 123456789, time.UTC)
	clock := &sequenceLeaseClock{
		times: []time.Time{
			leaseNow,
			leaseNow.Add(time.Millisecond),
		},
	}

	lock, err := objstore.TryLockRemoteWriteWithLeaseClock(ctx, strg, "v1/LOCK", "owner", clock)
	require.NoError(t, err)
	objstore.TESTClearLeaseClock(lock)

	restoreNow := objstore.TESTSetNow(func() time.Time { return leaseNow.Add(time.Minute) })
	defer restoreNow()

	err = objstore.TESTTryRenew(ctx, lock)
	require.ErrorContains(t, err, "lease clock is required")
}

func TestTryRenewTxnIDMismatch(t *testing.T) {
	ctx := context.Background()
	strg, _ := createMockStorage(t)
	lock, err := objstore.TryLockRemoteWrite(ctx, strg, "v1/LOCK", "owner")
	require.NoError(t, err)
	physicalPath := requireSinglePathWithPrefix(t, strg, "v1/LOCK.WRIT.")

	// Overwrite the lock file with a different TxnID, simulating that another
	// process has reclaimed and re-acquired the lock.
	hijacked := objstore.MakeLockMeta("hijacker")
	hijacked.TxnID = []byte{99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99}
	hijackedData, err := json.Marshal(hijacked)
	require.NoError(t, err)
	require.NoError(t, strg.WriteFile(ctx, physicalPath, hijackedData))

	err = objstore.TESTTryRenew(ctx, lock)
	require.ErrorIs(t, err, objstore.TESTRenewTxnIDMismatch)
}

func TestTryRenewLeaseExpired(t *testing.T) {
	ctx := context.Background()
	strg, _ := createMockStorage(t)
	lock, err := objstore.TryLockRemoteWrite(ctx, strg, "v1/LOCK", "owner")
	require.NoError(t, err)
	physicalPath := requireSinglePathWithPrefix(t, strg, "v1/LOCK.WRIT.")

	// Advance the clock far past the lease expiry.
	origData, err := strg.ReadFile(ctx, physicalPath)
	require.NoError(t, err)
	var origMeta objstore.LockMeta
	require.NoError(t, json.Unmarshal(origData, &origMeta))

	wayLate := origMeta.ExpireAt.Add(time.Hour)
	restoreNow := objstore.TESTSetNow(func() time.Time { return wayLate })
	defer restoreNow()

	err = objstore.TESTTryRenew(ctx, lock)
	require.ErrorIs(t, err, objstore.TESTRenewLeaseExpired)
}

func TestTryRenewWriteError(t *testing.T) {
	ctx := context.Background()
	strg, _ := createMockStorage(t)
	lock, err := objstore.TryLockRemoteWrite(ctx, strg, "v1/LOCK", "owner")
	require.NoError(t, err)

	require.NoError(t, failpoint.Enable(
		"github.com/pingcap/tidb/pkg/objstore/local_write_file_err",
		`return("simulated S3 outage")`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/objstore/local_write_file_err"))
	}()

	err = objstore.TESTTryRenew(ctx, lock)
	require.Error(t, err)
	require.NotErrorIs(t, err, objstore.TESTRenewTxnIDMismatch)
	require.NotErrorIs(t, err, objstore.TESTRenewLeaseExpired)
	require.Contains(t, err.Error(), "simulated S3 outage")
}

func TestStartRenewalRefreshesLeasePeriodically(t *testing.T) {
	ctx := context.Background()
	strg, _ := createMockStorage(t)
	defer objstore.TESTSetLeaseConstants(200*time.Millisecond, 30*time.Millisecond, 3, 5*time.Millisecond)()

	lock, err := objstore.TryLockRemoteWrite(ctx, strg, "v1/LOCK", "owner")
	require.NoError(t, err)
	defer objstore.TESTStopRenewal(lock)
	physicalPath := requireSinglePathWithPrefix(t, strg, "v1/LOCK.WRIT.")

	// Capture original ExpireAt.
	getExpireAt := func() time.Time {
		data, err := strg.ReadFile(ctx, physicalPath)
		require.NoError(t, err)
		var meta objstore.LockMeta
		require.NoError(t, json.Unmarshal(data, &meta))
		return meta.ExpireAt
	}
	orig := getExpireAt()

	objstore.TESTStartRenewal(ctx, lock, nil)

	// Wait for at least two renewal ticks (interval=30ms → 100ms is plenty).
	require.Eventually(t, func() bool {
		return requireSinglePathWithPrefix(t, strg, "v1/LOCK.WRIT.") == physicalPath && getExpireAt().After(orig)
	}, 500*time.Millisecond, 10*time.Millisecond,
		"ExpireAt should advance past the original within a few renewal ticks")
}

func TestStartRenewalCallsOnLeaseLostOnTxnIDMismatch(t *testing.T) {
	ctx := context.Background()
	strg, _ := createMockStorage(t)
	defer objstore.TESTSetLeaseConstants(200*time.Millisecond, 20*time.Millisecond, 3, 5*time.Millisecond)()

	lock, err := objstore.TryLockRemoteWrite(ctx, strg, "v1/LOCK", "owner")
	require.NoError(t, err)
	defer objstore.TESTStopRenewal(lock)
	physicalPath := requireSinglePathWithPrefix(t, strg, "v1/LOCK.WRIT.")

	lostCh := make(chan struct{}, 1)
	objstore.TESTStartRenewal(ctx, lock, func() { lostCh <- struct{}{} })

	// Hijack the lock with a different TxnID.
	hijacked := objstore.MakeLockMeta("hijacker")
	hijacked.TxnID = []byte{99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99}
	hijackedData, err := json.Marshal(hijacked)
	require.NoError(t, err)
	require.NoError(t, strg.WriteFile(ctx, physicalPath, hijackedData))

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
	defer objstore.TESTSetLeaseConstants(1*time.Second, 20*time.Millisecond, 5, 5*time.Millisecond)()

	lock, err := objstore.TryLockRemoteWrite(ctx, strg, "v1/LOCK", "owner")
	require.NoError(t, err)
	defer objstore.TESTStopRenewal(lock)

	require.NoError(t, failpoint.Enable(
		"github.com/pingcap/tidb/pkg/objstore/local_write_file_err",
		`return("simulated outage")`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/objstore/local_write_file_err"))
	}()

	lostCh := make(chan struct{}, 1)
	objstore.TESTStartRenewal(ctx, lock, func() { lostCh <- struct{}{} })

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
	defer objstore.TESTSetLeaseConstants(1*time.Second, 100*time.Millisecond, 3, 5*time.Millisecond)()

	lock, err := objstore.TryLockRemoteWrite(ctx, strg, "v1/LOCK", "owner")
	require.NoError(t, err)
	defer objstore.TESTStopRenewal(lock)

	objstore.TESTStartRenewal(ctx, lock, nil)
	require.Panics(t, func() {
		objstore.TESTStartRenewal(ctx, lock, nil)
	}, "second StartRenewal must panic")
}

func TestUnlockWithoutRenewalIsUnchanged(t *testing.T) {
	ctx := context.Background()
	strg, pth := createMockStorage(t)
	lock, err := objstore.TryLockRemoteWrite(ctx, strg, "v1/LOCK", "owner")
	require.NoError(t, err)
	physicalPath := requireSinglePathWithPrefix(t, strg, "v1/LOCK.WRIT.")
	requireFileExists(t, filepath.Join(pth, physicalPath))

	// No StartRenewal called; Unlock should still work.
	require.NoError(t, lock.Unlock(ctx))
	requireFileNotExists(t, filepath.Join(pth, physicalPath))
}

func TestUnlockWaitsForRenewalGoroutine(t *testing.T) {
	ctx := context.Background()
	strg, pth := createMockStorage(t)
	defer objstore.TESTSetLeaseConstants(1*time.Second, 30*time.Millisecond, 3, 5*time.Millisecond)()

	lock, err := objstore.TryLockRemoteWrite(ctx, strg, "v1/LOCK", "owner")
	require.NoError(t, err)
	physicalPath := requireSinglePathWithPrefix(t, strg, "v1/LOCK.WRIT.")
	objstore.TESTStartRenewal(ctx, lock, nil)

	// Let renewal run for a few ticks so a WriteFile is likely in flight.
	time.Sleep(60 * time.Millisecond)

	// Unlock must wait for the renewal goroutine to exit before deleting.
	// After Unlock returns, no further WriteFile can happen, so the lock
	// file stays deleted.
	require.NoError(t, lock.Unlock(ctx))
	requireFileNotExists(t, filepath.Join(pth, physicalPath))

	// Give any (incorrectly-still-running) goroutine a moment to misbehave.
	time.Sleep(60 * time.Millisecond)
	requireFileNotExists(t, filepath.Join(pth, physicalPath))

	require.NotPanics(t, func() {
		err = lock.Unlock(ctx)
	})
	require.Error(t, err)
}

// writeLockMeta is a small test helper that places a custom LockMeta JSON at a
// given path on the storage, simulating what a (possibly long-dead) holder
// would have written. Used by stale-lock cleanup tests to plant scenarios.
func writeLockMeta(t *testing.T, strg storeapi.Storage, path string, meta objstore.LockMeta) {
	t.Helper()
	data, err := json.Marshal(meta)
	require.NoError(t, err)
	require.NoError(t, strg.WriteFile(context.Background(), path, data))
}

func staleLockMeta(now time.Time, hint string) objstore.LockMeta {
	return objstore.LockMeta{
		LockedAt: now.Add(-time.Hour),
		ExpireAt: now.Add(-time.Hour),
		TxnID:    []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		Hint:     hint,
	}
}

func TestCleanUpStaleTruncateLockOverdueWithinTTL(t *testing.T) {
	ctx := context.Background()
	strg, pth := createMockStorage(t)
	defer objstore.TESTSetLeaseConstants(100*time.Millisecond, 30*time.Millisecond, 3, 5*time.Millisecond)()

	t.Run("missing lock is no-op", func(t *testing.T) {
		reclaimed, err := objstore.CleanUpStaleTruncateLock(ctx, strg)
		require.NoError(t, err)
		require.False(t, reclaimed, "missing lock means there is nothing to clean up")
	})

	// Plant a lock that expired 30ms ago. With LeaseTTL=100ms, the lock is
	// not reclaimable until ExpireAt+LeaseTTL.
	now := time.Now()
	defer objstore.TESTSetNow(func() time.Time { return now })()
	lockPath := "truncating.lock.0123456789abcdef0123456789abcdef"
	writeLockMeta(t, strg, lockPath, objstore.LockMeta{
		LockedAt: now.Add(-2 * time.Minute),
		ExpireAt: now.Add(-30 * time.Millisecond),
		TxnID:    []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		Hint:     "stale-overdue-within-ttl",
	})
	requireFileExists(t, filepath.Join(pth, lockPath))

	start := time.Now()
	reclaimed, err := objstore.CleanUpStaleTruncateLock(ctx, strg)
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.False(t, reclaimed, "lock must not be reclaimed until ExpireAt+LeaseTTL")
	requireFileExists(t, filepath.Join(pth, lockPath))
	require.Less(t, elapsed, 30*time.Millisecond, "cleanup should not wait for the grace window")
}

func TestCleanUpStaleTruncateLockOverduePastTTL(t *testing.T) {
	ctx := context.Background()
	strg, pth := createMockStorage(t)
	defer objstore.TESTSetLeaseConstants(100*time.Millisecond, 30*time.Millisecond, 3, 5*time.Millisecond)()

	// Lock expired well over a full TTL ago — no wait needed.
	now := time.Now()
	lockPath := "truncating.lock.0123456789abcdef0123456789abcdef"
	writeLockMeta(t, strg, lockPath, objstore.LockMeta{
		LockedAt: now.Add(-time.Hour),
		ExpireAt: now.Add(-time.Hour),
		TxnID:    []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		Hint:     "stale-overdue-past-ttl",
	})

	start := time.Now()
	reclaimed, err := objstore.CleanUpStaleTruncateLock(ctx, strg)
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.True(t, reclaimed)
	requireFileNotExists(t, filepath.Join(pth, lockPath))
	require.Less(t, elapsed, 30*time.Millisecond, "deeply overdue locks should reclaim immediately")
}

func TestCleanUpStaleTruncateLockAlive(t *testing.T) {
	ctx := context.Background()
	strg, pth := createMockStorage(t)
	defer objstore.TESTSetLeaseConstants(100*time.Millisecond, 30*time.Millisecond, 3, 5*time.Millisecond)()

	// Fresh lock with ExpireAt comfortably in the future.
	now := time.Now()
	lockPath := "truncating.lock.0123456789abcdef0123456789abcdef"
	writeLockMeta(t, strg, lockPath, objstore.LockMeta{
		LockedAt: now,
		ExpireAt: now.Add(50 * time.Millisecond),
		TxnID:    []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		Hint:     "alive",
	})

	reclaimed, err := objstore.CleanUpStaleTruncateLock(ctx, strg)
	require.NoError(t, err)
	require.False(t, reclaimed, "alive lock must NOT be reclaimed")
	requireFileExists(t, filepath.Join(pth, lockPath))
}

func TestCleanUpStaleTruncateLockZeroExpireAt(t *testing.T) {
	ctx := context.Background()
	strg, pth := createMockStorage(t)

	// A generated instance without ExpireAt is malformed for the new protocol.
	// Cleanup must keep it so acquire can still fail safely with a blocker path.
	lockPath := "truncating.lock.0123456789abcdef0123456789abcdef"
	writeLockMeta(t, strg, lockPath, objstore.LockMeta{
		LockedAt: time.Now().Add(-time.Hour),
		TxnID:    []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		Hint:     "zero-expire-truncate",
	})

	reclaimed, err := objstore.CleanUpStaleTruncateLock(ctx, strg)
	require.NoError(t, err)
	require.False(t, reclaimed, "zero ExpireAt instance locks must NOT be auto-reclaimed")
	requireFileExists(t, filepath.Join(pth, lockPath))
}

func TestCleanUpStaleTruncateLockRefreshedDuringWait(t *testing.T) {
	ctx := context.Background()
	strg, pth := createMockStorage(t)
	defer objstore.TESTSetLeaseConstants(100*time.Millisecond, 30*time.Millisecond, 3, 5*time.Millisecond)()

	// Plant a lock that expired 20ms ago. It is past ExpireAt but still
	// inside the extra LeaseTTL grace window, so cleanup must not delete it.
	now := time.Now()
	lockPath := "truncating.lock.0123456789abcdef0123456789abcdef"
	writeLockMeta(t, strg, lockPath, objstore.LockMeta{
		LockedAt: now.Add(-time.Minute),
		ExpireAt: now.Add(-20 * time.Millisecond),
		TxnID:    []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		Hint:     "to-be-refreshed",
	})

	reclaimed, err := objstore.CleanUpStaleTruncateLock(ctx, strg)
	require.NoError(t, err)
	require.False(t, reclaimed, "reclaim must wait until ExpireAt+LeaseTTL has passed")
	requireFileExists(t, filepath.Join(pth, lockPath))
}

func TestCleanUpStaleTruncateLockReclaimsOnlyInstancePath(t *testing.T) {
	ctx := context.Background()
	strg, pth := createMockStorage(t)
	defer objstore.TESTSetLeaseConstants(100*time.Millisecond, 30*time.Millisecond, 3, 5*time.Millisecond)()

	now := time.Now()
	instancePath := "truncating.lock.0123456789abcdef0123456789abcdef"
	aliveInstancePath := "truncating.lock.33333333333333333333333333333333"
	malformedPath := "truncating.lock.11111111111111111111111111111111"
	zeroExpirePath := "truncating.lock.22222222222222222222222222222222"
	writeLockMeta(t, strg, instancePath, staleLockMeta(now, "stale-truncate-instance"))
	writeLockMeta(t, strg, aliveInstancePath, objstore.LockMeta{
		LockedAt: now,
		ExpireAt: now.Add(time.Hour),
		TxnID:    []byte{16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1},
		Hint:     "alive-truncate-instance",
	})
	require.NoError(t, strg.WriteFile(ctx, malformedPath, []byte("{not-json")))
	writeLockMeta(t, strg, zeroExpirePath, objstore.LockMeta{
		LockedAt: now.Add(-time.Hour),
		TxnID:    []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		Hint:     "zero-expire-truncate",
	})
	writeLockMeta(t, strg, "truncating.lock", staleLockMeta(now, "old-fixed-truncate"))
	writeLockMeta(t, strg, "truncating.lock.backup", staleLockMeta(now, "unknown-protected-truncate"))

	reclaimed, err := objstore.CleanUpStaleTruncateLock(ctx, strg)
	require.NoError(t, err)
	require.True(t, reclaimed)
	requireFileNotExists(t, filepath.Join(pth, instancePath))
	requireFileExists(t, filepath.Join(pth, aliveInstancePath))
	requireFileExists(t, filepath.Join(pth, malformedPath))
	requireFileExists(t, filepath.Join(pth, zeroExpirePath))
	requireFileExists(t, filepath.Join(pth, "truncating.lock"))
	requireFileExists(t, filepath.Join(pth, "truncating.lock.backup"))
}

func TestLockWithRetryReclaimsStaleWriteLock(t *testing.T) {
	ctx := context.Background()
	strg, pth := createMockStorage(t)
	defer objstore.TESTSetLeaseConstants(100*time.Millisecond, 30*time.Millisecond, 3, 5*time.Millisecond)()

	now := time.Now()
	instancePath := "v1/LOCK.WRIT.0123456789abcdef0123456789abcdef"
	writeLockMeta(t, strg, instancePath, staleLockMeta(now, "crashed-prior-truncate"))
	requireFileExists(t, filepath.Join(pth, instancePath))

	shortCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	lock, err := objstore.LockWithRetry(shortCtx, objstore.TryLockRemoteWrite, strg, "v1/LOCK", "new truncate", func() {})
	require.Error(t, err)
	require.Nil(t, lock)
	requireFileNotExists(t, filepath.Join(pth, instancePath))
}

func TestLockWithRetryRejectsNilOnLeaseLost(t *testing.T) {
	ctx := context.Background()
	strg, pth := createMockStorage(t)

	lock, err := objstore.LockWithRetry(ctx, objstore.TryLockRemoteWrite, strg, "v1/LOCK", "owner", nil)
	require.Nil(t, lock)
	require.EqualError(t, err, "onLeaseLost callback is required for lease lock renewal")
	requireFileNotExists(t, filepath.Join(pth, "v1/LOCK.WRIT."))
	require.Empty(t, requireListedPathsWithPrefix(t, strg, "v1/LOCK.WRIT."))
}

func TestLockRemoteTruncateRejectsNilOnLeaseLost(t *testing.T) {
	ctx := context.Background()
	strg, pth := createMockStorage(t)

	lock, err := objstore.LockRemoteTruncate(ctx, strg, "truncate", nil)
	require.Nil(t, lock)
	require.EqualError(t, err, "onLeaseLost callback is required for lease lock renewal")
	requireFileNotExists(t, filepath.Join(pth, "truncating.lock."))
	require.Empty(t, requireListedPathsWithPrefix(t, strg, "truncating.lock."))
}

func TestLockRemoteTruncateStartsRenewal(t *testing.T) {
	ctx := context.Background()
	strg, _ := createMockStorage(t)
	defer objstore.TESTSetLeaseConstants(200*time.Millisecond, 30*time.Millisecond, 3, 5*time.Millisecond)()

	lock, err := objstore.LockRemoteTruncate(ctx, strg, "truncate", func() {})
	require.NoError(t, err)
	defer objstore.TESTStopRenewal(lock)

	physicalPath := requireSinglePathWithPrefix(t, strg, "truncating.lock.")
	getExpireAt := func() time.Time {
		data, err := strg.ReadFile(ctx, physicalPath)
		require.NoError(t, err)
		var meta objstore.LockMeta
		require.NoError(t, json.Unmarshal(data, &meta))
		return meta.ExpireAt
	}
	orig := getExpireAt()

	require.Eventually(t, func() bool {
		return requireSinglePathWithPrefix(t, strg, "truncating.lock.") == physicalPath && getExpireAt().After(orig)
	}, 500*time.Millisecond, 10*time.Millisecond,
		"ExpireAt should advance because LockRemoteTruncate starts renewal")
}

func TestLockWithRetryStartsRenewal(t *testing.T) {
	ctx := context.Background()
	strg, _ := createMockStorage(t)
	defer objstore.TESTSetLeaseConstants(200*time.Millisecond, 30*time.Millisecond, 3, 5*time.Millisecond)()

	lock, err := objstore.LockWithRetry(ctx, objstore.TryLockRemoteWrite, strg, "v1/LOCK", "owner", func() {})
	require.NoError(t, err)
	defer objstore.TESTStopRenewal(lock)

	physicalPath := requireSinglePathWithPrefix(t, strg, "v1/LOCK.WRIT.")
	getExpireAt := func() time.Time {
		data, err := strg.ReadFile(ctx, physicalPath)
		require.NoError(t, err)
		var meta objstore.LockMeta
		require.NoError(t, json.Unmarshal(data, &meta))
		return meta.ExpireAt
	}
	orig := getExpireAt()

	require.Eventually(t, func() bool {
		return requireSinglePathWithPrefix(t, strg, "v1/LOCK.WRIT.") == physicalPath && getExpireAt().After(orig)
	}, 500*time.Millisecond, 10*time.Millisecond,
		"ExpireAt should advance because LockWithRetry starts renewal")
}

func TestLockWithRetryReclaimsStaleReadLockBeforeWrite(t *testing.T) {
	ctx := context.Background()
	strg, pth := createMockStorage(t)
	defer objstore.TESTSetLeaseConstants(100*time.Millisecond, 30*time.Millisecond, 3, 5*time.Millisecond)()

	now := time.Now()
	instancePath := "v1/LOCK.READ.0123456789abcdef0123456789abcdef"
	oldReadPath := "v1/LOCK.READ.cafef00d12345678"
	writeLockMeta(t, strg, instancePath, staleLockMeta(now, "crashed-prior-restore"))
	writeLockMeta(t, strg, oldReadPath, staleLockMeta(now, "old-16hex-reader"))

	// Without reclaim the write would fail (stale read blocks write). The
	// cleanup pass should delete it, then fall through to ordinary backoff.
	shortCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	lock, err := objstore.LockWithRetry(shortCtx, objstore.TryLockRemoteWrite, strg, "v1/LOCK", "new compaction", func() {})
	require.Error(t, err)
	require.Nil(t, lock)
	requireFileNotExists(t, filepath.Join(pth, instancePath))
	requireFileExists(t, filepath.Join(pth, oldReadPath))

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
		}, strg, "v2/LOCK", "still blocked", func() {})
		require.Error(t, err)
		require.Nil(t, lock)
		require.Equal(t, int32(1), attempts.Load())
	})
}

func TestLockWithRetryReclaimsStaleAppendWriteLock(t *testing.T) {
	ctx := context.Background()
	strg, pth := createMockStorage(t)
	defer objstore.TESTSetLeaseConstants(100*time.Millisecond, 30*time.Millisecond, 3, 5*time.Millisecond)()

	now := time.Now()
	instancePath := "v1/APPEND_LOCK.WRIT.0123456789abcdef0123456789abcdef"
	writeLockMeta(t, strg, instancePath, staleLockMeta(now, "stale-append-writer"))

	shortCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	lock, err := objstore.LockWithRetry(shortCtx, objstore.TryLockRemoteWrite, strg, "v1/APPEND_LOCK", "append writer", func() {})
	require.Error(t, err)
	require.Nil(t, lock)
	requireFileNotExists(t, filepath.Join(pth, instancePath))
}

func TestLockWithRetryDoesNotReclaimFixedIntentOrUnknownProtectedObjects(t *testing.T) {
	ctx := context.Background()
	strg, pth := createMockStorage(t)
	defer objstore.TESTSetLeaseConstants(100*time.Millisecond, 30*time.Millisecond, 3, 5*time.Millisecond)()

	now := time.Now()
	paths := []string{
		"v1/LOCK.WRIT",
		"v1/APPEND_LOCK.WRIT",
		"v1/LOCK.WRIT.0123456789abcdef0123456789abcdef.INTENT.writer",
		"v1/LOCK.WRITER_NOTES",
	}
	for _, p := range paths {
		writeLockMeta(t, strg, p, staleLockMeta(now, p))
	}

	shortCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	lock, err := objstore.LockWithRetry(shortCtx, objstore.TryLockRemoteWrite, strg, "v1/LOCK", "writer", func() {})
	require.Error(t, err)
	require.Nil(t, lock)
	for _, p := range paths {
		requireFileExists(t, filepath.Join(pth, p))
	}

	appendCtx, appendCancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer appendCancel()
	lock, err = objstore.LockWithRetry(appendCtx, objstore.TryLockRemoteWrite, strg, "v1/APPEND_LOCK", "append writer", func() {})
	require.Error(t, err)
	require.Nil(t, lock)
	requireFileExists(t, filepath.Join(pth, "v1/APPEND_LOCK.WRIT"))
}

func TestLockWithRetryMalformedCleanupCandidateDoesNotBlockLaterReclaim(t *testing.T) {
	ctx := context.Background()
	strg, pth := createMockStorage(t)
	defer objstore.TESTSetLeaseConstants(100*time.Millisecond, 30*time.Millisecond, 3, 5*time.Millisecond)()

	malformedPath := "v1/LOCK.WRIT.0123456789abcdef0123456789abcdef"
	zeroExpirePath := "v1/LOCK.WRIT.11111111111111111111111111111111"
	stalePath := "v1/LOCK.WRIT.22222222222222222222222222222222"
	require.NoError(t, strg.WriteFile(ctx, malformedPath, []byte("{not-json")))
	writeLockMeta(t, strg, zeroExpirePath, objstore.LockMeta{
		LockedAt: time.Now().Add(-time.Hour),
		TxnID:    []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		Hint:     "zero-expire",
	})
	writeLockMeta(t, strg, stalePath, staleLockMeta(time.Now(), "later-stale-writer"))

	shortCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	lock, err := objstore.LockWithRetry(shortCtx, objstore.TryLockRemoteWrite, strg, "v1/LOCK", "writer", func() {})
	require.Error(t, err)
	require.Nil(t, lock)
	requireFileExists(t, filepath.Join(pth, malformedPath))
	requireFileExists(t, filepath.Join(pth, zeroExpirePath))
	requireFileNotExists(t, filepath.Join(pth, stalePath))
}

func TestLockWithRetryDoesNotReclaimAliveLock(t *testing.T) {
	ctx := context.Background()
	strg, _ := createMockStorage(t)
	defer objstore.TESTSetLeaseConstants(100*time.Millisecond, 30*time.Millisecond, 3, 5*time.Millisecond)()

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
		l, err := objstore.LockWithRetry(shortCtx, objstore.TryLockRemoteWrite, strg, "v1/LOCK", "wants write", func() {})
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
	defer objstore.TESTSetLeaseConstants(100*time.Millisecond, 30*time.Millisecond, 3, 5*time.Millisecond)()

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
		l, err := objstore.LockWithRetry(shortCtx, objstore.TryLockRemoteWrite, strg, "v1/LOCK", "wants write", func() {})
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
