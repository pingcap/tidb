// Copyright 2023 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"context"
	"encoding/json"
	"fmt"
<<<<<<< HEAD
=======
	"math"
	"math/rand"
>>>>>>> c9215ec93ce (br: copy full backup to pitr storage (#57716))
	"os"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
<<<<<<< HEAD
=======
	"github.com/pingcap/tidb/br/pkg/utils"
	"go.uber.org/multierr"
>>>>>>> c9215ec93ce (br: copy full backup to pitr storage (#57716))
	"go.uber.org/zap"
)

// LockMeta is the meta information of a lock.
type LockMeta struct {
	LockedAt   time.Time `json:"locked_at"`
	LockerHost string    `json:"locker_host"`
	LockerPID  int       `json:"locker_pid"`
	Hint       string    `json:"hint"`
}

func (l LockMeta) String() string {
	return fmt.Sprintf("Locked(at: %s, host: %s, pid: %d, hint: %s)", l.LockedAt.Format(time.DateTime), l.LockerHost, l.LockerPID, l.Hint)
}

// ErrLocked is the error returned when the lock is held by others.
type ErrLocked struct {
	Meta LockMeta
}

func (e ErrLocked) Error() string {
	return fmt.Sprintf("locked, meta = %s", e.Meta)
}

// MakeLockMeta creates a LockMeta by the current node's metadata.
// Including current time and hostname, etc..
func MakeLockMeta(hint string) LockMeta {
	hname, err := os.Hostname()
	if err != nil {
		hname = fmt.Sprintf("UnknownHost(err=%s)", err)
	}
	now := time.Now()
	meta := LockMeta{
		LockedAt:   now,
		LockerHost: hname,
		Hint:       hint,
		LockerPID:  os.Getpid(),
	}
	return meta
}

func readLockMeta(ctx context.Context, storage ExternalStorage, path string) (LockMeta, error) {
	file, err := storage.ReadFile(ctx, path)
	if err != nil {
		return LockMeta{}, errors.Annotatef(err, "failed to read existed lock file %s", path)
	}
	meta := LockMeta{}
	err = json.Unmarshal(file, &meta)
	if err != nil {
		return meta, errors.Annotatef(err, "failed to parse lock file %s", path)
	}

	return meta, nil
}

<<<<<<< HEAD
func putLockMeta(ctx context.Context, storage ExternalStorage, path string, meta LockMeta) error {
	file, err := json.Marshal(meta)
=======
type RemoteLock struct {
	txnID   uuid.UUID
	storage ExternalStorage
	path    string
}

func (l *RemoteLock) String() string {
	return fmt.Sprintf("{path=%s,uuid=%s,storage_uri=%s}", l.path, l.txnID, l.storage.URI())
}

func tryFetchRemoteLock(ctx context.Context, storage ExternalStorage, path string) error {
	meta, err := readLockMeta(ctx, storage, path)
>>>>>>> c9215ec93ce (br: copy full backup to pitr storage (#57716))
	if err != nil {
		return errors.Annotatef(err, "failed to marshal lock meta %s", path)
	}
	err = storage.WriteFile(ctx, path, file)
	if err != nil {
		return errors.Annotatef(err, "failed to write lock meta at %s", path)
	}
	return nil
}

// TryLockRemote tries to create a "lock file" at the external storage.
// If success, we will create a file at the path provided. So others may not access the file then.
// Will return a `ErrLocked` if there is another process already creates the lock file.
// This isn't a strict lock like flock in linux: that means, the lock might be forced removed by
// manually deleting the "lock file" in external storage.
func TryLockRemote(ctx context.Context, storage ExternalStorage, path, hint string) (err error) {
	defer func() {
		log.Info("Trying lock remote file.", zap.String("path", path), zap.String("hint", hint), logutil.ShortError(err))
	}()
	exists, err := storage.FileExists(ctx, path)
	if err != nil {
		return errors.Annotatef(err, "failed to check lock file %s exists", path)
	}
	if exists {
		meta, err := readLockMeta(ctx, storage, path)
		if err != nil {
			return err
		}
		return ErrLocked{Meta: meta}
	}

	meta := MakeLockMeta(hint)
	return putLockMeta(ctx, storage, path, meta)
}

// UnlockRemote removes the lock file at the specified path.
// Removing that file will release the lock.
func UnlockRemote(ctx context.Context, storage ExternalStorage, path string) error {
	meta, err := readLockMeta(ctx, storage, path)
	if err != nil {
		return err
	}
	// NOTE: this is for debug usage. For now, there isn't an Compare-And-Swap
	// operation in our ExternalStorage abstraction.
	// So, once our lock has been overwritten or we are overwriting other's lock,
	// this information will be useful for troubleshooting.
	log.Info("Releasing lock.", zap.Stringer("meta", meta), zap.String("path", path))
	err = storage.DeleteFile(ctx, path)
	if err != nil {
		return errors.Annotatef(err, "failed to delete lock file %s", path)
	}
	return nil
}
<<<<<<< HEAD
=======

func (l RemoteLock) UnlockOnCleanUp(ctx context.Context) {
	const cleanUpContextTimeOut = 30 * time.Second

	if ctx.Err() != nil {
		logutil.CL(ctx).Warn("Unlocking but the context was done. Use the background context with a deadline.",
			logutil.AShortError("ctx-err", ctx.Err()))
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), cleanUpContextTimeOut)
		defer cancel()
	}

	if err := l.Unlock(ctx); err != nil {
		logutil.CL(ctx).Warn("Failed to unlock a lock, you may need to manually delete it.",
			zap.Stringer("lock", &l), zap.Int("pid", os.Getpid()), logutil.ShortError(err))
	}
}

func writeLockName(path string) string {
	return fmt.Sprintf("%s.WRIT", path)
}

func newReadLockName(path string) string {
	readID := rand.Int63()
	return fmt.Sprintf("%s.READ.%016x", path, readID)
}

type Locker = func(ctx context.Context, storage ExternalStorage, path, hint string) (lock RemoteLock, err error)

func LockWith(ctx context.Context, locker Locker, storage ExternalStorage, path, hint string) (lock RemoteLock, err error) {
	const JitterMs = 5000

	retry := utils.InitialRetryState(math.MaxInt, 1*time.Second, 60*time.Second)
	jitter := time.Duration(rand.Uint32()%JitterMs+(JitterMs/2)) * time.Millisecond
	for {
		lock, err = locker(ctx, storage, path, hint)
		if err == nil {
			return lock, nil
		}
		retryAfter := retry.ExponentialBackoff() + jitter
		log.Info(
			"Encountered lock, will retry then.",
			logutil.ShortError(err),
			zap.String("path", path),
			zap.Duration("retry-after", retryAfter),
		)

		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		case <-time.After(retryAfter):
		}
	}
}

func TryLockRemoteWrite(ctx context.Context, storage ExternalStorage, path, hint string) (lock RemoteLock, err error) {
	target := writeLockName(path)
	writer := conditionalPut{
		Target: target,
		Content: func(txnID uuid.UUID) []byte {
			meta := MakeLockMeta(hint)
			meta.TxnID = txnID[:]
			res, err := json.Marshal(meta)
			if err != nil {
				log.Panic(
					"Unreachable: a plain object cannot be marshaled to JSON.",
					zap.String("path", path),
					logutil.ShortError(err),
				)
			}
			return res
		},
		Verify: func(ctx VerifyWriteContext) error {
			return ctx.assertNoOtherOfPrefixExpect(path, ctx.IntentFileName())
		},
	}

	lock.storage = storage
	lock.path = target
	lock.txnID, err = writer.CommitTo(ctx, storage)
	if err != nil {
		err = errors.Annotatef(err, "there is something about the lock: %s", tryFetchRemoteLock(ctx, storage, target))
	}
	return
}

func TryLockRemoteRead(ctx context.Context, storage ExternalStorage, path, hint string) (lock RemoteLock, err error) {
	target := newReadLockName(path)
	writeLock := writeLockName(path)
	writer := conditionalPut{
		Target: target,
		Content: func(txnID uuid.UUID) []byte {
			meta := MakeLockMeta(hint)
			meta.TxnID = txnID[:]
			res, err := json.Marshal(meta)
			if err != nil {
				log.Panic(
					"Unreachable: a trivial object cannot be marshaled to JSON.",
					zap.String("path", path),
					logutil.ShortError(err),
				)
			}
			return res
		},
		Verify: func(ctx VerifyWriteContext) error {
			return ctx.assertNoOtherOfPrefixExpect(writeLock, "")
		},
	}

	lock.storage = storage
	lock.path = target
	lock.txnID, err = writer.CommitTo(ctx, storage)
	if err != nil {
		err = errors.Annotatef(err, "failed to commit the lock due to existing lock: "+
			"there is something about the lock: %s", tryFetchRemoteLock(ctx, storage, writeLock))
	}

	return
}
>>>>>>> c9215ec93ce (br: copy full backup to pitr storage (#57716))
