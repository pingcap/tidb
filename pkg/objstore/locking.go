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

package objstore

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

// conditionalPut is a write that in a strong consistency storage.
//
// It provides a `Verify` hook and a `VerifyWriteContext`, you may check
// the conditions you wanting there.
//
// if the write is success and the file wasn't deleted, no other `conditionalPut`
// over the same file was success.
//
// For more details, check docs/design/2024-10-11-put-and-verify-transactions-for-external-storages.md.
type conditionalPut struct {
	// Target is the target file of this txn.
	// There shouldn't be other files shares this prefix with this file, or the txn will fail.
	Target string
	// Content is the content that needed to be written to that file.
	Content func(txnID uuid.UUID) []byte
	// Verify allows you add other preconditions to the write.
	// This will be called when the write is allowed and about to be performed.
	// If `Verify()` returns an error, the write will be aborted.
	Verify func(ctx VerifyWriteContext) error
}

// VerifyWriteContext is the verify write context
type VerifyWriteContext struct {
	context.Context
	Target  string
	Storage storeapi.Storage
	TxnID   uuid.UUID
}

// IntentFileName return the intent file name.
func (cx *VerifyWriteContext) IntentFileName() string {
	return fmt.Sprintf("%s.INTENT.%s", cx.Target, hex.EncodeToString(cx.TxnID[:]))
}

// CommitTo commits the write to the external storage.
// It contains two phases:
// - Intention phase, it will write an "intention" file named "$Target_$TxnID".
// - Put phase, here it actually write the "$Target" down.
//
// In each phase, before writing, it will verify whether the storage is suitable for writing, that is:
// - There shouldn't be any other intention files.
// - Verify() returns no error. (If there is one.)
func (w conditionalPut) CommitTo(ctx context.Context, s storeapi.Storage) (uuid.UUID, error) {
	if _, ok := s.(storeapi.StrongConsistency); !ok {
		log.Warn("The external storage implementation doesn't provide a strong consistency guarantee. "+
			"Please avoid concurrently accessing it if possible.",
			zap.String("type", fmt.Sprintf("%T", s)))
	}

	txnID := uuid.New()
	cx := VerifyWriteContext{
		Context: ctx,
		Target:  w.Target,
		Storage: s,
		TxnID:   txnID,
	}
	intentFileName := cx.IntentFileName()
	checkConflict := func() error {
		var err error
		if w.Verify != nil {
			err = multierr.Append(err, w.Verify(cx))
		}
		return multierr.Append(err, cx.assertOnlyMyIntent())
	}

	if err := checkConflict(); err != nil {
		return uuid.UUID{}, errors.Annotate(err, "during initial check")
	}
	failpoint.InjectCall("exclusive-write-commit-to-1")

	if err := s.WriteFile(cx, intentFileName, []byte{}); err != nil {
		return uuid.UUID{}, errors.Annotate(err, "during writing intention file")
	}

	deleteIntentionFile := func() {
		if err := s.DeleteFile(cx, intentFileName); err != nil {
			log.Warn("Cannot delete the intention file, you may delete it manually.", zap.String("file", intentFileName), logutil.ShortError(err))
		}
	}
	defer deleteIntentionFile()
	if err := checkConflict(); err != nil {
		return uuid.UUID{}, errors.Annotate(err, "during checking whether there are other intentions")
	}
	failpoint.InjectCall("exclusive-write-commit-to-2")

	return txnID, s.WriteFile(cx, w.Target, w.Content(txnID))
}

// assertNoOtherOfPrefixExpect asserts that there is no other file with the same prefix than the expect file.
func (cx VerifyWriteContext) assertNoOtherOfPrefixExpect(pfx string, expect string) error {
	fileName := path.Base(pfx)
	dirName := path.Dir(pfx)
	// Some object stores reject "." as a directory component in list prefixes.
	if dirName == "." {
		dirName = ""
	}

	return cx.Storage.WalkDir(cx, &storeapi.WalkOption{
		SubDir:    dirName,
		ObjPrefix: fileName,
		// We'd better read a deleted intention...
		IncludeTombstone: true,
	}, func(path string, size int64) error {
		if path != expect {
			return fmt.Errorf("there is conflict file %s", path)
		}
		return nil
	})
}

// assertOnlyMyIntent asserts that there is no other intention file than our intention file.
func (cx VerifyWriteContext) assertOnlyMyIntent() error {
	return cx.assertNoOtherOfPrefixExpect(cx.Target, cx.IntentFileName())
}

// Lease-based lock expiration parameters. Declared as var (not const) so
// tests may temporarily scale them down via export_test helpers; production
// callers treat them as immutable.
var (
	// LeaseTTL is how long a newly-acquired or just-renewed lock remains valid
	// before other processes may consider it stale and attempt to reclaim it.
	LeaseTTL = 5 * time.Minute

	// renewInterval is the cadence at which a StartRenewal-tracked lock
	// refreshes its ExpireAt. Renewing at TTL/3 leaves 2*TTL/3 for retry
	// backoff before the lease would actually expire.
	renewInterval = LeaseTTL / 3

	// renewMaxRetries and renewBaseBackoff define the exponential-backoff
	// retry schedule used by the renewal goroutine after a tryRenew failure.
	// With base 5s and 5 attempts the schedule is 5→10→20→40→80s (155s total).
	renewMaxRetries  = 5
	renewBaseBackoff = 5 * time.Second

	// reclaimWaitMultiplier × LeaseTTL is the observation window, starting
	// from ExpireAt, that a reclaimer waits before deleting a stale lock.
	reclaimWaitMultiplier = time.Duration(1)

	// nowFunc is indirected so tests can inject deterministic time.
	nowFunc = time.Now
)

// LockMeta is the meta information of a lock.
type LockMeta struct {
	LockedAt   time.Time `json:"locked_at"`
	LockerHost string    `json:"locker_host"`
	LockerPID  int       `json:"locker_pid"`
	TxnID      []byte    `json:"txn_id"`
	Hint       string    `json:"hint"`
	// ExpireAt is the wall-clock time after which this lock is considered
	// stale and may be reclaimed by another process. Zero value means the
	// lock was written by a pre-lease version of the code; such locks are
	// treated as valid and never auto-reclaimed (backward compat).
	ExpireAt time.Time `json:"expire_at,omitempty"`
}

// String implements fmt.Stringer interface.
func (l LockMeta) String() string {
	return fmt.Sprintf("Locked(at: %s, host: %s, pid: %d, hint: %s)", l.LockedAt.Format(time.DateTime), l.LockerHost, l.LockerPID, l.Hint)
}

// ErrLocked is the error returned when the lock is held by others.
type ErrLocked struct {
	Meta LockMeta
}

// Error return the error.
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
	now := nowFunc()
	meta := LockMeta{
		LockedAt:   now,
		LockerHost: hname,
		Hint:       hint,
		LockerPID:  os.Getpid(),
		ExpireAt:   now.Add(LeaseTTL),
	}
	return meta
}

func getLockMeta(ctx context.Context, storage storeapi.Storage, path string) (LockMeta, error) {
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

// RemoteLock is the remote lock.
//
// Renewal state (mu, renewalStarted, stopCh, done) is zero-initialized and
// activated by StartRenewal. Because RemoteLock embeds a sync.Mutex once
// renewal activates, callers must not copy a RemoteLock after StartRenewal
// has been invoked on it; pass pointers instead.
type RemoteLock struct {
	txnID   uuid.UUID
	storage storeapi.Storage
	path    string

	mu             sync.Mutex
	renewalStarted bool
	stopCh         chan struct{}
	done           chan struct{}
}

// String implements fmt.Stringer interface.
func (l *RemoteLock) String() string {
	return fmt.Sprintf("{path=%s,uuid=%s,storage_uri=%s}", l.path, l.txnID, l.storage.URI())
}

func tryFetchRemoteLockInfo(ctx context.Context, storage storeapi.Storage, path string) error {
	meta, err := getLockMeta(ctx, storage, path)
	if err != nil {
		return err
	}
	return ErrLocked{Meta: meta}
}

// TryLockRemote tries to create a "lock file" at the external storage.
// If success, we will create a file at the path provided. So others may not access the file then.
// Will return a `ErrLocked` if there is another process already creates the lock file.
// This isn't a strict lock like flock in linux: that means, the lock might be forced removed by
// manually deleting the "lock file" in external storage.
func TryLockRemote(ctx context.Context, storage storeapi.Storage, path, hint string) (*RemoteLock, error) {
	writer := conditionalPut{
		Target: path,
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
	}

	txnID, err := writer.CommitTo(ctx, storage)
	if err != nil {
		lockInfo := tryFetchRemoteLockInfo(ctx, storage, path)
		return nil, errors.Annotatef(err, "failed to acquire lock on '%s': %s", path, lockInfo)
	}
	return &RemoteLock{txnID: txnID, storage: storage, path: path}, nil
}

// Unlock  removes the lock file at the specified path.
// Removing that file will release the lock.
func (l *RemoteLock) Unlock(ctx context.Context) error {
	meta, err := getLockMeta(ctx, l.storage, l.path)
	if err != nil {
		return err
	}
	// NOTE: this is for debug usage. For now, there isn't a Compare-And-Swap
	// operation in our Storage abstraction.
	// So, once our lock has been overwritten, or we are overwriting other's lock,
	// this information will be useful for troubleshooting.
	if !bytes.Equal(l.txnID[:], meta.TxnID) {
		return errors.Errorf("Txn ID mismatch: remote is %v, our is %v", meta.TxnID, l.txnID)
	}

	log.Info("Releasing lock.", zap.Stringer("meta", meta), zap.String("path", l.path))
	err = l.storage.DeleteFile(ctx, l.path)
	if err != nil {
		return errors.Annotatef(err, "failed to delete lock file %s", l.path)
	}
	return nil
}

// errRenewTxnIDMismatch is returned by tryRenew when the lock file on remote
// storage carries a different TxnID than ours, meaning some other process has
// reclaimed the lock and acquired a new one. This is permanent — the renewal
// loop must call onLeaseLost and exit.
var errRenewTxnIDMismatch = errors.New("renewal: txn id mismatch (lock was taken by another holder)")

// errRenewLeaseExpired is returned by tryRenew when the on-disk ExpireAt is
// already in the past relative to nowFunc(). The renewal goroutine reached
// this read too late to refresh; the lease is irrecoverably gone.
var errRenewLeaseExpired = errors.New("renewal: lease already expired")

// tryRenew performs one Read → Verify → Write cycle to refresh ExpireAt.
//
// Returns:
//   - nil on successful refresh.
//   - errRenewTxnIDMismatch if the lock file's TxnID no longer matches ours.
//   - errRenewLeaseExpired if the on-disk ExpireAt is already past.
//   - any other error → transient (network/storage); caller should retry.
//
// Crucially, the GET-Verify-PUT order ensures we never write a refreshed
// ExpireAt onto a lock that has already been reclaimed by another holder.
func (l *RemoteLock) tryRenew(ctx context.Context) error {
	data, err := l.storage.ReadFile(ctx, l.path)
	if err != nil {
		return errors.Annotatef(err, "tryRenew: ReadFile %s", l.path)
	}
	var meta LockMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return errors.Annotatef(err, "tryRenew: unmarshal LockMeta from %s", l.path)
	}
	if !bytes.Equal(meta.TxnID, l.txnID[:]) {
		return errRenewTxnIDMismatch
	}
	if !meta.ExpireAt.IsZero() && nowFunc().After(meta.ExpireAt) {
		return errRenewLeaseExpired
	}
	meta.ExpireAt = nowFunc().Add(LeaseTTL)
	newData, err := json.Marshal(meta)
	if err != nil {
		log.Panic("Unreachable: LockMeta JSON marshal failed during renewal.",
			zap.String("path", l.path), logutil.ShortError(err))
	}
	if err := l.storage.WriteFile(ctx, l.path, newData); err != nil {
		return errors.Annotatef(err, "tryRenew: WriteFile %s", l.path)
	}
	return nil
}

// UnlockOnCleanUp unlock the lock on clean up.
func (l *RemoteLock) UnlockOnCleanUp(ctx context.Context) {
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
			zap.Stringer("lock", l), zap.Int("pid", os.Getpid()), logutil.ShortError(err))
	}
}

func writeLockName(path string) string {
	return fmt.Sprintf("%s.WRIT", path)
}

func newReadLockName(path string) string {
	readID := rand.Int63()
	return fmt.Sprintf("%s.READ.%016x", path, readID)
}

// Locker is a locker.
type Locker = func(ctx context.Context, storage storeapi.Storage, path, hint string) (*RemoteLock, error)

const (
	// lockRetryTimes specifies the maximum number of times to retry acquiring a lock.
	// This prevents infinite retries while allowing enough attempts for temporary contention to resolve.
	lockRetryTimes = 60
)

// LockWithRetry lock with retry.
func LockWithRetry(ctx context.Context, locker Locker, storage storeapi.Storage, path, hint string) (*RemoteLock, error) {
	const JitterMs = 5000

	retry := utils.InitialRetryState(lockRetryTimes, 1*time.Second, 60*time.Second)
	jitter := time.Duration(rand.Uint32()%JitterMs+(JitterMs/2)) * time.Millisecond
	var err error
	for {
		lock, lockErr := locker(ctx, storage, path, hint)
		if lockErr == nil {
			return lock, nil
		}
		err = lockErr

		if !retry.ShouldRetry() {
			return nil, errors.Annotatef(err, "failed to acquire lock after %d retries", lockRetryTimes)
		}

		retryAfter := retry.ExponentialBackoff() + jitter
		log.Info(
			"Encountered lock, will retry",
			logutil.ShortError(err),
			zap.String("path", path),
			zap.Duration("retry-after", retryAfter),
			zap.Int("remaining-attempts", retry.RemainingAttempts()),
		)

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(retryAfter):
		}
	}
}

// TryLockRemoteWrite try lock.
func TryLockRemoteWrite(ctx context.Context, storage storeapi.Storage, path, hint string) (*RemoteLock, error) {
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

	txnID, err := writer.CommitTo(ctx, storage)
	if err != nil {
		return nil, errors.Annotatef(err, "something wrong about the lock: %s", tryFetchRemoteLockInfo(ctx, storage, target))
	}
	return &RemoteLock{txnID: txnID, storage: storage, path: target}, nil
}

// TryLockRemoteRead try lock.
func TryLockRemoteRead(ctx context.Context, storage storeapi.Storage, path, hint string) (*RemoteLock, error) {
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

	txnID, err := writer.CommitTo(ctx, storage)
	if err != nil {
		return nil, errors.Annotatef(err, "failed to commit the lock due to existing lock: "+
			"something wrong about the lock: %s", tryFetchRemoteLockInfo(ctx, storage, writeLock))
	}
	return &RemoteLock{txnID: txnID, storage: storage, path: target}, nil
}
