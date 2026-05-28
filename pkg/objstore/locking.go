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
	stderrors "errors"
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
	LeaseTTL = time.Hour

	// renewInterval is the cadence at which a startRenewal-tracked lock
	// refreshes its ExpireAt. Renewing at TTL/3 leaves 2*TTL/3 for retry
	// backoff before the lease would actually expire.
	renewInterval = LeaseTTL / 3

	// renewMaxRetries and renewBaseBackoff define the exponential-backoff
	// retry schedule used by the renewal goroutine after a tryRenew failure.
	// With base 5s and 5 attempts the schedule is 5→10→20→40→80s (155s total).
	renewMaxRetries  = 5
	renewBaseBackoff = 5 * time.Second

	// nowFunc is indirected so tests can inject deterministic time.
	nowFunc = time.Now
)

// LeaseClock supplies the current lease time for lock metadata and lease
// validity checks.
type LeaseClock interface {
	Now(ctx context.Context) (time.Time, error)
}

type localLeaseClock struct{}

func (localLeaseClock) Now(context.Context) (time.Time, error) {
	return nowFunc(), nil
}

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
	return makeLockMetaAt(hint, nowFunc())
}

func makeLockMetaAt(hint string, lockedAt time.Time) LockMeta {
	hname, err := os.Hostname()
	if err != nil {
		hname = fmt.Sprintf("UnknownHost(err=%s)", err)
	}
	meta := LockMeta{
		LockedAt:   lockedAt,
		LockerHost: hname,
		Hint:       hint,
		LockerPID:  os.Getpid(),
		ExpireAt:   lockedAt.Add(LeaseTTL),
	}
	return meta
}

type renewalState uint8

const (
	renewalIdle renewalState = iota
	renewalRunning
	renewalStopped
)

// RemoteLock is the remote lock.
//
// Renewal state (mu, renewalState, stopCh, done) is zero-initialized and
// activated by startRenewal. Because RemoteLock embeds a sync.Mutex once
// renewal activates, callers must not copy a RemoteLock after startRenewal
// has been invoked on it; pass pointers instead.
type RemoteLock struct {
	txnID      uuid.UUID
	storage    storeapi.Storage
	path       string
	leaseClock LeaseClock

	mu           sync.Mutex
	renewalState renewalState
	stopCh       chan struct{}
	done         chan struct{}
}

// String implements fmt.Stringer interface.
func (l *RemoteLock) String() string {
	return fmt.Sprintf("{path=%s,uuid=%s,storage_uri=%s}", l.path, l.txnID, l.storage.URI())
}

// Unlock  removes the lock file at the specified path.
// Removing that file will release the lock.
//
// If startRenewal was previously invoked, Unlock first signals the renewal
// goroutine to stop and waits for it to exit. This ordering is important:
// without the wait, an in-flight tryRenew WriteFile could land after our
// DeleteFile below, recreating a zombie lock file on remote storage.
func (l *RemoteLock) Unlock(ctx context.Context) error {
	l.stopRenewalIfStarted()

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

func (l *RemoteLock) stopRenewalIfStarted() {
	var (
		stopCh      chan struct{}
		done        chan struct{}
		shouldClose bool
	)

	l.mu.Lock()
	switch l.renewalState {
	case renewalIdle:
		l.mu.Unlock()
		return
	case renewalStopped:
		done = l.done
	case renewalRunning:
		l.renewalState = renewalStopped
		stopCh = l.stopCh
		done = l.done
		shouldClose = true
	}
	l.mu.Unlock()

	if shouldClose {
		close(stopCh)
	}
	<-done
}

// CleanUpStaleTruncateLock reclaims only stale instance-form truncate locks.
// It intentionally ignores the legacy fixed path "truncating.lock".
func CleanUpStaleTruncateLock(ctx context.Context, storage storeapi.Storage) (bool, error) {
	return CleanUpStaleTruncateLockWithLeaseClock(ctx, storage, localLeaseClock{})
}

// CleanUpStaleTruncateLockWithLeaseClock reclaims only stale instance-form
// truncate locks using the provided lease clock for stale decisions. It
// intentionally ignores the legacy fixed path "truncating.lock".
func CleanUpStaleTruncateLockWithLeaseClock(ctx context.Context, storage storeapi.Storage, clock LeaseClock) (bool, error) {
	return cleanUpStaleLockFamily(ctx, storage, truncateLockPath, false, clock)
}

// errRenewTxnIDMismatch is returned by tryRenew when the lock file on remote
// storage carries a different TxnID than ours, meaning some other process has
// reclaimed the lock and acquired a new one. This is permanent — the renewal
// loop must call onLeaseLost and exit.
var errRenewTxnIDMismatch = errors.New("renewal: txn id mismatch (lock was taken by another holder)")

// errRenewLeaseExpired is returned by tryRenew when the on-disk ExpireAt is
// already in the past relative to the lock's lease clock. The renewal goroutine
// reached this read too late to refresh; the lease is irrecoverably gone.
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

	leaseClock := l.leaseClock
	if leaseClock == nil {
		return errors.New("lease clock is required")
	}
	leaseNow, err := leaseClock.Now(ctx)
	if err != nil {
		return errors.Annotate(err, "tryRenew: get lease time")
	}
	if !meta.ExpireAt.IsZero() && leaseNow.After(meta.ExpireAt) {
		return errRenewLeaseExpired
	}
	meta.ExpireAt = leaseNow.Add(LeaseTTL)
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

// startRenewal launches a background goroutine that periodically refreshes
// this lock's ExpireAt until Unlock is called. onLeaseLost is invoked if the
// renewal goroutine discovers the lease has been permanently lost (TxnID
// mismatch, ExpireAt already past, or renewal retries exhausted); callers
// typically pass a context.CancelFunc to tear down business work.
//
// startRenewal must be called at most once per *RemoteLock. A second call
// panics; this surfaces ownership-discipline bugs rather than silently
// double-starting a pair of conflicting renewal goroutines.
func (l *RemoteLock) startRenewal(ctx context.Context, onLeaseLost func()) {
	l.mu.Lock()
	if l.renewalState != renewalIdle {
		l.mu.Unlock()
		log.Panic("startRenewal called twice on the same lock; each RemoteLock has exactly one owner.",
			zap.Stringer("lock", l))
	}
	l.renewalState = renewalRunning
	l.stopCh = make(chan struct{})
	l.done = make(chan struct{})
	l.mu.Unlock()

	go l.renewalLoop(ctx, onLeaseLost)
}

// renewalLoop runs until stopCh is closed, retries are exhausted, or the
// lease is permanently lost. It always closes l.done before returning so
// that Unlock can wait for full teardown before deleting the lock file.
func (l *RemoteLock) renewalLoop(ctx context.Context, onLeaseLost func()) {
	defer close(l.done)

	invokeLost := func() {
		if onLeaseLost != nil {
			onLeaseLost()
		}
	}

	for {
		select {
		case <-l.stopCh:
			return
		case <-time.After(renewInterval):
		}

		var lastErr error
		renewSucceeded := false
		for attempt := 0; attempt <= renewMaxRetries; attempt++ {
			err := l.tryRenew(ctx)
			if err == nil {
				renewSucceeded = true
				break
			}
			if stderrors.Is(err, errRenewTxnIDMismatch) || stderrors.Is(err, errRenewLeaseExpired) {
				log.Warn("Lock renewal detected lease lost; calling onLeaseLost.",
					zap.Stringer("lock", l),
					zap.Int("attempt", attempt),
					logutil.ShortError(err))
				invokeLost()
				return
			}

			lastErr = err
			if attempt == renewMaxRetries {
				break
			}

			retryBackoff := renewBaseBackoff * time.Duration(1<<attempt)
			log.Warn("Lock renewal hit transient error; will retry with exponential backoff.",
				zap.Stringer("lock", l),
				zap.Int("attempt", attempt),
				zap.Duration("backoff", retryBackoff),
				logutil.ShortError(err))
			timer := time.NewTimer(retryBackoff)
			select {
			case <-l.stopCh:
				timer.Stop()
				return
			case <-timer.C:
			}
		}
		if !renewSucceeded {
			log.Warn("Lock renewal retries exhausted; calling onLeaseLost.",
				zap.Stringer("lock", l),
				zap.Int("max_retries", renewMaxRetries),
				logutil.ShortError(lastErr))
			invokeLost()
			return
		}
		// renewSucceeded → wait for the next tick.
	}
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

// Locker is a locker.
type Locker = func(ctx context.Context, storage storeapi.Storage, path, hint string) (*RemoteLock, error)

// LockerWithLeaseClock is a locker that receives the same lease clock used by
// the surrounding retry and cleanup flow.
type LockerWithLeaseClock = func(ctx context.Context, storage storeapi.Storage, path, hint string, clock LeaseClock) (*RemoteLock, error)

const (
	// lockRetryTimes specifies the maximum number of times to retry acquiring a lock.
	// This prevents infinite retries while allowing enough attempts for temporary contention to resolve.
	lockRetryTimes = 60
)

func validateOnLeaseLost(onLeaseLost func()) error {
	if onLeaseLost == nil {
		return errors.New("onLeaseLost callback is required for lease lock renewal")
	}
	return nil
}

// LockWithRetry lock with retry.
//
// On each conflict it opportunistically deletes any stale (ExpireAt-past +
// double-confirmed) lock files in the family rooted at `lockPath`, then
// continues with the standard exponential backoff.
func LockWithRetry(ctx context.Context, locker Locker, storage storeapi.Storage, lockPath, hint string, onLeaseLost func()) (*RemoteLock, error) {
	return LockWithRetryWithLeaseClock(ctx, func(ctx context.Context, storage storeapi.Storage, path, hint string, _ LeaseClock) (*RemoteLock, error) {
		return locker(ctx, storage, path, hint)
	}, storage, lockPath, hint, onLeaseLost, localLeaseClock{})
}

// LockWithRetryWithLeaseClock lock with retry using the same lease clock for
// acquire, stale cleanup, and later renewal on the returned lock.
func LockWithRetryWithLeaseClock(ctx context.Context, locker LockerWithLeaseClock, storage storeapi.Storage, lockPath, hint string, onLeaseLost func(), clock LeaseClock) (*RemoteLock, error) {
	if err := validateOnLeaseLost(onLeaseLost); err != nil {
		return nil, err
	}
	if clock == nil {
		return nil, errors.New("lease clock is required")
	}

	const JitterMs = 5000

	retry := utils.InitialRetryState(lockRetryTimes, 1*time.Second, 60*time.Second)
	jitter := time.Duration(rand.Uint32()%JitterMs+(JitterMs/2)) * time.Millisecond
	var err error
	for {
		lock, lockErr := locker(ctx, storage, lockPath, hint, clock)
		if lockErr == nil {
			lock.startRenewal(ctx, onLeaseLost)
			return lock, nil
		}
		err = lockErr

		if tryCleanUpStaleLockFamily(ctx, storage, lockPath, clock) {
			log.Info("Stale locks cleaned up while waiting for lock.",
				zap.String("path", lockPath))
		}

		if !retry.ShouldRetry() {
			return nil, errors.Annotatef(err, "failed to acquire lock after %d retries", lockRetryTimes)
		}

		retryAfter := retry.ExponentialBackoff() + jitter
		log.Info(
			"Encountered lock, will retry",
			logutil.ShortError(err),
			zap.String("path", lockPath),
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

// TryLockRemoteWrite is a single-attempt write-lock primitive. It does not
// retry, clean up stale locks, or start renewal.
func TryLockRemoteWrite(ctx context.Context, storage storeapi.Storage, path, hint string) (*RemoteLock, error) {
	return TryLockRemoteWriteWithLeaseClock(ctx, storage, path, hint, localLeaseClock{})
}

// TryLockRemoteWriteWithLeaseClock is a single-attempt write-lock primitive
// using the provided lease clock for lock metadata and post-acquire proof. It
// does not retry, clean up stale locks, or start renewal.
func TryLockRemoteWriteWithLeaseClock(ctx context.Context, storage storeapi.Storage, path, hint string, clock LeaseClock) (*RemoteLock, error) {
	if clock == nil {
		return nil, errors.New("lease clock is required")
	}

	var target string
	var acquireKind lockAcquireKind
	switch path {
	case migrationLockPath:
		leaseNow, err := clock.Now(ctx)
		if err != nil {
			return nil, errors.Annotate(err, "get lease time before acquiring lock")
		}
		generation, err := newLockGeneration(leaseNow)
		if err != nil {
			return nil, err
		}
		target = fmt.Sprintf("%s.%s", migrationWriteLockPrefix, generation)
		acquireKind = lockAcquireMigrationWrite
		return tryLockRemoteExactWithLeaseClock(ctx, storage, target, makeLockMetaAt(hint, leaseNow), clock, func(ctx VerifyWriteContext) error {
			return verifyLockFamily(ctx, path, acquireKind)
		})
	case appendLockPath:
		leaseNow, err := clock.Now(ctx)
		if err != nil {
			return nil, errors.Annotate(err, "get lease time before acquiring lock")
		}
		generation, err := newLockGeneration(leaseNow)
		if err != nil {
			return nil, err
		}
		target = fmt.Sprintf("%s.%s", appendWriteLockPrefix, generation)
		acquireKind = lockAcquireAppendWrite
		return tryLockRemoteExactWithLeaseClock(ctx, storage, target, makeLockMetaAt(hint, leaseNow), clock, func(ctx VerifyWriteContext) error {
			return verifyLockFamily(ctx, path, acquireKind)
		})
	default:
		return nil, errors.Errorf("unknown lock family %s", path)
	}
}

// TryLockRemoteRead is a single-attempt read-lock primitive. It does not
// retry, clean up stale locks, or start renewal.
func TryLockRemoteRead(ctx context.Context, storage storeapi.Storage, path, hint string) (*RemoteLock, error) {
	return TryLockRemoteReadWithLeaseClock(ctx, storage, path, hint, localLeaseClock{})
}

// TryLockRemoteReadWithLeaseClock is a single-attempt read-lock primitive
// using the provided lease clock for lock metadata and post-acquire proof. It
// does not retry, clean up stale locks, or start renewal.
func TryLockRemoteReadWithLeaseClock(ctx context.Context, storage storeapi.Storage, path, hint string, clock LeaseClock) (*RemoteLock, error) {
	if clock == nil {
		return nil, errors.New("lease clock is required")
	}
	if path != migrationLockPath {
		return nil, errors.Errorf("unknown lock family %s", path)
	}
	leaseNow, err := clock.Now(ctx)
	if err != nil {
		return nil, errors.Annotate(err, "get lease time before acquiring lock")
	}
	generation, err := newLockGeneration(leaseNow)
	if err != nil {
		return nil, err
	}
	target := fmt.Sprintf("%s.%s", migrationReadLockPrefix, generation)
	return tryLockRemoteExactWithLeaseClock(ctx, storage, target, makeLockMetaAt(hint, leaseNow), clock, func(ctx VerifyWriteContext) error {
		return verifyLockFamily(ctx, path, lockAcquireMigrationRead)
	})
}

// TryLockRemoteTruncate is a single-attempt primitive that tries to create an
// instance lock for truncation. It does not retry, clean up stale locks, or
// start renewal. Production truncate callers should use LockRemoteTruncate.
func TryLockRemoteTruncate(ctx context.Context, storage storeapi.Storage, hint string) (*RemoteLock, error) {
	return TryLockRemoteTruncateWithLeaseClock(ctx, storage, hint, localLeaseClock{})
}

// TryLockRemoteTruncateWithLeaseClock is a single-attempt primitive that tries
// to create an instance lock for truncation using the provided lease clock. It
// does not retry, clean up stale locks, or start renewal.
func TryLockRemoteTruncateWithLeaseClock(ctx context.Context, storage storeapi.Storage, hint string, clock LeaseClock) (*RemoteLock, error) {
	if clock == nil {
		return nil, errors.New("lease clock is required")
	}
	leaseNow, err := clock.Now(ctx)
	if err != nil {
		return nil, errors.Annotate(err, "get lease time before acquiring lock")
	}
	generation, err := newLockGeneration(leaseNow)
	if err != nil {
		return nil, err
	}
	target := fmt.Sprintf("%s.%s", truncateLockPath, generation)
	return tryLockRemoteExactWithLeaseClock(ctx, storage, target, makeLockMetaAt(hint, leaseNow), clock, func(ctx VerifyWriteContext) error {
		return verifyLockFamily(ctx, truncateLockPath, lockAcquireTruncate)
	})
}

// LockRemoteTruncate acquires a truncate lock once and starts lease renewal.
func LockRemoteTruncate(ctx context.Context, storage storeapi.Storage, hint string, onLeaseLost func()) (*RemoteLock, error) {
	return LockRemoteTruncateWithLeaseClock(ctx, storage, hint, onLeaseLost, localLeaseClock{})
}

// LockRemoteTruncateWithLeaseClock acquires a truncate lock once with the
// provided lease clock and starts lease renewal.
func LockRemoteTruncateWithLeaseClock(ctx context.Context, storage storeapi.Storage, hint string, onLeaseLost func(), clock LeaseClock) (*RemoteLock, error) {
	if err := validateOnLeaseLost(onLeaseLost); err != nil {
		return nil, err
	}
	if clock == nil {
		return nil, errors.New("lease clock is required")
	}

	lock, err := TryLockRemoteTruncateWithLeaseClock(ctx, storage, hint, clock)
	if err != nil {
		return nil, err
	}
	lock.startRenewal(ctx, onLeaseLost)
	return lock, nil
}
