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
	"strings"
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
	// Local records local lock metadata input for conflict reporting.
	Local LockMetaInput
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
		return multierr.Append(err, withLockContext(cx.assertOnlyMyIntent(), w.Target, w.Local))
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

func lockBlockerFromPath(ctx context.Context, storage storeapi.Storage, path string) LockBlocker {
	blocker := LockBlocker{Path: path}
	meta, err := getLockMeta(ctx, storage, path)
	if err != nil {
		blocker.Err = err
		return blocker
	}
	blocker.Meta = meta
	return blocker
}

// conflictingObjectsOfPrefixExpect finds files with the same prefix except the expected file.
func (cx VerifyWriteContext) conflictingObjectsOfPrefixExpect(pfx string, expect string) ([]LockBlocker, int, error) {
	fileName := path.Base(pfx)
	dirName := path.Dir(pfx)
	// Some object stores reject "." as a directory component in list prefixes.
	if dirName == "." {
		dirName = ""
	}

	var blockers []LockBlocker
	blockerCount := 0
	err := cx.Storage.WalkDir(cx, &storeapi.WalkOption{
		SubDir:    dirName,
		ObjPrefix: fileName,
		// We'd better read a deleted intention...
		IncludeTombstone: true,
	}, func(objectPath string, size int64) error {
		if objectPath != expect {
			blockerCount++
			if len(blockers) < lockBlockerMetaLimit {
				blockers = append(blockers, lockBlockerFromPath(cx, cx.Storage, objectPath))
			}
		}
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	return blockers, blockerCount, nil
}

// assertNoOtherOfPrefixExpect asserts that there is no other file with the same prefix than the expect file.
func (cx VerifyWriteContext) assertNoOtherOfPrefixExpect(pfx string, expect string) error {
	blockers, blockerCount, err := cx.conflictingObjectsOfPrefixExpect(pfx, expect)
	if err != nil {
		return err
	}
	if blockerCount > 0 {
		return ErrLocked{Path: cx.Target, BlockerCount: blockerCount, Blockers: blockers}
	}
	return nil
}

// assertOnlyMyIntent asserts that there is no other intention file than our intention file.
func (cx VerifyWriteContext) assertOnlyMyIntent() error {
	return cx.assertNoOtherOfPrefixExpect(cx.Target, cx.IntentFileName())
}

// LockMetaInput is the caller-provided metadata for a lock.
type LockMetaInput struct {
	OwnerID string
	// LockType is a caller-defined resource or scope label used only for diagnostics.
	// It does not control lock compatibility; conflict behavior is determined by
	// the lock path layout and the lock helper used by the caller.
	LockType string
	Hint     string
}

// LockMeta is the meta information of a lock.
type LockMeta struct {
	LockedAt   time.Time `json:"locked_at"`
	LockerHost string    `json:"locker_host"`
	LockerPID  int       `json:"locker_pid"`
	TxnID      []byte    `json:"txn_id"`
	OwnerID    string    `json:"owner_id,omitempty"`
	// LockType is a caller-defined resource or scope label used only for diagnostics.
	// It does not control lock compatibility; conflict behavior is determined by
	// the lock path layout and the lock helper used by the caller.
	LockType string `json:"lock_type,omitempty"`
	Hint     string `json:"hint"`
}

// String implements fmt.Stringer interface.
func (l LockMeta) String() string {
	fields := []string{
		fmt.Sprintf("at: %s", l.LockedAt.Format(time.DateTime)),
		fmt.Sprintf("host: %s", l.LockerHost),
		fmt.Sprintf("pid: %d", l.LockerPID),
		fmt.Sprintf("hint: %s", l.Hint),
	}
	if l.OwnerID != "" {
		fields = append(fields, fmt.Sprintf("owner_id: %s", l.OwnerID))
	}
	if l.LockType != "" {
		fields = append(fields, fmt.Sprintf("lock_type: %s", l.LockType))
	}
	return fmt.Sprintf("Locked(%s)", strings.Join(fields, ", "))
}

func (i LockMetaInput) String() string {
	fields := []string{fmt.Sprintf("hint: %s", i.Hint)}
	if i.OwnerID != "" {
		fields = append(fields, fmt.Sprintf("owner_id: %s", i.OwnerID))
	}
	if i.LockType != "" {
		fields = append(fields, fmt.Sprintf("lock_type: %s", i.LockType))
	}
	return fmt.Sprintf("LockMetaInput(%s)", strings.Join(fields, ", "))
}

// LockBlocker is a lock object that blocks a local lock attempt.
type LockBlocker struct {
	Path string
	Meta LockMeta
	Err  error
}

func (b LockBlocker) String() string {
	fields := []string{fmt.Sprintf("path: %s", b.Path)}
	if b.Err != nil {
		fields = append(fields, fmt.Sprintf("err: %s", b.Err))
	} else {
		fields = append(fields, fmt.Sprintf("meta: %s", b.Meta))
	}
	return fmt.Sprintf("Blocker(%s)", strings.Join(fields, ", "))
}

// ErrLocked is the error returned when the lock is held by others.
type ErrLocked struct {
	Path         string
	Meta         LockMeta
	Local        LockMetaInput
	BlockerCount int
	Blockers     []LockBlocker
}

// Error return the error.
func (e ErrLocked) Error() string {
	fields := []string{"locked"}
	if e.Path != "" {
		fields = append(fields, fmt.Sprintf("path = %s", e.Path))
	}
	if !isZeroLockMeta(e.Meta) {
		fields = append(fields, fmt.Sprintf("meta = %s", e.Meta))
	}
	if !isZeroLockMetaInput(e.Local) {
		fields = append(fields, fmt.Sprintf("local = %s", e.Local))
	}
	sampledBlockerCount := len(e.Blockers)
	if sampledBlockerCount > lockBlockerErrorLimit {
		sampledBlockerCount = lockBlockerErrorLimit
	}
	for _, blocker := range e.Blockers[:sampledBlockerCount] {
		fields = append(fields, fmt.Sprintf("conflict file %s", blocker.Path))
		if blocker.Err != nil {
			fields = append(fields, fmt.Sprintf("blocker_error = %s", blocker.Err))
		} else {
			fields = append(fields, fmt.Sprintf("blocker_meta = %s", blocker.Meta))
		}
	}
	if omitted := e.remoteBlockerCount() - sampledBlockerCount; omitted > 0 {
		fields = append(fields, fmt.Sprintf("omitted_conflict_files = %d", omitted))
	}
	return strings.Join(fields, ", ")
}

const lockBlockerErrorLimit = 3
const lockBlockerMetaLimit = 3

func (e ErrLocked) remoteBlockerCount() int {
	if e.BlockerCount > len(e.Blockers) {
		return e.BlockerCount
	}
	return len(e.Blockers)
}

func isZeroLockMeta(meta LockMeta) bool {
	return meta.LockedAt.IsZero() && meta.LockerHost == "" && meta.LockerPID == 0 &&
		len(meta.TxnID) == 0 && meta.Hint == "" && meta.OwnerID == "" && meta.LockType == ""
}

func isZeroLockMetaInput(input LockMetaInput) bool {
	return input.OwnerID == "" && input.LockType == "" && input.Hint == ""
}

func withLockContext(err error, path string, local LockMetaInput) error {
	if err == nil {
		return nil
	}
	var locked ErrLocked
	if !stderrors.As(err, &locked) {
		return err
	}
	if locked.Path == "" {
		locked.Path = path
	}
	if isZeroLockMetaInput(locked.Local) {
		locked.Local = local
	}
	return locked
}

type lockMultiCauseError struct {
	message string
	primary error
	causes  []error
}

func (e lockMultiCauseError) Error() string {
	return e.message
}

func (e lockMultiCauseError) Unwrap() []error {
	return e.causes
}

func (e lockMultiCauseError) Cause() error {
	return e.primary
}

func lockErrorWithCauses(message string, primary error, causes ...error) error {
	filtered := make([]error, 0, len(causes))
	for _, cause := range causes {
		if cause != nil {
			filtered = append(filtered, cause)
		}
	}
	if len(filtered) == 0 && primary != nil {
		filtered = append(filtered, primary)
	}
	if len(filtered) == 0 {
		return errors.New(message)
	}
	return lockMultiCauseError{
		message: message,
		primary: primary,
		causes:  filtered,
	}
}

// MakeLockMeta creates a LockMeta by the current node's metadata.
// Including current time and hostname, etc..
func MakeLockMeta(input LockMetaInput) LockMeta {
	hname, err := os.Hostname()
	if err != nil {
		hname = fmt.Sprintf("UnknownHost(err=%s)", err)
	}
	now := time.Now()
	meta := LockMeta{
		LockedAt:   now,
		LockerHost: hname,
		Hint:       input.Hint,
		LockerPID:  os.Getpid(),
		OwnerID:    input.OwnerID,
		LockType:   input.LockType,
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
type RemoteLock struct {
	txnID   uuid.UUID
	storage storeapi.Storage
	path    string
}

// String implements fmt.Stringer interface.
func (l *RemoteLock) String() string {
	return fmt.Sprintf("{path=%s,uuid=%s,storage_uri=%s}", l.path, l.txnID, l.storage.URI())
}

func enrichErrLocked(ctx context.Context, storage storeapi.Storage, path string, local LockMetaInput, locked ErrLocked) ErrLocked {
	if locked.Path == "" {
		locked.Path = path
	}
	if isZeroLockMetaInput(locked.Local) {
		locked.Local = local
	}
	if len(locked.Blockers) > 0 {
		if isZeroLockMeta(locked.Meta) {
			locked.Meta = locked.Blockers[0].Meta
		}
		return locked
	}
	if isZeroLockMeta(locked.Meta) {
		blocker := lockBlockerFromPath(ctx, storage, path)
		locked.Meta = blocker.Meta
		if blocker.Err != nil {
			locked.Blockers = []LockBlocker{blocker}
		}
	}
	return locked
}

func annotateLockAttemptError(ctx context.Context, storage storeapi.Storage, path string, local LockMetaInput, err error, format string, args ...any) error {
	message := fmt.Sprintf(format, args...)
	var locked ErrLocked
	if stderrors.As(err, &locked) {
		locked = enrichErrLocked(ctx, storage, path, local, locked)
		return lockErrorWithCauses(fmt.Sprintf("%s: %s", message, err), err, locked, err)
	}
	if meta, metaErr := getLockMeta(ctx, storage, path); metaErr == nil {
		return errors.Annotatef(err, "%s; remote_lock_meta = %s", message, meta)
	}
	return errors.Annotate(err, message)
}

// TryLockRemote tries to create a "lock file" at the external storage.
// If success, we will create a file at the path provided. So others may not access the file then.
// Will return a `ErrLocked` if there is another process already creates the lock file.
// This isn't a strict lock like flock in linux: that means, the lock might be forced removed by
// manually deleting the "lock file" in external storage.
func TryLockRemote(ctx context.Context, storage storeapi.Storage, path string, input LockMetaInput) (lock RemoteLock, err error) {
	writer := conditionalPut{
		Target: path,
		Local:  input,
		Content: func(txnID uuid.UUID) []byte {
			meta := MakeLockMeta(input)
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

	lock.storage = storage
	lock.path = path
	lock.txnID, err = writer.CommitTo(ctx, storage)
	if err != nil {
		err = annotateLockAttemptError(ctx, storage, path, input, err, "failed to acquire lock on '%s'", path)
	}
	return
}

// Unlock  removes the lock file at the specified path.
// Removing that file will release the lock.
func (l RemoteLock) Unlock(ctx context.Context) error {
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

// UnlockOnCleanUp unlock the lock on clean up.
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

// Locker is a locker.
type Locker = func(ctx context.Context, storage storeapi.Storage, path string, input LockMetaInput) (lock RemoteLock, err error)

const (
	// lockRetryTimes specifies the maximum number of times to retry acquiring a lock.
	// This prevents infinite retries while allowing enough attempts for temporary contention to resolve.
	lockRetryTimes = 60
)

// LockWithRetry lock with retry.
func LockWithRetry(ctx context.Context, locker Locker, storage storeapi.Storage, path string, input LockMetaInput) (
	lock RemoteLock, err error) {
	const JitterMs = 5000

	retry := utils.InitialRetryState(lockRetryTimes, 1*time.Second, 60*time.Second)
	jitter := time.Duration(rand.Uint32()%JitterMs+(JitterMs/2)) * time.Millisecond
	for {
		lock, err = locker(ctx, storage, path, input)
		if err == nil {
			return lock, nil
		}

		if !retry.ShouldRetry() {
			return RemoteLock{}, errors.Annotatef(err, "failed to acquire lock after %d retries", lockRetryTimes)
		}

		retryAfter := retry.ExponentialBackoff() + jitter
		fields := LockConflictLogFields(path, input, err)
		fields = append(fields,
			zap.Duration("retry-after", retryAfter),
			zap.Int("remaining-attempts", retry.RemainingAttempts()),
		)
		log.Info(
			"Encountered lock, will retry",
			fields...,
		)

		select {
		case <-ctx.Done():
			return RemoteLock{}, lockErrorWithCauses(fmt.Sprintf("lock retry stopped: %s: %s", ctx.Err(), err), ctx.Err(), ctx.Err(), err)
		case <-time.After(retryAfter):
		}
	}
}

// LockConflictLogFields returns structured fields for a failed lock attempt.
func LockConflictLogFields(path string, input LockMetaInput, err error) []zap.Field {
	fields := []zap.Field{
		logutil.ShortError(err),
		zap.String("path", path),
	}
	fields = append(fields, lockMetaInputLogFields("local", input)...)
	var locked ErrLocked
	if stderrors.As(err, &locked) {
		fields = append(fields, zap.Int("remote_blocker_count", locked.remoteBlockerCount()))
		if len(locked.Blockers) > 0 {
			fields = append(fields, lockBlockerLogFields(locked.Blockers, lockBlockerLogLimit)...)
		} else if !isZeroLockMeta(locked.Meta) {
			fields = append(fields, lockMetaLogFields("remote", locked.Meta)...)
		}
	}
	return fields
}

func lockMetaInputLogFields(prefix string, input LockMetaInput) []zap.Field {
	return []zap.Field{
		zap.String(prefix+"_owner_id", input.OwnerID),
		zap.String(prefix+"_lock_type", input.LockType),
		zap.String(prefix+"_hint", input.Hint),
	}
}

func lockMetaLogFields(prefix string, meta LockMeta) []zap.Field {
	return []zap.Field{
		zap.String(prefix+"_owner_id", meta.OwnerID),
		zap.String(prefix+"_lock_type", meta.LockType),
		zap.String(prefix+"_hint", meta.Hint),
	}
}

const lockBlockerLogLimit = 3

func lockBlockerLogFields(blockers []LockBlocker, limit int) []zap.Field {
	if len(blockers) < limit {
		limit = len(blockers)
	}
	fields := make([]zap.Field, 0, limit*6)
	for i := range limit {
		prefix := fmt.Sprintf("remote_blocker_%d", i)
		fields = append(fields, zap.String(prefix+"_path", blockers[i].Path))
		if !isZeroLockMeta(blockers[i].Meta) {
			fields = append(fields, lockMetaLogFields(prefix, blockers[i].Meta)...)
		}
		if blockers[i].Err != nil {
			fields = append(fields, logutil.AShortError(prefix+"_error", blockers[i].Err))
		}
	}
	return fields
}

// TryLockRemoteWrite try lock.
func TryLockRemoteWrite(ctx context.Context, storage storeapi.Storage, path string, input LockMetaInput) (lock RemoteLock, err error) {
	target := writeLockName(path)
	writer := conditionalPut{
		Target: target,
		Local:  input,
		Content: func(txnID uuid.UUID) []byte {
			meta := MakeLockMeta(input)
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
			blockers, blockerCount, err := ctx.conflictingObjectsOfPrefixExpect(path, ctx.IntentFileName())
			if err != nil {
				return err
			}
			if blockerCount > 0 {
				return ErrLocked{Path: target, Local: input, BlockerCount: blockerCount, Blockers: blockers}
			}
			return nil
		},
	}

	lock.storage = storage
	lock.path = target
	lock.txnID, err = writer.CommitTo(ctx, storage)
	if err != nil {
		err = annotateLockAttemptError(ctx, storage, target, input, err, "something wrong about the lock")
	}
	return
}

// TryLockRemoteRead try lock.
func TryLockRemoteRead(ctx context.Context, storage storeapi.Storage, path string, input LockMetaInput) (lock RemoteLock, err error) {
	target := newReadLockName(path)
	writeLock := writeLockName(path)
	writer := conditionalPut{
		Target: target,
		Local:  input,
		Content: func(txnID uuid.UUID) []byte {
			meta := MakeLockMeta(input)
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
			blockers, blockerCount, err := ctx.conflictingObjectsOfPrefixExpect(writeLock, "")
			if err != nil {
				return err
			}
			if blockerCount > 0 {
				return ErrLocked{Path: target, Local: input, BlockerCount: blockerCount, Blockers: blockers}
			}
			return nil
		},
	}

	lock.storage = storage
	lock.path = target
	lock.txnID, err = writer.CommitTo(ctx, storage)
	if err != nil {
		err = annotateLockAttemptError(ctx, storage, writeLock, input, err,
			"failed to commit the lock due to existing lock: something wrong about the lock")
	}
	return
}
