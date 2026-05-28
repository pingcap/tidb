// Copyright 2026 PingCAP, Inc.
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
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

type lockAcquireKind uint8

const (
	lockAcquireTruncate lockAcquireKind = iota
	lockAcquireMigrationWrite
	lockAcquireMigrationRead
	lockAcquireAppendWrite
)

type lockMemberKind uint8

const (
	lockMemberUnknown lockMemberKind = iota
	lockMemberTruncate
	lockMemberMigrationWrite
	lockMemberMigrationRead
	lockMemberAppendWrite
)

const (
	truncateLockPath         = "truncating.lock"
	migrationLockPath        = "v1/LOCK"
	appendLockPath           = "v1/APPEND_LOCK"
	migrationWriteLockPrefix = "v1/LOCK.WRIT"
	migrationReadLockPrefix  = "v1/LOCK.READ"
	appendWriteLockPrefix    = "v1/APPEND_LOCK.WRIT"
	acquireProofCleanupTTL   = 30 * time.Second
)

type lockFamilyMember struct {
	path            string
	kind            lockMemberKind
	ownIntent       bool
	intent          bool
	cleanupEligible bool
}

type lockFamilyConflictError struct {
	member lockFamilyMember
}

func (e lockFamilyConflictError) Error() string {
	return fmt.Sprintf("conflict file %s", e.member.path)
}

func newLockGeneration(leaseNow time.Time) (string, error) {
	var randomBytes [8]byte
	if _, err := cryptorand.Read(randomBytes[:]); err != nil {
		return "", errors.Annotate(err, "generate lock instance id")
	}
	return fmt.Sprintf("%016x%016x", leaseNow.UnixNano(), binary.BigEndian.Uint64(randomBytes[:])), nil
}

func makeLockContent(path string, meta LockMeta) func(uuid.UUID) []byte {
	return func(txnID uuid.UUID) []byte {
		meta.TxnID = txnID[:]
		res, err := json.Marshal(meta)
		if err != nil {
			log.Panic(
				"Unreachable: a lock meta object cannot be marshaled to JSON.",
				zap.String("path", path),
				logutil.ShortError(err),
			)
		}
		return res
	}
}

// SetLeaseConstantsForTest overrides the lease-related timing knobs for tests.
// The returned restore function must be called so later tests see production
// values again.
func SetLeaseConstantsForTest(ttl, interval time.Duration, maxRetries int, baseBackoff time.Duration) (restore func()) {
	oldTTL, oldInterval := LeaseTTL, renewInterval
	oldMax, oldBackoff := renewMaxRetries, renewBaseBackoff
	LeaseTTL = ttl
	renewInterval = interval
	renewMaxRetries = maxRetries
	renewBaseBackoff = baseBackoff
	return func() {
		LeaseTTL, renewInterval = oldTTL, oldInterval
		renewMaxRetries, renewBaseBackoff = oldMax, oldBackoff
	}
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

func tryLockRemoteExact(
	ctx context.Context,
	storage storeapi.Storage,
	physicalPath string,
	hint string,
	verify func(VerifyWriteContext) error,
) (*RemoteLock, error) {
	meta := MakeLockMeta(hint)
	return tryLockRemoteExactWithLeaseClock(ctx, storage, physicalPath, meta, localLeaseClock{}, verify)
}

func tryLockRemoteExactWithLeaseClock(
	ctx context.Context,
	storage storeapi.Storage,
	physicalPath string,
	meta LockMeta,
	clock LeaseClock,
	verify func(VerifyWriteContext) error,
) (*RemoteLock, error) {
	if clock == nil {
		return nil, errors.New("lease clock is required")
	}

	writer := conditionalPut{
		Target:  physicalPath,
		Content: makeLockContent(physicalPath, meta),
		Verify:  verify,
	}

	txnID, err := writer.CommitTo(ctx, storage)
	if err != nil {
		return nil, errors.Annotatef(err, "failed to acquire lock on '%s'", physicalPath)
	}
	if err := proveAcquiredLeaseIsValid(ctx, storage, physicalPath, meta, clock); err != nil {
		return nil, errors.Annotatef(err, "failed to acquire lock on '%s'", physicalPath)
	}
	return &RemoteLock{txnID: txnID, storage: storage, path: physicalPath, leaseClock: clock}, nil
}

func proveAcquiredLeaseIsValid(ctx context.Context, storage storeapi.Storage, physicalPath string, meta LockMeta, clock LeaseClock) error {
	nowAfterAcquire, err := clock.Now(ctx)
	if err != nil {
		deleteErr := deleteAcquiredLockAfterProofFailure(ctx, storage, physicalPath)
		return multierr.Append(errors.Annotate(err, "prove acquired lease is still valid"), deleteErr)
	}
	if nowAfterAcquire.After(meta.ExpireAt) {
		deleteErr := deleteAcquiredLockAfterProofFailure(ctx, storage, physicalPath)
		return multierr.Append(errors.Errorf("lease expired before acquire returned: now=%s expire_at=%s", nowAfterAcquire, meta.ExpireAt), deleteErr)
	}
	return nil
}

func deleteAcquiredLockAfterProofFailure(ctx context.Context, storage storeapi.Storage, physicalPath string) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), acquireProofCleanupTTL)
	defer cancel()

	if err := storage.DeleteFile(cleanupCtx, physicalPath); err != nil {
		log.Warn("Failed to delete acquired lock after lease proof failure.",
			zap.String("path", physicalPath),
			logutil.ShortError(err))
		return errors.Annotatef(err, "delete acquired lock after lease proof failure %s", physicalPath)
	}
	return nil
}

func is32Hex(s string) bool {
	if len(s) != 32 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

func is16Hex(s string) bool {
	if len(s) != 16 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

func lockFamilyPrefixes(logicalPath string) ([]string, bool) {
	switch logicalPath {
	case truncateLockPath:
		return []string{truncateLockPath}, true
	case migrationLockPath:
		return []string{migrationWriteLockPrefix, migrationReadLockPrefix}, true
	case appendLockPath:
		return []string{appendWriteLockPrefix}, true
	default:
		return nil, false
	}
}

func listLockFamilyCandidates(ctx context.Context, storage storeapi.Storage, logicalPath string, includeTombstone bool) ([]string, error) {
	prefixes, ok := lockFamilyPrefixes(logicalPath)
	if !ok {
		return nil, errors.Errorf("unknown lock family %s", logicalPath)
	}

	var candidates []string
	for _, pfx := range prefixes {
		fileName := path.Base(pfx)
		dirName := path.Dir(pfx)
		if dirName == "." {
			dirName = ""
		}
		err := storage.WalkDir(ctx, &storeapi.WalkOption{
			SubDir:           dirName,
			ObjPrefix:        fileName,
			IncludeTombstone: includeTombstone,
		}, func(p string, _ int64) error {
			candidates = append(candidates, p)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return candidates, nil
}

func stripLockIntentSuffix(objectPath string) (lockPath string, isIntent bool) {
	if idx := strings.Index(objectPath, ".INTENT."); idx >= 0 {
		return objectPath[:idx], true
	}
	return objectPath, false
}

func isLeaseLockInstancePath(committedPath, prefix string) bool {
	suffix, ok := strings.CutPrefix(committedPath, prefix+".")
	return ok && is32Hex(suffix)
}

func isLegacyMigrationReadLockPath(committedPath string) bool {
	suffix, ok := strings.CutPrefix(committedPath, migrationReadLockPrefix+".")
	return ok && is16Hex(suffix)
}

func markCleanupEligibleIfCommitted(member lockFamilyMember) lockFamilyMember {
	member.cleanupEligible = !member.intent
	return member
}

func classifyTruncateLockMember(member lockFamilyMember, committedPath string) lockFamilyMember {
	switch {
	case committedPath == truncateLockPath:
		member.kind = lockMemberTruncate
	case isLeaseLockInstancePath(committedPath, truncateLockPath):
		member.kind = lockMemberTruncate
		member = markCleanupEligibleIfCommitted(member)
	}
	return member
}

func classifyMigrationLockMember(member lockFamilyMember, committedPath string) lockFamilyMember {
	switch {
	case committedPath == migrationWriteLockPrefix:
		member.kind = lockMemberMigrationWrite
	case isLeaseLockInstancePath(committedPath, migrationWriteLockPrefix):
		member.kind = lockMemberMigrationWrite
		member = markCleanupEligibleIfCommitted(member)
	case isLegacyMigrationReadLockPath(committedPath):
		member.kind = lockMemberMigrationRead
	case isLeaseLockInstancePath(committedPath, migrationReadLockPrefix):
		member.kind = lockMemberMigrationRead
		member = markCleanupEligibleIfCommitted(member)
	}
	return member
}

func classifyAppendLockMember(member lockFamilyMember, committedPath string) lockFamilyMember {
	switch {
	case committedPath == appendWriteLockPrefix:
		member.kind = lockMemberAppendWrite
	case isLeaseLockInstancePath(committedPath, appendWriteLockPrefix):
		member.kind = lockMemberAppendWrite
		member = markCleanupEligibleIfCommitted(member)
	}
	return member
}

func classifyLockFamilyMember(logicalPath, objectPath, ownIntent string) lockFamilyMember {
	member := lockFamilyMember{
		path:      objectPath,
		kind:      lockMemberUnknown,
		ownIntent: objectPath == ownIntent,
	}
	if member.ownIntent {
		return member
	}

	committedPath, isIntent := stripLockIntentSuffix(objectPath)
	member.intent = isIntent

	switch logicalPath {
	case truncateLockPath:
		return classifyTruncateLockMember(member, committedPath)
	case migrationLockPath:
		return classifyMigrationLockMember(member, committedPath)
	case appendLockPath:
		return classifyAppendLockMember(member, committedPath)
	}
	return member
}

func verifyLockFamily(ctx VerifyWriteContext, logicalPath string, acquireKind lockAcquireKind) error {
	candidates, err := listLockFamilyCandidates(ctx, ctx.Storage, logicalPath, true)
	if err != nil {
		return err
	}
	for _, candidate := range candidates {
		member := classifyLockFamilyMember(logicalPath, candidate, ctx.IntentFileName())
		if member.ownIntent {
			continue
		}
		if lockFamilyConflicts(acquireKind, member.kind) {
			return lockFamilyConflictError{member: member}
		}
	}
	return nil
}

func lockFamilyConflicts(acquireKind lockAcquireKind, memberKind lockMemberKind) bool {
	switch acquireKind {
	case lockAcquireTruncate:
		return memberKind == lockMemberTruncate || memberKind == lockMemberUnknown
	case lockAcquireMigrationWrite:
		return memberKind == lockMemberMigrationWrite || memberKind == lockMemberMigrationRead || memberKind == lockMemberUnknown
	case lockAcquireMigrationRead:
		return memberKind == lockMemberMigrationWrite || memberKind == lockMemberUnknown
	case lockAcquireAppendWrite:
		return memberKind == lockMemberAppendWrite || memberKind == lockMemberUnknown
	default:
		return true
	}
}

var errLockMissingExpireAt = stderrors.New("lock missing ExpireAt")

func cleanUpStaleLockInstance(ctx context.Context, storage storeapi.Storage, path string) (reclaimed bool, err error) {
	meta, err := getLockMeta(ctx, storage, path)
	if err != nil {
		// Lock file may have been deleted between caller's observation and
		// our read. Confirm with FileExists so callers do not need to know
		// backend-specific "not found" error shapes.
		exists, existsErr := storage.FileExists(ctx, path)
		if existsErr != nil {
			return false, multierr.Append(err, errors.Annotatef(existsErr, "CleanUpStaleLock: FileExists %s", path))
		}
		if !exists {
			return false, nil
		}
		return false, err
	}
	if meta.ExpireAt.IsZero() {
		log.Warn("Encountered cleanup-eligible lock instance without ExpireAt; will not auto-reclaim. "+
			"Use `br log unlock --force` to clear manually if needed.",
			zap.String("path", path), zap.Stringer("meta", meta))
		return false, errLockMissingExpireAt
	}
	now := nowFunc()
	reclaimAfter := meta.ExpireAt.Add(LeaseTTL)
	if !now.After(reclaimAfter) {
		return false, nil
	}

	if err := storage.DeleteFile(ctx, path); err != nil {
		return false, errors.Annotatef(err, "CleanUpStaleLock: DeleteFile %s", path)
	}
	log.Info("Reclaimed stale lock.",
		zap.String("path", path),
		zap.Time("reclaim_after", reclaimAfter),
		zap.Stringer("original_meta", meta))
	return true, nil
}

func cleanUpStaleLockFamily(ctx context.Context, storage storeapi.Storage, logicalPath string, returnCandidateErrors bool) (bool, error) {
	if _, ok := lockFamilyPrefixes(logicalPath); !ok {
		return false, nil
	}

	candidates, err := listLockFamilyCandidates(ctx, storage, logicalPath, false)
	if err != nil {
		return false, err
	}

	anyReclaimed := false
	var cleanupErr error
	for _, candidate := range candidates {
		member := classifyLockFamilyMember(logicalPath, candidate, "")
		if !member.cleanupEligible {
			if member.kind == lockMemberUnknown && !member.intent {
				log.Warn("Stale-lock cleanup: skipping unknown protected-prefix object.",
					zap.String("logical_path", logicalPath),
					zap.String("path", member.path))
			}
			continue
		}

		reclaimed, err := cleanUpStaleLockInstance(ctx, storage, member.path)
		if err != nil {
			log.Warn("Stale-lock cleanup: candidate cleanup failed; continuing with next candidate.",
				zap.String("logical_path", logicalPath),
				zap.String("path", member.path),
				logutil.ShortError(err))
			cleanupErr = multierr.Append(cleanupErr, err)
			continue
		}
		if reclaimed {
			anyReclaimed = true
		}
	}
	if !returnCandidateErrors {
		cleanupErr = nil
	}
	return anyReclaimed, cleanupErr
}

func tryCleanUpStaleLockFamily(ctx context.Context, storage storeapi.Storage, logicalPath string) bool {
	reclaimed, err := cleanUpStaleLockFamily(ctx, storage, logicalPath, true)
	if err != nil {
		log.Warn("Stale-lock cleanup: family cleanup had candidate errors; continuing retry loop.",
			zap.String("logical_path", logicalPath),
			logutil.ShortError(err))
	}
	return reclaimed
}
