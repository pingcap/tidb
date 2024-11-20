// Copyright 2023 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
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

type VerifyWriteContext struct {
	context.Context
	Target  string
	Storage ExternalStorage
	TxnID   uuid.UUID
}

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
func (w conditionalPut) CommitTo(ctx context.Context, s ExternalStorage) (uuid.UUID, error) {
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
	failpoint.Inject("exclusive-write-commit-to-1", func() {})

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
	failpoint.Inject("exclusive-write-commit-to-2", func() {})

	return txnID, s.WriteFile(cx, w.Target, w.Content(txnID))
}

// assertNoOtherOfPrefixExpect asserts that there is no other file with the same prefix than the expect file.
func (cx VerifyWriteContext) assertNoOtherOfPrefixExpect(pfx string, expect string) error {
	fileName := path.Base(pfx)
	dirName := path.Dir(pfx)
	return cx.Storage.WalkDir(cx, &WalkOption{
		SubDir:    dirName,
		ObjPrefix: fileName,
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

// LockMeta is the meta information of a lock.
type LockMeta struct {
	LockedAt   time.Time `json:"locked_at"`
	LockerHost string    `json:"locker_host"`
	LockerPID  int       `json:"locker_pid"`
	TxnID      []byte    `json:"txn_id"`
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

type RemoteLock struct {
	txnID   uuid.UUID
	storage ExternalStorage
	path    string
}

func tryFetchRemoteLock(ctx context.Context, storage ExternalStorage, path string) error {
	meta, err := readLockMeta(ctx, storage, path)
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
func TryLockRemote(ctx context.Context, storage ExternalStorage, path, hint string) (lock RemoteLock, err error) {
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

	lock.storage = storage
	lock.path = path
	lock.txnID, err = writer.CommitTo(ctx, storage)
	if err != nil {
		err = errors.Annotatef(err, "there is something about the lock: %s", tryFetchRemoteLock(ctx, storage, path))
	}
	return
}

// UnlockRemote removes the lock file at the specified path.
// Removing that file will release the lock.
func (l RemoteLock) Unlock(ctx context.Context) error {
	meta, err := readLockMeta(ctx, l.storage, l.path)
	if err != nil {
		return err
	}
	// NOTE: this is for debug usage. For now, there isn't an Compare-And-Swap
	// operation in our ExternalStorage abstraction.
	// So, once our lock has been overwritten or we are overwriting other's lock,
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

func writeLockName(path string) string {
	return fmt.Sprintf("%s.WRIT", path)
}

func newReadLockName(path string) string {
	readID := rand.Int63()
	return fmt.Sprintf("%s.READ.%016x", path, readID)
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
