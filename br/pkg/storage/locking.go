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
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

// ExclusiveWrite is a write that in a strong consistency storage, it will either:
// - The written file won't be override by other `ExclusiveWrite` writers.
// - Or it files.
type ExclusiveWrite struct {
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

func (c *VerifyWriteContext) IntentFileName() string {
	return fmt.Sprintf("%s.INTENT.%s", c.Target, hex.EncodeToString(c.TxnID[:]))
}

// CommitTo commits the write to the external storage.
// It contains two phases:
// - Intention phase, it will write an "intention" file named "$Target_$TxnID".
// - Put phase, here it actually write the "$Target" down.
//
// In each phase, before writing, it will verify whether the storage is suitable for writing, that is:
// - There shouldn't be any other intention files.
func (w ExclusiveWrite) CommitTo(ctx context.Context, s ExternalStorage) (uuid.UUID, error) {
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
		return multierr.Append(err, assertNoOtherFileOfPrefix(cx, s, w.Target, intentFileName))
	}

	if err := checkConflict(); err != nil {
		return uuid.UUID{}, errors.Annotate(err, "during initial check")
	}

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

	return txnID, s.WriteFile(cx, w.Target, w.Content(txnID))
}

func assertNoOtherFileOfPrefix(ctx context.Context, s ExternalStorage, pfx string, expect string) error {
	fileName := path.Base(pfx)
	dirName := path.Dir(pfx)
	return s.WalkDir(ctx, &WalkOption{
		SubDir:    dirName,
		ObjPrefix: fileName,
	}, func(path string, size int64) error {
		if path != expect {
			return fmt.Errorf("there is conflict file %s", path)
		}
		return nil
	})
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

// TryLockRemote tries to create a "lock file" at the external storage.
// If success, we will create a file at the path provided. So others may not access the file then.
// Will return a `ErrLocked` if there is another process already creates the lock file.
// This isn't a strict lock like flock in linux: that means, the lock might be forced removed by
// manually deleting the "lock file" in external storage.
func TryLockRemote(ctx context.Context, storage ExternalStorage, path, hint string) (lock RemoteLock, err error) {
	writer := ExclusiveWrite{
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

func TryLockRemoteWrite(ctx context.Context, storage ExternalStorage, path, hint string) (lock RemoteLock, err error) {
	writer := ExclusiveWrite{
		Target: fmt.Sprintf("%s.WRIT", path),
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
			return assertNoOtherFileOfPrefix(ctx, ctx.Storage, ctx.Target, ctx.IntentFileName())
		},
	}

	lock.storage = storage
	lock.path = path
	lock.txnID, err = writer.CommitTo(ctx, storage)
	return
}

func TryLockRemoteRead(ctx context.Context, storage ExternalStorage, path, hint string) (lock RemoteLock, err error) {
	readSpec := rand.Int63()
	writer := ExclusiveWrite{
		Target: fmt.Sprintf("%s.READ.%016x", path, readSpec),
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
			return assertNoOtherFileOfPrefix(ctx, ctx.Storage, fmt.Sprintf("%s.WRIT", ctx.Target), "")
		},
	}

	lock.storage = storage
	lock.path = path
	lock.txnID, err = writer.CommitTo(ctx, storage)
	return
}
