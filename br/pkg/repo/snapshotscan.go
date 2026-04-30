// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package repo

import (
	"context"
	"iter"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/metautil"
	brstorage "github.com/pingcap/tidb/br/pkg/storage"
	"go.uber.org/zap"
)

// PendingMarker describes one repo marker object under `_meta/pending/`.
// A pending marker means BR recorded a snapshot backup attempt in the repository
// and has not yet discarded that attempt. Its presence alone does not prove the
// backup is resumable or completed; callers must still inspect the snapshot
// metadata and checkpoint state for that BackupID.
type PendingMarker struct {
	// BackupID identifies the snapshot backup attempt encoded in the marker file
	// name `<backup-id>.json`.
	BackupID BackupID
	// ConfigHash is the config-hash namespace segment between
	// `_meta/pending/` and the marker filename. BR-generated paths use
	// PendingConfigHashStorageName, so the stored value is typically upper-case
	// hex, but parsers only require it to occupy exactly one path segment.
	ConfigHash string
	// Path is the storage-relative object key returned by the backing storage,
	// typically `_meta/pending/<config-hash>/<backup-id>.json`. Callers should
	// treat it as an opaque repo path, not a local filesystem path.
	Path string
}

// SnapshotDataFile describes one repo snapshot data object under
// `_data/snapshot/`.
// The stable directory layout is `_data/snapshot/<store-id>/<backup-id>/<...>`.
// All files with the same StoreID and BackupID belong to the same per-store
// subtree for one snapshot attempt; the remaining suffix is repo-managed data
// path and may contain additional directories.
type SnapshotDataFile struct {
	// StoreID is the decimal TiKV store ID taken from the path segment
	// `_data/snapshot/<store-id>/...`.
	StoreID uint64
	// BackupID is the snapshot backup attempt ID taken from the path segment
	// `_data/snapshot/<store-id>/<backup-id>/...`.
	BackupID BackupID
	// Path is the storage-relative object key returned by the backing storage.
	// Consumers may rely on it residing under the StoreID/BackupID subtree, but
	// should not assume a particular filename or suffix format beyond at least
	// one additional path component after `<backup-id>/`.
	Path string
}

// TrySeq is an error-first iterator of `(error, T)` pairs built on
// `iter.Seq2`. Producers yield `(nil, item)` for normal values and may yield a
// terminal `(err, zero)` pair when scanning fails. Consumers typically range as
// `for err, item := range seq`, check `err` first, and stop once it becomes
// non-nil.
type TrySeq[T any] iter.Seq2[error, T]

var errStopWalkSeq = errors.New("stop walk seq")

// ListCompletedSnapshotIDs returns sorted backup IDs that have completed metadata.
// For example, `_meta/snapshot/000000000000F00D/backupmeta` yields
// BackupID(0xF00D).
func ListCompletedSnapshotIDs(ctx context.Context, storage brstorage.Storage) ([]BackupID, error) {
	ids := make(map[BackupID]struct{})
	err := storage.WalkDir(
		ctx,
		&brstorage.WalkOption{SubDir: snapshotMetadataRootDir},
		func(filePath string, _ int64) error {
			backupID, ok, err := ParseCompletedSnapshotMetaPath(filePath)
			if err != nil {
				return errors.Trace(err)
			}
			if ok {
				ids[backupID] = struct{}{}
			}
			return nil
		},
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	out := make([]BackupID, 0, len(ids))
	for id := range ids {
		out = append(out, id)
	}
	slices.Sort(out)
	return out, nil
}

// ListDeletingSnapshotIDs returns sorted backup IDs that have a DELETING marker.
func ListDeletingSnapshotIDs(ctx context.Context, storage brstorage.Storage) ([]BackupID, error) {
	ids := make(map[BackupID]struct{})
	for err, backupID := range WalkSnapshotDeletingMarkers(ctx, storage) {
		if err != nil {
			return nil, errors.Annotate(err, "walk snapshot deleting markers")
		}
		ids[backupID] = struct{}{}
	}
	out := make([]BackupID, 0, len(ids))
	for id := range ids {
		out = append(out, id)
	}
	slices.Sort(out)
	return out, nil
}

// WalkSnapshotDeletingMarkers streams valid DELETING marker objects under
// `_meta/snapshot/`. For example,
// `_meta/snapshot/000000000000F00D/DELETING` yields BackupID(0xF00D).
func WalkSnapshotDeletingMarkers(ctx context.Context, storage brstorage.Storage) TrySeq[BackupID] {
	return walkParsedSeq(
		ctx,
		storage,
		&brstorage.WalkOption{SubDir: snapshotMetadataRootDir},
		ParseSnapshotDeletingMarkerPath,
	)
}

// WalkPendingMarkers streams valid pending marker objects under `_meta/pending/`.
// For example, `_meta/pending/<hash>/000000000000F00D.json` yields
// BackupID(0xF00D).
func WalkPendingMarkers(ctx context.Context, storage brstorage.Storage) TrySeq[PendingMarker] {
	return walkPendingMarkers(ctx, storage, &brstorage.WalkOption{SubDir: pendingRootDir})
}

func walkPendingMarkers(
	ctx context.Context,
	storage brstorage.Storage,
	opt *brstorage.WalkOption,
) TrySeq[PendingMarker] {
	return walkParsedSeq(
		ctx,
		storage,
		opt,
		ParsePendingMarkerPath,
	)
}

// WalkSnapshotDataFiles streams valid snapshot data objects under `_data/snapshot/`.
// For example, `_data/snapshot/7/000000000000F00D/a.sst` yields StoreID 7 and
// BackupID(0xF00D).
func WalkSnapshotDataFiles(
	ctx context.Context,
	storage brstorage.Storage,
) TrySeq[SnapshotDataFile] {
	return walkParsedSeq(
		ctx,
		storage,
		&brstorage.WalkOption{SubDir: snapshotDataRootDir},
		ParseSnapshotDataFilePath,
	)
}

func walkParsedSeq[T any](
	ctx context.Context,
	storage brstorage.Storage,
	opt *brstorage.WalkOption,
	parse func(string) (T, bool, error),
) TrySeq[T] {
	return func(yield func(error, T) bool) {
		err := storage.WalkDir(ctx, opt, func(filePath string, _ int64) error {
			item, ok, err := parse(filePath)
			if err != nil {
				yield(errors.Trace(err), item)
				return errStopWalkSeq
			}
			if !ok {
				return nil
			}
			if yield(nil, item) {
				return nil
			}
			return errStopWalkSeq
		})
		if err != nil && !errors.ErrorEqual(err, errStopWalkSeq) {
			var zero T
			yield(errors.Trace(err), zero)
		}
	}
}

// ParseCompletedSnapshotMetaPath parses a completed snapshot metadata path.
// For example, `_meta/snapshot/000000000000F00D/backupmeta` returns
// BackupID(0xF00D), true, nil.
func ParseCompletedSnapshotMetaPath(filePath string) (id BackupID, parsed bool, err error) {
	parts := splitRepoPath(filePath)
	if len(parts) < 2 || parts[0] != "_meta" || parts[1] != "snapshot" {
		return 0, false, nil
	}
	if len(parts) == 4 && parts[3] == snapshotDeletingMarkerFile {
		return 0, false, nil
	}
	defer func() {
		logSkippedRepoPath(
			parsed,
			err,
			filePath,
			"skip non-completed repo snapshot metadata path while scanning completed snapshots",
		)
	}()
	if len(parts) < 4 {
		return 0, false, nil
	}
	base := parts[len(parts)-1]
	if base != metautil.MetaFile && !strings.HasPrefix(base, metautil.MetaFile+".") {
		return 0, false, nil
	}
	backupID, err := ParseBackupIDStorageName(parts[2])
	if err != nil {
		return 0, false, errors.Annotatef(err, "parse snapshot metadata path %s", filePath)
	}
	return backupID, true, nil
}

// ParseSnapshotDeletingMarkerPath parses one repo deleting marker path.
// For example, `_meta/snapshot/000000000000F00D/DELETING` returns
// BackupID(0xF00D), true, nil.
func ParseSnapshotDeletingMarkerPath(filePath string) (id BackupID, parsed bool, err error) {
	parts := splitRepoPath(filePath)
	if len(parts) != 4 ||
		parts[0] != "_meta" ||
		parts[1] != "snapshot" ||
		parts[3] != snapshotDeletingMarkerFile {
		return 0, false, nil
	}
	backupID, err := ParseBackupIDStorageName(parts[2])
	if err != nil {
		return 0, false, errors.Annotatef(err, "parse snapshot deleting marker path %s", filePath)
	}
	return backupID, true, nil
}

// ParsePendingMarkerPath parses one repo pending marker path.
// For example, `_meta/pending/ABCD/000000000000F00D.json` returns a marker with
// BackupID(0xF00D).
func ParsePendingMarkerPath(filePath string) (marker PendingMarker, parsed bool, err error) {
	parts := splitRepoPath(filePath)
	if len(parts) < 2 || parts[0] != "_meta" || parts[1] != "pending" {
		return PendingMarker{}, false, nil
	}
	defer func() {
		logSkippedRepoPath(parsed, err, filePath, "skip invalid repo pending marker path while scanning pending markers")
	}()
	if len(parts) != 4 {
		return PendingMarker{}, false, nil
	}
	base := parts[3]
	if path.Ext(base) != ".json" {
		return PendingMarker{}, false, nil
	}
	backupID, err := ParseBackupIDStorageName(strings.TrimSuffix(base, ".json"))
	if err != nil {
		return PendingMarker{}, false, errors.Annotatef(err, "parse pending marker path %s", filePath)
	}
	return PendingMarker{
		BackupID:   backupID,
		ConfigHash: parts[2],
		Path:       filePath,
	}, true, nil
}

// ParseSnapshotDataFilePath parses one repo snapshot data object path.
// For example, `_data/snapshot/7/000000000000F00D/a.sst` returns StoreID 7 and
// BackupID(0xF00D).
func ParseSnapshotDataFilePath(filePath string) (dataFile SnapshotDataFile, parsed bool, err error) {
	parts := splitRepoPath(filePath)
	if len(parts) < 2 || parts[0] != "_data" || parts[1] != "snapshot" {
		return SnapshotDataFile{}, false, nil
	}
	defer func() {
		logSkippedRepoPath(
			parsed,
			err,
			filePath,
			"skip invalid repo snapshot data path while scanning snapshot data files",
		)
	}()
	if len(parts) < 5 {
		return SnapshotDataFile{}, false, nil
	}
	storeID, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return SnapshotDataFile{}, false, errors.Annotatef(err, "parse snapshot data store id from %s", filePath)
	}
	backupID, err := ParseBackupIDStorageName(parts[3])
	if err != nil {
		return SnapshotDataFile{}, false, errors.Annotatef(err, "parse snapshot data backup id from %s", filePath)
	}
	return SnapshotDataFile{
		StoreID:  storeID,
		BackupID: backupID,
		Path:     filePath,
	}, true, nil
}

func logSkippedRepoPath(parsed bool, err error, filePath, message string) {
	if parsed {
		return
	}
	fields := []zap.Field{zap.String("path", filePath)}
	if err != nil {
		fields = append(fields, zap.Error(err))
	}
	log.Warn(message, fields...)
}

func splitRepoPath(filePath string) []string {
	cleaned := path.Clean(strings.TrimSpace(filePath))
	cleaned = strings.TrimPrefix(cleaned, "/")
	if cleaned == "." || cleaned == "" {
		return nil
	}
	return strings.Split(cleaned, "/")
}
