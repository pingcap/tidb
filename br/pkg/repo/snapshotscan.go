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
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

type PendingMarker struct {
	BackupID   BackupID
	ConfigHash string
	Path       string
}

type SnapshotDataFile struct {
	StoreID  uint64
	BackupID BackupID
	Path     string
}

var errStopWalkSeq = errors.New("stop walk seq")

func ListCompletedSnapshotIDs(ctx context.Context, storage storeapi.Storage) ([]BackupID, error) {
	ids := make(map[BackupID]struct{})
	err := storage.WalkDir(ctx, &storeapi.WalkOption{SubDir: path.Join("_meta", "snapshot")}, func(filePath string, _ int64) error {
		backupID, ok, err := ParseCompletedSnapshotMetaPath(filePath)
		if err != nil {
			return errors.Trace(err)
		}
		if ok {
			ids[backupID] = struct{}{}
		}
		return nil
	})
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

func WalkPendingMarkers(ctx context.Context, storage storeapi.Storage) iter.Seq2[PendingMarker, error] {
	return walkParsedSeq(
		ctx,
		storage,
		&storeapi.WalkOption{SubDir: path.Join("_meta", "pending")},
		ParsePendingMarkerPath,
	)
}

func WalkSnapshotDataFiles(
	ctx context.Context,
	storage storeapi.Storage,
) iter.Seq2[SnapshotDataFile, error] {
	return walkParsedSeq(
		ctx,
		storage,
		&storeapi.WalkOption{SubDir: path.Join("_data", "snapshot")},
		ParseSnapshotDataFilePath,
	)
}

func walkParsedSeq[T any](
	ctx context.Context,
	storage storeapi.Storage,
	opt *storeapi.WalkOption,
	parse func(string) (T, bool, error),
) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		err := storage.WalkDir(ctx, opt, func(filePath string, _ int64) error {
			item, ok, err := parse(filePath)
			if err != nil {
				yield(item, errors.Trace(err))
				return errStopWalkSeq
			}
			if !ok {
				return nil
			}
			if yield(item, nil) {
				return nil
			}
			return errStopWalkSeq
		})
		if err != nil && !errors.ErrorEqual(err, errStopWalkSeq) {
			var zero T
			yield(zero, errors.Trace(err))
		}
	}
}

func ParseCompletedSnapshotMetaPath(filePath string) (BackupID, bool, error) {
	parts := splitRepoPath(filePath)
	if len(parts) < 4 || parts[0] != "_meta" || parts[1] != "snapshot" {
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

func ParsePendingMarkerPath(filePath string) (PendingMarker, bool, error) {
	parts := splitRepoPath(filePath)
	if len(parts) != 4 || parts[0] != "_meta" || parts[1] != "pending" {
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

func ParseSnapshotDataFilePath(filePath string) (SnapshotDataFile, bool, error) {
	parts := splitRepoPath(filePath)
	if len(parts) < 5 || parts[0] != "_data" || parts[1] != "snapshot" {
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

func splitRepoPath(filePath string) []string {
	cleaned := path.Clean(strings.TrimSpace(filePath))
	cleaned = strings.TrimPrefix(cleaned, "/")
	if cleaned == "." || cleaned == "" {
		return nil
	}
	return strings.Split(cleaned, "/")
}
